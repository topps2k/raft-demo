package raft;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import raft.core.RaftNode;
import raft.core.Role;
import raft.kv.KvStateMachine;
import raft.sim.SimulatedCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The split-brain scenarios that motivate consensus in the first place.
 */
class PartitionTest {

    /**
     * A leader cut off with a minority keeps accepting proposals (it can't
     * know better) but can never commit them; the majority side elects a new
     * leader and moves on. On healing, the old leader steps down, its
     * uncommitted entries are truncated, and every node converges on the
     * majority's history — the stale write is gone everywhere.
     */
    @Test
    void deposedMinorityLeaderCannotCommitAndItsWritesVanish() {
        SimulatedCluster cluster = new SimulatedCluster(5, 21);
        int oldLeader = cluster.awaitLeader(500);
        int minorityPeer = cluster.nodeIds().stream().filter(id -> id != oldLeader).findFirst().orElseThrow();
        List<Integer> majority = cluster.nodeIds().stream()
                .filter(id -> id != oldLeader && id != minorityPeer).toList();

        cluster.partition(new int[]{oldLeader, minorityPeer},
                majority.stream().mapToInt(Integer::intValue).toArray());

        // The stranded leader still accepts the write...
        RaftNode.ProposeResult stale = cluster.proposeTo(oldLeader, KvStateMachine.set("stale", "x"));
        assertTrue(stale.accepted());

        // ...but with only 2 of 5 replicas it must never commit it.
        cluster.run(300);
        assertTrue(cluster.node(oldLeader).commitIndex() < stale.index(),
                "an entry on 2/5 nodes is not committed");

        // The majority side recovers and commits new writes meanwhile.
        cluster.awaitLeaderAmong(majority, 1000);
        cluster.propose(KvStateMachine.set("fresh", "y"));
        cluster.runUntil(() -> majority.stream()
                .allMatch(id -> "y".equals(cluster.kv(id).get("fresh"))), 500);

        // Heal: the old leader is deposed and rewritten with the majority history.
        cluster.heal();
        cluster.runUntil(() -> cluster.node(oldLeader).role() == Role.FOLLOWER, 500);
        cluster.runUntil(() -> cluster.nodeIds().stream()
                .allMatch(id -> "y".equals(cluster.kv(id).get("fresh"))), 500);

        for (int id : cluster.nodeIds()) {
            assertNull(cluster.kv(id).get("stale"), "node " + id + " must not apply the uncommitted write");
            assertTrue(cluster.kv(id).appliedCommands().stream().noneMatch(c -> c.contains("stale")));
        }
    }

    /** With no majority anywhere (2/2/1 split), the cluster stalls rather than diverges. */
    @Test
    void noQuorumMeansNoProgress() {
        SimulatedCluster cluster = new SimulatedCluster(5, 22);
        cluster.awaitLeader(500);
        cluster.propose(KvStateMachine.set("before", "split"));
        cluster.runUntil(() -> cluster.nodeIds().stream()
                .allMatch(id -> "split".equals(cluster.kv(id).get("before"))), 500);

        List<Integer> ids = cluster.nodeIds();
        cluster.partition(new int[]{ids.get(0), ids.get(1)},
                new int[]{ids.get(2), ids.get(3)},
                new int[]{ids.get(4)});

        Map<Integer, Long> commitBefore = ids.stream()
                .collect(java.util.stream.Collectors.toMap(id -> id, id -> cluster.node(id).commitIndex()));

        // Whoever still thinks it's leader may accept a proposal...
        for (int id : ids) {
            cluster.proposeTo(id, KvStateMachine.set("during", "split"));
        }
        cluster.run(500);

        // ...but consensus is impossible: nothing commits anywhere.
        for (int id : ids) {
            assertEquals(commitBefore.get(id), cluster.node(id).commitIndex(),
                    "node " + id + " advanced its commit index without a quorum");
            assertNull(cluster.kv(id).get("during"));
        }
    }
}
