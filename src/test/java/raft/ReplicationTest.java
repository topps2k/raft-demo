package raft;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import raft.core.RaftNode;
import raft.kv.KvStateMachine;
import raft.sim.SimulatedCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Log replication (§5.3): commands flow leader → followers, commit on a
 * majority, and apply in identical order everywhere.
 */
class ReplicationTest {

    @Test
    void committedCommandIsAppliedOnEveryNode() {
        SimulatedCluster cluster = new SimulatedCluster(3, 11);
        cluster.awaitLeader(500);

        cluster.propose(KvStateMachine.set("color", "teal"));

        cluster.runUntil(() -> cluster.nodeIds().stream()
                .allMatch(id -> "teal".equals(cluster.kv(id).get("color"))), 500);
    }

    @Test
    void commandsApplyInProposalOrderOnAllNodes() {
        SimulatedCluster cluster = new SimulatedCluster(5, 12);
        cluster.awaitLeader(500);

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            cluster.propose(KvStateMachine.set("key" + i, "value" + i));
            expected.add("SET key" + i + " value" + i);
            cluster.run(2); // interleave proposals with network traffic
        }

        cluster.runUntil(() -> cluster.nodeIds().stream()
                .allMatch(id -> cluster.kv(id).appliedCommands().size() == expected.size()), 1000);
        for (int id : cluster.nodeIds()) {
            assertEquals(expected, cluster.kv(id).appliedCommands(),
                    "node " + id + " must apply every command, in proposal order");
        }
    }

    @Test
    void followerRejectsProposalWithLeaderHint() {
        SimulatedCluster cluster = new SimulatedCluster(3, 13);
        int leader = cluster.awaitLeader(500);
        cluster.run(50); // let heartbeats teach everyone who leads
        int follower = cluster.nodeIds().stream().filter(id -> id != leader).findFirst().orElseThrow();

        RaftNode.ProposeResult result = cluster.proposeTo(follower, KvStateMachine.set("x", "1"));

        assertFalse(result.accepted());
        assertEquals(leader, result.leaderHint());
    }

    @Test
    void disconnectedFollowerCatchesUpAfterHealing() {
        SimulatedCluster cluster = new SimulatedCluster(3, 14);
        int leader = cluster.awaitLeader(500);
        int lagging = cluster.nodeIds().stream().filter(id -> id != leader).findFirst().orElseThrow();
        List<Integer> connected = cluster.nodeIds().stream().filter(id -> id != lagging).toList();

        cluster.partition(new int[]{lagging},
                connected.stream().mapToInt(Integer::intValue).toArray());

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            cluster.propose(KvStateMachine.set("key" + i, "value" + i));
            expected.add("SET key" + i + " value" + i);
            cluster.run(2);
        }
        cluster.runUntil(() -> connected.stream()
                .allMatch(id -> cluster.kv(id).appliedCommands().equals(expected)), 500);
        assertEquals(List.of(), cluster.kv(lagging).appliedCommands(),
                "the partitioned follower can't have seen anything");

        cluster.heal();
        cluster.runUntil(() -> cluster.kv(lagging).appliedCommands().equals(expected), 500);
    }
}
