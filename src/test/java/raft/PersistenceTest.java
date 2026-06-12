package raft;

import java.util.List;

import org.junit.jupiter.api.Test;

import raft.kv.KvStateMachine;
import raft.sim.SimulatedCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Crash-recovery: a node is its Storage. Everything else — role, commit
 * index, applied state — is volatile and gets rediscovered from the log.
 */
class PersistenceTest {

    @Test
    void committedStateSurvivesWholeClusterCrash() {
        SimulatedCluster cluster = new SimulatedCluster(3, 31);
        cluster.awaitLeader(500);

        List<String> expected = new java.util.ArrayList<>();
        for (int i = 0; i < 5; i++) {
            cluster.propose(KvStateMachine.set("key" + i, "value" + i));
            expected.add("SET key" + i + " value" + i);
            cluster.run(2);
        }
        cluster.runUntil(() -> cluster.nodeIds().stream()
                .allMatch(id -> cluster.kv(id).appliedCommands().equals(expected)), 500);

        // Power cut to the whole cluster. RaftNode instances are discarded;
        // only Storage (term, vote, log) survives.
        for (int id : cluster.nodeIds()) {
            cluster.crash(id);
        }
        cluster.run(50);
        for (int id : cluster.nodeIds()) {
            cluster.restart(id);
        }

        // The new leader re-establishes the commit index and every node
        // re-applies the same five commands from its persisted log.
        cluster.awaitLeader(1000);
        cluster.propose(KvStateMachine.set("after", "restart"));
        cluster.runUntil(() -> cluster.nodeIds().stream()
                .allMatch(id -> "restart".equals(cluster.kv(id).get("after"))), 500);

        for (int id : cluster.nodeIds()) {
            assertEquals(5 + 1, cluster.kv(id).appliedCommands().size());
            for (int i = 0; i < 5; i++) {
                assertEquals("value" + i, cluster.kv(id).get("key" + i));
            }
        }
    }

    @Test
    void crashedFollowerRejoinsAndCatchesUp() {
        SimulatedCluster cluster = new SimulatedCluster(3, 32);
        int leader = cluster.awaitLeader(500);
        int victim = cluster.nodeIds().stream().filter(id -> id != leader).findFirst().orElseThrow();

        cluster.propose(KvStateMachine.set("a", "1"));
        cluster.runUntil(() -> cluster.nodeIds().stream()
                .allMatch(id -> "1".equals(cluster.kv(id).get("a"))), 500);

        cluster.crash(victim);
        cluster.propose(KvStateMachine.set("b", "2"));
        cluster.propose(KvStateMachine.set("a", "3")); // overwrite while it's down
        cluster.runUntil(() -> "3".equals(cluster.kv(leader).get("a")), 500);

        cluster.restart(victim);
        cluster.runUntil(() -> "3".equals(cluster.kv(victim).get("a"))
                && "2".equals(cluster.kv(victim).get("b")), 500);
    }
}
