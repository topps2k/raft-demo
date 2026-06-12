package raft;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import raft.kv.KvStateMachine;
import raft.sim.SimulatedCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sustained pipelined load through the simulator: many proposals in flight at
 * once, never waiting for one to commit before sending the next. Checks the
 * things load exposes that one-at-a-time tests can't: replication keeps pace
 * (bounded lag), batching works, ordering holds at volume.
 */
class ThroughputTest {

    @Test
    void sustainedPipelinedLoadCommitsEverythingInOrder() {
        SimulatedCluster cluster = new SimulatedCluster(5, 99);
        cluster.awaitLeader(500);
        long loadStartTick = cluster.now();

        int total = 5_000;
        int perTick = 5; // 5 new proposals per tick, no waiting for commits
        List<String> expected = new ArrayList<>(total);
        long wallStart = System.nanoTime();

        int proposed = 0;
        while (proposed < total) {
            for (int j = 0; j < perTick && proposed < total; j++) {
                cluster.propose(KvStateMachine.set("key" + proposed, "value" + proposed));
                expected.add("SET key" + proposed + " value" + proposed);
                proposed++;
            }
            cluster.tick();
        }

        // Drain: everything proposed must commit and apply on every node.
        cluster.runUntil(() -> cluster.nodeIds().stream()
                .allMatch(id -> cluster.kv(id).appliedCount() == total), 2_000);
        double wallSeconds = (System.nanoTime() - wallStart) / 1e9;

        for (int id : cluster.nodeIds()) {
            assertEquals(expected, cluster.kv(id).appliedCommands(),
                    "node " + id + " must apply all " + total + " commands in proposal order");
        }

        // Commits must keep PACE with proposals, not pile up: the whole run
        // (load + drain) should take barely longer than the load itself. A
        // stalled pipeline (broken batching, regressing nextIndex, commit only
        // on heartbeat...) blows well past this deterministic bound.
        long ticksUsed = cluster.now() - loadStartTick;
        long loadTicks = total / perTick;
        assertTrue(ticksUsed < loadTicks + 100,
                "replication lagged " + (ticksUsed - loadTicks) + " ticks behind a "
                        + loadTicks + "-tick load (drain should be a few delays, not a backlog)");

        System.out.printf("sim throughput: %d commands fully replicated to 5 nodes in %d ticks, %.2fs wall (%.0f cmds/s)%n",
                total, ticksUsed, wallSeconds, total / wallSeconds);
    }
}
