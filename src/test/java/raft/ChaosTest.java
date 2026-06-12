package raft;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import raft.kv.KvStateMachine;
import raft.sim.SimulatedCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The heavy artillery: hundreds of randomized rounds of proposals, message
 * drops, delays, partitions, crashes and restarts, across many seeds. The
 * cluster checks Election Safety and State Machine Safety on every tick, so
 * any safety violation fails immediately — and because everything is seeded,
 * a failure replays identically for debugging.
 *
 * <p>At the end the network is made friendly again and we assert liveness
 * (a marker write commits everywhere) and convergence (every node applied
 * exactly the same command sequence, with no duplicates).
 */
class ChaosTest {

    private static final int NODES = 5;
    private static final int ROUNDS = 150;

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    void clusterSurvivesChaosAndConverges(long seed) {
        SimulatedCluster cluster = new SimulatedCluster(NODES, seed);
        cluster.setDropRate(0.15);
        cluster.setDelayRange(1, 5);
        Random chaos = new Random(seed * 7919);

        Set<Integer> down = new HashSet<>();
        int proposalCount = 0;

        for (int round = 0; round < ROUNDS; round++) {
            cluster.run(1 + chaos.nextInt(20));

            int action = chaos.nextInt(10);
            if (action < 5) {
                // Propose to a random node — usually not the leader; rejection
                // and stale-leader acceptance-then-truncation are both part of
                // the chaos.
                List<Integer> alive = cluster.aliveNodeIds();
                int target = alive.get(chaos.nextInt(alive.size()));
                cluster.proposeTo(target, KvStateMachine.set("key" + proposalCount, "value" + proposalCount));
                proposalCount++;
            } else if (action < 7) {
                if (!down.isEmpty() && chaos.nextBoolean()) {
                    int id = down.iterator().next();
                    cluster.restart(id);
                    down.remove(id);
                } else if (cluster.aliveNodeIds().size() > 3) {
                    List<Integer> alive = cluster.aliveNodeIds();
                    int id = alive.get(chaos.nextInt(alive.size()));
                    cluster.crash(id);
                    down.add(id);
                }
            } else if (action < 9) {
                List<Integer> shuffled = new ArrayList<>(cluster.nodeIds());
                java.util.Collections.shuffle(shuffled, chaos);
                int cut = 1 + chaos.nextInt(NODES - 1);
                cluster.partition(
                        shuffled.subList(0, cut).stream().mapToInt(Integer::intValue).toArray(),
                        shuffled.subList(cut, NODES).stream().mapToInt(Integer::intValue).toArray());
            } else {
                cluster.heal();
            }
        }

        // Storm over: heal everything and verify the cluster comes back.
        cluster.heal();
        cluster.setDropRate(0);
        cluster.setDelayRange(1, 3);
        for (int id : List.copyOf(down)) {
            cluster.restart(id);
        }

        // Liveness: a marker write must eventually commit on every node. The
        // first elected leader can still be deposed while the logs settle, so
        // allow a few attempts (each with a distinct value, since several
        // attempts may all legitimately commit).
        boolean markerEverywhere = false;
        for (int attempt = 0; attempt < 20 && !markerEverywhere; attempt++) {
            cluster.awaitLeader(2000);
            try {
                cluster.propose(KvStateMachine.set("marker", "done-" + attempt));
                proposalCount++;
            } catch (IllegalStateException raceLostLeader) {
                // try again next attempt
            }
            for (int i = 0; i < 500 && !markerEverywhere; i++) {
                cluster.tick();
                markerEverywhere = cluster.nodeIds().stream()
                        .allMatch(id -> cluster.kv(id).get("marker") != null);
            }
        }
        assertTrue(markerEverywhere, "cluster failed to recover after chaos (seed " + seed + ")");

        // With no further proposals, heartbeats drain everyone to the same point.
        cluster.runUntil(() -> cluster.nodeIds().stream()
                .allMatch(id -> cluster.kv(id).appliedCommands()
                        .equals(cluster.kv(1).appliedCommands())), 2000);

        // Convergence: identical applied sequences, every command at most once.
        List<String> reference = cluster.kv(1).appliedCommands();
        assertEquals(new HashSet<>(reference).size(), reference.size(),
                "a command was applied at two different log indices (seed " + seed + ")");
        assertTrue(reference.size() <= proposalCount,
                "more commands applied than were ever proposed (seed " + seed + ")");
    }
}
