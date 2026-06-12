package raft;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import raft.core.Role;
import raft.kv.KvStateMachine;
import raft.sim.SimulatedCluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Leader election (§5.2) and the election restriction (§5.4.1).
 * Election Safety itself (never two leaders in one term) is asserted
 * continuously by the cluster on every tick of every test.
 */
class ElectionTest {

    @Test
    void electsExactlyOneLeader() {
        SimulatedCluster cluster = new SimulatedCluster(3, 1);
        int leader = cluster.awaitLeader(500);

        // The other nodes settle as followers of the leader's term.
        cluster.runUntil(() -> cluster.nodeIds().stream()
                .filter(id -> id != leader)
                .allMatch(id -> cluster.node(id).role() == Role.FOLLOWER
                        && cluster.node(id).currentTerm() == cluster.node(leader).currentTerm()),
                500);
        assertEquals(leader, cluster.leaderId());
    }

    @Test
    void reElectsAfterLeaderCrash() {
        SimulatedCluster cluster = new SimulatedCluster(3, 2);
        int oldLeader = cluster.awaitLeader(500);
        long oldTerm = cluster.node(oldLeader).currentTerm();

        cluster.crash(oldLeader);
        List<Integer> rest = cluster.aliveNodeIds();
        int newLeader = cluster.awaitLeaderAmong(rest, 500);

        assertNotEquals(oldLeader, newLeader);
        assertTrue(cluster.node(newLeader).currentTerm() > oldTerm,
                "a new election must use a higher term");
    }

    @Test
    void minorityPartitionCannotElectLeader() {
        SimulatedCluster cluster = new SimulatedCluster(5, 3);
        int leader = cluster.awaitLeader(500);
        int minorityPeer = cluster.nodeIds().stream().filter(id -> id != leader).findFirst().orElseThrow();
        List<Integer> majority = cluster.nodeIds().stream()
                .filter(id -> id != leader && id != minorityPeer).toList();

        cluster.partition(new int[]{leader, minorityPeer},
                majority.stream().mapToInt(Integer::intValue).toArray());

        // The majority side recovers by electing among themselves...
        int newLeader = cluster.awaitLeaderAmong(majority, 1000);
        assertTrue(majority.contains(newLeader));

        // ...while no NEW leadership ever arises on the minority side (the old
        // leader keeps its stale title until it hears the new term, but the
        // minority peer can never win an election with 2 of 5 votes).
        for (int i = 0; i < 500; i++) {
            cluster.tick();
            assertNotEquals(Role.LEADER, cluster.node(minorityPeer).role());
        }

        // After healing, the deposed leader hears the higher term and steps down.
        cluster.heal();
        cluster.runUntil(() -> cluster.node(leader).role() == Role.FOLLOWER, 500);
    }

    /**
     * The §5.4.1 election restriction: a node that missed committed entries
     * must not become leader, no matter how high its term grows while isolated.
     */
    @Test
    void candidateWithStaleLogCannotWin() {
        SimulatedCluster cluster = new SimulatedCluster(3, 5);
        int leader = cluster.awaitLeader(500);
        int isolated = cluster.nodeIds().stream().filter(id -> id != leader).findFirst().orElseThrow();
        List<Integer> connected = cluster.nodeIds().stream().filter(id -> id != isolated).toList();

        cluster.partition(new int[]{isolated},
                connected.stream().mapToInt(Integer::intValue).toArray());

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            cluster.propose(KvStateMachine.set("key" + i, "value" + i));
            expected.add("SET key" + i + " value" + i);
        }
        cluster.runUntil(() -> connected.stream()
                .allMatch(id -> cluster.kv(id).appliedCommands().equals(expected)), 500);

        // While isolated, the node's term has been climbing with every failed
        // election. Heal — its next RequestVote carries a high term but a stale
        // log, so the others refuse it. It must never win.
        cluster.heal();
        boolean isolatedCaughtUp = false;
        for (int i = 0; i < 1500 && !isolatedCaughtUp; i++) {
            cluster.tick();
            assertNotEquals(Role.LEADER, cluster.node(isolated).role(),
                    "a candidate missing committed entries must never be elected");
            isolatedCaughtUp = cluster.kv(isolated).appliedCommands().equals(expected);
        }
        assertTrue(isolatedCaughtUp, "the stale node should instead be caught up by the new leader");
    }
}
