package raft.sim;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.function.BooleanSupplier;

import raft.core.InMemoryStorage;
import raft.core.Message;
import raft.core.RaftNode;
import raft.core.Role;
import raft.core.StateMachine;
import raft.kv.KvStateMachine;

/**
 * A whole Raft cluster running inside one deterministic event loop. Time is a
 * tick counter; the "network" is a priority queue of in-flight messages with
 * seeded random delays, drops, partitions, duplication-by-retry, and node
 * crashes. Same seed, same schedule, same result — a failing test reproduces
 * exactly.
 *
 * <p>Two safety invariants are checked continuously, so any violation fails
 * the instant it happens rather than as a downstream assertion mismatch:
 * <ul>
 *   <li><b>Election Safety</b> — at most one leader per term, ever.</li>
 *   <li><b>State Machine Safety</b> — if any node applies command X at log
 *       index i, every node that applies index i applies X.</li>
 * </ul>
 */
public final class SimulatedCluster {

    private record InFlight(long deliverAt, long seq, Message message) {}

    public static final int ELECTION_TIMEOUT_TICKS = 20;
    public static final int HEARTBEAT_TICKS = 4;

    private final List<Integer> ids = new ArrayList<>();
    private final Map<Integer, RaftNode> nodes = new HashMap<>();
    private final Map<Integer, InMemoryStorage> storages = new HashMap<>();
    private final Map<Integer, KvStateMachine> stateMachines = new HashMap<>();
    private final Set<Integer> crashed = new HashSet<>();
    private final Map<Integer, Integer> partitionGroup = new HashMap<>();

    private final PriorityQueue<InFlight> wire = new PriorityQueue<>(
            (a, b) -> a.deliverAt != b.deliverAt
                    ? Long.compare(a.deliverAt, b.deliverAt)
                    : Long.compare(a.seq, b.seq));
    private final Random rng;
    private long now;
    private long seq;

    private double dropRate = 0.0;
    private int minDelay = 1;
    private int maxDelay = 3;

    // Invariant trackers. Never reset, even across crashes and partitions.
    private final Map<Long, Integer> leaderByTerm = new HashMap<>();
    private final Map<Long, String> appliedByIndex = new HashMap<>();

    public SimulatedCluster(int nodeCount, long seed) {
        this.rng = new Random(seed);
        for (int id = 1; id <= nodeCount; id++) {
            ids.add(id);
        }
        for (int id : ids) {
            storages.put(id, new InMemoryStorage());
            partitionGroup.put(id, 0);
            boot(id);
        }
    }

    private void boot(int id) {
        List<Integer> peers = ids.stream().filter(p -> p != id).toList();
        KvStateMachine kv = new KvStateMachine();
        stateMachines.put(id, kv);
        RaftNode.Config config = new RaftNode.Config(
                id, peers, ELECTION_TIMEOUT_TICKS, HEARTBEAT_TICKS);
        nodes.put(id, new RaftNode(config, storages.get(id),
                checkedStateMachine(id, kv), new Random(rng.nextLong())));
    }

    /** Wraps a node's state machine to enforce State Machine Safety globally. */
    private StateMachine checkedStateMachine(int nodeId, KvStateMachine kv) {
        return (index, term, command) -> {
            String text = new String(command, StandardCharsets.UTF_8);
            String previous = appliedByIndex.putIfAbsent(index, text);
            if (previous != null && !previous.equals(text)) {
                throw new AssertionError("State Machine Safety violated at index " + index
                        + ": node " + nodeId + " applied \"" + text
                        + "\" but another node applied \"" + previous + "\"");
            }
            kv.apply(index, term, command);
        };
    }

    // ------------------------------------------------------------------
    // The event loop
    // ------------------------------------------------------------------

    /** Advance simulated time by one tick: deliver due messages, then tick every node. */
    public void tick() {
        now++;
        while (!wire.isEmpty() && wire.peek().deliverAt <= now) {
            Message m = wire.poll().message();
            // Connectivity is evaluated at delivery time: cutting a link also
            // kills packets already in flight on it, like a real partition.
            if (crashed.contains(m.to()) || crashed.contains(m.from()) || !connected(m.from(), m.to())) {
                continue;
            }
            send(nodes.get(m.to()).step(m));
        }
        for (int id : ids) {
            if (!crashed.contains(id)) {
                send(nodes.get(id).tick());
            }
        }
        checkElectionSafety();
    }

    public void run(int ticks) {
        for (int i = 0; i < ticks; i++) {
            tick();
        }
    }

    /** Tick until {@code condition} holds; fail the test if it never does. */
    public void runUntil(BooleanSupplier condition, int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            if (condition.getAsBoolean()) return;
            tick();
        }
        if (!condition.getAsBoolean()) {
            throw new AssertionError("condition not reached within " + maxTicks + " ticks (now=" + now + ")");
        }
    }

    private void send(List<Message> messages) {
        for (Message m : messages) {
            if (rng.nextDouble() < dropRate) continue;
            long delay = minDelay + rng.nextInt(maxDelay - minDelay + 1);
            wire.add(new InFlight(now + delay, seq++, m));
        }
    }

    private void checkElectionSafety() {
        for (int id : ids) {
            if (crashed.contains(id)) continue;
            RaftNode node = nodes.get(id);
            if (node.role() == Role.LEADER) {
                Integer existing = leaderByTerm.putIfAbsent(node.currentTerm(), id);
                if (existing != null && existing != id) {
                    throw new AssertionError("Election Safety violated: nodes " + existing
                            + " and " + id + " are both leader in term " + node.currentTerm());
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Fault injection
    // ------------------------------------------------------------------

    /** Stop a node dead. Its volatile state is lost; its Storage (the "disk") survives. */
    public void crash(int id) {
        crashed.add(id);
    }

    /** Bring a crashed node back from its persisted term, vote, and log. */
    public void restart(int id) {
        if (!crashed.remove(id)) {
            throw new IllegalStateException("node " + id + " is not crashed");
        }
        boot(id);
    }

    /** Split the cluster into groups; links between different groups go dark. */
    public void partition(int[]... groups) {
        for (int g = 0; g < groups.length; g++) {
            for (int id : groups[g]) {
                partitionGroup.put(id, g);
            }
        }
    }

    public void heal() {
        for (int id : ids) {
            partitionGroup.put(id, 0);
        }
    }

    public void setDropRate(double dropRate) {
        this.dropRate = dropRate;
    }

    public void setDelayRange(int minTicks, int maxTicks) {
        this.minDelay = minTicks;
        this.maxDelay = maxTicks;
    }

    private boolean connected(int a, int b) {
        return partitionGroup.get(a).equals(partitionGroup.get(b));
    }

    // ------------------------------------------------------------------
    // Driving and observing
    // ------------------------------------------------------------------

    /** Propose to the current leader; the test must ensure one exists. */
    public long propose(byte[] command) {
        Integer leader = leaderId();
        if (leader == null) {
            throw new IllegalStateException("no leader to propose to");
        }
        RaftNode.ProposeResult result = nodes.get(leader).propose(command);
        send(result.messages());
        return result.index();
    }

    public RaftNode.ProposeResult proposeTo(int id, byte[] command) {
        RaftNode.ProposeResult result = nodes.get(id).propose(command);
        send(result.messages());
        return result;
    }

    /** The live leader with the highest term, or null. May be a stale leader mid-partition. */
    public Integer leaderId() {
        return leaderAmong(ids);
    }

    public Integer leaderAmong(Collection<Integer> candidates) {
        Integer best = null;
        for (int id : candidates) {
            if (crashed.contains(id)) continue;
            RaftNode node = nodes.get(id);
            if (node.role() == Role.LEADER
                    && (best == null || node.currentTerm() > nodes.get(best).currentTerm())) {
                best = id;
            }
        }
        return best;
    }

    public int awaitLeader(int maxTicks) {
        runUntil(() -> leaderId() != null, maxTicks);
        return leaderId();
    }

    public int awaitLeaderAmong(Collection<Integer> candidates, int maxTicks) {
        runUntil(() -> leaderAmong(candidates) != null, maxTicks);
        return leaderAmong(candidates);
    }

    public RaftNode node(int id) {
        return nodes.get(id);
    }

    public KvStateMachine kv(int id) {
        return stateMachines.get(id);
    }

    public List<Integer> nodeIds() {
        return List.copyOf(ids);
    }

    public List<Integer> aliveNodeIds() {
        return ids.stream().filter(id -> !crashed.contains(id)).toList();
    }

    public long now() {
        return now;
    }
}
