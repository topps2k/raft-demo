package raft.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import raft.core.Message.AppendEntries;
import raft.core.Message.AppendEntriesReply;
import raft.core.Message.RequestVote;
import raft.core.Message.RequestVoteReply;

/**
 * A single Raft peer, implemented as a pure event processor: the only inputs
 * are {@link #tick()}, {@link #step(Message)} and {@link #propose(byte[])},
 * and the only outputs are returned messages, writes to {@link Storage}, and
 * applies to the {@link StateMachine}. There are no threads, sockets or clocks
 * in here — time is whatever calls tick(), the network is whoever delivers
 * messages. That inversion is what makes the algorithm deterministically
 * testable; transports (simulated or TCP) live outside.
 *
 * <p>Follows the Raft paper (Ongaro & Ousterhout 2014) sections 5.1–5.4, plus
 * two standard refinements: the conflict-index hint for fast log backtracking,
 * and a no-op entry appended on leadership change so prior-term entries commit
 * promptly (§5.4.2: a leader may only count replicas for its own term's entries).
 */
public final class RaftNode {

    /**
     * Timeouts are in abstract ticks. Election timeouts are randomized per
     * election into [electionTimeoutTicks, 2*electionTimeoutTicks) to break
     * split votes; the heartbeat interval must be much smaller so a healthy
     * leader always suppresses elections.
     */
    public record Config(int id, List<Integer> peers,
                         int electionTimeoutTicks, int heartbeatTicks) {}

    /**
     * Outcome of a proposal: {@code index} > 0 if this node was leader and
     * appended the command there; otherwise -1 with a best-effort
     * {@code leaderHint} for the client to redirect to.
     */
    public record ProposeResult(long index, long term, Integer leaderHint, List<Message> messages) {
        public boolean accepted() {
            return index > 0;
        }
    }

    /** Cap on entries per AppendEntries, so a far-behind follower catches up in batches. */
    private static final int MAX_BATCH = 100;

    private final Config config;
    private final Storage storage;
    private final StateMachine stateMachine;
    private final Random random;
    private final RaftLog log;
    private final int majority;

    private Role role = Role.FOLLOWER;
    private long currentTerm;
    private Integer votedFor;
    private Integer leaderId;

    private long commitIndex;
    private long lastApplied;

    private int electionElapsed;
    private int electionTimeout;
    private int heartbeatElapsed;

    // Candidate state.
    private final Set<Integer> votesGranted = new HashSet<>();

    // Leader state, reinitialized on every election win.
    private final Map<Integer, Long> nextIndex = new HashMap<>();
    private final Map<Integer, Long> matchIndex = new HashMap<>();

    /**
     * Flow control: at most one unacknowledged AppendEntries per follower.
     * New proposals between sends just accumulate in the log; each ack (or
     * heartbeat, which force-sends and doubles as the retransmit timer for
     * dropped messages) carries everything accumulated since — group commit.
     * Without this, every proposal re-broadcasts the whole uncommitted window
     * and the leader's work per op grows with the number in flight.
     */
    private final Map<Integer, Boolean> appendInFlight = new HashMap<>();

    public RaftNode(Config config, Storage storage, StateMachine stateMachine, Random random) {
        this.config = config;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.random = random;
        this.log = new RaftLog(storage);
        this.majority = (config.peers().size() + 1) / 2 + 1;
        this.currentTerm = storage.term();
        this.votedFor = storage.votedFor();
        resetElectionTimer();
    }

    // ------------------------------------------------------------------
    // Inputs
    // ------------------------------------------------------------------

    /** Advance logical time by one tick. May time out into an election or emit heartbeats. */
    public List<Message> tick() {
        List<Message> out = new ArrayList<>();
        if (role == Role.LEADER) {
            heartbeatElapsed++;
            if (heartbeatElapsed >= config.heartbeatTicks()) {
                heartbeatElapsed = 0;
                broadcastAppends(out, true);
            }
        } else {
            electionElapsed++;
            if (electionElapsed >= electionTimeout) {
                startElection(out);
            }
        }
        return out;
    }

    /** Process one inbound message. */
    public List<Message> step(Message m) {
        List<Message> out = new ArrayList<>();

        // Universal rule: a higher term instantly demotes us and adopts that term
        // (§5.1). votedFor resets because the vote belongs to the old term.
        if (m.term() > currentTerm) {
            becomeFollower(m.term(), null);
        }

        switch (m) {
            case RequestVote rv -> handleRequestVote(rv, out);
            case RequestVoteReply rvr -> handleVoteReply(rvr, out);
            case AppendEntries ae -> handleAppendEntries(ae, out);
            case AppendEntriesReply aer -> handleAppendReply(aer, out);
        }
        return out;
    }

    /**
     * Try to append a client command. Only the leader accepts; the entry is
     * replicated immediately rather than waiting for the next heartbeat.
     */
    public ProposeResult propose(byte[] command) {
        if (role != Role.LEADER) {
            return new ProposeResult(-1, currentTerm, leaderId, List.of());
        }
        log.append(List.of(new LogEntry(currentTerm, command)));
        List<Message> out = new ArrayList<>();
        maybeAdvanceCommit(); // a single-node cluster commits instantly
        broadcastAppends(out, false);
        return new ProposeResult(log.lastIndex(), currentTerm, config.id(), out);
    }

    // ------------------------------------------------------------------
    // Elections (§5.2)
    // ------------------------------------------------------------------

    private void startElection(List<Message> out) {
        role = Role.CANDIDATE;
        leaderId = null;
        currentTerm++;
        votedFor = config.id();
        storage.saveTermAndVote(currentTerm, votedFor);
        votesGranted.clear();
        votesGranted.add(config.id());
        resetElectionTimer();

        if (votesGranted.size() >= majority) { // single-node cluster
            becomeLeader(out);
            return;
        }
        for (int peer : config.peers()) {
            out.add(new RequestVote(config.id(), peer, currentTerm, log.lastIndex(), log.lastTerm()));
        }
    }

    private void handleRequestVote(RequestVote m, List<Message> out) {
        boolean grant = m.term() == currentTerm
                && (votedFor == null || votedFor == m.from())
                && candidateLogIsUpToDate(m);
        if (grant) {
            votedFor = m.from();
            storage.saveTermAndVote(currentTerm, votedFor);
            // Granting a vote means we believe in this candidate; don't immediately
            // time out and run against them.
            electionElapsed = 0;
        }
        out.add(new RequestVoteReply(config.id(), m.from(), currentTerm, grant));
    }

    /**
     * The election restriction (§5.4.1): only vote for a candidate whose log is
     * at least as up-to-date as ours, where up-to-date = higher last term, or
     * same last term and at least as long. This is the rule that guarantees any
     * elected leader already holds every committed entry.
     */
    private boolean candidateLogIsUpToDate(RequestVote m) {
        return m.lastLogTerm() > log.lastTerm()
                || (m.lastLogTerm() == log.lastTerm() && m.lastLogIndex() >= log.lastIndex());
    }

    private void handleVoteReply(RequestVoteReply m, List<Message> out) {
        if (role != Role.CANDIDATE || m.term() != currentTerm || !m.granted()) {
            return; // stale reply from an earlier election, or a rejection
        }
        votesGranted.add(m.from());
        if (votesGranted.size() >= majority) {
            becomeLeader(out);
        }
    }

    private void becomeLeader(List<Message> out) {
        role = Role.LEADER;
        leaderId = config.id();
        heartbeatElapsed = 0;
        for (int peer : config.peers()) {
            nextIndex.put(peer, log.lastIndex() + 1);
            matchIndex.put(peer, 0L);
            appendInFlight.put(peer, false);
        }
        // Commit a no-op of our own term right away: until an entry of the
        // current term is committed, nothing from prior terms may be declared
        // committed either (§5.4.2 / figure 8).
        log.append(List.of(new LogEntry(currentTerm, LogEntry.NOOP)));
        maybeAdvanceCommit();
        broadcastAppends(out, true);
    }

    private void becomeFollower(long term, Integer leader) {
        role = Role.FOLLOWER;
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            storage.saveTermAndVote(currentTerm, votedFor);
        }
        leaderId = leader;
        votesGranted.clear();
        resetElectionTimer();
    }

    private void resetElectionTimer() {
        electionElapsed = 0;
        int base = config.electionTimeoutTicks();
        electionTimeout = base + random.nextInt(base);
    }

    // ------------------------------------------------------------------
    // Log replication (§5.3)
    // ------------------------------------------------------------------

    /** Heartbeats force a send; proposal-triggered sends respect single-flight. */
    private void broadcastAppends(List<Message> out, boolean force) {
        for (int peer : config.peers()) {
            if (force || !appendInFlight.get(peer)) {
                sendAppend(peer, out);
            }
        }
    }

    private void sendAppend(int peer, List<Message> out) {
        out.add(buildAppend(peer));
        appendInFlight.put(peer, true);
    }

    private AppendEntries buildAppend(int peer) {
        long next = nextIndex.get(peer);
        long prevIndex = next - 1;
        List<LogEntry> entries = log.hasEntryAt(next) ? log.slice(next, MAX_BATCH) : List.of();
        return new AppendEntries(config.id(), peer, currentTerm,
                prevIndex, log.termAt(prevIndex), entries, commitIndex);
    }

    private void handleAppendEntries(AppendEntries m, List<Message> out) {
        if (m.term() < currentTerm) {
            out.add(new AppendEntriesReply(config.id(), m.from(), currentTerm, false, 0, 0));
            return;
        }
        // Equal term: there is a legitimate leader for this term. If we were a
        // candidate in it, we lost; either way, recognize the leader and reset
        // the election clock.
        becomeFollower(m.term(), m.from());

        // Consistency check: we must hold the entry the new ones splice onto.
        if (!matchesAt(m.prevLogIndex(), m.prevLogTerm())) {
            long conflictIndex = m.prevLogIndex() > log.lastIndex()
                    ? log.lastIndex() + 1                      // we're simply short
                    : log.firstIndexOfTerm(m.prevLogIndex());  // skip the whole bad term
            out.add(new AppendEntriesReply(config.id(), m.from(), currentTerm, false, 0, conflictIndex));
            return;
        }

        // Splice: skip entries we already have; on the first divergence, truncate
        // our suffix and take the leader's. Never truncate on a mere duplicate —
        // an old, reordered AppendEntries must not erase newer entries.
        long index = m.prevLogIndex();
        List<LogEntry> toAppend = new ArrayList<>();
        for (LogEntry entry : m.entries()) {
            index++;
            if (toAppend.isEmpty() && log.hasEntryAt(index)) {
                if (log.termAt(index) == entry.term()) continue;
                log.truncateFrom(index);
            }
            toAppend.add(entry);
        }
        log.append(toAppend);

        long lastNew = m.prevLogIndex() + m.entries().size();
        if (m.leaderCommit() > commitIndex) {
            commitIndex = Math.min(m.leaderCommit(), lastNew);
            applyCommitted();
        }
        out.add(new AppendEntriesReply(config.id(), m.from(), currentTerm, true, lastNew, 0));
    }

    private boolean matchesAt(long index, long term) {
        return index == 0 || (log.hasEntryAt(index) && log.termAt(index) == term);
    }

    private void handleAppendReply(AppendEntriesReply m, List<Message> out) {
        if (role != Role.LEADER || m.term() != currentTerm) {
            return;
        }
        appendInFlight.put(m.from(), false);
        if (m.success()) {
            // max() because replies can arrive out of order; matchIndex never regresses.
            long match = Math.max(matchIndex.get(m.from()), m.matchIndex());
            matchIndex.put(m.from(), match);
            nextIndex.put(m.from(), match + 1);
            maybeAdvanceCommit();
            if (log.lastIndex() >= nextIndex.get(m.from())) {
                sendAppend(m.from(), out); // next batch: everything accumulated since
            }
        } else {
            // Back up past the conflict and retry immediately.
            long next = Math.max(1, Math.min(m.conflictIndex(), nextIndex.get(m.from()) - 1));
            nextIndex.put(m.from(), next);
            sendAppend(m.from(), out);
        }
    }

    /**
     * Leader commit rule (§5.3, restricted by §5.4.2): an entry is committed
     * once a majority's matchIndex covers it AND it belongs to the current
     * term. Prior-term entries commit only transitively, via the Log Matching
     * property, when a current-term entry above them commits.
     */
    private void maybeAdvanceCommit() {
        for (long n = log.lastIndex(); n > commitIndex; n--) {
            if (log.termAt(n) != currentTerm) {
                break; // everything below is an older term; counting replicas proves nothing
            }
            int count = 1; // self
            for (long match : matchIndex.values()) {
                if (match >= n) count++;
            }
            if (count >= majority) {
                commitIndex = n;
                applyCommitted();
                return;
            }
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            if (!entry.isNoop()) {
                stateMachine.apply(lastApplied, entry.term(), entry.command());
            }
        }
    }

    // ------------------------------------------------------------------
    // Introspection (tests, demo server)
    // ------------------------------------------------------------------

    public int id() {
        return config.id();
    }

    public Role role() {
        return role;
    }

    public long currentTerm() {
        return currentTerm;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public Integer leaderHint() {
        return leaderId;
    }

    public long lastLogIndex() {
        return log.lastIndex();
    }

    public long termAt(long index) {
        return log.termAt(index);
    }
}
