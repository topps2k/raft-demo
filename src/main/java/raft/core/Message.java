package raft.core;

import java.util.List;

/**
 * The four RPCs of basic Raft, modeled as one-way messages rather than
 * request/response pairs. Treating replies as just another message keeps the
 * node a pure event processor and lets the test harness reorder, drop, and
 * duplicate them like a real network would.
 */
public sealed interface Message {

    int from();

    int to();

    /** The sender's term at the time of sending. Drives the "newer term wins" rule. */
    long term();

    record RequestVote(int from, int to, long term,
                       long lastLogIndex, long lastLogTerm) implements Message {}

    record RequestVoteReply(int from, int to, long term,
                            boolean granted) implements Message {}

    record AppendEntries(int from, int to, long term,
                         long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit) implements Message {}

    /**
     * On success, {@code matchIndex} is the index of the last entry the follower
     * now knows it shares with the leader. On failure, {@code conflictIndex} is a
     * hint for where the leader should back up to, so divergent followers catch
     * up in O(distinct terms) round trips instead of one index at a time.
     */
    record AppendEntriesReply(int from, int to, long term,
                              boolean success, long matchIndex,
                              long conflictIndex) implements Message {}
}
