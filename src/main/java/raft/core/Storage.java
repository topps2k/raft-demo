package raft.core;

import java.util.List;

/**
 * The durable state a node must survive a crash with: current term, who it
 * voted for in that term, and the log. Raft's safety argument depends on these
 * being persisted <em>before</em> any message that reveals them is sent — e.g.
 * a node that granted a vote, crashed, and forgot it could vote twice in the
 * same term and elect two leaders.
 *
 * <p>Implementations here are in-memory (a real one would fsync); the
 * simulation "crashes" a node by discarding the RaftNode while keeping its
 * Storage, which models exactly what disk persistence would preserve.
 */
public interface Storage {

    long term();

    /** null = has not voted in the current term. */
    Integer votedFor();

    void saveTermAndVote(long term, Integer votedFor);

    /** The full log, index 1 first. Loaded once at node construction. */
    List<LogEntry> entries();

    void append(List<LogEntry> entries);

    /** Discard the entry at 1-based {@code index} and everything after it. */
    void truncateFrom(long index);
}
