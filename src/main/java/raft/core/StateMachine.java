package raft.core;

/**
 * What replication is for: committed log entries are applied, in log order,
 * exactly once per node lifetime, to this. Raft guarantees every node's apply
 * stream is a prefix of the same sequence (State Machine Safety).
 */
public interface StateMachine {

    /** Called with strictly increasing {@code index}. No-op entries are not delivered. */
    void apply(long index, long term, byte[] command);
}
