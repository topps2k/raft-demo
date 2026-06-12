package raft.core;

import java.util.Arrays;

/**
 * One slot in the replicated log. An entry with an empty command is a no-op:
 * a new leader appends one immediately so it can commit (and thereby commit
 * everything before it) without waiting for a client proposal — Raft can only
 * count replicas toward commitment for entries of the leader's own term (§5.4.2).
 */
public record LogEntry(long term, byte[] command) {

    public static final byte[] NOOP = new byte[0];

    public boolean isNoop() {
        return command.length == 0;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof LogEntry e && e.term == term && Arrays.equals(e.command, command);
    }

    @Override
    public int hashCode() {
        return (int) (term * 31) + Arrays.hashCode(command);
    }

    @Override
    public String toString() {
        return "LogEntry[term=" + term + ", command=" + new String(command) + "]";
    }
}
