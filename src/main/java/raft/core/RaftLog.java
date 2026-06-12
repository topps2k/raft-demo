package raft.core;

import java.util.ArrayList;
import java.util.List;

/**
 * The in-memory view of the log, write-through to {@link Storage}. Indices are
 * 1-based as in the paper; index 0 is a virtual sentinel with term 0, which
 * makes the AppendEntries consistency check uniform (prevLogIndex=0 always
 * matches).
 */
public final class RaftLog {

    private final ArrayList<LogEntry> entries;
    private final Storage storage;

    public RaftLog(Storage storage) {
        this.storage = storage;
        this.entries = new ArrayList<>(storage.entries());
    }

    public long lastIndex() {
        return entries.size();
    }

    public long lastTerm() {
        return termAt(lastIndex());
    }

    /** Term of the entry at {@code index}, with the sentinel convention term(0) = 0. */
    public long termAt(long index) {
        if (index == 0) return 0;
        return get(index).term();
    }

    public LogEntry get(long index) {
        return entries.get((int) index - 1);
    }

    public boolean hasEntryAt(long index) {
        return index >= 1 && index <= lastIndex();
    }

    public void append(List<LogEntry> newEntries) {
        if (newEntries.isEmpty()) return;
        storage.append(newEntries);
        entries.addAll(newEntries);
    }

    /** Remove the entry at {@code index} and everything after it. */
    public void truncateFrom(long index) {
        storage.truncateFrom(index);
        while (entries.size() >= index) {
            entries.remove(entries.size() - 1);
        }
    }

    /** Entries from {@code from} (inclusive), at most {@code max} of them. */
    public List<LogEntry> slice(long from, int max) {
        long to = Math.min(lastIndex(), from + max - 1);
        if (from > to) return List.of();
        return List.copyOf(entries.subList((int) from - 1, (int) to));
    }

    /** First index of the term that {@code index} belongs to (for the conflict hint). */
    public long firstIndexOfTerm(long index) {
        long term = termAt(index);
        long i = index;
        while (i > 1 && termAt(i - 1) == term) {
            i--;
        }
        return i;
    }
}
