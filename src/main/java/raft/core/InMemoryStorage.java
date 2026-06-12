package raft.core;

import java.util.ArrayList;
import java.util.List;

public final class InMemoryStorage implements Storage {

    private long term;
    private Integer votedFor;
    private final ArrayList<LogEntry> log = new ArrayList<>();

    @Override
    public long term() {
        return term;
    }

    @Override
    public Integer votedFor() {
        return votedFor;
    }

    @Override
    public void saveTermAndVote(long term, Integer votedFor) {
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override
    public List<LogEntry> entries() {
        return List.copyOf(log);
    }

    @Override
    public void append(List<LogEntry> entries) {
        log.addAll(entries);
    }

    @Override
    public void truncateFrom(long index) {
        if (index < 1) throw new IllegalArgumentException("index " + index);
        while (log.size() >= index) {
            log.remove(log.size() - 1);
        }
    }
}
