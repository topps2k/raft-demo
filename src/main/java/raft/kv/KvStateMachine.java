package raft.kv;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import raft.core.StateMachine;

/**
 * A small key-value store as the replicated state machine. Commands are
 * plain text — {@code SET key value} or {@code DEL key} — which keeps the
 * wire format and test assertions human-readable.
 *
 * <p>Also records every applied command in order: the simulation uses that
 * trail to assert State Machine Safety (all nodes apply the same sequence).
 */
public final class KvStateMachine implements StateMachine {

    private final Map<String, String> data = new HashMap<>();
    private final List<String> appliedCommands = new ArrayList<>();
    private long lastAppliedIndex;

    @Override
    public void apply(long index, long term, byte[] command) {
        if (index <= lastAppliedIndex) {
            throw new IllegalStateException(
                    "apply went backwards: " + index + " after " + lastAppliedIndex);
        }
        lastAppliedIndex = index;
        String text = new String(command, StandardCharsets.UTF_8);
        appliedCommands.add(text);

        String[] parts = text.split(" ", 3);
        switch (parts[0]) {
            case "SET" -> data.put(parts[1], parts[2]);
            case "DEL" -> data.remove(parts[1]);
            default -> throw new IllegalArgumentException("unknown command: " + text);
        }
    }

    public String get(String key) {
        return data.get(key);
    }

    public Map<String, String> snapshot() {
        return Map.copyOf(data);
    }

    public List<String> appliedCommands() {
        return List.copyOf(appliedCommands);
    }

    /** Cheaper than {@code appliedCommands().size()} when polled in a loop. */
    public int appliedCount() {
        return appliedCommands.size();
    }

    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    public static byte[] set(String key, String value) {
        return ("SET " + key + " " + value).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] del(String key) {
        return ("DEL " + key).getBytes(StandardCharsets.UTF_8);
    }
}
