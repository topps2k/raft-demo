package raft.net;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import raft.core.LogEntry;
import raft.core.Message;

/**
 * JSON codec for peer-to-peer Raft messages, one object per line. Hand-mapped
 * rather than annotation-driven so {@code raft.core} stays free of any
 * dependency. Commands are carried as UTF-8 text — fine for the KV demo's
 * "SET k v" commands and it makes the wire traffic readable while watching
 * the cluster talk.
 */
final class Wire {

    private static final ObjectMapper JSON = new ObjectMapper();

    private Wire() {}

    static String encode(Message m) {
        ObjectNode n = JSON.createObjectNode();
        n.put("type", m.getClass().getSimpleName());
        n.put("from", m.from());
        n.put("to", m.to());
        n.put("term", m.term());
        switch (m) {
            case Message.RequestVote rv -> {
                n.put("lastLogIndex", rv.lastLogIndex());
                n.put("lastLogTerm", rv.lastLogTerm());
            }
            case Message.RequestVoteReply rvr -> n.put("granted", rvr.granted());
            case Message.AppendEntries ae -> {
                n.put("prevLogIndex", ae.prevLogIndex());
                n.put("prevLogTerm", ae.prevLogTerm());
                n.put("leaderCommit", ae.leaderCommit());
                ArrayNode entries = n.putArray("entries");
                for (LogEntry e : ae.entries()) {
                    ObjectNode en = entries.addObject();
                    en.put("term", e.term());
                    en.put("command", new String(e.command()));
                }
            }
            case Message.AppendEntriesReply aer -> {
                n.put("success", aer.success());
                n.put("matchIndex", aer.matchIndex());
                n.put("conflictIndex", aer.conflictIndex());
            }
        }
        return n.toString();
    }

    static Message decode(JsonNode n) {
        int from = n.get("from").asInt();
        int to = n.get("to").asInt();
        long term = n.get("term").asLong();
        return switch (n.get("type").asText()) {
            case "RequestVote" -> new Message.RequestVote(from, to, term,
                    n.get("lastLogIndex").asLong(), n.get("lastLogTerm").asLong());
            case "RequestVoteReply" -> new Message.RequestVoteReply(from, to, term,
                    n.get("granted").asBoolean());
            case "AppendEntries" -> {
                List<LogEntry> entries = new ArrayList<>();
                for (JsonNode en : n.get("entries")) {
                    entries.add(new LogEntry(en.get("term").asLong(),
                            en.get("command").asText().getBytes()));
                }
                yield new Message.AppendEntries(from, to, term,
                        n.get("prevLogIndex").asLong(), n.get("prevLogTerm").asLong(),
                        entries, n.get("leaderCommit").asLong());
            }
            case "AppendEntriesReply" -> new Message.AppendEntriesReply(from, to, term,
                    n.get("success").asBoolean(), n.get("matchIndex").asLong(),
                    n.get("conflictIndex").asLong());
            default -> throw new IllegalArgumentException("unknown message: " + n);
        };
    }

    static boolean isPeerMessage(JsonNode n) {
        String type = n.path("type").asText();
        return type.equals("RequestVote") || type.equals("RequestVoteReply")
                || type.equals("AppendEntries") || type.equals("AppendEntriesReply");
    }
}
