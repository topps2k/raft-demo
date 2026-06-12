package raft.net;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import raft.core.InMemoryStorage;
import raft.core.Message;
import raft.core.RaftNode;
import raft.core.StateMachine;
import raft.kv.KvStateMachine;

/**
 * The thin adapter that turns the pure {@link RaftNode} into a real server:
 * a TCP listener, a 50ms tick clock, and fire-and-forget JSON messages to
 * peers. All algorithm state is confined to a single-threaded executor (the
 * "raft thread"); sockets are handled on virtual threads that only ever talk
 * to the core by submitting work to it.
 *
 * <p>Everything interesting about consensus lives in raft.core and is tested
 * there; this class is deliberately just plumbing. Storage is the in-memory
 * implementation, so killing a process loses its disk too — a harsher
 * failure mode than a real deployment, and the cluster still survives any
 * minority of such failures.
 */
public final class NodeServer {

    private static final int TICK_MS = 50;
    private static final int ELECTION_TIMEOUT_TICKS = 10; // 500ms-1s real time
    private static final int HEARTBEAT_TICKS = 2;         // 100ms
    private static final ObjectMapper JSON = new ObjectMapper();

    private final int id;
    private final Map<Integer, InetSocketAddress> peers; // includes self
    private final RaftNode node;
    private final KvStateMachine kv = new KvStateMachine();

    private final ScheduledExecutorService raftThread = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService io = Executors.newVirtualThreadPerTaskExecutor();

    /** Proposals awaiting commit, keyed by log index. */
    private record Pending(long term, CompletableFuture<Void> done) {}
    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();

    public NodeServer(int id, Map<Integer, InetSocketAddress> peers) {
        this.id = id;
        this.peers = peers;
        List<Integer> others = peers.keySet().stream().filter(p -> p != id).toList();
        StateMachine stateMachine = (index, term, command) -> {
            kv.apply(index, term, command);
            Pending p = pending.remove(index);
            if (p != null) {
                // Same index but a different term means our entry was overwritten
                // by another leader's — the write was lost, tell the client.
                if (p.term() == term) {
                    p.done().complete(null);
                } else {
                    p.done().completeExceptionally(new IOException("entry overwritten after leader change"));
                }
            }
        };
        this.node = new RaftNode(
                new RaftNode.Config(id, others, ELECTION_TIMEOUT_TICKS, HEARTBEAT_TICKS),
                new InMemoryStorage(), stateMachine, new Random());
    }

    public void run() throws IOException {
        raftThread.scheduleAtFixedRate(() -> send(node.tick()), TICK_MS, TICK_MS, TimeUnit.MILLISECONDS);
        try (ServerSocket server = new ServerSocket(peers.get(id).getPort())) {
            System.out.println("node " + id + " listening on " + peers.get(id));
            while (true) {
                Socket socket = server.accept();
                io.submit(() -> handleConnection(socket));
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(
                     new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {
            String line = in.readLine();
            if (line == null) return;
            JsonNode request = JSON.readTree(line);
            if (Wire.isPeerMessage(request)) {
                Message m = Wire.decode(request);
                raftThread.submit(() -> send(node.step(m)));
            } else {
                out.println(handleClient(request));
            }
        } catch (Exception e) {
            // A dropped connection is just a lossy network; Raft already copes.
        }
    }

    private String handleClient(JsonNode request) throws Exception {
        return switch (request.path("type").asText()) {
            case "propose" -> propose(request.get("command").asText());
            case "get" -> get(request.get("key").asText());
            case "status" -> status();
            default -> error("unknown request type", null);
        };
    }

    private String propose(String command) throws Exception {
        // Append on the raft thread; registering the pending entry there too
        // closes the race with an immediate commit.
        CompletableFuture<RaftNode.ProposeResult> appended = new CompletableFuture<>();
        CompletableFuture<Void> committed = new CompletableFuture<>();
        raftThread.submit(() -> {
            RaftNode.ProposeResult r = node.propose(command.getBytes(StandardCharsets.UTF_8));
            if (r.accepted()) {
                if (kv.lastAppliedIndex() >= r.index()) {
                    committed.complete(null); // single-node cluster commits inline
                } else {
                    pending.put(r.index(), new Pending(r.term(), committed));
                }
            }
            send(r.messages());
            appended.complete(r);
        });

        RaftNode.ProposeResult result = appended.get();
        if (!result.accepted()) {
            return error("not leader", result.leaderHint());
        }
        try {
            committed.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            pending.remove(result.index());
            return error("commit failed or timed out (leadership lost?)", null);
        }
        ObjectNode reply = JSON.createObjectNode();
        reply.put("ok", true);
        reply.put("index", result.index());
        return reply.toString();
    }

    private String get(String key) throws Exception {
        // Served from the local state machine: simple, but NOT linearizable —
        // a deposed leader or lagging follower can return stale data. The fix
        // (Raft §8) is ReadIndex: confirm leadership with a heartbeat round
        // and wait for the state machine to reach that commit index first.
        String value = raftThread.submit(() -> kv.get(key)).get();
        ObjectNode reply = JSON.createObjectNode();
        reply.put("ok", true);
        if (value == null) {
            reply.putNull("value");
        } else {
            reply.put("value", value);
        }
        return reply.toString();
    }

    private String status() throws Exception {
        return raftThread.submit(() -> {
            ObjectNode reply = JSON.createObjectNode();
            reply.put("ok", true);
            reply.put("id", id);
            reply.put("role", node.role().name());
            reply.put("term", node.currentTerm());
            reply.put("commitIndex", node.commitIndex());
            reply.put("leader", node.leaderHint() == null ? null : String.valueOf(node.leaderHint()));
            return reply.toString();
        }).get();
    }

    private String error(String message, Integer leaderHint) {
        ObjectNode reply = JSON.createObjectNode();
        reply.put("ok", false);
        reply.put("error", message);
        InetSocketAddress leader = leaderHint == null ? null : peers.get(leaderHint);
        if (leader != null) {
            reply.put("leader", leader.getHostString() + ":" + leader.getPort());
        }
        return reply.toString();
    }

    /** Fire-and-forget delivery; an unreachable peer is just a dropped message. */
    private void send(List<Message> messages) {
        for (Message m : messages) {
            io.submit(() -> {
                try (Socket socket = new Socket()) {
                    socket.connect(peers.get(m.to()), 200);
                    PrintWriter out = new PrintWriter(
                            new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
                    out.println(Wire.encode(m));
                } catch (IOException dropped) {
                    // peer down or partitioned; retries come from Raft itself
                }
            });
        }
    }
}
