package raft.net;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A minimal client: one JSON request per connection, following "not leader"
 * redirects and retrying briefly while an election is in progress.
 */
final class Client {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_ATTEMPTS = 15;

    private Client() {}

    static JsonNode request(String address, ObjectNode body) throws Exception {
        String target = address;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            JsonNode reply;
            try {
                reply = exchange(target, body);
            } catch (IOException e) {
                Thread.sleep(300); // node down? give the cluster a moment, retry
                continue;
            }
            if (reply.path("ok").asBoolean()) {
                return reply;
            }
            if (reply.hasNonNull("leader")) {
                target = reply.get("leader").asText(); // redirected to the leader
            } else {
                Thread.sleep(300); // no leader known yet (election in progress)
            }
        }
        throw new IOException("no successful response after " + MAX_ATTEMPTS + " attempts");
    }

    private static JsonNode exchange(String address, ObjectNode body) throws IOException {
        String[] hostPort = address.split(":");
        try (Socket socket = new Socket()) {
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])), 1000);
            socket.setSoTimeout(10_000);
            PrintWriter out = new PrintWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            out.println(body.toString());
            String line = in.readLine();
            if (line == null) throw new IOException("connection closed by " + address);
            return JSON.readTree(line);
        }
    }
}
