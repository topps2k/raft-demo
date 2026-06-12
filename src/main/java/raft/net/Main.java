package raft.net;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Entry point for the live demo.
 *
 * <pre>
 *   java -jar toy-raft.jar node 1 1=localhost:5001,2=localhost:5002,3=localhost:5003
 *   java -jar toy-raft.jar set localhost:5001 color teal
 *   java -jar toy-raft.jar get localhost:5002 color
 *   java -jar toy-raft.jar del localhost:5003 color
 *   java -jar toy-raft.jar status localhost:5001
 * </pre>
 */
public final class Main {

    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        switch (args[0]) {
            case "node" -> {
                int id = Integer.parseInt(args[1]);
                Map<Integer, InetSocketAddress> peers = parsePeers(args[2]);
                if (!peers.containsKey(id)) {
                    throw new IllegalArgumentException("node id " + id + " not in peer list");
                }
                new NodeServer(id, peers).run();
            }
            case "set" -> {
                ObjectNode body = JSON.createObjectNode();
                body.put("type", "propose");
                body.put("command", "SET " + args[2] + " " + args[3]);
                System.out.println(Client.request(args[1], body));
            }
            case "del" -> {
                ObjectNode body = JSON.createObjectNode();
                body.put("type", "propose");
                body.put("command", "DEL " + args[2]);
                System.out.println(Client.request(args[1], body));
            }
            case "get" -> {
                ObjectNode body = JSON.createObjectNode();
                body.put("type", "get");
                body.put("key", args[2]);
                System.out.println(Client.request(args[1], body));
            }
            case "status" -> {
                ObjectNode body = JSON.createObjectNode();
                body.put("type", "status");
                System.out.println(Client.request(args[1], body));
            }
            case "bench" -> Bench.run(args[1],
                    args.length > 2 ? Integer.parseInt(args[2]) : 2000,
                    args.length > 3 ? Integer.parseInt(args[3]) : 32);
            default -> usage();
        }
    }

    private static Map<Integer, InetSocketAddress> parsePeers(String spec) {
        Map<Integer, InetSocketAddress> peers = new HashMap<>();
        for (String part : spec.split(",")) {
            String[] idAddr = part.split("=");
            String[] hostPort = idAddr[1].split(":");
            peers.put(Integer.parseInt(idAddr[0]),
                    new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])));
        }
        return peers;
    }

    private static void usage() {
        System.out.println("""
                usage:
                  node <id> <id=host:port,...>   run a cluster node
                  set <host:port> <key> <value>  replicate a write
                  get <host:port> <key>          read from a node's state machine
                  del <host:port> <key>          replicate a delete
                  status <host:port>             role/term/commitIndex of a node
                  bench <host:port> [ops] [workers]  load test (default 2000 ops, 32 workers)""");
    }

    private Main() {}
}
