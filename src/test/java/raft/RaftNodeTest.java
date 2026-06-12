package raft;

import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import raft.core.InMemoryStorage;
import raft.core.LogEntry;
import raft.core.Message;
import raft.core.RaftNode;
import raft.kv.KvStateMachine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the voting rules, driven by injecting messages directly into
 * a single node — no cluster, no network, no time.
 */
class RaftNodeTest {

    private static RaftNode node(InMemoryStorage storage) {
        return new RaftNode(new RaftNode.Config(1, List.of(2, 3), 20, 4),
                storage, new KvStateMachine(), new Random(42));
    }

    private static Message.RequestVoteReply voteReply(List<Message> out) {
        assertEquals(1, out.size());
        return assertInstanceOf(Message.RequestVoteReply.class, out.get(0));
    }

    @Test
    void grantsAtMostOneVotePerTerm() {
        RaftNode n = node(new InMemoryStorage());

        var first = voteReply(n.step(new Message.RequestVote(2, 1, 5, 0, 0)));
        assertTrue(first.granted());

        // Same candidate retransmits (its reply was lost): granted again, idempotently.
        var retry = voteReply(n.step(new Message.RequestVote(2, 1, 5, 0, 0)));
        assertTrue(retry.granted());

        // A different candidate in the same term must be refused.
        var rival = voteReply(n.step(new Message.RequestVote(3, 1, 5, 0, 0)));
        assertFalse(rival.granted());
    }

    @Test
    void voteSurvivesCrashAndRestart() {
        InMemoryStorage storage = new InMemoryStorage();
        RaftNode before = node(storage);
        assertTrue(voteReply(before.step(new Message.RequestVote(2, 1, 5, 0, 0))).granted());

        // Crash. A new instance over the same storage must remember the vote,
        // or two leaders could be elected in term 5.
        RaftNode after = node(storage);
        var rival = voteReply(after.step(new Message.RequestVote(3, 1, 5, 0, 0)));
        assertFalse(rival.granted());
        assertEquals(5, rival.term());
    }

    @Test
    void rejectsVoteRequestFromStaleTerm() {
        InMemoryStorage storage = new InMemoryStorage();
        storage.saveTermAndVote(7, null);
        RaftNode n = node(storage);

        var reply = voteReply(n.step(new Message.RequestVote(2, 1, 3, 99, 3)));
        assertFalse(reply.granted());
        assertEquals(7, reply.term(), "the reply carries our term so the candidate can update itself");
    }

    /**
     * The §5.4.2 commit restriction (the paper's figure 8): a leader must not
     * declare a prior-term entry committed just because a majority holds it —
     * a higher-term candidate could still be elected without it and overwrite
     * it. Prior-term entries commit only transitively, once an entry of the
     * leader's own term is committed above them.
     *
     * <p>Driven by hand because the dangerous schedule is essentially
     * unreachable through the simulator: the no-op a leader appends on
     * election means majority confirmation of old entries almost always
     * confirms a current-term entry too.
     */
    @Test
    void leaderMustNotCommitPriorTermEntriesByCountingReplicas() {
        // Node 1 crashed with one uncommitted entry from term 1 in its log.
        InMemoryStorage storage = new InMemoryStorage();
        storage.saveTermAndVote(1, null);
        storage.append(List.of(new LogEntry(1, "SET a 1".getBytes())));
        KvStateMachine kv = new KvStateMachine();
        RaftNode n = new RaftNode(new RaftNode.Config(1, List.of(2, 3), 20, 4),
                storage, kv, new Random(42));

        // It times out, runs for term 2, and wins with node 2's vote.
        List<Message> requests = List.of();
        while (requests.isEmpty()) {
            requests = n.tick();
        }
        assertInstanceOf(Message.RequestVote.class, requests.get(0));
        n.step(new Message.RequestVoteReply(2, 1, n.currentTerm(), true));
        assertEquals(2, n.currentTerm());
        assertEquals(2, n.lastLogIndex(), "the new leader appends a term-2 no-op at index 2");

        // Node 2 confirms it holds index 1 — the term-1 entry — on a majority.
        // Committing it now would be the figure-8 bug.
        n.step(new Message.AppendEntriesReply(2, 1, 2, true, 1, 0));
        assertEquals(0, n.commitIndex(),
                "a majority on a prior-term entry alone must not commit it");

        // Once node 2 also holds the term-2 no-op, both entries commit together.
        n.step(new Message.AppendEntriesReply(2, 1, 2, true, 2, 0));
        assertEquals(2, n.commitIndex());
        assertEquals("1", kv.get("a"), "the term-1 entry commits transitively");
    }

    /** The §5.4.1 up-to-date check, all four corners. */
    @Test
    void grantsVoteOnlyToCandidatesWithUpToDateLogs() {
        // Our log: two entries from term 1, so lastIndex=2, lastTerm=1.
        InMemoryStorage storage = new InMemoryStorage();
        storage.saveTermAndVote(1, null);
        storage.append(List.of(new LogEntry(1, "SET a 1".getBytes()),
                new LogEntry(1, "SET a 2".getBytes())));

        // Shorter log, same last term: refuse (it may be missing committed entries).
        assertFalse(voteReply(node(storage).step(
                new Message.RequestVote(2, 1, 2, 1, 1))).granted());

        // Same length, same last term: grant.
        assertTrue(voteReply(node(storage).step(
                new Message.RequestVote(2, 1, 2, 2, 1))).granted());

        // Shorter log but higher last term: grant (last term dominates).
        assertTrue(voteReply(node(storage).step(
                new Message.RequestVote(2, 1, 3, 1, 2))).granted());

        // Longer log but lower last term: refuse.
        InMemoryStorage higher = new InMemoryStorage();
        higher.saveTermAndVote(2, null);
        higher.append(List.of(new LogEntry(2, "SET b 1".getBytes())));
        assertFalse(voteReply(node(higher).step(
                new Message.RequestVote(2, 1, 3, 5, 1))).granted());
    }
}
