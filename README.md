# toy-raft

A from-first-principles implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
(leader election + log replication) in Java 21, replicating a small key-value
store, with a deterministic simulation test suite and a live multi-process TCP demo.

## Quick start

```
mvn test          # 27 deterministic tests, including a seeded chaos suite
mvn package       # builds the runnable jar
.\demo.ps1        # 3 real processes: elect, replicate, kill the leader, fail over
```

Manual demo:

```
java -jar target/toy-raft-0.1.0.jar node 1 1=localhost:5001,2=localhost:5002,3=localhost:5003
java -jar target/toy-raft-0.1.0.jar node 2 1=localhost:5001,2=localhost:5002,3=localhost:5003
java -jar target/toy-raft-0.1.0.jar node 3 1=localhost:5001,2=localhost:5002,3=localhost:5003

java -jar target/toy-raft-0.1.0.jar set localhost:5002 color teal   # any node; redirects to leader
java -jar target/toy-raft-0.1.0.jar get localhost:5003 color
java -jar target/toy-raft-0.1.0.jar status localhost:5001
java -jar target/toy-raft-0.1.0.jar bench localhost:5001 50000 128  # load test
```

## Design

The one decision everything else follows from: **the algorithm is a pure event
processor**. `RaftNode` has three inputs — `step(message)`, `tick()`,
`propose(command)` — and its only outputs are returned messages, synchronous
writes to a `Storage` interface, and `apply()` calls on a `StateMachine`
interface. No threads, no sockets, no clocks inside.

```
raft.core   the algorithm; zero dependencies, zero I/O
raft.kv     the replicated state machine (a string KV store)
raft.sim    a whole cluster in one deterministic event loop (what the tests drive)
raft.net    thin TCP/JSON adapter: real processes, a tick thread, a tiny client
```

Distributed algorithms are hard to test precisely because real networks and
clocks are nondeterministic. Inverting control means the test harness *is* the
network and the clock: the simulation runs a five-node cluster with seeded
random message delays, drops, duplications-by-retry, partitions, and crashes —
and the same seed replays the exact same execution, so a failing test is a
reproducible artifact rather than a flake. The TCP layer is deliberately just
plumbing; nothing about consensus lives there.

### Raft specifics worth defending

- **Persist before send.** Term, vote, and log entries hit `Storage` before
  any message revealing them is emitted. A node that granted a vote, crashed,
  and forgot could elect two leaders in one term. The simulation "crashes"
  nodes by discarding the `RaftNode` and rebooting from `Storage` — exactly
  what disk gives you.
- **Election restriction** Votes only go to candidates whose log is
  at least as up-to-date; this is the rule that lets Raft never ship entries
  *to* a leader.
- **Commit restriction** A leader only counts replicas for
  entries of its *own* term; prior-term entries commit transitively. Each new
  leader appends a no-op of its term so that happens promptly.
- **Conflict-index hints.** Rejected `AppendEntries` return the first index of
  the conflicting term, so divergent followers converge per-term, not per-entry.
- **Truncate only on conflict.** A duplicate or reordered `AppendEntries` never
  erases entries that arrived after it; followers truncate only on an actual
  term mismatch.

## Testing strategy

Safety invariants are asserted **continuously, on every simulated tick**, not
just at the end:

- *Election Safety* — at most one leader per term, ever (a term→leader map that
  never resets).
- *State Machine Safety* — a global index→command map; any two nodes applying
  different commands at the same index fail instantly, naming both.

On top of that:

- **Targeted scenario tests** — re-election on crash; minority partitions can't
  elect or commit; a stale-logged candidate can never win no matter how high
  its term climbed while isolated; split-brain writes on a deposed leader
  vanish everywhere after healing; a 2/2/1 split stalls rather than diverges;
  whole-cluster power loss recovers from persisted logs.
- **Unit tests** — message-level checks of the voting rules and a hand-driven
  figure-8 scenario (below).
- **Chaos suite** — 10 seeds × 150 rounds of random proposals, 15% drops,
  random delays, partitions, crashes, restarts; then the network turns friendly
  and we assert liveness (a marker write commits everywhere) and convergence
  (identical applied sequences, no duplicates).

### Do the tests have teeth? (mutation check)

A green suite proves nothing if it can't fail, so the algorithm was broken on
purpose, twice:

1. Removing the vote restriction → 6 of 10 chaos seeds fail with State
   Machine Safety violations, plus a unit test.
2. Removing the commit restriction (the figure-8 bug) → **the whole
   suite still passed.** Honest result: the leader no-op makes that schedule
   essentially unreachable by random exploration, because majority confirmation
   of an old entry almost always confirms the new leader's no-op in the same
   batch. Random testing is blind exactly there, so a deterministic unit test
   (`leaderMustNotCommitPriorTermEntriesByCountingReplicas`) drives the
   schedule by hand and kills the mutation.

## Load testing

Two layers, matching the architecture:

- **`ThroughputTest` (simulation)** — 5,000 proposals pipelined at 5/tick with
  no waiting for commits, asserting ordering at volume and that replication
  lag stays *bounded* (the whole run takes barely longer than the load
  itself). Deterministic, runs with the rest of the suite.
- **`bench` (live cluster)** — N workers over persistent connections firing
  unique writes, where an ack means committed-and-applied. Follows leader
  redirects, reports throughput and latency percentiles, then verifies a
  sample of keys.

Measured on a 3-node local cluster (one machine, in-memory storage):

| configuration | throughput | p50 | p99 |
|---|---|---|---|
| 1 worker (unloaded RTT) | ~1,500 ops/s | 0.4 ms | 6 ms |
| 128 workers, 50k ops | ~25,000 ops/s | 3.9 ms | 17 ms |

The first bench run told a story worth keeping: the naive transport
(connection per message) plus per-proposal broadcasting managed ~1,000 ops/s
with p50 = 54 ms — and adding workers made it *slower* than one worker alone.
Single-worker latency of 0.4 ms pinpointed queueing, not replication, as the
problem: every proposal re-broadcast the entire uncommitted window to every
follower, so leader work per op grew with pipeline depth. The fix is what real
implementations do — **single-flight appends**: at most one unacknowledged
AppendEntries per follower, proposals accumulate in the log, and each ack
carries everything accumulated since (group commit), with heartbeats doubling
as the retransmit timer. Send scheduling only, no semantic change; the chaos
suite passes untouched. Result: 25× throughput at 1/13th the p50.

## Deliberate simplifications

Chosen to keep the core readable; each is the standard next step in a real system:

- **No log compaction/snapshots** — the log grows forever; real systems
  snapshot the state machine and ship `InstallSnapshot` to lagging peers.
- **No membership changes** — the peer set is fixed at startup (joint
  consensus / single-server changes would go here).
- **Reads are not linearizable** — `get` serves the local state machine, so a
  deposed leader can briefly return stale data. The fix is ReadIndex:
  confirm leadership with a heartbeat round before serving.
- **No pre-vote** — a partitioned node's term climbs while isolated and
  forces one needless re-election on rejoin (visible in
  `candidateWithStaleLogCannotWin`, where it's handled safely).
- **In-memory `Storage`** — the interface and write ordering are
  correct; a production implementation would fsync a WAL behind the same
  interface. In the TCP demo this means killing a process loses its "disk"
  too — a harsher failure than designed for, which the cluster still survives
  for any minority.
- **Demo client is best-effort** — it retries through elections but doesn't
  deduplicate, so a retried write could apply twice. Real clients attach
  session/sequence numbers and the state machine filters duplicates.
