# Build Brief: Mini Distributed Key-Value Store with Raft-Lite Replication

## Goal

Build a production-quality, portfolio-grade distributed key-value store in **Go**. It must support a networked GET/PUT/DELETE API, leader-follower replication with heartbeats, a write-ahead log (WAL) for durability, and a simplified Raft consensus protocol (leader election + log replication). The end result must be able to **survive a killed leader (automatic failover) and a node restart (durability via WAL replay)**, with a scripted demo that proves both on camera.

Prioritize correctness and a clean, defensible architecture over feature breadth. This is meant to be read and discussed in a technical interview, so every design decision should be intentional and explainable.

---

## Tech Stack

- **Language**: Go (latest stable)
- **Transport**: gRPC + Protocol Buffers for inter-node RPCs (RequestVote, AppendEntries) and the client API. Use plain HTTP/JSON for the client-facing API if that's simpler to wire up — your call, but be consistent.
- **Persistence**: Custom append-only WAL file per node (no external DB). fsync on every write before acknowledging.
- **Orchestration for local multi-node testing**: docker-compose (3 or 5 node cluster), plus a way to run nodes as plain local processes for faster iteration.
- **No external consensus libraries** (no hashicorp/raft, no etcd/raft) — the point of this project is to implement the algorithm, not import it.

---

## Project Structure (suggested)

```
/cmd/kvnode        — main entrypoint for a single node
/cmd/kvctl         — CLI client for GET/PUT/DELETE against the cluster
/internal/raft     — election + log replication state machine
/internal/storage  — WAL + in-memory state machine + recovery/replay
/internal/transport — gRPC server/client definitions (proto + generated code)
/internal/server   — glue: wires raft + storage + transport together
/scripts           — chaos/demo scripts (kill leader, restart node, etc.)
/docker-compose.yml
/README.md
/DEMO.md
```

---

## Core Architecture Requirements

### 1. Storage layer
- In-memory map as the state machine.
- Append-only WAL on disk: every committed write is appended as a log entry `(term, index, command)` before being applied to the map.
- On startup, replay the WAL from disk to rebuild both the log and the map state before accepting traffic.
- fsync after every append — no silent data loss on crash.

### 2. Cluster membership
- Static membership list (3–5 nodes), passed via config file or flags — no dynamic membership changes needed.

### 3. Leader election (Raft-lite)
- Each node is in one of three states: **Follower**, **Candidate**, **Leader**.
- Randomized election timeout (e.g. 150–300ms) to avoid split votes.
- On timeout, a follower becomes a candidate, increments its term, votes for itself, and sends `RequestVote` RPCs to all peers.
- `RequestVote` includes candidate's term, id, and last log index/term — voters reject stale or behind candidates.
- A candidate becomes leader on receiving votes from a majority.
- On split vote, nodes time out and retry with a new randomized backoff.

### 4. Heartbeats + log replication
- Leader sends periodic `AppendEntries` heartbeats (even when empty) to all followers (e.g. every 50ms) to maintain authority and reset their election timers.
- Client writes go to the leader, which appends to its own log, then replicates via `AppendEntries` to followers.
- `AppendEntries` must include `prevLogIndex`/`prevLogTerm` for the consistency check; followers reject and force a backtrack/truncate on mismatch.
- An entry is **committed** once replicated to a majority of nodes; only then is it applied to the state machine and acknowledged to the client.
- Followers learn the leader's commit index via `leaderCommit` in `AppendEntries` and apply entries up to that index locally.

### 5. Client API
- Simple GET/PUT/DELETE, exposed over HTTP or gRPC.
- Writes sent to a follower must be rejected with a redirect/error pointing at the current known leader (don't silently proxy unless you want to — note the tradeoff either way in the README).
- Reads served by the leader by default. Optionally support a `--stale-read` flag for follower reads, and document the consistency tradeoff if you add it.

### 6. Explicit non-goals (so the agent doesn't over-scope)
- No dynamic cluster membership changes (joint consensus).
- No log compaction / snapshotting required (nice-to-have stretch only if time allows).
- No pre-vote optimization.
- No multi-raft / sharding.
State this explicitly in the README under "What's simplified vs. real Raft."

---

## Fault Tolerance Demo (this is the deliverable that matters most)

Build a `/scripts` folder with:
1. **`kill_leader.sh`** — identifies the current leader (via a `/status` endpoint each node exposes), kills that process, and leaves the rest of the cluster running.
2. **`demo.sh`** — orchestrates a full scripted run:
   - Start a 3 or 5 node cluster.
   - Client writes a few keys.
   - Kill the leader.
   - Poll `/status` on the remaining nodes and print the new leader once elected — should happen within ~1 second.
   - Client writes again, against the new leader, and confirms success.
   - Restart the killed node.
   - Confirm via WAL replay that the restarted node catches up and its state matches the cluster's.
3. Every node should expose a `/status` endpoint returning: node id, current role, current term, commit index, and log length — this is what makes the failover visible/provable instead of a black box.

---

## Observability

- Structured logging (e.g. with `slog`) on every role transition: `follower → candidate`, `candidate → leader`, `leader → follower` (on discovering a higher term), including term number and node id.
- Log every election outcome (won/lost/split) and every committed log entry.

---

## Testing

- **Unit tests**: WAL append/replay correctness, log matching/truncation logic, vote-granting rules.
- **Integration tests**: spin up N in-process nodes (goroutines + local ports), and verify:
  - Exactly one leader is elected per term.
  - A write is visible on a majority of nodes once acknowledged.
  - Killing the leader process/goroutine triggers election of a new leader.
  - A restarted node's state matches the cluster after WAL replay.
- These tests should be runnable with a single `go test ./...` and should not require Docker.

---

## Documentation Deliverables

1. **README.md** — must include:
   - Architecture diagram (Mermaid is fine).
   - How to run locally (docker-compose, 3–5 nodes) and how to run the demo script.
   - Design decisions and tradeoffs (e.g. why static membership, why no snapshotting yet).
   - Explicit "What's implemented vs. simplified vs. real Raft" section.
2. **DEMO.md** — step-by-step script matching `demo.sh`, written so a recruiter or interviewer could read it (or watch a recording of it) and immediately understand what's being proven: leader election, log replication, automatic failover, and durability across restarts.

---

## Suggested Build Order (phase the work, don't build everything in one shot)

1. Single-node KV store with WAL persistence and replay on restart.
2. Static leader-follower replication with heartbeats (no election yet — leader is hardcoded).
3. Add Raft-lite leader election (RequestVote, terms, randomized timeouts).
4. Add real log replication with consistency checks (AppendEntries with prevLogIndex/Term, commit index).
5. Build the fault-injection/demo tooling and `/status` endpoint.
6. Write unit + integration tests.
7. Write README.md and DEMO.md.

## Acceptance Criteria

- [ ] Cluster of 3–5 nodes elects exactly one leader on startup.
- [ ] Client writes are replicated to a majority before being acknowledged.
- [ ] Killing the leader process results in a new leader being elected automatically, visible via `/status`.
- [ ] A node killed and restarted recovers its state correctly via WAL replay, with no data loss for previously committed entries.
- [ ] `go test ./...` passes, covering WAL, election, and replication logic.
- [ ] README and DEMO docs are complete and a stranger could run the demo from a clean clone.

---

## Process Instructions for the Agent

- Work in phases per the build order above — commit working code at each phase rather than attempting everything at once.
- Write tests alongside each phase, not all at the end.
- Use Planning Mode: produce the Task List and Implementation Plan first; pause for review before writing code.
- If something here is genuinely ambiguous (e.g. exact heartbeat interval, exact election timeout range), make a reasonable, idiomatic Raft-paper-consistent choice and document it in the README rather than blocking on it.
- Do not silently drop the fault-tolerance demo or the WAL-replay test — these are the two things this project exists to prove.
