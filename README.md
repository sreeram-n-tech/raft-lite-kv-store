# Mini Distributed Key-Value Store with Raft-Lite Replication

This project is a production-quality, portfolio-grade distributed key-value store in Go. It implements Raft-lite consensus (leader election and log replication), a custom append-only Write-Ahead Log (WAL) with `fsync` persistence, a gRPC transport layer for inter-node communication, and a client-facing HTTP/JSON API. It proves that a simplified consensus implementation can successfully survive a killed leader (automatic failover) and recover gracefully from arbitrary node crashes (durability via startup WAL replay) without any data loss for committed writes.

---

## Architecture Diagram

```mermaid
graph TD
    subgraph Client Space
        Client[kvctl CLI / curl]
    end

    subgraph Node 1 (Leader)
        HttpSrv1[HTTP Server] -->|Propose| RaftNode1[Raft Engine: Leader]
        RaftNode1 -->|Append & Apply| Storage1[(WAL & In-Memory Map)]
    end

    subgraph Node 2 (Follower)
        HttpSrv2[HTTP Server] -->|Redirect| Client
        RaftNode2[Raft Engine: Follower] -->|Append & Apply| Storage2[(WAL & In-Memory Map)]
    end

    subgraph Node 3 (Follower)
        HttpSrv3[HTTP Server] -->|Redirect| Client
        RaftNode3[Raft Engine: Follower] -->|Append & Apply| Storage3[(WAL & In-Memory Map)]
    end

    Client -- HTTP PUT/DELETE --> HttpSrv1
    Client -- HTTP GET ?stale=true --> HttpSrv2
    
    RaftNode1 ===|gRPC AppendEntries/RequestVote| RaftNode2
    RaftNode1 ===|gRPC AppendEntries/RequestVote| RaftNode3
    RaftNode2 ===|gRPC AppendEntries/RequestVote| RaftNode3
```

---

## How to Run

### Option A: Local Goroutines (Integration Testing)
The entire cluster lifecycle (elections, replication, failover, node stops, restarts, and catch-ups) is verified via standard Go integration tests without requiring Docker.
Run the tests (including the race detector) with:
```bash
go test -race -v ./...
```

### Option B: Docker Compose (3-Node Cluster)
Build and run the 3-node cluster inside Docker containers:
```bash
docker-compose up --build
```
This spawns:
- **Node 1**: HTTP `8081`, gRPC `50051`
- **Node 2**: HTTP `8082`, gRPC `50052`
- **Node 3**: HTTP `8083`, gRPC `50053`

---

## How to Use `kvctl`

`kvctl` is a lightweight command-line tool designed to query the cluster. It automatically intercepts HTTP `307 Temporary Redirect` codes from followers and retries queries against the elected leader.

### PUT a Key-Value Pair
```bash
./kvctl -addr localhost:8081 PUT name SreeRam
```
*Note:* If `localhost:8081` is a follower, it returns a redirect pointing to the leader. `kvctl` intercepts this redirect and retries the PUT request against the leader.

### GET a Key-Value Pair (Linearizable read via Leader)
```bash
./kvctl -addr localhost:8081 GET name
```
*Note:* If you contact a follower, it redirects you to the leader to ensure you get the most up-to-date committed value.

### GET a Key-Value Pair (Stale read via Follower)
```bash
./kvctl -addr localhost:8082 -stale GET name
```
*Note:* Appending the `-stale` option adds a `?stale=true` parameter to the request. The follower will bypass the leader check and return its current local state immediately.

### DELETE a Key
```bash
./kvctl -addr localhost:8081 DELETE name
```

### STATUS of a Node
```bash
./kvctl -addr localhost:8081 STATUS
```
Returns a JSON summary indicating the node's current role (`Leader`, `Follower`, or `Candidate`), the current term, the commit index, and the log length.

---

## Design Decisions & Trade-Offs

### 1. Static Membership
- **Decision**: The cluster size is fixed (usually 3 or 5 nodes) and hardcoded via command-line flags or static configurations.
- **Trade-off**: Simpler codebase by omitting joint consensus and cluster transition mechanics. Membership changes require a rolling restart of the cluster.

### 2. No Log Compaction or Snapshotting
- **Decision**: The WAL file grows continuously with every committed command.
- **Trade-off**: Omitting log compaction simplifies file recovery. However, nodes with long-running uptime will take longer to replay the WAL on startup as the WAL file grows.

### 3. Stale Reads (?stale=true)
- **Decision**: Followers can optionally serve GET requests directly if requested by the client.
- **Trade-off**: Offers higher read throughput and lower latency since followers don't need to consult the leader. However, the client sacrifices linearizability and may read a stale value if the follower is partitioned or behind the leader's commit index.

### 4. Leader Crash Mid-Append Outcome
- **Decision**: Uncommitted entries are stored **only in memory** (both on the leader and the followers). They are not written to the disk WAL.
- **Trade-off**: 
  - If a leader receives a write, appends it to its in-memory log, and crashes *before* replicating it to a majority, the entry is completely lost.
  - Upon restart, the crashed node's WAL does not contain the uncommitted entry. On re-election, a new leader is elected. The restarted node joins as a follower and receives heartbeats that cleanly overwrite any uncommitted in-memory logs.
  - This avoids having to handle disk log truncation, making WAL replay simple and robust.

---

## What is Implemented vs. Simplified vs. Real Raft

- **Consensus States (Implemented)**: Follower, Candidate, and Leader states are fully implemented. Nodes vote for themselves on candidate transitions and grant votes based on term and log correctness checks.
- **Elections (Implemented)**: Randomized timeouts (150ms–300ms) successfully resolve split-vote deadlocks.
- **Log Matching (Implemented)**: `AppendEntries` executes consistency checks on `prevLogIndex`/`prevLogTerm`, forcing followers to backtrack/truncate conflicting in-memory entries.
- **WAL Durability (Simplified)**: Only *committed* entries are written to disk. Real Raft writes all entries (committed and uncommitted) to disk and handles disk truncation on log mismatch.
- **Pre-Vote (Simplified)**: Nodes transition directly to Candidate state on timeout without a pre-vote phase.
- **Dynamic Membership (Simplified)**: No support for dynamic addition/removal of nodes.
- **Snapshotting (Simplified)**: No support for database snapshotting.
