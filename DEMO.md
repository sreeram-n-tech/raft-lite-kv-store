# Fault-Tolerance & Durability Demo Transcript

This document presents the actually executed terminal transcript of the automated cluster test script `scripts/demo.sh`. It walks through cluster startup, log replication, automatic re-election after leader crash, writes during re-election, and state catch-up on node restart.

---

## Live Demo Run Transcript

```
=== Building kvnode and kvctl ===
```
> [!NOTE]
> **Step 1 (Build)**: Compiles the `kvnode` executable (consensus node server) and the `kvctl` executable (cluster query CLI client).

```
=== Starting 3-node Raft cluster ===
Waiting for cluster to initialize and elect leader...
=== Cluster Status ===
{"node_id":"node1","role":"Leader","term":1,"commit_index":0,"log_length":0,"leader":"localhost:8081"}

{"node_id":"node2","role":"Follower","term":1,"commit_index":0,"log_length":0,"leader":"localhost:8081"}

{"node_id":"node3","role":"Follower","term":1,"commit_index":0,"log_length":0,"leader":"localhost:8081"}
```
> [!NOTE]
> **Step 2 (Bootstrap & Election)**: Spawns three cluster nodes locally. Nodes trigger randomized timeouts and elect `node1` as the leader for Term 1. Followers correctly identify `node1` as the cluster leader.

```
=== Writing keys to the cluster ===
{"key":"name","status":"ok","value":"SreeRam"}

{"key":"role","status":"ok","value":"Engineer"}

{"key":"status","status":"ok","value":"Awesome"}
```
> [!NOTE]
> **Step 3 (Client Writes)**: Client sends data to the cluster. The leader receives the proposals, replicates them via gRPC `AppendEntries` to followers, commits them locally to its append-only WAL, applies them to the memory map, and returns `status: ok` to the client.

```
=== Verification of Writes (GET) ===
{"key":"name","value":"SreeRam"}

Redirecting to leader: http://localhost:8081/kv/role
{"key":"role","value":"Engineer"}

Redirecting to leader: http://localhost:8081/kv/status
{"key":"status","value":"Awesome"}
```
> [!NOTE]
> **Step 4 (Follower Redirection)**: GET request for `name` sent to `node1` (leader) returns the value immediately. GET requests for `role` and `status` sent to `node2` and `node3` (followers) return `307 Temporary Redirect` response pointing to the leader, which `kvctl` intercepts and automatically queries.

```
=== Killing Current Leader (node1 on port 8081) ===
Found leader: node1 on HTTP port 8081
Killing leader process (PID: 2948)...
SUCCESS: The process with PID 2948 has been terminated.
Leader killed successfully!
Waiting for a new leader to be elected (failover)...
New leader is: node2 on port 8082
```
> [!IMPORTANT]
> **Step 5 (Leader Failover)**: `kill_leader.sh` queries `/status` endpoints, finds the leader `node1` is running on port `8081` (PID `2948`), and terminates it. 
> The remaining followers detect the missing heartbeats. `node2` timeouts, increments the term to 2, requests votes, receives a majority vote, and becomes the new leader. Failover completes in under ~300ms.

```
=== Writing more data to New Leader ===
{"key":"project","status":"ok","value":"RaftLite"}
```
> [!NOTE]
> **Step 6 (Writes to New Leader)**: Client writes a new key (`project=RaftLite`) against the newly elected leader `node2` on port `8082`. The write succeeds, confirming cluster partition recovery.

```
=== Restarting the old leader node ===
Waiting for restarted node to catch up via WAL replay and replication...
```
> [!NOTE]
> **Step 7 (Durability Replay & Catch-Up)**: The crashed node (`node1`) is restarted. On boot, it replays its local `node1.wal` file to recover all committed entries (`name`, `role`, `status`) before accepting traffic. It then joins the cluster as a follower in Term 2.
> The leader (`node2`) detects `node1` is behind, and synchronizes the missing Term 2 entry (`project=RaftLite`) via gRPC.

```
=== Verification of caught up state on restarted node ===
Redirecting to leader: http://localhost:8082/kv/name
{"key":"name","value":"SreeRam"}

Redirecting to leader: http://localhost:8082/kv/project
{"key":"project","value":"RaftLite"}
```
> [!NOTE]
> **Step 8 (Consistency Assertions)**: Querying the restarted node on port `8081` confirms it has successfully recovered its original keys (`name`) and pulled the new post-failover key (`project`) from the leader.

```
=== Final Status of Restarted Node ===
{"node_id":"node1","role":"Follower","term":2,"commit_index":4,"log_length":4,"leader":"localhost:8082"}

=== Demo completed successfully! ===
Stopping all nodes...
```
> [!NOTE]
> **Step 9 (Clean Exit)**: Restarted `node1` is verified to be in `Follower` role in Term 2, matching the leader's commit index (4) and log length (4). All nodes are then stopped gracefully.
