import time
import random
import threading
import grpc
import sys
import json
from typing import Dict, Tuple, Optional, List

from internal.storage.storage import Storage, LogEntry
from internal.transport import raft_pb2, raft_pb2_grpc

class JsonLogger:
    def __init__(self, name: str, extra: dict = None):
        self.name = name
        self.extra = extra or {}

    def with_fields(self, **kwargs) -> 'JsonLogger':
        new_extra = {**self.extra, **kwargs}
        return JsonLogger(self.name, new_extra)

    def _log(self, level: str, msg: str, **kwargs):
        timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        record = {
            "time": timestamp,
            "level": level,
            "msg": msg,
            **self.extra,
            **kwargs
        }
        sys.stdout.write(json.dumps(record) + "\n")
        sys.stdout.flush()

    def info(self, msg: str, **kwargs):
        self._log("INFO", msg, **kwargs)

    def error(self, msg: str, **kwargs):
        self._log("ERROR", msg, **kwargs)

    def warn(self, msg: str, **kwargs):
        self._log("WARN", msg, **kwargs)

    def debug(self, msg: str, **kwargs):
        self._log("DEBUG", msg, **kwargs)


class Peer:
    def __init__(self, peer_id: str, addr: str):
        self.id = peer_id
        self.addr = addr


class RaftNode:
    def __init__(self, node_id: str, peers: Dict[str, Peer], store: Storage, logger: JsonLogger):
        self.mu = threading.Lock()
        self.id = node_id
        self.peers = peers
        self.storage = store
        self.grpc_peers: Dict[str, raft_pb2_grpc.RaftStub] = {}

        # Raft state
        self.role = "Follower"
        self.current_term = 0
        self.voted_for = ""
        self.leader_id = ""

        # Volatile state on all servers
        self.commit_index = store.commit_index()
        self.last_applied = store.commit_index()

        # Volatile state on leaders
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Timers and tracking
        self.election_timeout = 0.0
        self.last_heartbeat_time = 0.0

        # Coordination
        self.stop_event = threading.Event()
        self.on_commit_event = threading.Event()
        self.logger = logger.with_fields(node_id=node_id)
        self.partitioned = False

        self._reset_election_timeout()
        self.last_heartbeat_time = time.time()

    def set_grpc_peer(self, peer_id: str, client: raft_pb2_grpc.RaftStub):
        with self.mu:
            self.grpc_peers[peer_id] = client

    def start(self):
        t1 = threading.Thread(target=self._run_election_loop)
        t1.daemon = True
        t1.start()

        t2 = threading.Thread(target=self._run_heartbeat_loop)
        t2.daemon = True
        t2.start()

        t3 = threading.Thread(target=self._run_apply_loop)
        t3.daemon = True
        t3.start()

    def stop(self):
        self.stop_event.set()
        self.on_commit_event.set()

    def _reset_election_timeout(self):
        # Randomized timeout between 150ms and 300ms
        self.election_timeout = 0.150 + random.random() * 0.150

    def _transition_to(self, role: str):
        old_role = self.role
        self.role = role
        self.logger.info("Role transition", **{"from": old_role, "to": role, "term": self.current_term})

        if role == "Leader":
            self.leader_id = self.id
            last_idx, _ = self.storage.last_log_info()
            for peer_id in self.peers:
                self.next_index[peer_id] = last_idx + 1
                self.match_index[peer_id] = 0
        elif role == "Candidate":
            self.leader_id = ""

    def _run_election_loop(self):
        while not self.stop_event.is_set():
            time.sleep(0.020)
            self._check_election_timeout()

    def _check_election_timeout(self):
        with self.mu:
            if self.role == "Leader":
                return
            if time.time() - self.last_heartbeat_time >= self.election_timeout:
                self._start_election()

    # Caller must hold lock
    def _start_election(self):
        self.current_term += 1
        self._transition_to("Candidate")
        self.voted_for = self.id
        self.last_heartbeat_time = time.time()
        self._reset_election_timeout()

        term = self.current_term
        last_log_idx, last_log_term = self.storage.last_log_info()

        self.logger.info("Starting election", term=term)

        # Collect votes in parallel
        votes = 1  # Vote for self
        votes_lock = threading.Lock()
        threads = []

        def request_vote_thread(peer_id, client):
            nonlocal votes
            with self.mu:
                if self.partitioned:
                    return

            req = raft_pb2.RequestVoteRequest(
                term=term,
                candidate_id=self.id,
                last_log_index=last_log_idx,
                last_log_term=last_log_term
            )
            try:
                resp = client.RequestVote(req, timeout=0.080)
            except Exception:
                return

            with self.mu:
                if resp.term > self.current_term:
                    self.current_term = resp.term
                    self._transition_to("Follower")
                    self.voted_for = ""
                    self.last_heartbeat_time = time.time()
                    return

                if self.role == "Candidate" and term == self.current_term and resp.vote_granted:
                    with votes_lock:
                        votes += 1
                        current_votes = votes
                    
                    majority = (len(self.peers) + 1) // 2 + 1
                    if current_votes >= majority:
                        if self.role == "Candidate":
                            self._transition_to("Leader")
                            self.logger.info("Election won", term=term, votes=current_votes)
                            self._send_heartbeats()

        for peer_id in self.peers:
            client = self.grpc_peers.get(peer_id)
            if not client:
                continue
            t = threading.Thread(target=request_vote_thread, args=(peer_id, client))
            t.daemon = True
            t.start()
            threads.append((peer_id, t))

        # Log split vote if we don't win after all RPCs complete
        def check_split_vote():
            for _, t in threads:
                t.join(timeout=0.1)
            with self.mu:
                if self.role == "Candidate" and term == self.current_term:
                    with votes_lock:
                        current_votes = votes
                    self.logger.info("Election finished, did not win majority (split vote)", term=term, votes=current_votes)

        t_checker = threading.Thread(target=check_split_vote)
        t_checker.daemon = True
        t_checker.start()

    def _run_heartbeat_loop(self):
        while not self.stop_event.is_set():
            time.sleep(0.050)
            with self.mu:
                if self.role == "Leader":
                    self._send_heartbeats()

    # Caller must hold lock
    def _send_heartbeats(self):
        term = self.current_term

        for peer_id in self.peers:
            client = self.grpc_peers.get(peer_id)
            if not client:
                continue

            prev_log_index = self.next_index[peer_id] - 1
            prev_log_term = 0
            entry, ok = self.storage.get_entry(prev_log_index)
            if ok and entry:
                prev_log_term = entry.term

            raw_entries = self.storage.get_log(self.next_index[peer_id])
            entries = []
            for e in raw_entries:
                entries.append(raft_pb2.LogEntry(
                    index=e.index,
                    term=e.term,
                    command=e.command
                ))

            def send_append_entries_thread(p_id, cl, prev_idx, prev_term, ents):
                with self.mu:
                    if self.partitioned:
                        return

                req = raft_pb2.AppendEntriesRequest(
                    term=term,
                    leader_id=self.id,
                    prev_log_index=prev_idx,
                    prev_log_term=prev_term,
                    entries=ents,
                    leader_commit=self.commit_index
                )
                try:
                    resp = cl.AppendEntries(req, timeout=0.080)
                except Exception:
                    return

                with self.mu:
                    if resp.term > self.current_term:
                        self.current_term = resp.term
                        self._transition_to("Follower")
                        self.voted_for = ""
                        self.last_heartbeat_time = time.time()
                        return

                    if self.role == "Leader" and term == self.current_term:
                        if resp.success:
                            if ents:
                                self.next_index[p_id] = ents[-1].index + 1
                                self.match_index[p_id] = ents[-1].index
                                self._check_commit_index()
                        else:
                            if self.next_index[p_id] > 1:
                                self.next_index[p_id] -= 1

            t = threading.Thread(
                target=send_append_entries_thread,
                args=(peer_id, client, prev_log_index, prev_log_term, entries)
            )
            t.daemon = True
            t.start()

    # Caller must hold lock
    def _check_commit_index(self):
        if self.role != "Leader":
            return

        last_idx, _ = self.storage.last_log_info()
        for N in range(last_idx, self.commit_index, -1):
            entry, ok = self.storage.get_entry(N)
            if not ok or not entry or entry.term != self.current_term:
                continue

            count = 1  # Include self
            for peer_id in self.peers:
                if self.match_index.get(peer_id, 0) >= N:
                    count += 1

            majority = (len(self.peers) + 1) // 2 + 1
            if count >= majority:
                self.commit_index = N
                self.logger.info("Leader advanced commit index", commitIndex=N)
                self.on_commit_event.set()
                break

    def _run_apply_loop(self):
        while not self.stop_event.is_set():
            self.on_commit_event.wait(timeout=0.020)
            self.on_commit_event.clear()
            self._apply_entries()

    def _apply_entries(self):
        with self.mu:
            while self.commit_index > self.last_applied:
                next_apply_idx = self.last_applied + 1
                entry, ok = self.storage.get_entry(next_apply_idx)
                if not ok or not entry:
                    break

                try:
                    self.storage.append_and_apply(LogEntry(
                        index=entry.index,
                        term=entry.term,
                        command=entry.command
                    ))
                except Exception as e:
                    self.logger.error("Failed to apply entry to WAL/state machine", index=next_apply_idx, error=str(e))
                    break

                self.last_applied = next_apply_idx
                self.logger.info("Committed log entry applied to state machine", index=next_apply_idx, command=entry.command)

    def RequestVote(self, request: raft_pb2.RequestVoteRequest, context) -> raft_pb2.RequestVoteResponse:
        with self.mu:
            if self.partitioned:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("node is partitioned")
                return raft_pb2.RequestVoteResponse()

            resp = raft_pb2.RequestVoteResponse(
                term=self.current_term,
                vote_granted=False
            )

            if request.term < self.current_term:
                return resp

            if request.term > self.current_term:
                self.current_term = request.term
                self._transition_to("Follower")
                self.voted_for = ""
                self.last_heartbeat_time = time.time()

            last_log_idx, last_log_term = self.storage.last_log_info()

            # Up to date check
            log_up_to_date = False
            if request.last_log_term > last_log_term:
                log_up_to_date = True
            elif request.last_log_term == last_log_term and request.last_log_index >= last_log_idx:
                log_up_to_date = True

            if (self.voted_for == "" or self.voted_for == request.candidate_id) and log_up_to_date:
                self.voted_for = request.candidate_id
                resp.vote_granted = True
                self.last_heartbeat_time = time.time()
                self.logger.info("Vote granted", to=request.candidate_id, term=self.current_term)

            resp.term = self.current_term
            return resp

    def AppendEntries(self, request: raft_pb2.AppendEntriesRequest, context) -> raft_pb2.AppendEntriesResponse:
        with self.mu:
            if self.partitioned:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details("node is partitioned")
                return raft_pb2.AppendEntriesResponse()

            resp = raft_pb2.AppendEntriesResponse(
                term=self.current_term,
                success=False
            )

            if request.term < self.current_term:
                return resp

            if request.term > self.current_term:
                self.current_term = request.term
                self._transition_to("Follower")
                self.voted_for = ""

            self.leader_id = request.leader_id
            self.last_heartbeat_time = time.time()

            if self.role == "Candidate":
                self._transition_to("Follower")

            # Consistency check
            last_log_idx, _ = self.storage.last_log_info()
            if request.prev_log_index > last_log_idx:
                resp.term = self.current_term
                return resp

            prev_entry, ok = self.storage.get_entry(request.prev_log_index)
            if ok and prev_entry:
                if prev_entry.term != request.prev_log_term:
                    resp.term = self.current_term
                    return resp
            elif request.prev_log_index > 0:
                resp.term = self.current_term
                return resp

            # Append any new entries to in-memory log, resolve conflicts
            storage_entries = []
            for e in request.entries:
                storage_entries.append(LogEntry(
                    index=e.index,
                    term=e.term,
                    command=e.command
                ))
            self.storage.append_in_memory(storage_entries)

            resp.success = True
            resp.term = self.current_term

            # Update commit index
            if request.leader_commit > self.commit_index:
                last_idx, _ = self.storage.last_log_info()
                self.commit_index = request.leader_commit
                if last_idx < self.commit_index:
                    self.commit_index = last_idx
                self.on_commit_event.set()

            return resp

    def propose(self, command: str) -> Tuple[bool, Optional[str]]:
        with self.mu:
            if self.role != "Leader":
                return False, "not leader"

            last_idx, _ = self.storage.last_log_info()
            new_idx = last_idx + 1
            entry = LogEntry(
                index=new_idx,
                term=self.current_term,
                command=command
            )

            self.storage.append_in_memory([entry])
            term = self.current_term

        self.logger.info("Proposed command", index=new_idx, command=command)

        with self.mu:
            if self.role == "Leader":
                self._send_heartbeats()

        # Poll until committed or role/term changes
        start_time = time.time()
        while True:
            if self.stop_event.is_set():
                return False, "node stopped"
            if time.time() - start_time >= 2.0:
                return False, "timeout waiting for commit"

            time.sleep(0.010)

            with self.mu:
                if self.role != "Leader" or self.current_term != term:
                    return False, "lost leadership or term changed"
                if self.commit_index >= new_idx:
                    return True, None

    def get_status(self) -> Tuple[str, str, int, int, int]:
        with self.mu:
            last_idx, _ = self.storage.last_log_info()
            return self.id, self.role, self.current_term, self.commit_index, last_idx

    def get_role(self) -> str:
        with self.mu:
            return self.role

    def term(self) -> int:
        with self.mu:
            return self.current_term

    def leader_id_str(self) -> str:
        with self.mu:
            return self.leader_id

    def set_partitioned(self, p: bool):
        with self.mu:
            self.partitioned = p
