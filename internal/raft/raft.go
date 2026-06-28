package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"kvstore/internal/storage"
	"kvstore/internal/transport"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

type Peer struct {
	ID   string
	Addr string
}

type RaftNode struct {
	mu        sync.Mutex
	id        string
	peers     map[string]*Peer
	storage   *storage.Storage
	grpcPeers map[string]transport.RaftClient

	// Raft state
	role        Role
	currentTerm int64
	votedFor    string
	leaderID    string

	// Volatile state on all servers
	commitIndex int64
	lastApplied int64

	// Volatile state on leaders
	nextIndex  map[string]int64
	matchIndex map[string]int64

	// Timers and tracking
	electionTimeout   time.Duration
	lastHeartbeatTime time.Time

	// Coordination
	stopChan     chan struct{}
	logger       *slog.Logger
	onCommitChan chan struct{}

	partitioned bool
}

func NewRaftNode(id string, peers map[string]*Peer, store *storage.Storage, logger *slog.Logger) *RaftNode {
	rn := &RaftNode{
		id:           id,
		peers:        peers,
		storage:      store,
		grpcPeers:    make(map[string]transport.RaftClient),
		role:         Follower,
		currentTerm:  0,
		votedFor:     "",
		commitIndex:  store.CommitIndex(),
		lastApplied:  store.CommitIndex(),
		nextIndex:    make(map[string]int64),
		matchIndex:   make(map[string]int64),
		stopChan:     make(chan struct{}),
		logger:       logger.With("node_id", id),
		onCommitChan: make(chan struct{}, 100),
	}

	rn.resetElectionTimeout()
	rn.lastHeartbeatTime = time.Now()

	return rn
}

func (rn *RaftNode) SetGRPCPeer(peerID string, client transport.RaftClient) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.grpcPeers[peerID] = client
}

func (rn *RaftNode) Start() {
	go rn.runElectionLoop()
	go rn.runHeartbeatLoop()
	go rn.runApplyLoop()
}

func (rn *RaftNode) Stop() {
	close(rn.stopChan)
}

func (rn *RaftNode) resetElectionTimeout() {
	// Randomized timeout between 150ms and 300ms
	ms := 150 + rand.Intn(150)
	rn.electionTimeout = time.Duration(ms) * time.Millisecond
}

func (rn *RaftNode) transitionTo(role Role) {
	oldRole := rn.role
	rn.role = role
	rn.logger.Info("Role transition", "from", oldRole, "to", role, "term", rn.currentTerm)

	if role == Leader {
		rn.leaderID = rn.id
		lastIdx, _ := rn.storage.LastLogInfo()
		for peerID := range rn.peers {
			rn.nextIndex[peerID] = lastIdx + 1
			rn.matchIndex[peerID] = 0
		}
	} else if role == Candidate {
		rn.leaderID = ""
	}
}

func (rn *RaftNode) runElectionLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopChan:
			return
		case <-ticker.C:
			rn.checkElectionTimeout()
		}
	}
}

func (rn *RaftNode) checkElectionTimeout() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role == Leader {
		return
	}

	if time.Since(rn.lastHeartbeatTime) >= rn.electionTimeout {
		rn.startElection()
	}
}

// Caller must hold lock
func (rn *RaftNode) startElection() {
	rn.currentTerm++
	rn.transitionTo(Candidate)
	rn.votedFor = rn.id
	rn.lastHeartbeatTime = time.Now()
	rn.resetElectionTimeout()

	term := rn.currentTerm
	lastLogIdx, lastLogTerm := rn.storage.LastLogInfo()

	rn.logger.Info("Starting election", "term", term)

	// Collect votes in parallel
	var votes int = 1 // Vote for self
	var mu sync.Mutex
	var wg sync.WaitGroup

	for peerID := range rn.peers {
		client, exists := rn.grpcPeers[peerID]
		if !exists {
			continue
		}

		wg.Add(1)
		go func(pID string, cl transport.RaftClient) {
			defer wg.Done()

			rn.mu.Lock()
			if rn.partitioned {
				rn.mu.Unlock()
				return
			}
			rn.mu.Unlock()

			req := &transport.RequestVoteRequest{
				Term:         term,
				CandidateId:  rn.id,
				LastLogIndex: lastLogIdx,
				LastLogTerm:  lastLogTerm,
			}

			// Add a short timeout to RPCs to avoid blocking election
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()

			resp, err := cl.RequestVote(ctx, req)
			if err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if resp.Term > rn.currentTerm {
				rn.currentTerm = resp.Term
				rn.transitionTo(Follower)
				rn.votedFor = ""
				rn.lastHeartbeatTime = time.Now()
				return
			}

			if rn.role == Candidate && term == rn.currentTerm && resp.VoteGranted {
				mu.Lock()
				votes++
				// Check for majority
				majority := (len(rn.peers) + 1) / 2 + 1
				if votes >= majority {
					if rn.role == Candidate {
						rn.transitionTo(Leader)
						rn.logger.Info("Election won", "term", term, "votes", votes)
						go rn.sendHeartbeats()
					}
				}
				mu.Unlock()
			}
		}(peerID, client)
	}

	// Wait in background and log split votes
	go func() {
		wg.Wait()
		rn.mu.Lock()
		defer rn.mu.Unlock()
		if rn.role == Candidate && term == rn.currentTerm {
			mu.Lock()
			vCount := votes
			mu.Unlock()
			rn.logger.Info("Election finished, did not win majority (split vote)", "term", term, "votes", vCount)
		}
	}()
}

func (rn *RaftNode) runHeartbeatLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopChan:
			return
		case <-ticker.C:
			rn.mu.Lock()
			if rn.role == Leader {
				rn.sendHeartbeats()
			}
			rn.mu.Unlock()
		}
	}
}

// Caller must hold lock
func (rn *RaftNode) sendHeartbeats() {
	term := rn.currentTerm

	for peerID := range rn.peers {
		client, exists := rn.grpcPeers[peerID]
		if !exists {
			continue
		}

		prevLogIndex := rn.nextIndex[peerID] - 1
		prevLogTerm := int64(0)
		if entry, ok := rn.storage.GetEntry(prevLogIndex); ok {
			prevLogTerm = entry.Term
		}

		// Replicate entries starting from nextIndex
		rawEntries := rn.storage.GetLog(rn.nextIndex[peerID])
		var entries []*transport.LogEntry
		for _, e := range rawEntries {
			entries = append(entries, &transport.LogEntry{
				Index:   e.Index,
				Term:    e.Term,
				Command: e.Command,
			})
		}

		go func(pID string, cl transport.RaftClient, prevIdx, prevTerm int64, ents []*transport.LogEntry) {
			rn.mu.Lock()
			if rn.partitioned {
				rn.mu.Unlock()
				return
			}
			rn.mu.Unlock()

			req := &transport.AppendEntriesRequest{
				Term:         term,
				LeaderId:     rn.id,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  prevTerm,
				Entries:      ents,
				LeaderCommit: rn.commitIndex,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()

			resp, err := cl.AppendEntries(ctx, req)
			if err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if resp.Term > rn.currentTerm {
				rn.currentTerm = resp.Term
				rn.transitionTo(Follower)
				rn.votedFor = ""
				rn.lastHeartbeatTime = time.Now()
				return
			}

			if rn.role == Leader && term == rn.currentTerm {
				if resp.Success {
					// Consistency check succeeded
					if len(ents) > 0 {
						rn.nextIndex[pID] = ents[len(ents)-1].Index + 1
						rn.matchIndex[pID] = ents[len(ents)-1].Index
						rn.checkCommitIndex()
					}
				} else {
					// Mismatch: decrement nextIndex and retry later
					if rn.nextIndex[pID] > 1 {
						rn.nextIndex[pID]--
					}
				}
			}
		}(peerID, client, prevLogIndex, prevLogTerm, entries)
	}
}

// Caller must hold lock
func (rn *RaftNode) checkCommitIndex() {
	if rn.role != Leader {
		return
	}

	lastIdx, _ := rn.storage.LastLogInfo()
	for N := lastIdx; N > rn.commitIndex; N-- {
		entry, ok := rn.storage.GetEntry(N)
		if !ok || entry.Term != rn.currentTerm {
			continue
		}

		// Count replicates
		count := 1 // Include self
		for _, matchIdx := range rn.matchIndex {
			if matchIdx >= N {
				count++
			}
		}

		majority := (len(rn.peers) + 1) / 2 + 1
		if count >= majority {
			rn.commitIndex = N
			rn.logger.Info("Leader advanced commit index", "commitIndex", N)
			rn.onCommitChan <- struct{}{}
			break
		}
	}
}

func (rn *RaftNode) runApplyLoop() {
	for {
		select {
		case <-rn.stopChan:
			return
		case <-rn.onCommitChan:
			rn.applyEntries()
		case <-time.After(20 * time.Millisecond):
			// Periodically check in case channel was missed
			rn.applyEntries()
		}
	}
}

func (rn *RaftNode) applyEntries() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for rn.commitIndex > rn.lastApplied {
		nextApplyIdx := rn.lastApplied + 1
		entry, ok := rn.storage.GetEntry(nextApplyIdx)
		if !ok {
			break
		}

		// Write to WAL and apply to KV map
		err := rn.storage.AppendAndApply(storage.LogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: entry.Command,
		})
		if err != nil {
			rn.logger.Error("Failed to apply entry to WAL/state machine", "index", nextApplyIdx, "error", err)
			break
		}

		rn.lastApplied = nextApplyIdx
		rn.logger.Info("Committed log entry applied to state machine", "index", nextApplyIdx, "command", entry.Command)
	}
}

// RequestVote RPC Handler
func (rn *RaftNode) RequestVote(ctx context.Context, req *transport.RequestVoteRequest) (*transport.RequestVoteResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.partitioned {
		return nil, status.Error(codes.Unavailable, "node is partitioned")
	}

	resp := &transport.RequestVoteResponse{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	if req.Term < rn.currentTerm {
		return resp, nil
	}

	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.transitionTo(Follower)
		rn.votedFor = ""
		rn.lastHeartbeatTime = time.Now()
	}

	lastLogIdx, lastLogTerm := rn.storage.LastLogInfo()

	// Up to date check
	logUpToDate := false
	if req.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIdx {
		logUpToDate = true
	}

	if (rn.votedFor == "" || rn.votedFor == req.CandidateId) && logUpToDate {
		rn.votedFor = req.CandidateId
		resp.VoteGranted = true
		rn.lastHeartbeatTime = time.Now()
		rn.logger.Info("Vote granted", "to", req.CandidateId, "term", rn.currentTerm)
	}

	resp.Term = rn.currentTerm
	return resp, nil
}

// AppendEntries RPC Handler
func (rn *RaftNode) AppendEntries(ctx context.Context, req *transport.AppendEntriesRequest) (*transport.AppendEntriesResponse, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.partitioned {
		return nil, status.Error(codes.Unavailable, "node is partitioned")
	}

	resp := &transport.AppendEntriesResponse{
		Term:    rn.currentTerm,
		Success: false,
	}

	if req.Term < rn.currentTerm {
		return resp, nil
	}

	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.transitionTo(Follower)
		rn.votedFor = ""
	}

	rn.leaderID = req.LeaderId
	rn.lastHeartbeatTime = time.Now()

	if rn.role == Candidate {
		rn.transitionTo(Follower)
	}

	// Consistency check
	lastLogIdx, _ := rn.storage.LastLogInfo()
	if req.PrevLogIndex > lastLogIdx {
		return resp, nil
	}

	if prevEntry, ok := rn.storage.GetEntry(req.PrevLogIndex); ok {
		if prevEntry.Term != req.PrevLogTerm {
			return resp, nil
		}
	} else if req.PrevLogIndex > 0 {
		return resp, nil
	}

	// Append any new entries to in-memory log, resolve conflicts
	var storageEntries []storage.LogEntry
	for _, e := range req.Entries {
		storageEntries = append(storageEntries, storage.LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Command: e.Command,
		})
	}
	rn.storage.AppendInMemory(storageEntries)

	resp.Success = true
	resp.Term = rn.currentTerm

	// Update commit index
	if req.LeaderCommit > rn.commitIndex {
		lastIdx, _ := rn.storage.LastLogInfo()
		rn.commitIndex = req.LeaderCommit
		if lastIdx < rn.commitIndex {
			rn.commitIndex = lastIdx
		}
		rn.onCommitChan <- struct{}{}
	}

	return resp, nil
}

// Client Propose Write
func (rn *RaftNode) Propose(command string) (bool, error) {
	rn.mu.Lock()
	if rn.role != Leader {
		rn.mu.Unlock()
		return false, nil
	}

	lastIdx, _ := rn.storage.LastLogInfo()
	newIdx := lastIdx + 1
	entry := storage.LogEntry{
		Index:   newIdx,
		Term:    rn.currentTerm,
		Command: command,
	}

	// Append locally in memory
	rn.storage.AppendInMemory([]storage.LogEntry{entry})
	term := rn.currentTerm
	rn.mu.Unlock()

	// Wait for commit
	rn.logger.Info("Proposed command", "index", newIdx, "command", command)
	
	// Fast heartbeat send
	rn.mu.Lock()
	if rn.role == Leader {
		rn.sendHeartbeats()
	}
	rn.mu.Unlock()

	// Poll until committed or role/term changes
	timeout := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopChan:
			return false, fmt.Errorf("node stopped")
		case <-timeout:
			return false, fmt.Errorf("timeout waiting for commit")
		case <-ticker.C:
			rn.mu.Lock()
			if rn.role != Leader || rn.currentTerm != term {
				rn.mu.Unlock()
				return false, fmt.Errorf("lost leadership or term changed")
			}
			if rn.commitIndex >= newIdx {
				rn.mu.Unlock()
				return true, nil
			}
			rn.mu.Unlock()
		}
	}
}

// GetStatus returns the status of the node for telemetry/diagnostics
func (rn *RaftNode) GetStatus() (string, string, int64, int64, int64) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	lastIdx, _ := rn.storage.LastLogInfo()
	return rn.id, string(rn.role), rn.currentTerm, rn.commitIndex, lastIdx
}

func (rn *RaftNode) GetRole() Role {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.role
}

func (rn *RaftNode) Term() int64 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentTerm
}

func (rn *RaftNode) LeaderID() string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.leaderID
}

func (rn *RaftNode) SetPartitioned(p bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.partitioned = p
}
