package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kvstore/internal/raft"
	"kvstore/internal/storage"
)

func TestClusterIntegration(t *testing.T) {
	// Allocate directories and ports
	tmpDir := t.TempDir()

	nodeIDs := []string{"node1", "node2", "node3"}
	grpcAddrs := []string{"localhost:50151", "localhost:50152", "localhost:50153"}
	httpAddrs := []string{"localhost:8181", "localhost:8182", "localhost:8183"}

	peerHTTPs := map[string]string{
		"node1": "localhost:8181",
		"node2": "localhost:8182",
		"node3": "localhost:8183",
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Setup storages
	stores := make(map[string]*storage.Storage)
	walPaths := make(map[string]string)
	for _, id := range nodeIDs {
		path := filepath.Join(tmpDir, id+".wal")
		walPaths[id] = path
		store, err := storage.NewStorage(path)
		if err != nil {
			t.Fatalf("failed to create storage: %v", err)
		}
		stores[id] = store
	}

	// Helper to build peer list for a node
	getPeersFor := func(id string) map[string]*raft.Peer {
		peers := make(map[string]*raft.Peer)
		for i, nID := range nodeIDs {
			if nID == id {
				continue
			}
			peers[nID] = &raft.Peer{
				ID:   nID,
				Addr: grpcAddrs[i],
			}
		}
		return peers
	}

	// Create and start servers
	servers := make(map[string]*Server)
	for i, id := range nodeIDs {
		srv := NewServer(id, grpcAddrs[i], httpAddrs[i], getPeersFor(id), peerHTTPs, stores[id], logger)
		if err := srv.Start(); err != nil {
			t.Fatalf("failed to start server %s: %v", id, err)
		}
		servers[id] = srv
	}

	defer func() {
		for _, srv := range servers {
			if srv != nil {
				srv.Stop()
			}
		}
		for _, store := range stores {
			store.Close()
		}
	}()

	// 1. Wait for leader election
	time.Sleep(3 * time.Second)

	var leaderID string
	var leaderAddr string
	for _, id := range nodeIDs {
		role := servers[id].raftNode.GetRole()
		if role == raft.Leader {
			if leaderID != "" {
				t.Fatalf("multiple leaders elected: %s and %s", leaderID, id)
			}
			leaderID = id
			leaderAddr = peerHTTPs[id]
		}
	}

	if leaderID == "" {
		t.Fatalf("no leader elected")
	}
	t.Logf("Elected leader: %s", leaderID)

	// 2. Perform write to leader
	putURL := fmt.Sprintf("http://%s/kv/testkey", leaderAddr)
	resp, err := http.Post(putURL, "text/plain", bytes.NewBufferString("testvalue"))
	if err != nil {
		t.Fatalf("PUT request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT request failed with status %d: %s", resp.StatusCode, string(body))
	}
	resp.Body.Close()

	// Wait for replication
	time.Sleep(100 * time.Millisecond)

	// Verify reads (direct from leader)
	getURL := fmt.Sprintf("http://%s/kv/testkey", leaderAddr)
	resp, err = http.Get(getURL)
	if err != nil {
		t.Fatalf("GET request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET failed on leader with status %d", resp.StatusCode)
	}
	var data map[string]string
	json.NewDecoder(resp.Body).Decode(&data)
	resp.Body.Close()
	if data["value"] != "testvalue" {
		t.Errorf("expected value 'testvalue', got '%s'", data["value"])
	}

	// Verify stale read on follower
	var followerID string
	for _, id := range nodeIDs {
		if id != leaderID {
			followerID = id
			break
		}
	}
	followerAddr := peerHTTPs[followerID]
	resp, err = http.Get(fmt.Sprintf("http://%s/kv/testkey?stale=true", followerAddr))
	if err != nil {
		t.Fatalf("stale GET failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stale GET on follower failed with status %d", resp.StatusCode)
	}
	json.NewDecoder(resp.Body).Decode(&data)
	resp.Body.Close()
	if data["value"] != "testvalue" {
		t.Errorf("expected value 'testvalue' from follower, got '%s'", data["value"])
	}

	// 3. Stop the leader node
	t.Logf("Stopping leader: %s", leaderID)
	servers[leaderID].Stop()
	stores[leaderID].Close()
	servers[leaderID] = nil // remove pointer

	// Wait for new election
	time.Sleep(3 * time.Second)

	// Verify a new leader is elected
	var newLeaderID string
	var newLeaderAddr string
	for _, id := range nodeIDs {
		if id == leaderID {
			continue
		}
		role := servers[id].raftNode.GetRole()
		if role == raft.Leader {
			newLeaderID = id
			newLeaderAddr = peerHTTPs[id]
			break
		}
	}

	if newLeaderID == "" {
		t.Fatalf("no new leader elected after killing old leader")
	}
	t.Logf("New leader elected: %s", newLeaderID)

	// Write against new leader
	resp, err = http.Post(fmt.Sprintf("http://%s/kv/newkey", newLeaderAddr), "text/plain", bytes.NewBufferString("newvalue"))
	if err != nil {
		t.Fatalf("PUT on new leader failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT on new leader failed with status %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 4. Restart the old leader node
	t.Logf("Restarting old leader: %s", leaderID)
	// Create new storage to trigger WAL replay on the same path
	newStore, err := storage.NewStorage(walPaths[leaderID])
	if err != nil {
		t.Fatalf("failed to recreate storage: %v", err)
	}
	stores[leaderID] = newStore

	srvIdx := 0
	for i, id := range nodeIDs {
		if id == leaderID {
			srvIdx = i
		}
	}

	restartedServer := NewServer(leaderID, grpcAddrs[srvIdx], httpAddrs[srvIdx], getPeersFor(leaderID), peerHTTPs, newStore, logger)
	if err := restartedServer.Start(); err != nil {
		t.Fatalf("failed to restart server %s: %v", leaderID, err)
	}
	servers[leaderID] = restartedServer

	// Wait for catch up and synchronization
	time.Sleep(3 * time.Second)

	// Verify restarted node caught up and has both keys
	val, ok := restartedServer.storage.Get("testkey")
	if !ok || val != "testvalue" {
		t.Errorf("restarted node missing 'testkey': ok=%v, val=%v", ok, val)
	}

	val, ok = restartedServer.storage.Get("newkey")
	if !ok || val != "newvalue" {
		t.Errorf("restarted node missing 'newkey': ok=%v, val=%v", ok, val)
	}
}

func TestSplitVote(t *testing.T) {
	tmpDir := t.TempDir()

	nodeIDs := []string{"node1", "node2", "node3"}
	grpcAddrs := []string{"localhost:50251", "localhost:50252", "localhost:50253"}
	httpAddrs := []string{"localhost:8281", "localhost:8282", "localhost:8283"}

	peerHTTPs := map[string]string{
		"node1": "localhost:8281",
		"node2": "localhost:8282",
		"node3": "localhost:8283",
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Setup storages
	stores := make(map[string]*storage.Storage)
	for _, id := range nodeIDs {
		path := filepath.Join(tmpDir, id+".wal")
		store, err := storage.NewStorage(path)
		if err != nil {
			t.Fatalf("failed to create storage: %v", err)
		}
		stores[id] = store
	}

	getPeersFor := func(id string) map[string]*raft.Peer {
		peers := make(map[string]*raft.Peer)
		for i, nID := range nodeIDs {
			if nID == id {
				continue
			}
			peers[nID] = &raft.Peer{
				ID:   nID,
				Addr: grpcAddrs[i],
			}
		}
		return peers
	}

	// Start all 3 servers normally
	servers := make(map[string]*Server)
	for i, id := range nodeIDs {
		srv := NewServer(id, grpcAddrs[i], httpAddrs[i], getPeersFor(id), peerHTTPs, stores[id], logger)
		if err := srv.Start(); err != nil {
			t.Fatalf("failed to start server %s: %v", id, err)
		}
		servers[id] = srv
	}

	defer func() {
		for _, srv := range servers {
			if srv != nil {
				srv.Stop()
			}
		}
		for _, store := range stores {
			store.Close()
		}
	}()

	t.Log("Forcing all nodes into a partitioned candidate state (split vote)...")
	// Set Partitioned on all nodes so they cannot communicate
	for _, srv := range servers {
		srv.raftNode.SetPartitioned(true)
	}

	// Wait to trigger election timeouts in isolation
	time.Sleep(1500 * time.Millisecond)

	// Verify that none is elected leader yet
	for _, id := range nodeIDs {
		role := servers[id].raftNode.GetRole()
		if role == raft.Leader {
			t.Fatalf("node %s became leader while partitioned", id)
		}
	}

	t.Log("Restoring network connectivity to resolve split vote...")
	for _, srv := range servers {
		srv.raftNode.SetPartitioned(false)
	}

	t.Log("Waiting for election to converge...")
	time.Sleep(4 * time.Second)

	// Verify that EXACTLY ONE leader is elected across all three nodes
	leaderCount := 0
	var leaderID string
	for _, id := range nodeIDs {
		role := servers[id].raftNode.GetRole()
		if role == raft.Leader {
			leaderCount++
			leaderID = id
		}
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader after split vote convergence, got %d", leaderCount)
	}
	t.Logf("Elected leader successfully after split vote convergence: %s", leaderID)
}

