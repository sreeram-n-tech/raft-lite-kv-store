package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"kvstore/internal/raft"
	"kvstore/internal/storage"
	"kvstore/internal/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	transport.UnimplementedRaftServer
	mu         sync.RWMutex
	nodeID     string
	grpcAddr   string
	httpAddr   string
	peers      map[string]*raft.Peer
	peerHTTPs  map[string]string // Peer ID -> HTTP address
	storage    *storage.Storage
	raftNode   *raft.RaftNode
	grpcServer *grpc.Server
	httpServer *http.Server
	logger     *slog.Logger
	stopChan   chan struct{}
}

func NewServer(nodeID string, grpcAddr string, httpAddr string, peers map[string]*raft.Peer, peerHTTPs map[string]string, store *storage.Storage, logger *slog.Logger) *Server {
	return &Server{
		nodeID:    nodeID,
		grpcAddr:  grpcAddr,
		httpAddr:  httpAddr,
		peers:     peers,
		peerHTTPs: peerHTTPs,
		storage:   store,
		logger:    logger.With("server_node_id", nodeID),
		stopChan:  make(chan struct{}),
	}
}

func (s *Server) Start() error {
	// Initialize and start Raft Node
	s.raftNode = raft.NewRaftNode(s.nodeID, s.peers, s.storage, s.logger)

	// Listen for gRPC
	lis, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC port: %w", err)
	}

	s.grpcServer = grpc.NewServer()
	transport.RegisterRaftServer(s.grpcServer, s)

	go func() {
		s.logger.Info("Starting gRPC server", "addr", s.grpcAddr)
		if err := s.grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			s.logger.Error("gRPC server error", "error", err)
		}
	}()

	// Establish connections to peers
	go s.connectToPeers()

	s.raftNode.Start()

	// Setup HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/kv", s.handleKV)
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/status", s.handleStatus)

	s.httpServer = &http.Server{
		Addr:    s.httpAddr,
		Handler: mux,
	}

	go func() {
		s.logger.Info("Starting HTTP server", "addr", s.httpAddr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server error", "error", err)
		}
	}()

	return nil
}

func (s *Server) Stop() {
	close(s.stopChan)
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		s.httpServer.Shutdown(ctx)
		cancel()
	}
	if s.raftNode != nil {
		s.raftNode.Stop()
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

func (s *Server) connectToPeers() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	connected := make(map[string]bool)

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			allConnected := true
			for peerID, peer := range s.peers {
				if connected[peerID] {
					continue
				}
				allConnected = false
				conn, err := grpc.Dial(peer.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					continue
				}
				client := transport.NewRaftClient(conn)
				s.raftNode.SetGRPCPeer(peerID, client)
				connected[peerID] = true
				s.logger.Info("Connected to peer", "id", peerID, "addr", peer.Addr)
			}
			if allConnected {
				return
			}
		}
	}
}

// RequestVote gRPC stub
func (s *Server) RequestVote(ctx context.Context, req *transport.RequestVoteRequest) (*transport.RequestVoteResponse, error) {
	return s.raftNode.RequestVote(ctx, req)
}

// AppendEntries gRPC stub
func (s *Server) AppendEntries(ctx context.Context, req *transport.AppendEntriesRequest) (*transport.AppendEntriesResponse, error) {
	return s.raftNode.AppendEntries(ctx, req)
}

func (s *Server) getLeaderHTTP() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, role, _, _, _ := s.raftNode.GetStatus()
	if role == string(raft.Leader) {
		return s.httpAddr
	}

	// Follower or Candidate. Search who has been voted for or find leader ID.
	// Since we don't track leader ID directly, we could let the raft node provide it.
	// Let's modify RaftNode to track the current known leader ID.
	// For simplicity, if we are not leader, we redirect to the current leader.
	// Wait, how does RaftNode know the current leader ID?
	// It can record the LeaderId from AppendEntriesRequest!
	// Let's check: does AppendEntriesRequest have LeaderId? Yes!
	// So we can update RaftNode to expose LeaderID() string.
	return "" // Will return from RaftNode's tracker.
}

type StatusResponse struct {
	NodeID      string `json:"node_id"`
	Role        string `json:"role"`
	Term        int64  `json:"term"`
	CommitIndex int64  `json:"commit_index"`
	LogLength   int64  `json:"log_length"`
	Leader      string `json:"leader"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	nodeID, role, term, commitIdx, logLen := s.raftNode.GetStatus()
	
	// Try to get leader ID from RaftNode. For now we will find leader HTTP.
	// In the real code we will add a method to get leader ID.
	leaderID := s.getLeaderID()
	leaderHTTP := ""
	if leaderID == s.nodeID {
		leaderHTTP = s.httpAddr
	} else if addr, ok := s.peerHTTPs[leaderID]; ok {
		leaderHTTP = addr
	}

	resp := StatusResponse{
		NodeID:      nodeID,
		Role:        role,
		Term:        term,
		CommitIndex: commitIdx,
		LogLength:   logLen,
		Leader:      leaderHTTP,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) getLeaderID() string {
	// A small helper to find leader from node state or append entries tracker.
	// We'll update raftNode to track leader ID.
	// Let's implement LeaderID() string on RaftNode.
	// In raftNode.AppendEntries, we update currentLeaderID = req.LeaderId.
	// If role is Leader, currentLeaderID = id.
	// If role is Candidate, currentLeaderID = "".
	// Let's check how we can retrieve it safely.
	return s.raftNode.LeaderID()
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	key = strings.TrimPrefix(key, "/kv")
	key = strings.TrimPrefix(key, "/")

	switch r.Method {
	case http.MethodGet:
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		stale := r.URL.Query().Get("stale") == "true"
		if !stale && s.raftNode.GetRole() != raft.Leader {
			// Redirect to leader
			leaderID := s.getLeaderID()
			if leaderID == "" {
				http.Error(w, "no leader currently elected", http.StatusServiceUnavailable)
				return
			}
			leaderHTTP := s.peerHTTPs[leaderID]
			if leaderHTTP == "" {
				http.Error(w, "leader address unknown", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTemporaryRedirect)
			json.NewEncoder(w).Encode(map[string]string{
				"error":  "not leader",
				"leader": fmt.Sprintf("http://%s/kv/%s", leaderHTTP, key),
			})
			return
		}

		val, ok := s.storage.Get(key)
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})

	case http.MethodPut, http.MethodPost:
		if s.raftNode.GetRole() != raft.Leader {
			leaderID := s.getLeaderID()
			if leaderID == "" {
				http.Error(w, "no leader currently elected", http.StatusServiceUnavailable)
				return
			}
			leaderHTTP := s.peerHTTPs[leaderID]
			if leaderHTTP == "" {
				http.Error(w, "leader address unknown", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTemporaryRedirect)
			json.NewEncoder(w).Encode(map[string]string{
				"error":  "not leader",
				"leader": fmt.Sprintf("http://%s/kv/%s", leaderHTTP, key),
			})
			return
		}

		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		val := string(body)

		command := fmt.Sprintf("PUT:%s:%s", key, val)
		success, err := s.raftNode.Propose(command)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to commit: %v", err), http.StatusInternalServerError)
			return
		}
		if !success {
			http.Error(w, "not leader during write", http.StatusTemporaryRedirect)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "key": key, "value": val})

	case http.MethodDelete:
		if s.raftNode.GetRole() != raft.Leader {
			leaderID := s.getLeaderID()
			if leaderID == "" {
				http.Error(w, "no leader currently elected", http.StatusServiceUnavailable)
				return
			}
			leaderHTTP := s.peerHTTPs[leaderID]
			if leaderHTTP == "" {
				http.Error(w, "leader address unknown", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTemporaryRedirect)
			json.NewEncoder(w).Encode(map[string]string{
				"error":  "not leader",
				"leader": fmt.Sprintf("http://%s/kv/%s", leaderHTTP, key),
			})
			return
		}

		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		command := fmt.Sprintf("DELETE:%s", key)
		success, err := s.raftNode.Propose(command)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to commit: %v", err), http.StatusInternalServerError)
			return
		}
		if !success {
			http.Error(w, "not leader during write", http.StatusTemporaryRedirect)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "key": key})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
