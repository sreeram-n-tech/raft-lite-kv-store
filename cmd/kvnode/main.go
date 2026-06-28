package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"kvstore/internal/raft"
	"kvstore/internal/server"
	"kvstore/internal/storage"
)

func main() {
	nodeID := flag.String("id", "", "Unique ID of this node")
	grpcAddr := flag.String("grpc-addr", "", "Address to bind for gRPC communication")
	httpAddr := flag.String("http-addr", "", "Address to bind for HTTP client API")
	peersFlag := flag.String("peers", "", "Comma-separated peer list nodeID=gRPCAddr")
	peerHTTPsFlag := flag.String("peer-https", "", "Comma-separated peer list nodeID=HTTPAddr")
	walPath := flag.String("wal-path", "", "Path to the WAL file")
	flag.Parse()

	if *nodeID == "" || *grpcAddr == "" || *httpAddr == "" || *walPath == "" {
		slog.Error("Missing required flags")
		flag.Usage()
		os.Exit(1)
	}

	// Setup slog logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Parse peers
	peers := make(map[string]*raft.Peer)
	if *peersFlag != "" {
		for _, part := range strings.Split(*peersFlag, ",") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				peers[kv[0]] = &raft.Peer{
					ID:   kv[0],
					Addr: kv[1],
				}
			}
		}
	}

	// Parse peer HTTP addresses
	peerHTTPs := make(map[string]string)
	if *peerHTTPsFlag != "" {
		for _, part := range strings.Split(*peerHTTPsFlag, ",") {
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				peerHTTPs[kv[0]] = kv[1]
			}
		}
	}

	logger.Info("Starting node storage", "wal_path", *walPath)
	store, err := storage.NewStorage(*walPath)
	if err != nil {
		logger.Error("Failed to initialize storage", "error", err)
		os.Exit(1)
	}

	srv := server.NewServer(*nodeID, *grpcAddr, *httpAddr, peers, peerHTTPs, store, logger)
	if err := srv.Start(); err != nil {
		logger.Error("Failed to start server", "error", err)
		os.Exit(1)
	}

	// Wait for termination signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Stopping node server")
	srv.Stop()
	store.Close()
	logger.Info("Node stopped successfully")
}
