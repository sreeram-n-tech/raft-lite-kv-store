import sys
import signal
import argparse
import threading

# Add workspace root to sys.path so we can import internal.* cleanly if executed directly
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from internal.storage.storage import Storage
from internal.raft.raft import JsonLogger, Peer
from internal.server.server import Server

def main():
    parser = argparse.ArgumentParser(description="Raft Key-Value Node")
    parser.add_argument("--id", required=True, help="Unique ID of this node")
    parser.add_argument("--grpc-addr", required=True, help="Address to bind for gRPC communication")
    parser.add_argument("--http-addr", required=True, help="Address to bind for HTTP client API")
    parser.add_argument("--peers", default="", help="Comma-separated peer list nodeID=gRPCAddr")
    parser.add_argument("--peer-https", default="", help="Comma-separated peer list nodeID=HTTPAddr")
    parser.add_argument("--wal-path", required=True, help="Path to the WAL file")
    
    args = parser.parse_args()

    logger = JsonLogger(name="kvnode")

    # Parse peers
    peers = {}
    if args.peers:
        for part in args.peers.split(","):
            if not part:
                continue
            kv = part.split("=", 1)
            if len(kv) == 2:
                peers[kv[0]] = Peer(kv[0], kv[1])

    # Parse peer HTTPs
    peer_https = {}
    if args.peer_https:
        for part in args.peer_https.split(","):
            if not part:
                continue
            kv = part.split("=", 1)
            if len(kv) == 2:
                peer_https[kv[0]] = kv[1]

    logger.info("Starting node storage", wal_path=args.wal_path)
    try:
        store = Storage(args.wal_path)
    except Exception as e:
        logger.error("Failed to initialize storage", error=str(e))
        sys.exit(1)

    srv = Server(args.id, args.grpc_addr, args.http_addr, peers, peer_https, store, logger)
    try:
        srv.start()
    except Exception as e:
        logger.error("Failed to start server", error=str(e))
        store.close()
        sys.exit(1)

    # Wires up signals
    stop_event = threading.Event()
    def signal_handler(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while not stop_event.is_set():
            stop_event.wait(timeout=1.0)
    except (KeyboardInterrupt, SystemExit):
        pass

    logger.info("Stopping node server")
    srv.stop()
    store.close()
    logger.info("Node stopped successfully")

if __name__ == "__main__":
    main()
