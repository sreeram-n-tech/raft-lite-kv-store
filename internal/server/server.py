import json
import time
import threading
import concurrent.futures
from urllib.parse import urlparse, parse_qs
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import grpc

from internal.raft import RaftNode, Peer, JsonLogger
from internal.storage import Storage
from internal.transport import raft_pb2, raft_pb2_grpc

class RaftServicer(raft_pb2_grpc.RaftServicer):
    def __init__(self, raft_node: RaftNode):
        self.raft_node = raft_node

    def RequestVote(self, request, context):
        return self.raft_node.RequestVote(request, context)

    def AppendEntries(self, request, context):
        return self.raft_node.AppendEntries(request, context)


class KVHTTPHandler(BaseHTTPRequestHandler):
    # Disable standard logging of every HTTP request to keep stdout clean unless required
    def log_message(self, format, *args):
        pass

    def _send_error(self, code: int, message: str):
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(message.encode("utf-8"))

    def _send_json(self, code: int, data: dict):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data, separators=(',', ':')).encode("utf-8"))

    def do_GET(self):
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        if path == "/status":
            node_id, role, term, commit_idx, log_len = self.server.app_server.raft_node.get_status()
            leader_id = self.server.app_server.raft_node.leader_id_str()
            leader_http = ""
            if leader_id == self.server.app_server.node_id:
                leader_http = self.server.app_server.http_addr
            elif leader_id in self.server.app_server.peer_https:
                leader_http = self.server.app_server.peer_https[leader_id]

            status_resp = {
                "node_id": node_id,
                "role": role,
                "term": term,
                "commit_index": commit_idx,
                "log_length": log_len,
                "leader": leader_http
            }
            self._send_json(200, status_resp)
            return

        if path.startswith("/kv"):
            key = path[3:]
            if key.startswith("/"):
                key = key[1:]

            if not key:
                self._send_error(400, "missing key")
                return

            query_params = parse_qs(parsed_url.query)
            stale = query_params.get("stale", [""])[0] == "true"

            role = self.server.app_server.raft_node.get_role()
            if not stale and role != "Leader":
                leader_id = self.server.app_server.raft_node.leader_id_str()
                if not leader_id:
                    self._send_error(503, "no leader currently elected")
                    return
                leader_http = self.server.app_server.peer_https.get(leader_id)
                if not leader_http:
                    self._send_error(503, "leader address unknown")
                    return

                redirect_url = f"http://{leader_http}/kv/{key}"
                self._send_json(307, {
                    "error": "not leader",
                    "leader": redirect_url
                })
                return

            val, ok = self.server.app_server.storage.get(key)
            if not ok:
                self._send_error(404, "key not found")
                return

            self._send_json(200, {"key": key, "value": val})
            return

        self._send_error(404, "not found")

    def do_POST(self):
        self.do_PUT()

    def do_PUT(self):
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        if path.startswith("/kv"):
            key = path[3:]
            if key.startswith("/"):
                key = key[1:]

            role = self.server.app_server.raft_node.get_role()
            if role != "Leader":
                leader_id = self.server.app_server.raft_node.leader_id_str()
                if not leader_id:
                    self._send_error(503, "no leader currently elected")
                    return
                leader_http = self.server.app_server.peer_https.get(leader_id)
                if not leader_http:
                    self._send_error(503, "leader address unknown")
                    return

                redirect_url = f"http://{leader_http}/kv/{key}"
                self._send_json(307, {
                    "error": "not leader",
                    "leader": redirect_url
                })
                return

            if not key:
                self._send_error(400, "missing key")
                return

            content_length = int(self.headers.get("Content-Length", 0))
            val = self.rfile.read(content_length).decode("utf-8")

            command = f"PUT:{key}:{val}"
            success, err = self.server.app_server.raft_node.propose(command)
            if err:
                self._send_error(500, f"failed to commit: {err}")
                return
            if not success:
                self._send_error(307, "not leader during write")
                return

            self._send_json(200, {
                "status": "ok",
                "key": key,
                "value": val
            })
            return

        self._send_error(404, "not found")

    def do_DELETE(self):
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        if path.startswith("/kv"):
            key = path[3:]
            if key.startswith("/"):
                key = key[1:]

            role = self.server.app_server.raft_node.get_role()
            if role != "Leader":
                leader_id = self.server.app_server.raft_node.leader_id_str()
                if not leader_id:
                    self._send_error(503, "no leader currently elected")
                    return
                leader_http = self.server.app_server.peer_https.get(leader_id)
                if not leader_http:
                    self._send_error(503, "leader address unknown")
                    return

                redirect_url = f"http://{leader_http}/kv/{key}"
                self._send_json(307, {
                    "error": "not leader",
                    "leader": redirect_url
                })
                return

            if not key:
                self._send_error(400, "missing key")
                return

            command = f"DELETE:{key}"
            success, err = self.server.app_server.raft_node.propose(command)
            if err:
                self._send_error(500, f"failed to commit: {err}")
                return
            if not success:
                self._send_error(307, "not leader during write")
                return

            self._send_json(200, {
                "status": "ok",
                "key": key
            })
            return

        self._send_error(404, "not found")


class Server:
    def __init__(self, node_id: str, grpc_addr: str, http_addr: str, peers: dict, peer_https: dict, store: Storage, logger: JsonLogger):
        self.node_id = node_id
        self.grpc_addr = grpc_addr
        self.http_addr = http_addr
        self.peers = peers
        self.peer_https = peer_https
        self.storage = store
        self.logger = logger.with_fields(server_node_id=node_id)
        self.stop_event = threading.Event()
        self.channels = {}

        # Initialize Raft Node
        self.raft_node = RaftNode(self.node_id, self.peers, self.storage, self.logger)
        self.grpc_server = None
        self.http_server = None

    def start(self) -> None:
        # Start gRPC Server
        self.grpc_server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServicer_to_server(RaftServicer(self.raft_node), self.grpc_server)
        self.grpc_server.add_insecure_port(self.grpc_addr)
        self.logger.info("Starting gRPC server", addr=self.grpc_addr)
        self.grpc_server.start()

        # Connect to peers in the background
        t_peers = threading.Thread(target=self._connect_to_peers)
        t_peers.daemon = True
        t_peers.start()

        # Start Raft State Machine
        self.raft_node.start()

        # Start HTTP Server
        http_host, http_port = self.http_addr.split(":", 1)
        self.http_server = ThreadingHTTPServer((http_host, int(http_port)), KVHTTPHandler)
        self.http_server.app_server = self

        t_http = threading.Thread(target=self._run_http_server)
        t_http.daemon = True
        t_http.start()

    def _run_http_server(self):
        self.logger.info("Starting HTTP server", addr=self.http_addr)
        try:
            self.http_server.serve_forever()
        except Exception as e:
            if not self.stop_event.is_set():
                self.logger.error("HTTP server error", error=str(e))

    def stop(self) -> None:
        self.stop_event.set()
        if self.http_server:
            self.http_server.shutdown()
            self.http_server.server_close()
        if self.raft_node:
            self.raft_node.stop()
        if self.grpc_server:
            self.grpc_server.stop(grace=2.0)
        # Close peer channels
        for channel in self.channels.values():
            channel.close()

    def _connect_to_peers(self):
        connected = {}
        while not self.stop_event.is_set():
            all_connected = True
            for peer_id, peer in self.peers.items():
                if peer_id in connected:
                    continue
                all_connected = False
                try:
                    channel = grpc.insecure_channel(peer.addr)
                    # Use a stub client
                    client = raft_pb2_grpc.RaftStub(channel)
                    self.raft_node.set_grpc_peer(peer_id, client)
                    connected[peer_id] = channel
                    self.channels[peer_id] = channel
                    self.logger.info("Connected to peer", id=peer_id, addr=peer.addr)
                except Exception as e:
                    continue

            if all_connected:
                break
            time.sleep(0.200)
