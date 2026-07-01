import os
import time
import tempfile
import unittest
import requests
from typing import Dict

from internal.storage.storage import Storage
from internal.raft.raft import JsonLogger, Peer
from internal.server.server import Server

class TestServerIntegration(unittest.TestCase):
    def test_cluster_integration(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            node_ids = ["node1", "node2", "node3"]
            grpc_addrs = ["localhost:50151", "localhost:50152", "localhost:50153"]
            http_addrs = ["localhost:8181", "localhost:8182", "localhost:8183"]

            peer_https = {
                "node1": "localhost:8181",
                "node2": "localhost:8182",
                "node3": "localhost:8183",
            }

            logger = JsonLogger(name="test_logger")

            # Setup storages
            stores = {}
            wal_paths = {}
            for nid in node_ids:
                path = os.path.join(tmp_dir, nid + ".wal")
                wal_paths[nid] = path
                stores[nid] = Storage(path)

            # Helper to build peer list for a node
            def get_peers_for(id_):
                peers = {}
                for i, nid in enumerate(node_ids):
                    if nid == id_:
                        continue
                    peers[nid] = Peer(nid, grpc_addrs[i])
                return peers

            # Create and start servers
            servers = {}
            for i, nid in enumerate(node_ids):
                srv = Server(nid, grpc_addrs[i], http_addrs[i], get_peers_for(nid), peer_https, stores[nid], logger)
                srv.start()
                servers[nid] = srv

            try:
                # 1. Wait for leader election
                time.sleep(3.0)

                leader_id = ""
                leader_addr = ""
                for nid in node_ids:
                    role = servers[nid].raft_node.get_role()
                    if role == "Leader":
                        if leader_id:
                            self.fail(f"multiple leaders elected: {leader_id} and {nid}")
                        leader_id = nid
                        leader_addr = peer_https[nid]

                self.assertTrue(leader_id != "", "no leader elected")
                print(f"Elected leader: {leader_id}")

                # 2. Perform write to leader
                put_url = f"http://{leader_addr}/kv/testkey"
                resp = requests.post(put_url, data="testvalue", timeout=5.0)
                self.assertEqual(resp.status_code, 200, f"PUT request failed: {resp.text}")

                # Wait for replication
                time.sleep(0.15)

                # Verify reads (direct from leader)
                get_url = f"http://{leader_addr}/kv/testkey"
                resp = requests.get(get_url, timeout=5.0)
                self.assertEqual(resp.status_code, 200)
                data = resp.json()
                self.assertEqual(data.get("value"), "testvalue")

                # Verify stale read on follower
                follower_id = ""
                for nid in node_ids:
                    if nid != leader_id:
                        follower_id = nid
                        break
                follower_addr = peer_https[follower_id]
                resp = requests.get(f"http://{follower_addr}/kv/testkey?stale=true", timeout=5.0)
                self.assertEqual(resp.status_code, 200)
                data = resp.json()
                self.assertEqual(data.get("value"), "testvalue")

                # 3. Stop the leader node
                print(f"Stopping leader: {leader_id}")
                servers[leader_id].stop()
                stores[leader_id].close()
                servers[leader_id] = None

                # Wait for new election
                time.sleep(3.5)

                # Verify a new leader is elected
                new_leader_id = ""
                new_leader_addr = ""
                for nid in node_ids:
                    if nid == leader_id:
                        continue
                    role = servers[nid].raft_node.get_role()
                    if role == "Leader":
                        new_leader_id = nid
                        new_leader_addr = peer_https[nid]
                        break

                self.assertTrue(new_leader_id != "", "no new leader elected after killing old leader")
                print(f"New leader elected: {new_leader_id}")

                # Write against new leader
                resp = requests.post(f"http://{new_leader_addr}/kv/newkey", data="newvalue", timeout=5.0)
                self.assertEqual(resp.status_code, 200)

                # 4. Restart the old leader node
                print(f"Restarting old leader: {leader_id}")
                new_store = Storage(wal_paths[leader_id])
                stores[leader_id] = new_store

                srv_idx = node_ids.index(leader_id)
                restarted_server = Server(
                    leader_id,
                    grpc_addrs[srv_idx],
                    http_addrs[srv_idx],
                    get_peers_for(leader_id),
                    peer_https,
                    new_store,
                    logger
                )
                restarted_server.start()
                servers[leader_id] = restarted_server

                # Wait for catch up and synchronization
                time.sleep(4.0)

                # Verify restarted node caught up and has both keys
                val, ok = restarted_server.storage.get("testkey")
                self.assertTrue(ok)
                self.assertEqual(val, "testvalue")

                val, ok = restarted_server.storage.get("newkey")
                self.assertTrue(ok)
                self.assertEqual(val, "newvalue")

            finally:
                for srv in servers.values():
                    if srv:
                        srv.stop()
                for store in stores.values():
                    if store:
                        store.close()

    def test_split_vote(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            node_ids = ["node1", "node2", "node3"]
            grpc_addrs = ["localhost:50251", "localhost:50252", "localhost:50253"]
            http_addrs = ["localhost:8281", "localhost:8282", "localhost:8283"]

            peer_https = {
                "node1": "localhost:8281",
                "node2": "localhost:8282",
                "node3": "localhost:8283",
            }

            logger = JsonLogger(name="test_logger")

            stores = {}
            for nid in node_ids:
                path = os.path.join(tmp_dir, nid + ".wal")
                stores[nid] = Storage(path)

            def get_peers_for(id_):
                peers = {}
                for i, nid in enumerate(node_ids):
                    if nid == id_:
                        continue
                    peers[nid] = Peer(nid, grpc_addrs[i])
                return peers

            servers = {}
            for i, nid in enumerate(node_ids):
                srv = Server(nid, grpc_addrs[i], http_addrs[i], get_peers_for(nid), peer_https, stores[nid], logger)
                srv.start()
                servers[nid] = srv

            try:
                print("Forcing all nodes into a partitioned candidate state (split vote)...")
                for srv in servers.values():
                    srv.raft_node.set_partitioned(True)

                # Wait to trigger election timeouts in isolation
                time.sleep(1.5)

                # Verify that none is elected leader yet
                for nid in node_ids:
                    role = servers[nid].raft_node.get_role()
                    self.assertNotEqual(role, "Leader", f"node {nid} became leader while partitioned")

                print("Restoring network connectivity to resolve split vote...")
                for srv in servers.values():
                    srv.raft_node.set_partitioned(False)

                print("Waiting for election to converge...")
                time.sleep(4.0)

                leader_count = 0
                leader_id = ""
                for nid in node_ids:
                    role = servers[nid].raft_node.get_role()
                    if role == "Leader":
                        leader_count += 1
                        leader_id = nid

                self.assertEqual(leader_count, 1, f"expected exactly 1 leader after split vote convergence, got {leader_count}")
                print(f"Elected leader successfully after split vote convergence: {leader_id}")

            finally:
                for srv in servers.values():
                    if srv:
                        srv.stop()
                for store in stores.values():
                    if store:
                        store.close()

if __name__ == "__main__":
    unittest.main()
