#!/bin/bash

export PATH="/c/Program Files/Go/bin:$PATH"
# Cleanup function to kill all nodes
cleanup() {
  echo "Stopping all nodes..."
  for port in 8081 8082 8083; do
    if command -v netstat &> /dev/null; then
      PID=$(netstat -ano | grep LISTENING | grep ":$port" | awk '{print $NF}' | head -n 1 | tr -d '\r')
      if [ -n "$PID" ]; then
        taskkill //F //PID $PID &> /dev/null
      fi
    elif command -v lsof &> /dev/null; then
      PID=$(lsof -t -i:$port)
      if [ -n "$PID" ]; then
        kill -9 $PID &> /dev/null
      fi
    fi
  done
  # Clean up WAL files
  rm -f node1.wal node2.wal node3.wal
}

# Trap cleanup on exit
trap cleanup EXIT

echo "=== Building kvnode and kvctl ==="
go build -o kvnode ./cmd/kvnode
go build -o kvctl ./cmd/kvctl

echo "=== Starting 3-node Raft cluster ==="
# Clean up old WALs
rm -f node1.wal node2.wal node3.wal

# Start nodes
./kvnode --id=node1 --grpc-addr=localhost:50051 --http-addr=localhost:8081 --peers=node2=localhost:50052,node3=localhost:50053 --peer-https=node2=localhost:8082,node3=localhost:8083 --wal-path=node1.wal > node1.log 2>&1 &
./kvnode --id=node2 --grpc-addr=localhost:50052 --http-addr=localhost:8082 --peers=node1=localhost:50051,node3=localhost:50053 --peer-https=node1=localhost:8081,node3=localhost:8083 --wal-path=node2.wal > node2.log 2>&1 &
./kvnode --id=node3 --grpc-addr=localhost:50053 --http-addr=localhost:8083 --peers=node1=localhost:50051,node2=localhost:50052 --peer-https=node1=localhost:8081,node2=localhost:8082 --wal-path=node3.wal > node3.log 2>&1 &

echo "Waiting for cluster to initialize and elect leader..."
sleep 3

echo "=== Cluster Status ==="
curl -s http://localhost:8081/status
echo ""
curl -s http://localhost:8082/status
echo ""
curl -s http://localhost:8083/status
echo ""

echo "=== Writing keys to the cluster ==="
# Perform writes. Any node should redirect to leader automatically.
./kvctl --addr localhost:8081 PUT name SreeRam
./kvctl --addr localhost:8081 PUT role Engineer
./kvctl --addr localhost:8081 PUT status Awesome

echo "=== Verification of Writes (GET) ==="
./kvctl --addr localhost:8081 GET name
./kvctl --addr localhost:8082 GET role
./kvctl --addr localhost:8083 GET status

# Find leader before killing
LEADER_PORT=""
LEADER_ID=""
for port in 8081 8082 8083; do
  resp=$(curl -s http://localhost:$port/status)
  if echo "$resp" | grep -q '"role":"Leader"'; then
    LEADER_PORT=$port
    LEADER_ID=$(echo "$resp" | grep -o '"node_id":"[^"]*' | cut -d'"' -f4)
    break
  fi
done

echo "=== Killing Current Leader ($LEADER_ID on port $LEADER_PORT) ==="
./scripts/kill_leader.sh

echo "Waiting for a new leader to be elected (failover)..."
sleep 2

# Find new leader
NEW_LEADER_PORT=""
NEW_LEADER_ID=""
for port in 8081 8082 8083; do
  if [ "$port" = "$LEADER_PORT" ]; then
    continue
  fi
  resp=$(curl -s http://localhost:$port/status)
  if echo "$resp" | grep -q '"role":"Leader"'; then
    NEW_LEADER_PORT=$port
    NEW_LEADER_ID=$(echo "$resp" | grep -o '"node_id":"[^"]*' | cut -d'"' -f4)
    break
  fi
done

echo "New leader is: $NEW_LEADER_ID on port $NEW_LEADER_PORT"

echo "=== Writing more data to New Leader ==="
./kvctl --addr localhost:$NEW_LEADER_PORT PUT project RaftLite

echo "=== Restarting the old leader node ==="
if [ "$LEADER_PORT" = "8081" ]; then
  ./kvnode --id=node1 --grpc-addr=localhost:50051 --http-addr=localhost:8081 --peers=node2=localhost:50052,node3=localhost:50053 --peer-https=node2=localhost:8082,node3=localhost:8083 --wal-path=node1.wal > node1.log 2>&1 &
elif [ "$LEADER_PORT" = "8082" ]; then
  ./kvnode --id=node2 --grpc-addr=localhost:50052 --http-addr=localhost:8082 --peers=node1=localhost:50051,node3=localhost:50053 --peer-https=node1=localhost:8081,node3=localhost:8083 --wal-path=node2.wal > node2.log 2>&1 &
elif [ "$LEADER_PORT" = "8083" ]; then
  ./kvnode --id=node3 --grpc-addr=localhost:50053 --http-addr=localhost:8083 --peers=node1=localhost:50051,node2=localhost:50052 --peer-https=node1=localhost:8081,node2=localhost:8082 --wal-path=node3.wal > node3.log 2>&1 &
fi

echo "Waiting for restarted node to catch up via WAL replay and replication..."
sleep 3

echo "=== Verification of caught up state on restarted node ==="
./kvctl --addr localhost:$LEADER_PORT GET name
./kvctl --addr localhost:$LEADER_PORT GET project

echo "=== Final Status of Restarted Node ==="
curl -s http://localhost:$LEADER_PORT/status
echo ""

echo "=== Demo completed successfully! ==="
