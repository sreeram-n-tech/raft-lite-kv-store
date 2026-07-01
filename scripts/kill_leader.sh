#!/bin/bash

# Find which port (8081, 8082, 8083) has the leader
LEADER_PORT=""
LEADER_ID=""

for port in 8081 8082 8083; do
  resp=$(python.exe cmd/kvctl/main.py --addr localhost:$port STATUS 2>/dev/null)
  if echo "$resp" | grep -q '"role":"Leader"'; then
    LEADER_PORT=$port
    LEADER_ID=$(echo "$resp" | grep -o '"node_id":"[^"]*' | cut -d'"' -f4)
    break
  fi
done

if [ -z "$LEADER_PORT" ]; then
  echo "Error: No leader found!"
  exit 1
fi

echo "Found leader: $LEADER_ID on HTTP port $LEADER_PORT"

# Find PID of the process listening on LEADER_PORT
PID=""
if command -v netstat.exe &> /dev/null; then
  PID=$(netstat.exe -ano | grep LISTENING | grep ":$LEADER_PORT" | awk '{print $NF}' | head -n 1 | tr -d '\r')
elif command -v netstat &> /dev/null; then
  PID=$(netstat -ano | grep LISTENING | grep ":$LEADER_PORT" | awk '{print $NF}' | head -n 1 | tr -d '\r')
fi

if [ -z "$PID" ] && command -v lsof &> /dev/null; then
  PID=$(lsof -t -i:$LEADER_PORT)
fi

if [ -z "$PID" ]; then
  echo "Error: Could not find PID listening on port $LEADER_PORT"
  exit 1
fi

echo "Killing leader process (PID: $PID)..."
if command -v taskkill.exe &> /dev/null; then
  taskkill.exe /F /PID $PID
elif command -v taskkill &> /dev/null; then
  taskkill //F //PID $PID
else
  kill -9 $PID
fi

echo "Leader killed successfully!"
