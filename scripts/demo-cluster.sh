#!/bin/bash
set -e

if ! command -v mqd >/dev/null && [ ! -f ./bin/mqd ]; then
  echo "build bin/mqd first: go build -o bin/mqd ./cmd/mqd" >&2
  exit 1
fi
MQD_BIN="$(cd . && pwd)/bin/mqd"

token_raft="$1"; [ -z "$token_raft" ] && token_raft="demo-raft-token"
token_client="$2"; [ -z "$token_client" ] && token_client="demo-client-token"
base=/tmp/mq-demo
rm -rf "$base"*
for i in 0 1 2; do
  mkdir -p "$base-$i"
done
for i in 0 1 2; do
  port=$((5200 + i))
  nohup "$MQD_BIN" -role cluster -listen 127.0.0.1:$port -data "$base-$i" \
    -raft-peers http://127.0.0.1:5200,http://127.0.0.1:5201,http://127.0.0.1:5202 -raft-self $i \
    -raft-token "$token_raft" -client-token "$token_client" > "$base-$i.log" 2>&1 &
  echo "$!" > /tmp/mq-demo-$i.pid
done
sleep 2
LEADER_PORT=""
for i in 0 1 2; do
  port=$((5200 + i))
  role=$(curl -sS -H "X-Raft-Token: $token_raft" "http://127.0.0.1:$port/raft/role" | python3 -c 'import json,sys;print(json.load(sys.stdin)["role"])')
  if [ "$role" = "2" ]; then LEADER_PORT=$port; break; fi
done
echo "leader = $LEADER_PORT"
for i in 1 2 3 4 5; do
  resp=$(curl -sS -X POST "http://127.0.0.1:$LEADER_PORT/v1/publish-batch" -H 'Content-Type: application/json' -H "Authorization: Bearer $token_client" -d '{"topic":"events","messages":[{"key":"","value":"'$(printf 'batch-record-%d' "$i" | base64)'"}]}')
  echo "publish $i: $resp"
done
echo "fetch:"
curl -sS -X POST "http://127.0.0.1:$LEADER_PORT/v1/fetch" -H 'Content-Type: application/json' -H "Authorization: Bearer $token_client" -d '{"topic":"events","offset":0,"limit":10}'
echo
for i in 0 1 2; do
  kill $(cat /tmp/mq-demo-$i.pid)
  wait $(cat /tmp/mq-demo-$i.pid) 2>/dev/null || true
done
echo "done"
