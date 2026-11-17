#!/bin/bash
set -e

if ! command -v python3 >/dev/null; then
  echo "python3 is required" >&2
  exit 1
fi
if ! command -v jq >/dev/null; then
  json='{"ok":true,"records":['"$(printf '')"']}'
  echo "jq is required for parsing /raft/role" >&2
  exit 1
fi

MQD_BIN="$(cd . && pwd)/bin/mqd"
if [ ! -x "$MQD_BIN" ]; then
  echo "build bin/mqd first: go build -o bin/mqd ./cmd/mqd" >&2
  exit 1
fi

token_raft="$1"; [ -z "$token_raft" ] && token_raft="demo-raft-token"
token_client="$2"; [ -z "$token_client" ] && token_client="demo-client-token"
base=/tmp/mq-demo
rm -rf "$base"*
for i in 0 1 2; do
  mkdir -p "$base-$i"
done
for i in 0 1 2; do
  port=$((5200 + i))
  "$MQD_BIN" -role cluster -listen 127.0.0.1:$port -data "$base-$i" \
    -raft-peers http://127.0.0.1:5200,http://127.0.0.1:5201,http://127.0.0.1:5202 -raft-self $i \
    -raft-token "$token_raft" -client-token "$token_client" > "$base-$i.log" 2>&1 &
  echo "$!" > /tmp/mq-demo-$i.pid
done
sleep 2

get_role() {
  local role
  role=$(curl -sS -m 2 -H "X-Raft-Token: $token_raft" "http://127.0.0.1:$1/raft/role" 2>/dev/null | python3 -c 'import json,sys
try: print(json.load(sys.stdin)["role"])
except Exception: print("")' 2>/dev/null) || true
  echo "$role"
}

query_leader() {
  local L
  for i in 0 1 2; do
    port=$((5200 + i))
    role=$(get_role "$port")
    if [ "$role" = "2" ]; then
      L=$port
      break
    fi
  done
  echo "$L"
}

LEADER_PORT=$(query_leader)
echo "first leader = $LEADER_PORT"

for rawkey in alpha bravo charlie delta; do
  body=$(python3 -c "import json,base64;print(json.dumps({'topic':'events','key':base64.b64encode('$rawkey'.encode()).decode(),'value':base64.b64encode(('hello-'+'$rawkey').encode()).decode()}))")
  curl -sS --location-trusted -L -X POST "http://127.0.0.1:$LEADER_PORT/v1/publish" -H 'Content-Type: application/json' -H "Authorization: Bearer $token_client" -d "$body"
  echo
done

echo "killing leader $LEADER_PORT"
kill "$(cat /tmp/mq-demo-$((LEADER_PORT - 5200)).pid)"
sleep 2

NEW_LEADER=$(query_leader)
echo "new leader = $NEW_LEADER"

echo "fetch after leader kill:"
curl -sS -X POST "http://127.0.0.1:$NEW_LEADER/v1/fetch" -H 'Content-Type: application/json' -H "Authorization: Bearer $token_client" -d '{"topic":"events","offset":0,"limit":10}'
echo
echo "join members to partitioned group:"
curl -sS -X POST "http://127.0.0.1:$NEW_LEADER/v1/groups/join" -H 'Content-Type: application/json' -H "Authorization: Bearer $token_client" -d '{"group":"workers","topic":"events"}'
echo
curl -sS -X POST "http://127.0.0.1:$NEW_LEADER/v1/groups/join" -H 'Content-Type: application/json' -H "Authorization: Bearer $token_client" -d '{"group":"workers","topic":"events"}'
echo
echo "fetch partitions [0,1]:"
curl -sS -X POST "http://127.0.0.1:$NEW_LEADER/v1/fetch" -H 'Content-Type: application/json' -H "Authorization: Bearer $token_client" -d '{"topic":"events","offset":0,"limit":10,"partitions":[0,1]}'
echo

for i in 0 1 2; do
  kill "$(cat /tmp/mq-demo-$i.pid)" 2>/dev/null || true
done
wait
echo "done"
