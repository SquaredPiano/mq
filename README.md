# mq

A small, durable message broker written in Go. Published records live on disk in a write-ahead log. A single-node cluster survives a restart; a follower keeps a byte replica of the leader's log; a three-node cluster elects a leader and commits every write to a majority.

It speaks HTTP and JSON. It is not Kafka. It does what this README describes and nothing more.

## Run it

```sh
go run ./cmd/mqd -listen 127.0.0.1:8080
```

Data goes in `./mq-data`. Pass `-data` to change that. The server is plain HTTP on localhost; non-loopback listeners require TLS and a client token.

## Publish and read

Every call is `POST` with one JSON object. `key` and `value` are base64.

| Endpoint | What you send | What it does |
| --- | --- | --- |
| `/v1/publish` | `topic`, `key`, `value`, optional `request_id` | Append one record and return its `offset`. |
| `/v1/publish-batch` | `topic`, `messages`, optional `request_id` | Append up to 32 records in one fsync. |
| `/v1/fetch` | `topic`, `offset`, `limit` | Read records from an offset. |
| `/v1/groups/fetch` | `group`, `topic`, `limit` | Read records from the group's committed offset. |
| `/v1/groups/commit` | `group`, `topic`, `next_offset` | Commit the offset the group should read next. |
| `/v1/topics/retain` | `topic`, `start_offset` | Move a topic's start offset. Admin token required. |
| `/v1/snapshot` | empty body | Write a recovery snapshot. Admin token required. |

Request ids give at-least-once delivery: a retry with the same id returns the original offset. The same id with different bytes returns 409.

Messages are at most 256 KiB key+value. Batches are at most 32 records. Topic names, group names, and request ids are at most 255 bytes.

Consumer groups track their own offsets. If the process dies before a commit lands, the next fetch returns the record again — so delivery is at least once.

## Run a cluster

Standalone:

```sh
go run ./cmd/mqd -listen 127.0.0.1:8080
```

Primary-backup replication (follower first):

```sh
go run ./cmd/mqd -role follower -listen 127.0.0.1:8081 -data ./follower-data -replication-token 'replace-with-a-long-random-token'
go run ./cmd/mqd -role leader -listen 127.0.0.1:8080 -data ./leader-data -replica http://127.0.0.1:8081 -replication-token 'replace-with-a-long-random-token' -epoch 1
```

Three-node Raft cluster (any odd number of nodes works):

```sh
export MQ_RAFT_TOKEN='replace-with-a-long-random-token'
go run ./cmd/mqd -role cluster -listen 127.0.0.1:8081 -data ./node-1-data \
  -raft-peers http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083 -raft-self 0 -raft-token "$MQ_RAFT_TOKEN"
go run ./cmd/mqd -role cluster -listen 127.0.0.1:8082 -data ./node-2-data \
  -raft-peers http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083 -raft-self 1 -raft-token "$MQ_RAFT_TOKEN"
go run ./cmd/mqd -role cluster -listen 127.0.0.1:8083 -data ./node-3-data \
  -raft-peers http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083 -raft-self 2 -raft-token "$MQ_RAFT_TOKEN"
```

A client may publish or fetch from any node. Followers redirect to the current leader with HTTP 307. To find the leader:

```sh
curl -H "X-Raft-Token: $MQ_RAFT_TOKEN" http://127.0.0.1:8081/raft/role
```

Every write is acknowledged only after a majority has committed it.

## Limits

- At-least-once delivery; not exactly-once.
- Topics are single-partition. Consumer groups cannot share out work.
- Cluster membership is fixed at start; no member add/remove.
- Snapshot restore is full broker state; request-id state is preserved.
- Standalone (single-node) is lean and fast. Follower mode pairs durability with a minor latency cost; cluster mode gives automatic failover but is the slowest path — every write is a majority agreement across fsyncs.

## License

MIT. See [LICENSE](LICENSE).
