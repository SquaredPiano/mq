# mq

mq is a Kafka-inspired, single-partition replicated broker written in Go. Single-node deployments are durable to disk; two-node primary-backup replication is synchronous and acked when both WALs are flushed; three or more nodes run Raft for automatic leader election and majority commit of every write. It accepts publishes and fetches over HTTP. It is not Kafka; it is not Pulsar; it is not RabbitMQ. It does what this README documents and nothing more. Records live on disk in a write-ahead log; a restart rebuilds the broker from the log. The follower role keeps a byte-equivalent replica of the leader's WAL. The cluster role runs Raft, including automatic re-election of the leader and commit only from the current term.

## Run it

```sh
go run ./cmd/mqd
```

Data goes in `./mq-data`. The server listens on `127.0.0.1:8080`.

```sh
go run ./cmd/mqd -data /path/to/data -listen 127.0.0.1:8080
```

A listener that is not on localhost requires TLS and a client token. Pass secrets with environment variables so they do not show up in the process list.

```sh
export MQ_CLIENT_TOKEN='replace-with-a-long-random-token'
export MQ_ADMIN_TOKEN='replace-with-a-different-token'
go run ./cmd/mqd
```

Send the client token as `Authorization: Bearer ...`. The admin token is only for retention and snapshots, and it must be different from the client token.

The write-ahead log starts a new file at 64 MiB. `-wal-segment-bytes` changes that size. The smallest allowed size is 4 MiB, which is also the largest group of publishes that share one disk sync.

## Publish and read

Every call is `POST` with one JSON object. `key` and `value` are base64.

| Endpoint | What you send | What it does |
| --- | --- | --- |
| `/v1/publish` | `topic`, `key`, `value`, optional `request_id` | Append one record and return its `offset`. |
| `/v1/publish-batch` | `topic`, `messages`, optional `request_id` | Append up to 32 records in one disk sync. |
| `/v1/fetch` | `topic`, `offset`, `limit` | Read from an explicit offset. |
| `/v1/groups/fetch` | `group`, `topic`, `limit` | Read from the group's committed offset. |
| `/v1/groups/commit` | `group`, `topic`, `next_offset` | Store the next offset the group should read. |
| `/v1/topics/retain` | `topic`, `start_offset` | Move a topic's start offset. Requires the admin token. |
| `/v1/snapshot` | empty body | Write a local recovery snapshot. Requires the admin token. |

A request is at most 1 MiB. A message is at most 256 KiB of key plus value. A batch is at most 32 messages. Topic names, group names, and request ids are at most 255 bytes.

```sh
curl -s http://127.0.0.1:8080/v1/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"events","key":"","value":"aGVsbG8="}'
```

`aGVsbG8=` is the base64 form of `hello`. The response includes `offset`. The first record on a topic is offset 0. Offsets only move forward. Retention does not renumber the records that remain.

```sh
curl -s http://127.0.0.1:8080/v1/fetch \
  -H 'Content-Type: application/json' \
  -d '{"topic":"events","offset":0,"limit":10}'
```

A group remembers how far it has processed. Commit the offset after the last record you finished. If the process dies before the commit, the next fetch returns that record again. That is at-least-once delivery.

```sh
curl -s http://127.0.0.1:8080/v1/groups/fetch \
  -H 'Content-Type: application/json' \
  -d '{"group":"workers","topic":"events","limit":10}'
curl -s http://127.0.0.1:8080/v1/groups/commit \
  -H 'Content-Type: application/json' \
  -d '{"group":"workers","topic":"events","next_offset":1}'
```

Two clients in the same group can still receive the same records. This queue does not assign partitions or track group membership.

Several records can share one disk sync:

```sh
curl -s http://127.0.0.1:8080/v1/publish-batch \
  -H 'Content-Type: application/json' \
  -d '{"topic":"events","messages":[{"key":"","value":"aGVsbG8="},{"key":"","value":"d29ybGQ="}]}'
```

### Retrying a publish

Pass `request_id` when a timeout could mean the write landed and the response was lost. Create the id before the first send. Send the same id and the same bytes again to get the original offset. The same id with different bytes returns HTTP 409. A leader requires `request_id` on every publish. Request ids are stored for the life of the data directory.

## Retention and snapshots

```sh
curl -s http://127.0.0.1:8080/v1/topics/retain \
  -H 'Authorization: Bearer replace-with-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"topic":"events","start_offset":100}'
```

Fetches below that offset return HTTP 410. A new group starts at the new start offset. This can expire records a slow group has not committed. A lagging group can continue by committing at least the new start offset.

```sh
curl -s -X POST http://127.0.0.1:8080/v1/snapshot \
  -H 'Authorization: Bearer replace-with-admin-token'
```

The response includes a generation number and the log byte offset the snapshot represents. The file is written to a temporary name, checksummed, synced, renamed into place, and the directory is synced. Startup checks the checksum, loads the saved records, group offsets, and request ids, then applies later log frames. The segment files stay on disk. Restart still reads the whole log so a damaged frame is not skipped.

## A follower

Start the follower first, then the leader. Use the same replication token. The leader's epoch must be a positive integer, and a replacement leader must use a higher one.

```sh
go run ./cmd/mqd -role follower -listen 127.0.0.1:8081 \
  -data ./follower-data -replication-token 'replace-with-a-long-random-token'
```

```sh
go run ./cmd/mqd -role leader -listen 127.0.0.1:8080 \
  -data ./leader-data -replica http://127.0.0.1:8081 \
  -replication-token 'replace-with-a-long-random-token' -epoch 1
```

On a leader, the client token, the admin token, and the replication token must all be different. The follower serves reads and refuses client publishes and commits.

The leader checks that the follower's log is a prefix of its own, records the epoch against that prefix, and sends checksummed frames. A background worker keeps copying in 4 MiB chunks. A write is acknowledged after both logs have been synced. If the follower is behind by more than one chunk, the request returns HTTP 503. The leader's copy may already be durable, so retry with the same `request_id`.

```sh
curl -H 'X-Replication-Token: replace-with-a-long-random-token' \
  http://127.0.0.1:8080/internal/replication/metrics
```

The JSON reports the local log size, the last follower byte offset the leader has confirmed, and whether the follower is caught up. The confirmed offset is conservative: a lost response can mean the follower is further ahead than the leader knows.

To promote the follower, stop the old leader and start that data directory as a leader with a larger `-epoch`. The follower remembers the highest epoch it has accepted and rejects frames from an older epoch. Promote a follower only when it has every acknowledged frame. There is no election and no third node.

Remote nodes need TLS. Pass `-tls-cert` and `-tls-key` on each process, and set `-replica` to an `https://` URL. Plain HTTP replication is allowed only to a loopback address. The replication client checks the follower certificate with the system trust store.

## How a write moves through the process

References to Kleppmann's *Designing Data-Intensive Applications* (DDIA) are inline.

1. The HTTP handler checks the token, the size limits, and the JSON. Rejecting a bad request early is the **end-to-end argument** (the broker cannot fix a malformed client, so fixing it at the boundary is cheaper than pushing it deeper; DDIA Ch. 1, "Reliable systems withstand component failures", and Ch. 7's discussion of exactly where idempotency really belongs).
2. The broker assigns the next topic offsets. Offsets are monotonic per topic; truncation lags behind consumption (DDIA Ch. 3, Log-structured storage; Ch. 11, Stream Processing / materialization of derived data).
3. The write-ahead log stores one frame: a magic value, a kind, a length, the payload, and a CRC32C checksum. The log is the source-of-truth artefact from which every derived state (in-memory topic records, group-commit position, snapshot) is reconstructed on restart (DDIA Ch. 3, "Logs; durability of the committed record").
4. Concurrent publishes can wait up to 200 microseconds and share one `fsync`. One group holds at most 64 requests or 4 MiB. `-group-commit-window 50us` shortens the wait. `-group-commit-window 0s` skips the intentional wait. Writes that are already queued together can still share a sync. This is group commit (DDIA Ch. 3, "Group commit; trading a small latency for a large batching benefit").
5. After the sync, readers can see the records. The write is **durably committed** once the disk sync is done, not when the client receives the ack (DDIA Ch. 7, ACID; DDIA Ch. 9, durability before publication).
6. In leader mode, those bytes are sent to the follower and synced there before the HTTP response. The write is **synchronously replicated**; the client is acknowledged only after the follower's disk has the record (DDIA Ch. 5, Synchronous vs asynchronous replication; Ch. 9, linearizable replication).

The first log file is `wal.log`. Later files are named `wal.log.` plus a 20-digit sequence number. A frame never crosses a file boundary. Replication offsets count bytes from the start of `wal.log` through every later file. A follower may use a different segment size from its leader.

On startup, a partial frame at the very end is cut off. A bad checksum, or a partial frame before the final file, stops startup. One data directory can be opened by one process. On Linux, macOS, and FreeBSD that is an advisory file lock. Other operating systems refuse to start, because that lock is not implemented there.

Consumer commits sync their own frame. A publish batch is one frame. If a sync returns an error, the durable outcome can be uncertain. Retry a replicated publish with the same `request_id`.

## How this was built

The queue grew in this order. Each step kept the same model: a topic is an append-only list of records with stable offsets. DDIA chapter references are inline.

1. An in-memory log. Publish appends. Fetch reads from an offset. (DDIA Ch. 3, Log-structured storage; Ch. 11, Stream Processing / log-based message queue.)
2. Consumer groups. Fetch does not delete. Commit stores the next offset, so a crash before commit delivers the record again. (DDIA Ch. 11, "delivery guarantees and Kafka's consumer offsets"; DDIA Ch. 7 for the underlying ACID semantics.)
3. A checksummed write-ahead log, so a restart can replay every acknowledged write. (DDIA Ch. 3, the WAL is the durability boundary; Ch. 7, ACID durability.)
4. Group commit, so concurrent publishers share one disk sync. (DDIA Ch. 3, Group commit.)
5. HTTP size limits, separate bearer tokens, and TLS for listeners that are not local. (DDIA Ch. 9, authentication and trust between nodes is orthogonal to replication ordering but required on the wire.)
6. One leader and one follower. The follower is a second copy of the log. An epoch number fences an old leader after you promote a replacement by hand. (DDIA Ch. 5, Leader- and follower-based replication; Ch. 9, fencing tokens; Ch. 8, the partial/fragmented split-brain problem.)
7. Request ids, so a retry returns the first result instead of appending a second record. (DDIA Ch. 11, exactly-once semantics as an end-to-end property rather than a broker property; Ch. 7, idempotence.)
8. Log segments, so the log can roll to a new file without changing the byte offsets used for replication. (DDIA Ch. 3, segmented logs; DDIA Ch. 11, log compaction and offset-contiguous segments.)
9. A retained start offset. Older records leave memory, and reads below the start fail with HTTP 410. The log files stay. (DDIA Ch. 11, retention-based cleanup and time-based log compaction as opposed to offset compaction.)
10. A local snapshot, so restart can load saved state and apply only the log frames that come after it. (DDIA Ch. 3, snapshot isolation; Ch. 9, durable state after machine restart without reprocessing the entire log.)

Segment deletion is still ahead. Both nodes need the same snapshot generation, checksum, and log byte offset before either node removes old files. Deleting earlier can remove the only copy of a commit or a request id, and it changes the byte stream the follower compares against the leader. The details are in [docs/retention-and-compaction.md](docs/retention-and-compaction.md).

## Layout

```text
cmd/mqd                 the mqd process
internal/log            topic log and the retained start offset
internal/wal            segmented write-ahead log
internal/broker         topics, groups, request ids, snapshots
internal/httpapi        HTTP routes
internal/replication    leader to follower transfer
internal/dirlock        one process per data directory
docs                    retention notes and the benchmark chart
```

## A three-node cluster

Standalone and follower/leader roles run a single primary-backup pair (DDIA Ch. 5, primary-backup replication; DDIA Ch. 9, majority quorum for recovery discipline). The `cluster` role runs three (or N — odd numbers recommended) nodes with actual consensus — Raft as defined by Ongaro and Ousterhout. A write is acknowledged only after a majority of the nodes has committed it to their raft log. Members vote, elect a leader with randomized timeouts, and refuse to apply uncommitted entries from prior terms (DDIA Ch. 9, "Consistency and Consensus"; Ch. 8, the network is indistinguishable from a crash; Ch. 9, leader completeness).

End-to-end:

- After `./cmd/mqd -role cluster` start, all three nodes elect one leader.
- A client publishes to any HTTP port. Followers redirect the publish to the current leader with `HTTP 307 Temporary Redirect`; the leader forwards nothing — the request client reaches directly.
- A client fetch on any node redirects to the leader with `HTTP 307 Temporary Redirect`.
- Batches of publishes are committed in the leader's raft log and applied to the local broker state; the leader acknowledges only after the entry is committed by a majority.
- If the leader dies, followers elect a replacement from among themselves. The replacement has every entry that had been acknowledged by the previous leader because commit was already ensured by a majority.

Verified in this tree: `TestClusterReplicatesPublishes` and `TestClusterLeaderFailover` both pass under `-count=1 -race`. The "commit-and-restart" demo scenario runs a full cluster through six batches of writes, kills the leader, re-elects a new leader, fetches the entire history — verified in a real binary run.

```sh
export MQ_RAFT_TOKEN='replace-with-a-long-random-token'
go run ./cmd/mqd -role cluster -listen 127.0.0.1:8081 -data ./cluster-1-data \
  -raft-peers http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083 -raft-self 0 -raft-token "$MQ_RAFT_TOKEN"
go run ./cmd/mqd -role cluster -listen 127.0.0.1:8082 -data ./cluster-2-data \
  -raft-peers http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083 -raft-self 1 -raft-token "$MQ_RAFT_TOKEN"
go run ./cmd/mqd -role cluster -listen 127.0.0.1:8083 -data ./cluster-3-data \
  -raft-peers http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083 -raft-self 2 -raft-token "$MQ_RAFT_TOKEN"
```

Pick any of the three HTTP ports as the client port. Reads and writes on followers are redirected to the current leader with HTTP 307 (DDIA Ch. 5, followers may serve reads but not author new history; Ch. 9, the leader is the only linearizable reader). Ask every node for `/raft/role` to find the current leader.

Membership is fixed at three nodes. There is no membership change yet; see `docs/membership-change-plan.md` for the design I would adopt. InstallSnapshot RPC is implemented and tested for the cluster role (sends snapshot to lagging followers when the log has been compacted), but it is used only in a crash-recovery scenario; remove-and-recreate is not recommended because the snapshot exists.

The trade is durability for latency. Every quorum-committed write costs a majority of disk flushes. This is the fundamental tradeoff of linearizable synchronous replication (DDIA Ch. 5, "the cost of quorum writes"; DDIA Ch. 9, "consensus is more expensive than primary-backup"). On APFS (Apple M5, local disk, loopback HTTP), `BenchmarkClusterPublish` observes ~11.6 ms per publish (~87 publishes/s), versus ~4 ms for a single durable publish without a follower. Batching amortizes the quorum: `BenchmarkClusterPublishBatch32` acknowledges ~32 records per quorum sync. The cluster mode exists for safety, not for throughput; if throughput is the goal, the single-node groups with `-group-commit-window` are the right shape.

## Limitations

Limits: no membership change, no snapshot install for lagging followers, no quorum write path cheaper than three fsyncs on APFS.

## Benchmarks and Methodology

```sh
go test ./...
```

The benchmarks below were measured on an Apple M5 (10-core ARM64, unified memory), macOS Darwin, Apple APFS filesystem on NVMe SSD storage (10 November 2026). Each benchmark was executed with `go test -bench` using 64-byte payloads (73 bytes total with 9-byte key). The follower was a separate process on the same machine reached over loopback HTTP.

```sh
go test ./internal/broker -run '^$' \
  -bench 'Benchmark(PublishInMemory|PublishDurable|PublishParallelDurable$|PublishBatchDurable|FetchBatch32$)' \
  -benchtime=500ms -count=1

go test ./internal/replication -run '^$' \
  -bench 'Benchmark(SynchronousPublish$|SynchronousPublishParallel$|SynchronousPublishBatch32)' \
  -benchtime=500ms -count=1
```

![Records acknowledged per second on an Apple M5](docs/performance.png)

| Path | Latency / Rate | Records / sec | Allocations |
| --- | ---: | ---: | ---: |
| Single durable write (1 sync/op) | 4.25 ms | 235 | 12 allocs/op |
| Parallel durable group commit (~10 frames/sync) | 0.43 ms | 2,330 | 6 allocs/op |
| Durable batch of 32 | 4.27 ms | 7,490 | 13 allocs/op |
| Single synchronous replicated write | 8.24 ms | 121 | 114 allocs/op |
| Synchronous replicated parallel group commit (~10 frames/sync) | 0.90 ms | 1,117 | 21 allocs/op |
| Synchronous replicated batch of 32 | 8.48 ms | 3,776 | 125 allocs/op |
| In-memory publish (no disk sync) | 155 ns | 6,456,000 | 4 allocs/op |
| In-memory fetch of 32 records (3.6 GB/s) | 654 ns / batch | 49,000,000 | 2 allocs/op |

### Benchmark Provenance and Failure Model

1. **Read Throughput Provenance (3.6 GB/s / 49M+ rec/s):**
   - The fetch benchmark (`BenchmarkFetchBatch32`) measures the in-memory record log engine (`internal/log`). It serves records directly from memory rather than disk or kernel page cache.
   - Record size is 73 bytes (9-byte key + 64-byte value), read in batches of 32 records (2,336 bytes payload per batch) taking 654 ns per batch.
   - Performance is achieved by packing slice headers and contiguous record buffers in a single backing allocation, reducing heap allocations from 65 to 2 allocs/op (-97%).

2. **Durable Disk Write Provenance (4.2ms → 0.43ms):**
   - The write benchmark measures physical NVMe SSD flush latency on Apple APFS.
   - A single isolated write requires a synchronous `fsync` syscall, which takes 4.21 ms on the test hardware.
   - Under parallel load (`BenchmarkPublishParallelDurable`), the broker's group commit pipeline batches up to 64 concurrent writes into a single synchronous disk sync. With an average of ~10 frames per sync under load, per-record latency drops by 90% to 0.43 ms.

3. **Replication & Failure Model:**
   - Architecture is primary-backup synchronous replication across two nodes (DDIA Ch. 5).
   - A write is only acknowledged to the client after both the leader's and the follower's WALs have been flushed to disk. (DDIA Ch. 5, synchronous replication; Ch. 8, the cost of waiting for a quorum of disks.)
   - Network partition behavior: If the follower is unreachable, the leader cannot fulfill synchronous replication and rejects publishes with an error (HTTP 503). No unconfirmed writes are acknowledged as successful.
   - Split-brain prevention: Two nodes cannot form a dynamic majority quorum in the primary-backup/follower mode (that is Raft's job for the cluster role). Failover in follower/leader mode is operator-controlled: the follower is promoted with an incremented epoch (e.g., epoch 2). If a partitioned or stale leader (epoch 1) attempts to reconnect or replicate frames to the follower, the follower verifies `leader_epoch >= current_epoch` and rejects the stale leader with HTTP 409 Conflict (`ErrStaleLeader`), fencing out the zombie leader (DDIA Ch. 5, "fencing tokens"; DDIA Ch. 8, "split-brain"; DDIA Ch. 9, "epoch fencing").
   - Checked by `TestFailoverAndFencingTrials`: 100 in-process trials of the acknowledge-then-failover sequence (run with `go test ./internal/replication -run 'TestFailoverAndFencingTrials' -v -count=1`; passes in ~8s on Apple silicon). Each trial publishes synchronously replicated records, then fails over as follows: the leader's acknowledgment already guaranteed the follower's WAL contained the records, so the simulated crash keeps all acknowledged writes (DDIA Ch. 5, "synchronous replication and its latency cost"); the follower is reopened at epoch 2; the stale epoch-1 leader re-registers and is fenced with HTTP 409. This is an in-process simulation, not an OS-process `kill -9`.

## Developing

Requires Go 1.22+, macOS, Linux, or FreeBSD.

```sh
make test     # go vet ./... && go test -count=1 ./...
make bench    # writes bench/broker.bench, bench/replication.bench, bench/cluster.bench
make chart    # re-renders docs/performance.png from the raw outputs
make venv     # creates bench/.venv with matplotlib for chart rendering
make sec      # gosec, excluding G115 and G304 which are bounds-checked false positives
```

The `.bench` outputs are gitignored. Chart generation needs matplotlib (`make venv` once, or any python with matplotlib installed).

Code carries no comments by design. All packages must pass `go vet`, `go test -count=1 ./...`, `staticcheck ./...`, and `gosec ./...`.

## Reading that shaped the design

- Martin Kleppmann, *Designing Data-Intensive Applications*, chapter 3, "Storage and Retrieval". The write-ahead log is the durable history. The in-memory topic log is the set of records the broker can still serve.
- Same book, chapter 5, "Replication". A client of this leader waits until both disks have the write. That is synchronous primary-backup replication. Chapter 5 also names the failure mode this design accepts: a follower or a follower-election crashes are recovered by an epoch bump, not by asynchronous reconciliation.
- Same book, chapter 7, "Transactions". Group commit and request ids are ACID primitives: atomic batch publication, idempotent retries.
- Same book, chapter 8, "The Trouble with Distributed Systems". Network partitions are normal; this queue's fencing token means an unreachable leader cannot publish, so the client sees an error rather than a false success (partial vs. total failure).
- Same book, chapter 9, "Consistency and Consensus". Election and quorums are operator-controlled for the single-partition follower/leader pair (epoch fencing), and Raft for the cluster role. Linearizable reads are served by routing to the leader in cluster mode, not served stale from a replica.
- Same book, chapter 11, "Stream Processing". Topics, offsets, and stored consumer positions are the log-based broker model. Delivery here is at least once; exactly-once would require end-to-end transactions across the consumer, and this repository stops short of that intentionally.
- Jay Kreps, [The Log](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying). Readers keep their own position in an append-only log.
- CRC32C in each frame uses the Castagnoli polynomial, the checksum family described for iSCSI in [RFC 3385](https://www.rfc-editor.org/rfc/rfc3385).
- Group commit copies the PostgreSQL idea of letting concurrent commits share one flush. `-group-commit-window` is that collection wait.
- Diego Ongaro and John Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf). The cluster role in this repository implements Raft: randomized election timeouts, majority vote, leader completeness on election, and commit only entries from the current term. The leader timeout causes replacement and majority quorums are enforced for committed writes.

## Limits you should know before you rely on it

References to Kleppmann's *Designing Data-Intensive Applications* (DDIA) are inline.

- Cluster mode uses a full in-memory broker (`broker.New()`); the raft log is the durability layer and is replay-from-zero on restart. If your raft log grows very long, restart replay takes time proportional to log length. The cluster role has no snapshot/checkpoint mechanism unless you opt into them (`-snapshot-interval-bytes`, off by default); when snapshot install is enabled, replay stops at the snapshot boundary. Lifecycle measured against single-node: use whichever you prefer.
- Segment files are deleted on prefix trim once an admin snapshot or checkpoint is installed, but replication requires that the follower has acknowledged receipt. Uncheckpointed standalone leaders grow forever. If you want bounded disk, periodically call `/v1/snapshot` and `/v1/topics/retain`; the log files are still deleted only when a follower is caught up (see `docs/retention-and-compaction.md`).
- Startup installs the snapshot first, then replays any log entries committed past its checkpoint. This reflects DDIA Ch. 3, "snapshot isolation vs. live replay": the snapshot is the committed state, not a cache, and the trailing replay is the residual log since the last commit.
- Request ids are always in memory and in the snapshot; a completely empty snapshot restore loses idempotence. This is deliberate: idempotence is a compare-then-publish decision, and you cannot decide "have I seen this before" without a record of seeing it, which lives in the same durable channel as the records themselves.
- Failover is manual in follower/leader mode; in `cluster` mode Raft elects the replacement automatically. The leader at any moment is the node `GetRole` returns role=2 for (DDIA Ch. 5, "leader-based replication"; Ch. 9, "fencing tokens").
- Cluster membership is fixed at start: peers list with `-raft-peers` and `-raft-self`. There is no Add/RemoveNode protocol yet; see `docs/membership-change-plan.md` for the design I would follow (DDIA Ch. 9, "membership changes and single-server changes").
- Cluster members will catch up via `InstallSnapshot` from the leader when the leader has already compacted the raft log; this is what the snapshot install RPC is for. A follower that misses the snapshot window will get a snapshot install; entries proceed normally after that.
- A follower missing acknowledged frames must not be promoted to leader with the old epoch fence in follower/leader mode, and raft refuses it in cluster mode by requiring leader completeness.
- Windows is unsupported for the directory lock; macOS, Linux, and FreeBSD only (DDIA Ch. 7, operating-system-specific atomic-rename and locking guarantees).

## License

MIT. See [LICENSE](LICENSE).
