# mq

mq is a small durable message queue written in Go. One process accepts publishes and fetches over HTTP. Records stay on disk in a write-ahead log, and a restart rebuilds the queue from that log. You can add one follower. The leader then waits until the follower has stored a write before it tells the client the write succeeded.

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

1. The HTTP handler checks the token, the size limits, and the JSON.
2. The broker assigns the next topic offsets.
3. The write-ahead log stores one frame: a magic value, a kind, a length, the payload, and a CRC32C checksum.
4. Concurrent publishes can wait up to 200 microseconds and share one `fsync`. One group holds at most 64 requests or 4 MiB. `-group-commit-window 50us` shortens the wait. `-group-commit-window 0s` skips the intentional wait. Writes that are already queued together can still share a sync.
5. After the sync, readers can see the records.
6. In leader mode, those bytes are sent to the follower and synced there before the HTTP response.

The first log file is `wal.log`. Later files are named `wal.log.` plus a 20-digit sequence number. A frame never crosses a file boundary. Replication offsets count bytes from the start of `wal.log` through every later file. A follower may use a different segment size from its leader.

On startup, a partial frame at the very end is cut off. A bad checksum, or a partial frame before the final file, stops startup. One data directory can be opened by one process. On Linux, macOS, and FreeBSD that is an advisory file lock. Other operating systems refuse to start, because that lock is not implemented there.

Consumer commits sync their own frame. A publish batch is one frame. If a sync returns an error, the durable outcome can be uncertain. Retry a replicated publish with the same `request_id`.

## How this was built

The queue grew in this order. Each step kept the same model: a topic is an append-only list of records with stable offsets.

1. An in-memory log. Publish appends. Fetch reads from an offset.
2. Consumer groups. Fetch does not delete. Commit stores the next offset, so a crash before commit delivers the record again.
3. A checksummed write-ahead log, so a restart can replay every acknowledged write.
4. Group commit, so concurrent publishers share one disk sync.
5. HTTP size limits, separate bearer tokens, and TLS for listeners that are not local.
6. One leader and one follower. The follower is a second copy of the log. An epoch number fences an old leader after you promote a replacement by hand.
7. Request ids, so a retry returns the first result instead of appending a second record.
8. Log segments, so the log can roll to a new file without changing the byte offsets used for replication.
9. A retained start offset. Older records leave memory, and reads below the start fail with HTTP 410. The log files stay.
10. A local snapshot, so restart can load saved state and apply only the log frames that come after it.

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
| Single durable write (1 sync/op) | 4.11 ms | 243 | 12 allocs/op |
| Parallel durable group commit (~10 frames/sync) | 0.42 ms | 2,382 | 6 allocs/op |
| Durable batch of 32 | 4.01 ms | 7,972 | 13 allocs/op |
| Single synchronous replicated write | 8.04 ms | 124 | 114 allocs/op |
| Synchronous replicated parallel group commit | 0.86 ms | 1,161 | 22 allocs/op |
| Synchronous replicated batch of 32 | 8.16 ms | 3,921 | 125 allocs/op |
| In-memory publish (no disk sync) | 162 ns | 6,177,000 | 3 allocs/op |
| In-memory fetch of 32 records (3.6 GB/s) | 642 ns / batch | 49,844,000 | 2 allocs/op |

### Benchmark Provenance and Failure Model

1. **Read Throughput Provenance (3.6 GB/s / 49.8M+ rec/s):**
   - The fetch benchmark (`BenchmarkFetchBatch32`) measures the in-memory record log engine (`internal/log`). It serves records directly from memory rather than disk or kernel page cache.
   - Record size is 73 bytes (9-byte key + 64-byte value), read in batches of 32 records (2,336 bytes payload per batch) taking 642 ns per batch.
   - Performance is achieved by packing slice headers and contiguous record buffers in a single backing allocation, reducing heap allocations from 65 to 2 allocs/op (-97%).

2. **Durable Disk Write Provenance (4.2ms → 0.43ms):**
   - The write benchmark measures physical NVMe SSD flush latency on Apple APFS.
   - A single isolated write requires a synchronous `fsync` syscall, which takes 4.21 ms on the test hardware.
   - Under parallel load (`BenchmarkPublishParallelDurable`), the broker's group commit pipeline batches up to 64 concurrent writes into a single synchronous disk sync. With an average of ~10 frames per sync under load, per-record latency drops by 90% to 0.43 ms.

3. **Replication & Failure Model:**
   - Architecture is primary-backup synchronous replication across two nodes.
   - A write is only acknowledged to the client after both the leader's and the follower's WALs have been flushed to disk.
   - Network partition behavior: If the follower is unreachable, the leader cannot fulfill synchronous replication and rejects publishes with an error (HTTP 503). No unconfirmed writes are acknowledged as successful.
   - Split-brain prevention: Two nodes cannot form a dynamic majority quorum (Raft/Paxos). Failover is operator-controlled: the follower is promoted with an incremented epoch (e.g., epoch 2). If a partitioned or stale leader (epoch 1) attempts to reconnect or replicate frames to the follower, the follower verifies `leader_epoch >= current_epoch` and rejects the stale leader with HTTP 409 Conflict (`ErrStaleLeader`), fencing out the zombie leader.
   - Checked by `TestFailoverAndFencingTrials`: 100 in-process trials of the acknowledge-then-failover sequence (run with `go test ./internal/replication -run 'TestFailoverAndFencingTrials' -v -count=1`; passes in ~8s on Apple silicon). Each trial publishes synchronously replicated records, then fails over as follows: the leader's acknowledgment already guaranteed the follower's WAL contained the records, so the simulated crash keeps all acknowledged writes; the follower is reopened at epoch 2; the stale epoch-1 leader re-registers and is fenced with HTTP 409. This is an in-process simulation, not an OS-process `kill -9`.

## Reading that shaped the design

- Martin Kleppmann, *Designing Data-Intensive Applications*, chapter 3, "Storage and Retrieval". The write-ahead log is the durable history. The in-memory topic log is the set of records the broker can still serve.
- Same book, chapter 5, "Replication". A client of this leader waits until both disks have the write. That is synchronous primary-backup replication.
- Same book, chapter 11, "Stream Processing". Topics, offsets, and stored consumer positions are the log-based broker model. Delivery here is at least once.
- Same book, chapter 9, "Consistency and Consensus". Election and quorums are left to the operator. The epoch fences one follower. It does not vote.
- Jay Kreps, [The Log](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying). Readers keep their own position in an append-only log.
- CRC32C in each frame uses the Castagnoli polynomial, the checksum family described for iSCSI in [RFC 3385](https://www.rfc-editor.org/rfc/rfc3385).
- Group commit copies the PostgreSQL idea of letting concurrent commits share one flush. `-group-commit-window` is that collection wait.
- Diego Ongaro and John Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf). Raft is the background for a later quorum design. This repository does not implement Raft.

## Limits you should know before you rely on it

- Segment files are never deleted, so disk use grows with every acknowledged write.
- Startup reads the whole log even when a snapshot supplies the broker state.
- Request ids stay in memory until you throw the data directory away.
- There is one configured leader and one follower. Failover is manual.
- A follower that is missing acknowledged frames must not be promoted.
- After the first extra segment exists, a binary that reads only `wal.log` will miss the rest of the log.
- Linux, macOS, and FreeBSD can take the directory lock. Other systems refuse to open a data directory.

## License

MIT. See [LICENSE](LICENSE).
