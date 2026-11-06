# Retention and compaction

A topic has two absolute offsets. `endOffset` is the next offset a publish will receive. `startOffset` is the first offset a read is still allowed to use. Moving `startOffset` forward drops older records from memory. It does not renumber the records that remain.

`POST /v1/topics/retain` is the only way to move `startOffset`. The broker does not guess from consumer commits. A client can fetch any offset, and a group does not register itself before it starts reading. An operator can therefore expire records a slow group still wanted. Those reads return HTTP 410.

A fetch from `startOffset` up to `endOffset` returns records. A commit below `startOffset` is rejected. A commit cannot move backward and cannot pass `endOffset`. A group that has never committed starts at `startOffset`.

The write-ahead log also stores commits and request ids, for every topic, in one byte stream. A segment file is only a size boundary in that stream. Deleting a prefix because one topic has moved its start offset can delete another topic's records, a group commit, or the only copy of a request id. It also changes the byte prefix the follower uses to prove it matches the leader.

`POST /v1/snapshot` writes a checksummed copy of the broker state at a log byte offset: retained records, topic ranges, group commits, and request ids. Startup loads that file and applies later log frames on top. The segment files stay where they are. Restart still reads the whole log so a corrupt frame is not ignored.

Deleting segment files has to wait until both nodes have the same checkpoint: snapshot generation, snapshot checksum, and the first log byte after that snapshot. Until both have stored that checkpoint, both keep every segment. After that, a node may delete only whole segments that end at or before the checkpoint, then sync the directory.

The code for the current behavior is in `internal/wal`, `internal/broker`, and `internal/replication`.
