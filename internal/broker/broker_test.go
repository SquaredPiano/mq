package broker

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestDurablePublishFetchCommitRetainAndReopen(t *testing.T) {
	dir := t.TempDir()
	queue, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	offset, err := queue.Publish("events", []byte("k"), []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0 {
		t.Fatalf("offset = %d, want 0", offset)
	}
	again, err := queue.PublishWithID("events", "req-1", []byte("k"), []byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	retried, err := queue.PublishWithID("events", "req-1", []byte("k"), []byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	if retried != again {
		t.Fatalf("retry offset = %d, want %d", retried, again)
	}
	if _, err := queue.PublishWithID("events", "req-1", []byte("k"), []byte("other")); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflict error = %v", err)
	}

	records, err := queue.Fetch("events", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || string(records[1].Value) != "world" {
		t.Fatalf("records = %#v", records)
	}
	groupRecords, err := queue.FetchGroup("workers", "events", 1)
	if err != nil || len(groupRecords) != 1 || groupRecords[0].Offset != 0 {
		t.Fatalf("group fetch = %#v, err %v", groupRecords, err)
	}
	if err := queue.Commit("workers", "events", 1); err != nil {
		t.Fatal(err)
	}
	groupRecords, err = queue.FetchGroup("workers", "events", 10)
	if err != nil || len(groupRecords) != 1 || groupRecords[0].Offset != 1 {
		t.Fatalf("group fetch after commit = %#v, err %v", groupRecords, err)
	}
	if err := queue.RetainFrom("events", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Fetch("events", 0, 1); !errors.Is(err, ErrOffsetExpired) {
		t.Fatalf("expired fetch error = %v", err)
	}
	snapshot, err := queue.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Generation != 1 || snapshot.WALOffset <= 0 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Fetch("events", 0, 1); !errors.Is(err, ErrOffsetExpired) {
		t.Fatalf("expired fetch after reopen = %v", err)
	}
	kept, err := reopened.Fetch("events", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 1 || !bytes.Equal(kept[0].Value, []byte("world")) || kept[0].Offset != 1 {
		t.Fatalf("kept records = %#v", kept)
	}
	fresh, err := reopened.FetchGroup("late", "events", 10)
	if err != nil || len(fresh) != 1 || fresh[0].Offset != 1 {
		t.Fatalf("new group = %#v, err %v", fresh, err)
	}
}

func TestPartitionedPublishFetchReopen(t *testing.T) {
	dir := t.TempDir()
	queue, err := OpenWithOptions(dir, OpenOptions{
		GroupCommitWindow: DefaultGroupCommitWindow,
		WALSegmentBytes:   DefaultWALSegmentBytes,
		PartitionCount:    4,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = queue.Publish("events", []byte("key-1"), []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = queue.Publish("events", []byte("key-2"), []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	recs, err := queue.Fetch("events", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	for _, r := range recs {
		if r.Partition >= 4 {
			t.Fatalf("record has bad partition %d", r.Partition)
		}
	}
}

func TestKeyRoutingIsConsistent(t *testing.T) {
	dir := t.TempDir()
	queue, err := OpenWithOptions(dir, OpenOptions{
		GroupCommitWindow: DefaultGroupCommitWindow,
		WALSegmentBytes:   DefaultWALSegmentBytes,
		PartitionCount:    8,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := queue.Publish("events", []byte("user-42"), []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := queue.Publish("events", []byte("user-42"), []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("expected different offsets for repeat publish, got %d", first)
	}
	for i := 1; i < 60; i++ {
		if _, err := queue.Publish("events", []byte("user-42"), []byte("ok")); err != nil {
			t.Fatal(err)
		}
	}
	recs, err := queue.Fetch("events", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var partition byte
	set := false
	for _, r := range recs {
		if !set {
			partition = byte(r.Partition)
			set = true
			continue
		}
		if byte(r.Partition) != partition {
			t.Fatalf("partition drifted from %d to %d mid-key", partition, r.Partition)
		}
	}
}

func TestMaxAgeDropsOldRecords(t *testing.T) {
	dir := t.TempDir()
	queue, err := OpenWithOptions(dir, OpenOptions{
		GroupCommitWindow: DefaultGroupCommitWindow,
		WALSegmentBytes:   DefaultWALSegmentBytes,
		PartitionCount:    1,
		MaxAge:            40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Publish("events", []byte("k"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err = queue.Fetch("events", 0, 1)
		if errors.Is(err, ErrOffsetExpired) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected retention to drop the record, got err=%v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMaxAgeZeroDisablesTimeRetention(t *testing.T) {
	dir := t.TempDir()
	queue, err := OpenWithOptions(dir, OpenOptions{
		GroupCommitWindow: DefaultGroupCommitWindow,
		WALSegmentBytes:   DefaultWALSegmentBytes,
		PartitionCount:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Publish("events", []byte("k"), []byte("keeps")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := queue.Fetch("events", 0, 1); err != nil {
		t.Fatalf("expected record to still be readable, got err=%v", err)
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGroupJoinHeartbeatLeaveList(t *testing.T) {
	dir := t.TempDir()
	queue, err := OpenWithOptions(dir, OpenOptions{
		GroupCommitWindow: DefaultGroupCommitWindow,
		WALSegmentBytes:   DefaultWALSegmentBytes,
		PartitionCount:    3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	if _, err := queue.Publish("events", []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	m1, err := queue.GroupJoin("workers", "events", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(m1.Assigned) == 0 {
		t.Fatalf("expected partitions assigned, got %v", m1.Assigned)
	}

	m2, err := queue.GroupJoin("workers", "events", 3)
	if err != nil {
		t.Fatal(err)
	}
	if m2.MemberID == m1.MemberID {
		t.Fatalf("expected different member IDs")
	}

	members := queue.GroupList("workers", "events")
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}

	m1b, err := queue.GroupHeartbeat("workers", "events", m1.MemberID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !m1b.Expires.After(m1.Expires) {
		t.Fatalf("expected heartbeat to extend expiry")
	}

	if err := queue.GroupLeave("workers", "events", m1.MemberID, 3); err != nil {
		t.Fatal(err)
	}
	members = queue.GroupList("workers", "events")
	if len(members) != 1 || members[0].MemberID != m2.MemberID {
		t.Fatalf("expected only m2 after leave, got %v", members)
	}

	if err := queue.GroupLeave("workers", "events", "unknown", 3); err == nil {
		t.Fatal("expected error leaving unknown member")
	}

	if _, err := queue.GroupHeartbeat("workers", "events", "unknown", 3); err == nil {
		t.Fatal("expected error heartbeating unknown member")
	}
}

func TestGroupJoinDuplicateIDReturnsExisting(t *testing.T) {
	dir := t.TempDir()
	queue, err := OpenWithOptions(dir, OpenOptions{
		GroupCommitWindow: DefaultGroupCommitWindow,
		WALSegmentBytes:   DefaultWALSegmentBytes,
		PartitionCount:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	if _, err := queue.Publish("events", []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	m1, err := queue.GroupJoin("workers", "events", 1)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := queue.GroupJoin("workers", "events", 1)
	if err != nil {
		t.Fatal(err)
	}
	if m2.MemberID == m1.MemberID {
		t.Fatalf("expected different member IDs for separate joins")
	}

	members := queue.GroupList("workers", "events")
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}
}

func TestGroupListEmptyForUnknownTopic(t *testing.T) {
	dir := t.TempDir()
	queue, err := OpenWithOptions(dir, OpenOptions{
		GroupCommitWindow: DefaultGroupCommitWindow,
		WALSegmentBytes:   DefaultWALSegmentBytes,
		PartitionCount:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	members := queue.GroupList("workers", "nonexistent")
	if len(members) != 0 {
		t.Fatalf("expected 0 members for unknown topic, got %d", len(members))
	}
}

func TestBrokerStats(t *testing.T) {
	dir := t.TempDir()
	queue, err := OpenWithOptions(dir, OpenOptions{
		GroupCommitWindow: DefaultGroupCommitWindow,
		WALSegmentBytes:   DefaultWALSegmentBytes,
		PartitionCount:    3,
		MaxAge:            24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	if _, err := queue.Publish("events", []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Publish("events", []byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Publish("logs", []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	stats := queue.Stats()
	if stats.TopicCount != 2 {
		t.Fatalf("expected 2 topics, got %d", stats.TopicCount)
	}
	if stats.Closed {
		t.Fatal("expected broker not closed")
	}
	if stats.Closing {
		t.Fatal("expected broker not closing")
	}
	if stats.RetentionMaxAgeSecs != 24*3600 {
		t.Fatalf("expected retention 86400s, got %g", stats.RetentionMaxAgeSecs)
	}

	events, ok := stats.Topics["events"]
	if !ok {
		t.Fatal("expected events topic stats")
	}
	if events.RecordCount != 2 {
		t.Fatalf("expected 2 records in events, got %d", events.RecordCount)
	}
	if events.Partitions != 3 {
		t.Fatalf("expected 3 partitions, got %d", events.Partitions)
	}
	logs, ok := stats.Topics["logs"]
	if !ok {
		t.Fatal("expected logs topic stats")
	}
	if logs.RecordCount != 1 {
		t.Fatalf("expected 1 record in logs, got %d", logs.RecordCount)
	}

	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	stats = queue.Stats()
	if !stats.Closed {
		t.Fatal("expected broker closed after Close")
	}
}

func TestBrokerReady(t *testing.T) {
	dir := t.TempDir()
	queue, err := OpenWithOptions(dir, OpenOptions{
		GroupCommitWindow: DefaultGroupCommitWindow,
		WALSegmentBytes:   DefaultWALSegmentBytes,
		PartitionCount:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !queue.Ready() {
		t.Fatal("expected broker ready")
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	if queue.Ready() {
		t.Fatal("expected broker not ready after Close")
	}
}
