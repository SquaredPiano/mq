package broker

import (
	"bytes"
	"errors"
	"testing"
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
