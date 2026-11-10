package replication

import (
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"mq/internal/broker"
	"mq/internal/httpapi"
)

func TestFailoverAndFencingTrials(t *testing.T) {
	const trials = 100
	token := "replication-token"

	for trial := 0; trial < trials; trial++ {
		followerDir := t.TempDir()
		leaderDir := t.TempDir()

		follower, err := broker.Open(followerDir)
		if err != nil {
			t.Fatalf("trial %d: open follower: %v", trial, err)
		}
		handler, err := httpapi.NewFollowerHandler(follower, token)
		if err != nil {
			t.Fatalf("trial %d: follower handler: %v", trial, err)
		}
		server := httptest.NewServer(handler)

		leader, err := broker.Open(leaderDir)
		if err != nil {
			t.Fatalf("trial %d: open leader: %v", trial, err)
		}
		client, err := New(leader, server.URL, token, 1)
		if err != nil {
			t.Fatalf("trial %d: new client: %v", trial, err)
		}
		leader.SetReplicator(client.Sync)
		if err := client.Sync(); err != nil {
			t.Fatalf("trial %d: initial sync: %v", trial, err)
		}

		const numRecords = 5
		for i := 0; i < numRecords; i++ {
			reqID := fmt.Sprintf("trial-%d-msg-%d", trial, i)
			val := fmt.Sprintf("val-%d", i)
			if _, err := leader.PublishWithID("test-topic", reqID, []byte("key"), []byte(val)); err != nil {
				t.Fatalf("trial %d: publish %d: %v", trial, i, err)
			}
		}

		_ = leader.Close()

		records, err := follower.Fetch("test-topic", 0, numRecords)
		if err != nil {
			t.Fatalf("trial %d: fetch from follower: %v", trial, err)
		}
		if len(records) != numRecords {
			t.Fatalf("trial %d: lost acknowledged writes: got %d, want %d", trial, len(records), numRecords)
		}
		for i, rec := range records {
			expected := fmt.Sprintf("val-%d", i)
			if string(rec.Value) != expected {
				t.Fatalf("trial %d: record %d corrupted: got %s, want %s", trial, i, rec.Value, expected)
			}
		}

		if err := os.WriteFile(filepath.Join(followerDir, "replication.epoch"), []byte("2\n"), 0o600); err != nil {
			t.Fatalf("trial %d: write epoch: %v", trial, err)
		}
		_ = follower.Close()
		server.Close()

		reopenedFollower, err := broker.Open(followerDir)
		if err != nil {
			t.Fatalf("trial %d: reopen follower: %v", trial, err)
		}
		reopenedHandler, err := httpapi.NewFollowerHandler(reopenedFollower, token)
		if err != nil {
			t.Fatalf("trial %d: reopened follower handler: %v", trial, err)
		}
		reopenedServer := httptest.NewServer(reopenedHandler)

		staleLeader, err := broker.Open(leaderDir)
		if err != nil {
			t.Fatalf("trial %d: reopen stale leader: %v", trial, err)
		}
		staleClient, err := New(staleLeader, reopenedServer.URL, token, 1)
		if err != nil {
			t.Fatalf("trial %d: stale client: %v", trial, err)
		}

		if err := staleClient.Sync(); err == nil {
			t.Fatalf("trial %d: expected stale leader with epoch 1 to be fenced, but sync succeeded", trial)
		}

		_ = staleLeader.Close()
		reopenedServer.Close()
		_ = reopenedFollower.Close()
	}
}

func TestPartitionRefusesUnreplicatedPublishes(t *testing.T) {
	token := "replication-token"
	followerDir := t.TempDir()
	leaderDir := t.TempDir()

	follower, err := broker.Open(followerDir)
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	handler, err := httpapi.NewFollowerHandler(follower, token)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)

	leader, err := broker.Open(leaderDir)
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	client, err := New(leader, server.URL, token, 1)
	if err != nil {
		t.Fatal(err)
	}
	leader.SetReplicator(client.Sync)
	if err := client.Sync(); err != nil {
		t.Fatal(err)
	}

	if _, err := leader.PublishWithID("events", "req-1", []byte("k"), []byte("ok")); err != nil {
		t.Fatalf("publish while healthy: %v", err)
	}

	server.Close()

	if _, err := leader.PublishWithID("events", "req-2", []byte("k"), []byte("unacked")); err == nil {
		t.Fatal("expected publish to fail when follower is partitioned, but got nil error")
	}
}
