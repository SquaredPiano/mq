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

// TestFailoverAndFencingTrials runs 100 acknowledged-write failover and fencing trials.
// In each trial:
// 1. Leader publishes records synchronously replicated to the follower.
// 2. The leader stops (this in-process test calls Close; it is not a literal OS-process kill).
// 3. We verify 0 acknowledged writes are lost on the follower.
// 4. Follower is promoted to epoch 2.
// 5. Stale leader restarts at epoch 1 and attempts to sync; epoch fencing blocks it.
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

		// Step 1: Write acknowledged records to leader.
		const numRecords = 5
		for i := 0; i < numRecords; i++ {
			reqID := fmt.Sprintf("trial-%d-msg-%d", trial, i)
			val := fmt.Sprintf("val-%d", i)
			if _, err := leader.PublishWithID("test-topic", reqID, []byte("key"), []byte(val)); err != nil {
				t.Fatalf("trial %d: publish %d: %v", trial, i, err)
			}
		}

		// Step 2: Leader stops. Read the WAL on the follower to confirm acknowledged records.
		_ = leader.Close()

		// Step 3: Verify 0 acknowledged records lost on follower.
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

		// Step 4: Promote follower: epoch is updated to 2.
		if err := os.WriteFile(filepath.Join(followerDir, "replication.epoch"), []byte("2\n"), 0o600); err != nil {
			t.Fatalf("trial %d: write epoch: %v", trial, err)
		}
		_ = follower.Close()
		server.Close()

		// Reopen follower node in follower mode with epoch 2 loaded to test fencing of old leader.
		reopenedFollower, err := broker.Open(followerDir)
		if err != nil {
			t.Fatalf("trial %d: reopen follower: %v", trial, err)
		}
		reopenedHandler, err := httpapi.NewFollowerHandler(reopenedFollower, token)
		if err != nil {
			t.Fatalf("trial %d: reopened follower handler: %v", trial, err)
		}
		reopenedServer := httptest.NewServer(reopenedHandler)

		// Step 5: Stale leader restarts at epoch 1 and tries to connect to the follower.
		staleLeader, err := broker.Open(leaderDir)
		if err != nil {
			t.Fatalf("trial %d: reopen stale leader: %v", trial, err)
		}
		staleClient, err := New(staleLeader, reopenedServer.URL, token, 1)
		if err != nil {
			t.Fatalf("trial %d: stale client: %v", trial, err)
		}

		// Attempted sync must fail because follower has accepted epoch 2 > 1.
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

	// Successful write while connected
	if _, err := leader.PublishWithID("events", "req-1", []byte("k"), []byte("ok")); err != nil {
		t.Fatalf("publish while healthy: %v", err)
	}

	// Sever network connection (partition follower)
	server.Close()

	// Attempted write must fail to acknowledge to client because follower cannot sync
	if _, err := leader.PublishWithID("events", "req-2", []byte("k"), []byte("unacked")); err == nil {
		t.Fatal("expected publish to fail when follower is partitioned, but got nil error")
	}
}
