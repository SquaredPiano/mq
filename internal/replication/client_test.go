package replication

import (
	"net/http/httptest"
	"testing"

	"mq/internal/broker"
	"mq/internal/httpapi"
)

func TestLeaderPublishReachesFollower(t *testing.T) {
	token := "replication-token"
	follower, err := broker.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := follower.Close(); err != nil {
			t.Error(err)
		}
	})
	handler, err := httpapi.NewFollowerHandler(follower, token)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	leader, err := broker.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := leader.Close(); err != nil {
			t.Error(err)
		}
	})
	client, err := New(leader, server.URL, token, 1)
	if err != nil {
		t.Fatal(err)
	}
	leader.SetReplicator(client.Sync)
	if err := client.Sync(); err != nil {
		t.Fatal(err)
	}

	offset, err := leader.PublishWithID("events", "req-1", []byte("k"), []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	records, err := follower.Fetch("events", offset, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || string(records[0].Value) != "hello" {
		t.Fatalf("follower records = %#v", records)
	}
	status, err := client.Status()
	if err != nil {
		t.Fatal(err)
	}
	if !status.InSync || status.UnconfirmedWALBytes != 0 {
		t.Fatalf("status = %+v", status)
	}
}
