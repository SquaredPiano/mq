package mqcluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mq/internal/httpapi"
	"mq/internal/raft"
)

type holder struct {
	node *Node
	raft http.Handler
	api  http.Handler
	mux  *http.ServeMux
}

func newClusterMux() *http.ServeMux {
	return http.NewServeMux()
}

func (h *holder) muxFallback() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.api == nil {
			http.Error(w, "cluster node is starting", http.StatusServiceUnavailable)
			return
		}
		h.api.ServeHTTP(w, r)
	})
}

func newTestCluster(t *testing.T, count int) ([]*Node, []*httptest.Server, []string, []*holder) {
	t.Helper()
	holders := make([]*holder, count)
	servers := make([]*httptest.Server, count)
	for i := range holders {
		h := &holder{mux: http.NewServeMux()}
		mux := h.mux
		mux.HandleFunc("/raft/", func(w http.ResponseWriter, r *http.Request) {
			h.raft.ServeHTTP(w, r)
		})
		mux.Handle("/", h.muxFallback())
		holders[i] = h
		servers[i] = httptest.NewServer(mux)
	}
	urls := make([]string, count)
	for i, server := range servers {
		urls[i] = server.URL
	}
	nodes := make([]*Node, count)
	token := "test-raft-token"
	for i := 0; i < count; i++ {
		dir := t.TempDir()
		node, err := Open(dir, urls, i, token, 20*time.Millisecond)
		if err != nil {
			t.Fatalf("open node %d: %v", i, err)
		}
		nodes[i] = node
		for _, other := range nodes {
			_ = other
		}
		rng := muxHandler(node.Raft, token)
		holders[i].raft = rng
		admin, err := httpapi.NewAuthenticatedHandlerWithAdmin(node.Broker, "", "")
		if err != nil {
			t.Fatal(err)
		}
		holders[i].api = admin
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			_ = node.Close()
		}
		for _, server := range servers {
			server.Close()
		}
	})
	return nodes, servers, urls, holders
}

func muxHandler(node *raft.Node, token string) http.Handler {
	return raft.HTTPHandler(node, token)
}

func waitForLeader(t *testing.T, nodes []*Node, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, node := range nodes {
			if node.Raft.StateRole() == raft.Leader {
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected in %v; roles = %v", timeout, roles(nodes))
	return -1
}

func roles(nodes []*Node) []raft.Role {
	out := make([]raft.Role, len(nodes))
	for i, node := range nodes {
		out[i] = node.Raft.StateRole()
	}
	return out
}

func TestClusterReplicatesPublishes(t *testing.T) {
	nodes, _, _, _ := newTestCluster(t, 3)
	leaderIdx := waitForLeader(t, nodes, 5*time.Second)
	payload := []byte("hello-raft")
	for i := 0; i < 5; i++ {
		_ = i
	}
	_, err := nodes[leaderIdx].Broker.PublishWithID("events", "req-0", []byte("k"), payload)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	for index, node := range nodes {
		value, err := node.Broker.Fetch("events", 0, 1)
		if err != nil {
			t.Fatalf("node %d fetch: %v", index, err)
		}
		if len(value) != 1 || !bytes.Equal(value[0].Value, payload) {
			t.Fatalf("node %d value = %q", index, value)
		}
	}
}

func TestClusterLeaderFailover(t *testing.T) {
	nodes, servers0, _, _ := newTestCluster(t, 3)
	leaderIdx := waitForLeader(t, nodes, 5*time.Second)
	_, err := nodes[leaderIdx].Broker.PublishWithID("events", "req-1", []byte("k"), []byte("still-here"))
	if err != nil {
		t.Fatal(err)
	}
	for i, node := range nodes {
		value, err := node.Broker.Fetch("events", 0, 1)
		if err != nil {
			t.Fatalf("node %d fetch: %v", i, err)
		}
		if len(value) != 1 || !bytes.Equal(value[0].Value, []byte("still-here")) {
			t.Fatalf("node %d value mismatch", i)
		}
	}
	servers0[leaderIdx].Config.Handler = http.NewServeMux()
	nodes[leaderIdx].Close()
	newLeader := waitForLeaderExcept(t, nodes, leaderIdx, 5*time.Second)
	value, err := nodes[newLeader].Broker.Fetch("events", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(value) != 1 || !bytes.Equal(value[0].Value, []byte("still-here")) {
		t.Fatalf("new leader value = %v", value)
	}
}

func waitForLeaderExcept(t *testing.T, nodes []*Node, excluded int, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, node := range nodes {
			if i == excluded {
				continue
			}
			if node.Raft.StateRole() == raft.Leader {
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no new leader emerged")
	return -1
}

var _ = json.Marshal
var _ = fmt.Sprint
