package mqcluster

import (
	"net/http/httptest"
	"testing"
	"time"

	"mq/internal/broker"
	"mq/internal/raft"
)

var clusterBenchKey = []byte("bench-key")
var clusterBenchValue = make([]byte, 64)
var clusterBenchMessages = func() []broker.Message {
	messages := make([]broker.Message, 32)
	for i := range messages {
		messages[i] = broker.Message{Key: clusterBenchKey, Value: clusterBenchValue}
	}
	return messages
}()

func newBenchCluster(b *testing.B) (*Node, int) {
	b.Helper()
	holders := make([]*holder, 3)
	servers := make([]*httptest.Server, 3)
	for i := range holders {
		h := &holder{mux: newClusterMux()}
		holders[i] = h
		servers[i] = httptest.NewServer(h.mux)
	}
	urls := make([]string, 3)
	for i, server := range servers {
		urls[i] = server.URL
	}
	nodes := make([]*Node, 3)
	token := "benchmark-raft-token"
	for i := 0; i < 3; i++ {
		node, err := Open(b.TempDir(), urls, i, token, 20*time.Millisecond)
		if err != nil {
			b.Fatal(err)
		}
		nodes[i] = node
		holders[i].node = node
		holders[i].raft = raft.HTTPHandler(node.Raft, token)
		mux := holders[i].mux
		mux.Handle("/raft/", holders[i].raft)
		mux.Handle("/", holders[i].muxFallback())
	}
	b.Cleanup(func() {
		for _, node := range nodes {
			_ = node.Close()
		}
		for _, server := range servers {
			server.Close()
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, node := range nodes {
			if node.Raft.StateRole() == raft.Leader {
				return node, i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.Fatal("no leader elected")
	return nil, -1
}

func BenchmarkClusterPublish(b *testing.B) {
	leader, _ := newBenchCluster(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(clusterBenchKey) + len(clusterBenchValue)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := leader.Broker.Publish("bench", clusterBenchKey, clusterBenchValue); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterPublishParallel(b *testing.B) {
	leader, _ := newBenchCluster(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(clusterBenchKey) + len(clusterBenchValue)))
	b.ResetTimer()
	b.RunParallel(func(worker *testing.PB) {
		for worker.Next() {
			if _, err := leader.Broker.Publish("bench", clusterBenchKey, clusterBenchValue); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkClusterPublishBatch32(b *testing.B) {
	leader, _ := newBenchCluster(b)
	b.ReportAllocs()
	b.SetBytes(32 * int64(len(clusterBenchKey)+len(clusterBenchValue)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := leader.Broker.PublishBatch("bench", clusterBenchMessages); err != nil {
			b.Fatal(err)
		}
	}
}
