package replication

import (
	"net/http/httptest"
	"testing"
	"time"

	"mq/internal/broker"
	"mq/internal/httpapi"
)

var replicationBenchmarkKey = []byte("bench-key")
var replicationBenchmarkValue = make([]byte, 64)

func BenchmarkSynchronousPublish(b *testing.B) {
	leader := newBenchmarkPair(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(replicationBenchmarkKey) + len(replicationBenchmarkValue)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := leader.Publish("bench", replicationBenchmarkKey, replicationBenchmarkValue); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSynchronousPublishParallel(b *testing.B) {
	leader := newBenchmarkPair(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(replicationBenchmarkKey) + len(replicationBenchmarkValue)))
	b.RunParallel(func(worker *testing.PB) {
		for worker.Next() {
			if _, err := leader.Publish("bench", replicationBenchmarkKey, replicationBenchmarkValue); err != nil {
				b.Error(err)
				return
			}
		}
	})
	stats := leader.PublishGroupCommitStats()
	b.ReportMetric(stats.FramesPerGroup(), "frames/sync")
	b.ReportMetric(float64(stats.Groups)/float64(b.N), "syncs/op")
}

func BenchmarkSynchronousPublishParallelWindow(b *testing.B) {
	for _, window := range []time.Duration{0, 50 * time.Microsecond, 200 * time.Microsecond, 500 * time.Microsecond} {
		b.Run(window.String(), func(b *testing.B) {
			leader := newBenchmarkPairWithWindow(b, window)
			b.ReportAllocs()
			b.SetBytes(int64(len(replicationBenchmarkKey) + len(replicationBenchmarkValue)))
			b.RunParallel(func(worker *testing.PB) {
				for worker.Next() {
					if _, err := leader.Publish("bench", replicationBenchmarkKey, replicationBenchmarkValue); err != nil {
						b.Error(err)
						return
					}
				}
			})
			stats := leader.PublishGroupCommitStats()
			b.ReportMetric(stats.FramesPerGroup(), "frames/sync")
			b.ReportMetric(float64(stats.Groups)/float64(b.N), "syncs/op")
		})
	}
}

func newBenchmarkPair(b *testing.B) *broker.Broker {
	return newBenchmarkPairWithWindow(b, broker.DefaultGroupCommitWindow)
}

func newBenchmarkPairWithWindow(b *testing.B, window time.Duration) *broker.Broker {
	b.Helper()
	token := "benchmark-replication-token"
	follower, err := broker.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := follower.Close(); err != nil {
			b.Error(err)
		}
	})

	handler, err := httpapi.NewFollowerHandler(follower, token)
	if err != nil {
		b.Fatal(err)
	}
	server := httptest.NewServer(handler)
	b.Cleanup(server.Close)

	leader, err := broker.OpenWithOptions(b.TempDir(), broker.OpenOptions{GroupCommitWindow: window})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := leader.Close(); err != nil {
			b.Error(err)
		}
	})
	replicator, err := New(leader, server.URL, token, 1)
	if err != nil {
		b.Fatal(err)
	}
	leader.SetReplicator(replicator.Sync)
	if err := replicator.Sync(); err != nil {
		b.Fatal(err)
	}
	return leader
}
