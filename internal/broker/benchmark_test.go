package broker

import (
	"errors"
	"testing"
	"time"
)

var benchmarkKey = []byte("bench-key")
var benchmarkValue = make([]byte, 64)

func BenchmarkPublishInMemory(b *testing.B) {
	queue := New()
	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkKey) + len(benchmarkValue)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := queue.Publish("bench", benchmarkKey, benchmarkValue); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishParallelInMemory(b *testing.B) {
	queue := New()
	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkKey) + len(benchmarkValue)))
	b.RunParallel(func(worker *testing.PB) {
		for worker.Next() {
			if _, err := queue.Publish("bench", benchmarkKey, benchmarkValue); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkPublishDurable(b *testing.B) {
	queue, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := queue.Close(); err != nil {
			b.Error(err)
		}
	})

	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkKey) + len(benchmarkValue)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := queue.Publish("bench", benchmarkKey, benchmarkValue); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishParallelDurable(b *testing.B) {
	queue, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := queue.Close(); err != nil {
			b.Error(err)
		}
	})

	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkKey) + len(benchmarkValue)))
	b.RunParallel(func(worker *testing.PB) {
		for worker.Next() {
			if _, err := queue.Publish("bench", benchmarkKey, benchmarkValue); err != nil {
				b.Error(err)
				return
			}
		}
	})
	stats := queue.PublishGroupCommitStats()
	b.ReportMetric(stats.FramesPerGroup(), "frames/sync")
	b.ReportMetric(float64(stats.Groups)/float64(b.N), "syncs/op")
}

func BenchmarkPublishParallelDurableWindow(b *testing.B) {
	for _, window := range []time.Duration{0, 50 * time.Microsecond, 200 * time.Microsecond, 500 * time.Microsecond} {
		b.Run(window.String(), func(b *testing.B) {
			queue, err := OpenWithOptions(b.TempDir(), OpenOptions{GroupCommitWindow: window})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := queue.Close(); err != nil {
					b.Error(err)
				}
			})

			b.ReportAllocs()
			b.SetBytes(int64(len(benchmarkKey) + len(benchmarkValue)))
			b.RunParallel(func(worker *testing.PB) {
				for worker.Next() {
					if _, err := queue.Publish("bench", benchmarkKey, benchmarkValue); err != nil {
						b.Error(err)
						return
					}
				}
			})
			stats := queue.PublishGroupCommitStats()
			b.ReportMetric(stats.FramesPerGroup(), "frames/sync")
			b.ReportMetric(float64(stats.Groups)/float64(b.N), "syncs/op")
		})
	}
}

func BenchmarkPublishBatchDurable(b *testing.B) {
	queue, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := queue.Close(); err != nil {
			b.Error(err)
		}
	})

	const batchSize = 32
	messages := make([]Message, batchSize)
	for i := range messages {
		messages[i] = Message{Key: benchmarkKey, Value: benchmarkValue}
	}
	b.ReportAllocs()
	b.SetBytes(int64(batchSize * (len(benchmarkKey) + len(benchmarkValue))))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := queue.PublishBatch("bench", messages); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishIdempotentRetry(b *testing.B) {
	queue := New()
	if _, err := queue.PublishWithID("bench", "stable-request-id", benchmarkKey, benchmarkValue); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkKey) + len(benchmarkValue)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := queue.PublishWithID("bench", "stable-request-id", benchmarkKey, benchmarkValue); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFetchBatch32(b *testing.B) {
	queue := New()
	for i := 0; i < 32; i++ {
		if _, err := queue.Publish("bench", benchmarkKey, benchmarkValue); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.SetBytes(int64(32 * (len(benchmarkKey) + len(benchmarkValue))))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := queue.Fetch("bench", 0, 32); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFetchBatch32AfterRetention(b *testing.B) {
	dataDir := b.TempDir()
	queue, err := Open(dataDir)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if queue != nil {
			if err := queue.Close(); err != nil {
				b.Error(err)
			}
		}
	})
	for i := 0; i < 64; i++ {
		if _, err := queue.Publish("bench", benchmarkKey, benchmarkValue); err != nil {
			b.Fatal(err)
		}
	}
	if err := queue.RetainFrom("bench", 32); err != nil {
		b.Fatal(err)
	}
	if _, err := queue.Fetch("bench", 31, 32); !errors.Is(err, ErrOffsetExpired) {
		b.Fatalf("fetch before retained start returned %v, want ErrOffsetExpired", err)
	}
	records, err := queue.Fetch("bench", 32, 32)
	firstOffset := uint64(0)
	if len(records) > 0 {
		firstOffset = records[0].Offset
	}
	if err != nil || len(records) != 32 || firstOffset != 32 {
		b.Fatalf("fetch retained records: got %d records, first offset %d, err %v", len(records), firstOffset, err)
	}
	if err := queue.Close(); err != nil {
		b.Fatal(err)
	}
	queue, err = Open(dataDir)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := queue.Fetch("bench", 31, 32); !errors.Is(err, ErrOffsetExpired) {
		b.Fatalf("fetch before retained start after reopen returned %v, want ErrOffsetExpired", err)
	}
	records, err = queue.Fetch("bench", 32, 32)
	firstOffset = 0
	if len(records) > 0 {
		firstOffset = records[0].Offset
	}
	if err != nil || len(records) != 32 || firstOffset != 32 {
		b.Fatalf("fetch retained records after reopen: got %d records, first offset %d, err %v", len(records), firstOffset, err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(32 * (len(benchmarkKey) + len(benchmarkValue))))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := queue.Fetch("bench", 32, 32); err != nil {
			b.Fatal(err)
		}
	}
}
