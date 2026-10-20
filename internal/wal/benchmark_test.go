package wal

import (
	"path/filepath"
	"strconv"
	"testing"
	"time"

	messageLog "mq/internal/log"
)

func BenchmarkAppendPublishSegmentRotation(b *testing.B) {
	for _, segmentBytes := range []int64{MinSegmentBytes, DefaultSegmentBytes} {
		b.Run(strconv.FormatInt(segmentBytes>>20, 10)+"MiB", func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "wal.log")
			store, err := OpenAndReplayWithSegmentBytes(path, segmentBytes, func(Entry) error {
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := store.Close(); err != nil {
					b.Error(err)
				}
			})

			record := messageLog.Record{
				Key:       []byte("key"),
				Value:     make([]byte, (256<<10)-3),
				Timestamp: time.Unix(1, 0).UTC(),
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(record.Key) + len(record.Value)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				record.Offset = uint64(i)
				if err := store.AppendPublish("bench", record); err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
			if err := store.AppendRetention("bench", uint64(b.N/2)); err != nil {
				b.Fatal(err)
			}
			if err := store.Close(); err != nil {
				b.Fatal(err)
			}
			var replayed uint64
			reopened, err := OpenAndReplayWithSegmentBytes(path, segmentBytes, func(Entry) error {
				replayed++
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					b.Error(err)
				}
			}()
			wantEntries := uint64(b.N) + 1
			if replayed != wantEntries {
				b.Fatalf("replayed %d entries, want %d", replayed, wantEntries)
			}
			size, err := reopened.Size()
			if err != nil {
				b.Fatal(err)
			}
			if _, err := reopened.HashPrefix(size); err != nil {
				b.Fatalf("hash full logical WAL: %v", err)
			}
			var readOffset int64
			var readEntries uint64
			for readOffset < size {
				frames, err := reopened.ReadFrames(readOffset, int(MaxGroupCommitBytes))
				if err != nil {
					b.Fatalf("read logical WAL at %d: %v", readOffset, err)
				}
				if len(frames) == 0 {
					b.Fatalf("read no frames before WAL end %d", size)
				}
				entries, _, err := decodeFrames(frames)
				if err != nil {
					b.Fatalf("decode logical WAL at %d: %v", readOffset, err)
				}
				readEntries += uint64(len(entries))
				readOffset += int64(len(frames))
			}
			if readEntries != wantEntries {
				b.Fatalf("read %d entries, want %d", readEntries, wantEntries)
			}

			followerPath := filepath.Join(b.TempDir(), "follower.log")
			follower, err := OpenAndReplayWithSegmentBytes(followerPath, segmentBytes, func(Entry) error {
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			defer func() {
				if follower != nil {
					if err := follower.Close(); err != nil {
						b.Error(err)
					}
				}
			}()
			var replicatedOffset int64
			for replicatedOffset < size {
				frames, err := reopened.ReadFrames(replicatedOffset, int(MaxGroupCommitBytes))
				if err != nil {
					b.Fatalf("read leader frames at %d: %v", replicatedOffset, err)
				}
				if len(frames) == 0 {
					b.Fatalf("read no leader frames before WAL end %d", size)
				}
				if _, err := follower.AppendFrames(replicatedOffset, frames); err != nil {
					b.Fatalf("append follower frames at %d: %v", replicatedOffset, err)
				}
				replicatedOffset += int64(len(frames))
			}
			leaderHash, err := reopened.HashPrefix(size)
			if err != nil {
				b.Fatal(err)
			}
			followerHash, err := follower.HashPrefix(size)
			if err != nil {
				b.Fatal(err)
			}
			if leaderHash != followerHash {
				b.Fatal("replicated WAL hash differs from leader")
			}
			if err := follower.Close(); err != nil {
				b.Fatal(err)
			}
			followerReplayed := uint64(0)
			follower, err = OpenAndReplayWithSegmentBytes(followerPath, segmentBytes, func(Entry) error {
				followerReplayed++
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			if followerReplayed != wantEntries {
				b.Fatalf("follower replayed %d entries, want %d", followerReplayed, wantEntries)
			}
			if err := follower.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}
