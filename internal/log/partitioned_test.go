package log

import (
	"fmt"
	"testing"
	"time"
)

func TestPartitionedLogAppendsMonotonicOffsets(t *testing.T) {
	p, err := NewPartitionedLog(2)
	if err != nil {
		t.Fatal(err)
	}
	p.Append([]byte("a"), []byte("x"))
	p.Append([]byte("b"), []byte("y"))
	p.Append([]byte("a"), []byte("z"))
	recs, err := p.ReadFromStart(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d records", len(recs))
	}
	for i, r := range recs {
		if r.Offset != uint64(i) {
			t.Fatalf("record %d has offset %d", i, r.Offset)
		}
		if r.Partition >= 2 {
			t.Fatalf("record %d has bad partition %d", i, r.Partition)
		}
	}
}

func TestPartitionedLogRestoreContiguity(t *testing.T) {
	p, err := NewPartitionedLog(1)
	if err != nil {
		t.Fatal(err)
	}
	p.Append([]byte("a"), []byte("one"))
	p.Append([]byte("b"), []byte("two"))
	start, end, snapshot := p.Snapshot()
	restored, err := RestorePartitionedOwned(start, end, snapshot, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, n, _ := restored.Snapshot()
	if n != end-start {
		t.Fatalf("restored end = %d, want %d", n, end-start)
	}
}

func TestPartitionedLogHealthyAfterRetain(t *testing.T) {
	p, err := NewPartitionedLog(4)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		p.Append([]byte(fmt.Sprintf("k-%d", i)), []byte("v"))
	}
	if err := p.RetainFrom(10); err != nil {
		t.Fatal(err)
	}
	recs, err := p.ReadFromStart(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 10 {
		t.Fatalf("reading %d records, want 10", len(recs))
	}
	if p.StartOffset() != 10 {
		t.Fatalf("start offset = %d", p.StartOffset())
	}
}

func TestPartitionedLogOldestOffsetBeforeKeepsNewEnough(t *testing.T) {
	p, err := NewPartitionedLog(1)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour).UTC()
	for i := 0; i < 3; i++ {
		p.records = append(p.records, Record{Offset: uint64(i), Partition: 0, Key: []byte("k"), Value: []byte("v"), Timestamp: old})
	}
	p.records = append(p.records, Record{Offset: 3, Partition: 0, Key: []byte("k"), Value: []byte("v"), Timestamp: time.Now().UTC()})
	cutoff := time.Now().Add(-time.Hour)
	got, ok := p.OldestOffsetBefore(cutoff)
	if !ok {
		t.Fatal("expected some records to expire")
	}
	if got != 3 {
		t.Fatalf("oldest offset before = %d, want 3", got)
	}
}

func TestPartitionedLogOldestOffsetBeforeNothingExpires(t *testing.T) {
	p, err := NewPartitionedLog(1)
	if err != nil {
		t.Fatal(err)
	}
	p.Append([]byte("k"), []byte("v"))
	got, ok := p.OldestOffsetBefore(time.Now().Add(-time.Hour))
	if ok {
		t.Fatalf("expected nothing to expire, got offset %d", got)
	}
	if got != p.StartOffset() {
		t.Fatalf("start = %d, unchanged = %d", got, p.StartOffset())
	}
}

func TestPartitionedLogOldestOffsetBeforeExpiresEverything(t *testing.T) {
	p, err := NewPartitionedLog(1)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour).UTC()
	for i := 0; i < 3; i++ {
		p.records = append(p.records, Record{Offset: uint64(i), Partition: 0, Key: []byte("k"), Value: []byte("v"), Timestamp: old})
	}
	got, ok := p.OldestOffsetBefore(time.Now().Add(-time.Hour))
	if !ok {
		t.Fatal("expected something to expire")
	}
	if got != 3 {
		t.Fatalf("oldest offset before = %d, want 3", got)
	}
}
