package log

import (
	"fmt"
	"testing"
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
