package broker

import (
	"fmt"
	"testing"
)

func TestDebugPartitionCountInBroker(t *testing.T) {
	dir := t.TempDir()
	b, err := OpenWithOptions(dir, OpenOptions{PartitionCount: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, key := range []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india"} {
		off, err := b.Publish("events", []byte(key), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("publish %s off %d\n", key, off)
	}
	b.mu.RLock()
	plog := b.topics["events"]
	b.mu.RUnlock()
	fmt.Printf("partitionCount in %d\n", plog.Partitions())
	recs, _ := plog.Read(0, 10)
	for _, r := range recs {
		fmt.Printf("offset %d partition %d key %s\n", r.Offset, r.Partition, string(r.Key))
	}
}
