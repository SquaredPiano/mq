package log

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

const DefaultPartitionCount = 1

type PartitionedLog struct {
	mu             sync.RWMutex
	shards         []*Log
	records        []Record
	startOffset    uint64
	partitionCount int
}

func NewPartitionedLog(shards int) (*PartitionedLog, error) {
	if shards < 1 {
		return nil, fmt.Errorf("partition shards must be positive")
	}
	p := &PartitionedLog{partitionCount: shards, shards: make([]*Log, shards)}
	for i := range p.shards {
		p.shards[i] = &Log{}
	}
	return p, nil
}

func (p *PartitionedLog) AssignPartition(key []byte) int {
	if p.partitionCount == 1 || len(key) == 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write(key)
	return int(h.Sum64() % uint64(p.partitionCount))
}

func (p *PartitionedLog) Partitions() int {
	return p.partitionCount
}

func (p *PartitionedLog) Append(key, value []byte) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	offset := p.startOffset + uint64(len(p.records))
	totalBytes := len(key) + len(value)
	var buf []byte
	if totalBytes > 0 {
		buf = make([]byte, totalBytes)
		copy(buf, key)
		copy(buf[len(key):], value)
	}
	var k, v []byte
	if len(key) > 0 {
		k = buf[:len(key)]
	}
	if len(value) > 0 {
		v = buf[len(key):]
	}
	partition := p.AssignPartition(key)
	record := Record{Offset: offset, Partition: uint8(partition), Key: k, Value: v, Timestamp: time.Now().UTC()}
	p.records = append(p.records, record)
	return offset
}

func (p *PartitionedLog) AppendRecord(record Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	endOffset := p.startOffset + uint64(len(p.records))
	if record.Offset != endOffset {
		return fmt.Errorf("%w: got %d, want %d", ErrOffsetMismatch, record.Offset, endOffset)
	}
	record.Partition = uint8(p.AssignPartition(record.Key))
	p.records = append(p.records, record)
	return nil
}

func (p *PartitionedLog) AppendRecords(records []Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	endOffset := p.startOffset + uint64(len(p.records))
	for i := range records {
		want := endOffset + uint64(i)
		if records[i].Offset != want {
			return fmt.Errorf("%w: got %d, want %d", ErrOffsetMismatch, records[i].Offset, want)
		}
		records[i].Partition = uint8(p.AssignPartition(records[i].Key))
	}
	p.records = append(p.records, records...)
	return nil
}

func (p *PartitionedLog) Read(offset uint64, limit int) ([]Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if offset < p.startOffset {
		return nil, fmt.Errorf("%w: requested %d, start is %d", ErrOffsetExpired, offset, p.startOffset)
	}
	endOffset := p.startOffset + uint64(len(p.records))
	if offset >= endOffset {
		return nil, nil
	}
	start := int(offset - p.startOffset)
	end := len(p.records)
	if limit < end-start {
		end = start + limit
	}
	return append([]Record(nil), p.records[start:end]...), nil
}

func (p *PartitionedLog) ReadFromStart(limit int) ([]Record, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	start := p.startOffset
	endOffset := p.startOffset + uint64(len(p.records))
	if start >= endOffset {
		return nil, nil
	}
	end := int(endOffset - start)
	if limit < end {
		end = limit
	}
	return append([]Record(nil), p.records[:end]...), nil
}

func (p *PartitionedLog) EndOffset() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.startOffset + uint64(len(p.records))
}

func (p *PartitionedLog) StartOffset() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.startOffset
}

func (p *PartitionedLog) OldestOffsetBefore(cutoff time.Time) (uint64, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for i := 0; i < len(p.records); i++ {
		if !p.records[i].Timestamp.Before(cutoff) {
			return p.startOffset + uint64(i), i > 0
		}
	}
	return p.startOffset + uint64(len(p.records)), len(p.records) > 0
}

func (p *PartitionedLog) RetainFrom(startOffset uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if startOffset < p.startOffset {
		return ErrStartOffsetRegression
	}
	endOffset := p.startOffset + uint64(len(p.records))
	if startOffset > endOffset {
		return ErrStartOffsetOutOfRange
	}
	drop := int(startOffset - p.startOffset)
	if drop > 0 {
		copy(p.records, p.records[drop:])
		p.records = p.records[:len(p.records)-drop]
		p.startOffset = startOffset
	}
	return nil
}

func (p *PartitionedLog) Snapshot() (startOffset, endOffset uint64, records []Record) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	startOffset = p.startOffset
	endOffset = p.startOffset + uint64(len(p.records))
	records = append([]Record(nil), p.records...)
	return startOffset, endOffset, records
}

func RestorePartitionedOwned(startOffset, endOffset uint64, records []Record, partitions int) (*PartitionedLog, error) {
	if endOffset < startOffset || uint64(len(records)) != endOffset-startOffset {
		return nil, fmt.Errorf("snapshot log range does not match its record count")
	}
	if partitions < 1 {
		partitions = 1
	}
	return &PartitionedLog{startOffset: startOffset, records: records, partitionCount: partitions, shards: make([]*Log, partitions)}, nil
}
