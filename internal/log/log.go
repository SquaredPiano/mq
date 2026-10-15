package log

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrOffsetMismatch = errors.New("record offset does not match log end")

var ErrOffsetExpired = errors.New("record offset has expired")

var ErrStartOffsetOutOfRange = errors.New("retention start exceeds log end")

var ErrStartOffsetRegression = errors.New("retention start cannot move backwards")

type Record struct {
	Offset    uint64    `json:"offset"`
	Key       []byte    `json:"key"`
	Value     []byte    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

type Log struct {
	mu          sync.RWMutex
	startOffset uint64
	records     []Record
}

func (l *Log) Append(key, value []byte) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	offset := l.startOffset + uint64(len(l.records))
	l.appendRecordLocked(Record{
		Offset:    offset,
		Key:       clone(key),
		Value:     clone(value),
		Timestamp: time.Now().UTC(),
	})
	return offset
}

func (l *Log) AppendRecord(record Record) error {
	return l.AppendRecords([]Record{record})
}

func (l *Log) AppendRecords(records []Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	endOffset := l.startOffset + uint64(len(l.records))
	if uint64(len(records)) > ^uint64(0)-endOffset {
		return errors.New("record offset overflow")
	}
	for i, record := range records {
		want := endOffset + uint64(i)
		if record.Offset != want {
			return fmt.Errorf("%w: got %d, want %d", ErrOffsetMismatch, record.Offset, want)
		}
	}

	cloned := make([]Record, len(records))
	for i, record := range records {
		record.Key = clone(record.Key)
		record.Value = clone(record.Value)
		cloned[i] = record
	}
	l.records = append(l.records, cloned...)
	return nil
}

func (l *Log) appendRecordLocked(record Record) {
	l.records = append(l.records, record)
}

func (l *Log) Read(offset uint64, limit int) ([]Record, error) {
	if limit <= 0 {
		return nil, nil
	}

	l.mu.RLock()
	defer l.mu.RUnlock()
	if offset < l.startOffset {
		return nil, fmt.Errorf("%w: requested %d, start is %d", ErrOffsetExpired, offset, l.startOffset)
	}
	return l.readLocked(offset, limit), nil
}

func (l *Log) ReadFromStart(limit int) ([]Record, error) {
	if limit <= 0 {
		return nil, nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.readLocked(l.startOffset, limit), nil
}

func (l *Log) readLocked(offset uint64, limit int) []Record {
	endOffset := l.startOffset + uint64(len(l.records))
	if offset >= endOffset {
		return nil
	}

	start := int(offset - l.startOffset)
	end := len(l.records)
	if limit < end-start {
		end = start + limit
	}

	result := make([]Record, end-start)
	for i, record := range l.records[start:end] {
		result[i] = Record{
			Offset:    record.Offset,
			Key:       clone(record.Key),
			Value:     clone(record.Value),
			Timestamp: record.Timestamp,
		}
	}
	return result
}

func (l *Log) RetainFrom(startOffset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if startOffset < l.startOffset {
		return ErrStartOffsetRegression
	}
	endOffset := l.startOffset + uint64(len(l.records))
	if startOffset > endOffset {
		return ErrStartOffsetOutOfRange
	}
	drop := int(startOffset - l.startOffset)
	if drop > 0 {
		l.records = append([]Record(nil), l.records[drop:]...)
		l.startOffset = startOffset
	}
	return nil
}

func (l *Log) StartOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.startOffset
}

func (l *Log) Snapshot() (startOffset, endOffset uint64, records []Record) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	startOffset = l.startOffset
	endOffset = l.startOffset + uint64(len(l.records))
	records = append([]Record(nil), l.records...)
	return startOffset, endOffset, records
}

func Restore(startOffset, endOffset uint64, records []Record) (*Log, error) {
	if err := validateRestoredRange(startOffset, endOffset, records); err != nil {
		return nil, err
	}
	copyOfRecords := make([]Record, len(records))
	for i, record := range records {
		record.Key = clone(record.Key)
		record.Value = clone(record.Value)
		copyOfRecords[i] = record
	}
	return &Log{startOffset: startOffset, records: copyOfRecords}, nil
}

func RestoreOwned(startOffset, endOffset uint64, records []Record) (*Log, error) {
	if err := validateRestoredRange(startOffset, endOffset, records); err != nil {
		return nil, err
	}
	return &Log{startOffset: startOffset, records: records}, nil
}

func validateRestoredRange(startOffset, endOffset uint64, records []Record) error {
	if endOffset < startOffset || uint64(len(records)) != endOffset-startOffset {
		return errors.New("snapshot log range does not match its record count")
	}
	for i, record := range records {
		if record.Offset != startOffset+uint64(i) {
			return fmt.Errorf("snapshot record offset %d is not contiguous at %d", record.Offset, startOffset+uint64(i))
		}
	}
	return nil
}

func (l *Log) EndOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.startOffset + uint64(len(l.records))
}

func clone(value []byte) []byte {
	return append([]byte(nil), value...)
}
