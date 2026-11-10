package wal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	messageLog "mq/internal/log"
)

const (
	headerSize                = 13
	maxFramePayload           = 1 << 20
	MaxGroupCommitBytes int64 = 4 << 20
	MinSegmentBytes     int64 = MaxGroupCommitBytes
	DefaultSegmentBytes int64 = 64 << 20
	maxStringBytes            = 1<<16 - 1
	maxBatchRecords           = 1024
	kindPublish         byte  = 1
	kindCommit          byte  = 2
	kindBatch           byte  = 3
	kindIdempotentBatch byte  = 4
	kindRetention       byte  = 5
)

var (
	ErrClosed      = errors.New("WAL is closed")
	ErrUnavailable = errors.New("WAL is unavailable after a write failure")
	frameMagic     = [4]byte{'M', 'Q', 'W', '1'}
	crcTable       = crc32.MakeTable(crc32.Castagnoli)
)

type EntryKind uint8

const (
	PublishEntry EntryKind = iota + 1
	CommitEntry
	BatchEntry
	IdempotentBatchEntry
	RetentionEntry
)

type Entry struct {
	Kind        EntryKind
	Topic       string
	Group       string
	RequestID   string
	Record      messageLog.Record
	Records     []messageLog.Record
	NextOffset  uint64
	StartOffset uint64
}

type EntryEncodingError struct {
	Index int
	Err   error
}

func (e *EntryEncodingError) Error() string {
	return fmt.Sprintf("encode WAL group entry %d: %v", e.Index, e.Err)
}

func (e *EntryEncodingError) Unwrap() error { return e.Err }

type walSegment struct {
	sequence uint64
	path     string
	file     *os.File
	start    int64
	size     int64
}

type WAL struct {
	mu           sync.Mutex
	path         string
	segmentBytes int64
	segments     []*walSegment
	readSegment  *walSegment
	readFile     *os.File
	totalSize    int64
	base         int64
	failed       error
	closed       bool
	closeErr     error
}

func Open(path string) (*WAL, []Entry, error) {
	var entries []Entry
	store, err := OpenAndReplay(path, func(entry Entry) error {
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return store, entries, nil
}

func OpenAndReplay(path string, apply func(Entry) error) (*WAL, error) {
	return OpenAndReplayWithSegmentBytes(path, DefaultSegmentBytes, apply)
}

func OpenAndReplayWithSegmentBytes(path string, segmentBytes int64, apply func(Entry) error) (*WAL, error) {
	return OpenAndReplayFrom(path, segmentBytes, 0, apply)
}

func OpenAndReplayFrom(path string, segmentBytes, checkpoint int64, apply func(Entry) error) (*WAL, error) {
	if apply == nil {
		return nil, errors.New("WAL replay callback is required")
	}
	if checkpoint < 0 {
		return nil, errors.New("WAL replay checkpoint cannot be negative")
	}
	if segmentBytes == 0 {
		segmentBytes = DefaultSegmentBytes
	}
	if segmentBytes < MinSegmentBytes {
		return nil, fmt.Errorf("WAL segment size must be at least %d bytes", MinSegmentBytes)
	}

	if err := os.Remove(walBaseStagePath(path)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("discard stale WAL base marker stage: %w", err)
	}
	marker, err := readBaseMarker(path)
	if err != nil {
		return nil, err
	}
	paths, err := listSegmentFiles(path)
	if err != nil {
		return nil, err
	}
	base := int64(0)
	if marker != nil {
		if marker.freshPlan {
			if checkpoint > marker.checkpoint {
				return nil, fmt.Errorf("%w: checkpoint %d exceeds fresh stream base %d", ErrCheckpointExceedsStream, checkpoint, marker.checkpoint)
			}
			if len(paths) > 0 {
				if err := removeWALFiles(path); err != nil {
					return nil, err
				}
				paths = nil
			}
			base = marker.checkpoint
		} else {
			base = marker.checkpoint
		}
	}
	effectiveCheckpoint := checkpoint
	if base > effectiveCheckpoint {
		effectiveCheckpoint = base
	}

	w := &WAL{path: path, segmentBytes: segmentBytes, base: base}
	if len(paths) == 0 {
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return nil, fmt.Errorf("open WAL segment: %w", err)
		}
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return nil, errors.Join(fmt.Errorf("sync WAL directory: %w", err), file.Close())
		}
		firstStart := int64(0)
		if marker != nil {
			firstStart = marker.prefixStart
		}
		w.segments = append(w.segments, &walSegment{path: path, file: file, start: firstStart})
		w.totalSize = firstStart
		if marker != nil && marker.freshPlan {
			if err := writeBaseMarker(path, walBaseMarker{
				checkpoint:  marker.checkpoint,
				prefixStart: marker.prefixStart,
				survivorSeq: 0,
			}); err != nil {
				return nil, errors.Join(err, w.closeFiles())
			}
		}
	} else {
		starts := make([]int64, len(paths))
		plannedCount := 0
		if marker != nil && !marker.freshPlan {
			var planErr error
			plannedCount, planErr = planTrimmedStarts(paths, marker, starts)
			if planErr != nil {
				return nil, planErr
			}
		} else if marker == nil {
			if paths[0].sequence != 0 {
				return nil, errors.New("WAL segment zero is missing")
			}
		}
		current := int64(0)
		for index, item := range paths {
			if marker != nil && !marker.freshPlan {
				current = starts[index]
			}
			segment, err := w.openReplaySegment(item, current, index == len(paths)-1, effectiveCheckpoint, apply)
			if err != nil {
				return nil, err
			}
			current = segment.start + segment.size
		}
		w.totalSize = current
		if marker != nil && !marker.freshPlan && plannedCount > 0 {
			if err := w.dropPrefixSegmentsLocked(plannedCount); err != nil {
				return nil, errors.Join(err, w.closeFiles())
			}
		}
	}
	if effectiveCheckpoint > w.totalSize {
		return nil, fmt.Errorf("%w: checkpoint %d exceeds WAL size %d", ErrCheckpointExceedsStream, effectiveCheckpoint, w.totalSize)
	}
	if err := w.removeEmptyTrailingSegments(); err != nil {
		return nil, errors.Join(err, w.closeFiles())
	}
	if len(w.segments) > 0 {
		w.recomputeSegmentStarts()
	}
	if _, err := w.segments[len(w.segments)-1].file.Seek(0, io.SeekEnd); err != nil {
		return nil, errors.Join(fmt.Errorf("seek WAL end: %w", err), w.closeFiles())
	}
	return w, nil
}

func planTrimmedStarts(paths []segmentPath, marker *walBaseMarker, starts []int64) (int, error) {
	sizes := make([]int64, len(paths))
	planned := -1
	for index, item := range paths {
		info, err := os.Stat(item.path)
		if err != nil {
			return 0, fmt.Errorf("stat WAL segment %q: %w", item.path, err)
		}
		sizes[index] = info.Size()
		if item.sequence < marker.survivorSeq {
			planned = index
		}
	}
	if planned >= 0 {
		for count := 0; count <= planned; count++ {
			index := planned - count
			want := marker.survivorSeq - uint64(count+1)
			if paths[index].sequence != want {
				return 0, errors.New("WAL segment deletion state is inconsistent with the base marker")
			}
		}
		end := marker.prefixStart
		for index := planned; index >= 0; index-- {
			if end-sizes[index] < 0 {
				return 0, errors.New("WAL segment deletion state is inconsistent with the base marker")
			}
			starts[index] = end - sizes[index]
			end = starts[index]
		}
	}
	survivorIndex := planned + 1
	plannedCount := survivorIndex
	if survivorIndex >= len(paths) || paths[survivorIndex].sequence != marker.survivorSeq {
		return 0, errors.New("WAL segment referenced by the base marker is missing")
	}
	current := marker.prefixStart
	for index := survivorIndex; index < len(paths); index++ {
		if paths[index].sequence != marker.survivorSeq+uint64(index-survivorIndex) {
			return 0, fmt.Errorf("WAL segment sequence has a gap before %q", paths[index].path)
		}
		starts[index] = current
		current += sizes[index]
	}
	return plannedCount, nil
}

type segmentPath struct {
	sequence uint64
	path     string
}

func (w *WAL) openReplaySegment(item segmentPath, start int64, isLast bool, checkpoint int64, apply func(Entry) error) (*walSegment, error) {
	file, err := os.OpenFile(item.path, os.O_RDWR, 0o600)
	if err != nil {
		_ = w.closeFiles()
		return nil, fmt.Errorf("open WAL segment %q: %w", item.path, err)
	}
	info, err := file.Stat()
	if err != nil {
		closeErr := file.Close()
		_ = w.closeFiles()
		return nil, errors.Join(fmt.Errorf("stat WAL segment %q: %w", item.path, err), closeErr)
	}
	segment := &walSegment{
		sequence: item.sequence,
		path:     item.path,
		file:     file,
		start:    start,
		size:     info.Size(),
	}
	w.segments = append(w.segments, segment)
	if err := w.replaySegment(segment, isLast, checkpoint, apply); err != nil {
		return nil, err
	}
	if !isLast {
		if segment.size == 0 {
			return nil, fmt.Errorf("empty WAL segment before final segment: %q", segment.path)
		}
		if err := segment.file.Close(); err != nil {
			segment.file = nil
			return nil, errors.Join(fmt.Errorf("close replayed WAL segment %q: %w", segment.path, err), w.closeFiles())
		}
		segment.file = nil
	}
	return segment, nil
}

func (w *WAL) replaySegment(segment *walSegment, isLast bool, checkpoint int64, apply func(Entry) error) error {
	var position int64
	for position < segment.size {
		frameStart := position
		remaining := segment.size - frameStart
		if remaining < headerSize {
			if isLast {
				return w.truncateSegmentTail(segment, frameStart)
			}
			return fmt.Errorf("incomplete WAL frame before final segment at byte %d", segment.start+frameStart)
		}

		var header [headerSize]byte
		if _, err := segment.file.ReadAt(header[:], frameStart); err != nil {
			return fmt.Errorf("read WAL header at byte %d: %w", segment.start+frameStart, err)
		}
		if !bytes.Equal(header[:4], frameMagic[:]) {
			return fmt.Errorf("invalid WAL magic at byte %d", segment.start+frameStart)
		}
		payloadSize := binary.LittleEndian.Uint32(header[5:9])
		if payloadSize > maxFramePayload {
			return fmt.Errorf("WAL frame at byte %d exceeds maximum size", segment.start+frameStart)
		}
		frameSize := int64(headerSize) + int64(payloadSize)
		if frameSize > remaining {
			if isLast {
				return w.truncateSegmentTail(segment, frameStart)
			}
			return fmt.Errorf("incomplete WAL frame before final segment at byte %d", segment.start+frameStart)
		}

		payload := make([]byte, int(payloadSize))
		if payloadSize > 0 {
			if _, err := segment.file.ReadAt(payload, frameStart+headerSize); err != nil {
				return fmt.Errorf("read WAL payload at byte %d: %w", segment.start+frameStart, err)
			}
		}
		checksum := crc32.Update(0, crcTable, header[4:9])
		checksum = crc32.Update(checksum, crcTable, payload)
		if got, want := checksum, binary.LittleEndian.Uint32(header[9:headerSize]); got != want {
			return fmt.Errorf("WAL checksum mismatch at byte %d", segment.start+frameStart)
		}
		entry, err := decodeEntry(header[4], payload)
		if err != nil {
			return fmt.Errorf("decode WAL frame at byte %d: %w", segment.start+frameStart, err)
		}
		absoluteStart := segment.start + frameStart
		absoluteEnd := absoluteStart + frameSize
		if absoluteStart < checkpoint && checkpoint < absoluteEnd {
			return fmt.Errorf("WAL checkpoint %d is not on a frame boundary", checkpoint)
		}
		if absoluteStart >= checkpoint {
			if err := apply(entry); err != nil {
				return fmt.Errorf("apply WAL frame at byte %d: %w", absoluteStart, err)
			}
		}
		position += frameSize
	}
	segment.size = position
	return nil
}

func (w *WAL) truncateSegmentTail(segment *walSegment, offset int64) error {
	if err := segment.file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate incomplete WAL tail: %w", err)
	}
	if err := segment.file.Sync(); err != nil {
		return fmt.Errorf("sync truncated WAL: %w", err)
	}
	segment.size = offset
	return nil
}

func (w *WAL) removeEmptyTrailingSegments() error {
	removed := false
	for len(w.segments) > 1 && w.segments[len(w.segments)-1].size == 0 {
		segment := w.segments[len(w.segments)-1]
		if segment.file != nil {
			if err := segment.file.Close(); err != nil {
				return fmt.Errorf("close empty WAL segment: %w", err)
			}
			segment.file = nil
		}
		if err := os.Remove(segment.path); err != nil {
			return fmt.Errorf("remove empty WAL segment: %w", err)
		}
		w.segments = w.segments[:len(w.segments)-1]
		removed = true
	}
	if removed {
		if err := syncDirectory(filepath.Dir(w.path)); err != nil {
			return fmt.Errorf("sync WAL segment removal: %w", err)
		}
		segment := w.segments[len(w.segments)-1]
		file, err := os.OpenFile(segment.path, os.O_RDWR, 0o600)
		if err != nil {
			return fmt.Errorf("reopen final WAL segment %q: %w", segment.path, err)
		}
		segment.file = file
	}
	return nil
}

func (w *WAL) recomputeSegmentStarts() {
	if len(w.segments) == 0 {
		w.totalSize = 0
		return
	}
	w.totalSize = w.segments[0].start
	for _, segment := range w.segments {
		segment.start = w.totalSize
		w.totalSize += segment.size
	}
}

func (w *WAL) closeFiles() error {
	closeErr := error(nil)
	if w.readFile != nil {
		closeErr = errors.Join(closeErr, w.readFile.Close())
		w.readFile = nil
		w.readSegment = nil
	}
	for _, segment := range w.segments {
		if segment.file != nil {
			closeErr = errors.Join(closeErr, segment.file.Close())
			segment.file = nil
		}
	}
	return closeErr
}

func (w *WAL) createSegmentLocked() (*walSegment, error) {
	previous := w.segments[len(w.segments)-1]
	if previous.sequence == ^uint64(0) {
		return nil, errors.New("WAL segment sequence is exhausted")
	}
	sequence := previous.sequence + 1
	path := fmt.Sprintf("%s.%020d", w.path, sequence)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create WAL segment %q: %w", path, err)
	}
	segment := &walSegment{sequence: sequence, path: path, file: file, start: w.totalSize}
	w.segments = append(w.segments, segment)
	if err := syncDirectory(filepath.Dir(w.path)); err != nil {
		return segment, fmt.Errorf("sync WAL segment directory: %w", err)
	}
	return segment, nil
}

func (w *WAL) fileForReadLocked(segment *walSegment) (*os.File, error) {
	if segment.file != nil {
		return segment.file, nil
	}
	if w.readSegment == segment && w.readFile != nil {
		return w.readFile, nil
	}
	if w.readFile != nil {
		if err := w.readFile.Close(); err != nil {
			return nil, fmt.Errorf("close cached WAL segment reader: %w", err)
		}
		w.readFile = nil
		w.readSegment = nil
	}
	file, err := os.Open(segment.path)
	if err != nil {
		return nil, fmt.Errorf("open WAL segment for read: %w", err)
	}
	w.readSegment = segment
	w.readFile = file
	return file, nil
}

func (w *WAL) AppendPublish(topic string, record messageLog.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.readyLocked(); err != nil {
		return err
	}

	payload, err := encodePublish(topic, record)
	if err != nil {
		return err
	}
	return w.appendFrameLocked(kindPublish, payload)
}

func (w *WAL) AppendBatch(topic string, records []messageLog.Record) error {
	if len(records) == 1 {
		return w.AppendPublish(topic, records[0])
	}
	if len(records) < 2 {
		return errors.New("WAL batch must contain at least two records")
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.readyLocked(); err != nil {
		return err
	}

	payload, err := encodeBatch(topic, records)
	if err != nil {
		return err
	}
	return w.appendFrameLocked(kindBatch, payload)
}

func (w *WAL) AppendIdempotentBatch(requestID, topic string, records []messageLog.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.readyLocked(); err != nil {
		return err
	}

	payload, err := encodeIdempotentBatch(requestID, topic, records)
	if err != nil {
		return err
	}
	return w.appendFrameLocked(kindIdempotentBatch, payload)
}

func (w *WAL) AppendCommit(group, topic string, nextOffset uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.readyLocked(); err != nil {
		return err
	}

	payload, err := encodeCommit(group, topic, nextOffset)
	if err != nil {
		return err
	}
	return w.appendFrameLocked(kindCommit, payload)
}

func (w *WAL) AppendRetention(topic string, startOffset uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.readyLocked(); err != nil {
		return err
	}
	payload, err := encodeRetention(topic, startOffset)
	if err != nil {
		return err
	}
	return w.appendFrameLocked(kindRetention, payload)
}

func (w *WAL) AppendGroup(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.readyLocked(); err != nil {
		return err
	}

	frames := make([]byte, 0, len(entries)*128)
	for i, entry := range entries {
		kind, payload, err := encodeEntry(entry)
		if err != nil {
			return &EntryEncodingError{Index: i, Err: err}
		}
		frames = appendFrame(frames, kind, payload)
	}

	return w.appendFramesLocked(frames)
}

func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.readyLocked(); err != nil {
		return 0, err
	}
	return w.totalSize, nil
}

func (w *WAL) HashPrefix(offset int64) ([sha256.Size]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var digest [sha256.Size]byte
	if err := w.readyLocked(); err != nil {
		return digest, err
	}
	if offset < 0 || offset > w.totalSize {
		return digest, fmt.Errorf("WAL prefix offset %d is outside WAL size %d", offset, w.totalSize)
	}
	if offset < w.base {
		return digest, fmt.Errorf("WAL prefix offset %d is below the durable prefix %d", offset, w.base)
	}

	hasher := sha256.New()
	position := w.base
	for position < offset {
		frame, _, err := w.readFrameAtLocked(position)
		if err != nil {
			return digest, err
		}
		if position+int64(len(frame)) > offset {
			return digest, fmt.Errorf("WAL prefix offset %d is not on a frame boundary", offset)
		}
		_, _ = hasher.Write(frame)
		position += int64(len(frame))
	}
	if position != offset {
		return digest, fmt.Errorf("WAL prefix offset %d is not on a frame boundary", offset)
	}
	copy(digest[:], hasher.Sum(nil))
	return digest, nil
}

func (w *WAL) ReadFrames(offset int64, maxBytes int) ([]byte, error) {
	return w.ReadFramesUntil(offset, -1, maxBytes)
}

func (w *WAL) ReadFramesUntil(offset, end int64, maxBytes int) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.readyLocked(); err != nil {
		return nil, err
	}
	if end < 0 {
		end = w.totalSize
	}
	if offset < 0 || offset > end || end > w.totalSize {
		return nil, fmt.Errorf("WAL read range %d..%d is outside WAL size %d", offset, end, w.totalSize)
	}
	if offset < w.base {
		return nil, fmt.Errorf("WAL read offset %d is below the durable prefix %d", offset, w.base)
	}
	if offset == end {
		return []byte{}, nil
	}
	if maxBytes <= 0 {
		return nil, errors.New("WAL frame read limit must be positive")
	}

	capacity := maxBytes
	if available := end - offset; available < int64(capacity) {
		capacity = int(available)
	}
	result := make([]byte, 0, capacity)
	position := offset
	for position < end {
		frame, _, err := w.readFrameAtLocked(position)
		if err != nil {
			return nil, err
		}
		if position+int64(len(frame)) > end {
			return nil, fmt.Errorf("WAL read end %d is not on a frame boundary", end)
		}
		if len(result) > 0 && len(result)+len(frame) > maxBytes {
			break
		}
		result = append(result, frame...)
		position += int64(len(frame))
	}
	return result, nil
}

func (w *WAL) AppendFrames(offset int64, data []byte) ([]Entry, error) {
	return w.AppendFramesChecked(offset, data, nil)
}

func (w *WAL) AppendFramesChecked(offset int64, data []byte, validate func([]Entry) error) ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.readyLocked(); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, errors.New("replication offset cannot be negative")
	}
	if offset < w.base {
		return nil, fmt.Errorf("replication offset %d is below the durable prefix %d", offset, w.base)
	}
	entries, frameEnds, err := decodeFrames(data)
	if err != nil {
		return nil, fmt.Errorf("validate replicated WAL frames: %w", err)
	}
	if len(data) == 0 {
		return nil, errors.New("replication frame body must not be empty")
	}

	if offset > w.totalSize {
		return nil, fmt.Errorf("replication gap: follower ends at %d, stream starts at %d", w.totalSize, offset)
	}
	overlap := w.totalSize - offset
	if overlap > int64(len(data)) {
		overlap = int64(len(data))
	}
	if overlap > 0 {
		existing := make([]byte, int(overlap))
		if err := w.readLogicalAtLocked(offset, existing); err != nil {
			return nil, fmt.Errorf("read existing WAL prefix: %w", err)
		}
		if !bytes.Equal(existing, data[:overlap]) {
			return nil, errors.New("replicated WAL does not match follower prefix")
		}
	}

	if overlap == int64(len(data)) {
		return nil, nil
	}
	firstNewFrame := 0
	for firstNewFrame < len(frameEnds) && frameEnds[firstNewFrame] <= int(overlap) {
		firstNewFrame++
	}
	if firstNewFrame == len(frameEnds) || (overlap > 0 && (firstNewFrame == 0 || frameEnds[firstNewFrame-1] != int(overlap))) {
		return nil, errors.New("replication overlap ends inside a WAL frame")
	}
	newEntries := entries[firstNewFrame:]
	if validate != nil {
		if err := validate(newEntries); err != nil {
			return nil, fmt.Errorf("replicated entries are invalid: %w", err)
		}
	}

	if err := w.appendFramesLocked(data[overlap:]); err != nil {
		return nil, err
	}
	return newEntries, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.closeErr
	}
	w.closed = true

	closeErr := w.failed
	if closeErr == nil {
		for _, segment := range w.segments {
			if segment.file != nil {
				if err := segment.file.Sync(); err != nil {
					closeErr = errors.Join(closeErr, err)
				}
			}
		}
	}
	w.closeErr = errors.Join(closeErr, w.closeFiles())
	if w.closeErr != nil {
		w.closeErr = fmt.Errorf("close WAL: %w", w.closeErr)
	}
	return w.closeErr
}

func (w *WAL) readyLocked() error {
	if w.closed {
		return ErrClosed
	}
	if w.failed != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, w.failed)
	}
	return nil
}

func (w *WAL) appendFrameLocked(kind byte, payload []byte) error {
	frame := appendFrame(nil, kind, payload)
	return w.appendFramesLocked(frame)
}

func (w *WAL) appendFramesLocked(frames []byte) error {
	var touched []*walSegment
	if int64(len(frames)) <= w.segmentBytes {
		segment := w.segments[len(w.segments)-1]
		if segment.size > 0 && segment.size+int64(len(frames)) > w.segmentBytes {
			if _, err := w.createSegmentLocked(); err != nil {
				return w.poisonLocked(err)
			}
		}
	}
	for position := 0; position < len(frames); {
		if len(frames)-position < headerSize {
			return w.poisonLocked(errors.New("incomplete frame in WAL append"))
		}
		payloadSize := binary.LittleEndian.Uint32(frames[position+5 : position+9])
		if payloadSize > maxFramePayload {
			return w.poisonLocked(errors.New("WAL append frame exceeds maximum size"))
		}
		frameSize := headerSize + int(payloadSize)
		if frameSize > len(frames)-position {
			return w.poisonLocked(errors.New("incomplete frame in WAL append"))
		}

		segment := w.segments[len(w.segments)-1]
		if segment.size > 0 && segment.size+int64(frameSize) > w.segmentBytes {
			var err error
			segment, err = w.createSegmentLocked()
			if err != nil {
				return w.poisonLocked(err)
			}
		}
		if _, err := segment.file.Seek(0, io.SeekEnd); err != nil {
			return w.poisonLocked(err)
		}
		frame := frames[position : position+frameSize]
		if err := writeFull(segment.file, frame); err != nil {
			return w.poisonLocked(err)
		}
		segment.size += int64(frameSize)
		w.totalSize += int64(frameSize)
		if len(touched) == 0 || touched[len(touched)-1] != segment {
			touched = append(touched, segment)
		}
		position += frameSize
	}
	for _, segment := range touched {
		if err := segment.file.Sync(); err != nil {
			return w.poisonLocked(err)
		}
	}
	for _, segment := range w.segments[:len(w.segments)-1] {
		if segment.file == nil {
			continue
		}
		if err := segment.file.Close(); err != nil {
			segment.file = nil
			return w.poisonLocked(fmt.Errorf("close sealed WAL segment: %w", err))
		}
		segment.file = nil
	}
	return nil
}

func (w *WAL) readFrameAtLocked(offset int64) ([]byte, Entry, error) {
	index := sort.Search(len(w.segments), func(index int) bool {
		segment := w.segments[index]
		return segment.start+segment.size > offset
	})
	if index == len(w.segments) {
		return nil, Entry{}, io.ErrUnexpectedEOF
	}
	segment := w.segments[index]
	file, err := w.fileForReadLocked(segment)
	if err != nil {
		return nil, Entry{}, fmt.Errorf("read WAL at logical byte %d: %w", offset, err)
	}
	frame, entry, err := readFrameAt(file, offset-segment.start, segment.size)
	if err != nil {
		return nil, Entry{}, fmt.Errorf("read WAL at logical byte %d: %w", offset, err)
	}
	return frame, entry, nil
}

func (w *WAL) readLogicalAtLocked(offset int64, destination []byte) error {
	if offset < 0 || offset > w.totalSize || int64(len(destination)) > w.totalSize-offset {
		return errors.New("WAL read range is outside the logical stream")
	}
	position := offset
	read := 0
	for read < len(destination) {
		index := sort.Search(len(w.segments), func(index int) bool {
			segment := w.segments[index]
			return segment.start+segment.size > position
		})
		if index == len(w.segments) {
			return io.ErrUnexpectedEOF
		}
		segment := w.segments[index]
		localOffset := position - segment.start
		available := segment.size - localOffset
		count := len(destination) - read
		if int64(count) > available {
			count = int(available)
		}
		file, err := w.fileForReadLocked(segment)
		if err != nil {
			return err
		}
		n, err := file.ReadAt(destination[read:read+count], localOffset)
		read += n
		position += int64(n)
		if err != nil {
			return err
		}
		if n != count {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

func appendFrame(destination []byte, kind byte, payload []byte) []byte {
	start := len(destination)
	frameSize := headerSize + len(payload)
	needed := start + frameSize
	if cap(destination) < needed {
		newCap := 2 * cap(destination)
		if newCap < needed {
			newCap = needed
		}
		newDest := make([]byte, needed, newCap)
		copy(newDest, destination)
		destination = newDest
	} else {
		destination = destination[:needed]
	}
	frame := destination[start:needed]
	copy(frame[:4], frameMagic[:])
	frame[4] = kind
	binary.LittleEndian.PutUint32(frame[5:9], uint32(len(payload)))
	copy(frame[headerSize:], payload)

	c := crc32.Update(0, crcTable, frame[4:9])
	c = crc32.Update(c, crcTable, payload)
	binary.LittleEndian.PutUint32(frame[9:headerSize], c)
	return destination
}

func encodeEntry(entry Entry) (byte, []byte, error) {
	switch entry.Kind {
	case PublishEntry:
		payload, err := encodePublish(entry.Topic, entry.Record)
		return kindPublish, payload, err
	case CommitEntry:
		payload, err := encodeCommit(entry.Group, entry.Topic, entry.NextOffset)
		return kindCommit, payload, err
	case BatchEntry:
		if len(entry.Records) < 2 {
			return 0, nil, errors.New("WAL batch must contain at least two records")
		}
		payload, err := encodeBatch(entry.Topic, entry.Records)
		return kindBatch, payload, err
	case IdempotentBatchEntry:
		payload, err := encodeIdempotentBatch(entry.RequestID, entry.Topic, entry.Records)
		return kindIdempotentBatch, payload, err
	case RetentionEntry:
		payload, err := encodeRetention(entry.Topic, entry.StartOffset)
		return kindRetention, payload, err
	default:
		return 0, nil, fmt.Errorf("unknown WAL entry kind %d", entry.Kind)
	}
}

func (w *WAL) poisonLocked(err error) error {
	w.failed = fmt.Errorf("append WAL frame: %w", err)
	return fmt.Errorf("%w: %v", ErrUnavailable, w.failed)
}

func readFrameAt(file *os.File, offset, fileSize int64) ([]byte, Entry, error) {
	if offset < 0 || offset > fileSize || fileSize-offset < headerSize {
		return nil, Entry{}, io.ErrUnexpectedEOF
	}
	var header [headerSize]byte
	if _, err := file.ReadAt(header[:], offset); err != nil {
		return nil, Entry{}, fmt.Errorf("read WAL frame header at %d: %w", offset, err)
	}
	payloadSize := binary.LittleEndian.Uint32(header[5:9])
	if payloadSize > maxFramePayload {
		return nil, Entry{}, fmt.Errorf("WAL frame at byte %d exceeds maximum size", offset)
	}
	frameSize := headerSize + int(payloadSize)
	if int64(frameSize) > fileSize-offset {
		return nil, Entry{}, io.ErrUnexpectedEOF
	}
	frame := make([]byte, frameSize)
	copy(frame, header[:])
	if payloadSize > 0 {
		if _, err := file.ReadAt(frame[headerSize:], offset+headerSize); err != nil {
			return nil, Entry{}, fmt.Errorf("read WAL frame payload at %d: %w", offset, err)
		}
	}
	entry, err := decodeFrame(frame)
	if err != nil {
		return nil, Entry{}, fmt.Errorf("decode WAL frame at byte %d: %w", offset, err)
	}
	return frame, entry, nil
}

func decodeFrames(data []byte) ([]Entry, []int, error) {
	var entries []Entry
	var frameEnds []int
	for position := 0; position < len(data); {
		if len(data)-position < headerSize {
			return nil, nil, io.ErrUnexpectedEOF
		}
		payloadSize := binary.LittleEndian.Uint32(data[position+5 : position+9])
		if payloadSize > maxFramePayload {
			return nil, nil, fmt.Errorf("WAL frame at byte %d exceeds maximum size", position)
		}
		frameSize := headerSize + int(payloadSize)
		if frameSize > len(data)-position {
			return nil, nil, io.ErrUnexpectedEOF
		}
		entry, err := decodeFrame(data[position : position+frameSize])
		if err != nil {
			return nil, nil, fmt.Errorf("decode WAL frame at byte %d: %w", position, err)
		}
		position += frameSize
		entries = append(entries, entry)
		frameEnds = append(frameEnds, position)
	}
	return entries, frameEnds, nil
}

func decodeFrame(frame []byte) (Entry, error) {
	if len(frame) < headerSize {
		return Entry{}, io.ErrUnexpectedEOF
	}
	if !bytes.Equal(frame[:4], frameMagic[:]) {
		return Entry{}, errors.New("invalid WAL magic")
	}
	payloadSize := binary.LittleEndian.Uint32(frame[5:9])
	if payloadSize > maxFramePayload {
		return Entry{}, errors.New("WAL frame exceeds maximum size")
	}
	if len(frame) != headerSize+int(payloadSize) {
		return Entry{}, errors.New("WAL frame length does not match header")
	}
	payload := frame[headerSize:]
	c := crc32.Update(0, crcTable, frame[4:9])
	c = crc32.Update(c, crcTable, payload)
	if got, want := c, binary.LittleEndian.Uint32(frame[9:headerSize]); got != want {
		return Entry{}, errors.New("WAL checksum mismatch")
	}
	return decodeEntry(frame[4], payload)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func encodePublish(topic string, record messageLog.Record) ([]byte, error) {
	if len(topic) > maxStringBytes {
		return nil, errors.New("topic exceeds WAL string limit")
	}
	const fixedBytes = 2 + 8 + 8 + 4 + 4
	remaining := maxFramePayload - fixedBytes - len(topic)
	if remaining < 0 || len(record.Key) > remaining || len(record.Value) > remaining-len(record.Key) {
		return nil, errors.New("record exceeds WAL frame limit")
	}

	payload := make([]byte, 0, fixedBytes+len(topic)+len(record.Key)+len(record.Value))
	payload = appendString(payload, topic)
	return appendEncodedRecord(payload, record), nil
}

func encodeBatch(topic string, records []messageLog.Record) ([]byte, error) {
	if len(topic) > maxStringBytes {
		return nil, errors.New("topic exceeds WAL string limit")
	}
	if len(records) > maxBatchRecords {
		return nil, fmt.Errorf("batch exceeds maximum of %d records", maxBatchRecords)
	}

	payloadSize := 2 + len(topic) + 4
	for i, record := range records {
		if i > 0 && record.Offset != records[0].Offset+uint64(i) {
			return nil, errors.New("WAL batch offsets must be contiguous")
		}
		const recordFixedBytes = 8 + 8 + 4 + 4
		remaining := maxFramePayload - payloadSize - recordFixedBytes
		if remaining < 0 || len(record.Key) > remaining || len(record.Value) > remaining-len(record.Key) {
			return nil, errors.New("batch exceeds WAL frame limit")
		}
		payloadSize += recordFixedBytes + len(record.Key) + len(record.Value)
	}

	payload := make([]byte, 0, payloadSize)
	payload = appendString(payload, topic)
	payload = appendUint32(payload, uint32(len(records)))
	for _, record := range records {
		payload = appendEncodedRecord(payload, record)
	}
	return payload, nil
}

func encodeIdempotentBatch(requestID, topic string, records []messageLog.Record) ([]byte, error) {
	if len(requestID) == 0 || len(requestID) > maxStringBytes {
		return nil, errors.New("request ID must fit in a non-empty WAL string")
	}
	if len(topic) == 0 || len(topic) > maxStringBytes {
		return nil, errors.New("topic must fit in a non-empty WAL string")
	}
	if len(records) == 0 || len(records) > maxBatchRecords {
		return nil, fmt.Errorf("idempotent batch must contain 1 to %d records", maxBatchRecords)
	}

	payloadSize := 2 + len(requestID) + 2 + len(topic) + 4
	for i, record := range records {
		if i > 0 && record.Offset != records[0].Offset+uint64(i) {
			return nil, errors.New("WAL batch offsets must be contiguous")
		}
		const recordFixedBytes = 8 + 8 + 4 + 4
		remaining := maxFramePayload - payloadSize - recordFixedBytes
		if remaining < 0 || len(record.Key) > remaining || len(record.Value) > remaining-len(record.Key) {
			return nil, errors.New("idempotent batch exceeds WAL frame limit")
		}
		payloadSize += recordFixedBytes + len(record.Key) + len(record.Value)
	}

	payload := make([]byte, 0, payloadSize)
	payload = appendString(payload, requestID)
	payload = appendString(payload, topic)
	payload = appendUint32(payload, uint32(len(records)))
	for _, record := range records {
		payload = appendEncodedRecord(payload, record)
	}
	return payload, nil
}

func appendEncodedRecord(payload []byte, record messageLog.Record) []byte {
	payload = appendUint64(payload, record.Offset)
	payload = appendUint64(payload, uint64(record.Timestamp.UnixNano()))
	payload = appendUint32(payload, uint32(len(record.Key)))
	payload = append(payload, record.Key...)
	payload = appendUint32(payload, uint32(len(record.Value)))
	return append(payload, record.Value...)
}

func encodeCommit(group, topic string, nextOffset uint64) ([]byte, error) {
	if len(group) > maxStringBytes || len(topic) > maxStringBytes {
		return nil, errors.New("group or topic exceeds WAL string limit")
	}
	payload := make([]byte, 0, 2+len(group)+2+len(topic)+8)
	payload = appendString(payload, group)
	payload = appendString(payload, topic)
	payload = appendUint64(payload, nextOffset)
	return payload, nil
}

func encodeRetention(topic string, startOffset uint64) ([]byte, error) {
	if len(topic) == 0 || len(topic) > maxStringBytes {
		return nil, errors.New("topic must fit in a non-empty WAL string")
	}
	payload := make([]byte, 0, 2+len(topic)+8)
	payload = appendString(payload, topic)
	payload = appendUint64(payload, startOffset)
	return payload, nil
}

func decodeEntry(kind byte, payload []byte) (Entry, error) {
	reader := payloadReader{data: payload}
	switch kind {
	case kindPublish:
		topic, err := reader.readString()
		if err != nil {
			return Entry{}, err
		}
		record, err := readRecord(&reader)
		if err != nil {
			return Entry{}, err
		}
		if err := reader.requireDone(); err != nil {
			return Entry{}, err
		}
		return Entry{Kind: PublishEntry, Topic: topic, Record: record}, nil
	case kindBatch:
		topic, err := reader.readString()
		if err != nil {
			return Entry{}, err
		}
		count, err := reader.readUint32()
		if err != nil {
			return Entry{}, err
		}
		if count < 2 || count > maxBatchRecords {
			return Entry{}, fmt.Errorf("invalid batch record count %d", count)
		}
		records := make([]messageLog.Record, int(count))
		for i := range records {
			records[i], err = readRecord(&reader)
			if err != nil {
				return Entry{}, err
			}
		}
		if err := reader.requireDone(); err != nil {
			return Entry{}, err
		}
		return Entry{Kind: BatchEntry, Topic: topic, Records: records}, nil
	case kindIdempotentBatch:
		requestID, err := reader.readString()
		if err != nil {
			return Entry{}, err
		}
		topic, err := reader.readString()
		if err != nil {
			return Entry{}, err
		}
		count, err := reader.readUint32()
		if err != nil {
			return Entry{}, err
		}
		if count < 1 || count > maxBatchRecords {
			return Entry{}, fmt.Errorf("invalid idempotent batch record count %d", count)
		}
		records := make([]messageLog.Record, int(count))
		for i := range records {
			records[i], err = readRecord(&reader)
			if err != nil {
				return Entry{}, err
			}
		}
		if err := reader.requireDone(); err != nil {
			return Entry{}, err
		}
		return Entry{Kind: IdempotentBatchEntry, RequestID: requestID, Topic: topic, Records: records}, nil
	case kindCommit:
		group, err := reader.readString()
		if err != nil {
			return Entry{}, err
		}
		topic, err := reader.readString()
		if err != nil {
			return Entry{}, err
		}
		nextOffset, err := reader.readUint64()
		if err != nil {
			return Entry{}, err
		}
		if err := reader.requireDone(); err != nil {
			return Entry{}, err
		}
		return Entry{Kind: CommitEntry, Group: group, Topic: topic, NextOffset: nextOffset}, nil
	case kindRetention:
		topic, err := reader.readString()
		if err != nil {
			return Entry{}, err
		}
		startOffset, err := reader.readUint64()
		if err != nil {
			return Entry{}, err
		}
		if err := reader.requireDone(); err != nil {
			return Entry{}, err
		}
		return Entry{Kind: RetentionEntry, Topic: topic, StartOffset: startOffset}, nil
	default:
		return Entry{}, fmt.Errorf("unknown entry kind %d", kind)
	}
}

func readRecord(reader *payloadReader) (messageLog.Record, error) {
	offset, err := reader.readUint64()
	if err != nil {
		return messageLog.Record{}, err
	}
	timestamp, err := reader.readUint64()
	if err != nil {
		return messageLog.Record{}, err
	}
	keySize, err := reader.readUint32()
	if err != nil {
		return messageLog.Record{}, err
	}
	key, err := reader.readBytes(keySize)
	if err != nil {
		return messageLog.Record{}, err
	}
	valueSize, err := reader.readUint32()
	if err != nil {
		return messageLog.Record{}, err
	}
	value, err := reader.readBytes(valueSize)
	if err != nil {
		return messageLog.Record{}, err
	}
	return messageLog.Record{
		Offset:    offset,
		Key:       key,
		Value:     value,
		Timestamp: time.Unix(0, int64(timestamp)).UTC(),
	}, nil
}

func appendString(destination []byte, value string) []byte {
	destination = appendUint16(destination, uint16(len(value)))
	return append(destination, value...)
}

func appendUint16(destination []byte, value uint16) []byte {
	var encoded [2]byte
	binary.LittleEndian.PutUint16(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendUint32(destination []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(destination, encoded[:]...)
}

func appendUint64(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}

type payloadReader struct {
	data []byte
	pos  int
}

func (r *payloadReader) readString() (string, error) {
	size, err := r.readUint16()
	if err != nil {
		return "", err
	}
	value, err := r.readBytes(uint32(size))
	return string(value), err
}

func (r *payloadReader) readUint16() (uint16, error) {
	value, err := r.readBytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(value), nil
}

func (r *payloadReader) readUint32() (uint32, error) {
	value, err := r.readBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(value), nil
}

func (r *payloadReader) readUint64() (uint64, error) {
	value, err := r.readBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(value), nil
}

func (r *payloadReader) readBytes(size uint32) ([]byte, error) {
	if uint64(size) > uint64(len(r.data)-r.pos) {
		return nil, io.ErrUnexpectedEOF
	}
	length := int(size)
	value := r.data[r.pos : r.pos+length]
	r.pos += length
	return value, nil
}

func (r *payloadReader) requireDone() error {
	if r.pos != len(r.data) {
		return errors.New("trailing bytes in WAL payload")
	}
	return nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
