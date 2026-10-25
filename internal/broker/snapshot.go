package broker

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	messageLog "mq/internal/log"
)

const (
	snapshotFileName = "broker.snapshot"
	snapshotVersion  = uint16(1)
	snapshotHeader   = 14
	maxSnapshotBytes = int64(1) << 40
)

var snapshotMagic = [4]byte{'M', 'Q', 'S', '1'}

type SnapshotInfo struct {
	Generation uint64 `json:"generation"`
	WALOffset  int64  `json:"wal_offset"`
}

type brokerSnapshot struct {
	Version    uint16            `json:"version"`
	Generation uint64            `json:"generation"`
	WALOffset  int64             `json:"wal_offset"`
	Topics     []topicSnapshot   `json:"topics"`
	Groups     []groupSnapshot   `json:"groups"`
	Requests   []requestSnapshot `json:"requests"`
}

type topicSnapshot struct {
	Name        string              `json:"name"`
	StartOffset uint64              `json:"start_offset"`
	EndOffset   uint64              `json:"end_offset"`
	Records     []messageLog.Record `json:"records"`
}

type groupSnapshot struct {
	Group      string `json:"group"`
	Topic      string `json:"topic"`
	NextOffset uint64 `json:"next_offset"`
}

type requestSnapshot struct {
	RequestID string   `json:"request_id"`
	Topic     string   `json:"topic"`
	Digest    [32]byte `json:"digest"`
	Offsets   []uint64 `json:"offsets"`
}

func (b *Broker) CreateSnapshot() (SnapshotInfo, error) {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	if b.closing || b.closed {
		return SnapshotInfo{}, ErrBrokerClosed
	}
	if b.unavailable != nil {
		return SnapshotInfo{}, fmt.Errorf("%w: %v", ErrBrokerUnavailable, b.unavailable)
	}
	if b.wal == nil || b.dataDir == "" {
		return SnapshotInfo{}, errors.New("snapshots require a persistent broker")
	}
	if b.snapshotGeneration == ^uint64(0) {
		return SnapshotInfo{}, errors.New("snapshot generation is exhausted")
	}

	walOffset, err := b.wal.Size()
	if err != nil {
		return SnapshotInfo{}, fmt.Errorf("read WAL offset for snapshot: %w", err)
	}
	snapshot := b.captureSnapshot(b.snapshotGeneration+1, walOffset)
	filename := filepath.Join(b.dataDir, snapshotFileName)
	installed, digest, fileSize, err := persistSnapshot(filename, snapshot)
	if installed {
		b.snapshotGeneration = snapshot.Generation
		b.snapshotCheckpoint = snapshot.WALOffset
		b.snapshotDigest = digest
		b.snapshotSize = fileSize
	}
	if err != nil {
		return SnapshotInfo{}, err
	}
	if b.replicate == nil {
		if err := b.wal.TrimPrefixTo(snapshot.WALOffset); err != nil {
			b.setUnavailableLocked(err)
			return SnapshotInfo{}, fmt.Errorf("%w: trim WAL after snapshot: %v", ErrBrokerUnavailable, err)
		}
	}
	return SnapshotInfo{Generation: snapshot.Generation, WALOffset: snapshot.WALOffset}, nil
}

func (b *Broker) captureSnapshot(generation uint64, walOffset int64) brokerSnapshot {
	snapshot := brokerSnapshot{
		Version:    snapshotVersion,
		Generation: generation,
		WALOffset:  walOffset,
	}

	b.mu.RLock()
	topics := make(map[string]*messageLog.Log, len(b.topics))
	for name, topicLog := range b.topics {
		topics[name] = topicLog
	}
	commits := make(map[groupTopic]uint64, len(b.committed))
	for key, offset := range b.committed {
		commits[key] = offset
	}
	requests := make(map[string]publishResult, len(b.requests))
	for requestID, result := range b.requests {
		result.offsets = append([]uint64(nil), result.offsets...)
		requests[requestID] = result
	}
	b.mu.RUnlock()

	topicNames := make([]string, 0, len(topics))
	for name := range topics {
		topicNames = append(topicNames, name)
	}
	sort.Strings(topicNames)
	snapshot.Topics = make([]topicSnapshot, 0, len(topicNames))
	for _, name := range topicNames {
		start, end, records := topics[name].Snapshot()
		snapshot.Topics = append(snapshot.Topics, topicSnapshot{
			Name: name, StartOffset: start, EndOffset: end, Records: records,
		})
	}

	groupKeys := make([]groupTopic, 0, len(commits))
	for key := range commits {
		groupKeys = append(groupKeys, key)
	}
	sort.Slice(groupKeys, func(i, j int) bool {
		if groupKeys[i].group == groupKeys[j].group {
			return groupKeys[i].topic < groupKeys[j].topic
		}
		return groupKeys[i].group < groupKeys[j].group
	})
	snapshot.Groups = make([]groupSnapshot, 0, len(groupKeys))
	for _, key := range groupKeys {
		snapshot.Groups = append(snapshot.Groups, groupSnapshot{
			Group: key.group, Topic: key.topic, NextOffset: commits[key],
		})
	}

	requestIDs := make([]string, 0, len(requests))
	for requestID := range requests {
		requestIDs = append(requestIDs, requestID)
	}
	sort.Strings(requestIDs)
	snapshot.Requests = make([]requestSnapshot, 0, len(requestIDs))
	for _, requestID := range requestIDs {
		result := requests[requestID]
		snapshot.Requests = append(snapshot.Requests, requestSnapshot{
			RequestID: requestID, Topic: result.topic, Digest: result.digest, Offsets: result.offsets,
		})
	}
	return snapshot
}

type snapshotState struct {
	topics    map[string]*messageLog.Log
	committed map[groupTopic]uint64
	requests  map[string]publishResult
}

func buildSnapshotState(snapshot brokerSnapshot) (*snapshotState, error) {
	if snapshot.Version != snapshotVersion {
		return nil, fmt.Errorf("unsupported broker snapshot version %d", snapshot.Version)
	}
	if snapshot.Generation == 0 {
		return nil, errors.New("broker snapshot generation must be positive")
	}
	if snapshot.WALOffset < 0 {
		return nil, errors.New("broker snapshot WAL offset cannot be negative")
	}

	state := &snapshotState{
		topics:    make(map[string]*messageLog.Log),
		committed: make(map[groupTopic]uint64),
		requests:  make(map[string]publishResult),
	}
	for _, item := range snapshot.Topics {
		if !validName(item.Name) {
			return nil, fmt.Errorf("invalid topic name in snapshot: %q", item.Name)
		}
		if _, exists := state.topics[item.Name]; exists {
			return nil, fmt.Errorf("duplicate topic %q in snapshot", item.Name)
		}
		topicLog, err := messageLog.RestoreOwned(item.StartOffset, item.EndOffset, item.Records)
		if err != nil {
			return nil, fmt.Errorf("restore topic %q: %w", item.Name, err)
		}
		state.topics[item.Name] = topicLog
	}

	for _, item := range snapshot.Groups {
		if !validName(item.Group) || !validName(item.Topic) {
			return nil, errors.New("invalid group or topic name in snapshot")
		}
		topicLog := state.topics[item.Topic]
		if topicLog == nil {
			return nil, fmt.Errorf("snapshot group references missing topic %q", item.Topic)
		}
		if item.NextOffset > topicLog.EndOffset() {
			return nil, fmt.Errorf("snapshot commit %d exceeds topic %q end", item.NextOffset, item.Topic)
		}
		key := groupTopic{group: item.Group, topic: item.Topic}
		if _, exists := state.committed[key]; exists {
			return nil, fmt.Errorf("duplicate group offset for %q/%q in snapshot", item.Group, item.Topic)
		}
		state.committed[key] = item.NextOffset
	}

	for _, item := range snapshot.Requests {
		if !validName(item.RequestID) || !validName(item.Topic) || len(item.Offsets) == 0 {
			return nil, errors.New("invalid request ID entry in snapshot")
		}
		topicLog := state.topics[item.Topic]
		if topicLog == nil {
			return nil, fmt.Errorf("snapshot request references missing topic %q", item.Topic)
		}
		end := topicLog.EndOffset()
		for index, offset := range item.Offsets {
			if offset >= end {
				return nil, fmt.Errorf("snapshot request offset %d exceeds topic %q end", offset, item.Topic)
			}
			if index > 0 && offset != item.Offsets[index-1]+1 {
				return nil, fmt.Errorf("snapshot request offsets for %q are not contiguous", item.RequestID)
			}
		}
		if _, exists := state.requests[item.RequestID]; exists {
			return nil, fmt.Errorf("duplicate request ID %q in snapshot", item.RequestID)
		}
		state.requests[item.RequestID] = publishResult{
			topic: item.Topic, digest: item.Digest, offsets: item.Offsets,
		}
	}
	return state, nil
}

func loadSnapshot(path string) (*brokerSnapshot, [sha256.Size]byte, int64, error) {
	var payloadDigest [sha256.Size]byte
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, payloadDigest, 0, os.ErrNotExist
	}
	if err != nil {
		return nil, payloadDigest, 0, fmt.Errorf("open broker snapshot: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, payloadDigest, 0, fmt.Errorf("stat broker snapshot: %w", err)
	}
	if info.Size() < snapshotHeader+sha256.Size || info.Size() > maxSnapshotBytes+snapshotHeader+sha256.Size {
		return nil, payloadDigest, 0, fmt.Errorf("broker snapshot size %d is outside supported bounds", info.Size())
	}
	var header [snapshotHeader]byte
	if _, err := io.ReadFull(file, header[:]); err != nil {
		return nil, payloadDigest, 0, fmt.Errorf("read broker snapshot header: %w", err)
	}
	if string(header[:4]) != string(snapshotMagic[:]) {
		return nil, payloadDigest, 0, errors.New("invalid broker snapshot magic")
	}
	if version := binary.LittleEndian.Uint16(header[4:6]); version != snapshotVersion {
		return nil, payloadDigest, 0, fmt.Errorf("unsupported broker snapshot version %d", version)
	}
	payloadLength := binary.LittleEndian.Uint64(header[6:14])
	if payloadLength > uint64(maxSnapshotBytes) || int64(payloadLength) != info.Size()-snapshotHeader-sha256.Size {
		return nil, payloadDigest, 0, errors.New("broker snapshot payload length is invalid")
	}

	hasher := sha256.New()
	limited := &io.LimitedReader{R: file, N: int64(payloadLength)}
	decoder := json.NewDecoder(io.TeeReader(limited, hasher))
	decoder.DisallowUnknownFields()
	var snapshot brokerSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, payloadDigest, 0, fmt.Errorf("decode broker snapshot: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, payloadDigest, 0, errors.New("broker snapshot contains multiple JSON values")
		}
		return nil, payloadDigest, 0, fmt.Errorf("decode broker snapshot trailer: %w", err)
	}
	if _, err := io.Copy(io.Discard, io.TeeReader(limited, hasher)); err != nil {
		return nil, payloadDigest, 0, fmt.Errorf("read broker snapshot payload: %w", err)
	}
	if limited.N != 0 {
		return nil, payloadDigest, 0, errors.New("broker snapshot payload ended early")
	}
	var storedDigest [sha256.Size]byte
	if _, err := io.ReadFull(file, storedDigest[:]); err != nil {
		return nil, payloadDigest, 0, fmt.Errorf("read broker snapshot checksum: %w", err)
	}
	if !equalDigest(storedDigest[:], hasher.Sum(nil)) {
		return nil, payloadDigest, 0, errors.New("broker snapshot checksum mismatch")
	}
	copy(payloadDigest[:], hasher.Sum(nil))
	if err := installSnapshotValidation(&snapshot); err != nil {
		return nil, payloadDigest, 0, err
	}
	return &snapshot, payloadDigest, info.Size(), nil
}

func installSnapshotValidation(snapshot *brokerSnapshot) error {
	if snapshot.Version != snapshotVersion || snapshot.Generation == 0 || snapshot.WALOffset < 0 {
		return errors.New("broker snapshot metadata is invalid")
	}
	return nil
}

func persistSnapshot(path string, snapshot brokerSnapshot) (bool, [sha256.Size]byte, int64, error) {
	digest := [sha256.Size]byte{}
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, ".broker-snapshot-*")
	if err != nil {
		return false, digest, 0, fmt.Errorf("create temporary broker snapshot: %w", err)
	}
	temporaryPath := file.Name()
	defer os.Remove(temporaryPath)

	if _, err := file.Write(make([]byte, snapshotHeader)); err != nil {
		return false, digest, 0, errors.Join(fmt.Errorf("write broker snapshot placeholder: %w", err), file.Close())
	}
	hasher := sha256.New()
	limited := &snapshotLimitWriter{writer: io.MultiWriter(file, hasher), limit: maxSnapshotBytes}
	if err := json.NewEncoder(limited).Encode(snapshot); err != nil {
		return false, digest, 0, errors.Join(fmt.Errorf("encode broker snapshot: %w", err), file.Close())
	}
	payloadLength := limited.written
	copy(digest[:], hasher.Sum(nil))
	fileSize := snapshotHeader + payloadLength + sha256.Size
	if _, err := file.Write(hasher.Sum(nil)); err != nil {
		return false, digest, 0, errors.Join(fmt.Errorf("write broker snapshot checksum: %w", err), file.Close())
	}
	var header [snapshotHeader]byte
	copy(header[:4], snapshotMagic[:])
	binary.LittleEndian.PutUint16(header[4:6], snapshotVersion)
	binary.LittleEndian.PutUint64(header[6:14], uint64(payloadLength))
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, digest, 0, errors.Join(fmt.Errorf("seek broker snapshot header: %w", err), file.Close())
	}
	if n, err := file.Write(header[:]); err != nil || n != len(header) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return false, digest, 0, errors.Join(fmt.Errorf("write broker snapshot header: %w", err), file.Close())
	}
	if err := file.Sync(); err != nil {
		return false, digest, 0, errors.Join(fmt.Errorf("sync broker snapshot: %w", err), file.Close())
	}
	if err := file.Close(); err != nil {
		return false, digest, 0, fmt.Errorf("close broker snapshot: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return false, digest, 0, fmt.Errorf("replace broker snapshot: %w", err)
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return true, digest, fileSize, fmt.Errorf("open broker snapshot directory: %w", err)
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return true, digest, fileSize, fmt.Errorf("sync broker snapshot directory: %w", err)
	}
	return true, digest, fileSize, nil
}

type snapshotLimitWriter struct {
	writer  io.Writer
	limit   int64
	written int64
}

func (w *snapshotLimitWriter) Write(value []byte) (int, error) {
	if int64(len(value)) > w.limit-w.written {
		return 0, errors.New("broker snapshot exceeds maximum size")
	}
	n, err := w.writer.Write(value)
	w.written += int64(n)
	if err == nil && n != len(value) {
		err = io.ErrShortWrite
	}
	return n, err
}

func MaxSnapshotTransferBytes() int64 {
	return maxSnapshotBytes + snapshotHeader + sha256.Size
}

func equalDigest(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for i := range left {
		difference |= left[i] ^ right[i]
	}
	return difference == 0
}
