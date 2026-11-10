package broker

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mq/internal/dirlock"
	messageLog "mq/internal/log"
	"mq/internal/wal"
)

var (
	ErrInvalidTopic            = errors.New("topic name must contain 1 to 255 bytes")
	ErrInvalidGroup            = errors.New("consumer group name must contain 1 to 255 bytes")
	ErrTopicNotFound           = errors.New("topic not found")
	ErrOffsetExpired           = messageLog.ErrOffsetExpired
	ErrCommitOutOfRange        = errors.New("commit offset exceeds topic end")
	ErrCommitRegression        = errors.New("commit offset cannot move backwards")
	ErrRetentionOutOfRange     = errors.New("retention start exceeds topic end")
	ErrRetentionRegression     = errors.New("retention start cannot move backwards")
	ErrEmptyBatch              = errors.New("publish batch must not be empty")
	ErrBatchTooLarge           = errors.New("publish batch exceeds the record limit")
	ErrInvalidRequestID        = errors.New("request ID must contain 1 to 255 bytes")
	ErrIdempotencyConflict     = errors.New("request ID was already used with different publish contents")
	ErrReadOnly                = errors.New("broker is read-only")
	ErrNotFollower             = errors.New("broker is not a follower")
	ErrStaleLeader             = errors.New("leader epoch is stale")
	ErrLeaderEpochMismatch     = errors.New("leader epoch does not match follower epoch")
	ErrReplicationStateChanged = errors.New("follower WAL changed during leader fencing")
	ErrReplicationUnavailable  = errors.New("replication is unavailable")
	ErrBrokerUnavailable       = errors.New("broker is unavailable")
	ErrBrokerClosed            = errors.New("broker is closed")
)

const maxBrokerBatchRecords = 1024
const maxBrokerNameBytes = 255
const maxBrokerRequestIDBytes = 255
const publishQueueCapacity = 64
const maxGroupCommitRequests = 64
const maxGroupCommitBytes = wal.MaxGroupCommitBytes

const DefaultGroupCommitWindow = 200 * time.Microsecond

const DefaultWALSegmentBytes = wal.DefaultSegmentBytes

type OpenOptions struct {
	GroupCommitWindow time.Duration

	WALSegmentBytes int64
}

type GroupCommitStats struct {
	Groups uint64
	Frames uint64
}

func (s GroupCommitStats) FramesPerGroup() float64 {
	if s.Groups == 0 {
		return 0
	}
	return float64(s.Frames) / float64(s.Groups)
}

type groupTopic struct {
	group string
	topic string
}

type publishResult struct {
	topic   string
	digest  [sha256.Size]byte
	offsets []uint64
}

type publishRequest struct {
	topic     string
	requestID string
	digest    [sha256.Size]byte
	messages  []Message
	singleMsg [1]Message
	walBytes  int64
	done      chan publishResponse
	offsets   []uint64
	alias     *publishRequest
	err       error
	accepted  bool
}

var publishRequestPool = sync.Pool{
	New: func() any {
		return &publishRequest{
			done: make(chan publishResponse, 1),
		}
	},
}

type publishResponse struct {
	offsets []uint64
	err     error
}

type Message struct {
	Key   []byte
	Value []byte
}

type Broker struct {
	writeMu            sync.Mutex
	publishSubmit      sync.Mutex
	mu                 sync.RWMutex
	walMu              sync.RWMutex
	topics             map[string]*messageLog.Log
	committed          map[groupTopic]uint64
	requests           map[string]publishResult
	wal                *wal.WAL
	segmentBytes       int64
	dirLock            *dirlock.Lock
	dataDir            string
	snapshotGeneration uint64
	snapshotCheckpoint int64
	snapshotDigest     [sha256.Size]byte
	snapshotSize       int64
	epoch              uint64
	replicate          func() error
	readOnly           bool
	unavailable        error
	closing            bool
	closed             bool
	closeErr           error
	publishQueue       chan *publishRequest
	publishDone        chan struct{}
	acceptPublish      bool
	groupCommitWindow  time.Duration
	groupCommitGroups  atomic.Uint64
	groupCommitFrames  atomic.Uint64
}

func New() *Broker {
	return &Broker{}
}

func Open(dataDir string) (*Broker, error) {
	return OpenWithOptions(dataDir, OpenOptions{
		GroupCommitWindow: DefaultGroupCommitWindow,
		WALSegmentBytes:   DefaultWALSegmentBytes,
	})
}

func OpenWithOptions(dataDir string, options OpenOptions) (*Broker, error) {
	if options.GroupCommitWindow < 0 {
		return nil, errors.New("group commit window must not be negative")
	}
	if options.WALSegmentBytes < 0 {
		return nil, errors.New("WAL segment size must not be negative")
	}
	if options.WALSegmentBytes == 0 {
		options.WALSegmentBytes = DefaultWALSegmentBytes
	} else if options.WALSegmentBytes < wal.MinSegmentBytes {
		return nil, fmt.Errorf("WAL segment size must be at least %d bytes", wal.MinSegmentBytes)
	}
	if dataDir == "" {
		return nil, errors.New("data directory must not be empty")
	}
	dataDirCreated := false
	if _, err := os.Stat(dataDir); errors.Is(err, os.ErrNotExist) {
		dataDirCreated = true
	} else if err != nil {
		return nil, fmt.Errorf("stat data directory: %w", err)
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	var syncErr error
	if dataDirCreated {

		syncErr = syncDirectoryChain(dataDir)
	} else {

		syncErr = syncDirectory(filepath.Dir(dataDir))
	}
	if syncErr != nil {
		return nil, fmt.Errorf("sync data directory metadata: %w", syncErr)
	}
	lock, err := dirlock.Acquire(filepath.Join(dataDir, "broker.lock"))
	if err != nil {
		return nil, err
	}

	b := &Broker{
		dirLock:           lock,
		dataDir:           dataDir,
		groupCommitWindow: options.GroupCommitWindow,
		segmentBytes:      options.WALSegmentBytes,
	}
	snapshot, digest, fileSize, err := loadSnapshot(filepath.Join(dataDir, snapshotFileName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, errors.Join(err, lock.Close())
	}
	if snapshot != nil {
		state, stateErr := buildSnapshotState(*snapshot)
		if stateErr != nil {
			return nil, errors.Join(fmt.Errorf("install broker snapshot: %w", stateErr), lock.Close())
		}
		b.topics = state.topics
		b.committed = state.committed
		b.requests = state.requests
		b.snapshotGeneration = snapshot.Generation
		b.snapshotCheckpoint = snapshot.WALOffset
		b.snapshotDigest = digest
		b.snapshotSize = fileSize
	}
	walPath := filepath.Join(dataDir, "wal.log")
	store, err := wal.OpenAndReplayFrom(walPath, options.WALSegmentBytes, b.snapshotCheckpoint, b.applyEntry)
	if errors.Is(err, wal.ErrCheckpointExceedsStream) {
		resetErr := wal.PrepareFreshStream(walPath, b.snapshotCheckpoint)
		if resetErr == nil {
			store, resetErr = wal.OpenAndReplayFrom(walPath, options.WALSegmentBytes, b.snapshotCheckpoint, b.applyEntry)
		}
		err = errors.Join(err, resetErr)
	}
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	b.wal = store
	b.publishQueue = make(chan *publishRequest, publishQueueCapacity)
	b.publishDone = make(chan struct{})
	b.acceptPublish = true
	go b.runPublishGroupCommitter()
	return b, nil
}

func syncDirectoryChain(path string) error {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	for current := filepath.Clean(absolutePath); ; current = filepath.Dir(current) {
		if err := syncDirectory(current); err != nil {
			return fmt.Errorf("sync directory %q: %w", current, err)
		}
		if parent := filepath.Dir(current); parent == current {
			return nil
		}
	}
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (b *Broker) Close() error {
	b.publishSubmit.Lock()
	b.writeMu.Lock()
	if b.closed {
		closeErr := b.closeErr
		b.writeMu.Unlock()
		b.publishSubmit.Unlock()
		return closeErr
	}
	b.closing = true
	if b.publishQueue != nil && b.acceptPublish {
		b.acceptPublish = false
		close(b.publishQueue)
	}
	done := b.publishDone
	b.writeMu.Unlock()
	b.publishSubmit.Unlock()
	if done != nil {
		<-done
	}

	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if b.closed {
		return b.closeErr
	}
	b.closed = true
	if b.wal != nil {
		b.closeErr = errors.Join(b.closeErr, b.wal.Close())
	}
	if b.dirLock != nil {
		b.closeErr = errors.Join(b.closeErr, b.dirLock.Close())
	}
	return b.closeErr
}

func (b *Broker) PublishGroupCommitStats() GroupCommitStats {
	return GroupCommitStats{
		Groups: b.groupCommitGroups.Load(),
		Frames: b.groupCommitFrames.Load(),
	}
}

func (b *Broker) SetReadOnly() error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if b.closing || b.closed {
		return ErrBrokerClosed
	}
	if b.wal == nil || b.dataDir == "" {
		return errors.New("a follower requires a persistent broker")
	}
	data, err := os.ReadFile(filepath.Join(b.dataDir, "replication.epoch"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read follower leader epoch: %w", err)
	}
	if len(data) > 32 {
		return errors.New("follower leader epoch file is too large")
	}
	if err == nil {
		if len(data) == 0 {
			return errors.New("follower leader epoch file is empty")
		}
		epoch, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		if err != nil {
			return fmt.Errorf("parse follower leader epoch: %w", err)
		}
		b.epoch = epoch
	}
	b.readOnly = true
	return nil
}

func (b *Broker) AdvanceLeaderEpoch(epoch uint64, expectedOffset int64, expectedHash [sha256.Size]byte) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if b.closing || b.closed {
		return ErrBrokerClosed
	}
	if !b.readOnly {
		return ErrNotFollower
	}
	if b.unavailable != nil {
		return fmt.Errorf("%w: %v", ErrBrokerUnavailable, b.unavailable)
	}
	if epoch == 0 {
		return errors.New("leader epoch must be positive")
	}
	if epoch < b.epoch {
		return fmt.Errorf("%w: received %d, current %d", ErrStaleLeader, epoch, b.epoch)
	}
	if epoch == b.epoch {
		return nil
	}
	if b.wal == nil {
		return errors.New("WAL replication requires a persistent broker")
	}
	currentOffset, err := b.wal.Size()
	if err != nil {
		b.setUnavailableLocked(err)
		return fmt.Errorf("%w: read follower WAL before fencing: %v", ErrBrokerUnavailable, err)
	}
	if currentOffset != expectedOffset {
		return fmt.Errorf("%w: observed byte %d, current byte %d", ErrReplicationStateChanged, expectedOffset, currentOffset)
	}
	currentHash, err := b.wal.HashPrefix(currentOffset)
	if err != nil {
		b.setUnavailableLocked(err)
		return fmt.Errorf("%w: hash follower WAL before fencing: %v", ErrBrokerUnavailable, err)
	}
	if currentHash != expectedHash {
		return ErrReplicationStateChanged
	}
	if err := persistLeaderEpoch(b.dataDir, epoch); err != nil {
		b.setUnavailableLocked(err)
		return fmt.Errorf("%w: persist leader epoch: %v", ErrBrokerUnavailable, err)
	}
	b.epoch = epoch
	return nil
}

func (b *Broker) captureLeaderEpoch(epoch uint64) error {
	if epoch < b.epoch {
		return fmt.Errorf("%w: received %d, current %d", ErrStaleLeader, epoch, b.epoch)
	}
	if epoch > b.epoch {
		if err := persistLeaderEpoch(b.dataDir, epoch); err != nil {
			b.setUnavailableLocked(err)
			return fmt.Errorf("%w: persist leader epoch: %v", ErrBrokerUnavailable, err)
		}
		b.epoch = epoch
	}
	return nil
}

func persistLeaderEpoch(dataDir string, epoch uint64) error {
	file, err := os.CreateTemp(dataDir, ".replication-epoch-*")
	if err != nil {
		return fmt.Errorf("create leader epoch file: %w", err)
	}
	temporaryPath := file.Name()
	defer os.Remove(temporaryPath)
	contents := []byte(strconv.FormatUint(epoch, 10) + "\n")
	n, writeErr := file.Write(contents)
	if writeErr == nil && n != len(contents) {
		writeErr = io.ErrShortWrite
	}
	syncErr := error(nil)
	if writeErr == nil {
		syncErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("write leader epoch file: %w", err)
	}
	if err := os.Rename(temporaryPath, filepath.Join(dataDir, "replication.epoch")); err != nil {
		return fmt.Errorf("replace leader epoch file: %w", err)
	}
	directory, err := os.Open(dataDir)
	if err != nil {
		return fmt.Errorf("open data directory for epoch sync: %w", err)
	}
	syncErr = directory.Sync()
	closeErr = directory.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync leader epoch directory entry: %w", err)
	}
	return nil
}

func (b *Broker) SetReplicator(replicate func() error) {
	b.writeMu.Lock()
	if !b.closing && !b.closed {
		b.replicate = replicate
	}
	b.writeMu.Unlock()
}

func (b *Broker) currentWAL() *wal.WAL {
	b.walMu.RLock()
	defer b.walMu.RUnlock()
	return b.wal
}

func (b *Broker) WALBase() int64 {
	current := b.currentWAL()
	if current == nil {
		return 0
	}
	return current.Base()
}

func (b *Broker) WALSize() (int64, error) {
	b.mu.RLock()
	unavailable := b.unavailable
	b.mu.RUnlock()
	if unavailable != nil {
		return 0, fmt.Errorf("%w: %v", ErrBrokerUnavailable, unavailable)
	}
	current := b.currentWAL()
	if current == nil {
		return 0, errors.New("WAL replication requires a persistent broker")
	}
	return current.Size()
}

func (b *Broker) WALHashPrefix(offset int64) ([sha256.Size]byte, error) {
	b.mu.RLock()
	unavailable := b.unavailable
	b.mu.RUnlock()
	if unavailable != nil {
		return [sha256.Size]byte{}, fmt.Errorf("%w: %v", ErrBrokerUnavailable, unavailable)
	}
	current := b.currentWAL()
	if current == nil {
		return [sha256.Size]byte{}, errors.New("WAL replication requires a persistent broker")
	}
	return current.HashPrefix(offset)
}

func (b *Broker) WALReplicationStatus() (int64, [sha256.Size]byte, uint64, error) {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	var emptyHash [sha256.Size]byte
	if b.closing || b.closed {
		return 0, emptyHash, 0, ErrBrokerClosed
	}
	if !b.readOnly {
		return 0, emptyHash, 0, ErrNotFollower
	}
	if b.unavailable != nil {
		return 0, emptyHash, 0, fmt.Errorf("%w: %v", ErrBrokerUnavailable, b.unavailable)
	}
	if b.wal == nil {
		return 0, emptyHash, 0, errors.New("WAL replication requires a persistent broker")
	}
	offset, err := b.wal.Size()
	if err != nil {
		b.setUnavailableLocked(err)
		return 0, emptyHash, 0, err
	}
	hash, err := b.wal.HashPrefix(offset)
	if err != nil {
		b.setUnavailableLocked(err)
		return 0, emptyHash, 0, err
	}
	return offset, hash, b.epoch, nil
}

func (b *Broker) ReadWALFrames(offset int64, maxBytes int) ([]byte, error) {
	return b.ReadWALFramesUntil(offset, -1, maxBytes)
}

func (b *Broker) ReadWALFramesUntil(offset, end int64, maxBytes int) ([]byte, error) {
	b.mu.RLock()
	unavailable := b.unavailable
	b.mu.RUnlock()
	if unavailable != nil {
		return nil, fmt.Errorf("%w: %v", ErrBrokerUnavailable, unavailable)
	}
	current := b.currentWAL()
	if current == nil {
		return nil, errors.New("WAL replication requires a persistent broker")
	}
	return current.ReadFramesUntil(offset, end, maxBytes)
}

func (b *Broker) ReplicateWALFrames(offset int64, epoch uint64, data []byte) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if b.closing || b.closed {
		return ErrBrokerClosed
	}
	if !b.readOnly {
		return ErrNotFollower
	}
	if epoch < b.epoch {
		return fmt.Errorf("%w: received %d, current %d", ErrStaleLeader, epoch, b.epoch)
	}
	if epoch > b.epoch {
		return fmt.Errorf("%w: received %d, current %d", ErrLeaderEpochMismatch, epoch, b.epoch)
	}
	if b.unavailable != nil {
		return fmt.Errorf("%w: %v", ErrBrokerUnavailable, b.unavailable)
	}
	if b.wal == nil {
		return errors.New("WAL replication requires a persistent broker")
	}
	entries, err := b.wal.AppendFramesChecked(offset, data, b.validateReplicatedEntries)
	if err != nil {
		if errors.Is(err, wal.ErrUnavailable) {
			b.setUnavailableLocked(err)
			return fmt.Errorf("%w: %v", ErrBrokerUnavailable, err)
		}
		return err
	}
	for _, entry := range entries {
		if err := b.applyEntry(entry); err != nil {
			b.setUnavailableLocked(err)
			return fmt.Errorf("%w: apply replicated WAL entry: %v", ErrBrokerUnavailable, err)
		}
	}
	return nil
}

func (b *Broker) validateReplicatedEntries(entries []wal.Entry) error {
	ends := make(map[string]uint64)
	topicExists := make(map[string]bool)
	loadedTopics := make(map[string]bool)
	starts := make(map[string]uint64)
	loadedStarts := make(map[string]bool)
	committed := make(map[groupTopic]uint64)
	hasCommit := make(map[groupTopic]bool)
	loadedCommits := make(map[groupTopic]bool)
	requestIDs := make(map[string]bool)
	getTopicEnd := func(topic string) (uint64, bool) {
		if loadedTopics[topic] {
			return ends[topic], topicExists[topic]
		}
		b.mu.RLock()
		topicLog, exists := b.topics[topic]
		b.mu.RUnlock()
		var end uint64
		if exists {
			end = topicLog.EndOffset()
		}
		ends[topic] = end
		topicExists[topic] = exists
		loadedTopics[topic] = true
		return end, exists
	}
	getCommit := func(key groupTopic) (uint64, bool) {
		if loadedCommits[key] {
			return committed[key], hasCommit[key]
		}
		b.mu.RLock()
		offset, exists := b.committed[key]
		b.mu.RUnlock()
		committed[key] = offset
		hasCommit[key] = exists
		loadedCommits[key] = true
		return offset, exists
	}
	getTopicStart := func(topic string) uint64 {
		if loadedStarts[topic] {
			return starts[topic]
		}
		b.mu.RLock()
		topicLog := b.topics[topic]
		b.mu.RUnlock()
		if topicLog != nil {
			starts[topic] = topicLog.StartOffset()
		}
		loadedStarts[topic] = true
		return starts[topic]
	}

	for _, entry := range entries {
		switch entry.Kind {
		case wal.PublishEntry:
			if !validName(entry.Topic) {
				return ErrInvalidTopic
			}
			end, _ := getTopicEnd(entry.Topic)
			if entry.Record.Offset != end || end == ^uint64(0) {
				return fmt.Errorf("replicated publish offset %d does not match %q end %d", entry.Record.Offset, entry.Topic, end)
			}
			ends[entry.Topic] = end + 1
			topicExists[entry.Topic] = true
		case wal.BatchEntry, wal.IdempotentBatchEntry:
			if !validName(entry.Topic) {
				return ErrInvalidTopic
			}
			if entry.Kind == wal.IdempotentBatchEntry {
				if !validName(entry.RequestID) {
					return ErrInvalidRequestID
				}
				if requestIDs[entry.RequestID] {
					return fmt.Errorf("duplicate request ID %q in replicated WAL", entry.RequestID)
				}
				b.mu.RLock()
				_, exists := b.requests[entry.RequestID]
				b.mu.RUnlock()
				if exists {
					return fmt.Errorf("duplicate request ID %q in replicated WAL", entry.RequestID)
				}
				requestIDs[entry.RequestID] = true
			}
			if len(entry.Records) == 0 {
				return errors.New("replicated batch is empty")
			}
			end, _ := getTopicEnd(entry.Topic)
			for i, record := range entry.Records {
				if uint64(i) > ^uint64(0)-end || record.Offset != end+uint64(i) {
					return fmt.Errorf("replicated batch offset %d does not match %q end %d", record.Offset, entry.Topic, end)
				}
			}
			if uint64(len(entry.Records)) > ^uint64(0)-end {
				return errors.New("replicated topic offset overflow")
			}
			ends[entry.Topic] = end + uint64(len(entry.Records))
			topicExists[entry.Topic] = true
		case wal.CommitEntry:
			if !validName(entry.Group) {
				return ErrInvalidGroup
			}
			if !validName(entry.Topic) {
				return ErrInvalidTopic
			}
			end, exists := getTopicEnd(entry.Topic)
			if !exists {
				return fmt.Errorf("%w: %q", ErrTopicNotFound, entry.Topic)
			}
			if entry.NextOffset > end {
				return ErrCommitOutOfRange
			}
			if entry.NextOffset < getTopicStart(entry.Topic) {
				return ErrOffsetExpired
			}
			key := groupTopic{group: entry.Group, topic: entry.Topic}
			previous, hasPrevious := getCommit(key)
			if hasPrevious && entry.NextOffset < previous {
				return ErrCommitRegression
			}
			committed[key] = entry.NextOffset
			hasCommit[key] = true
			loadedCommits[key] = true
		case wal.RetentionEntry:
			if !validName(entry.Topic) {
				return ErrInvalidTopic
			}
			end, exists := getTopicEnd(entry.Topic)
			if !exists {
				return fmt.Errorf("%w: %q", ErrTopicNotFound, entry.Topic)
			}
			currentStart := getTopicStart(entry.Topic)
			if entry.StartOffset < currentStart {
				return ErrRetentionRegression
			}
			if entry.StartOffset > end {
				return ErrRetentionOutOfRange
			}
			starts[entry.Topic] = entry.StartOffset
			loadedStarts[entry.Topic] = true
		default:
			return fmt.Errorf("unknown replicated WAL entry kind %d", entry.Kind)
		}
	}
	return nil
}

func (b *Broker) replicateLocked() error {
	if b.replicate == nil {
		return nil
	}
	if err := b.replicate(); err != nil {
		return fmt.Errorf("%w: %v", ErrReplicationUnavailable, err)
	}
	return nil
}

func (b *Broker) setUnavailableLocked(err error) {
	b.mu.Lock()
	b.unavailable = err
	b.mu.Unlock()
}

func (b *Broker) Publish(topic string, key, value []byte) (uint64, error) {
	if !validName(topic) {
		return 0, ErrInvalidTopic
	}
	if b.publishQueue != nil {
		req := publishRequestPool.Get().(*publishRequest)
		req.topic = topic
		req.requestID = ""
		req.singleMsg[0] = Message{Key: key, Value: value}
		req.messages = req.singleMsg[:]
		req.walBytes = int64(walHeaderBytes + 2 + len(topic) + 24 + len(key) + len(value))
		req.offsets = nil
		req.alias = nil
		req.err = nil
		req.accepted = false

		b.publishSubmit.Lock()
		if !b.acceptPublish {
			b.publishSubmit.Unlock()
			publishRequestPool.Put(req)
			return 0, ErrBrokerClosed
		}
		b.publishQueue <- req
		b.publishSubmit.Unlock()
		response := <-req.done
		publishRequestPool.Put(req)
		if response.err != nil {
			return 0, response.err
		}
		return response.offsets[0], nil
	}

	var singleMsg [1]Message
	singleMsg[0] = Message{Key: key, Value: value}
	var emptyDigest [sha256.Size]byte
	offsets, err := b.publishBatchImmediate(topic, "", singleMsg[:], emptyDigest)
	if err != nil {
		return 0, err
	}
	return offsets[0], nil
}

func (b *Broker) PublishWithID(topic, requestID string, key, value []byte) (uint64, error) {
	offsets, err := b.PublishBatchWithID(topic, requestID, []Message{{Key: key, Value: value}})
	if err != nil {
		return 0, err
	}
	return offsets[0], nil
}

func (b *Broker) PublishBatch(topic string, messages []Message) ([]uint64, error) {
	return b.publishBatch(topic, "", messages)
}

func (b *Broker) PublishBatchWithID(topic, requestID string, messages []Message) ([]uint64, error) {
	if len(requestID) == 0 || len(requestID) > maxBrokerRequestIDBytes {
		return nil, ErrInvalidRequestID
	}
	return b.publishBatch(topic, requestID, messages)
}

func (b *Broker) publishBatch(topic, requestID string, messages []Message) ([]uint64, error) {
	if !validName(topic) {
		return nil, ErrInvalidTopic
	}
	if requestID != "" && len(requestID) > maxBrokerRequestIDBytes {
		return nil, ErrInvalidRequestID
	}
	if len(messages) == 0 {
		return nil, ErrEmptyBatch
	}
	if len(messages) > maxBrokerBatchRecords {
		return nil, ErrBatchTooLarge
	}

	var digest [sha256.Size]byte
	if requestID != "" {
		digest = batchDigest(topic, messages)
	}
	if b.publishQueue != nil {
		request := publishRequestPool.Get().(*publishRequest)
		request.topic = topic
		request.requestID = requestID
		request.digest = digest
		request.messages = messages
		request.walBytes = estimatePublishFrameSize(topic, requestID, messages)
		request.offsets = nil
		request.alias = nil
		request.err = nil
		request.accepted = false

		b.publishSubmit.Lock()
		if !b.acceptPublish {
			b.publishSubmit.Unlock()
			publishRequestPool.Put(request)
			return nil, ErrBrokerClosed
		}
		b.publishQueue <- request
		b.publishSubmit.Unlock()
		response := <-request.done
		publishRequestPool.Put(request)
		return response.offsets, response.err
	}
	return b.publishBatchImmediate(topic, requestID, messages, digest)
}

func (b *Broker) publishBatchImmediate(topic, requestID string, messages []Message, digest [sha256.Size]byte) ([]uint64, error) {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if b.closing || b.closed {
		return nil, ErrBrokerClosed
	}
	if b.unavailable != nil {
		return nil, fmt.Errorf("%w: %v", ErrBrokerUnavailable, b.unavailable)
	}
	if b.readOnly {
		return nil, ErrReadOnly
	}

	if requestID != "" {
		b.mu.RLock()
		previous, ok := b.requests[requestID]
		b.mu.RUnlock()
		if ok {
			if previous.topic != topic || previous.digest != digest {
				return nil, ErrIdempotencyConflict
			}
			if err := b.replicateLocked(); err != nil {
				return nil, err
			}
			return append([]uint64(nil), previous.offsets...), nil
		}
	}

	b.mu.RLock()
	topicLog := b.topics[topic]
	b.mu.RUnlock()
	firstOffset := uint64(0)
	if topicLog != nil {
		firstOffset = topicLog.EndOffset()
	}
	var singleRec [1]messageLog.Record
	var singleOff [1]uint64
	var records []messageLog.Record
	var offsets []uint64
	if len(messages) == 1 {
		records = singleRec[:]
		offsets = singleOff[:]
	} else {
		records = make([]messageLog.Record, len(messages))
		offsets = make([]uint64, len(messages))
	}
	for i, message := range messages {
		offset := firstOffset + uint64(i)
		offsets[i] = offset
		records[i] = messageLog.Record{
			Offset:    offset,
			Key:       message.Key,
			Value:     message.Value,
			Timestamp: time.Now().UTC(),
		}
	}
	if b.wal != nil {
		var err error
		if requestID == "" {
			err = b.wal.AppendBatch(topic, records)
		} else {
			err = b.wal.AppendIdempotentBatch(requestID, topic, records)
		}
		if err != nil {
			return nil, err
		}
	}
	if topicLog == nil {
		topicLog = b.topicForAppend(topic)
	}
	if err := topicLog.AppendRecords(records); err != nil {
		return nil, fmt.Errorf("apply published batch: %w", err)
	}
	if requestID != "" {
		result := publishResult{
			topic:   topic,
			digest:  digest,
			offsets: append([]uint64(nil), offsets...),
		}
		b.mu.Lock()
		if b.requests == nil {
			b.requests = make(map[string]publishResult)
		}
		b.requests[requestID] = result
		b.mu.Unlock()
	}
	if err := b.replicateLocked(); err != nil {
		return nil, err
	}
	if len(messages) == 1 {
		return []uint64{offsets[0]}, nil
	}
	return offsets, nil
}

func estimatePublishFrameSize(topic, requestID string, messages []Message) int64 {
	size := int64(walHeaderBytes + 2 + len(topic))
	if requestID != "" {
		size += int64(2 + len(requestID) + 4)
	} else if len(messages) > 1 {
		size += 4
	}
	for _, message := range messages {
		size += 24 + int64(len(message.Key)) + int64(len(message.Value))
	}
	return size
}

const walHeaderBytes = 13

func (b *Broker) runPublishGroupCommitter() {
	defer close(b.publishDone)

	var pending *publishRequest
	for {
		first := pending
		if first == nil {
			request, ok := <-b.publishQueue
			if !ok {
				return
			}
			first = request
		}
		pending = nil

		requests := []*publishRequest{first}
		groupBytes := first.walBytes
		collecting := groupBytes < maxGroupCommitBytes
		addRequest := func(request *publishRequest) bool {
			if groupBytes+request.walBytes > maxGroupCommitBytes {
				pending = request
				return false
			}
			requests = append(requests, request)
			groupBytes += request.walBytes
			return true
		}
		if collecting && b.groupCommitWindow == 0 {
			for len(requests) < maxGroupCommitRequests {
				select {
				case request, ok := <-b.publishQueue:
					if !ok || !addRequest(request) {
						collecting = false
					}
				default:
					collecting = false
				}
				if !collecting || groupBytes >= maxGroupCommitBytes {
					break
				}
			}
		} else if collecting {
			timer := time.NewTimer(b.groupCommitWindow)
			for collecting && len(requests) < maxGroupCommitRequests {
				select {
				case request, ok := <-b.publishQueue:
					if !ok || !addRequest(request) {
						collecting = false
					}
					if groupBytes >= maxGroupCommitBytes {
						collecting = false
					}
				case <-timer.C:
					collecting = false
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		b.processPublishGroup(requests)
	}
}

func (b *Broker) processPublishGroup(requests []*publishRequest) {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	if b.closed {
		finishPublishGroup(requests, ErrBrokerClosed)
		return
	}
	if b.unavailable != nil {
		finishPublishGroup(requests, fmt.Errorf("%w: %v", ErrBrokerUnavailable, b.unavailable))
		return
	}
	if b.readOnly {
		finishPublishGroup(requests, ErrReadOnly)
		return
	}

	var entries []wal.Entry
	var owners []*publishRequest
	for {
		entries, owners = b.preparePublishGroup(requests)
		if len(entries) == 0 {
			break
		}
		if err := b.wal.AppendGroup(entries); err != nil {
			var encodingErr *wal.EntryEncodingError
			if errors.As(err, &encodingErr) && encodingErr.Index >= 0 && encodingErr.Index < len(owners) {
				owners[encodingErr.Index].err = encodingErr.Err
				continue
			}
			b.setUnavailableLocked(err)
			for _, request := range requests {
				if request.err == nil {
					request.err = fmt.Errorf("%w: %v", ErrBrokerUnavailable, err)
				}
			}
			finishPublishGroup(requests, nil)
			return
		}
		b.groupCommitGroups.Add(1)
		b.groupCommitFrames.Add(uint64(len(entries)))
		break
	}

	for _, entry := range entries {
		if err := b.applyEntry(entry); err != nil {
			b.setUnavailableLocked(err)
			for _, request := range requests {
				if request.accepted && request.err == nil {
					request.err = fmt.Errorf("%w: apply durable publish: %v", ErrBrokerUnavailable, err)
				}
			}
			finishPublishGroup(requests, nil)
			return
		}
	}

	needsReplication := false
	for _, request := range requests {
		if request.accepted && request.err == nil {
			needsReplication = true
			break
		}
	}
	if needsReplication {
		if err := b.replicateLocked(); err != nil {
			for _, request := range requests {
				if request.accepted && request.err == nil {
					request.err = err
				}
			}
		}
	}

	for _, request := range requests {
		if request.alias != nil {
			request.err = request.alias.err
			if request.err == nil {
				request.offsets = append([]uint64(nil), request.alias.offsets...)
			}
		}
	}
	finishPublishGroup(requests, nil)
}

func (b *Broker) preparePublishGroup(requests []*publishRequest) ([]wal.Entry, []*publishRequest) {
	entries := make([]wal.Entry, 0, len(requests))
	owners := make([]*publishRequest, 0, len(requests))
	ends := make(map[string]uint64)
	var seenIDs map[string]*publishRequest

	for _, request := range requests {
		request.alias = nil
		request.offsets = nil
		request.accepted = false
		if request.err != nil {
			continue
		}

		if request.requestID != "" {
			if seenIDs == nil {
				seenIDs = make(map[string]*publishRequest)
			}
			b.mu.RLock()
			previous, exists := b.requests[request.requestID]
			b.mu.RUnlock()
			if exists {
				if previous.topic != request.topic || previous.digest != request.digest {
					request.err = ErrIdempotencyConflict
					continue
				}
				request.offsets = append([]uint64(nil), previous.offsets...)
				request.accepted = true
				seenIDs[request.requestID] = request
				continue
			}
			if previous := seenIDs[request.requestID]; previous != nil {
				if previous.topic != request.topic || previous.digest != request.digest {
					request.err = ErrIdempotencyConflict
					continue
				}
				request.alias = previous
				request.accepted = true
				continue
			}
		}

		end, ok := ends[request.topic]
		if !ok {
			b.mu.RLock()
			topicLog := b.topics[request.topic]
			b.mu.RUnlock()
			if topicLog != nil {
				end = topicLog.EndOffset()
			}
		}
		count := uint64(len(request.messages))
		if count > ^uint64(0)-end {
			request.err = errors.New("topic offset overflow")
			continue
		}
		records := make([]messageLog.Record, len(request.messages))
		offsets := make([]uint64, len(request.messages))
		for i, message := range request.messages {
			offset := end + uint64(i)
			offsets[i] = offset
			records[i] = messageLog.Record{
				Offset:    offset,
				Key:       message.Key,
				Value:     message.Value,
				Timestamp: time.Now().UTC(),
			}
		}
		request.offsets = offsets
		request.accepted = true
		ends[request.topic] = end + count

		var entry wal.Entry
		switch {
		case request.requestID != "":
			entry = wal.Entry{Kind: wal.IdempotentBatchEntry, RequestID: request.requestID, Topic: request.topic, Records: records}
			seenIDs[request.requestID] = request
		case len(records) == 1:
			entry = wal.Entry{Kind: wal.PublishEntry, Topic: request.topic, Record: records[0]}
		default:
			entry = wal.Entry{Kind: wal.BatchEntry, Topic: request.topic, Records: records}
		}
		entries = append(entries, entry)
		owners = append(owners, request)
	}
	return entries, owners
}

func finishPublishGroup(requests []*publishRequest, groupErr error) {
	for _, request := range requests {
		err := request.err
		if err == nil {
			err = groupErr
		}
		request.done <- publishResponse{
			offsets: append([]uint64(nil), request.offsets...),
			err:     err,
		}
	}
}

func (b *Broker) Fetch(topic string, offset uint64, limit int) ([]messageLog.Record, error) {
	if !validName(topic) {
		return nil, ErrInvalidTopic
	}

	b.mu.RLock()
	topicLog, ok := b.topics[topic]
	unavailable := b.unavailable
	b.mu.RUnlock()
	if unavailable != nil {
		return nil, fmt.Errorf("%w: %v", ErrBrokerUnavailable, unavailable)
	}
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTopicNotFound, topic)
	}

	return topicLog.Read(offset, limit)
}

func (b *Broker) FetchGroup(group, topic string, limit int) ([]messageLog.Record, error) {
	if !validName(group) {
		return nil, ErrInvalidGroup
	}
	if !validName(topic) {
		return nil, ErrInvalidTopic
	}

	key := groupTopic{group: group, topic: topic}
	b.mu.RLock()
	topicLog, topicExists := b.topics[topic]
	nextOffset, hasCommit := b.committed[key]
	unavailable := b.unavailable
	b.mu.RUnlock()
	if unavailable != nil {
		return nil, fmt.Errorf("%w: %v", ErrBrokerUnavailable, unavailable)
	}
	if !topicExists {
		return nil, fmt.Errorf("%w: %q", ErrTopicNotFound, topic)
	}

	if !hasCommit {
		return topicLog.ReadFromStart(limit)
	}
	return topicLog.Read(nextOffset, limit)
}

func (b *Broker) Commit(group, topic string, nextOffset uint64) error {
	if !validName(group) {
		return ErrInvalidGroup
	}
	if !validName(topic) {
		return ErrInvalidTopic
	}

	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if b.closing || b.closed {
		return ErrBrokerClosed
	}
	if b.unavailable != nil {
		return fmt.Errorf("%w: %v", ErrBrokerUnavailable, b.unavailable)
	}
	if b.readOnly {
		return ErrReadOnly
	}

	key := groupTopic{group: group, topic: topic}
	b.mu.RLock()
	topicLog, topicExists := b.topics[topic]
	committed, hasCommit := b.committed[key]
	b.mu.RUnlock()
	if !topicExists {
		return fmt.Errorf("%w: %q", ErrTopicNotFound, topic)
	}

	endOffset := topicLog.EndOffset()
	startOffset := topicLog.StartOffset()
	if nextOffset < startOffset {
		return fmt.Errorf("%w: got %d, topic start is %d", ErrOffsetExpired, nextOffset, startOffset)
	}
	if nextOffset > endOffset {
		return fmt.Errorf("%w: got %d, topic end is %d", ErrCommitOutOfRange, nextOffset, endOffset)
	}

	if hasCommit && nextOffset < committed {
		return fmt.Errorf("%w: current %d, requested %d", ErrCommitRegression, committed, nextOffset)
	}
	if b.wal != nil {
		if err := b.wal.AppendCommit(group, topic, nextOffset); err != nil {
			return err
		}
	}

	b.mu.Lock()
	if b.committed == nil {
		b.committed = make(map[groupTopic]uint64)
	}
	b.committed[key] = nextOffset
	b.mu.Unlock()
	return b.replicateLocked()
}

func (b *Broker) RetainFrom(topic string, startOffset uint64) error {
	if !validName(topic) {
		return ErrInvalidTopic
	}

	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	if b.closing || b.closed {
		return ErrBrokerClosed
	}
	if b.unavailable != nil {
		return fmt.Errorf("%w: %v", ErrBrokerUnavailable, b.unavailable)
	}
	if b.readOnly {
		return ErrReadOnly
	}
	b.mu.RLock()
	topicLog, exists := b.topics[topic]
	b.mu.RUnlock()
	if !exists {
		return fmt.Errorf("%w: %q", ErrTopicNotFound, topic)
	}
	currentStart := topicLog.StartOffset()
	if startOffset < currentStart {
		return fmt.Errorf("%w: current %d, requested %d", ErrRetentionRegression, currentStart, startOffset)
	}
	if startOffset > topicLog.EndOffset() {
		return fmt.Errorf("%w: got %d, topic end is %d", ErrRetentionOutOfRange, startOffset, topicLog.EndOffset())
	}
	if startOffset > currentStart && b.wal != nil {
		if err := b.wal.AppendRetention(topic, startOffset); err != nil {
			if errors.Is(err, wal.ErrUnavailable) {
				b.setUnavailableLocked(err)
				return fmt.Errorf("%w: persist retention start: %v", ErrBrokerUnavailable, err)
			}
			return err
		}
	}
	if startOffset > currentStart {
		if err := topicLog.RetainFrom(startOffset); err != nil {
			b.setUnavailableLocked(err)
			return fmt.Errorf("%w: apply durable retention start: %v", ErrBrokerUnavailable, err)
		}
	}

	return b.replicateLocked()
}

func (b *Broker) applyEntry(entry wal.Entry) error {
	switch entry.Kind {
	case wal.PublishEntry:
		if !validName(entry.Topic) {
			return ErrInvalidTopic
		}
		topicLog := b.topicForAppend(entry.Topic)
		return topicLog.AppendRecord(entry.Record)
	case wal.BatchEntry:
		if !validName(entry.Topic) {
			return ErrInvalidTopic
		}
		topicLog := b.topicForAppend(entry.Topic)
		return topicLog.AppendRecords(entry.Records)
	case wal.IdempotentBatchEntry:
		if !validName(entry.RequestID) {
			return ErrInvalidRequestID
		}
		if !validName(entry.Topic) {
			return ErrInvalidTopic
		}
		b.mu.RLock()
		_, exists := b.requests[entry.RequestID]
		b.mu.RUnlock()
		if exists {
			return fmt.Errorf("duplicate request ID %q in WAL", entry.RequestID)
		}
		topicLog := b.topicForAppend(entry.Topic)
		if err := topicLog.AppendRecords(entry.Records); err != nil {
			return err
		}
		offsets := make([]uint64, len(entry.Records))
		messages := make([]Message, len(entry.Records))
		for i, record := range entry.Records {
			offsets[i] = record.Offset
			messages[i] = Message{Key: record.Key, Value: record.Value}
		}
		b.mu.Lock()
		if b.requests == nil {
			b.requests = make(map[string]publishResult)
		}
		b.requests[entry.RequestID] = publishResult{
			topic:   entry.Topic,
			digest:  batchDigest(entry.Topic, messages),
			offsets: offsets,
		}
		b.mu.Unlock()
		return nil
	case wal.CommitEntry:
		if !validName(entry.Group) {
			return ErrInvalidGroup
		}
		if !validName(entry.Topic) {
			return ErrInvalidTopic
		}
		b.mu.RLock()
		topicLog, topicExists := b.topics[entry.Topic]
		b.mu.RUnlock()
		if !topicExists {
			return fmt.Errorf("%w: %q", ErrTopicNotFound, entry.Topic)
		}
		if entry.NextOffset > topicLog.EndOffset() {
			return ErrCommitOutOfRange
		}
		if entry.NextOffset < topicLog.StartOffset() {
			return ErrOffsetExpired
		}
		key := groupTopic{group: entry.Group, topic: entry.Topic}
		b.mu.RLock()
		committed, hasCommit := b.committed[key]
		b.mu.RUnlock()
		if hasCommit && entry.NextOffset < committed {
			return ErrCommitRegression
		}
		b.mu.Lock()
		if b.committed == nil {
			b.committed = make(map[groupTopic]uint64)
		}
		b.committed[key] = entry.NextOffset
		b.mu.Unlock()
		return nil
	case wal.RetentionEntry:
		if !validName(entry.Topic) {
			return ErrInvalidTopic
		}
		b.mu.RLock()
		topicLog, topicExists := b.topics[entry.Topic]
		b.mu.RUnlock()
		if !topicExists {
			return fmt.Errorf("%w: %q", ErrTopicNotFound, entry.Topic)
		}
		if entry.StartOffset < topicLog.StartOffset() {
			return ErrRetentionRegression
		}
		if entry.StartOffset > topicLog.EndOffset() {
			return ErrRetentionOutOfRange
		}
		return topicLog.RetainFrom(entry.StartOffset)
	default:
		return fmt.Errorf("unknown WAL entry kind %d", entry.Kind)
	}
}

func validName(value string) bool {
	return len(value) > 0 && len(value) <= maxBrokerNameBytes
}

var sha256Pool = sync.Pool{
	New: func() any {
		return sha256.New()
	},
}

func batchDigest(topic string, messages []Message) [sha256.Size]byte {
	hasher := sha256Pool.Get().(hash.Hash)
	hasher.Reset()
	defer sha256Pool.Put(hasher)

	writeDigestField(hasher, []byte(topic))
	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], uint64(len(messages)))
	_, _ = hasher.Write(count[:])
	for _, message := range messages {
		writeDigestField(hasher, message.Key)
		writeDigestField(hasher, message.Value)
	}
	var digest [sha256.Size]byte
	hasher.Sum(digest[:0])
	return digest
}

func writeDigestField(hasher hash.Hash, value []byte) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write(value)
}

func (b *Broker) topicForAppend(topic string) *messageLog.Log {
	b.mu.RLock()
	topicLog := b.topics[topic]
	b.mu.RUnlock()
	if topicLog != nil {
		return topicLog
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.topics == nil {
		b.topics = make(map[string]*messageLog.Log)
	}
	if topicLog = b.topics[topic]; topicLog == nil {
		topicLog = &messageLog.Log{}
		b.topics[topic] = topicLog
	}
	return topicLog
}
