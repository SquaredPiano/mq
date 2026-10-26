package broker

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"mq/internal/wal"
)

var (
	ErrStaleSnapshot    = errors.New("snapshot generation predates the installed snapshot")
	ErrSnapshotRejected = errors.New("replicated snapshot was rejected")
)

type SnapshotFileMeta struct {
	Generation uint64
	WALOffset  int64
	Digest     [sha256.Size]byte
	Size       int64
}

func (b *Broker) SnapshotIdentity() (SnapshotFileMeta, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.snapshotGeneration == 0 || b.snapshotSize == 0 {
		return SnapshotFileMeta{}, false
	}
	return SnapshotFileMeta{
		Generation: b.snapshotGeneration,
		WALOffset:  b.snapshotCheckpoint,
		Digest:     b.snapshotDigest,
		Size:       b.snapshotSize,
	}, true
}

func (b *Broker) ExportSnapshot() (*os.File, SnapshotFileMeta, error) {
	meta, ok := b.SnapshotIdentity()
	if !ok {
		return nil, SnapshotFileMeta{}, errors.New("no broker snapshot exists")
	}
	file, err := os.Open(filepath.Join(b.dataDir, snapshotFileName))
	if err != nil {
		return nil, SnapshotFileMeta{}, fmt.Errorf("%w: open snapshot file: %v", ErrSnapshotRejected, err)
	}
	info, err := file.Stat()
	if err != nil {
		closeErr := file.Close()
		return nil, SnapshotFileMeta{}, errors.Join(fmt.Errorf("%w: stat snapshot file: %v", ErrSnapshotRejected, err), closeErr)
	}
	if info.Size() != meta.Size {
		closeErr := file.Close()
		return nil, SnapshotFileMeta{}, errors.Join(fmt.Errorf("%w: snapshot file on disk reported %d bytes, recorded %d bytes", ErrSnapshotRejected, info.Size(), meta.Size), closeErr)
	}
	return file, meta, nil
}

func (b *Broker) TrimWALPrefix(offset int64) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	if b.closing || b.closed {
		return ErrBrokerClosed
	}
	if b.unavailable != nil {
		return fmt.Errorf("%w: %v", ErrBrokerUnavailable, b.unavailable)
	}
	if b.wal == nil {
		return errors.New("WAL replication requires a persistent broker")
	}
	if err := b.wal.TrimPrefixTo(offset); err != nil {
		b.setUnavailableLocked(err)
		return fmt.Errorf("%w: trim WAL prefix: %v", ErrBrokerUnavailable, err)
	}
	return nil
}

func (b *Broker) InstallReplicaSnapshot(stream io.Reader, generation uint64, digest [sha256.Size]byte, epoch uint64) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()

	if b.closing || b.closed {
		return ErrBrokerClosed
	}
	if !b.readOnly {
		return ErrNotFollower
	}
	if err := b.captureLeaderEpoch(epoch); err != nil {
		return err
	}
	if b.unavailable != nil {
		return fmt.Errorf("%w: %v", ErrBrokerUnavailable, b.unavailable)
	}
	if b.wal == nil || b.dataDir == "" {
		return errors.New("snapshot install requires a persistent broker")
	}

	stagingPath := filepath.Join(b.dataDir, ".replica-snapshot-staging")
	file, err := os.OpenFile(stagingPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("%w: create staging file: %v", ErrSnapshotRejected, err)
	}
	transferLimit := maxSnapshotBytes + snapshotHeader + sha256.Size
	written, copyErr := io.Copy(file, io.LimitReader(stream, transferLimit+1))
	closeErr := file.Close()
	oversize := written > transferLimit
	if err := errors.Join(copyErr, closeErr); err != nil || oversize {
		limitErr := error(nil)
		if oversize {
			limitErr = errors.New("replicated snapshot exceeds maximum size")
		}
		return errors.Join(fmt.Errorf("%w: stage replicated snapshot: %v", ErrSnapshotRejected, limitErr), err)
	}
	defer os.Remove(stagingPath)

	replicaSnapshot, replicaDigest, fileSize, err := loadSnapshot(stagingPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSnapshotRejected, err)
	}
	if generation == b.snapshotGeneration {
		if equalDigest(digest[:], b.snapshotDigest[:]) && replicaSnapshot.WALOffset == b.snapshotCheckpoint && equalDigest(replicaDigest[:], digest[:]) {
			return nil
		}
		return fmt.Errorf("%w: generation %d is already installed with different contents", ErrSnapshotRejected, generation)
	}
	if generation < b.snapshotGeneration {
		return fmt.Errorf("%w: received %d, installed %d", ErrStaleSnapshot, generation, b.snapshotGeneration)
	}

	newState, err := buildSnapshotState(*replicaSnapshot)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSnapshotRejected, err)
	}

	b.walMu.Lock()
	previous := b.wal
	b.wal = nil
	b.walMu.Unlock()
	if previous != nil {
		if err := previous.Close(); err != nil {
			b.setUnavailableLocked(err)
			return fmt.Errorf("%w: close WAL before snapshot install: %v", ErrSnapshotRejected, err)
		}
	}
	walPath := filepath.Join(b.dataDir, "wal.log")
	if err := os.Rename(stagingPath, filepath.Join(b.dataDir, snapshotFileName)); err != nil {
		b.setUnavailableLocked(err)
		return fmt.Errorf("%w: install snapshot file: %v", ErrSnapshotRejected, err)
	}
	if err := syncDirectory(b.dataDir); err != nil {
		b.setUnavailableLocked(err)
		return fmt.Errorf("%w: sync snapshot install: %v", ErrSnapshotRejected, err)
	}
	if resetErr := wal.PrepareFreshStream(walPath, replicaSnapshot.WALOffset); resetErr != nil {
		b.setUnavailableLocked(resetErr)
		return fmt.Errorf("%w: prepare WAL stream: %v", ErrSnapshotRejected, resetErr)
	}
	store, openErr := wal.OpenAndReplayFrom(walPath, b.segmentBytes, replicaSnapshot.WALOffset, func(entry wal.Entry) error {
		return errors.New("unexpected WAL frame below snapshot checkpoint")
	})
	if openErr != nil {
		b.setUnavailableLocked(openErr)
		return fmt.Errorf("%w: open WAL stream: %v", ErrSnapshotRejected, openErr)
	}
	b.walMu.Lock()
	b.wal = store
	b.walMu.Unlock()

	b.mu.Lock()
	b.topics = newState.topics
	b.committed = newState.committed
	b.requests = newState.requests
	b.snapshotGeneration = replicaSnapshot.Generation
	b.snapshotCheckpoint = replicaSnapshot.WALOffset
	b.snapshotDigest = replicaDigest
	b.snapshotSize = fileSize
	b.mu.Unlock()
	return nil
}
