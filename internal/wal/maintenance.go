package wal

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const baseFileName = "wal.base"

var ErrCheckpointExceedsStream = errors.New("WAL checkpoint exceeds WAL size")

type walBaseMarker struct {
	checkpoint  int64
	prefixStart int64
	survivorSeq uint64
	freshPlan   bool
}

func walBasePath(walPath string) string {
	return filepath.Join(filepath.Dir(walPath), baseFileName)
}

func walBaseStagePath(walPath string) string {
	return walBasePath(walPath) + ".stage"
}

func readBaseMarker(walPath string) (*walBaseMarker, error) {
	data, err := os.ReadFile(walBasePath(walPath))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read WAL base marker: %w", err)
	}
	if len(data) > 128 {
		return nil, errors.New("WAL base marker is too large")
	}
	fields := strings.Fields(string(data))
	if len(fields) != 4 {
		return nil, errors.New("WAL base marker must contain four values")
	}
	checkpoint, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse WAL base marker checkpoint: %w", err)
	}
	prefixStart, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse WAL base marker prefix start: %w", err)
	}
	survivorSeq, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse WAL base marker survivor sequence: %w", err)
	}
	mode, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil || mode > 1 {
		return nil, errors.New("WAL base marker mode is invalid")
	}
	if checkpoint <= 0 || prefixStart < 0 || prefixStart > checkpoint {
		return nil, errors.New("WAL base marker values are inconsistent")
	}
	return &walBaseMarker{checkpoint: checkpoint, prefixStart: prefixStart, survivorSeq: survivorSeq, freshPlan: mode == 1}, nil
}

func writeBaseMarker(walPath string, marker walBaseMarker) error {
	stage := walBaseStagePath(walPath)
	mode := uint64(0)
	if marker.freshPlan {
		mode = 1
	}
	content := fmt.Sprintf("%d %d %d %d\n", marker.checkpoint, marker.prefixStart, marker.survivorSeq, mode)
	file, err := os.OpenFile(stage, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("write WAL base marker: %w", err)
	}
	writeErr := writeFull(file, []byte(content))
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("write WAL base marker: %w", err)
	}
	if err := os.Rename(stage, walBasePath(walPath)); err != nil {
		return fmt.Errorf("commit WAL base marker: %w", err)
	}
	if err := syncDirectory(filepath.Dir(walBasePath(walPath))); err != nil {
		return fmt.Errorf("sync WAL base marker directory: %w", err)
	}
	return nil
}

func (w *WAL) TrimPrefixTo(target int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.readyLocked(); err != nil {
		return err
	}
	if target < w.base {
		return fmt.Errorf("WAL prefix trim target %d is below the current base %d", target, w.base)
	}
	if target > w.totalSize {
		return fmt.Errorf("WAL prefix trim target %d exceeds WAL size %d", target, w.totalSize)
	}
	if target == w.base {
		return nil
	}
	removeCount := 0
	for removeCount < len(w.segments)-1 && w.segments[removeCount].start+w.segments[removeCount].size <= target {
		removeCount++
	}
	survivor := w.segments[removeCount]
	marker := walBaseMarker{
		checkpoint:  target,
		prefixStart: survivor.start,
		survivorSeq: survivor.sequence,
	}
	if err := writeBaseMarker(w.path, marker); err != nil {
		return err
	}
	w.base = target
	return w.dropPrefixSegmentsLocked(removeCount)
}

func (w *WAL) dropPrefixSegmentsLocked(count int) error {
	if count <= 0 {
		return nil
	}
	closeErr := error(nil)
	for index := 0; index < count; index++ {
		segment := w.segments[index]
		if w.readSegment == segment && w.readFile != nil {
			closeErr = errors.Join(closeErr, w.readFile.Close())
			w.readFile = nil
			w.readSegment = nil
		}
		if segment.file != nil {
			closeErr = errors.Join(closeErr, segment.file.Close())
			segment.file = nil
		}
		if err := os.Remove(segment.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			closeErr = errors.Join(closeErr, fmt.Errorf("remove WAL segment %q: %w", segment.path, err))
		}
	}
	w.segments = w.segments[count:]
	if err := syncDirectory(filepath.Dir(w.path)); err != nil {
		return errors.Join(closeErr, fmt.Errorf("sync WAL directory after trim: %w", err))
	}
	return closeErr
}

func (w *WAL) Base() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.base
}

func PrepareFreshStream(walPath string, offset int64) error {
	if offset < 0 {
		return errors.New("WAL stream offset cannot be negative")
	}
	if offset == 0 {
		if err := removeWALFiles(walPath); err != nil {
			return err
		}
		return nil
	}
	marker := walBaseMarker{checkpoint: offset, prefixStart: offset, survivorSeq: 0, freshPlan: true}
	return writeBaseMarker(walPath, marker)
}

func removeWALFiles(walPath string) error {
	directory := filepath.Dir(walPath)
	base := filepath.Base(walPath)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("read WAL directory: %w", err)
	}
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if name != base && !strings.HasPrefix(name, base+".") {
			continue
		}
		if name != base && len(name) != len(base)+21 {
			continue
		}
		if err := os.Remove(filepath.Join(directory, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove WAL segment %q: %w", name, err)
		}
		removed = true
	}
	if removed {
		if err := syncDirectory(directory); err != nil {
			return fmt.Errorf("sync WAL directory after removal: %w", err)
		}
	}
	return nil
}

func listSegmentFiles(walPath string) ([]segmentPath, error) {
	directory := filepath.Dir(walPath)
	base := filepath.Base(walPath)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read WAL directory: %w", err)
	}
	segments := make([]segmentPath, 0, 4)
	for _, entry := range entries {
		name := entry.Name()
		if name == base {
			segments = append(segments, segmentPath{path: walPath})
			continue
		}
		prefix := base + "."
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		suffix := name[len(prefix):]
		if len(suffix) != 20 {
			continue
		}
		sequence, err := strconv.ParseUint(suffix, 10, 64)
		if err != nil {
			continue
		}
		segments = append(segments, segmentPath{
			sequence: sequence,
			path:     filepath.Join(directory, name),
		})
	}
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].sequence != segments[j].sequence {
			return segments[i].sequence < segments[j].sequence
		}
		return segments[i].path < segments[j].path
	})
	return segments, nil
}
