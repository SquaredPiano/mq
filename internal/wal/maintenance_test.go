package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	messageLog "mq/internal/log"
)

const walTestSegmentBytes = MinSegmentBytes

func openWalForMaintenance(t *testing.T, path string, checkpoint int64, apply func(Entry) error) *WAL {
	t.Helper()
	store, err := OpenAndReplayWithSegmentBytes(path, walTestSegmentBytes, apply)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func largeSegmentRecord(sequence int) messageLog.Record {
	return messageLog.Record{Offset: uint64(sequence), Value: make([]byte, 96*1024)}
}

func segmentFilesLive(store *WAL) int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.segments)
}

func fillSegmentsForTrim(t *testing.T, path string, minimumSegmentFiles int, capture func(*WAL)) {
	t.Helper()
	store := openWalForMaintenance(t, path, 0, func(Entry) error { return nil })
	for i := 0; segmentFilesLive(store) < minimumSegmentFiles; i++ {
		if err := store.AppendPublish("events", largeSegmentRecord(i)); err != nil {
			t.Fatal(err)
		}
		if i > 1000 {
			t.Fatalf("segments = %d, want at least %d after %d writes", segmentFilesLive(store), minimumSegmentFiles, i)
		}
	}
	if capture != nil {
		capture(store)
	}
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestTrimPrefixToDropsLeadingSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	var replay []uint64
	var lastSegmentStart int64
	fillSegmentsForTrim(t, path, 3, func(store *WAL) {
		store.mu.Lock()
		defer store.mu.Unlock()
		lastSegmentStart = store.segments[len(store.segments)-1].start
	})
	store := openWalForMaintenance(t, path, 0, func(entry Entry) error {
		replay = append(replay, uint64(entry.Kind))
		return nil
	})
	beforeFiles, err := listSegmentFiles(path)
	if err != nil {
		t.Fatal(err)
	}
	replay = nil
	if err := store.TrimPrefixTo(lastSegmentStart); err != nil {
		t.Fatalf("TrimPrefixTo(%d): %v", lastSegmentStart, err)
	}
	if store.Base() != lastSegmentStart {
		t.Fatalf("base = %d, want %d", store.Base(), lastSegmentStart)
	}
	if _, err := store.HashPrefix(lastSegmentStart); err != nil {
		t.Fatalf("HashPrefix(%d) after trim: %v", lastSegmentStart, err)
	}
	if _, err := store.HashPrefix(lastSegmentStart - 1); err == nil {
		t.Fatal("HashPrefix below base must fail")
	}
	afterFiles, err := listSegmentFiles(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterFiles) >= len(beforeFiles) {
		t.Fatalf("files after trim = %d, before = %d", len(afterFiles), len(beforeFiles))
	}
	for _, item := range afterFiles {
		if _, err := os.Stat(item.path); err != nil {
			t.Fatalf("survivor file missing: %v", err)
		}
	}
	data, err := os.ReadFile(walBasePath(path))
	if err != nil {
		t.Fatalf("read wal.base marker: %v", err)
	}
	fields := strings.Fields(strings.TrimSpace(string(data)))
	if len(fields) != 4 || fields[0] != fmt.Sprintf("%d", lastSegmentStart) {
		t.Fatalf("marker contents %q", string(data))
	}
	survivor, err := strconvFor(fields[2])
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	firstSequence := store.segments[0].sequence
	store.mu.Unlock()
	if survivor != firstSequence {
		t.Fatalf("marker survivor sequence %d != live sequence %d", survivor, firstSequence)
	}
	if len(replay) != 0 {
		t.Fatalf("trim should not replay: %d entries", len(replay))
	}
	if err := store.TrimPrefixTo(lastSegmentStart - 1); err == nil {
		t.Fatal("trim below base must fail")
	}
	if store.Base() != lastSegmentStart {
		t.Fatalf("failed trim moved base = %d", store.Base())
	}
}

func TestTrimPrefixToRefusesBeyondSizeAndBelowBase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	store := openWalForMaintenance(t, path, 0, func(Entry) error { return nil })
	for i := 0; segmentFilesLive(store) < 3; i++ {
		if err := store.AppendPublish("events", largeSegmentRecord(i)); err != nil {
			t.Fatal(err)
		}
	}
	size, err := store.Size()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.TrimPrefixTo(size + 1); err == nil {
		t.Fatal("trim above WAL size must fail")
	}
	if err := store.TrimPrefixTo(-1); err == nil {
		t.Fatal("negative trim must fail")
	}
	store.mu.Lock()
	second := store.segments[len(store.segments)-2].start + store.segments[len(store.segments)-2].size
	store.mu.Unlock()
	if err := store.TrimPrefixTo(second); err != nil {
		t.Fatalf("in-segment trim %d: %v", second, err)
	}
	if second < store.Base() {
		t.Fatalf("trim target %d below base %d", second, store.Base())
	}
	if err := store.TrimPrefixTo(second - 1); err == nil {
		t.Fatal("trim below base must fail")
	}
}

func TestTrimCrashMarkerCommittedButSegmentsUnlinkedPartially(t *testing.T) {
	full, err := os.MkdirTemp(t.TempDir(), "roll")
	if err != nil {
		t.Fatal(err)
	}
	rollPath := filepath.Join(full, "wal.log")
	store := openWalForMaintenance(t, rollPath, 0, func(Entry) error { return nil })
	for i := 0; segmentFilesLive(store) < 5; i++ {
		if err := store.AppendPublish("events", largeSegmentRecord(i)); err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	survivor := store.segments[2]
	preceding := make([]string, 2)
	for index := 0; index < 2; index++ {
		preceding[index] = store.segments[index].path
	}
	frames := store.totalSize - survivor.start
	store.mu.Unlock()
	target := survivor.start
	if err := store.TrimPrefixTo(target); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	crash, err := os.ReadDir(filepath.Dir(rollPath))
	if err != nil {
		t.Fatal(err)
	}
	rollback := filepath.Join(t.TempDir(), "wal.log")
	if err := os.MkdirAll(filepath.Dir(rollback), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, entry := range crash {
		source := filepath.Join(filepath.Dir(rollPath), entry.Name())
		if err := os.Rename(source, filepath.Join(filepath.Dir(rollback), entry.Name())); err != nil {
			t.Fatal(err)
		}
	}
	marker, err := os.ReadFile(walBasePath(rollback))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(strings.TrimSpace(string(marker)))
	if fields[0] != fmt.Sprintf("%d", target) || fields[2] == "0" {
		t.Fatalf("marker %q", string(marker))
	}
	for _, p := range preceding {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("preceding segment %q survived rollback: %v", p, err)
		}
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(rollback), filepath.Base(survivor.path))); err != nil {
		t.Fatalf("survivor missing: %v", err)
	}
	restored, err := OpenAndReplayFrom(rollback, walTestSegmentBytes, target, func(Entry) error { return nil })
	if err != nil {
		t.Fatalf("reopen crash window: %v", err)
	}
	if restored.Base() != target {
		t.Fatalf("base = %d, want %d", restored.Base(), target)
	}
	recoveredSize, err := restored.Size()
	if err != nil {
		t.Fatal(err)
	}
	if recoveredSize != target+frames {
		t.Fatalf("size = %d, want %d", recoveredSize, target+frames)
	}
	if len(restored.segments) != 3 {
		t.Fatalf("segments = %d, want 3", len(restored.segments))
	}
	restored.mu.Lock()
	firstStart := restored.segments[0].start
	lastSeq := restored.segments[len(restored.segments)-1].sequence
	restored.mu.Unlock()
	if firstStart != target || lastSeq != survivor.sequence+2 {
		t.Fatalf("layout start=%d seq=%d want start=%d seq=%d", firstStart, lastSeq, target, survivor.sequence+2)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	_ = trimParcel(t, marker)
}

func TestMarkerStageFileIsDiscardedOnLaunch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	store := openWalForMaintenance(t, path, 0, func(Entry) error { return nil })
	for i := 0; segmentFilesLive(store) < 3; i++ {
		if err := store.AppendPublish("events", largeSegmentRecord(i)); err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	target := store.segments[1].start
	zero := store.segments[0].path
	one := store.segments[1].path
	store.mu.Unlock()
	if err := store.TrimPrefixTo(target); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(walBasePath(path)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(one, zero); err != nil {
		t.Fatal(err)
	}
	writeFile(t, walBaseStagePath(path), fmt.Sprintf("%d %d 1 0\n", target, target))
	if _, err := OpenAndReplayFrom(path, walTestSegmentBytes, target, func(Entry) error { return nil }); err != nil {
		t.Fatalf("staged marker must be discarded, launch failed: %v", err)
	}
	if _, err := os.ReadFile(walBasePath(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("marker should stay absent: %v", err)
	}
}

func TestOpenDeletesFreshPlanLeftoverSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	store := openWalForMaintenance(t, path, 0, func(Entry) error { return nil })
	for i := 0; segmentFilesLive(store) < 3; i++ {
		if err := store.AppendPublish("events", largeSegmentRecord(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := listSegmentFiles(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 3 {
		t.Fatalf("files = %d, want at least 3", len(files))
	}
	reset := int64(15 << 20)
	if err := PrepareFreshStream(path, reset); err != nil {
		t.Fatal(err)
	}
	markerData, err := os.ReadFile(walBasePath(path))
	if err != nil {
		t.Fatal(err)
	}
	markerFields := strings.Fields(strings.TrimSpace(string(markerData)))
	if markerFields[3] != "1" || markerFields[0] != fmt.Sprintf("%d", reset) {
		t.Fatalf("fresh marker %q", string(markerData))
	}
	if _, err := os.Stat(files[len(files)-1].path); err != nil {
		t.Fatalf("leftover file should still exist before relaunch: %v", err)
	}
	restored, err := OpenAndReplayFrom(path, walTestSegmentBytes, reset, func(Entry) error { return nil })
	if err != nil {
		t.Fatalf("relaunch after fresh marker: %v", err)
	}
	defer restored.Close()
	leftovers, err := listSegmentFiles(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(leftovers) != 1 || leftovers[0].path != path {
		t.Fatalf("leftover segments survived the fresh wipe: %#v", leftovers)
	}
	size, err := restored.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != reset {
		t.Fatalf("size = %d, want reset offset %d", size, reset)
	}
	if restored.Base() != reset {
		t.Fatalf("base = %d, want %d", restored.Base(), reset)
	}
	for i := 0; i < 3; i++ {
		if err := restored.AppendPublish("events", messageLog.Record{Offset: uint64(i), Value: []byte("later")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := OpenAndReplayFrom(path, walTestSegmentBytes, reset, func(Entry) error { return nil })
	if err != nil {
		t.Fatalf("second relaunch: %v", err)
	}
	defer next.Close()
	nextSize, err := next.Size()
	if err != nil {
		t.Fatal(err)
	}
	if nextSize == reset {
		t.Fatal("appended frames vanished after converting the marker")
	}
}

func TestCorruptMarkerRefusesLaunch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	store := openWalForMaintenance(t, path, 0, func(Entry) error { return nil })
	if err := store.AppendPublish("events", messageLog.Record{Offset: 0, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	writeFile(t, walBasePath(path), "1 2 3 4 5\n")
	if _, err := OpenAndReplayFrom(path, walTestSegmentBytes, 0, func(Entry) error { return nil }); err == nil {
		t.Fatal("corrupt marker must refuse launch")
	}
	writeFile(t, walBasePath(path), "999 0 0 0\n")
	if _, err := OpenAndReplayFrom(path, walTestSegmentBytes, 0, func(Entry) error { return nil }); err == nil {
		t.Fatal("inconsistent marker must refuse launch")
	}
	writeFile(t, walBasePath(path), "5 5 0 1\n")
	if _, err := OpenAndReplayFrom(path, walTestSegmentBytes, 0, func(Entry) error { return nil }); err != nil {
		t.Fatalf("coherent fresh marker permits launch: %v", err)
	}
}

func TestEmptyMarkerlessLogRequiresZeroStart(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "wal.log")
	store := openWalForMaintenance(t, path, 0, func(Entry) error { return nil })
	for i := 0; segmentFilesLive(store) < 2; i++ {
		if err := store.AppendPublish("events", largeSegmentRecord(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	listed, err := listSegmentFiles(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) < 2 {
		t.Fatalf("segments = %d, want rotation", len(listed))
	}
	if _, err := os.Stat(listed[0].path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(listed[0].path); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenAndReplayFrom(path, walTestSegmentBytes, 0, func(Entry) error { return nil }); err == nil {
		t.Fatal("markerless log missing its start segment must refuse launch")
	}
}

func trimParcel(t *testing.T, marker []byte) string {
	t.Helper()
	return strings.TrimSpace(string(marker))
}

func strconvFor(text string) (uint64, error) {
	var parsed uint64
	_, err := fmt.Sscanf(text, "%d", &parsed)
	return parsed, err
}
