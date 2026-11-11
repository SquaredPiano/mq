package raft

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	metaFileName = "raft.meta"
	logFileName  = "raft.log"
	magic        = uint32(0x3BC2A94F)
	headerSize   = 16
	metaMagic    = uint32(0xBE516C01)
	metaSize     = metaMagicSize
)

const metaMagicSize = 4 + 8 + 4 + sha256.Size

type FileStorage struct {
	dir string
	log *os.File
}

func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if len(name) > 11 && (name[:11] == ".raft-meta-" || len(name) > 10 && name[:10] == ".raft-log-") {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
	file, err := os.OpenFile(filepath.Join(dir, logFileName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir, log: file}, nil
}

func (s *FileStorage) LoadMeta() (uint64, int, error) {
	body, err := os.ReadFile(filepath.Join(s.dir, metaFileName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, -1, nil
	}
	if err != nil {
		return 0, -1, err
	}
	if len(body) != metaMagicSize {
		return 0, -1, fmt.Errorf("raft.meta has %d bytes, want %d", len(body), metaMagicSize)
	}
	if binary.LittleEndian.Uint32(body[:4]) != metaMagic {
		return 0, -1, errors.New("raft.meta magic is invalid")
	}
	stored := body[metaMagicSize-sha256.Size:]
	digest := sha256.Sum256(body[:metaMagicSize-sha256.Size])
	if !bytes.Equal(stored, digest[:]) {
		return 0, -1, errors.New("raft.meta checksum mismatch")
	}
	term := binary.LittleEndian.Uint64(body[4:12])
	votedFor := int(int32(binary.LittleEndian.Uint32(body[12:16])))
	return term, votedFor, nil
}

func syncDir(directory string) error {
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(directoryFile.Sync(), directoryFile.Close())
}

func (s *FileStorage) SaveMeta(term uint64, votedFor int) error {
	body := make([]byte, metaMagicSize)
	binary.LittleEndian.PutUint32(body[:4], metaMagic)
	binary.LittleEndian.PutUint64(body[4:12], term)
	binary.LittleEndian.PutUint32(body[12:16], uint32(int32(votedFor)))
	digest := sha256.Sum256(body[:metaMagicSize-sha256.Size])
	copy(body[metaMagicSize-sha256.Size:], digest[:])
	stage, err := os.CreateTemp(s.dir, ".raft-meta-*")
	if err != nil {
		return err
	}
	stagePath := stage.Name()
	if _, err := stage.Write(body); err != nil {
		_ = stage.Close()
		_ = os.Remove(stagePath)
		return err
	}
	if err := stage.Sync(); err != nil {
		_ = stage.Close()
		_ = os.Remove(stagePath)
		return err
	}
	if err := stage.Close(); err != nil {
		_ = os.Remove(stagePath)
		return err
	}
	if err := os.Rename(stagePath, filepath.Join(s.dir, metaFileName)); err != nil {
		return err
	}
	return syncDir(s.dir)
}

func encodeFrame(entry Entry) []byte {
	frame := make([]byte, headerSize+len(entry.Command)+4)
	binary.LittleEndian.PutUint32(frame[:4], magic)
	binary.LittleEndian.PutUint64(frame[4:12], entry.Term)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(len(entry.Command)))
	copy(frame[headerSize:headerSize+len(entry.Command)], entry.Command)
	checksum := crc32.ChecksumIEEE(frame[:headerSize+len(entry.Command)])
	binary.LittleEndian.PutUint32(frame[headerSize+len(entry.Command):], checksum)
	return frame
}

func (s *FileStorage) Load() ([]Entry, error) {
	stat, err := s.log.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() == 0 {
		return nil, nil
	}
	if _, err := s.log.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	buffer := make([]byte, stat.Size())
	if _, err := io.ReadFull(s.log, buffer); err != nil {
		return nil, err
	}
	var entries []Entry
	var position uint64
	for position < uint64(len(buffer)) {
		remaining := uint64(len(buffer)) - position
		if remaining < headerSize {
			break
		}
		stored := buffer[position : position+headerSize]
		if binary.LittleEndian.Uint32(stored[:4]) != magic {
			break
		}
		payloadLength := uint64(binary.LittleEndian.Uint32(stored[12:16]))
		if payloadLength > uint64(1)<<40 {
			return nil, fmt.Errorf("raft log frame at offset %d declares %d bytes", position, payloadLength)
		}
		end := position + headerSize + payloadLength + 4
		if end > uint64(len(buffer)) {
			break
		}
		payload := buffer[position+headerSize : position+headerSize+payloadLength]
		checksum := binary.LittleEndian.Uint32(buffer[position+headerSize+payloadLength : end])
		if crc32.ChecksumIEEE(buffer[position:position+headerSize+payloadLength]) != checksum {
			return nil, fmt.Errorf("raft log frame at offset %d has a bad checksum", position)
		}
		entries = append(entries, Entry{
			Term:    binary.LittleEndian.Uint64(stored[4:12]),
			Command: append([]byte(nil), payload...),
		})
		position = end
	}
	if position < uint64(len(buffer)) {
		if position == 0 {
			return nil, fmt.Errorf("raft log is corrupt; refusing to open with %d bytes", len(buffer))
		}
		if err := s.log.Truncate(int64(position)); err != nil {
			return nil, fmt.Errorf("truncate torn raft log tail: %w", err)
		}
		if err := s.log.Sync(); err != nil {
			return nil, err
		}
		if err := syncDir(s.dir); err != nil {
			return nil, err
		}
	}
	if _, err := s.log.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *FileStorage) Append(entry Entry, index uint64) error {
	if _, err := s.log.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if _, err := s.log.Write(encodeFrame(entry)); err != nil {
		return err
	}
	return s.log.Sync()
}

func (s *FileStorage) TruncateFrom(index uint64) error {
	current, err := s.Load()
	if err != nil {
		return err
	}
	if index >= uint64(len(current)) {
		return nil
	}
	tmp := filepath.Join(s.dir, logFileName+".tmp")
	stage, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	for currentIndex, entry := range current {
		if uint64(currentIndex) >= index {
			break
		}
		if _, err := stage.Write(encodeFrame(entry)); err != nil {
			_ = stage.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := stage.Sync(); err != nil {
		_ = stage.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := stage.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, logFileName)); err != nil {
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	log, err := os.OpenFile(filepath.Join(s.dir, logFileName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	_ = s.log.Close()
	s.log = log
	return nil
}

func (s *FileStorage) Close() error {
	return s.log.Close()
}

type MemoryStorage struct {
	term  uint64
	voted int
	meta  bool
	lo    []Entry
}

func (m *MemoryStorage) LoadMeta() (uint64, int, error) {
	if !m.meta {
		return 0, -1, nil
	}
	return m.term, m.voted, nil
}

func (m *MemoryStorage) SaveMeta(term uint64, votedFor int) error {
	m.term = term
	m.voted = votedFor
	m.meta = true
	return nil
}

func (m *MemoryStorage) Load() ([]Entry, error) {
	entries := make([]Entry, len(m.lo))
	for i := range m.lo {
		entries[i] = Entry{Term: m.lo[i].Term, Command: append([]byte(nil), m.lo[i].Command...)}
	}
	return entries, nil
}

func (m *MemoryStorage) Append(entry Entry, index uint64) error {
	if index != uint64(len(m.lo))+1 {
		return fmt.Errorf("append index %d, want %d", index, len(m.lo)+1)
	}
	m.lo = append(m.lo, Entry{Term: entry.Term, Command: append([]byte(nil), entry.Command...)})
	return nil
}

func (m *MemoryStorage) TruncateFrom(index uint64) error {
	if index >= uint64(len(m.lo)) {
		return nil
	}
	m.lo = m.lo[:index]
	return nil
}

func (m *MemoryStorage) Close() error {
	return nil
}
