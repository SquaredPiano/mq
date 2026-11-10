package raft

import (
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
	headerSize   = 13
)

type FileStorage struct {
	dir string
	log *os.File
}

func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
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
	if len(body) != 12 {
		return 0, -1, errors.New("raft.meta must contain 12 bytes")
	}
	term := binary.LittleEndian.Uint64(body[:8])
	votedFor := int(int32(binary.LittleEndian.Uint32(body[8:])))
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
	body := make([]byte, 12)
	binary.LittleEndian.PutUint64(body[:8], term)
	binary.LittleEndian.PutUint32(body[8:], uint32(int32(votedFor)))
	stage, err := os.CreateTemp(s.dir, ".raft-meta-*")
	if err != nil {
		return err
	}
	stagePath := stage.Name()
	if _, err := stage.Write(body); err != nil {
		stage.Close()
		os.Remove(stagePath)
		return err
	}
	if err := stage.Sync(); err != nil {
		stage.Close()
		os.Remove(stagePath)
		return err
	}
	if err := stage.Close(); err != nil {
		os.Remove(stagePath)
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
	binary.LittleEndian.PutUint32(frame[9:13], uint32(len(entry.Command)))
	copy(frame[headerSize:headerSize+len(entry.Command)], entry.Command)
	binary.LittleEndian.PutUint32(frame[headerSize+len(entry.Command):], crc32.ChecksumIEEE(entry.Command))
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
		if uint64(len(buffer))-position < headerSize {
			break
		}
		header := buffer[position : position+headerSize]
		if binary.LittleEndian.Uint32(header[:4]) != magic {
			break
		}
		payloadLength := uint64(binary.LittleEndian.Uint32(header[9:headerSize]))
		end := position + headerSize + payloadLength + 4
		if end > uint64(len(buffer)) {
			break
		}
		stored := buffer[position+headerSize : position+headerSize+payloadLength]
		checksum := binary.LittleEndian.Uint32(buffer[position+headerSize+payloadLength : end])
		if crc32.ChecksumIEEE(stored) != checksum {
			break
		}
		entries = append(entries, Entry{Term: binary.LittleEndian.Uint64(header[4:12]), Command: append([]byte(nil), stored...)})
		position = end
	}
	if position < uint64(len(buffer)) {
		if err := s.log.Truncate(int64(position)); err != nil {
			return nil, fmt.Errorf("truncate torn raft log tail: %w", err)
		}
		if err := s.log.Sync(); err != nil {
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
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	for currentIndex, entry := range current {
		if uint64(currentIndex) >= index {
			break
		}
		if _, err := file.Write(encodeFrame(entry)); err != nil {
			file.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmp)
		return err
	}
	if err := file.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, logFileName)); err != nil {
		return err
	}
	s.log.Close()
	log, err := os.OpenFile(filepath.Join(s.dir, logFileName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
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
	copy(entries, m.lo)
	return entries, nil
}

func (m *MemoryStorage) Append(entry Entry, index uint64) error {
	m.lo = append(m.lo, entry)
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
