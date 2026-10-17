//go:build darwin || freebsd || linux

package dirlock

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

var ErrLocked = errors.New("data directory is already in use")

type Lock struct {
	file *os.File
}

func Acquire(path string) (*Lock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open data directory lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, path)
		}
		return nil, fmt.Errorf("lock data directory: %w", err)
	}
	return &Lock{file: file}, nil
}

func (l *Lock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	if unlockErr != nil {
		unlockErr = fmt.Errorf("unlock data directory: %w", unlockErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close data directory lock: %w", closeErr)
	}
	return errors.Join(unlockErr, closeErr)
}
