//go:build !darwin && !freebsd && !linux

package dirlock

import (
	"errors"
	"fmt"
	"runtime"
)

var ErrLocked = errors.New("data directory is already in use")

type Lock struct{}

func Acquire(path string) (*Lock, error) {
	return nil, fmt.Errorf("advisory data directory locking is unsupported on %s (%s)", runtime.GOOS, path)
}

func (l *Lock) Close() error { return nil }
