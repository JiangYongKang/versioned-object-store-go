// Package filelock provides an advisory exclusive lock for the storage root,
// preventing two store processes from opening the same root concurrently.
package filelock

import (
	"errors"
	"os"
	"syscall"
)

// Lock is a held exclusive file lock.
type Lock struct {
	f *os.File
}

// ErrLocked indicates another process holds the lock.
var ErrLocked = errors.New("filelock: storage root is locked by another process")

// Acquire tries to take an exclusive, non-blocking flock on path.
func Acquire(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return &Lock{f: f}, nil
}

// Release unlocks and closes the lock file.
func (l *Lock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err := l.f.Close()
	l.f = nil
	return err
}
