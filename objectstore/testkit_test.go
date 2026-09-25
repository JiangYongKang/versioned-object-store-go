package objectstore

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openRecoveryStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := New(DefaultConfig(dir),
		WithMinPartSize(4),
		WithMaxParts(10),
		WithGCInterval(0),
		WithGCUnrefAge(time.Millisecond),
		WithLogger(func(string, ...any) {}),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func walPathOf(dir string) string { return filepath.Join(dir, "meta.wal") }

func errorIs(err, target error) bool { return errors.Is(err, target) }

func dataPathOf(dir, checksum string) string {
	return filepath.Join(dir, "data", checksum)
}
