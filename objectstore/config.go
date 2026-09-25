package objectstore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// Default tunable values.
const (
	defaultMinPartSize int64 = 5 * 1024 * 1024 // 5 MiB, like S3
	defaultMaxParts          = 10000
)

// DefaultConfig returns a Config with sensible defaults rooted at rootDir.
func DefaultConfig(rootDir string) Config {
	return Config{
		RootDir:     rootDir,
		MinPartSize: defaultMinPartSize,
		MaxParts:    defaultMaxParts,
		GCInterval:  time.Minute,
		GCUnrefAge:  30 * time.Second,
		EnableGC:    true,
	}
}

// WithMinPartSize overrides the minimum part size (the final part is exempt).
func WithMinPartSize(n int64) Option {
	return func(s *Store) { s.cfg.MinPartSize = n }
}

// WithMaxParts overrides the maximum allowed part number.
func WithMaxParts(n int) Option {
	return func(s *Store) { s.cfg.MaxParts = n }
}

// WithGCInterval sets the automatic GC sweep interval; 0 disables the timer
// (garbage collection remains available through RunGC).
func WithGCInterval(d time.Duration) Option {
	return func(s *Store) {
		s.cfg.EnableGC = d > 0
		s.cfg.GCInterval = d
	}
}

// WithGCUnrefAge sets how long a block must be unreferenced before collection.
func WithGCUnrefAge(d time.Duration) Option {
	return func(s *Store) { s.cfg.GCUnrefAge = d }
}

// WithLogger installs a structured log sink; the default writes key, version
// and part identifiers plus the decision basis to standard output.
func WithLogger(f func(format string, args ...any)) Option {
	return func(s *Store) {
		if f != nil {
			s.logf = f
		}
	}
}

// LoadConfig reads a JSON configuration file. Missing fields fall back to
// DefaultConfig for the file's directory-relative root.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	cfg := DefaultConfig(filepath.Dir(path))
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, err
	}
	if cfg.RootDir == "" {
		cfg.RootDir = filepath.Join(filepath.Dir(path), "data-root")
	}
	return cfg, nil
}
