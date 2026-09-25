// Package blocks manages content-addressed immutable data blocks on local disk.
//
// Writers first stage a block under <root>/tmp (never readable by name), then
// Publish atomically renames it to <root>/data/<sha256-hex>. Publishing the
// same content twice is idempotent (content de-duplication). Every rename and
// write is followed by an fsync (of the file and, where required, its parent
// directory) so that a published block is guaranteed readable after restart.
package blocks

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ErrInvalidChecksum indicates a malformed checksum argument.
var ErrInvalidChecksum = errors.New("blocks: invalid checksum")

// Store stores content-addressed immutable blocks under root/data.
type Store struct {
	root string
	data string
	tmp  string
}

// TempBlock is a block staged in root/tmp, not yet published.
type TempBlock struct {
	path     string
	size     int64
	checksum string
}

// Open opens (creating if necessary) the block store rooted at root.
func Open(root string) (*Store, error) {
	s := &Store{
		root: root,
		data: filepath.Join(root, "data"),
		tmp:  filepath.Join(root, "tmp"),
	}
	for _, d := range []string{s.data, s.tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
		if err := syncDir(filepath.Dir(d)); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// CreateTemp streams a new temporary block from r; size may be -1 if unknown.
// The returned block is fully fsynced. On reader error the staged file is
// removed and the error is returned.
func (s *Store) CreateTemp(r io.Reader, size int64) (*TempBlock, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	path := filepath.Join(s.tmp, hex.EncodeToString(id[:])+".tmp")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	n, cerr := io.Copy(io.MultiWriter(f, h), r)
	if syncErr := f.Sync(); cerr == nil {
		cerr = syncErr
	}
	if closeErr := f.Close(); cerr == nil {
		cerr = closeErr
	}
	if cerr != nil {
		_ = os.Remove(path)
		return nil, cerr
	}
	if size >= 0 && n != size {
		_ = os.Remove(path)
		return nil, fmt.Errorf("blocks: short write: got %d bytes, declared %d", n, size)
	}
	if err := syncDir(s.tmp); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return &TempBlock{path: path, size: n, checksum: hex.EncodeToString(h.Sum(nil))}, nil
}

// Size returns the byte length of the temporary block.
func (t *TempBlock) Size() int64 { return t.size }

// Checksum returns the lowercase hex SHA-256 of the temporary block contents.
func (t *TempBlock) Checksum() string { return t.checksum }

// dataPath validates checksum and returns its on-disk path.
func (s *Store) dataPath(checksum string) (string, error) {
	if !validChecksum(checksum) {
		return "", fmt.Errorf("%w: %q", ErrInvalidChecksum, checksum)
	}
	return filepath.Join(s.data, checksum), nil
}

func validChecksum(c string) bool {
	if len(c) != 64 {
		return false
	}
	_, err := hex.DecodeString(c)
	return err == nil && !strings.ContainsRune(c, filepath.Separator)
}

// Publish atomically renames the temp block to data/<hex> (idempotent).
// It returns the lowercase hex SHA-256 checksum.
func (s *Store) Publish(t *TempBlock) (string, error) {
	dst, err := s.dataPath(t.checksum)
	if err != nil {
		return "", err
	}
	if _, statErr := os.Stat(dst); statErr == nil {
		// Identical content already published: discard the duplicate temp file.
		if rmErr := os.Remove(t.path); rmErr != nil && !os.IsNotExist(rmErr) {
			return "", rmErr
		}
		if err := syncDir(s.tmp); err != nil {
			return "", err
		}
		t.path = ""
		return t.checksum, nil
	} else if !os.IsNotExist(statErr) {
		return "", statErr
	}
	if err := os.Link(t.path, dst); err != nil {
		return "", err
	}
	if err := syncDir(s.data); err != nil {
		return "", err
	}
	if err := os.Remove(t.path); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if err := syncDir(s.tmp); err != nil {
		return "", err
	}
	t.path = ""
	return t.checksum, nil
}

// Open opens a published block for reading by checksum.
func (s *Store) Open(checksum string) (io.ReadCloser, error) {
	p, err := s.dataPath(checksum)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("blocks: missing block %s", checksum)
	}
	return f, err
}

// Stat reports whether a published block exists and its size.
func (s *Store) Stat(checksum string) (size int64, exists bool, err error) {
	p, err := s.dataPath(checksum)
	if err != nil {
		return 0, false, err
	}
	fi, statErr := os.Stat(p)
	if os.IsNotExist(statErr) {
		return 0, false, nil
	}
	if statErr != nil {
		return 0, false, statErr
	}
	return fi.Size(), true, nil
}

// Remove deletes one published block if present (idempotent).
func (s *Store) Remove(checksum string) error {
	p, err := s.dataPath(checksum)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(s.data)
}

// RemoveTemp deletes an unpublished temporary block (idempotent).
func (s *Store) RemoveTemp(t *TempBlock) error {
	if t == nil || t.path == "" {
		return nil
	}
	if err := os.Remove(t.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	t.path = ""
	return syncDir(s.tmp)
}

// CleanupTemp removes leftover files from root/tmp (crash recovery).
func (s *Store) CleanupTemp() error {
	entries, err := os.ReadDir(s.tmp)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(s.tmp, e.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return syncDir(s.tmp)
}

// List returns checksums of all published blocks.
func (s *Store) List() ([]string, error) {
	entries, err := os.ReadDir(s.data)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() && validChecksum(name) {
			out = append(out, name)
		}
	}
	return out, nil
}
