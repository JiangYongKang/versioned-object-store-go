// Package objectstore implements a local-disk, versioned object store with
// conditional writes, multipart uploads, crash recovery and garbage collection.
//
// # Concurrency model
//
// A single in-process mutex (s.mu) serializes every metadata decision, and an
// flock on <root>/lock prevents two processes from opening the same root.
// Data blocks are written first (fsynced, content-addressed, immutable); only
// then is the metadata record appended to the fsynced WAL. A reader therefore
// never sees metadata pointing at missing data, and an interrupted write only
// ever leaves unreferenced blocks that GC reclaims.
package objectstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/highcumontoa/versioned-object-store-go/internal/blocks"
	"github.com/highcumontoa/versioned-object-store-go/internal/filelock"
	"github.com/highcumontoa/versioned-object-store-go/internal/meta"
	"github.com/highcumontoa/versioned-object-store-go/internal/wal"
)

// Config holds tunable parameters for a Store.
type Config struct {
	RootDir     string        `json:"root_dir"`
	MinPartSize int64         `json:"min_part_size"`
	MaxParts    int           `json:"max_parts"`
	GCInterval  time.Duration `json:"gc_interval"`
	GCUnrefAge  time.Duration `json:"gc_unref_age"`
	EnableGC    bool          `json:"enable_gc"`
}

// Option customizes a Store at construction.
type Option func(*Store)

// VersionInfo describes one addressable object version.
type VersionInfo struct {
	Key       string
	VersionID string
	Size      int64
	Checksum  string
	Current   bool
	Deleted   bool
	CreatedAt time.Time
}

// PutOptions carries conditional-write preconditions.
type PutOptions struct {
	// IfNoneMatch != nil requests a "version must not currently exist" check.
	// *true rejects when a live current version exists (If-None-Match: *).
	IfNoneMatch *bool
	// IfMatch != nil requests the current version checksum to equal ETag
	// (use "*" to require that the object currently exists).
	IfMatch *string
}

// Store is the versioned object store.
type Store struct {
	cfg       Config
	logf      func(format string, args ...any)
	mu        sync.Mutex
	bs        *blocks.Store
	w         *wal.WAL
	st        *meta.State
	lock      *filelock.Lock
	stopGC    chan struct{}
	doneGC    chan struct{}
	closeOnce sync.Once
}

// New opens (or bootstraps) a store under the configured root directory,
// replaying metadata and recovering from any crash that occurred previously.
func New(cfg Config, opts ...Option) (*Store, error) {
	if cfg.RootDir == "" {
		return nil, errors.New("objectstore: empty RootDir")
	}
	if cfg.MinPartSize <= 0 {
		cfg.MinPartSize = defaultMinPartSize
	}
	if cfg.MaxParts <= 0 {
		cfg.MaxParts = defaultMaxParts
	}
	if cfg.GCUnrefAge <= 0 {
		cfg.GCUnrefAge = 30 * time.Second
	}
	if err := os.MkdirAll(cfg.RootDir, 0o755); err != nil {
		return nil, err
	}
	lk, err := filelock.Acquire(filepath.Join(cfg.RootDir, "lock"))
	if err != nil {
		if errors.Is(err, filelock.ErrLocked) {
			return nil, ErrStorageLocked
		}
		return nil, err
	}

	bs, err := blocks.Open(cfg.RootDir)
	if err != nil {
		_ = lk.Release()
		return nil, err
	}
	// Staged temp files never survive an opener: their metadata was not
	// committed (crash happened before the WAL append).
	if err := bs.CleanupTemp(); err != nil {
		_ = lk.Release()
		return nil, err
	}

	st := meta.NewState()
	walPath := filepath.Join(cfg.RootDir, "meta.wal")
	w, err := wal.Open(walPath, func(rec wal.Record) error {
		var mr meta.Record
		if err := jsonUnmarshal(rec.Data, &mr); err != nil {
			return err
		}
		return st.Apply(&mr)
	})
	if err != nil {
		_ = lk.Release()
		if errors.Is(err, wal.ErrCorrupted) || errors.Is(err, meta.ErrInconsistent) {
			return nil, fmt.Errorf("%w: %v", ErrCorruptedMetadata, err)
		}
		return nil, err
	}

	s := &Store{
		cfg:    cfg,
		logf:   defaultLogger,
		bs:     bs,
		w:      w,
		st:     st,
		lock:   lk,
		stopGC: make(chan struct{}),
		doneGC: make(chan struct{}),
	}
	for _, o := range opts {
		o(s)
	}
	// Options may have altered the root-level defaults; re-derive limits.
	if s.cfg.MinPartSize <= 0 {
		s.cfg.MinPartSize = defaultMinPartSize
	}
	if s.cfg.MaxParts <= 0 {
		s.cfg.MaxParts = defaultMaxParts
	}
	if s.cfg.GCUnrefAge <= 0 {
		s.cfg.GCUnrefAge = 30 * time.Second
	}

	// Recovery check: every block referenced by metadata must exist on disk.
	if err := s.verifyReferencedBlocksLocked(); err != nil {
		_ = w.Close()
		_ = lk.Release()
		return nil, err
	}
	s.logf("BOOT root=%s versions_keys=%d uploads_inprogress=%d recovery=ok",
		s.cfg.RootDir, len(st.Versions), len(st.Uploads))

	if s.cfg.EnableGC && s.cfg.GCInterval > 0 {
		go s.gcLoop()
	} else {
		close(s.doneGC)
	}
	return s, nil
}

// closeOnce makes Close idempotent.
//
// Close stops background workers and releases resources.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.stopGC)
	})
	<-s.doneGC
	err := s.w.Close()
	if lerr := s.lock.Release(); err == nil {
		err = lerr
	}
	return err
}

// verifyReferencedBlocksLocked asserts no metadata entry references a missing
// data block after replay. Runs while holding no reader open, at boot only.
func (s *Store) verifyReferencedBlocksLocked() error {
	check := func(csum string) error {
		if _, ok, err := s.bs.Stat(csum); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("%w: referenced block %s is absent", ErrCorruptedData, csum)
		}
		return nil
	}
	for key, byID := range s.st.Versions {
		for id, v := range byID {
			if s.st.IsDeleted(key, id) {
				continue
			}
			if len(v.Parts) > 0 {
				for _, p := range v.Parts {
					if err := check(p.Checksum); err != nil {
						return err
					}
				}
			} else if err := check(v.Checksum); err != nil {
				return err
			}
		}
	}
	for _, u := range s.st.Uploads {
		for _, p := range u.Parts {
			if err := check(p.Checksum); err != nil {
				return err
			}
		}
	}
	return nil
}

// Put writes a new version of key. The precondition in opt (if any) is
// evaluated atomically before the version is committed.
func (s *Store) Put(ctx context.Context, key string, r io.Reader, size int64, opt *PutOptions) (*VersionInfo, error) {
	if key == "" {
		return nil, errors.New("objectstore: empty key")
	}
	tmp, err := s.bs.CreateTemp(r, size)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		_ = s.bs.RemoveTemp(tmp)
		return nil, err
	}
	if err := s.checkPreconditionsLocked(key, opt); err != nil {
		_ = s.bs.RemoveTemp(tmp)
		return nil, err
	}

	csum, err := s.bs.Publish(tmp)
	if err != nil {
		return nil, err
	}
	v := &meta.Version{
		ID:        newID("v-"),
		Key:       key,
		Size:      tmp.Size(),
		Checksum:  csum,
		CreatedAt: time.Now().UTC(),
	}
	if err := s.appendLocked(meta.NewPutVersionRecord(v, "")); err != nil {
		return nil, err
	}
	s.logf("PUT key=%q version=%s checksum=%s size=%d basis=%s -> committed",
		key, v.ID, csum, v.Size, preconditionBasis(opt))
	return s.versionInfoLocked(v, true), nil
}

// checkPreconditionsLocked evaluates conditional-write preconditions against
// the current metadata state. Caller must hold s.mu.
func (s *Store) checkPreconditionsLocked(key string, opt *PutOptions) error {
	if opt == nil {
		return nil
	}
	cur := s.st.CurrentVersion(key)
	if opt.IfNoneMatch != nil && *opt.IfNoneMatch && cur != nil {
		s.logf("COND key=%q basis=if-none-match:* current=%s -> rejected: %s",
			key, cur.ID, ErrConditionConflict.Error())
		return ErrConditionConflict
	}
	if opt.IfMatch != nil {
		if cur == nil {
			s.logf("COND key=%q basis=if-match:%s current=absent -> rejected: %s",
				key, *opt.IfMatch, ErrPreconditionFailed.Error())
			return ErrPreconditionFailed
		}
		if *opt.IfMatch != "*" && cur.Checksum != *opt.IfMatch {
			s.logf("COND key=%q basis=if-match:%s current_checksum=%s -> rejected: %s",
				key, *opt.IfMatch, cur.Checksum, ErrPreconditionFailed.Error())
			return ErrPreconditionFailed
		}
	}
	return nil
}

func preconditionBasis(opt *PutOptions) string {
	if opt == nil {
		return "none"
	}
	parts := []string{}
	if opt.IfNoneMatch != nil {
		parts = append(parts, fmt.Sprintf("if-none-match=*:%v", *opt.IfNoneMatch))
	}
	if opt.IfMatch != nil {
		parts = append(parts, "if-match="+*opt.IfMatch)
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ",")
}

// Get reads the current version, or a specific version when versionID is set.
func (s *Store) Get(ctx context.Context, key, versionID string) (io.ReadCloser, *VersionInfo, error) {
	s.mu.Lock()
	v, info, err := s.resolveForReadLocked(key, versionID)
	if err != nil {
		s.mu.Unlock()
		return nil, nil, err
	}
	// Open all block handles while holding the lock + state snapshot. GC never
	// removes a referenced block, so these opens cannot race a deletion.
	var readers []io.ReadCloser
	openOne := func(csum string) error {
		rc, err := s.bs.Open(csum)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCorruptedData, err)
		}
		readers = append(readers, rc)
		return nil
	}
	if len(v.Parts) > 0 {
		for _, p := range v.Parts {
			if err := openOne(p.Checksum); err != nil {
				s.mu.Unlock()
				closeAll(readers)
				return nil, nil, err
			}
		}
	} else {
		if err := openOne(v.Checksum); err != nil {
			s.mu.Unlock()
			closeAll(readers)
			return nil, nil, err
		}
	}
	s.mu.Unlock()

	s.logf("GET key=%q version=%s parts=%d -> opened", key, info.VersionID, len(readers))
	return &multiReadCloser{readers: readers}, info, nil
}

// resolveForReadLocked finds the requested version and enforces visibility.
func (s *Store) resolveForReadLocked(key, versionID string) (*meta.Version, *VersionInfo, error) {
	if versionID != "" {
		byKey := s.st.Versions[key]
		v := byKey[versionID]
		if v == nil {
			return nil, nil, fmt.Errorf("%w: %q/%s", ErrVersionNotFound, key, versionID)
		}
		if s.st.IsDeleted(key, versionID) {
			return nil, nil, fmt.Errorf("%w: %q/%s", ErrVersionDeleted, key, versionID)
		}
		return v, s.versionInfoLocked(v, s.st.Current[key] == versionID), nil
	}
	v := s.st.CurrentVersion(key)
	if v == nil {
		if byKey, ever := s.st.Versions[key]; ever && len(byKey) > 0 {
			return nil, nil, fmt.Errorf("%w: %q (delete marker)", ErrNotFound, key)
		}
		return nil, nil, fmt.Errorf("%w: %q", ErrNotFound, key)
	}
	return v, s.versionInfoLocked(v, true), nil
}

// Delete marks the current version of key as deleted (a delete marker);
// historical versions remain addressable by version ID.
func (s *Store) Delete(ctx context.Context, key string) (*VersionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.st.CurrentVersion(key)
	if cur == nil {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, key)
	}
	now := time.Now().UTC()
	if err := s.appendLocked(meta.NewDeleteMarkerRecord(key, now)); err != nil {
		return nil, err
	}
	s.logf("DELETE key=%q current=%s basis=explicit_delete -> marker_installed (history retained)",
		key, cur.ID)
	return &VersionInfo{
		Key:       key,
		VersionID: cur.ID,
		Size:      cur.Size,
		Checksum:  cur.Checksum,
		Current:   false,
		Deleted:   false,
		CreatedAt: cur.CreatedAt,
	}, nil
}

// DeleteVersion permanently removes one specific historical version; if it was
// the current version, a delete marker is installed.
func (s *Store) DeleteVersion(ctx context.Context, key, versionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	byKey := s.st.Versions[key]
	v := byKey[versionID]
	if v == nil {
		return fmt.Errorf("%w: %q/%s", ErrVersionNotFound, key, versionID)
	}
	if s.st.IsDeleted(key, versionID) {
		return fmt.Errorf("%w: %q/%s", ErrVersionDeleted, key, versionID)
	}
	if err := s.appendLocked(meta.NewDeleteVersionRecord(key, versionID)); err != nil {
		return err
	}
	s.logf("DELETE_VERSION key=%q version=%s blocks=%d -> removed (blocks gc candidates)",
		key, versionID, blockCount(v))
	return nil
}

// ListVersions enumerates versions of key (newest first), including deleted ones.
func (s *Store) ListVersions(ctx context.Context, key string) ([]VersionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byKey := s.st.Versions[key]
	if len(byKey) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, key)
	}
	curID := s.st.Current[key]
	out := make([]VersionInfo, 0, len(byKey))
	for id, v := range byKey {
		info := s.versionInfoLocked(v, id == curID)
		out = append(out, *info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

// RunGC performs one synchronous garbage-collection sweep: data blocks not
// referenced by any live version or in-progress upload, and older than the
// configured grace age, are deleted. Visible objects are never touched.
func (s *Store) RunGC(ctx context.Context) (int, error) {
	s.mu.Lock()
	referenced := s.referencedBlocksLocked()
	s.mu.Unlock()

	onDisk, err := s.bs.List()
	if err != nil {
		return 0, err
	}
	cutoff := time.Now().Add(-s.cfg.GCUnrefAge)
	removed := 0
	for _, csum := range onDisk {
		if referenced[csum] {
			continue
		}
		fi, err := os.Stat(filepath.Join(s.cfg.RootDir, "data", csum))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return removed, err
		}
		if fi.ModTime().After(cutoff) {
			continue // grace period: survive one more sweep
		}
		// Re-check under the lock: a writer may have referenced it meanwhile.
		s.mu.Lock()
		if s.referencedBlocksLocked()[csum] {
			s.mu.Unlock()
			continue
		}
		err = s.bs.Remove(csum)
		s.mu.Unlock()
		if err != nil {
			return removed, err
		}
		removed++
		s.logf("GC block=%s basis=unreferenced older_than=%s -> removed", csum, s.cfg.GCUnrefAge)
	}
	if removed > 0 {
		s.logf("GC sweep blocks_on_disk=%d referenced=%d removed=%d", len(onDisk), len(referenced), removed)
	}
	return removed, nil
}

func (s *Store) gcLoop() {
	defer close(s.doneGC)
	d := s.cfg.GCInterval
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-s.stopGC:
			return
		case <-t.C:
			if _, err := s.RunGC(context.Background()); err != nil {
				s.logf("GC error=%v", err)
			}
		}
	}
}

// referencedBlocksLocked builds the set of every block reachable from live
// metadata: live (non-deleted) versions and parts of in-progress uploads.
func (s *Store) referencedBlocksLocked() map[string]bool {
	ref := map[string]bool{}
	for key, byID := range s.st.Versions {
		for id, v := range byID {
			if s.st.IsDeleted(key, id) {
				continue
			}
			if len(v.Parts) > 0 {
				for _, p := range v.Parts {
					ref[p.Checksum] = true
				}
			} else {
				ref[v.Checksum] = true
			}
		}
	}
	for _, u := range s.st.Uploads {
		for _, p := range u.Parts {
			ref[p.Checksum] = true
		}
	}
	return ref
}

// appendLocked serializes one state transition: state.Apply first (guards the
// WAL against inconsistent records), then durable WAL append.
func (s *Store) appendLocked(r *meta.Record) error {
	if err := s.st.Apply(r); err != nil {
		return fmt.Errorf("%w: %v", ErrCorruptedMetadata, err)
	}
	data, err := jsonMarshal(r)
	if err != nil {
		return err
	}
	if err := s.w.Append(wal.Record{Type: r.Kind, Data: data}); err != nil {
		return err
	}
	return nil
}

func (s *Store) versionInfoLocked(v *meta.Version, current bool) *VersionInfo {
	return &VersionInfo{
		Key:       v.Key,
		VersionID: v.ID,
		Size:      v.Size,
		Checksum:  v.Checksum,
		Current:   current,
		Deleted:   s.st.IsDeleted(v.Key, v.ID),
		CreatedAt: v.CreatedAt,
	}
}

func blockCount(v *meta.Version) int {
	if len(v.Parts) > 0 {
		return len(v.Parts)
	}
	if v.Checksum == "" {
		return 0
	}
	return 1
}

func closeAll(rcs []io.ReadCloser) {
	for _, rc := range rcs {
		_ = rc.Close()
	}
}

// multiReadCloser concatenates several block readers into one object stream.
type multiReadCloser struct {
	readers []io.ReadCloser
}

func (m *multiReadCloser) Read(p []byte) (int, error) {
	for len(m.readers) > 0 {
		n, err := m.readers[0].Read(p)
		if n > 0 {
			return n, nil
		}
		if err == io.EOF {
			_ = m.readers[0].Close()
			m.readers = m.readers[1:]
			continue
		}
		return n, err
	}
	return 0, io.EOF
}

func (m *multiReadCloser) Close() error {
	var first error
	for _, rc := range m.readers {
		if err := rc.Close(); err != nil && first == nil {
			first = err
		}
	}
	m.readers = nil
	return first
}

func newID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b[:])
}

func defaultLogger(format string, args ...any) {
	fmt.Fprintf(os.Stdout, "%s "+format+"\n", append([]any{time.Now().UTC().Format("15:04:05.000")}, args...)...)
}
