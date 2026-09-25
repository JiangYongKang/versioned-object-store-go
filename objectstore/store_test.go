package objectstore

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "root")
	s, err := New(DefaultConfig(dir),
		WithMinPartSize(4),
		WithMaxParts(10),
		WithGCInterval(0),
		WithGCUnrefAge(time.Millisecond),
		WithLogger(func(string, ...any) {}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func putBytes(t *testing.T, s *Store, key, body string, opt *PutOptions) *VersionInfo {
	t.Helper()
	info, err := s.Put(context.Background(), key, strings.NewReader(body), int64(len(body)), opt)
	if err != nil {
		t.Fatalf("Put %q: %v", key, err)
	}
	return info
}

func readAll(t *testing.T, rc io.ReadCloser) string {
	t.Helper()
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(b)
}

func TestPutGetVersioning(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	v1 := putBytes(t, s, "a", "hello", nil)
	v2 := putBytes(t, s, "a", "hello-world", nil)
	if v1.VersionID == v2.VersionID {
		t.Fatalf("version ids must differ: %s", v1.VersionID)
	}

	rc, info, err := s.Get(ctx, "a", "")
	if err != nil {
		t.Fatalf("Get current: %v", err)
	}
	if got := readAll(t, rc); got != "hello-world" {
		t.Fatalf("current = %q", got)
	}
	if info.VersionID != v2.VersionID || !info.Current {
		t.Fatalf("current info mismatch: %+v", info)
	}

	// Overwritten historical version stays addressable and immutable.
	rc, _, err = s.Get(ctx, "a", v1.VersionID)
	if err != nil {
		t.Fatalf("Get v1: %v", err)
	}
	if got := readAll(t, rc); got != "hello" {
		t.Fatalf("v1 = %q, want hello", got)
	}

	versions, err := s.ListVersions(ctx, "a")
	if err != nil || len(versions) != 2 {
		t.Fatalf("ListVersions: %v %+v", err, versions)
	}
	if !versions[0].Current || versions[1].Current {
		t.Fatalf("exactly one current version expected: %+v", versions)
	}

	// Delete installs a marker; history remains readable.
	if _, err := s.Delete(ctx, "a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, err := s.Get(ctx, "a", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete err=%v, want ErrNotFound", err)
	}
	rc, _, err = s.Get(ctx, "a", v1.VersionID)
	if err != nil {
		t.Fatalf("history after delete: %v", err)
	}
	if got := readAll(t, rc); got != "hello" {
		t.Fatalf("history changed after delete: %q", got)
	}

	// DeleteVersion removes one history entry; others survive.
	if err := s.DeleteVersion(ctx, "a", v1.VersionID); err != nil {
		t.Fatalf("DeleteVersion: %v", err)
	}
	if _, _, err := s.Get(ctx, "a", v1.VersionID); !errors.Is(err, ErrVersionDeleted) {
		t.Fatalf("deleted version read err=%v, want ErrVersionDeleted", err)
	}
	rc, _, err = s.Get(ctx, "a", v2.VersionID)
	if err != nil {
		t.Fatalf("other version must survive: %v", err)
	}
	if got := readAll(t, rc); got != "hello-world" {
		t.Fatalf("v2 = %q", got)
	}
}

func TestConditionalWrites(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	ifNone := true
	v1 := putBytes(t, s, "k", "one", &PutOptions{IfNoneMatch: &ifNone})

	// Existing object: If-None-Match:* must conflict.
	_, err := s.Put(ctx, "k", strings.NewReader("two"), 3, &PutOptions{IfNoneMatch: &ifNone})
	if !errors.Is(err, ErrConditionConflict) {
		t.Fatalf("IfNoneMatch err=%v, want ErrConditionConflict", err)
	}
	if CodeOf(err) != "condition_conflict" {
		t.Fatalf("code=%q", CodeOf(err))
	}

	// Wrong etag -> precondition failed; right etag -> accepted.
	bad := "deadbeef"
	if _, err := s.Put(ctx, "k", strings.NewReader("x"), 1, &PutOptions{IfMatch: &bad}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("IfMatch bad err=%v, want ErrPreconditionFailed", err)
	}
	good := v1.Checksum
	putBytes(t, s, "k", "two", &PutOptions{IfMatch: &good})

	// If-Match on absent key -> precondition failed.
	star := "*"
	if _, err := s.Put(ctx, "absent", strings.NewReader("x"), 1, &PutOptions{IfMatch: &star}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("IfMatch on absent err=%v", err)
	}
	// If-None-Match on absent key -> accepted.
	putBytes(t, s, "absent", "x", &PutOptions{IfNoneMatch: &ifNone})
	// Delete marker makes If-None-Match succeed again.
	if _, err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	putBytes(t, s, "k", "three", &PutOptions{IfNoneMatch: &ifNone})
}

func TestContentDedup(t *testing.T) {
	s := testStore(t)
	v1 := putBytes(t, s, "k1", strings.Repeat("x", 20), nil)
	v2 := putBytes(t, s, "k2", strings.Repeat("x", 20), nil)
	if v1.Checksum != v2.Checksum {
		t.Fatal("identical content should share one block")
	}
	rc, _, err := s.Get(context.Background(), "k2", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, rc); len(got) != 20 {
		t.Fatalf("len=%d", len(got))
	}
}
