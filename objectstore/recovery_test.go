package objectstore

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func reopenStore(t *testing.T, dir string) *Store {
	t.Helper()
	cfg := DefaultConfig(dir)
	cfg.MinPartSize = 4
	cfg.MaxParts = 10
	cfg.GCInterval = 0
	cfg.GCUnrefAge = time.Millisecond
	s, err := New(cfg, WithLogger(func(string, ...any) {}))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestRestartPreservesVersionsAndHistory(t *testing.T) {
	dir := t.TempDir()
	s := openRecoveryStore(t, dir)
	ctx := context.Background()
	v1 := putBytes(t, s, "a", "first", nil)
	v2 := putBytes(t, s, "a", "second", nil)
	up, _ := s.CreateMultipart(ctx, "m")
	if _, err := s.UploadPart(ctx, up, 1, strings.NewReader("xxxx"), 4); err != nil {
		t.Fatal(err)
	}
	putBytes(t, s, "del", "gone", nil)
	if _, err := s.Delete(ctx, "del"); err != nil {
		t.Fatal(err)
	}
	// In-progress multipart upload survives restart and can continue.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := reopenStore(t, dir)
	rc, info, err := s2.Get(ctx, "a", "")
	if err != nil {
		t.Fatalf("current after restart: %v", err)
	}
	if body := readAll(t, rc); body != "second" || info.VersionID != v2.VersionID {
		t.Fatalf("current after restart body/version mismatch: %q %s", body, info.VersionID)
	}
	rc, _, err = s2.Get(ctx, "a", v1.VersionID)
	if err != nil {
		t.Fatalf("history after restart: %v", err)
	}
	if body := readAll(t, rc); body != "first" {
		t.Fatalf("history after restart: %q", body)
	}
	if _, _, err := s2.Get(ctx, "del", ""); err == nil {
		t.Fatal("delete marker must survive restart")
	}
	// Continue and complete the upload that was in flight at shutdown.
	if _, err := s2.UploadPart(ctx, up, 2, strings.NewReader("yy"), 2); err != nil {
		t.Fatalf("continue upload after restart: %v", err)
	}
	if _, err := s2.CompleteMultipart(ctx, up, []CompletedPart{{1}, {2}}); err != nil {
		t.Fatalf("complete after restart: %v", err)
	}
	rc, _, err = s2.Get(ctx, "m", "")
	if err != nil {
		t.Fatal(err)
	}
	if body := readAll(t, rc); body != "xxxxyy" {
		t.Fatalf("multipart after restart: %q", body)
	}
}

func TestRestartRejectsDoubleProcess(t *testing.T) {
	dir := t.TempDir()
	s1 := openRecoveryStore(t, dir)
	defer s1.Close()
	cfg := DefaultConfig(dir)
	_, err := New(cfg, WithGCInterval(0), WithLogger(func(string, ...any) {}))
	if err == nil {
		t.Fatal("second opener of same root must be rejected")
	}
	if !errorIs(err, ErrStorageLocked) {
		t.Fatalf("err=%v, want ErrStorageLocked", err)
	}
}

func TestGCReclaimsOnlyUnreferenced(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	putBytes(t, s, "keep", "hello-data", nil)
	gone := putBytes(t, s, "gone", "bye-data-1234", nil)
	if err := s.DeleteVersion(ctx, "gone", gone.VersionID); err != nil {
		t.Fatal(err)
	}
	// Delete marker alone keeps nothing referencing the block.
	removed, err := s.RunGC(ctx)
	if err != nil || removed != 1 {
		t.Fatalf("removed=%d err=%v, want 1", removed, err)
	}
	// A second sweep removes nothing (idempotent).
	removed2, _ := s.RunGC(ctx)
	if removed2 != 0 {
		t.Fatalf("second sweep removed %d", removed2)
	}
	// Visible object intact after GC.
	rc, _, err := s.Get(ctx, "keep", "")
	if err != nil {
		t.Fatal(err)
	}
	if body := readAll(t, rc); body != "hello-data" {
		t.Fatalf("visible object harmed by GC: %q", body)
	}
}

func TestTornWALTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	s := openRecoveryStore(t, dir)
	v := putBytes(t, s, "a", "durable", nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a torn append: garbage bytes appended past the last good frame.
	walPath := walPathOf(dir)
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x00, 0x00, 0x01, 0x00, 0xff, 0xff, 0xab, 0xcd, 'x', 'y'}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	s2 := reopenStore(t, dir)
	rc, _, err := s2.Get(context.Background(), "a", v.VersionID)
	if err != nil {
		t.Fatalf("committed version must survive torn tail: %v", err)
	}
	if body := readAll(t, rc); body != "durable" {
		t.Fatalf("body=%q", body)
	}
	// Appending after recovery works (tail was truncated).
	putBytes(t, s2, "b", "more", nil)
}

func TestOrphanBlocksAreCollectedAfterRestart(t *testing.T) {
	dir := t.TempDir()
	s := openRecoveryStore(t, dir)
	putBytes(t, s, "a", "persisted-data", nil)
	up, _ := s.CreateMultipart(context.Background(), "z")
	pr, err := s.UploadPart(context.Background(), up, 1, strings.NewReader("orphan-part"), 11)
	if err != nil {
		t.Fatal(err)
	}
	_ = pr
	if err := s.AbortMultipart(context.Background(), up); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := reopenStore(t, dir)
	removed, err := s2.RunGC(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed=%d, want aborted part block only", removed)
	}
	rc, _, err := s2.Get(context.Background(), "a", "")
	if err != nil {
		t.Fatal(err)
	}
	if body := readAll(t, rc); body != "persisted-data" {
		t.Fatalf("body=%q", body)
	}
}

func TestMissingReferencedBlockDetectedAtBoot(t *testing.T) {
	dir := t.TempDir()
	s := openRecoveryStore(t, dir)
	v := putBytes(t, s, "a", "important-body-here", nil)
	rc, info, err := s.Get(context.Background(), "a", "")
	if err != nil {
		t.Fatal(err)
	}
	_ = readAll(t, rc)
	_ = info
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Sabotage: remove the live object's data block from under the metadata.
	if err := os.RemoveAll(dataPathOf(dir, v.Checksum)); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(dir)
	cfg.MinPartSize = 4
	cfg.GCInterval = 0
	_, err = New(cfg, WithLogger(func(string, ...any) {}))
	if !errorIs(err, ErrCorruptedData) {
		t.Fatalf("boot err=%v, want ErrCorruptedData", err)
	}
}
