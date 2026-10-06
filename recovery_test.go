package objectstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func reopen(t *testing.T, dir string) *Store {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	s, err := Open(Config{Dir: dir, Logger: log})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// 重启后已提交的版本与分片上传结果保持可见且内容不变。
func TestRestartPreservesCommittedState(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	v1 := putString(t, s, "a", "one", PutOptions{})
	v2 := putString(t, s, "a", "two", PutOptions{})
	id, _ := s.CreateUpload("b")
	s.UploadPart(id, 1, bytes.NewReader([]byte("x")))
	s.UploadPart(id, 2, bytes.NewReader([]byte("y")))
	if _, err := s.CompleteUpload(id, []int{1, 2}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	s.Close()

	s = reopen(t, dir)
	for vid, want := range map[string]string{v1.VersionID: "one", v2.VersionID: "two"} {
		r, _, err := s.GetVersion("a", vid)
		if err != nil {
			t.Fatalf("get version %s after restart: %v", vid, err)
		}
		if got := getString(t, r); got != want {
			t.Fatalf("version %s = %q, want %q", vid, got, want)
		}
	}
	r, _, err := s.Get("b")
	if err != nil {
		t.Fatalf("get b after restart: %v", err)
	}
	if got := getString(t, r); got != "xy" {
		t.Fatalf("b = %q, want %q", got, "xy")
	}
}

// 崩溃残留（无引用的数据文件、临时元数据、未知上传目录、未登记分片）
// 必须在重启恢复时被清除，且不影响已提交数据。
func TestRecoveryRemovesCrashResidue(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	v1 := putString(t, s, "keep", "good", PutOptions{})
	// 进行中的上传：一个已登记分片。
	id, _ := s.CreateUpload("inflight")
	if _, err := s.UploadPart(id, 1, bytes.NewReader([]byte("p1"))); err != nil {
		t.Fatalf("part: %v", err)
	}
	s.Close()

	// 制造崩溃残留。
	orphan := filepath.Join(dir, "data", "deadbeef", "v00000099")
	os.MkdirAll(filepath.Dir(orphan), 0o755)
	os.WriteFile(orphan, []byte("orphan"), 0o644)
	os.WriteFile(filepath.Join(dir, "meta.json.tmp"), []byte("garbage"), 0o644)
	os.MkdirAll(filepath.Join(dir, "uploads", "u99999999"), 0o755)
	os.WriteFile(filepath.Join(dir, "uploads", "u99999999", "part-1"), []byte("x"), 0o644)
	// initiated 会话里未登记的分片文件。
	os.WriteFile(filepath.Join(dir, "uploads", id, "part-7"), []byte("stale"), 0o644)
	os.WriteFile(filepath.Join(dir, "uploads", id, "part-2.tmp"), []byte("half"), 0o644)

	s = reopen(t, dir)
	// 残留被清除。
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan data file should be removed, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "uploads", "u99999999")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unknown upload dir should be removed, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "uploads", id, "part-7")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unregistered part should be removed, stat err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "uploads", id, "part-2.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp part should be removed, stat err = %v", err)
	}
	// 已提交数据不受影响。
	r, _, err := s.GetVersion("keep", v1.VersionID)
	if err != nil {
		t.Fatalf("get committed version: %v", err)
	}
	if got := getString(t, r); got != "good" {
		t.Fatalf("content = %q, want %q", got, "good")
	}
	// 进行中的上传可跨重启继续完成。
	if _, err := s.UploadPart(id, 2, bytes.NewReader([]byte("p2"))); err != nil {
		t.Fatalf("resume part: %v", err)
	}
	if _, err := s.CompleteUpload(id, []int{1, 2}); err != nil {
		t.Fatalf("complete after restart: %v", err)
	}
	r, _, err = s.Get("inflight")
	if err != nil {
		t.Fatalf("get inflight: %v", err)
	}
	if got := getString(t, r); got != "p1p2" {
		t.Fatalf("content = %q, want %q", got, "p1p2")
	}
}

// 元数据引用了缺失数据文件时属于真正的数据损坏：Open 必须失败并给出
// 可区分的 *CorruptionError，指出是哪个键、哪个版本；不得静默抹掉该版本，
// 且反复打开结论一致、磁盘现场不被改动。
func TestRecoveryFailsOnVersionWithMissingData(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	v1 := putString(t, s, "k", "v1", PutOptions{})
	v2 := putString(t, s, "k", "v2", PutOptions{})
	s.Close()
	// 模拟数据文件丢失。
	matches, _ := filepath.Glob(filepath.Join(dir, "data", "*", v1.VersionID))
	if len(matches) != 1 {
		t.Fatalf("cannot locate data file for %s", v1.VersionID)
	}
	if err := os.Remove(matches[0]); err != nil {
		t.Fatalf("remove data file: %v", err)
	}
	// 反复打开：每次都必须以相同的损坏结论失败。
	for attempt := 1; attempt <= 3; attempt++ {
		_, err := Open(Config{Dir: dir, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))})
		var ce *CorruptionError
		if !errors.As(err, &ce) {
			t.Fatalf("attempt %d: open err = %v, want *CorruptionError", attempt, err)
		}
		if !errors.Is(err, ErrCorruption) {
			t.Fatalf("attempt %d: err should match ErrCorruption", attempt)
		}
		if len(ce.Versions) != 1 || ce.Versions[0].Key != "k" || ce.Versions[0].VersionID != v1.VersionID {
			t.Fatalf("attempt %d: corrupt versions = %+v, want k/%s", attempt, ce.Versions, v1.VersionID)
		}
		t.Logf("attempt %d: corruption reported as: %v", attempt, err)
	}
	// 现场未被改动：丢失版本的记录仍在，未损坏版本的数据仍在。
	if _, err := os.Stat(filepath.Join(dir, "meta.json")); err != nil {
		t.Fatalf("meta.json should be untouched: %v", err)
	}
	matches, _ = filepath.Glob(filepath.Join(dir, "data", "*", v2.VersionID))
	if len(matches) != 1 {
		t.Fatalf("healthy version %s data should be untouched", v2.VersionID)
	}
}

// GC 回收无引用文件；被读取中的版本必须能完成读取，可见对象不受影响。
func TestGCReclaimsOrphansAndKeepsVisible(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	v1 := putString(t, s, "g", "live", PutOptions{})
	// 制造无引用残留。
	orphan := filepath.Join(dir, "data", "cafe", "v00000077")
	os.MkdirAll(filepath.Dir(orphan), 0o755)
	os.WriteFile(orphan, []byte("junk"), 0o644)
	os.MkdirAll(filepath.Join(dir, "uploads", "u0000042"), 0o755)
	os.WriteFile(filepath.Join(dir, "uploads", "u0000042", "part-1"), []byte("junk"), 0o644)

	// 读取进行中发起 GC：读取必须完成且内容正确。
	rc, info, err := s.Get("g")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if info.VersionID != v1.VersionID {
		t.Fatalf("version = %s, want %s", info.VersionID, v1.VersionID)
	}
	rep, err := s.GC()
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if rep.RemovedDataFiles != 1 || rep.RemovedUploadDirs != 1 || rep.ReclaimedBytes == 0 {
		t.Fatalf("report = %+v, want 1 data file + 1 upload dir reclaimed", rep)
	}
	if got := getString(t, rc); got != "live" {
		t.Fatalf("in-flight read = %q, want %q", got, "live")
	}
	// 可见版本仍可读取。
	r, _, err := s.GetVersion("g", v1.VersionID)
	if err != nil {
		t.Fatalf("get after gc: %v", err)
	}
	if got := getString(t, r); got != "live" {
		t.Fatalf("content = %q, want %q", got, "live")
	}
	// 再次 GC 无可回收项。
	rep, err = s.GC()
	if err != nil {
		t.Fatalf("gc2: %v", err)
	}
	if rep.RemovedDataFiles != 0 || rep.RemovedUploadDirs != 0 {
		t.Fatalf("second gc report = %+v, want empty", rep)
	}
}

// 并发读写与 GC 交错时数据状态自洽。
func TestConcurrentReadWriteWithGC(t *testing.T) {
	s := openTestStore(t)
	const n = 8
	done := make(chan struct{})
	errs := make(chan error, 3*n)
	// 写者。
	go func() {
		for i := 0; i < n; i++ {
			if _, err := s.Put("rw", bytes.NewReader([]byte(fmt.Sprintf("gen-%d", i))), PutOptions{}); err != nil {
				errs <- fmt.Errorf("put: %w", err)
			}
		}
		done <- struct{}{}
	}()
	// 读者。
	go func() {
		for i := 0; i < n; i++ {
			rc, _, err := s.Get("rw")
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				errs <- fmt.Errorf("get: %w", err)
				continue
			}
			b, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				errs <- fmt.Errorf("read: %w", err)
				continue
			}
			if len(b) == 0 {
				errs <- fmt.Errorf("empty body read")
			}
		}
		done <- struct{}{}
	}()
	// 回收者。
	go func() {
		for i := 0; i < n; i++ {
			if _, err := s.GC(); err != nil {
				errs <- fmt.Errorf("gc: %w", err)
			}
		}
		done <- struct{}{}
	}()
	<-done
	<-done
	<-done
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent error: %v", err)
	}
	// 最终状态自洽：当前版本可读。
	r, _, err := s.Get("rw")
	if err != nil {
		t.Fatalf("final get: %v", err)
	}
	if got := getString(t, r); got != fmt.Sprintf("gen-%d", n-1) {
		t.Fatalf("final content = %q, want gen-%d", got, n-1)
	}
}
