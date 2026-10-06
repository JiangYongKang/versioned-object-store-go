package objectstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"
)

func dataFileExists(t *testing.T, dir, versionID string) bool {
	t.Helper()
	matches, _ := filepath.Glob(filepath.Join(dir, "data", "*", versionID))
	return len(matches) == 1
}

// 按保留窗口回收单个键：超出窗口的旧版本数据被回收，
// 当前版本与窗口内历史版本不受影响，报告字节数与磁盘占用变化一致。
func TestRetainVersionsReclaimsOldVersions(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	var ids []string
	for i := 0; i < 5; i++ {
		vi := putString(t, s, "k", fmt.Sprintf("gen-%d", i), PutOptions{})
		ids = append(ids, vi.VersionID)
	}
	before := dirSize(filepath.Join(dir, "data"))

	rep, err := s.RetainVersions("k", 2)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if len(rep.Removed) != 3 || len(rep.Deferred) != 0 {
		t.Fatalf("report = %+v, want 3 removed 0 deferred", rep)
	}
	after := dirSize(filepath.Join(dir, "data"))
	if before-after != rep.ReclaimedBytes {
		t.Fatalf("reclaimed = %d, actual disk delta = %d", rep.ReclaimedBytes, before-after)
	}
	t.Logf("reclaimed %d bytes across %d versions", rep.ReclaimedBytes, len(rep.Removed))

	// 窗口外版本：元数据与数据都被移除，不再可寻址。
	for _, vid := range ids[:3] {
		if dataFileExists(t, dir, vid) {
			t.Fatalf("data file for reclaimed version %s still exists", vid)
		}
		if _, _, err := s.GetVersion("k", vid); !errors.Is(err, ErrNotFound) {
			t.Fatalf("get reclaimed version %s err = %v, want ErrNotFound", vid, err)
		}
	}
	// 窗口内版本与当前版本完好。
	for i, vid := range ids[3:] {
		r, _, err := s.GetVersion("k", vid)
		if err != nil {
			t.Fatalf("get retained version %s: %v", vid, err)
		}
		if got := getString(t, r); got != fmt.Sprintf("gen-%d", i+3) {
			t.Fatalf("version %s = %q, want gen-%d", vid, got, i+3)
		}
	}
	vs, err := s.ListVersions("k")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(vs) != 2 {
		t.Fatalf("versions = %d, want 2", len(vs))
	}
	// 回收结果可重启后保持：窗口外版本不会复活。
	s.Close()
	s = reopen(t, dir)
	vs, err = s.ListVersions("k")
	if err != nil {
		t.Fatalf("list after restart: %v", err)
	}
	if len(vs) != 2 {
		t.Fatalf("versions after restart = %d, want 2", len(vs))
	}
}

// 删除标记语义不受回收影响：当前删除标记保留，Get 仍返回 ErrDeleted；
// 窗口内的历史版本可读。
func TestRetainKeepsDeleteMarkerSemantics(t *testing.T) {
	s := openTestStore(t)
	v1 := putString(t, s, "k", "v1", PutOptions{})
	v2 := putString(t, s, "k", "v2", PutOptions{})
	del, err := s.Delete("k")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	// 保留最近 2 个版本（v2 与删除标记），v1 被回收。
	rep, err := s.RetainVersions("k", 2)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if len(rep.Removed) != 1 || rep.Removed[0].VersionID != v1.VersionID {
		t.Fatalf("removed = %+v, want only %s", rep.Removed, v1.VersionID)
	}
	// 当前版本仍是删除标记。
	if _, _, err := s.Get("k"); !errors.Is(err, ErrDeleted) {
		t.Fatalf("get err = %v, want ErrDeleted", err)
	}
	// 窗口内历史版本可读。
	r, _, err := s.GetVersion("k", v2.VersionID)
	if err != nil {
		t.Fatalf("get v2: %v", err)
	}
	if got := getString(t, r); got != "v2" {
		t.Fatalf("v2 = %q, want v2", got)
	}
	// 删除标记本身仍可寻址。
	_, info, err := s.GetVersion("k", del.VersionID)
	if err != nil || !info.Deleted {
		t.Fatalf("delete marker info = %+v err = %v, want addressable tombstone", info, err)
	}
	// 删除后 IfAbsent 语义不变。
	if _, err := s.Put("k", bytes.NewReader([]byte("reborn")), PutOptions{IfAbsent: true}); err != nil {
		t.Fatalf("put after delete with IfAbsent: %v", err)
	}
}

// RetainAll 对整个存储生效，每个键独立应用保留窗口。
func TestRetainAllAppliesPerKey(t *testing.T) {
	s := openTestStore(t)
	for _, key := range []string{"a", "b"} {
		for i := 0; i < 4; i++ {
			putString(t, s, key, fmt.Sprintf("%s-%d", key, i), PutOptions{})
		}
	}
	rep, err := s.RetainAll(1)
	if err != nil {
		t.Fatalf("retain all: %v", err)
	}
	if len(rep.Removed) != 6 || rep.ReclaimedBytes == 0 {
		t.Fatalf("report = %+v, want 6 removed", rep)
	}
	for _, key := range []string{"a", "b"} {
		vs, err := s.ListVersions(key)
		if err != nil {
			t.Fatalf("list %s: %v", key, err)
		}
		if len(vs) != 1 {
			t.Fatalf("key %s versions = %d, want 1", key, len(vs))
		}
		r, _, err := s.Get(key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if got := getString(t, r); got != key+"-3" {
			t.Fatalf("key %s current = %q, want %s-3", key, got, key)
		}
	}
	// 再次回收无可回收项。
	rep, err = s.RetainAll(1)
	if err != nil {
		t.Fatalf("retain all again: %v", err)
	}
	if len(rep.Removed) != 0 || rep.ReclaimedBytes != 0 {
		t.Fatalf("second report = %+v, want empty", rep)
	}
}

// 正在被读取的版本数据不得被回收：本次暂缓并记入 Deferred，
// 读取完成后再次回收才真正删除。
func TestRetainDefersVersionWithActiveReader(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	v1 := putString(t, s, "k", "old-content", PutOptions{})
	putString(t, s, "k", "new-content", PutOptions{})

	// 打开旧版本的读取句柄后发起回收。
	rc, _, err := s.GetVersion("k", v1.VersionID)
	if err != nil {
		t.Fatalf("get v1: %v", err)
	}
	rep, err := s.RetainVersions("k", 1)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if len(rep.Removed) != 0 || len(rep.Deferred) != 1 || rep.Deferred[0].VersionID != v1.VersionID {
		t.Fatalf("report = %+v, want v1 deferred", rep)
	}
	// 读取中的版本内容完整。
	if got := getString(t, rc); got != "old-content" {
		t.Fatalf("in-flight read = %q, want old-content", got)
	}
	// 数据文件与元数据都还在。
	if !dataFileExists(t, dir, v1.VersionID) {
		t.Fatal("deferred version data must remain while read is open")
	}
	rc2, _, err := s.GetVersion("k", v1.VersionID)
	if err != nil {
		t.Fatalf("deferred version must remain addressable: %v", err)
	}
	rc2.Close()
	// 读取结束后再回收：版本被真正移除。
	rep, err = s.RetainVersions("k", 1)
	if err != nil {
		t.Fatalf("retain again: %v", err)
	}
	if len(rep.Removed) != 1 || rep.Removed[0].VersionID != v1.VersionID {
		t.Fatalf("report = %+v, want v1 removed", rep)
	}
	if dataFileExists(t, dir, v1.VersionID) {
		t.Fatal("reclaimed version data must be gone")
	}
}

// 回收与并发读写交错进行时结果自洽：不删在读数据、当前版本始终可读。
func TestConcurrentReadWriteWithRetention(t *testing.T) {
	s := openTestStore(t)
	const n = 16
	done := make(chan struct{})
	errs := make(chan error, 4*n)
	go func() { // 写者
		for i := 0; i < n; i++ {
			if _, err := s.Put("rw", bytes.NewReader([]byte(fmt.Sprintf("gen-%d", i))), PutOptions{}); err != nil {
				errs <- fmt.Errorf("put: %w", err)
			}
		}
		done <- struct{}{}
	}()
	go func() { // 读者：读到的内容必须完整且属于某个已写入版本。
		for i := 0; i < n; i++ {
			rc, info, err := s.Get("rw")
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
			var gen int
			if _, err := fmt.Sscanf(string(b), "gen-%d", &gen); err != nil {
				errs <- fmt.Errorf("torn read %q of version %s", b, info.VersionID)
			}
		}
		done <- struct{}{}
	}()
	go func() { // 回收者：在写入进行期间持续回收。
		for i := 0; i < n; i++ {
			if _, err := s.RetainVersions("rw", 2); err != nil && !errors.Is(err, ErrNotFound) {
				errs <- fmt.Errorf("retain: %w", err)
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
	// 全部读写结束后做最终回收（此刻无在读版本），保留窗口必须精确生效。
	rep, err := s.RetainVersions("rw", 2)
	if err != nil {
		t.Fatalf("final retain: %v", err)
	}
	if len(rep.Deferred) != 0 {
		t.Fatalf("no readers remain, deferred = %+v, want empty", rep.Deferred)
	}
	// 最终状态自洽：当前版本为最后一次写入，保留窗口生效。
	r, info, err := s.Get("rw")
	if err != nil {
		t.Fatalf("final get: %v", err)
	}
	if got := getString(t, r); got != fmt.Sprintf("gen-%d", n-1) {
		t.Fatalf("final content = %q, want gen-%d", got, n-1)
	}
	vs, err := s.ListVersions("rw")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(vs) != 2 {
		t.Fatalf("versions = %d, want exactly retention window 2", len(vs))
	}
	if vs[len(vs)-1].VersionID != info.VersionID {
		t.Fatalf("current version %s not in retained window %+v", info.VersionID, vs)
	}
}

// 不存在的键回收返回 ErrNotFound；keep<=0 时仍保留当前版本。
func TestRetainEdgeCases(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.RetainVersions("missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retain missing key err = %v, want ErrNotFound", err)
	}
	v1 := putString(t, s, "k", "only", PutOptions{})
	rep, err := s.RetainVersions("k", 0)
	if err != nil {
		t.Fatalf("retain keep=0: %v", err)
	}
	if len(rep.Removed) != 0 {
		t.Fatalf("keep=0 must retain current version, removed = %+v", rep.Removed)
	}
	r, _, err := s.Get("k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := getString(t, r); got != "only" {
		t.Fatalf("content = %q, want only", got)
	}
	vs, _ := s.ListVersions("k")
	if len(vs) != 1 || vs[0].VersionID != v1.VersionID {
		t.Fatalf("versions = %+v, want only %s", vs, v1.VersionID)
	}
	// 多版本时 keep<1 按 1 处理：只留当前版本。
	putString(t, s, "k", "second", PutOptions{})
	putString(t, s, "k", "third", PutOptions{})
	rep, err = s.RetainVersions("k, ", -5)
	_ = rep
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("unexpected err: %v", err)
	}
	rep, err = s.RetainVersions("k", -5)
	if err != nil {
		t.Fatalf("retain keep=-5: %v", err)
	}
	if len(rep.Removed) != 2 {
		t.Fatalf("keep<1 must behave as keep=1, removed = %+v", rep.Removed)
	}
	vs, _ = s.ListVersions("k")
	if len(vs) != 1 {
		t.Fatalf("versions = %d, want 1", len(vs))
	}
	// 关闭后调用报错。
	s.Close()
	if _, err := s.RetainVersions("k", 1); err == nil {
		t.Fatal("retain after close must fail")
	}
}
