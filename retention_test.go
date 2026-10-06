package objectstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// 场景【保留回收·单键】：只保留最近 N 个版本，超窗版本被回收；
// 回收报告的字节数能与磁盘实际占用变化对上。
func TestRetainVersionsSingleKey(t *testing.T) {
	t.Log("scenario: retention / keep newest N versions of one key, report matches disk")
	s := openTestStore(t)
	var ids []string
	for i := 0; i < 5; i++ {
		v := putString(t, s, "k", fmt.Sprintf("gen-%d", i), PutOptions{})
		ids = append(ids, v.VersionID)
	}
	before := storeDataBytes(t, s)

	rep, err := s.RetainVersions("k", 2)
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if rep.RemovedVersions != 3 {
		t.Fatalf("removed = %d, want 3", rep.RemovedVersions)
	}
	if len(rep.Keys) != 1 || len(rep.Keys[0].RemovedVersionIDs) != 3 {
		t.Fatalf("report keys = %+v", rep.Keys)
	}
	for i, id := range ids[:3] {
		if rep.Keys[0].RemovedVersionIDs[i] != id {
			t.Fatalf("removed[%d] = %s, want %s", i, rep.Keys[0].RemovedVersionIDs[i], id)
		}
	}
	// 报告字节数 == 磁盘实际减少量。
	after := storeDataBytes(t, s)
	if got := before - after; got != rep.ReclaimedBytes {
		t.Fatalf("disk reclaimed = %d, report says %d", got, rep.ReclaimedBytes)
	}
	t.Logf("reclaimed %d bytes for %d versions", rep.ReclaimedBytes, rep.RemovedVersions)

	// 窗口内版本可读，超窗版本不可寻址。
	vs, err := s.ListVersions("k")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(vs) != 2 || vs[0].VersionID != ids[3] || vs[1].VersionID != ids[4] {
		t.Fatalf("remaining versions = %+v, want newest 2", vs)
	}
	r, _, err := s.Get("k")
	if err != nil {
		t.Fatalf("get current: %v", err)
	}
	if got := getString(t, r); got != "gen-4" {
		t.Fatalf("current = %q, want gen-4", got)
	}
	if _, _, err := s.GetVersion("k", ids[0]); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reclaimed version should be gone, err = %v", err)
	}
}

// 场景【保留回收·删除标记语义】：当前版本是删除标记时回收旧版本，
// 删除标记本身与窗口内版本不受影响，键对外仍表现为已删除。
func TestRetainPreservesDeleteMarkerSemantics(t *testing.T) {
	t.Log("scenario: retention / delete marker and current version untouched")
	s := openTestStore(t)
	putString(t, s, "d", "a", PutOptions{})
	putString(t, s, "d", "b", PutOptions{})
	putString(t, s, "d", "c", PutOptions{})
	if _, err := s.Delete("d"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	rep, err := s.RetainVersions("d", 2) // 保留 gen-c 与删除标记
	if err != nil {
		t.Fatalf("retain: %v", err)
	}
	if rep.RemovedVersions != 2 {
		t.Fatalf("removed = %d, want 2", rep.RemovedVersions)
	}
	// 当前版本仍是删除标记。
	if _, _, err := s.Get("d"); !errors.Is(err, ErrDeleted) {
		t.Fatalf("get after retention err = %v, want ErrDeleted", err)
	}
	vs, _ := s.ListVersions("d")
	if len(vs) != 2 || !vs[1].Deleted {
		t.Fatalf("versions = %+v, want [c, delete-marker]", vs)
	}
	// 窗口内的历史版本仍可读。
	r, _, err := s.GetVersion("d", vs[0].VersionID)
	if err != nil {
		t.Fatalf("get retained version: %v", err)
	}
	if got := getString(t, r); got != "c" {
		t.Fatalf("retained content = %q, want c", got)
	}
}

// 场景【保留回收·全库】：RetainAll 对每个键生效，不影响其他键的当前版本。
func TestRetainAllKeys(t *testing.T) {
	t.Log("scenario: retention / whole-store retain window")
	s := openTestStore(t)
	for i := 0; i < 4; i++ {
		putString(t, s, "x", fmt.Sprintf("x%d", i), PutOptions{})
	}
	for i := 0; i < 2; i++ {
		putString(t, s, "y", fmt.Sprintf("y%d", i), PutOptions{})
	}
	rep, err := s.RetainAll(1)
	if err != nil {
		t.Fatalf("retain all: %v", err)
	}
	if rep.RemovedVersions != 4 { // x 超窗 3 个 + y 超窗 1 个
		t.Fatalf("removed = %d, want 4", rep.RemovedVersions)
	}
	for key, want := range map[string]string{"x": "x3", "y": "y1"} {
		r, _, err := s.Get(key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if got := getString(t, r); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	// 再次回收无可回收项，报告为零。
	rep, err = s.RetainAll(1)
	if err != nil {
		t.Fatalf("retain all again: %v", err)
	}
	if rep.RemovedVersions != 0 || rep.ReclaimedBytes != 0 {
		t.Fatalf("second report = %+v, want empty", rep)
	}
}

// 场景【保留回收·与并发读写交错】：回收进行中，已开始的读取必须完成且内容正确，
// 并发写入不丢失，最终状态自洽。
func TestRetentionConcurrentWithReadWrite(t *testing.T) {
	t.Log("scenario: retention / interleaved with concurrent reads and writes")
	s := openTestStore(t)
	putString(t, s, "c", "seed", PutOptions{})

	const n = 10
	errs := make(chan error, 3*n)
	var wg sync.WaitGroup
	// 写者。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			if _, err := s.Put("c", bytes.NewReader([]byte(fmt.Sprintf("gen-%d", i))), PutOptions{}); err != nil {
				errs <- fmt.Errorf("put: %w", err)
			}
		}
	}()
	// 读者：拿到读句柄后稍慢读完，覆盖「回收删掉正在读的版本」的交错。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			rc, _, err := s.Get("c")
			if err != nil {
				errs <- fmt.Errorf("get: %w", err)
				continue
			}
			b, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				errs <- fmt.Errorf("read: %w", err)
			}
			if len(b) == 0 {
				errs <- fmt.Errorf("empty body")
			}
		}
	}()
	// 回收者。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			if _, err := s.RetainVersions("c", 3); err != nil {
				errs <- fmt.Errorf("retain: %w", err)
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent error: %v", err)
	}
	// 最终自洽：再执行一次回收收敛窗口，当前版本是最后一次写入且在窗口内。
	if _, err := s.RetainVersions("c", 3); err != nil {
		t.Fatalf("final retain: %v", err)
	}
	r, info, err := s.Get("c")
	if err != nil {
		t.Fatalf("final get: %v", err)
	}
	if got := getString(t, r); got != fmt.Sprintf("gen-%d", n-1) {
		t.Fatalf("final content = %q, want gen-%d", got, n-1)
	}
	vs, _ := s.ListVersions("c")
	if len(vs) > 3 {
		t.Fatalf("versions after retention = %d, want <= 3", len(vs))
	}
	if vs[len(vs)-1].VersionID != info.VersionID {
		t.Fatalf("current version %s not in retained window", info.VersionID)
	}
	for _, v := range vs {
		rc, _, err := s.GetVersion("c", v.VersionID)
		if err != nil {
			t.Fatalf("retained version %s unreadable: %v", v.VersionID, err)
		}
		rc.Close()
	}
}

// 场景【保留回收·参数与边界】：keep<1 被拒绝；不存在的键返回 ErrNotFound；
// 版本数不超过窗口时不产生任何回收。
func TestRetainValidationAndNoop(t *testing.T) {
	t.Log("scenario: retention / argument validation and no-op cases")
	s := openTestStore(t)
	putString(t, s, "k", "only", PutOptions{})

	if _, err := s.RetainVersions("k", 0); err == nil {
		t.Fatalf("keep=0 should be rejected")
	}
	if _, err := s.RetainAll(-1); err == nil {
		t.Fatalf("keep=-1 should be rejected")
	}
	if _, err := s.RetainVersions("missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing key err = %v, want ErrNotFound", err)
	}
	rep, err := s.RetainVersions("k", 5)
	if err != nil {
		t.Fatalf("retain noop: %v", err)
	}
	if rep.RemovedVersions != 0 || rep.ReclaimedBytes != 0 {
		t.Fatalf("noop report = %+v, want empty", rep)
	}
}

func storeDataBytes(t *testing.T, s *Store) int64 {
	t.Helper()
	var total int64
	err := filepath.Walk(s.dataDir(), func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk data dir: %v", err)
	}
	return total
}
