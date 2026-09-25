package objectstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	s, err := Open(Config{Dir: t.TempDir(), Logger: log})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func putString(t *testing.T, s *Store, key, val string, opts PutOptions) VersionInfo {
	t.Helper()
	vi, err := s.Put(key, bytes.NewReader([]byte(val)), opts)
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return vi
}

func getString(t *testing.T, r io.ReadCloser) string {
	t.Helper()
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(b)
}

func TestPutGetAndVersionAddressing(t *testing.T) {
	s := openTestStore(t)
	v1 := putString(t, s, "k", "first", PutOptions{})
	v2 := putString(t, s, "k", "second", PutOptions{})
	if v1.VersionID == v2.VersionID {
		t.Fatalf("version ids must differ: %s", v1.VersionID)
	}
	// 当前版本为最新写入。
	r, info, err := s.Get("k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if info.VersionID != v2.VersionID {
		t.Fatalf("current version = %s, want %s", info.VersionID, v2.VersionID)
	}
	if got := getString(t, r); got != "second" {
		t.Fatalf("current content = %q, want %q", got, "second")
	}
	// 被覆盖的历史版本仍可寻址读取且内容不变。
	r, info, err = s.GetVersion("k", v1.VersionID)
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	if got := getString(t, r); got != "first" {
		t.Fatalf("historical content = %q, want %q", got, "first")
	}
	_ = info
	// 不存在的键与版本。
	if _, _, err := s.Get("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing key err = %v, want ErrNotFound", err)
	}
	if _, _, err := s.GetVersion("k", "v99999999"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing version err = %v, want ErrNotFound", err)
	}
}

func TestConditionalWrites(t *testing.T) {
	s := openTestStore(t)
	v1 := putString(t, s, "k", "v1", PutOptions{IfAbsent: true})

	// IfAbsent 冲突：原因须可区分为 key_exists。
	_, err := s.Put("k", bytes.NewReader([]byte("x")), PutOptions{IfAbsent: true})
	var pe *PreconditionError
	if !errors.As(err, &pe) || !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("err = %v, want PreconditionError", err)
	}
	if pe.Kind != PreconditionKeyExists {
		t.Fatalf("kind = %s, want %s", pe.Kind, PreconditionKeyExists)
	}

	// ExpectVersion 不匹配：原因须可区分为 version_mismatch。
	_, err = s.Put("k", bytes.NewReader([]byte("x")), PutOptions{ExpectVersion: "v99999999"})
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want PreconditionError", err)
	}
	if pe.Kind != PreconditionVersionMismatch {
		t.Fatalf("kind = %s, want %s", pe.Kind, PreconditionVersionMismatch)
	}
	if pe.Expected != "v99999999" || pe.Actual != v1.VersionID {
		t.Fatalf("expected=%s actual=%s, want expected=v99999999 actual=%s", pe.Expected, pe.Actual, v1.VersionID)
	}

	// ExpectVersion 匹配则成功。
	v2 := putString(t, s, "k", "v2", PutOptions{ExpectVersion: v1.VersionID})
	if v2.VersionID == v1.VersionID {
		t.Fatal("version id must advance")
	}
	// 失败的条件写入不得产生版本。
	vs, err := s.ListVersions("k")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(vs) != 2 {
		t.Fatalf("versions = %d, want 2 (rejected writes must not create versions)", len(vs))
	}
}

func TestConcurrentUnconditionalPuts(t *testing.T) {
	s := openTestStore(t)
	const n = 32
	var wg sync.WaitGroup
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			vi, err := s.Put("hot", bytes.NewReader([]byte(fmt.Sprintf("body-%d", i))), PutOptions{})
			if err != nil {
				t.Errorf("put %d: %v", i, err)
				return
			}
			ids[i] = vi.VersionID
		}(i)
	}
	wg.Wait()
	// 全部成功且版本号互不相同：并发写入不得互相覆盖。
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" || seen[id] {
			t.Fatalf("duplicate or empty version id %q", id)
		}
		seen[id] = true
	}
	vs, err := s.ListVersions("hot")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(vs) != n {
		t.Fatalf("versions = %d, want %d", len(vs), n)
	}
	// 每个版本内容与其写入体一致，无半写数据。
	for i := 0; i < n; i++ {
		r, _, err := s.GetVersion("hot", ids[i])
		if err != nil {
			t.Fatalf("get version %s: %v", ids[i], err)
		}
		want := fmt.Sprintf("body-%d", i)
		if got := getString(t, r); got != want {
			t.Fatalf("version %s content = %q, want %q", ids[i], got, want)
		}
	}
}

func TestConcurrentIfAbsentOnlyOneWins(t *testing.T) {
	s := openTestStore(t)
	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	var successes, keyExists int
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Put("once", bytes.NewReader([]byte("x")), PutOptions{IfAbsent: true})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successes++
				return
			}
			var pe *PreconditionError
			if errors.As(err, &pe) && pe.Kind == PreconditionKeyExists {
				keyExists++
			} else {
				t.Errorf("unexpected err: %v", err)
			}
		}()
	}
	wg.Wait()
	if successes != 1 || keyExists != n-1 {
		t.Fatalf("successes=%d keyExists=%d, want 1 and %d", successes, keyExists, n-1)
	}
}

func TestDeleteKeepsHistoryAddressable(t *testing.T) {
	s := openTestStore(t)
	v1 := putString(t, s, "k", "data", PutOptions{})
	del, err := s.Delete("k")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !del.Deleted {
		t.Fatal("delete marker must be flagged deleted")
	}
	// 当前版本为删除标记。
	if _, _, err := s.Get("k"); !errors.Is(err, ErrDeleted) {
		t.Fatalf("get after delete err = %v, want ErrDeleted", err)
	}
	// 历史版本仍可寻址读取。
	r, _, err := s.GetVersion("k", v1.VersionID)
	if err != nil {
		t.Fatalf("get deleted history: %v", err)
	}
	if got := getString(t, r); got != "data" {
		t.Fatalf("content = %q, want %q", got, "data")
	}
	// 版本列表包含删除标记。
	vs, err := s.ListVersions("k")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(vs) != 2 || !vs[1].Deleted {
		t.Fatalf("versions = %+v, want 2 with tombstone last", vs)
	}
	// 重复删除被拒绝。
	if _, err := s.Delete("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("re-delete err = %v, want ErrNotFound", err)
	}
	// 删除后 IfAbsent 写入成功（键视为不存在）。
	if _, err := s.Put("k", bytes.NewReader([]byte("reborn")), PutOptions{IfAbsent: true}); err != nil {
		t.Fatalf("put after delete with IfAbsent: %v", err)
	}
}
