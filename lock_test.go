package objectstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// 场景【目录独占·同进程】：同一进程内重复打开同一目录被明确拒绝，
// 错误中能看出持有者信息；被拒绝方拿不到 Store，无法改动已有数据。
func TestDirLockRejectsSecondOpenInProcess(t *testing.T) {
	t.Log("scenario: dir-exclusivity / same-process second open rejected")
	dir := t.TempDir()
	s := openTestStoreWithDir(t, dir)
	putString(t, s, "k", "v1", PutOptions{})

	_, err := Open(Config{Dir: dir})
	var le *LockError
	if !errors.As(err, &le) {
		t.Fatalf("second open err = %v, want *LockError", err)
	}
	if !errors.Is(err, ErrDirLocked) {
		t.Fatalf("err should match ErrDirLocked: %v", err)
	}
	if !strings.Contains(le.Holder, "pid=") {
		t.Fatalf("holder info should identify the holder, got %q", le.Holder)
	}
	t.Logf("rejected with holder info: %v", err)

	// 已有数据未被影响。
	r, _, err := s.Get("k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := getString(t, r); got != "v1" {
		t.Fatalf("content = %q, want v1", got)
	}
}

// 场景【目录独占·关闭后放行】：第一个使用者 Close 后，后来者可以正常打开。
func TestDirLockReleasedAfterClose(t *testing.T) {
	t.Log("scenario: dir-exclusivity / reopen allowed after close")
	dir := t.TempDir()
	s := openTestStoreWithDir(t, dir)
	v := putString(t, s, "k", "v1", PutOptions{})
	s.Close()

	s2, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open after close: %v", err)
	}
	defer s2.Close()
	r, info, err := s2.Get("k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if info.VersionID != v.VersionID {
		t.Fatalf("version = %s, want %s", info.VersionID, v.VersionID)
	}
	if got := getString(t, r); got != "v1" {
		t.Fatalf("content = %q, want v1", got)
	}
}

// 场景【目录独占·跨进程】：另一个进程持有目录时，本进程打开被拒绝；
// 对方进程退出后（内核释放 flock），本进程才能打开。
func TestDirLockAcrossProcesses(t *testing.T) {
	t.Log("scenario: dir-exclusivity / cross-process open rejected until holder exits")
	if os.Getenv("GO_TEST_LOCK_HELPER") == "1" {
		// 辅助进程：打开目录，通知父进程，然后保持持有直到被杀。
		s, err := Open(Config{Dir: os.Args[len(os.Args)-1]})
		if err != nil {
			fmt.Fprintln(os.Stderr, "helper open failed:", err)
			os.Exit(2)
		}
		defer s.Close()
		fmt.Println("LOCKED")
		select {} // 保持持有，等待父进程杀死
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestDirLockAcrossProcesses", dir)
	cmd.Env = append(os.Environ(), "GO_TEST_LOCK_HELPER=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	buf := make([]byte, 6)
	if _, err := readFull(stdout, buf); err != nil || string(buf) != "LOCKED" {
		t.Fatalf("helper did not lock the dir: %v", err)
	}

	// 持有者存活期间：打开被拒绝，且错误可区分。
	_, err = Open(Config{Dir: dir})
	var le *LockError
	if !errors.As(err, &le) {
		t.Fatalf("open while helper holds lock err = %v, want *LockError", err)
	}
	if !strings.Contains(le.Holder, "pid="+strconv.Itoa(cmd.Process.Pid)) {
		t.Fatalf("holder info should name helper pid %d, got %q", cmd.Process.Pid, le.Holder)
	}
	t.Logf("rejected while helper alive: %v", err)

	// 持有者退出后：可以打开。
	cmd.Process.Kill()
	cmd.Wait()
	s, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open after helper exit: %v", err)
	}
	s.Close()
}

// 场景【目录独占·并发写不丢失】：独占打开期间，多个写者并发写同一个键，
// 每次写入都产生独立版本，不存在互相覆盖导致的丢失。
func TestConcurrentWritersSameKeyNoLostWrites(t *testing.T) {
	t.Log("scenario: dir-exclusivity / concurrent writers on same key lose nothing")
	s := openTestStore(t)
	const writers = 4
	const perWriter = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				body := fmt.Sprintf("w%d-%d", w, i)
				if _, err := s.Put("hot", bytes.NewReader([]byte(body)), PutOptions{}); err != nil {
					errs <- err
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent put: %v", err)
	}
	vs, err := s.ListVersions("hot")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(vs) != writers*perWriter {
		t.Fatalf("versions = %d, want %d (lost writes!)", len(vs), writers*perWriter)
	}
	// 每个版本内容都可读且互不重复。
	seen := map[string]bool{}
	for _, v := range vs {
		r, _, err := s.GetVersion("hot", v.VersionID)
		if err != nil {
			t.Fatalf("get %s: %v", v.VersionID, err)
		}
		body := getString(t, r)
		if seen[body] {
			t.Fatalf("duplicate body %q: versions overwrote each other", body)
		}
		seen[body] = true
	}
}

func openTestStoreWithDir(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func readFull(r io.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// 确保 LOCK 文件在 Close 后仍留存（仅作诊断），但锁已释放——由
// TestDirLockReleasedAfterClose 覆盖打开行为，这里只核对文件存在性约定。
func TestLockFileRemainsForDiagnostics(t *testing.T) {
	t.Log("scenario: dir-exclusivity / LOCK file kept for diagnostics after close")
	dir := t.TempDir()
	s := openTestStoreWithDir(t, dir)
	s.Close()
	if _, err := os.Stat(filepath.Join(dir, "LOCK")); err != nil {
		t.Fatalf("LOCK file should remain for diagnostics: %v", err)
	}
}
