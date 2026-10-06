package objectstore

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// 同一进程内：同一目录只能打开一次，第二次打开被明确拒绝且原因可区分；
// 第一个使用者关闭后，后来者可以正常打开。
func TestDirExclusiveSameProcess(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	putString(t, s, "k", "v1", PutOptions{})

	_, err := Open(Config{Dir: dir, Logger: testLogger()})
	var dle *DirLockedError
	if !errors.As(err, &dle) || !errors.Is(err, ErrDirLocked) {
		t.Fatalf("second open err = %v, want DirLockedError", err)
	}
	if dle.Dir != dir {
		t.Fatalf("locked dir = %q, want %q", dle.Dir, dir)
	}
	if dle.HolderPID != os.Getpid() {
		t.Fatalf("holder pid = %d, want %d", dle.HolderPID, os.Getpid())
	}
	t.Logf("second open rejected: %v", err)

	// 持有者关闭后，后来者可以打开并读到既有数据。
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s2 := reopen(t, dir)
	r, _, err := s2.Get("k")
	if err != nil {
		t.Fatalf("get after handover: %v", err)
	}
	if got := getString(t, r); got != "v1" {
		t.Fatalf("content = %q, want %q", got, "v1")
	}
}

// 跨进程：父进程持有目录时，子进程打开同一目录必须被拒绝，
// 且子进程（被拒绝方）不能改动已有数据；父进程关闭后子进程可正常打开。
func TestDirExclusiveAcrossProcesses(t *testing.T) {
	if os.Getenv("OS_HELPER_OPEN") == "1" {
		// 子进程：尝试打开目录，结果打印到 stdout 供父进程断言。
		dir := os.Args[len(os.Args)-1]
		s, err := Open(Config{Dir: dir})
		if err != nil {
			fmt.Printf("open failed: %v\n", err)
			if errors.Is(err, ErrDirLocked) {
				fmt.Println("locked")
			}
			os.Exit(0)
		}
		defer s.Close()
		if _, err := s.Put("child", strings.NewReader("child-was-here"), PutOptions{}); err != nil {
			fmt.Printf("put failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("open-ok")
		return
	}

	dir := t.TempDir()
	s := reopen(t, dir)
	v1 := putString(t, s, "k", "parent-data", PutOptions{})
	metaBefore, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}

	runChild := func() string {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=TestDirExclusiveAcrossProcesses", dir)
		cmd.Env = append(os.Environ(), "OS_HELPER_OPEN=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("child process: %v\n%s", err, out)
		}
		return string(out)
	}

	// 父进程持锁期间：子进程必须被拒绝，且明确报告锁原因。
	out := runChild()
	if !strings.Contains(out, "locked") || strings.Contains(out, "open-ok") {
		t.Fatalf("child output = %q, want lock rejection", out)
	}
	t.Logf("child rejected while parent holds lock: %s", strings.TrimSpace(out))

	// 被拒绝的子进程不得改动已有数据：元数据与对象内容保持原样。
	metaAfter, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatalf("read meta after child: %v", err)
	}
	if !bytes.Equal(metaBefore, metaAfter) {
		t.Fatal("rejected opener modified meta.json")
	}
	r, _, err := s.Get("k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := getString(t, r); got != "parent-data" {
		t.Fatalf("content = %q, want %q", got, "parent-data")
	}
	if _, _, err := s.Get("child"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("child key err = %v, want ErrNotFound (rejected opener must not write)", err)
	}

	// 父进程关闭后：子进程可正常打开并写入。
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	out = runChild()
	if !strings.Contains(out, "open-ok") {
		t.Fatalf("child output after parent close = %q, want opened", out)
	}
	// 子进程的写入与父进程的数据都可见。
	s2 := reopen(t, dir)
	r, _, err = s2.GetVersion("k", v1.VersionID)
	if err != nil {
		t.Fatalf("get parent version: %v", err)
	}
	if got := getString(t, r); got != "parent-data" {
		t.Fatalf("parent content = %q, want %q", got, "parent-data")
	}
	r, _, err = s2.Get("child")
	if err != nil {
		t.Fatalf("get child key: %v", err)
	}
	if got := getString(t, r); got != "child-was-here" {
		t.Fatalf("child content = %q, want %q", got, "child-was-here")
	}
}

// 同一持有者对同一键的并发写入：版本各自独立、互不覆盖、无数据丢失。
func TestConcurrentWritersSameKeyNoLostUpdate(t *testing.T) {
	s := openTestStore(t)
	const writers = 4
	const perWriter = 8
	var wg sync.WaitGroup
	ids := make([][]string, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				vi, err := s.Put("shared", bytes.NewReader([]byte(fmt.Sprintf("w%d-%d", w, i))), PutOptions{})
				if err != nil {
					t.Errorf("writer %d put %d: %v", w, i, err)
					return
				}
				ids[w] = append(ids[w], vi.VersionID)
			}
		}(w)
	}
	wg.Wait()
	// 全部写入都产生了独立版本，且每个版本内容与其写入体一致。
	seen := map[string]bool{}
	for w := 0; w < writers; w++ {
		if len(ids[w]) != perWriter {
			t.Fatalf("writer %d produced %d versions, want %d", w, len(ids[w]), perWriter)
		}
		for i, vid := range ids[w] {
			if seen[vid] {
				t.Fatalf("duplicate version id %s", vid)
			}
			seen[vid] = true
			r, _, err := s.GetVersion("shared", vid)
			if err != nil {
				t.Fatalf("get %s: %v", vid, err)
			}
			want := fmt.Sprintf("w%d-%d", w, i)
			if got := getString(t, r); got != want {
				t.Fatalf("version %s = %q, want %q (lost update?)", vid, got, want)
			}
		}
	}
	vs, err := s.ListVersions("shared")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(vs) != writers*perWriter {
		t.Fatalf("versions = %d, want %d", len(vs), writers*perWriter)
	}
}
