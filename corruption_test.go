package objectstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// 元数据文件被截断/破坏：打开必须失败，错误可区分为元数据损坏，
// 且反复重开结论一致。
func TestOpenFailsOnTruncatedMeta(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	putString(t, s, "k", "data", PutOptions{})
	s.Close()

	b, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	// 截断到一半，制造不完整的 JSON。
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), b[:len(b)/2], 0o644); err != nil {
		t.Fatalf("truncate meta: %v", err)
	}
	for i := 0; i < 3; i++ {
		_, err := Open(Config{Dir: dir, Logger: testLogger()})
		var ce *CorruptionError
		if !errors.As(err, &ce) || !errors.Is(err, ErrCorruption) {
			t.Fatalf("reopen %d: err = %v, want CorruptionError", i, err)
		}
		if ce.Meta == nil {
			t.Fatalf("reopen %d: Meta must be set for truncated meta.json", i)
		}
		t.Logf("reopen %d rejected: %v", i, err)
	}
}

// 可见版本的数据内容被篡改（校验和不匹配）：打开必须失败并指出键与版本；
// 修复数据后可正常打开且内容正确。
func TestOpenFailsOnTamperedDataThenRepairs(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	v1 := putString(t, s, "k", "good-content", PutOptions{})
	v2 := putString(t, s, "k", "second-version", PutOptions{})
	s.Close()

	matches, _ := filepath.Glob(filepath.Join(dir, "data", "*", v1.VersionID))
	if len(matches) != 1 {
		t.Fatalf("cannot locate data file for %s", v1.VersionID)
	}
	orig, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read data: %v", err)
	}
	// 同长度篡改内容：尺寸不变但校验和不匹配。
	tampered := []byte("XXXX" + string(orig[4:]))
	if err := os.WriteFile(matches[0], tampered, 0o644); err != nil {
		t.Fatalf("tamper: %v", err)
	}

	// 反复重开：每次都必须失败并指向 k@v1，完好的 v2 不被指认。
	for i := 0; i < 3; i++ {
		_, err := Open(Config{Dir: dir, Logger: testLogger()})
		var ce *CorruptionError
		if !errors.As(err, &ce) || !errors.Is(err, ErrCorruption) {
			t.Fatalf("reopen %d: err = %v, want CorruptionError", i, err)
		}
		if len(ce.Versions) != 1 || ce.Versions[0].Key != "k" || ce.Versions[0].VersionID != v1.VersionID {
			t.Fatalf("reopen %d: corrupt = %+v, want k@%s", i, ce.Versions, v1.VersionID)
		}
		t.Logf("reopen %d rejected: %v", i, err)
	}

	// 修复数据（写回原始内容）后可正常打开，两个版本都完整可读。
	if err := os.WriteFile(matches[0], orig, 0o644); err != nil {
		t.Fatalf("repair: %v", err)
	}
	s = reopen(t, dir)
	for vid, want := range map[string]string{v1.VersionID: "good-content", v2.VersionID: "second-version"} {
		r, _, err := s.GetVersion("k", vid)
		if err != nil {
			t.Fatalf("get %s after repair: %v", vid, err)
		}
		if got := getString(t, r); got != want {
			t.Fatalf("version %s = %q, want %q", vid, got, want)
		}
	}
}

// 数据文件尺寸与元数据不符（被截断）：打开失败并指出具体版本。
func TestOpenFailsOnSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	v1 := putString(t, s, "k", "some-longer-content", PutOptions{})
	s.Close()

	matches, _ := filepath.Glob(filepath.Join(dir, "data", "*", v1.VersionID))
	if len(matches) != 1 {
		t.Fatalf("cannot locate data file for %s", v1.VersionID)
	}
	if err := os.Truncate(matches[0], 4); err != nil {
		t.Fatalf("truncate data: %v", err)
	}
	_, err := Open(Config{Dir: dir, Logger: testLogger()})
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want CorruptionError", err)
	}
	if len(ce.Versions) != 1 || ce.Versions[0].VersionID != v1.VersionID {
		t.Fatalf("corrupt = %+v, want k@%s", ce.Versions, v1.VersionID)
	}
	t.Logf("rejected: %v", err)
}

// 可安全丢弃的崩溃残留（无引用数据文件、未提交临时元数据）仍然自动修复，
// 且修复后反复重启结论一致、数据完好。
func TestSafeResidueAutoRepairedConsistently(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	v1 := putString(t, s, "k", "committed", PutOptions{})
	s.Close()

	orphan := filepath.Join(dir, "data", "beef", "v00000055")
	if err := os.MkdirAll(filepath.Dir(orphan), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan, []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "meta.json.tmp"), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	// 第一次重开：自动修复残留。
	s = reopen(t, dir)
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan should be removed, stat err = %v", err)
	}
	r, _, err := s.GetVersion("k", v1.VersionID)
	if err != nil {
		t.Fatalf("get committed: %v", err)
	}
	if got := getString(t, r); got != "committed" {
		t.Fatalf("content = %q, want %q", got, "committed")
	}
	s.Close()
	// 第二次重开：状态自洽，结论一致（无残留、数据完好）。
	s = reopen(t, dir)
	r, _, err = s.Get("k")
	if err != nil {
		t.Fatalf("get after second reopen: %v", err)
	}
	if got := getString(t, r); got != "committed" {
		t.Fatalf("content = %q, want %q", got, "committed")
	}
}
