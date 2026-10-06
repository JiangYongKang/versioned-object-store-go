package objectstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// 场景【崩溃判定·内容被篡改】：数据文件内容与记录校验和不一致，
// Open 必须失败并指出键与版本；反复打开结论一致。
func TestOpenFailsOnChecksumMismatch(t *testing.T) {
	t.Log("scenario: crash-judgement / tampered data (checksum mismatch) fails open")
	dir := t.TempDir()
	s := openTestStoreWithDir(t, dir)
	v1 := putString(t, s, "doc", "original-content", PutOptions{})
	s.Close()

	p := findDataFile(t, dir, v1.VersionID)
	// 篡改内容但保持长度不变（绕过大小校验，专测校验和）。
	b, _ := os.ReadFile(p)
	b[0] ^= 0xff
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatalf("tamper: %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		_, err := Open(Config{Dir: dir})
		var ce *CorruptionError
		if !errors.As(err, &ce) {
			t.Fatalf("attempt %d: err = %v, want *CorruptionError", attempt, err)
		}
		if len(ce.Versions) != 1 || ce.Versions[0].Key != "doc" || ce.Versions[0].VersionID != v1.VersionID {
			t.Fatalf("attempt %d: versions = %+v, want doc/%s", attempt, ce.Versions, v1.VersionID)
		}
		t.Logf("attempt %d: %v", attempt, err)
	}
}

// 场景【崩溃判定·记录被截断】：数据文件被截断（大小与记录不一致），
// Open 失败；修复（恢复正确内容）后同一目录可以正常打开且数据完好。
func TestOpenFailsOnTruncatedDataThenRecovers(t *testing.T) {
	t.Log("scenario: crash-judgement / truncated data fails open; manual repair then opens")
	dir := t.TempDir()
	s := openTestStoreWithDir(t, dir)
	v1 := putString(t, s, "log", "0123456789abcdef", PutOptions{})
	s.Close()

	p := findDataFile(t, dir, v1.VersionID)
	if err := os.Truncate(p, 4); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	_, err := Open(Config{Dir: dir})
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want *CorruptionError", err)
	}
	t.Logf("truncation reported: %v", err)

	// 人工修复：写回正确内容后必须能正常打开。
	if err := os.WriteFile(p, []byte("0123456789abcdef"), 0o644); err != nil {
		t.Fatalf("repair: %v", err)
	}
	s2, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open after repair: %v", err)
	}
	defer s2.Close()
	r, _, err := s2.Get("log")
	if err != nil {
		t.Fatalf("get after repair: %v", err)
	}
	if got := getString(t, r); got != "0123456789abcdef" {
		t.Fatalf("content = %q, want original", got)
	}
}

// 场景【崩溃判定·元数据损坏】：meta.json 被写坏时 Open 失败且可区分，
// 不会把它当作空存储继续启动（否则会掩盖已有数据）。
func TestOpenFailsOnCorruptMeta(t *testing.T) {
	t.Log("scenario: crash-judgement / corrupt meta.json fails open")
	dir := t.TempDir()
	s := openTestStoreWithDir(t, dir)
	putString(t, s, "k", "v1", PutOptions{})
	s.Close()

	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(`{"objects": {"k": `), 0o644); err != nil {
		t.Fatalf("corrupt meta: %v", err)
	}
	_, err := Open(Config{Dir: dir})
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want *CorruptionError", err)
	}
	if ce.Detail == "" {
		t.Fatalf("corruption detail should describe meta.json problem: %+v", ce)
	}
	t.Logf("corrupt meta reported: %v", err)
}

// 场景【崩溃判定·安全残留自动修复】：崩溃留在末尾的残留（未提交临时文件、
// 无引用数据、未知上传目录）自动清除后能正常启动，且反复重启结论一致。
// （残留构造与已提交数据校验由 recovery_test.go 覆盖，这里专注幂等性。）
func TestSafeResidueRepairIsIdempotent(t *testing.T) {
	t.Log("scenario: crash-judgement / safe residue auto-repaired, idempotent across restarts")
	dir := t.TempDir()
	s := openTestStoreWithDir(t, dir)
	v1 := putString(t, s, "k", "good", PutOptions{})
	s.Close()

	orphan := filepath.Join(dir, "data", "beef", "v00000001")
	os.MkdirAll(filepath.Dir(orphan), 0o755)
	os.WriteFile(orphan, []byte("residue"), 0o644)
	os.WriteFile(filepath.Join(dir, "meta.json.tmp"), []byte("half-written"), 0o644)

	// 连续三次重启：都成功，结论一致（残留已清、数据完好、不再反复清理）。
	for i := 1; i <= 3; i++ {
		s, err := Open(Config{Dir: dir})
		if err != nil {
			t.Fatalf("restart %d: %v", i, err)
		}
		r, _, err := s.Get("k")
		if err != nil {
			t.Fatalf("restart %d get: %v", i, err)
		}
		if got := getString(t, r); got != "good" {
			t.Fatalf("restart %d content = %q", i, got)
		}
		s.Close()
		if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("restart %d: residue should be gone", i)
		}
	}
	_ = v1
}

func findDataFile(t *testing.T, dir, versionID string) string {
	t.Helper()
	matches, _ := filepath.Glob(filepath.Join(dir, "data", "*", versionID))
	if len(matches) != 1 {
		t.Fatalf("cannot locate data file for %s", versionID)
	}
	return matches[0]
}
