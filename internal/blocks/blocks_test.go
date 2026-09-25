package blocks

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestTempPublishDedupOpen(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mk := func(body string) *TempBlock {
		tb, err := s.CreateTemp(bytes.NewReader([]byte(body)), int64(len(body)))
		if err != nil {
			t.Fatal(err)
		}
		return tb
	}
	tb1 := mk("same-body")
	tb2 := mk("same-body")
	c1, err := s.Publish(tb1)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := s.Publish(tb2)
	if err != nil {
		t.Fatal(err)
	}
	if c1 != c2 {
		t.Fatal("identical content must dedup to one block")
	}
	rc, err := s.Open(c1)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(b) != "same-body" {
		t.Fatalf("body=%q", b)
	}
	if err := s.Remove(c1); err != nil {
		t.Fatal(err)
	}
	// temp dir is empty after publishes
	entries, _ := os.ReadDir(filepath.Join(s.root, "tmp"))
	if len(entries) != 0 {
		t.Fatalf("leftover temp files: %d", len(entries))
	}
}

func TestCleanupTempAndList(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	tb, err := s.CreateTemp(bytes.NewReader([]byte("staged")), 6)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupTemp(); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveTemp(tb); err != nil { // idempotent after cleanup
		t.Fatal(err)
	}
	tb2, _ := s.CreateTemp(bytes.NewReader([]byte("persist")), 7)
	c, _ := s.Publish(tb2)
	list, err := s.List()
	if err != nil || len(list) != 1 || list[0] != c {
		t.Fatalf("list=%v err=%v", list, err)
	}
	if size, ok, err := s.Stat(c); err != nil || !ok || size != 7 {
		t.Fatalf("stat size=%d ok=%v err=%v", size, ok, err)
	}
}
