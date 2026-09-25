package wal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.wal")
	var got []Record
	w, err := Open(path, func(r Record) error { got = append(got, r); return nil })
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if err := w.Append(Record{Type: "t", Data: json.RawMessage(`{"i":` + itoa(i) + `}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: all durable records are replayed in order.
	var got2 []Record
	w2, err := Open(path, func(r Record) error { got2 = append(got2, r); return nil })
	if err != nil {
		t.Fatal(err)
	}
	_ = w2.Close()
	if len(got2) != 50 {
		t.Fatalf("replayed after reopen=%d, want 50", len(got2))
	}
}

func TestTornTailTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.wal")
	w, _ := Open(path, nil)
	if err := w.Append(Record{Type: "t", Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	_, _ = f.Write([]byte{0, 0, 0, 9, 0, 0, 0, 0, 'a', 'b'}) // header+partial body
	_ = f.Close()

	count := 0
	w2, err := Open(path, func(r Record) error { count++; return nil })
	if err != nil {
		t.Fatalf("torn tail must truncate, got %v", err)
	}
	_ = w2.Close()
	if count != 1 {
		t.Fatalf("count=%d", count)
	}
	// Append after recovery lands cleanly.
	w3, err := Open(path, func(r Record) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := w3.Append(Record{Type: "t2", Data: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	_ = w3.Close()
}

func TestCorruptedMiddleFrame(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.wal")
	w, _ := Open(path, nil)
	for range 5 {
		if err := w.Append(Record{Type: "t", Data: json.RawMessage(`{"ok":true}`)}); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()
	data, _ := os.ReadFile(path)
	frameLen := len(data) / 5
	// Flip a payload byte inside the second frame (non-tail) -> CRC mismatch.
	data[frameLen+headerLen] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(path, nil)
	if !errors.Is(err, ErrCorrupted) {
		t.Fatalf("err=%v, want ErrCorrupted", err)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	return string(b[n:])
}
