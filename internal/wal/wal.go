// Package wal provides an append-only write-ahead log with fsync-durable appends.
//
// On-disk frame format (all integers big-endian):
//
//	uint32 payloadLen
//	uint32 payloadCRC32 (IEEE, over payload only)
//	bytes  payload (JSON)
//
// A torn frame at the tail (partial write caused by a crash) is truncated on
// open. A length/CRC/JSON failure anywhere before the tail is corruption.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Record is one framed log entry.
type Record struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// ErrCorrupted is returned when the log contains a non-tail invalid frame.
var ErrCorrupted = errors.New("wal: corrupted frame")

// WAL is an append-only write-ahead log backed by a single file.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

const (
	headerLen = 8
	maxFrame  = 64 * 1024 * 1024
)

// Open opens (creating if necessary) and replays the WAL at path.
// Replayed records are delivered to onRecord in order. A torn tail frame is
// truncated; any other malformed frame yields ErrCorrupted.
func Open(path string, onRecord func(Record) error) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	// Durability of the directory entry when the file is newly created.
	if dir, derr := os.Open(filepath.Dir(path)); derr == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	goodEnd, err := replay(f, onRecord)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := f.Truncate(int64(goodEnd)); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &WAL{f: f, path: path}, nil
}

func replay(f *os.File, onRecord func(Record) error) (int, error) {
	buf := make([]byte, headerLen)
	off := 0
	for {
		n, err := io.ReadFull(f, buf)
		if err == io.EOF && n == 0 {
			return off, nil // clean end
		}
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			// Torn header at the tail: discard it.
			return off, nil
		}
		if err != nil {
			return 0, err
		}
		plen := int(binary.BigEndian.Uint32(buf[0:4]))
		pcrc := binary.BigEndian.Uint32(buf[4:8])
		if plen <= 0 || plen > maxFrame {
			return 0, fmt.Errorf("%w at offset %d: bad length %d", ErrCorrupted, off, plen)
		}
		payload := make([]byte, plen)
		pn, perr := io.ReadFull(f, payload)
		if perr == io.ErrUnexpectedEOF || perr == io.EOF {
			return off, nil // torn payload at the tail
		}
		if perr != nil {
			return 0, perr
		}
		if crc32.ChecksumIEEE(payload) != pcrc {
			return 0, fmt.Errorf("%w at offset %d: crc mismatch", ErrCorrupted, off)
		}
		var rec Record
		if err := json.Unmarshal(payload, &rec); err != nil {
			return 0, fmt.Errorf("%w at offset %d: %v", ErrCorrupted, off, err)
		}
		if onRecord != nil {
			if err := onRecord(rec); err != nil {
				return 0, fmt.Errorf("%w at offset %d: %v", ErrCorrupted, off, err)
			}
		}
		off += headerLen + pn
	}
}

// Append atomically appends one record and fsyncs it before returning.
// Concurrent callers are serialized; the frame either is fully durable or is
// absent (a torn tail is discarded on the next Open).
func (w *WAL) Append(rec Record) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if len(payload) > maxFrame {
		return fmt.Errorf("wal: record too large (%d bytes)", len(payload))
	}
	frame := make([]byte, headerLen+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[headerLen:], payload)

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Write(frame); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close releases the WAL resources.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}
