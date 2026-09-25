package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/highcumontoa/versioned-object-store-go/internal/meta"
)

// PartResult is returned for each successfully uploaded part.
type PartResult struct {
	Number   int
	Checksum string
	Size     int64
}

// CompletedPart identifies one part at CompleteMultipart time. The client must
// list the parts strictly in ascending number order with no duplicates.
type CompletedPart struct {
	Number int
}

// CreateMultipart begins a multipart upload for key and returns its upload ID.
func (s *Store) CreateMultipart(ctx context.Context, key string) (uploadID string, err error) {
	if key == "" {
		return "", errors.New("objectstore: empty key")
	}
	id := newID("mp-")
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.appendLocked(meta.NewUploadBeginRecord(id, key, now)); err != nil {
		return "", err
	}
	s.logf("MP_CREATE key=%q upload=%s -> initiated", key, id)
	return id, nil
}

// UploadPart stores one part (1..MaxParts) of uploadID. Parts may arrive in any
// order; repeating a part number replaces the previous part atomically.
func (s *Store) UploadPart(ctx context.Context, uploadID string, partNumber int, r io.Reader, size int64) (*PartResult, error) {
	if partNumber < 1 || partNumber > s.cfg.MaxParts {
		s.logf("MP_PART upload=%s part=%d -> rejected: %s (allowed 1..%d)",
			uploadID, partNumber, ErrInvalidPartNumber.Error(), s.cfg.MaxParts)
		return nil, fmt.Errorf("%w: part %d", ErrInvalidPartNumber, partNumber)
	}
	// Stage outside the metadata lock; blocks are immutable and anonymous.
	tmp, err := s.bs.CreateTemp(r, size)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	u := s.st.Uploads[uploadID]
	if u == nil {
		_ = s.bs.RemoveTemp(tmp)
		return nil, fmt.Errorf("%w: %s", ErrUploadNotFound, uploadID)
	}
	// Part sizes are validated at Complete time (the last part is exempt from
	// the minimum); here any positive byte count is accepted and stored.
	csum, err := s.bs.Publish(tmp)
	if err != nil {
		return nil, err
	}
	prev := u.Parts[partNumber]
	if err := s.appendLocked(meta.NewUploadPartRecord(uploadID, partNumber, csum, tmp.Size())); err != nil {
		return nil, err
	}
	if prev.Checksum != "" {
		s.logf("MP_PART upload=%s key=%q part=%d checksum=%s -> replaced old=%s (old block now unreferenced, gc candidate)",
			uploadID, u.Key, partNumber, csum, prev.Checksum)
	} else {
		s.logf("MP_PART upload=%s key=%q part=%d checksum=%s size=%d -> stored",
			uploadID, u.Key, partNumber, csum, tmp.Size())
	}
	return &PartResult{Number: partNumber, Checksum: csum, Size: tmp.Size()}, nil
}

// CompleteMultipart commits the object version assembled from the given,
// strictly ordered, unique part list. The last listed part is exempt from the
// minimum-size rule.
func (s *Store) CompleteMultipart(ctx context.Context, uploadID string, parts []CompletedPart) (*VersionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	u := s.st.Uploads[uploadID]
	if u == nil {
		// A completed/aborted upload no longer exists. The operation is not
		// idempotent on its own id (completion yields exactly one version);
		// callers get a distinguishable reason.
		return nil, fmt.Errorf("%w: %s", ErrUploadNotFound, uploadID)
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: empty part list", ErrInvalidPartOrder)
	}

	assembled := make([]meta.Part, 0, len(parts))
	var total int64
	for i, cp := range parts {
		if i > 0 && cp.Number <= parts[i-1].Number {
			s.logf("MP_COMPLETE upload=%s key=%q part=%d -> rejected: %s (parts must be strictly ascending, unique)",
				uploadID, u.Key, cp.Number, ErrInvalidPartOrder.Error())
			return nil, fmt.Errorf("%w: parts not strictly ascending at index %d", ErrInvalidPartOrder, i)
		}
		p, ok := u.Parts[cp.Number]
		if !ok {
			s.logf("MP_COMPLETE upload=%s key=%q part=%d -> rejected: %s",
				uploadID, u.Key, cp.Number, ErrInvalidPartNumber.Error())
			return nil, fmt.Errorf("%w: part %d was never uploaded", ErrInvalidPartNumber, cp.Number)
		}
		if cp.Number < 1 || cp.Number > s.cfg.MaxParts {
			return nil, fmt.Errorf("%w: part %d", ErrInvalidPartNumber, cp.Number)
		}
		isLast := i == len(parts)-1
		if !isLast && p.Size < s.cfg.MinPartSize {
			s.logf("MP_COMPLETE upload=%s key=%q part=%d size=%d -> rejected: %s",
				uploadID, u.Key, cp.Number, p.Size, ErrPartTooSmall.Error())
			return nil, fmt.Errorf("%w: part %d size %d < min %d", ErrPartTooSmall, cp.Number, p.Size, s.cfg.MinPartSize)
		}
		assembled = append(assembled, meta.Part{Number: cp.Number, Checksum: p.Checksum, Size: p.Size})
		total += p.Size
	}

	v := &meta.Version{
		ID:        newID("v-"),
		Key:       u.Key,
		Size:      total,
		Checksum:  compositeChecksum(assembled),
		Parts:     assembled,
		CreatedAt: time.Now().UTC(),
	}
	// Transition sequence: version visible, then upload slot removed. Both are
	// WAL records; a crash in between is harmless (the upload remains abortable
	// and its blocks remain referenced, possibly redundantly).
	// Single atomic commit: version becomes visible AND the upload slot is
	// removed in one WAL record. A repeated Complete after this returns
	// ErrUploadNotFound (the slot no longer exists).
	if err := s.appendLocked(meta.NewPutVersionRecord(v, uploadID)); err != nil {
		return nil, err
	}
	s.logf("MP_COMPLETE upload=%s key=%q version=%s parts=%d total_size=%d -> committed atomically (upload slot closed)",
		uploadID, u.Key, v.ID, len(assembled), total)
	return s.versionInfoLocked(v, true), nil
}

// AbortMultipart aborts an upload; all its parts become reclaimable.
// Aborting an unknown/completed upload returns ErrUploadNotFound.
func (s *Store) AbortMultipart(ctx context.Context, uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := s.st.Uploads[uploadID]
	if u == nil {
		return fmt.Errorf("%w: %s", ErrUploadNotFound, uploadID)
	}
	partCount := len(u.Parts)
	key := u.Key
	if err := s.appendLocked(meta.NewUploadAbortRecord(uploadID)); err != nil {
		return err
	}
	s.logf("MP_ABORT upload=%s key=%q parts=%d -> aborted (blocks unreferenced, gc candidates)",
		uploadID, key, partCount)
	return nil
}

// compositeChecksum deterministically names a multipart object from its parts.
// (Hex of SHA-256 over the concatenation of part checksums and sizes; stored
// versions are read part-by-part so this is an identity, not a content hash.)
func compositeChecksum(parts []meta.Part) string {
	h := newChecksumHasher()
	for _, p := range parts {
		fmt.Fprintf(h, "%d:%s:%d;", p.Number, p.Checksum, p.Size)
	}
	return h.Sum()
}
