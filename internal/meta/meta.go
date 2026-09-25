// Package meta holds the metadata state model reconstructed from the WAL.
//
// Every state transition is one WAL record. Records are designed so that
// replaying them is deterministic and idempotent-ish only via the ordering
// written by the store: each record asserts the precondition the store checked
// before appending it, and replay rejects any record that contradicts the
// state built so far, which indicates WAL corruption.
package meta

import (
	"errors"
	"fmt"
	"time"
)

// Record kinds.
const (
	KindPutVersion    = "put_version"    // put/complete committed a new version
	KindDeleteVersion = "delete_version" // one specific version removed
	KindDeleteMarker  = "delete_marker"  // current version of key replaced by marker
	KindUploadBegin   = "upload_begin"   // multipart upload created
	KindUploadPart    = "upload_part"    // one part uploaded / replaced
	KindUploadAbort   = "upload_abort"   // multipart upload aborted
	KindUploadDone    = "upload_done"    // multipart upload completed & removed
)

// ErrInconsistent is returned when a replayed record contradicts the state.
var ErrInconsistent = errors.New("meta: inconsistent record")

// Version is one immutable object version.
type Version struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	Size      int64     `json:"size"`
	Checksum  string    `json:"checksum"`
	Parts     []Part    `json:"parts,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Part references one block within a multipart version.
type Part struct {
	Number   int    `json:"number"`
	Checksum string `json:"checksum"`
	Size     int64  `json:"size"`
}

// Upload is an in-progress multipart upload.
type Upload struct {
	ID        string             `json:"id"`
	Key       string             `json:"key"`
	CreatedAt time.Time          `json:"created_at"`
	Parts     map[int]UploadPart `json:"parts"`
}

// UploadPart is one uploaded (but not yet committed) part.
type UploadPart struct {
	Checksum string `json:"checksum"`
	Size     int64  `json:"size"`
}

// Record is a WAL-serializable metadata mutation. Exactly one mutation is
// carried per record; Kind selects which fields are meaningful.
type Record struct {
	Kind string `json:"kind"`

	// Common selectors.
	Key      string `json:"key,omitempty"`
	UploadID string `json:"upload_id,omitempty"`

	// KindPutVersion: when UploadID is set, the record also atomically closes
	// that in-progress multipart upload (single-record commit).
	Version *Version `json:"version,omitempty"`

	// KindDeleteVersion / KindDeleteMarker
	VersionID string `json:"version_id,omitempty"`

	// KindUploadBegin
	CreatedAt time.Time `json:"created_at,omitempty"`

	// KindUploadPart
	PartNumber int    `json:"part_number,omitempty"`
	Checksum   string `json:"checksum,omitempty"`
	PartSize   int64  `json:"part_size,omitempty"`
}

// NewPutVersionRecord builds a KindPutVersion record. uploadID (when non-empty)
// makes the record also atomically close a multipart upload.
func NewPutVersionRecord(v *Version, uploadID string) *Record {
	return &Record{Kind: KindPutVersion, Version: v, UploadID: uploadID}
}

// NewDeleteVersionRecord builds a KindDeleteVersion record.
func NewDeleteVersionRecord(key, versionID string) *Record {
	return &Record{Kind: KindDeleteVersion, Key: key, VersionID: versionID}
}

// NewDeleteMarkerRecord builds a KindDeleteMarker record.
func NewDeleteMarkerRecord(key string, at time.Time) *Record {
	return &Record{Kind: KindDeleteMarker, Key: key, CreatedAt: at}
}

// NewUploadBeginRecord builds a KindUploadBegin record.
func NewUploadBeginRecord(uploadID, key string, at time.Time) *Record {
	return &Record{Kind: KindUploadBegin, UploadID: uploadID, Key: key, CreatedAt: at}
}

// NewUploadPartRecord builds a KindUploadPart record.
func NewUploadPartRecord(uploadID string, partNumber int, checksum string, size int64) *Record {
	return &Record{Kind: KindUploadPart, UploadID: uploadID, PartNumber: partNumber, Checksum: checksum, PartSize: size}
}

// NewUploadAbortRecord builds a KindUploadAbort record.
func NewUploadAbortRecord(uploadID string) *Record {
	return &Record{Kind: KindUploadAbort, UploadID: uploadID}
}

// NewUploadDoneRecord builds a KindUploadDone record.
func NewUploadDoneRecord(uploadID string) *Record {
	return &Record{Kind: KindUploadDone, UploadID: uploadID}
}

// Validate performs cheap field-level validation before append/replay.
func (r *Record) Validate() error {
	if r.Kind == "" {
		return fmt.Errorf("%w: empty kind", ErrInconsistent)
	}
	switch r.Kind {
	case KindPutVersion:
		if r.Version == nil || r.Version.ID == "" || r.Version.Key == "" {
			return fmt.Errorf("%w: put_version missing version", ErrInconsistent)
		}
	case KindDeleteVersion:
		if r.Key == "" || r.VersionID == "" {
			return fmt.Errorf("%w: delete_version missing selectors", ErrInconsistent)
		}
	case KindDeleteMarker:
		if r.Key == "" {
			return fmt.Errorf("%w: delete_marker missing key", ErrInconsistent)
		}
	case KindUploadBegin:
		if r.UploadID == "" || r.Key == "" {
			return fmt.Errorf("%w: upload_begin missing fields", ErrInconsistent)
		}
	case KindUploadPart:
		if r.UploadID == "" || r.PartNumber <= 0 || r.Checksum == "" {
			return fmt.Errorf("%w: upload_part missing fields", ErrInconsistent)
		}
	case KindUploadAbort, KindUploadDone:
		if r.UploadID == "" {
			return fmt.Errorf("%w: %s missing upload id", ErrInconsistent, r.Kind)
		}
	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInconsistent, r.Kind)
	}
	return nil
}
