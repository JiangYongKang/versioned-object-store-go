package objectstore

import "errors"

// Sentinel errors: use errors.Is to distinguish failure reasons.
var (
	ErrNotFound           = newErrorCode("not_found")
	ErrVersionNotFound    = newErrorCode("version_not_found")
	ErrVersionDeleted     = newErrorCode("version_deleted")
	ErrPreconditionFailed = newErrorCode("precondition_failed")
	ErrConditionConflict  = newErrorCode("condition_conflict")
	ErrUploadNotFound     = newErrorCode("upload_not_found")
	ErrUploadCompleted    = newErrorCode("upload_completed")
	ErrInvalidPartNumber  = newErrorCode("invalid_part_number")
	ErrPartTooSmall       = newErrorCode("part_too_small")
	ErrInvalidPartOrder   = newErrorCode("invalid_part_order")
	ErrChecksumMismatch   = newErrorCode("checksum_mismatch")
	ErrCorruptedMetadata  = newErrorCode("corrupted_metadata")
	ErrCorruptedData      = newErrorCode("corrupted_data")
	ErrStorageLocked      = newErrorCode("storage_locked")
)

type codeError struct {
	code string
}

func newErrorCode(code string) error { return &codeError{code: code} }

func (e *codeError) Error() string { return "objectstore: " + e.code }

// CodeOf extracts the machine-readable reason code from an objectstore error,
// including errors wrapped with %w. Returns "" for unrelated errors.
func CodeOf(err error) string {
	var c *codeError
	if errors.As(err, &c) {
		return c.code
	}
	return ""
}
