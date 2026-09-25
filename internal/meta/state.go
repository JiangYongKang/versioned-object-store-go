package meta

import "fmt"

// State is the in-memory metadata reconstructed by replaying the WAL.
// All access must be serialized by the caller (store.mu).
type State struct {
	// versions maps key -> versionID -> Version (includes deleted history).
	Versions map[string]map[string]*Version
	// current maps key -> current version ID ("" means a delete marker).
	Current map[string]string
	// deleted maps key -> set of explicitly removed version IDs.
	Deleted map[string]map[string]bool
	// deletedAt records when the delete marker for key was installed.
	DeletedAt map[string]string
	// uploads maps uploadID -> Upload.
	Uploads map[string]*Upload
}

// NewState returns an empty state.
func NewState() *State {
	return &State{
		Versions:  map[string]map[string]*Version{},
		Current:   map[string]string{},
		Deleted:   map[string]map[string]bool{},
		DeletedAt: map[string]string{},
		Uploads:   map[string]*Upload{},
	}
}

// Apply mutates the state by replaying one record; it returns an error if the
// record is malformed or inconsistent with previously replayed records.
func (st *State) Apply(r *Record) error {
	if err := r.Validate(); err != nil {
		return err
	}
	switch r.Kind {
	case KindPutVersion:
		v := r.Version
		byKey := st.Versions[v.Key]
		if byKey == nil {
			byKey = map[string]*Version{}
			st.Versions[v.Key] = byKey
		}
		if _, exists := byKey[v.ID]; exists {
			return fmt.Errorf("%w: duplicate version id %s for key %s", ErrInconsistent, v.ID, v.Key)
		}
		byKey[v.ID] = v
		st.Current[v.Key] = v.ID
		// A new version supersedes any delete marker; the version itself is
		// individually addressable unless explicitly deleted.
		delete(st.DeletedAt, v.Key)
		// A multipart completion closes its upload slot in the SAME record:
		// either both the version is visible and the slot is gone, or neither.
		if r.UploadID != "" {
			if _, ok := st.Uploads[r.UploadID]; !ok {
				return fmt.Errorf("%w: put_version closes unknown upload %s", ErrInconsistent, r.UploadID)
			}
			delete(st.Uploads, r.UploadID)
		}

	case KindDeleteVersion:
		byKey := st.Versions[r.Key]
		v, ok := byKey[r.VersionID]
		if !ok {
			return fmt.Errorf("%w: delete_version unknown %s/%s", ErrInconsistent, r.Key, r.VersionID)
		}
		_ = v
		if st.Deleted[r.Key] == nil {
			st.Deleted[r.Key] = map[string]bool{}
		}
		st.Deleted[r.Key][r.VersionID] = true
		if st.Current[r.Key] == r.VersionID {
			st.Current[r.Key] = ""
			st.DeletedAt[r.Key] = r.VersionID
		}

	case KindDeleteMarker:
		st.Current[r.Key] = ""
		st.DeletedAt[r.Key] = "@"

	case KindUploadBegin:
		if _, exists := st.Uploads[r.UploadID]; exists {
			return fmt.Errorf("%w: duplicate upload id %s", ErrInconsistent, r.UploadID)
		}
		st.Uploads[r.UploadID] = &Upload{
			ID:        r.UploadID,
			Key:       r.Key,
			CreatedAt: r.CreatedAt,
			Parts:     map[int]UploadPart{},
		}

	case KindUploadPart:
		u := st.Uploads[r.UploadID]
		if u == nil {
			return fmt.Errorf("%w: upload_part unknown upload %s", ErrInconsistent, r.UploadID)
		}
		u.Parts[r.PartNumber] = UploadPart{Checksum: r.Checksum, Size: r.PartSize}

	case KindUploadAbort, KindUploadDone:
		if _, ok := st.Uploads[r.UploadID]; !ok {
			return fmt.Errorf("%w: %s unknown upload %s", ErrInconsistent, r.Kind, r.UploadID)
		}
		delete(st.Uploads, r.UploadID)

	default:
		return fmt.Errorf("%w: unknown kind %q", ErrInconsistent, r.Kind)
	}
	return nil
}

// CurrentVersion returns the current version of key, or nil when the key has no
// current version (never existed, or delete marker installed).
func (st *State) CurrentVersion(key string) *Version {
	id, ok := st.Current[key]
	if !ok || id == "" {
		return nil
	}
	return st.Versions[key][id]
}

// IsDeleted reports whether the given version ID was explicitly removed.
func (st *State) IsDeleted(key, versionID string) bool {
	return st.Deleted[key] != nil && st.Deleted[key][versionID]
}
