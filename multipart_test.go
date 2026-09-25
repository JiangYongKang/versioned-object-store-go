package objectstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMultipartOutOfOrderAndDuplicateParts(t *testing.T) {
	s := openTestStore(t)
	id, err := s.CreateUpload("mp")
	if err != nil {
		t.Fatalf("create upload: %v", err)
	}
	// 乱序到达。
	if _, err := s.UploadPart(id, 3, bytes.NewReader([]byte("ccc"))); err != nil {
		t.Fatalf("part 3: %v", err)
	}
	if _, err := s.UploadPart(id, 1, bytes.NewReader([]byte("aaa"))); err != nil {
		t.Fatalf("part 1: %v", err)
	}
	if _, err := s.UploadPart(id, 2, bytes.NewReader([]byte("bbb"))); err != nil {
		t.Fatalf("part 2: %v", err)
	}
	// 重复上传同一编号：幂等覆盖。
	if _, err := s.UploadPart(id, 2, bytes.NewReader([]byte("BBB"))); err != nil {
		t.Fatalf("re-upload part 2: %v", err)
	}
	info, err := s.ListParts(id)
	if err != nil {
		t.Fatalf("list parts: %v", err)
	}
	if len(info.Parts) != 3 || info.Parts[0].PartNumber != 1 || info.Parts[2].PartNumber != 3 {
		t.Fatalf("parts = %+v", info.Parts)
	}
	vi, err := s.CompleteUpload(id, []int{1, 2, 3})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	r, _, err := s.Get("mp")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got := getString(t, r); got != "aaaBBBccc" {
		t.Fatalf("assembled content = %q, want %q", got, "aaaBBBccc")
	}
	// 重复完成：幂等返回首次完成的版本。
	vi2, err := s.CompleteUpload(id, []int{1, 2, 3})
	if err != nil {
		t.Fatalf("re-complete: %v", err)
	}
	if vi2.VersionID != vi.VersionID {
		t.Fatalf("idempotent complete returned %s, want %s", vi2.VersionID, vi.VersionID)
	}
	vs, _ := s.ListVersions("mp")
	if len(vs) != 1 {
		t.Fatalf("versions = %d, want 1 (idempotent complete must not add versions)", len(vs))
	}
	// 完成后分片目录已回收。
	if _, err := os.Stat(filepath.Join(s.uploadsDir(), id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("upload dir should be removed after complete, stat err = %v", err)
	}
}

func TestMultipartRejections(t *testing.T) {
	s := openTestStore(t)
	id, _ := s.CreateUpload("mp")

	// 越界分片。
	if _, err := s.UploadPart(id, 0, bytes.NewReader([]byte("x"))); !errors.Is(err, ErrPartNumberInvalid) {
		t.Fatalf("part 0 err = %v, want ErrPartNumberInvalid", err)
	}
	if _, err := s.UploadPart(id, s.cfg.MaxParts+1, bytes.NewReader([]byte("x"))); !errors.Is(err, ErrPartNumberInvalid) {
		t.Fatalf("part max+1 err = %v, want ErrPartNumberInvalid", err)
	}
	if _, err := s.UploadPart(id, 1, bytes.NewReader([]byte("a"))); err != nil {
		t.Fatalf("part 1: %v", err)
	}
	// 完成时引用未上传分片。
	if _, err := s.CompleteUpload(id, []int{1, 2}); !errors.Is(err, ErrPartMissing) {
		t.Fatalf("complete with missing part err = %v, want ErrPartMissing", err)
	}
	// 完成时越界编号。
	if _, err := s.CompleteUpload(id, []int{1, 0}); !errors.Is(err, ErrPartNumberInvalid) {
		t.Fatalf("complete with part 0 err = %v, want ErrPartNumberInvalid", err)
	}
	// 未知会话。
	if _, err := s.UploadPart("u99999999", 1, bytes.NewReader([]byte("x"))); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("unknown upload err = %v, want ErrUploadNotFound", err)
	}
	if _, err := s.CompleteUpload("u99999999", []int{1}); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("complete unknown err = %v, want ErrUploadNotFound", err)
	}
	if err := s.AbortUpload("u99999999"); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("abort unknown err = %v, want ErrUploadNotFound", err)
	}

	// 正常完成后：继续传分片、再中止均被拒绝。
	if _, err := s.UploadPart(id, 2, bytes.NewReader([]byte("b"))); err != nil {
		t.Fatalf("part 2: %v", err)
	}
	if _, err := s.CompleteUpload(id, []int{1, 2}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := s.UploadPart(id, 3, bytes.NewReader([]byte("c"))); !errors.Is(err, ErrUploadCompleted) {
		t.Fatalf("part after complete err = %v, want ErrUploadCompleted", err)
	}
	if err := s.AbortUpload(id); !errors.Is(err, ErrUploadCompleted) {
		t.Fatalf("abort after complete err = %v, want ErrUploadCompleted", err)
	}
}

func TestAbortLifecycle(t *testing.T) {
	s := openTestStore(t)
	id, _ := s.CreateUpload("gone")
	if _, err := s.UploadPart(id, 1, bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("part: %v", err)
	}
	if err := s.AbortUpload(id); err != nil {
		t.Fatalf("abort: %v", err)
	}
	// 重复中止幂等成功。
	if err := s.AbortUpload(id); err != nil {
		t.Fatalf("re-abort: %v", err)
	}
	// 中止后传分片与完成均被拒绝。
	if _, err := s.UploadPart(id, 2, bytes.NewReader([]byte("x"))); !errors.Is(err, ErrUploadAborted) {
		t.Fatalf("part after abort err = %v, want ErrUploadAborted", err)
	}
	if _, err := s.CompleteUpload(id, []int{1}); !errors.Is(err, ErrUploadAborted) {
		t.Fatalf("complete after abort err = %v, want ErrUploadAborted", err)
	}
	// 中止的上传不产生可见对象。
	if _, _, err := s.Get("gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get aborted key err = %v, want ErrNotFound", err)
	}
	if _, err := s.ListVersions("gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("list aborted key err = %v, want ErrNotFound", err)
	}
	// 不留下残留分片。
	if _, err := os.Stat(filepath.Join(s.uploadsDir(), id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("upload dir should be removed after abort, stat err = %v", err)
	}
}
