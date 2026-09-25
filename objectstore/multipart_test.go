package objectstore

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func partBody(n int) string {
	b := strings.Builder{}
	b.Grow(n)
	for i := 0; i < n; i++ {
		b.WriteByte(byte('a' + i%26))
	}
	return b.String()
}

func TestMultipartHappyPathOutOfOrder(t *testing.T) {
	s := testStore(t) // MinPartSize=4, MaxParts=10
	ctx := context.Background()

	up, err := s.CreateMultipart(ctx, "big")
	if err != nil {
		t.Fatal(err)
	}
	// Upload out of order: 2, then 1.
	r2, err := s.UploadPart(ctx, up, 2, strings.NewReader("EFGH"), 4)
	if err != nil {
		t.Fatalf("part2: %v", err)
	}
	r1, err := s.UploadPart(ctx, up, 1, strings.NewReader("ABCD"), 4)
	if err != nil {
		t.Fatalf("part1: %v", err)
	}

	_, err = s.CompleteMultipart(ctx, up, []CompletedPart{{1}, {2}})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	rc, got, err := s.Get(ctx, "big", "")
	if err != nil {
		t.Fatal(err)
	}
	if body := readAll(t, rc); body != "ABCDEFGH" {
		t.Fatalf("body=%q", body)
	}
	if got.Size != r1.Size+r2.Size {
		t.Fatalf("size=%d", got.Size)
	}
	if !got.Current {
		t.Fatal("completed multipart must be current")
	}
}

func TestMultipartPartValidation(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	up, err := s.CreateMultipart(ctx, "big")
	if err != nil {
		t.Fatal(err)
	}

	// Out of range part numbers.
	if _, err := s.UploadPart(ctx, up, 0, strings.NewReader("abcd"), 4); !errors.Is(err, ErrInvalidPartNumber) {
		t.Fatalf("part 0 err=%v", err)
	}
	if _, err := s.UploadPart(ctx, up, 11, strings.NewReader("abcd"), 4); !errors.Is(err, ErrInvalidPartNumber) {
		t.Fatalf("part 11 err=%v", err)
	}

	if _, err := s.UploadPart(ctx, up, 1, strings.NewReader("abcd"), 4); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UploadPart(ctx, up, 2, strings.NewReader("ef"), 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UploadPart(ctx, up, 3, strings.NewReader("ghij"), 4); err != nil {
		t.Fatal(err)
	}

	// Not ascending / duplicated in complete list.
	_, err = s.CompleteMultipart(ctx, up, []CompletedPart{{1}, {3}, {2}})
	if !errors.Is(err, ErrInvalidPartOrder) {
		t.Fatalf("unordered err=%v", err)
	}
	_, err = s.CompleteMultipart(ctx, up, []CompletedPart{{1}, {1}})
	if !errors.Is(err, ErrInvalidPartOrder) {
		t.Fatalf("duplicate err=%v", err)
	}

	// Non-final part too small.
	_, err = s.CompleteMultipart(ctx, up, []CompletedPart{{2}, {3}})
	if !errors.Is(err, ErrPartTooSmall) {
		t.Fatalf("small part err=%v", err)
	}

	// Referencing an uploaded? part 99 never uploaded.
	_, err = s.CompleteMultipart(ctx, up, []CompletedPart{{1}, {99}})
	if !errors.Is(err, ErrInvalidPartNumber) {
		t.Fatalf("missing part err=%v", err)
	}

	// Last part allowed smaller: should succeed.
	if _, err := s.CompleteMultipart(ctx, up, []CompletedPart{{1}, {2}}); err != nil {
		t.Fatalf("complete with small last part: %v", err)
	}
}

func TestMultipartDuplicatePartReplaces(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	up, err := s.CreateMultipart(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	p1a, err := s.UploadPart(ctx, up, 1, strings.NewReader("abcd"), 4)
	if err != nil {
		t.Fatal(err)
	}
	p1b, err := s.UploadPart(ctx, up, 1, strings.NewReader("wxyz"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if p1a.Checksum == p1b.Checksum {
		t.Fatal("replacement part checksum should differ")
	}
	p2, _ := s.UploadPart(ctx, up, 2, strings.NewReader("zz"), 2)
	if _, err := s.CompleteMultipart(ctx, up, []CompletedPart{{1}, {2}}); err != nil {
		t.Fatal(err)
	}
	rc, _, err := s.Get(ctx, "k", "")
	if err != nil {
		t.Fatal(err)
	}
	if body := readAll(t, rc); body != "wxyzzz" {
		t.Fatalf("body=%q (replacement part must win)", body)
	}
	_ = p2

	// Old block from the replaced part is now unreferenced: GC must reclaim it
	// without harming the visible object.
	removed, err := s.RunGC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if removed == 0 {
		t.Fatal("expected GC to remove replaced-part block")
	}
	rc, _, err = s.Get(ctx, "k", "")
	if err != nil {
		t.Fatalf("object broken after GC: %v", err)
	}
	if body := readAll(t, rc); body != "wxyzzz" {
		t.Fatalf("body changed after GC: %q", body)
	}
}

func TestMultipartCompleteAbortIdempotency(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	up, _ := s.CreateMultipart(ctx, "k")
	if _, err := s.UploadPart(ctx, up, 1, strings.NewReader("abcd"), 4); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteMultipart(ctx, up, []CompletedPart{{1}}); err != nil {
		t.Fatal(err)
	}
	// Repeated complete: upload slot gone -> distinguishable error.
	if _, err := s.CompleteMultipart(ctx, up, []CompletedPart{{1}}); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("repeat complete err=%v, want ErrUploadNotFound", err)
	}
	// Repeated/racing abort on completed upload: same reason, no side effects.
	if err := s.AbortMultipart(ctx, up); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("abort completed err=%v", err)
	}

	// Abort then repeat abort: explicit rejection (not silent success).
	up2, _ := s.CreateMultipart(ctx, "k2")
	// Distinct content: this block is referenced nowhere after abort.
	if _, err := s.UploadPart(ctx, up2, 1, strings.NewReader("mnop"), 4); err != nil {
		t.Fatal(err)
	}
	if err := s.AbortMultipart(ctx, up2); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if err := s.AbortMultipart(ctx, up2); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("repeat abort err=%v", err)
	}
	// Completing an aborted upload is rejected too.
	if _, err := s.CompleteMultipart(ctx, up2, []CompletedPart{{1}}); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("complete aborted err=%v", err)
	}
	// Aborted upload must never have produced a visible object.
	if _, _, err := s.Get(ctx, "k2", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("aborted upload visible: %v", err)
	}
	// Its part blocks are reclaimed by GC.
	removed, err := s.RunGC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if removed == 0 {
		t.Fatal("aborted part blocks should be collectible")
	}
}

func TestPartUploadOnUnknownUpload(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	_, err := s.UploadPart(ctx, "mp-nope", 1, strings.NewReader("abcd"), 4)
	if !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("err=%v", err)
	}
}
