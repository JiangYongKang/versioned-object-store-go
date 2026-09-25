package objectstore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// Concurrent unconditional puts of the same key must all commit distinct,
// fully readable versions: no overwrites lost, no half-written data.
func TestConcurrentPutsNoLostOrTornWrites(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	const n = 20

	var wg sync.WaitGroup
	errs := make(chan error, n)
	infos := make(chan *VersionInfo, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf("body-%02d-%s", i, strings.Repeat("q", 32))
			info, err := s.Put(ctx, "k", strings.NewReader(body), int64(len(body)), nil)
			if err != nil {
				errs <- err
				return
			}
			infos <- info
		}(i)
	}
	wg.Wait()
	close(errs)
	close(infos)
	for err := range errs {
		t.Fatalf("concurrent put: %v", err)
	}

	ids := map[string]bool{}
	for info := range infos {
		if ids[info.VersionID] {
			t.Fatalf("duplicate version id %s", info.VersionID)
		}
		ids[info.VersionID] = true
	}
	if len(ids) != n {
		t.Fatalf("committed=%d, want %d", len(ids), n)
	}

	versions, err := s.ListVersions(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != n {
		t.Fatalf("versions=%d, want %d (no concurrent overwrite)", len(versions), n)
	}

	// Every committed version reads back intact (no torn data).
	for _, vi := range versions {
		rc, info, err := s.Get(ctx, "k", vi.VersionID)
		if err != nil {
			t.Fatalf("Get %s: %v", vi.VersionID, err)
		}
		body := readAll(t, rc)
		if int64(len(body)) != info.Size || len(body) < 38 || body[:5] != "body-" {
			t.Fatalf("torn/garbled version %s: size=%d content=%q", vi.VersionID, info.Size, body)
		}
	}
}

// Only one of many If-None-Match:* writers may succeed; the rest get the
// distinguishable ErrConditionConflict, and nothing is half-written.
func TestConcurrentIfNoneMatchConflict(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	const n = 20
	star := true

	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted, conflicts := 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf("writer-%02d", i)
			_, err := s.Put(ctx, "only-one", strings.NewReader(body), int64(len(body)),
				&PutOptions{IfNoneMatch: &star})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				accepted++
			case isConditionConflict(err):
				conflicts++
			default:
				t.Errorf("unexpected err: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if accepted != 1 {
		t.Fatalf("accepted=%d, want 1", accepted)
	}
	if conflicts != n-1 {
		t.Fatalf("conflicts=%d, want %d", conflicts, n-1)
	}
	versions, err := s.ListVersions(ctx, "only-one")
	if err != nil || len(versions) != 1 {
		t.Fatalf("versions=%+v err=%v", versions, err)
	}
}

// Concurrent multipart completions of distinct uploads for the same key all
// produce distinct current versions; aborted uploads produce nothing.
func TestConcurrentMultiparts(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			up, err := s.CreateMultipart(ctx, "mkey")
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			body := fmt.Sprintf("part-%02d-xxxx", i)
			if _, err := s.UploadPart(ctx, up, 1, strings.NewReader(body), int64(len(body))); err != nil {
				t.Errorf("upload: %v", err)
				return
			}
			if i%2 == 0 {
				if _, err := s.CompleteMultipart(ctx, up, []CompletedPart{{1}}); err != nil {
					t.Errorf("complete: %v", err)
				}
			} else if err := s.AbortMultipart(ctx, up); err != nil {
				t.Errorf("abort: %v", err)
			}
		}(i)
	}
	wg.Wait()
	versions, err := s.ListVersions(ctx, "mkey")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != n/2 {
		t.Fatalf("committed versions=%d, want %d", len(versions), n/2)
	}
	for _, v := range versions {
		rc, _, err := s.Get(ctx, "mkey", v.VersionID)
		if err != nil {
			t.Fatal(err)
		}
		b := readAll(t, rc)
		if len(b) < 8 || b[:5] != "part-" {
			t.Fatalf("garbled multipart version: %q", b)
		}
	}
}
