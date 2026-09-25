// Command demo exercises the versioned object store end to end on local disk:
// conditional writes, version history, multipart upload lifecycle, abort,
// restart recovery and garbage collection. No external services are used.
//
//	go run ./cmd/demo -root /tmp/vos-demo
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/highcumontoa/versioned-object-store-go/objectstore"
)

func main() {
	root := flag.String("root", "./.demo-data", "storage root directory")
	flag.Parse()

	cfg := objectstore.DefaultConfig(*root)
	cfg.MinPartSize = 16 // tiny, so the demo is self-contained
	cfg.GCInterval = 0   // manual GC in the demo
	cfg.GCUnrefAge = time.Millisecond

	s, err := objectstore.New(cfg)
	must("open store", err)
	defer func() { _ = s.Close() }()

	ctx := context.Background()

	// 1) Unconditional versions.
	v1 := mustPut(ctx, s, "greeting", "hello v1", nil)
	v2 := mustPut(ctx, s, "greeting", "hello v2 -- current", nil)
	mustGet(ctx, s, "greeting", "")
	mustGet(ctx, s, "greeting", v1.VersionID) // history still addressable
	_ = v2

	// 2) Conditional writes: distinguishable reasons.
	ifNone := true
	_, err = s.Put(ctx, "greeting", strings.NewReader("nope"), 4,
		&objectstore.PutOptions{IfNoneMatch: &ifNone})
	fmt.Printf("demo: IfNoneMatch on existing -> %v (code=%s)\n", err, objectstore.CodeOf(err))
	good := v2.Checksum
	v3 := mustPut(ctx, s, "greeting", "hello v3 via IfMatch",
		&objectstore.PutOptions{IfMatch: &good})
	_ = v3

	// 3) Multipart: out-of-order parts, duplicate part number replacement.
	up, err := s.CreateMultipart(ctx, "multi/object")
	must("create multipart", err)
	_, err = s.UploadPart(ctx, up, 2, strings.NewReader("tail"), 4) // final part: may be small
	must("upload part 2", err)
	_, err = s.UploadPart(ctx, up, 1, strings.NewReader("first-part-v1!!!"), 16)
	must("upload part 1", err)
	_, err = s.UploadPart(ctx, up, 1, strings.NewReader("first-part-v2!!!"), 16) // replace
	must("replace part 1", err)
	mv, err := s.CompleteMultipart(ctx, up, []objectstore.CompletedPart{{Number: 1}, {Number: 2}})
	must("complete multipart", err)
	mustGetInfo(ctx, s, "multi/object", mv.VersionID)

	// Repeated complete is rejected with a distinguishable reason.
	_, err = s.CompleteMultipart(ctx, up, []objectstore.CompletedPart{{Number: 1}})
	fmt.Printf("demo: repeated complete -> %v (code=%s)\n", err, objectstore.CodeOf(err))

	// 4) Abort: no visible object, parts become garbage.
	up2, _ := s.CreateMultipart(ctx, "aborted/object")
	_, _ = s.UploadPart(ctx, up2, 1, strings.NewReader("will-be-aborted!!!"), 19)
	must("abort multipart", s.AbortMultipart(ctx, up2))
	err = s.AbortMultipart(ctx, up2)
	fmt.Printf("demo: repeated abort -> %v (code=%s)\n", err, objectstore.CodeOf(err))
	if _, _, err := s.Get(ctx, "aborted/object", ""); !errors.Is(err, objectstore.ErrNotFound) {
		panic("aborted upload must not be visible")
	}
	fmt.Println("demo: aborted upload is not visible: OK")

	// 5) Delete marker + explicit version removal.
	_, err = s.Delete(ctx, "greeting")
	must("delete current", err)
	_, _, err = s.Get(ctx, "greeting", "")
	fmt.Printf("demo: get after delete -> %v (code=%s)\n", err, objectstore.CodeOf(err))
	mustGetInfo(ctx, s, "greeting", v1.VersionID) // history survives marker
	must("delete version", s.DeleteVersion(ctx, "greeting", v1.VersionID))

	// 6) Garbage collection (manual).
	removed, err := s.RunGC(ctx)
	must("gc", err)
	fmt.Printf("demo: GC removed %d unreferenced block(s); visible objects untouched\n", removed)

	// 7) Restart recovery: reopen the same root and read surviving data.
	must("close", s.Close())
	s2, err := objectstore.New(cfg)
	must("reopen store (recovery)", err)
	defer func() { _ = s2.Close() }()
	rc, info, err := s2.Get(ctx, "multi/object", mv.VersionID)
	must("read multipart after restart", err)
	body, _ := io.ReadAll(rc)
	_ = rc.Close()
	fmt.Printf("demo: after restart multi/object version=%s size=%d body=%q\n",
		info.VersionID, info.Size, string(body))
	versions, err := s2.ListVersions(ctx, "greeting")
	must("list versions after restart", err)
	fmt.Printf("demo: greeting versions after restart=%d\n", len(versions))
}

func must(what string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "demo: %s failed: %v\n", what, err)
		os.Exit(1)
	}
}

func mustPut(ctx context.Context, s *objectstore.Store, key, body string,
	opt *objectstore.PutOptions) *objectstore.VersionInfo {
	info, err := s.Put(ctx, key, strings.NewReader(body), int64(len(body)), opt)
	must("put "+key, err)
	return info
}

func mustGet(ctx context.Context, s *objectstore.Store, key, version string) {
	rc, info, err := s.Get(ctx, key, version)
	must("get "+key, err)
	body, _ := io.ReadAll(rc)
	_ = rc.Close()
	fmt.Printf("demo: GET %q (version=%s current=%v) -> %q\n", key, info.VersionID, info.Current, string(body))
}

func mustGetInfo(ctx context.Context, s *objectstore.Store, key, version string) {
	mustGet(ctx, s, key, version)
}
