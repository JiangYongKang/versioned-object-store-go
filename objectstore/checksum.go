package objectstore

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
)

// checksumHasher builds deterministic composite identifiers for multipart
// versions from their ordered part descriptors. The result names the assembly
// (ordered part checksums and sizes); object bytes are always read back
// part-by-part, so it is an assembly identity rather than a full-content hash.
type checksumHasher struct{ h hash.Hash }

func newChecksumHasher() *checksumHasher { return &checksumHasher{h: sha256.New()} }

func (c *checksumHasher) Write(p []byte) (int, error) { return c.h.Write(p) }

func (c *checksumHasher) Sum() string { return hex.EncodeToString(c.h.Sum(nil)) }
