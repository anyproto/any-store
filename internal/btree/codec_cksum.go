package btree

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/zeebo/xxh3"
)

// cksumOverhead is the cksumCodec's per-page reserved-trailer size. Equal to
// the XXH3-128 output size (16 bytes), which also satisfies the codec
// contract's 16-byte alignment requirement.
const cksumOverhead = 16

// cksumCodec is a no-encrypt Codec that stamps an XXH3-128 hash into the
// page trailer and verifies it on read. Conceptually mirrors SQLite's
// cksumvfs (https://sqlite.org/cksumvfs.html), but with XXH3 instead of
// Fletcher and a 16-byte trailer instead of 8 (for AES-block alignment
// with the AEAD codecs).
//
// Trailer layout (last 16 bytes of every page):
//
//	[pageSize-16 .. pageSize-8]  Lo  uint64 (XXH3.Lo, little-endian)
//	[pageSize-8  .. pageSize  ]  Hi  uint64 (XXH3.Hi, little-endian)
//
// Hash input is page[start..pageSize-16], where start is 0 for pages 2+
// and dbHeaderSize (100) for page 1 (because encryptPageWithCodec keeps
// the page-1 DB header as a plaintext prefix). The codec sees only the
// post-prefix slice; the page-1 DB header is therefore NOT covered by
// the per-page checksum. SQLite-format invariants in the header are
// validated separately by dbHeader.deserialize at open time.
type cksumCodec struct {
	// verify is 1 when verify-on-read is on. When 0, mismatches are
	// detected (and OnError still fires) but the read returns the
	// page bytes instead of ErrCodecTamper. Mirrors PRAGMA
	// checksum_verification.
	verify atomic.Uint32
	// onError, if non-nil, is invoked on every mismatch regardless of
	// verify state. atomic.Pointer so callers can swap atomically.
	onError atomic.Pointer[func(uint32, error)]
}

// newCksumCodec returns a cksumCodec with verify-on-read enabled.
func newCksumCodec() *cksumCodec {
	c := &cksumCodec{}
	c.verify.Store(1)
	return c
}

func (c *cksumCodec) Overhead() int { return cksumOverhead }

// SetVerify toggles verify-on-read at runtime. Mirror of PRAGMA
// checksum_verification = ON/OFF.
func (c *cksumCodec) SetVerify(on bool) {
	if on {
		c.verify.Store(1)
	} else {
		c.verify.Store(0)
	}
}

// VerifyOn reports the current verify-on-read state.
func (c *cksumCodec) VerifyOn() bool { return c.verify.Load() == 1 }

// SetOnError installs a callback fired on every checksum mismatch.
// Pass nil to clear. The callback runs on the I/O goroutine and must
// not block; route through a buffered channel + drainer for retention.
func (c *cksumCodec) SetOnError(fn func(pgno uint32, inner error)) {
	if fn == nil {
		c.onError.Store(nil)
		return
	}
	c.onError.Store(&fn)
}

func (c *cksumCodec) fireOnError(pgno uint32, inner error) {
	if cb := c.onError.Load(); cb != nil {
		(*cb)(pgno, inner)
	}
}

// Encrypt copies src→dst and writes the XXH3-128 of dst[:len(src)-16]
// into the trailing 16 bytes of dst.
func (c *cksumCodec) Encrypt(dst, src []byte, pgno uint32, _ *aeadScratch) ([]byte, error) {
	if len(dst) < len(src) {
		panic("btree: cksumCodec.Encrypt: dst too short")
	}
	if len(src) < cksumOverhead+8 {
		panic("btree: cksumCodec.Encrypt: page too small for trailer")
	}
	bodyEnd := len(src) - cksumOverhead
	copy(dst[:bodyEnd], src[:bodyEnd])
	h := xxh3.Hash128(dst[:bodyEnd])
	putUint128LE(dst[bodyEnd:bodyEnd+cksumOverhead], h)
	return dst[:len(src)], nil
}

// Decrypt verifies the trailer hash. On mismatch fires OnError; if
// verify-on is enabled, returns ErrCodecTamper. dst receives src verbatim
// (this codec doesn't transform plaintext).
func (c *cksumCodec) Decrypt(dst, src []byte, pgno uint32, _ *aeadScratch) ([]byte, error) {
	if len(dst) < len(src) {
		panic("btree: cksumCodec.Decrypt: dst too short")
	}
	if len(src) < cksumOverhead+8 {
		panic("btree: cksumCodec.Decrypt: page too small for trailer")
	}
	bodyEnd := len(src) - cksumOverhead
	want := xxh3.Hash128(src[:bodyEnd])
	var got xxh3.Uint128
	got.Lo = binary.LittleEndian.Uint64(src[bodyEnd : bodyEnd+8])
	got.Hi = binary.LittleEndian.Uint64(src[bodyEnd+8 : bodyEnd+16])
	if want != got {
		c.fireOnError(pgno, ErrCodecTamper)
		if c.VerifyOn() {
			return nil, ErrCodecTamper
		}
	}
	copy(dst, src)
	return dst[:len(src)], nil
}

// putUint128LE writes h.Lo (8 bytes) followed by h.Hi (8 bytes) in
// little-endian into out[0:16].
func putUint128LE(out []byte, h xxh3.Uint128) {
	binary.LittleEndian.PutUint64(out[0:8], h.Lo)
	binary.LittleEndian.PutUint64(out[8:16], h.Hi)
}

// VerifyPageChecksum returns true iff page[len(page)-16:] equals the
// XXH3-128 of page[:len(page)-16]. Returns false for invalid page sizes
// (must be a power of two in [32, 65536]). Public; mirrors SQLite's
// verify_checksum() SQL function.
//
// Note: for a page-1 buffer read directly from disk, this hashes the
// entire prefix [0..len-16], which differs from the codec's runtime
// range [100..len-16]. For codec-equivalent verification (respecting
// the page-1 DB-header plaintext prefix), use db.VerifyIntegrity.
func VerifyPageChecksum(page []byte) bool {
	n := len(page)
	if n < cksumOverhead+8 || n > 65536 || (n&(n-1)) != 0 {
		return false
	}
	want := xxh3.Hash128(page[:n-cksumOverhead])
	var got xxh3.Uint128
	got.Lo = binary.LittleEndian.Uint64(page[n-16 : n-8])
	got.Hi = binary.LittleEndian.Uint64(page[n-8 : n])
	return want == got
}

// StampPageChecksum writes a valid trailer into page[len-16:]. Test/migration
// helper; production code goes through cksumCodec.Encrypt.
func StampPageChecksum(page []byte) {
	n := len(page)
	if n < cksumOverhead+8 {
		panic("btree: StampPageChecksum: page too small")
	}
	h := xxh3.Hash128(page[:n-cksumOverhead])
	putUint128LE(page[n-cksumOverhead:n], h)
}
