package btree

// Page encryption in this package is an independent Go implementation that
// follows design conventions from SQLCipher (BSD-3-Clause, Zetetic LLC) —
// reserved-tail page overhead, the page-1 plaintext-prefix rule, and PBKDF2
// key derivation — using Go-native AEAD primitives. No SQLCipher code is
// included. Scope and attribution: NOTICE.md. Per-site provenance:
// docs/btree/mappings/sqlcipher_codec.json.

import (
	"crypto/rand"
	"fmt"
)

// Codec is the pluggable page-encryption interface. When a non-nil Codec is
// installed on the pager, every file/WAL I/O site routes through it.
// Implementations must be safe for concurrent use, Encrypt included: the
// pager shares one Codec across the writer and all readers, and one Codec
// object can serve several databases at once (Options.Codec is installed
// as given, so two databases opened with it, or a database and its Backup
// destination, which opens with the source's Options, encrypt through it
// from their own writers at the same time). A codec therefore keeps no
// per-call mutable state and nothing a database sets (the one exception,
// cksumCodec's verify flag, is in a codec that is never shared): whatever
// an Encrypt or Decrypt call needs to write, the nonce source included,
// lives in the caller's aeadScratch, and Decrypt reports an integrity
// failure to the hook its caller passes in, the calling database's own
// Options.OnIntegrityError. SQLCipher, too, reports a page it cannot
// decrypt to the pager the call came from: each pager passes its own
// codec_ctx to every codec call (pPager->xCodec(pPager->pCodec, ...)), and
// outside recovery mode sqlite3Codec records the failure on that ctx and
// its pager (sqlcipher_codec_ctx_set_error → sqlite3pager_error;
// sqlcipher.c:3287-3291).
//
// Overhead() bytes are reserved at the end of every page for the codec's
// per-page metadata (IV/nonce, authentication tag, padding). The btree cell
// layout automatically respects this via (page_size - reserve_size).
//
// Encrypt/Decrypt operate on fixed-size page buffers: src is pageSize long,
// dst receives the transformed bytes (also pageSize). pgno is the 1-based
// page number, bound into the authentication tag so shuffling pages is
// detected.
type Codec interface {
	// Overhead returns the number of reserved bytes per page for codec
	// metadata (nonce + tag + padding). Must be constant for the codec's
	// lifetime and a multiple of 16 (AES block size) for alignment.
	Overhead() int

	// Encrypt transforms plaintext src (len == pageSize) into ciphertext
	// in dst, returning a slice of dst with the same length as src.
	// The last Overhead() bytes of dst contain the nonce+tag+padding.
	// pgno is bound into the authentication tag as associated data.
	// s is a caller-owned scratch holding the nonce/AAD buffers and the
	// nonce pool the nonce is drawn from; it must not be nil. See
	// aeadScratch for concurrency rules.
	Encrypt(dst, src []byte, pgno uint32, s *aeadScratch) ([]byte, error)

	// Decrypt is the inverse of Encrypt. On tag verification failure it
	// returns an error that wraps ErrPageIntegrity (without modifying dst
	// past any intermediate state). Callers must not use dst on error.
	// Every integrity failure it detects, including one it does not return
	// (cksumCodec with verification off), goes to onError with pgno and the
	// failure's detail; a nil onError reports nothing.
	Decrypt(dst, src []byte, pgno uint32, s *aeadScratch, onError integrityHook) ([]byte, error)
}

// integrityHook is the database's Options.OnIntegrityError as its pager and
// WAL hold it, passed to every Decrypt they make. It is per database: a
// codec never stores one, because a codec can serve several databases.
type integrityHook func(pgno uint32, inner error)

// fire calls h, if there is one.
func (h integrityHook) fire(pgno uint32, inner error) {
	if h != nil {
		h(pgno, inner)
	}
}

// aeadScratch is a caller-owned, per-call scratch for codec Encrypt/Decrypt.
// It holds the nonce and AAD buffers outside the codec so that they live
// on the caller's heap-allocated parent struct (pager / pcache / wal) and
// don't escape to the heap on every call. Sized for the largest built-in
// cipher: XChaCha20-Poly1305 uses a 24-byte nonce; AAD is always 4 bytes
// but the field is padded to 16 for alignment / future use.
//
// It also owns the nonce source, the noncePool Encrypt draws random nonces
// from. The pool cannot live in the codec: one codec object can be shared
// by several databases, each encrypting from its own writer (see Codec),
// and an unlocked pool drawn from by two of them hands out the same nonce
// twice under one key. Here it follows the rule the buffers above follow:
// one owner, one goroutine at a time. SQLCipher keeps no shared nonce state
// either: every pager gets its own codec_ctx built from the key
// (sqlcipher_codec_ctx_init; connections that share a shared-cache pager
// share its ctx, sqlcipher.c:3417-3435), and sqlcipher_page_cipher writes
// reserve_sz fresh random bytes into each page it encrypts, straight from
// the provider's random function (for OpenSSL the process-wide RAND_bytes,
// which the default build calls under SQLCipher's own global
// SQLCIPHER_MUTEX_PROVIDER_RAND, crypto_openssl.c:131).
// The pool here is an unlocked batch in front of crypto/rand, so it takes
// its caller's ownership instead of a lock. It is allocated on the first
// Encrypt, so a scratch that only ever decrypts (every reader's) never
// carries one.
//
// Not safe for concurrent use: each caller (the pager's writer side, the
// WAL's frame writer, each per-reader-tx pcache) owns one instance, the way
// each SQLCipher pager owns its codec_ctx and the page buffer in it.
// Must not be copied: a copy made after the first Encrypt would share the
// pool with the original (go vet's copylocks check reports copies).
type aeadScratch struct {
	_      noCopy
	aad    [16]byte
	nonce  [24]byte
	nonces *noncePool // nil until the first drawNonce
}

// drawNonce fills out with fresh random bytes from s's own nonce pool,
// allocating the pool on the first call.
func (s *aeadScratch) drawNonce(out []byte) error {
	if s.nonces == nil {
		s.nonces = newNoncePool()
	}
	return s.nonces.Draw(out)
}

// noCopy makes go vet's copylocks check report copies of a struct that
// holds it, as sync.noCopy does in the standard library. It has no state.
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// ErrPageIntegrity is the umbrella sentinel returned (wrapped) by every
// codec when a page fails its on-disk integrity check. Both modes report
// the same outer sentinel so callers can match a single value:
//
//	if errors.Is(err, btree.ErrPageIntegrity) { ... }
//
// The wrapped inner detail distinguishes the failure mode:
//
//	"checksum mismatch"            — cksumCodec (XXH3-128 trailer)
//	"AEAD authentication failed"   — aesCodec / chachaCodec (Poly1305 / GCM tag)
//
// Callers that need to switch on the kind without parsing strings can use
// SweepError.Kind from VerifyIntegrity instead.
var ErrPageIntegrity = nestedError("btree: page integrity check failed", ErrCorrupt)

// errCksumMismatch / errAEADAuthFail wrap ErrPageIntegrity at package init
// so the codec hot paths return a constant value with no per-call
// allocation. Unexported on purpose — external matching is via
// errors.Is(err, ErrPageIntegrity).
var (
	errCksumMismatch = fmt.Errorf("%w: checksum mismatch", ErrPageIntegrity)
	errAEADAuthFail  = fmt.Errorf("%w: AEAD authentication failed", ErrPageIntegrity)
)

// overheadOrZero returns c.Overhead() or 0 if c is nil. Used by the pager
// to compute ReservedSpace without branching at every call site.
func overheadOrZero(c Codec) int {
	if c == nil {
		return 0
	}
	return c.Overhead()
}

// encryptWith is the pager's CODEC1/CODEC2 analogue for the encrypt path.
// When c is nil, it returns src unchanged (pass-through). When c is
// non-nil, it delegates to c.Encrypt. The dst argument is a scratch buffer
// with at least len(src) capacity; on the nil path dst is ignored and src
// is returned directly.
func encryptWith(c Codec, dst, src []byte, pgno uint32, s *aeadScratch) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	return c.Encrypt(dst, src, pgno, s)
}

// decryptWith is the corresponding decrypt-path helper. Same pass-through
// semantics on nil codec.
func decryptWith(c Codec, dst, src []byte, pgno uint32, s *aeadScratch, onError integrityHook) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	return c.Decrypt(dst, src, pgno, s, onError)
}

// encryptPageWithCodec is a package-level variant of pager.encryptPage that
// applies the same page-1 plaintext-prefix rule (first dbHeaderSize bytes
// stay plaintext for pgno==1, rest encrypted). Used from wal.go, which
// doesn't hold a pager reference. dst must have len >= len(src) and must
// not alias src. Returns src unchanged when c is nil.
func encryptPageWithCodec(c Codec, dst, src []byte, pgno uint32, s *aeadScratch) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	plainPrefix := 0
	if pgno == 1 {
		plainPrefix = dbHeaderSize
	}
	copy(dst[:plainPrefix], src[:plainPrefix])
	if _, err := c.Encrypt(dst[plainPrefix:], src[plainPrefix:], pgno, s); err != nil {
		return nil, err
	}
	return dst[:len(src)], nil
}

// decryptPageWithCodec is the decrypt-path counterpart. onError is the
// calling database's integrity hook (see Codec.Decrypt).
func decryptPageWithCodec(c Codec, dst, src []byte, pgno uint32, s *aeadScratch, onError integrityHook) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	plainPrefix := 0
	if pgno == 1 {
		plainPrefix = dbHeaderSize
	}
	copy(dst[:plainPrefix], src[:plainPrefix])
	if _, err := c.Decrypt(dst[plainPrefix:], src[plainPrefix:], pgno, s, onError); err != nil {
		return nil, err
	}
	return dst[:len(src)], nil
}

// noncePoolSize is the buffer size for batched nonce generation. At
// 4 KB, one crypto/rand.Read syscall (~500-1000ns on Linux via
// getrandom) amortizes over ~340 12-byte nonces or ~170 24-byte nonces
// — eliminating the per-page syscall bottleneck observed in benchmarks.
//
// Analogous to OpenSSL's internal RAND_bytes buffer that SQLCipher
// relies on for per-page IV generation.
const noncePoolSize = 4096

// noncePool batches crypto/rand.Read syscalls behind a userspace buffer.
//
// NOT safe for concurrent use, and it takes no lock: each pool belongs to
// one aeadScratch, whose owner encrypts from one goroutine at a time. The
// WAL's frame writer draws under the database's write lock; the pager's
// only draw is initNewDB's page-1 encrypt, inside Open, which has the pager
// to itself then. A pool must never be reachable from a codec, which is
// shared (see aeadScratch).
type noncePool struct {
	buf [noncePoolSize]byte
	off int // position to read from; starts == len(buf) so first call triggers a refill
}

// newNoncePool constructs a pool whose initial state is "exhausted",
// so the first Draw triggers a fill. Using the zero value would leak
// 4 KB of constant zeros as "nonces", which is catastrophic for AEAD.
func newNoncePool() *noncePool {
	return &noncePool{off: noncePoolSize}
}

// Draw fills out with fresh randomness. Refills the internal buffer
// from crypto/rand.Read when exhausted.
func (p *noncePool) Draw(out []byte) error {
	for len(out) > 0 {
		if p.off >= noncePoolSize {
			if _, err := rand.Read(p.buf[:]); err != nil {
				return err
			}
			p.off = 0
		}
		n := copy(out, p.buf[p.off:])
		p.off += n
		out = out[n:]
	}
	return nil
}
