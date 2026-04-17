package btree

import "crypto/rand"

// Codec is the pluggable page-encryption interface. When a non-nil Codec is
// installed on the pager, every file/WAL I/O site routes through it.
// Implementations must be safe for concurrent use: the pager shares one Codec
// across the writer and all readers.
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
	// s is a caller-owned scratch holding the nonce/AAD buffers; it must
	// not be nil. See aeadScratch for concurrency rules.
	Encrypt(dst, src []byte, pgno uint32, s *aeadScratch) ([]byte, error)

	// Decrypt is the inverse of Encrypt. On tag verification failure it
	// returns ErrCodecTamper without modifying dst past any intermediate
	// state. Callers must not use dst on error.
	Decrypt(dst, src []byte, pgno uint32, s *aeadScratch) ([]byte, error)
}

// aeadScratch is a caller-owned, per-call scratch for codec Encrypt/Decrypt.
// It holds the nonce and AAD buffers outside the codec so that they live
// on the caller's heap-allocated parent struct (pager / pcache / wal) and
// don't escape to the heap on every call. Sized for the largest built-in
// cipher: XChaCha20-Poly1305 uses a 24-byte nonce; AAD is always 4 bytes
// but the field is padded to 16 for alignment / future use.
//
// Not safe for concurrent use: each caller (single-writer or per-reader-tx
// pcache) owns one instance. Modeled after SQLCipher's reuse of
// codec_ctx->iv and the EVP_CIPHER_CTX scratch.
type aeadScratch struct {
	aad   [16]byte
	nonce [24]byte
}

// ErrCodecTamper indicates the AEAD tag failed to verify. The page has been
// modified in a way the codec cannot authenticate (wrong key, corruption,
// or tampering).
var ErrCodecTamper = codecError("encryption: page authentication failed")

// codecError is a stdlib-free sentinel error type.
type codecError string

func (e codecError) Error() string { return string(e) }

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
func decryptWith(c Codec, dst, src []byte, pgno uint32, s *aeadScratch) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	return c.Decrypt(dst, src, pgno, s)
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

// decryptPageWithCodec is the decrypt-path counterpart.
func decryptPageWithCodec(c Codec, dst, src []byte, pgno uint32, s *aeadScratch) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	plainPrefix := 0
	if pgno == 1 {
		plainPrefix = dbHeaderSize
	}
	copy(dst[:plainPrefix], src[:plainPrefix])
	if _, err := c.Decrypt(dst[plainPrefix:], src[plainPrefix:], pgno, s); err != nil {
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
// NOT safe for concurrent use. any-store's Codec is only invoked for
// nonce-drawing Encrypt calls by the writer goroutine (writeMu-serialized
// at the DB level); readers never call Encrypt. If a future refactor
// lets multiple goroutines share a codec for encryption, wrap Draw in
// external synchronization or add a lock here.
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
