package btree

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
	Encrypt(dst, src []byte, pgno uint32) ([]byte, error)

	// Decrypt is the inverse of Encrypt. On tag verification failure it
	// returns ErrCodecTamper without modifying dst past any intermediate
	// state. Callers must not use dst on error.
	Decrypt(dst, src []byte, pgno uint32) ([]byte, error)
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
func encryptWith(c Codec, dst, src []byte, pgno uint32) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	return c.Encrypt(dst, src, pgno)
}

// decryptWith is the corresponding decrypt-path helper. Same pass-through
// semantics on nil codec.
func decryptWith(c Codec, dst, src []byte, pgno uint32) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	return c.Decrypt(dst, src, pgno)
}

// encryptPageWithCodec is a package-level variant of pager.encryptPage that
// applies the same page-1 plaintext-prefix rule (first dbHeaderSize bytes
// stay plaintext for pgno==1, rest encrypted). Used from wal.go, which
// doesn't hold a pager reference. dst must have len >= len(src) and must
// not alias src. Returns src unchanged when c is nil.
func encryptPageWithCodec(c Codec, dst, src []byte, pgno uint32) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	plainPrefix := 0
	if pgno == 1 {
		plainPrefix = dbHeaderSize
	}
	copy(dst[:plainPrefix], src[:plainPrefix])
	if _, err := c.Encrypt(dst[plainPrefix:], src[plainPrefix:], pgno); err != nil {
		return nil, err
	}
	return dst[:len(src)], nil
}

// decryptPageWithCodec is the decrypt-path counterpart.
func decryptPageWithCodec(c Codec, dst, src []byte, pgno uint32) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	plainPrefix := 0
	if pgno == 1 {
		plainPrefix = dbHeaderSize
	}
	copy(dst[:plainPrefix], src[:plainPrefix])
	if _, err := c.Decrypt(dst[plainPrefix:], src[plainPrefix:], pgno); err != nil {
		return nil, err
	}
	return dst[:len(src)], nil
}
