package btree

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
)

// aesCodecOverhead is the per-page overhead: 16-byte GCM tag + 12-byte
// GCM nonce = 28 bytes, rounded up to 32 for AES block alignment. Must
// stay a multiple of 16.
const aesCodecOverhead = 32

// aesNonceLen is the GCM standard nonce length.
const aesNonceLen = 12

// aesTagLen is the GCM authentication tag length.
const aesTagLen = 16

// aesCodec implements Codec using AES-256-GCM. One instance per DB — the
// underlying cipher.AEAD is safe for concurrent use from writer and all
// readers.
//
// Per-page layout (pageSize = 4096, bodyLen = 4064):
//
//	[ciphertext (bodyLen)] [tag (16)] [nonce (12)] [padding (4)]
//	 0                      bodyLen    bodyLen+16  bodyLen+28    bodyLen+32
//
// This keeps ciphertext and tag contiguous, matching GCM Seal/Open's
// expected input/output format, so neither path needs a scratch copy.
// Padding at the tail keeps the total overhead a multiple of 16 (AES
// block size), which is the SQLCipher convention for reserve areas.
type aesCodec struct {
	aead cipher.AEAD
}

// NewAESCodec constructs the default codec. key must be exactly 32 bytes
// (AES-256). Callers derive the key via DeriveKey (passphrase path) or
// supply it directly (raw-key path).
func NewAESCodec(key []byte) (Codec, error) {
	if len(key) != KeyLen {
		return nil, fmt.Errorf("btree: AES codec requires %d-byte key, got %d", KeyLen, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("btree: aes.NewCipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("btree: cipher.NewGCM: %w", err)
	}
	return &aesCodec{aead: aead}, nil
}

func (c *aesCodec) Overhead() int { return aesCodecOverhead }

// Encrypt transforms plaintext src into ciphertext in dst. src and dst must
// both have length pageSize; dst must not alias src. The first
// (pageSize - Overhead()) bytes of src are the page body to encrypt; the
// trailing Overhead() bytes of src are ignored.
func (c *aesCodec) Encrypt(dst, src []byte, pgno uint32) ([]byte, error) {
	if len(dst) < len(src) {
		return nil, fmt.Errorf("btree: aesCodec.Encrypt: dst too small (%d < %d)", len(dst), len(src))
	}
	bodyLen := len(src) - aesCodecOverhead
	if bodyLen < 0 {
		return nil, fmt.Errorf("btree: aesCodec.Encrypt: page too small (%d < overhead %d)", len(src), aesCodecOverhead)
	}

	// Draw a fresh random nonce into a local array (not aliased with dst).
	var nonce [aesNonceLen]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("btree: aesCodec.Encrypt: rand.Read: %w", err)
	}

	// AAD = page number as little-endian uint32. Binds the page location
	// into the MAC so pages cannot be shuffled between file positions.
	var aad [4]byte
	binary.LittleEndian.PutUint32(aad[:], pgno)

	// Seal(dst[:0], nonce, plaintext, aad) appends ct||tag to dst[:0],
	// producing dst[0 : bodyLen+aesTagLen].
	_ = c.aead.Seal(dst[:0], nonce[:], src[:bodyLen], aad[:])

	// Write nonce after the tag: dst[bodyLen+16 : bodyLen+28].
	noncePos := bodyLen + aesTagLen
	copy(dst[noncePos:noncePos+aesNonceLen], nonce[:])

	// Zero any trailing padding between the nonce and end-of-page.
	for i := noncePos + aesNonceLen; i < len(src); i++ {
		dst[i] = 0
	}
	return dst[:len(src)], nil
}

// Decrypt is the inverse of Encrypt. Returns ErrCodecTamper on any
// authentication failure (wrong key, corruption, or pgno mismatch).
// dst must not alias src.
func (c *aesCodec) Decrypt(dst, src []byte, pgno uint32) ([]byte, error) {
	if len(dst) < len(src) {
		return nil, fmt.Errorf("btree: aesCodec.Decrypt: dst too small (%d < %d)", len(dst), len(src))
	}
	bodyLen := len(src) - aesCodecOverhead
	if bodyLen < 0 {
		return nil, fmt.Errorf("btree: aesCodec.Decrypt: page too small")
	}

	// Layout: [ct][tag][nonce][pad]. ct||tag is contiguous at src[0:bodyLen+16].
	ctAndTag := src[0 : bodyLen+aesTagLen]
	noncePos := bodyLen + aesTagLen
	nonce := src[noncePos : noncePos+aesNonceLen]

	var aad [4]byte
	binary.LittleEndian.PutUint32(aad[:], pgno)

	// Open(dst[:0], nonce, ct||tag, aad) writes plaintext to dst[0:bodyLen].
	// dst and src are separate buffers — no aliasing.
	pt, err := c.aead.Open(dst[:0], nonce, ctAndTag, aad[:])
	if err != nil {
		return nil, ErrCodecTamper
	}
	if len(pt) != bodyLen {
		return nil, fmt.Errorf("btree: aesCodec.Decrypt: unexpected plaintext length %d", len(pt))
	}

	// Zero the reserve tail in dst so callers never see stale bytes there.
	for i := bodyLen; i < len(src); i++ {
		dst[i] = 0
	}
	return dst[:len(src)], nil
}
