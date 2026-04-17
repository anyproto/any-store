package btree

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
)

// chachaCodec wraps either ChaCha20-Poly1305 (12-byte nonce) or
// XChaCha20-Poly1305 (24-byte nonce). Per-page layout:
//
//	[ciphertext][tag (16)][nonce (nonceLen)][padding]
//
// Total overhead is rounded up to a multiple of 16 for AES-block-aligned
// reserved-area semantics inherited from SQLCipher conventions.
type chachaCodec struct {
	aead     interface {
		Seal(dst, nonce, plaintext, additionalData []byte) []byte
		Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error)
		NonceSize() int
		Overhead() int
	}
	nonceLen int
	overhead int
}

const (
	chacha20NonceLen  = chacha20poly1305.NonceSize       // 12
	xChaCha20NonceLen = chacha20poly1305.NonceSizeX      // 24
	chachaTagLen      = chacha20poly1305.Overhead        // 16
	chachaKeyLen      = chacha20poly1305.KeySize         // 32

	// chacha20Overhead: 12 + 16 = 28 → 32 (aligned to 16).
	chacha20Overhead = 32
	// xChaCha20Overhead: 24 + 16 = 40 → 48 (aligned to 16).
	xChaCha20Overhead = 48
)

// NewChaCha20Poly1305Codec constructs a ChaCha20-Poly1305 codec.
// Per-page overhead: 32 bytes. key must be exactly 32 bytes.
func NewChaCha20Poly1305Codec(key []byte) (Codec, error) {
	if len(key) != chachaKeyLen {
		return nil, fmt.Errorf("btree: ChaCha20-Poly1305 codec requires %d-byte key, got %d", chachaKeyLen, len(key))
	}
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, fmt.Errorf("btree: chacha20poly1305.New: %w", err)
	}
	return &chachaCodec{aead: aead, nonceLen: chacha20NonceLen, overhead: chacha20Overhead}, nil
}

// NewXChaCha20Poly1305Codec constructs an XChaCha20-Poly1305 codec with a
// 24-byte nonce. Per-page overhead: 48 bytes. key must be exactly 32 bytes.
// Recommended for workloads exceeding ~2^32 page writes where the 12-byte
// random-nonce collision risk of ChaCha20/AES-GCM is a concern.
func NewXChaCha20Poly1305Codec(key []byte) (Codec, error) {
	if len(key) != chachaKeyLen {
		return nil, fmt.Errorf("btree: XChaCha20-Poly1305 codec requires %d-byte key, got %d", chachaKeyLen, len(key))
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, fmt.Errorf("btree: chacha20poly1305.NewX: %w", err)
	}
	return &chachaCodec{aead: aead, nonceLen: xChaCha20NonceLen, overhead: xChaCha20Overhead}, nil
}

func (c *chachaCodec) Overhead() int { return c.overhead }

// Encrypt follows the same [ct][tag][nonce][pad] layout as aesCodec.
func (c *chachaCodec) Encrypt(dst, src []byte, pgno uint32) ([]byte, error) {
	if len(dst) < len(src) {
		return nil, fmt.Errorf("btree: chachaCodec.Encrypt: dst too small (%d < %d)", len(dst), len(src))
	}
	bodyLen := len(src) - c.overhead
	if bodyLen < 0 {
		return nil, fmt.Errorf("btree: chachaCodec.Encrypt: page too small (%d < overhead %d)", len(src), c.overhead)
	}

	nonce := make([]byte, c.nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("btree: chachaCodec.Encrypt: rand.Read: %w", err)
	}

	var aad [4]byte
	binary.LittleEndian.PutUint32(aad[:], pgno)

	_ = c.aead.Seal(dst[:0], nonce, src[:bodyLen], aad[:])
	// Layout: [ct(bodyLen)][tag(16)] are at dst[0:bodyLen+16]; now place
	// nonce after the tag, then zero any remaining padding.
	noncePos := bodyLen + chachaTagLen
	copy(dst[noncePos:noncePos+c.nonceLen], nonce)
	for i := noncePos + c.nonceLen; i < len(src); i++ {
		dst[i] = 0
	}
	return dst[:len(src)], nil
}

func (c *chachaCodec) Decrypt(dst, src []byte, pgno uint32) ([]byte, error) {
	if len(dst) < len(src) {
		return nil, fmt.Errorf("btree: chachaCodec.Decrypt: dst too small (%d < %d)", len(dst), len(src))
	}
	bodyLen := len(src) - c.overhead
	if bodyLen < 0 {
		return nil, fmt.Errorf("btree: chachaCodec.Decrypt: page too small")
	}

	ctAndTag := src[0 : bodyLen+chachaTagLen]
	noncePos := bodyLen + chachaTagLen
	nonce := src[noncePos : noncePos+c.nonceLen]

	var aad [4]byte
	binary.LittleEndian.PutUint32(aad[:], pgno)

	pt, err := c.aead.Open(dst[:0], nonce, ctAndTag, aad[:])
	if err != nil {
		return nil, ErrCodecTamper
	}
	if len(pt) != bodyLen {
		return nil, fmt.Errorf("btree: chachaCodec.Decrypt: unexpected plaintext length %d", len(pt))
	}
	for i := bodyLen; i < len(src); i++ {
		dst[i] = 0
	}
	return dst[:len(src)], nil
}
