package btree

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"
)

func TestChaCha20Poly1305Codec_Overhead(t *testing.T) {
	key := make([]byte, 32)
	c, err := NewChaCha20Poly1305Codec(key)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Overhead(); got != 32 {
		t.Fatalf("Overhead = %d, want 32", got)
	}
}

func TestXChaCha20Poly1305Codec_Overhead(t *testing.T) {
	key := make([]byte, 32)
	c, err := NewXChaCha20Poly1305Codec(key)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Overhead(); got != 48 {
		t.Fatalf("Overhead = %d, want 48", got)
	}
}

func testChachaRoundTrip(t *testing.T, c Codec) {
	t.Helper()
	pageSize := 4096
	src := make([]byte, pageSize)
	if _, err := rand.Read(src[:pageSize-c.Overhead()]); err != nil {
		t.Fatal(err)
	}
	srcCopy := make([]byte, pageSize)
	copy(srcCopy, src)
	dst := make([]byte, pageSize)
	ct, err := c.Encrypt(dst, src, 7, &aeadScratch{})
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if len(ct) != pageSize {
		t.Fatalf("ciphertext length = %d, want %d", len(ct), pageSize)
	}
	pt := make([]byte, pageSize)
	out, err := c.Decrypt(pt, ct, 7, &aeadScratch{})
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	bodyLen := pageSize - c.Overhead()
	if !bytes.Equal(out[:bodyLen], srcCopy[:bodyLen]) {
		t.Fatalf("round-trip body mismatch")
	}
}

func TestChaCha20Poly1305Codec_RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	c, _ := NewChaCha20Poly1305Codec(key)
	testChachaRoundTrip(t, c)
}

func TestXChaCha20Poly1305Codec_RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	c, _ := NewXChaCha20Poly1305Codec(key)
	testChachaRoundTrip(t, c)
}

func testChachaTamperDetected(t *testing.T, c Codec) {
	t.Helper()
	pageSize := 4096
	src := make([]byte, pageSize)
	dst := make([]byte, pageSize)
	ct, _ := c.Encrypt(dst, src, 1, &aeadScratch{})
	ct[100] ^= 0x01
	pt := make([]byte, pageSize)
	if _, err := c.Decrypt(pt, ct, 1, &aeadScratch{}); !errors.Is(err, ErrPageIntegrity) {
		t.Fatalf("flipped-bit decrypt: got err=%v, want ErrPageIntegrity", err)
	}
}

func TestChaCha20Poly1305Codec_TamperDetected(t *testing.T) {
	c, _ := NewChaCha20Poly1305Codec(make([]byte, 32))
	testChachaTamperDetected(t, c)
}

func TestXChaCha20Poly1305Codec_TamperDetected(t *testing.T) {
	c, _ := NewXChaCha20Poly1305Codec(make([]byte, 32))
	testChachaTamperDetected(t, c)
}

func TestChaCha20Poly1305Codec_PageNumberBound(t *testing.T) {
	c, _ := NewChaCha20Poly1305Codec(make([]byte, 32))
	pageSize := 4096
	src := make([]byte, pageSize)
	dst := make([]byte, pageSize)
	ct, _ := c.Encrypt(dst, src, 5, &aeadScratch{})
	pt := make([]byte, pageSize)
	if _, err := c.Decrypt(pt, ct, 6, &aeadScratch{}); !errors.Is(err, ErrPageIntegrity) {
		t.Fatalf("wrong-pgno decrypt: got err=%v, want ErrPageIntegrity", err)
	}
}

func TestChaCha20Poly1305Codec_InvalidKeyLength(t *testing.T) {
	if _, err := NewChaCha20Poly1305Codec(make([]byte, 16)); err == nil {
		t.Fatalf("16-byte key accepted, want error")
	}
	if _, err := NewXChaCha20Poly1305Codec(make([]byte, 16)); err == nil {
		t.Fatalf("16-byte key accepted, want error")
	}
}

func TestNewCodecFromType(t *testing.T) {
	key := make([]byte, 32)
	for _, ct := range []CipherType{CipherAES256GCM, CipherChaCha20Poly1305, CipherXChaCha20Poly1305} {
		c, err := newCodecFromType(ct, key)
		if err != nil {
			t.Fatalf("newCodecFromType(%q): %v", ct, err)
		}
		if c == nil {
			t.Fatalf("newCodecFromType(%q) returned nil codec", ct)
		}
	}
	if _, err := newCodecFromType("bogus", key); err == nil {
		t.Fatalf("unknown CipherType accepted")
	}
}
