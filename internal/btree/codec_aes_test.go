package btree

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestAESCodec_Overhead(t *testing.T) {
	key := make([]byte, 32)
	c, err := NewAESCodec(key)
	if err != nil {
		t.Fatalf("NewAESCodec: %v", err)
	}
	if got := c.Overhead(); got != 32 {
		t.Fatalf("Overhead = %d, want 32", got)
	}
}

func TestAESCodec_RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	c, err := NewAESCodec(key)
	if err != nil {
		t.Fatal(err)
	}

	pageSize := 4096
	src := make([]byte, pageSize)
	if _, err := rand.Read(src[:pageSize-c.Overhead()]); err != nil {
		t.Fatal(err)
	}
	srcCopy := make([]byte, pageSize)
	copy(srcCopy, src)

	dst := make([]byte, pageSize)
	ct, err := c.Encrypt(dst, src, 7)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if len(ct) != pageSize {
		t.Fatalf("ciphertext length = %d, want %d", len(ct), pageSize)
	}
	// Plaintext must not equal ciphertext (sanity).
	if bytes.Equal(ct[:pageSize-c.Overhead()], srcCopy[:pageSize-c.Overhead()]) {
		t.Fatalf("ciphertext body matches plaintext — encryption didn't run")
	}

	pt := make([]byte, pageSize)
	out, err := c.Decrypt(pt, ct, 7)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(out[:pageSize-c.Overhead()], srcCopy[:pageSize-c.Overhead()]) {
		t.Fatalf("round-trip body mismatch")
	}
}

func TestAESCodec_TamperDetected(t *testing.T) {
	key := make([]byte, 32)
	c, _ := NewAESCodec(key)
	pageSize := 4096
	src := make([]byte, pageSize)
	dst := make([]byte, pageSize)
	ct, _ := c.Encrypt(dst, src, 1)

	// Flip a bit somewhere in the encrypted body.
	ct[100] ^= 0x01
	pt := make([]byte, pageSize)
	if _, err := c.Decrypt(pt, ct, 1); err != ErrCodecTamper {
		t.Fatalf("flipped-bit decrypt: got err=%v, want ErrCodecTamper", err)
	}
}

func TestAESCodec_PageNumberBound(t *testing.T) {
	key := make([]byte, 32)
	c, _ := NewAESCodec(key)
	pageSize := 4096
	src := make([]byte, pageSize)
	dst := make([]byte, pageSize)
	ct, _ := c.Encrypt(dst, src, 5)
	// Decrypting with the wrong page number must fail.
	pt := make([]byte, pageSize)
	if _, err := c.Decrypt(pt, ct, 6); err != ErrCodecTamper {
		t.Fatalf("wrong-pgno decrypt: got err=%v, want ErrCodecTamper", err)
	}
}

func TestAESCodec_NonceUniqueness(t *testing.T) {
	// Same plaintext encrypted twice must produce different ciphertext
	// (because nonce is random per call).
	key := make([]byte, 32)
	c, _ := NewAESCodec(key)
	pageSize := 4096
	src := make([]byte, pageSize)
	dst1 := make([]byte, pageSize)
	dst2 := make([]byte, pageSize)
	ct1, _ := c.Encrypt(dst1, src, 1)
	ct2, _ := c.Encrypt(dst2, src, 1)
	if bytes.Equal(ct1, ct2) {
		t.Fatalf("same plaintext/page produced identical ciphertext — nonce not random")
	}
}

func TestAESCodec_WrongKeyRejected(t *testing.T) {
	pageSize := 4096
	src := make([]byte, pageSize)
	k1 := bytes.Repeat([]byte{1}, 32)
	k2 := bytes.Repeat([]byte{2}, 32)
	c1, _ := NewAESCodec(k1)
	c2, _ := NewAESCodec(k2)
	dst := make([]byte, pageSize)
	ct, _ := c1.Encrypt(dst, src, 1)
	pt := make([]byte, pageSize)
	if _, err := c2.Decrypt(pt, ct, 1); err != ErrCodecTamper {
		t.Fatalf("wrong-key decrypt: got err=%v, want ErrCodecTamper", err)
	}
}

func TestAESCodec_InvalidKeyLength(t *testing.T) {
	if _, err := NewAESCodec(make([]byte, 16)); err == nil {
		t.Fatalf("16-byte key accepted, want error")
	}
	if _, err := NewAESCodec(nil); err == nil {
		t.Fatalf("nil key accepted, want error")
	}
}

// Micro-benchmark: single Encrypt call with nonce pool.
// Run with: go test -bench BenchmarkCodec_Encrypt -run '^$' -benchtime=3s
func BenchmarkCodec_Encrypt_AES256GCM(b *testing.B) {
	benchCodecEncrypt(b, mustAES(b))
}

func BenchmarkCodec_Encrypt_ChaCha20Poly1305(b *testing.B) {
	benchCodecEncrypt(b, mustChaCha(b))
}

func BenchmarkCodec_Encrypt_XChaCha20Poly1305(b *testing.B) {
	benchCodecEncrypt(b, mustXChaCha(b))
}

func benchCodecEncrypt(b *testing.B, c Codec) {
	b.Helper()
	const pageSize = 4096
	src := make([]byte, pageSize)
	dst := make([]byte, pageSize)
	b.SetBytes(pageSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Encrypt(dst, src, uint32(i+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func mustAES(b *testing.B) Codec {
	b.Helper()
	c, err := NewAESCodec(make([]byte, 32))
	if err != nil {
		b.Fatal(err)
	}
	return c
}

func mustChaCha(b *testing.B) Codec {
	b.Helper()
	c, err := NewChaCha20Poly1305Codec(make([]byte, 32))
	if err != nil {
		b.Fatal(err)
	}
	return c
}

func mustXChaCha(b *testing.B) Codec {
	b.Helper()
	c, err := NewXChaCha20Poly1305Codec(make([]byte, 32))
	if err != nil {
		b.Fatal(err)
	}
	return c
}
