package btree

import (
	"sync/atomic"
	"testing"
)

// testAEADOnError checks that an AEAD codec reports an authentication
// failure to the hook passed to that Decrypt call, and to no other: two
// calls through one codec with two hooks each reach their own.
func testAEADOnError(t *testing.T, mk func() (Codec, error)) {
	t.Helper()
	c, err := mk()
	if err != nil {
		t.Fatal(err)
	}
	type hits struct{ fired, pgno atomic.Uint32 }
	var a, b hits
	hook := func(h *hits) integrityHook {
		return func(pgno uint32, _ error) {
			h.fired.Add(1)
			h.pgno.Store(pgno)
		}
	}
	pageSize := 4096
	src := make([]byte, pageSize)
	for i := range src {
		src[i] = byte(i)
	}
	dst := make([]byte, pageSize)
	var s aeadScratch
	out, err := c.Encrypt(dst, src, 42, &s)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	out[100] ^= 0x01
	dst2 := make([]byte, pageSize)
	if _, derr := c.Decrypt(dst2, out, 42, &s, hook(&a)); derr == nil {
		t.Fatal("expected decrypt error on tampered ciphertext")
	}
	if got := a.fired.Load(); got != 1 {
		t.Fatalf("onError fired %d times on AEAD tamper, want 1", got)
	}
	if got := a.pgno.Load(); got != 42 {
		t.Fatalf("onError pgno = %d, want 42", got)
	}
	if _, derr := c.Decrypt(dst2, out, 42, &s, hook(&b)); derr == nil {
		t.Fatal("expected decrypt error on tampered ciphertext")
	}
	if _, derr := c.Decrypt(dst2, out, 42, &s, nil); derr == nil {
		t.Fatal("expected decrypt error on tampered ciphertext")
	}
	if a.fired.Load() != 1 || b.fired.Load() != 1 {
		t.Fatalf("hooks fired a=%d b=%d, want 1 each: each failure goes to its own call's hook", a.fired.Load(), b.fired.Load())
	}
}

func TestAESCodec_OnError(t *testing.T) {
	key := make([]byte, KeyLen)
	for i := range key {
		key[i] = byte(i)
	}
	testAEADOnError(t, func() (Codec, error) { return NewAESCodec(key) })
}

func TestChaCha20Codec_OnError(t *testing.T) {
	key := make([]byte, KeyLen)
	testAEADOnError(t, func() (Codec, error) { return NewChaCha20Poly1305Codec(key) })
}

func TestXChaCha20Codec_OnError(t *testing.T) {
	key := make([]byte, KeyLen)
	testAEADOnError(t, func() (Codec, error) { return NewXChaCha20Poly1305Codec(key) })
}
