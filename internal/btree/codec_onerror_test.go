package btree

import (
	"sync/atomic"
	"testing"
)

func testAEADOnError(t *testing.T, mk func() (Codec, error)) {
	t.Helper()
	c, err := mk()
	if err != nil {
		t.Fatal(err)
	}
	sink, ok := c.(OnErrorSink)
	if !ok {
		t.Fatalf("codec %T does not implement OnErrorSink", c)
	}
	var fired uint32
	var gotPgno uint32
	sink.SetOnError(func(pgno uint32, _ error) {
		atomic.StoreUint32(&fired, 1)
		atomic.StoreUint32(&gotPgno, pgno)
	})
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
	_, derr := c.Decrypt(dst2, out, 42, &s)
	if derr == nil {
		t.Fatal("expected decrypt error on tampered ciphertext")
	}
	if atomic.LoadUint32(&fired) != 1 {
		t.Fatal("OnError did not fire on AEAD tamper")
	}
	if atomic.LoadUint32(&gotPgno) != 42 {
		t.Fatalf("OnError pgno = %d, want 42", atomic.LoadUint32(&gotPgno))
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
