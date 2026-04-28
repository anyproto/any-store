package btree

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/zeebo/xxh3"
)

func TestCksumCodec_Overhead(t *testing.T) {
	c := newCksumCodec()
	if got := c.Overhead(); got != 16 {
		t.Fatalf("Overhead() = %d, want 16", got)
	}
}

func TestCksumCodec_RoundTrip(t *testing.T) {
	c := newCksumCodec()
	pageSize := 4096
	src := make([]byte, pageSize)
	for i := range src {
		src[i] = byte(i)
	}
	dst := make([]byte, pageSize)
	var s aeadScratch
	out, err := c.Encrypt(dst, src, 2, &s)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if len(out) != pageSize {
		t.Fatalf("Encrypt len = %d, want %d", len(out), pageSize)
	}
	h := xxh3.Hash128(out[:pageSize-16])
	var want [16]byte
	putUint128LE(want[:], h)
	if !bytes.Equal(out[pageSize-16:], want[:]) {
		t.Fatalf("trailer = %x, want %x", out[pageSize-16:], want)
	}
	dst2 := make([]byte, pageSize)
	plain, err := c.Decrypt(dst2, out, 2, &s)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(plain[:pageSize-16], src[:pageSize-16]) {
		t.Fatal("Decrypt body mismatch")
	}
}

func TestCksumCodec_TamperReturnsErr(t *testing.T) {
	c := newCksumCodec()
	pageSize := 4096
	src := make([]byte, pageSize)
	for i := range src {
		src[i] = byte(i)
	}
	dst := make([]byte, pageSize)
	var s aeadScratch
	out, _ := c.Encrypt(dst, src, 2, &s)
	out[100] ^= 0x01
	dst2 := make([]byte, pageSize)
	if _, err := c.Decrypt(dst2, out, 2, &s); !errors.Is(err, ErrPageIntegrity) {
		t.Fatalf("Decrypt: want ErrPageIntegrity, got %v", err)
	}
}

func TestCksumCodec_VerifyOff_NoError(t *testing.T) {
	c := newCksumCodec()
	c.SetVerify(false)
	pageSize := 4096
	src := make([]byte, pageSize)
	dst := make([]byte, pageSize)
	var s aeadScratch
	out, _ := c.Encrypt(dst, src, 2, &s)
	out[100] ^= 0x01
	dst2 := make([]byte, pageSize)
	if _, err := c.Decrypt(dst2, out, 2, &s); err != nil {
		t.Fatalf("verify-off: want nil err, got %v", err)
	}
}

func TestCksumCodec_OnErrorCallback(t *testing.T) {
	c := newCksumCodec()
	var fired uint32
	var gotPgno uint32
	c.SetOnError(func(pgno uint32, _ error) {
		atomic.StoreUint32(&fired, 1)
		atomic.StoreUint32(&gotPgno, pgno)
	})
	pageSize := 4096
	src := make([]byte, pageSize)
	dst := make([]byte, pageSize)
	var s aeadScratch
	out, _ := c.Encrypt(dst, src, 17, &s)
	out[200] ^= 0x01
	dst2 := make([]byte, pageSize)
	_, _ = c.Decrypt(dst2, out, 17, &s)
	if atomic.LoadUint32(&fired) != 1 {
		t.Fatal("OnError did not fire")
	}
	if got := atomic.LoadUint32(&gotPgno); got != 17 {
		t.Fatalf("OnError pgno = %d, want 17", got)
	}
}

func TestCksumCodec_OnError_FiresWhenVerifyOff(t *testing.T) {
	c := newCksumCodec()
	c.SetVerify(false)
	var fired uint32
	c.SetOnError(func(uint32, error) { atomic.StoreUint32(&fired, 1) })
	pageSize := 4096
	src := make([]byte, pageSize)
	dst := make([]byte, pageSize)
	var s aeadScratch
	out, _ := c.Encrypt(dst, src, 1, &s)
	out[300] ^= 0x80
	dst2 := make([]byte, pageSize)
	if _, err := c.Decrypt(dst2, out, 1, &s); err != nil {
		t.Fatalf("verify-off should suppress err, got %v", err)
	}
	if atomic.LoadUint32(&fired) != 1 {
		t.Fatal("OnError should fire even with verify off")
	}
}
