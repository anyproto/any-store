package btree

import (
	"bytes"
	"testing"
)

func TestCodec_NilIsZeroOverhead(t *testing.T) {
	var c Codec
	if got := overheadOrZero(c); got != 0 {
		t.Fatalf("overheadOrZero(nil) = %d, want 0", got)
	}
}

func TestCodec_EncryptPlainNil(t *testing.T) {
	var c Codec
	src := []byte("hello world")
	dst := make([]byte, 0, len(src))
	out, err := encryptWith(c, dst, src, 42)
	if err != nil {
		t.Fatalf("encryptWith(nil) error: %v", err)
	}
	if !bytes.Equal(out, src) {
		t.Fatalf("nil codec must pass-through: got %q want %q", out, src)
	}
}

func TestCodec_DecryptPlainNil(t *testing.T) {
	var c Codec
	src := []byte("hello world")
	dst := make([]byte, 0, len(src))
	out, err := decryptWith(c, dst, src, 42)
	if err != nil {
		t.Fatalf("decryptWith(nil) error: %v", err)
	}
	if !bytes.Equal(out, src) {
		t.Fatalf("nil codec must pass-through")
	}
}
