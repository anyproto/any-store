package btree

import (
	"bytes"
	"testing"
)

func TestDeriveKey_DeterministicForSameInput(t *testing.T) {
	pass := []byte("correct horse battery staple")
	salt := bytes.Repeat([]byte{0xAB}, 16)
	k1 := DeriveKey(pass, salt, 1000)
	k2 := DeriveKey(pass, salt, 1000)
	if !bytes.Equal(k1, k2) {
		t.Fatalf("same inputs produced different keys")
	}
	if len(k1) != 32 {
		t.Fatalf("key length = %d, want 32", len(k1))
	}
}

func TestDeriveKey_DifferentSaltDifferentKey(t *testing.T) {
	pass := []byte("pw")
	k1 := DeriveKey(pass, bytes.Repeat([]byte{1}, 16), 1000)
	k2 := DeriveKey(pass, bytes.Repeat([]byte{2}, 16), 1000)
	if bytes.Equal(k1, k2) {
		t.Fatalf("different salts produced same key")
	}
}

func TestDeriveKey_IterationMatters(t *testing.T) {
	pass := []byte("pw")
	salt := bytes.Repeat([]byte{1}, 16)
	k1 := DeriveKey(pass, salt, 1000)
	k2 := DeriveKey(pass, salt, 2000)
	if bytes.Equal(k1, k2) {
		t.Fatalf("different iteration counts produced same key")
	}
}

func TestDeriveKey_ZeroIterationsDefaults(t *testing.T) {
	pass := []byte("pw")
	salt := bytes.Repeat([]byte{1}, 16)
	k := DeriveKey(pass, salt, 0)
	if len(k) != 32 {
		t.Fatalf("default-iter key length = %d, want 32", len(k))
	}
}
