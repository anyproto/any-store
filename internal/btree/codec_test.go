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
	out, err := encryptWith(c, dst, src, 42, &aeadScratch{})
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
	out, err := decryptWith(c, dst, src, 42, &aeadScratch{})
	if err != nil {
		t.Fatalf("decryptWith(nil) error: %v", err)
	}
	if !bytes.Equal(out, src) {
		t.Fatalf("nil codec must pass-through")
	}
}

func TestPager_InstallCodec_ReservedSpace(t *testing.T) {
	p := &pager{pageSize: 4096}
	p.usableSize_ = int(p.pageSize) // pre-install: no reserve
	if p.usableSize() != 4096 {
		t.Fatalf("usableSize before install = %d, want 4096", p.usableSize())
	}

	key := make([]byte, 32)
	c, err := NewAESCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	p.installCodec(c)
	if p.header.ReservedSpace != uint8(c.Overhead()) {
		t.Fatalf("ReservedSpace = %d, want %d", p.header.ReservedSpace, c.Overhead())
	}
	if p.usableSize() != 4096-c.Overhead() {
		t.Fatalf("usableSize after install = %d, want %d", p.usableSize(), 4096-c.Overhead())
	}
	if p.codec != c {
		t.Fatalf("codec not stored")
	}
}

func TestPager_InstallCodec_NilIsNoop(t *testing.T) {
	p := &pager{pageSize: 4096}
	p.usableSize_ = int(p.pageSize)
	p.installCodec(nil)
	if p.codec != nil {
		t.Fatalf("nil install set codec")
	}
	if p.header.ReservedSpace != 0 {
		t.Fatalf("nil install changed ReservedSpace")
	}
}
