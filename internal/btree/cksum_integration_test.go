package btree

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestCksum_OpenCreate_SetsReservedSpace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	opts := DefaultOptions()
	opts.Checksum = true
	db, err := Open(path,opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := db.pager.header.ReservedSpace; got != 16 {
		t.Fatalf("ReservedSpace = %d, want 16", got)
	}
	var zero [SaltLen]byte
	if db.pager.header.Salt != zero {
		t.Fatalf("Salt should be zero in checksum-only mode, got %x", db.pager.header.Salt)
	}
	_ = db.Close()
}

func TestCksum_Reopen_DetectsModeMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")

	opts := DefaultOptions()
	opts.Checksum = true
	db, err := Open(path,opts)
	if err != nil {
		t.Fatalf("Open create: %v", err)
	}
	_ = db.Close()

	plainOpts := DefaultOptions()
	if _, err := Open(path,plainOpts); err == nil {
		t.Fatal("expected error reopening checksum DB without Checksum=true")
	} else if !errors.Is(err, ErrIntegrityModeMismatch) {
		t.Fatalf("want ErrIntegrityModeMismatch, got %v", err)
	}

	db2, err := Open(path,opts)
	if err != nil {
		t.Fatalf("Open reopen: %v", err)
	}
	_ = db2.Close()
}

func TestCksum_CannotCombineWithEncryption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	opts := DefaultOptions()
	opts.Checksum = true
	opts.Key = []byte("passphrase")
	if _, err := Open(path,opts); err == nil {
		t.Fatal("expected error: Checksum + Key both set")
	} else if !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCksum_CannotEnableOnExistingPlainDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	plain := DefaultOptions()
	db, err := Open(path,plain)
	if err != nil {
		t.Fatalf("create plain: %v", err)
	}
	_ = db.Close()

	cksOpts := DefaultOptions()
	cksOpts.Checksum = true
	_, err = Open(path,cksOpts)
	if err == nil {
		t.Fatal("expected ErrIntegrityModeMismatch enabling cksum on existing plain DB")
	}
	if !errors.Is(err, ErrIntegrityModeMismatch) {
		t.Fatalf("want ErrIntegrityModeMismatch, got %v", err)
	}
}
