package btree

import (
	"bytes"
	"errors"
	"os"
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
	_, err = Open(path, cksOpts)
	if err == nil {
		t.Fatal("expected ErrIntegrityModeMismatch enabling cksum on existing plain DB")
	}
	if !errors.Is(err, ErrIntegrityModeMismatch) {
		t.Fatalf("want ErrIntegrityModeMismatch, got %v", err)
	}
}

// TestCksum_TamperDetected mirrors TestEncryption_TamperDetected: write data,
// close, flip a body byte on disk, reopen and read — checksum mismatch must
// surface as an error. Modeled on encryption_integration_test.go:344-426.
func TestCksum_TamperDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tamper.db")
	opts := DefaultOptions()
	opts.Checksum = true
	opts.InProcess = true

	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	wtx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	ns, err := wtx.CreateNamespace("x")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		k := []byte{byte(i)}
		v := bytes.Repeat([]byte{byte(i)}, 100)
		if err := wtx.Put(ns, k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := wtx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(CheckpointFull); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Flip a byte well past the page-1 plaintext header.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < int(opts.PageSize)+200 {
		t.Fatalf("file too small: %d bytes", len(data))
	}
	off := int(opts.PageSize) + 200 // somewhere in page 2's body
	data[off] ^= 0x01
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Reopen and read. The codec MUST surface an error on the page that
	// was tampered. We don't constrain where exactly — open, begin-read,
	// or the first Get touching page 2 are all acceptable.
	db2, err := Open(path, opts)
	if err != nil {
		return
	}
	defer db2.Close()

	rtx, err := db2.BeginRead()
	if err != nil {
		return
	}
	defer rtx.Rollback()

	ns2, err := db2.GetNamespace("x")
	if err != nil {
		return
	}
	sawError := false
	for i := 0; i < 50; i++ {
		k := []byte{byte(i)}
		if _, err := rtx.Get(ns2, k); err != nil {
			sawError = true
		}
	}
	if !sawError {
		t.Fatal("no error observed after tampering page 2 body")
	}
}
