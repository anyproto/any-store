package btree

import (
	"bytes"
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
	db, err := Open(path, opts)
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

// TestCksum_Reopen_FileStateAuthoritative verifies the simplified Open
// contract: the file's on-disk state determines codec mode regardless of
// Options.Checksum at reopen. A cksum DB always reopens with the codec
// installed; a plain DB always reopens plain. No "mismatch" errors —
// callers can't accidentally upgrade or downgrade an existing DB via
// config drift.
func TestCksum_Reopen_FileStateAuthoritative(t *testing.T) {
	dir := t.TempDir()
	cksPath := filepath.Join(dir, "cksum.db")
	plainPath := filepath.Join(dir, "plain.db")

	opts := DefaultOptions()
	opts.Checksum = true
	db, err := Open(cksPath, opts)
	if err != nil {
		t.Fatalf("Open cksum create: %v", err)
	}
	_ = db.Close()

	// Reopen cksum DB without Options.Checksum — codec must auto-install.
	db2, err := Open(cksPath, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen cksum without Checksum: %v", err)
	}
	if db2.IntegrityMode() != IntegrityChecksum {
		t.Fatalf("auto-detect failed: mode = %v, want IntegrityChecksum", db2.IntegrityMode())
	}
	_ = db2.Close()

	// Plain DB.
	db3, err := Open(plainPath, DefaultOptions())
	if err != nil {
		t.Fatalf("Open plain: %v", err)
	}
	_ = db3.Close()

	// Reopen plain with Options.Checksum=true — must NOT upgrade; stays plain.
	db4, err := Open(plainPath, opts)
	if err != nil {
		t.Fatalf("reopen plain with Checksum: %v", err)
	}
	if db4.IntegrityMode() != IntegrityNone {
		t.Fatalf("should not upgrade plain DB: mode = %v, want IntegrityNone", db4.IntegrityMode())
	}
	_ = db4.Close()
}

func TestCksum_CannotCombineWithEncryption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	opts := DefaultOptions()
	opts.Checksum = true
	opts.Key = []byte("passphrase")
	if _, err := Open(path, opts); err == nil {
		t.Fatal("expected error: Checksum + Key both set")
	} else if !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestCksum_TamperDetected mirrors TestEncryption_TamperDetected: write data,
// close, flip a body byte on disk, reopen and read — checksum mismatch must
// surface as an error. Modeled on encryption_integration_test.go:344-426.
func TestCksum_TamperDetected(t *testing.T) {
	// Opens a default-4096 DB via raw Open. Reset the process-global page
	// buffer pool first so a predecessor that leaked a non-4096 pool size
	// (corrupt-DB tests at 512/1024) does not make this Open fail with
	// ErrPageBufferPoolSizeMismatch under -shuffle=on. See helpers_test.go.
	resetPoolForTest(t)
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
