package btree

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// loadDB creates a test database at path, writes 50 entries into namespace
// "x", checkpoints to flush WAL into the main DB file, and closes. Used as
// a setup for sweep tests.
//
// Calls resetPageBufferPool() so that running this in a suite alongside
// other tests that opened with a different page size does not produce
// ErrPageBufferPoolSizeMismatch on Open.
func setupSweepDB(t *testing.T, path string, opts Options) {
	t.Helper()
	resetPageBufferPool()
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
}

func TestVerifyIntegrity_Cksum_AllGood(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	opts := DefaultOptions()
	opts.Checksum = true
	opts.InProcess = true
	setupSweepDB(t, path, opts)

	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Mode != IntegrityChecksum {
		t.Fatalf("Mode = %v, want IntegrityChecksum", rep.Mode)
	}
	if rep.Pages == 0 {
		t.Fatal("Pages = 0")
	}
	if len(rep.Errors) != 0 {
		t.Fatalf("unexpected errors: %+v", rep.Errors)
	}
}

func TestVerifyIntegrity_AEAD_AllGood(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	opts := DefaultOptions()
	opts.Key = []byte("p")
	opts.KDFIterations = 1000
	opts.InProcess = true
	setupSweepDB(t, path, opts)

	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Mode != IntegrityAEAD {
		t.Fatalf("Mode = %v, want IntegrityAEAD", rep.Mode)
	}
	if rep.Pages == 0 {
		t.Fatal("Pages = 0")
	}
	if len(rep.Errors) != 0 {
		t.Fatalf("unexpected errors: %+v", rep.Errors)
	}
}

func TestVerifyIntegrity_Plain_NoMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	opts := DefaultOptions()
	opts.InProcess = true
	setupSweepDB(t, path, opts)

	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Mode != IntegrityNone {
		t.Fatalf("Mode = %v, want IntegrityNone", rep.Mode)
	}
	if rep.Pages != 0 {
		t.Fatalf("plain DB should report 0 pages scanned, got %d", rep.Pages)
	}
}

func TestVerifyIntegrity_Cksum_DetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	opts := DefaultOptions()
	opts.Checksum = true
	opts.InProcess = true
	setupSweepDB(t, path, opts)

	// Corrupt page 2 body on disk.
	pageSize := int64(opts.PageSize)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[pageSize+200] ^= 0x01
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Sweep — VerifyIntegrity reads pages raw, past the codec, so this
	// must succeed and accumulate the error.
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatalf("VerifyIntegrity: %v", err)
	}
	if len(rep.Errors) != 1 {
		t.Fatalf("want 1 error, got %d: %+v", len(rep.Errors), rep.Errors)
	}
	if rep.Errors[0].PageNo != 2 {
		t.Fatalf("Errors[0].PageNo = %d, want 2", rep.Errors[0].PageNo)
	}
	if rep.Errors[0].Kind != IntegrityChecksumMismatch {
		t.Fatalf("Errors[0].Kind = %v, want IntegrityChecksumMismatch", rep.Errors[0].Kind)
	}
}

func TestVerifyIntegrity_AEAD_DetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	opts := DefaultOptions()
	opts.Key = []byte("p")
	opts.KDFIterations = 1000
	opts.InProcess = true
	setupSweepDB(t, path, opts)

	pageSize := int64(opts.PageSize)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[pageSize+200] ^= 0x01
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatalf("VerifyIntegrity: %v", err)
	}
	if len(rep.Errors) == 0 {
		t.Fatal("expected at least one AEAD auth fail")
	}
	saw := false
	for _, e := range rep.Errors {
		if e.Kind == IntegrityAEADAuthFail && e.PageNo == 2 {
			saw = true
			break
		}
	}
	if !saw {
		t.Fatalf("want IntegrityAEADAuthFail on page 2; got %+v", rep.Errors)
	}
}

// TestVerifyIntegrity_DamagedPage1Listed: the sweep takes its page count
// from page 1's header, which is stored outside the codec. Damage to the
// rest of page 1 is a page error of the report, not the sweep's error.
func TestVerifyIntegrity_DamagedPage1Listed(t *testing.T) {
	cksum := DefaultOptions()
	cksum.Checksum = true
	cksum.InProcess = true
	aead := DefaultOptions()
	aead.Key = []byte("p")
	aead.KDFIterations = 1000
	aead.InProcess = true
	cases := []struct {
		name string
		opts Options
		kind IntegrityErrorKind
	}{
		{name: "checksum", opts: cksum, kind: IntegrityChecksumMismatch},
		{name: "aead", opts: aead, kind: IntegrityAEADAuthFail},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			setupSweepDB(t, path, tc.opts)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			pages := len(data) / int(tc.opts.PageSize)

			// The page is damaged under the open database: an open under a
			// key authenticates page 1 and refuses a damaged one
			// (readPage1AtOpen).
			db, err := Open(path, tc.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.WriteAt([]byte{data[dbHeaderSize+100] ^ 0x01}, dbHeaderSize+100)
			if cerr := f.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				t.Fatal(err)
			}
			rep, err := db.VerifyIntegrity(context.Background())
			if err != nil {
				t.Fatalf("VerifyIntegrity: %v", err)
			}
			if rep.Pages != pages {
				t.Fatalf("Pages = %d, want %d", rep.Pages, pages)
			}
			if len(rep.Errors) != 1 || rep.Errors[0].PageNo != 1 || rep.Errors[0].Kind != tc.kind {
				t.Fatalf("want one error of kind %v on page 1, got %+v", tc.kind, rep.Errors)
			}
		})
	}
}

// TestVerifyIntegrity_InMemoryHasNoMode: the in-memory store and the
// in-memory WAL hold pages as the b-tree wrote them, with no codec
// (wal.writeFramesMem), even when the options name one. The integrity mode
// is that of the stored pages: none.
func TestVerifyIntegrity_InMemoryHasNoMode(t *testing.T) {
	cksum := Options{PageSize: 4096, InMemory: true, Checksum: true}
	aead := Options{PageSize: 4096, InMemory: true, Key: []byte("p"), KDFIterations: 1000}
	for name, opts := range map[string]Options{"checksum": cksum, "aead": aead} {
		t.Run(name, func(t *testing.T) {
			resetPageBufferPool()
			db, err := Open("", opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			wtx, err := db.BeginWrite()
			if err != nil {
				t.Fatal(err)
			}
			ns, err := wtx.CreateNamespace("x")
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 200; i++ {
				if err := wtx.Put(ns, []byte{byte(i)}, bytes.Repeat([]byte{byte(i)}, 300)); err != nil {
					t.Fatal(err)
				}
			}
			if err := wtx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(CheckpointFull); err != nil {
				t.Fatal(err)
			}
			if m := db.IntegrityMode(); m != IntegrityNone {
				t.Fatalf("IntegrityMode = %v, want IntegrityNone", m)
			}
			rep, err := db.VerifyIntegrity(context.Background())
			if err != nil {
				t.Fatalf("VerifyIntegrity: %v", err)
			}
			if rep.Mode != IntegrityNone || rep.Pages != 0 || len(rep.Errors) != 0 {
				t.Fatalf("want no mode, no pages, no errors; got mode %v, %d pages, %d errors", rep.Mode, rep.Pages, len(rep.Errors))
			}
		})
	}
}
