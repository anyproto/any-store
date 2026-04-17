package btree

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func tmpEncryptedFile(t *testing.T, name string) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, name)
}

func TestOpen_KeyRoundTrip(t *testing.T) {
	path := tmpEncryptedFile(t, "enc.db")
	opts := DefaultOptions()
	opts.Key = []byte("correct horse battery staple")
	opts.KDFIterations = 1000
	opts.InProcess = true

	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open (create): %v", err)
	}
	wtx, err := db.BeginWrite()
	if err != nil {
		t.Fatalf("BeginWrite: %v", err)
	}
	ns, err := wtx.CreateNamespace("default")
	if err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	if err := wtx.Put(ns, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := wtx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := db.Checkpoint(CheckpointFull); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with correct key.
	db2, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open (reopen): %v", err)
	}
	defer db2.Close()
	rtx, err := db2.BeginRead()
	if err != nil {
		t.Fatalf("BeginRead: %v", err)
	}
	defer rtx.Rollback()
	ns2, err := db2.GetNamespace("default")
	if err != nil {
		t.Fatalf("GetNamespace: %v", err)
	}
	val, err := rtx.Get(ns2, []byte("k1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("wrong value: got %q want v1", val)
	}
}

func TestOpen_WrongKeyFails(t *testing.T) {
	path := tmpEncryptedFile(t, "enc.db")
	opts := DefaultOptions()
	opts.Key = []byte("right-passphrase")
	opts.KDFIterations = 1000
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
	if err := wtx.Put(ns, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
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

	opts.Key = []byte("wrong-passphrase")
	db2, err := Open(path, opts)
	if err != nil {
		// Clean fail at open — acceptable.
		return
	}
	// Open succeeded; a subsequent page read should surface the error.
	defer db2.Close()
	_, gnErr := db2.GetNamespace("x")
	if gnErr == nil {
		t.Fatalf("GetNamespace with wrong key returned nil error")
	}
}

func TestOpen_MissingKeyOnEncryptedFile(t *testing.T) {
	path := tmpEncryptedFile(t, "enc.db")
	opts := DefaultOptions()
	opts.Key = []byte("pw")
	opts.KDFIterations = 1000
	opts.InProcess = true
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	wtx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wtx.CreateNamespace("x"); err != nil {
		t.Fatal(err)
	}
	if err := wtx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen without any key — must fail.
	opts.Key = nil
	_, err = Open(path, opts)
	if err == nil {
		t.Fatalf("Open encrypted DB without key returned nil error")
	}
}

func TestOpen_KeyOnPlainFile(t *testing.T) {
	path := tmpEncryptedFile(t, "plain.db")
	opts := DefaultOptions()
	opts.InProcess = true
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	wtx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wtx.CreateNamespace("x"); err != nil {
		t.Fatal(err)
	}
	if err := wtx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with key — must fail.
	opts.Key = []byte("pw")
	opts.KDFIterations = 1000
	_, err = Open(path, opts)
	if err == nil {
		t.Fatalf("Open plain DB with key returned nil error")
	}
}

func TestEncryption_SpillCheckpointReopen(t *testing.T) {
	path := tmpEncryptedFile(t, "spill.db")
	opts := DefaultOptions()
	opts.Key = []byte("spill-test")
	opts.KDFIterations = 1000
	opts.CacheSize = 50 // tiny cache to force spill
	opts.InProcess = true

	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}

	const n = 10_000
	wtx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	ns, err := wtx.CreateNamespace("ns")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		k := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		v := bytes.Repeat([]byte{byte(i)}, 200)
		if err := wtx.Put(ns, k, v); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if err := wtx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := db.Checkpoint(CheckpointFull); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = Open(path, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	rtx, err := db.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()
	ns2, err := db.GetNamespace("ns")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		k := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		v, err := rtx.Get(ns2, k)
		if err != nil {
			t.Fatalf("Get key %x: %v", k, err)
		}
		if len(v) != 200 || v[0] != byte(i) {
			t.Fatalf("wrong value for key %x: len=%d v[0]=%d", k, len(v), v[0])
		}
	}
}

// Make sure os is used even if the rest of the test file does not reference it.
var _ = os.Open
