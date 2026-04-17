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

// encTestSetup resets the global page buffer pool so this test can Open a
// database with its own PageSize without conflicting with a prior test's
// configuration. Uses a Cleanup to reset again at teardown for hygiene.
func encTestSetup(t testing.TB) {
	t.Helper()
	resetPageBufferPool()
}

func TestOpen_KeyRoundTrip(t *testing.T) {
	encTestSetup(t)
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
	encTestSetup(t)
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
	encTestSetup(t)
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
	encTestSetup(t)
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
	encTestSetup(t)
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
	// 10,000 keys × (4-byte key + 200-byte value) ≈ 2 MB of payload. At
	// 4 KB pages with a 50-page cache (~200 KB), pagerStress spill is
	// unavoidable — the test exercises encrypted-WAL spill plus encrypted
	// checkpoint plus reopen-and-decrypt.
	for i := 0; i < n; i++ {
		k := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		v, err := rtx.Get(ns2, k)
		if err != nil {
			t.Fatalf("Get key %x: %v", k, err)
		}
		want := bytes.Repeat([]byte{byte(i)}, 200)
		if !bytes.Equal(v, want) {
			t.Fatalf("wrong value for key %x: len=%d first-mismatch-at=%d",
				k, len(v), firstDiff(v, want))
		}
	}
}

func firstDiff(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func BenchmarkCommit_Plain(b *testing.B) {
	benchCommit(b, nil)
}

func BenchmarkCommit_Encrypted(b *testing.B) {
	benchCommit(b, []byte("benchmark-passphrase-used-for-pbkdf2"))
}

func benchCommit(b *testing.B, key []byte) {
	encTestSetup(b)
	path := filepath.Join(b.TempDir(), "bench.db")
	opts := DefaultOptions()
	opts.NoCommitSync = true // measure CPU, not fsync
	opts.InProcess = true
	if key != nil {
		opts.Key = key
		opts.KDFIterations = 1000
	}
	db, err := Open(path, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Seed the namespace once so each iteration just Puts values.
	seed, err := db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	if _, err := seed.CreateNamespace("ns"); err != nil {
		b.Fatal(err)
	}
	if err := seed.Commit(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		nsTx, err := tx.GetNamespace("ns")
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 100; j++ {
			k := []byte{byte(i), byte(j), byte(j >> 8)}
			if err := tx.Put(nsTx, k, bytes.Repeat([]byte{byte(j)}, 500)); err != nil {
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestEncryption_TamperDetected(t *testing.T) {
	encTestSetup(t)
	path := tmpEncryptedFile(t, "tamper.db")
	opts := DefaultOptions()
	opts.Key = []byte("tamper-test")
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

	// Flip a byte well past the plaintext header (>= 200) so the tampered
	// region is ciphertext.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < int(opts.PageSize)+200 {
		t.Fatalf("file too small: %d bytes", len(data))
	}
	// Tamper inside page 2's ciphertext body.
	off := int(opts.PageSize) + 200
	data[off] ^= 0x01
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Reopen and try to read. The codec MUST surface an error — no crash,
	// no silent corruption. We don't constrain where the error surfaces
	// (open, begin-read, or the first Get touching the tampered page).
	db2, err := Open(path, opts)
	if err != nil {
		return // clean failure at Open is acceptable
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
		t.Fatalf("no error observed after tampering page 2 body")
	}
}
