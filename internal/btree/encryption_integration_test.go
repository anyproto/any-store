package btree

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
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
	opts.Passphrase = []byte("correct horse battery staple")
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
	opts.Passphrase = []byte("right-passphrase")
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

	opts.Passphrase = []byte("wrong-passphrase")
	if _, err := Open(path, opts); !errors.Is(err, ErrWrongKey) {
		t.Fatalf("Open with the wrong key: got %v, want ErrWrongKey", err)
	}
}

// TestOpen_PassphraseKeyDerivedOncePerOpen: an Open with a passphrase derives
// its key once, when it creates the database (at a path with no file, or
// with an empty one) and when it opens the database again. The open that
// creates the database writes into page 1 the salt its key was derived
// from, so the header it reads under the DB-file lock leaves that key in
// place. SQLCipher derives the key of a codec context once, at its first
// use (sqlcipher_codec_key_derive).
func TestOpen_PassphraseKeyDerivedOncePerOpen(t *testing.T) {
	var derived atomic.Int32
	testDeriveKeyHook = func() { derived.Add(1) }
	t.Cleanup(func() { testDeriveKeyHook = nil })

	for _, inProcess := range []bool{true, false} {
		for _, emptyFile := range []bool{false, true} {
			name := "multi-process"
			if inProcess {
				name = "in-process"
			}
			if emptyFile {
				name += "/empty-file"
			} else {
				name += "/no-file"
			}
			t.Run(name, func(t *testing.T) {
				path := tmpEncryptedFile(t, "enc.db")
				if emptyFile {
					require.NoError(t, os.WriteFile(path, nil, 0o644))
				}
				opts := DefaultOptions()
				opts.Passphrase = []byte("correct horse battery staple")
				opts.KDFIterations = 1000
				opts.InProcess = inProcess

				derived.Store(0)
				db, err := testOpen(t, path, opts)
				require.NoError(t, err)
				require.EqualValues(t, 1, derived.Load(), "key derivations of the open that created the database")
				tx, err := db.BeginWrite()
				require.NoError(t, err)
				ns, err := tx.CreateNamespace("x")
				require.NoError(t, err)
				require.NoError(t, tx.Put(ns, []byte("k"), []byte("v")))
				require.NoError(t, tx.Commit())
				require.NoError(t, db.Close())

				derived.Store(0)
				db, err = testOpen(t, path, opts)
				require.NoError(t, err)
				defer func() { _ = db.Close() }()
				require.EqualValues(t, 1, derived.Load(), "key derivations of an open of the database")
				ns, err = db.GetNamespace("x")
				require.NoError(t, err)
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				defer rtx.Rollback()
				val, err := rtx.Get(ns, []byte("k"))
				require.NoError(t, err)
				require.Equal(t, []byte("v"), val)
			})
		}
	}
}

func TestOpen_MissingKeyOnEncryptedFile(t *testing.T) {
	encTestSetup(t)
	path := tmpEncryptedFile(t, "enc.db")
	opts := DefaultOptions()
	opts.Passphrase = []byte("pw")
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
	opts.Passphrase = nil
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
	opts.Passphrase = []byte("pw")
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
	opts.Passphrase = []byte("spill-test")
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
	benchCommit(b, nil, "")
}

func BenchmarkCommit_AES256GCM(b *testing.B) {
	benchCommit(b, []byte("benchmark-passphrase-used-for-pbkdf2"), CipherAES256GCM)
}

func BenchmarkCommit_ChaCha20Poly1305(b *testing.B) {
	benchCommit(b, []byte("benchmark-passphrase-used-for-pbkdf2"), CipherChaCha20Poly1305)
}

func BenchmarkCommit_XChaCha20Poly1305(b *testing.B) {
	benchCommit(b, []byte("benchmark-passphrase-used-for-pbkdf2"), CipherXChaCha20Poly1305)
}

func benchCommit(b *testing.B, key []byte, cipher CipherType) {
	encTestSetup(b)
	path := filepath.Join(b.TempDir(), "bench.db")
	opts := DefaultOptions()
	opts.NoCommitSync = true // measure CPU, not fsync
	opts.InProcess = true
	if key != nil {
		opts.Passphrase = key
		opts.KDFIterations = 1000
		opts.CipherType = cipher
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
	opts.Passphrase = []byte("tamper-test")
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

// requireCodecsShareKey seals a page with one codec and opens it with the
// other: it opens when both were made from one key, and fails its
// authentication when they were not.
func requireCodecsShareKey(t *testing.T, seal, open Codec, same bool) {
	t.Helper()
	const pageSize = 4096
	src := bytes.Repeat([]byte{0xA5}, pageSize)
	ct := make([]byte, pageSize)
	pt := make([]byte, pageSize)
	var w, r aeadScratch
	_, err := seal.Encrypt(ct, src, 2, &w)
	require.NoError(t, err)
	_, err = open.Decrypt(pt, ct, 2, &r, nil)
	if !same {
		require.ErrorIs(t, err, ErrPageIntegrity)
		return
	}
	require.NoError(t, err)
	body := pageSize - seal.Overhead()
	require.Equal(t, src[:body], pt[:body])
}

// Options.Key is a raw key, used as it is, whatever KDFIterations says.
// Options.Passphrase is always stretched with PBKDF2, one of KeyLen bytes
// too, so the same bytes in the two fields are two keys. The field says
// which a secret is, never its length: a Key of another length is refused,
// and so are both fields set and an empty passphrase.
func TestBuildCodec_KeyIsRawPassphraseIsDerived(t *testing.T) {
	var derived atomic.Int32
	testDeriveKeyHook = func() { derived.Add(1) }
	t.Cleanup(func() { testDeriveKeyHook = nil })
	salt := bytes.Repeat([]byte{0x5a}, SaltLen)
	secret := bytes.Repeat([]byte{0x42}, KeyLen)
	raw, err := NewAESCodec(secret)
	require.NoError(t, err)
	stretched, err := NewAESCodec(DeriveKey(secret, salt, 1000))
	require.NoError(t, err)

	for _, iter := range []int{0, 1000, math.MaxInt} {
		c, err := buildCodec(Options{Key: secret, KDFIterations: iter}, salt, false)
		require.NoError(t, err)
		requireCodecsShareKey(t, c, raw, true)
	}
	require.Zero(t, derived.Load(), "a raw key is not derived")

	c, err := buildCodec(Options{Passphrase: secret, KDFIterations: 1000}, salt, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, derived.Load(), "a passphrase of KeyLen bytes is derived")
	requireCodecsShareKey(t, c, stretched, true)
	requireCodecsShareKey(t, c, raw, false)

	derived.Store(0)
	for _, tc := range []struct {
		name string
		opts Options
		msg  string
	}{
		{"empty key", Options{Key: []byte{}}, "Options.Key is 0 bytes"},
		{"key one byte short", Options{Key: secret[:KeyLen-1]}, "Options.Key is 31 bytes"},
		{"key one byte long", Options{Key: append(bytes.Clone(secret), 0)}, "Options.Key is 33 bytes"},
		{"key and passphrase", Options{Key: secret, Passphrase: []byte("pw")}, "both set"},
		{"empty passphrase", Options{Passphrase: []byte{}}, "Options.Passphrase is empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildCodec(tc.opts, salt, false)
			require.ErrorContains(t, err, tc.msg)
		})
	}
	for _, opts := range []Options{{Key: secret}, {Passphrase: secret}} {
		_, err := buildCodec(opts, salt, true)
		require.ErrorContains(t, err, "cannot be combined")
	}
	require.Zero(t, derived.Load(), "a refused option derives nothing")
}

// encFileState is the content of a database file and of its WAL ("" when
// there is none).
func encFileState(t *testing.T, path string) [2]string {
	t.Helper()
	var st [2]string
	for i, p := range []string{path, path + "-wal"} {
		b, err := os.ReadFile(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		require.NoError(t, err)
		st[i] = string(b)
	}
	return st
}

// A database created with a Key opens with that Key, and one created with a
// Passphrase of KeyLen bytes with that Passphrase; the same bytes in the
// other field are a wrong key, which leaves the file as it was. A file
// created with a raw key carries a salt all the same, its encryption marker.
func TestOpen_KeyAndPassphraseOfTheSameBytesAreTwoKeys(t *testing.T) {
	encTestSetup(t)
	secret := bytes.Repeat([]byte{0x42}, KeyLen)
	asKey := DefaultOptions()
	asKey.InProcess = true
	asKey.Key = secret
	asPassphrase := DefaultOptions()
	asPassphrase.InProcess = true
	asPassphrase.Passphrase = secret
	asPassphrase.KDFIterations = 1000

	for _, tc := range []struct {
		name          string
		create, other Options
	}{
		{"created with Key", asKey, asPassphrase},
		{"created with Passphrase", asPassphrase, asKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := tmpEncryptedFile(t, "enc.db")
			db, err := Open(path, tc.create)
			require.NoError(t, err)
			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := wtx.CreateNamespace("x")
			require.NoError(t, err)
			for i := 0; i < 50; i++ {
				require.NoError(t, wtx.Put(ns, []byte(fmt.Sprintf("k%03d", i)), bytes.Repeat([]byte("v"), 500)))
			}
			require.NoError(t, wtx.Commit())
			require.NoError(t, db.Close())
			hdr, err := readInitialHeader(path)
			require.NoError(t, err)
			require.NotEqual(t, [SaltLen]byte{}, hdr.Salt, "an encrypted file carries a salt")

			before := encFileState(t, path)
			_, err = Open(path, tc.other)
			require.ErrorIs(t, err, ErrWrongKey)
			require.NotErrorIs(t, err, ErrCorrupt)
			require.Equal(t, before, encFileState(t, path), "a wrong key leaves the file as it was")

			db, err = Open(path, tc.create)
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			ns, err = db.GetNamespace("x")
			require.NoError(t, err)
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer rtx.Rollback()
			v, err := rtx.Get(ns, []byte("k049"))
			require.NoError(t, err)
			require.Equal(t, bytes.Repeat([]byte("v"), 500), v)
		})
	}
}

// A Key that is not KeyLen bytes, or set with a Passphrase, is refused
// before a new file is created.
func TestOpen_BadKeyCreatesNoFile(t *testing.T) {
	encTestSetup(t)
	for _, opts := range []Options{
		{Key: []byte{}},
		{Key: make([]byte, KeyLen-1)},
		{Key: make([]byte, KeyLen+1)},
		{Key: make([]byte, KeyLen), Passphrase: []byte("pw")},
	} {
		path := tmpEncryptedFile(t, "enc.db")
		opts.PageSize = DefaultPageSize
		opts.InProcess = true
		_, err := Open(path, opts)
		require.Error(t, err)
		_, statErr := os.Stat(path)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	}
}
