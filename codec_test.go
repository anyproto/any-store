package anystore

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cipher round-trips and the passphrase-derived key path behind Config.
// The end-to-end encryption behaviour these drive is codec.go's public
// surface: the CipherType values and EncryptionConfig.

func TestEncryption_RoundTrip(t *testing.T) {
	dir, err := os.MkdirTemp("", "anystore-enc-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "enc.db")

	cfg := &Config{
		Encryption: EncryptionConfig{
			Passphrase:    []byte("my-secret-passphrase"),
			KDFIterations: 1000, // low cost for test speed
		},
	}

	// Create + write.
	db, err := Open(context.Background(), path, cfg)
	require.NoError(t, err)
	coll, err := db.CreateCollection(context.Background(), "users")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(context.Background(), anyenc.MustParseJson(`{"id":"u1","name":"Alice"}`)))
	require.NoError(t, db.Close())

	// Reopen with correct passphrase.
	db2, err := Open(context.Background(), path, cfg)
	require.NoError(t, err)
	coll2, err := db2.OpenCollection(context.Background(), "users")
	require.NoError(t, err)
	got, err := coll2.FindId(context.Background(), "u1")
	require.NoError(t, err)
	assert.Equal(t, `{"id":"u1","name":"Alice"}`, got.Value().String())
	require.NoError(t, db2.Close())
}

func TestEncryption_DisabledByDefault(t *testing.T) {
	var cfg EncryptionConfig
	assert.False(t, cfg.Enabled())
	cfg.Passphrase = []byte("x")
	assert.True(t, cfg.Enabled())
}

func TestEncryption_AllCipherTypes_RoundTrip(t *testing.T) {
	for _, ct := range []CipherType{CipherAES256GCM, CipherChaCha20Poly1305, CipherXChaCha20Poly1305} {
		t.Run(string(ct)+"-default", func(t *testing.T) {
			dir, err := os.MkdirTemp("", "anystore-enc-*")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			path := filepath.Join(dir, "enc.db")

			cfg := &Config{
				Encryption: EncryptionConfig{
					Passphrase:    []byte("pass-" + string(ct)),
					KDFIterations: 1000,
					CipherType:    ct,
				},
			}
			db, err := Open(context.Background(), path, cfg)
			require.NoError(t, err)
			coll, err := db.CreateCollection(context.Background(), "c")
			require.NoError(t, err)
			require.NoError(t, coll.Insert(context.Background(), anyenc.MustParseJson(`{"id":"x","v":1}`)))
			require.NoError(t, db.Close())

			db2, err := Open(context.Background(), path, cfg)
			require.NoError(t, err)
			defer db2.Close()
			coll2, err := db2.OpenCollection(context.Background(), "c")
			require.NoError(t, err)
			got, err := coll2.FindId(context.Background(), "x")
			require.NoError(t, err)
			assert.Equal(t, `{"id":"x","v":1}`, got.Value().String())
		})
	}
}

// encryptedDoc is the document number i of an encrypted-database test, padded
// so that a few of them fill a page.
func encryptedDoc(prefix string, i int) *anyenc.Value {
	return anyenc.MustParseJson(fmt.Sprintf(`{"id":"%s-%d","pad":"%0300d"}`, prefix, i, i))
}

// requireEncryptedDocs reads every document of collection "c" in the
// database at path, which must hold the ids prefix-0..prefix-(n-1) and,
// only when moreOK, other documents besides.
func requireEncryptedDocs(t *testing.T, path string, cfg *Config, prefix string, n int, moreOK bool) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, path, cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	coll, err := db.OpenCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		got, err := coll.FindId(ctx, fmt.Sprintf("%s-%d", prefix, i))
		require.NoError(t, err)
		require.Equal(t, encryptedDoc(prefix, i).String(), got.Value().String())
	}
	count, err := coll.Count(ctx)
	require.NoError(t, err)
	if !moreOK {
		require.Equal(t, n, count)
		return
	}
	// The other documents decrypt too: walk the whole collection.
	iter, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	walked := 0
	for iter.Next() {
		_, err := iter.Doc()
		require.NoError(t, err)
		walked++
	}
	require.NoError(t, iter.Err())
	require.Equal(t, count, walked)
	require.GreaterOrEqual(t, count, n)
}

// A database encrypted with a 32-byte key backed up while it is written to:
// the source's writer and the destination's encrypt pages at the same time.
// Run it with -race.
//
// A 32-byte key is used as it is, with no KDF, and the default page size and
// no checksum mode give the destination the source's page geometry: the
// copy, which has a salt of its own, opens with the same Config.
func TestEncryption_BackupWhileWriting(t *testing.T) {
	ctx := context.Background()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	// Uncompressed, so that the documents below fill hundreds of pages.
	cfg := &Config{Encryption: EncryptionConfig{Passphrase: key}, DisableCompression: true}

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")

	db, err := Open(ctx, srcPath, cfg)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	// Enough pages that the copy takes more than one Step and its commit
	// writes many frames.
	const pre = 3000
	batch := make([]*anyenc.Value, 0, 500)
	for i := 0; i < pre; i++ {
		batch = append(batch, encryptedDoc("pre", i))
		if len(batch) == cap(batch) {
			require.NoError(t, coll.Insert(ctx, batch...))
			batch = batch[:0]
		}
	}

	// The writer commits one document at a time for the whole backup, and
	// pauses briefly after each commit. A Step is busy while the source is in
	// a write transaction (sqlite3_backup_step, backup.c:337-341), so the
	// pause leaves each retry of a busy Step a gap to copy in.
	stop := make(chan struct{})
	running := make(chan struct{})
	writerDone := make(chan struct{})
	var writerErr error
	go func() {
		defer close(writerDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := coll.Insert(ctx, encryptedDoc("w", i)); err != nil {
				writerErr = err
				return
			}
			if i == 10 {
				close(running)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-running:
	case <-writerDone:
		require.NoError(t, writerErr)
	}

	// A busy Backup is the busy timeout running out, which heavy load can
	// still cause. The test is about the race between the two writers, so a
	// busy Backup is run again.
	//
	// The stepped hook runs after each Step that copied and left pages to
	// copy, so one call in the attempt that succeeded shows that its copy
	// took more than one Step.
	var steps int
	testBackupSteppedHook = func() { steps++ }
	t.Cleanup(func() { testBackupSteppedHook = nil })
	var backupErr error
	for attempt := 0; attempt < 5; attempt++ {
		steps = 0
		if backupErr = db.Backup(ctx, dstPath); !errors.Is(backupErr, ErrBusy) {
			break
		}
	}
	close(stop)
	<-writerDone
	require.NoError(t, writerErr)
	require.NoError(t, backupErr)
	require.NoError(t, db.Close())
	require.GreaterOrEqual(t, steps, 1, "the copy must take more than one Step")

	requireEncryptedDocs(t, dstPath, cfg, "pre", pre, true)
}

func BenchmarkAnystore_Insert_Plain(b *testing.B) {
	benchAnystoreInsert(b, EncryptionConfig{})
}

func BenchmarkAnystore_Insert_AES256GCM(b *testing.B) {
	benchAnystoreInsert(b, EncryptionConfig{
		Passphrase:    []byte("bench-pw"),
		KDFIterations: 1000,
		CipherType:    CipherAES256GCM,
	})
}

func BenchmarkAnystore_Insert_ChaCha20Poly1305(b *testing.B) {
	benchAnystoreInsert(b, EncryptionConfig{
		Passphrase:    []byte("bench-pw"),
		KDFIterations: 1000,
		CipherType:    CipherChaCha20Poly1305,
	})
}

func BenchmarkAnystore_Insert_XChaCha20Poly1305(b *testing.B) {
	benchAnystoreInsert(b, EncryptionConfig{
		Passphrase:    []byte("bench-pw"),
		KDFIterations: 1000,
		CipherType:    CipherXChaCha20Poly1305,
	})
}

func benchAnystoreInsert(b *testing.B, enc EncryptionConfig) {
	b.Helper()
	dir, err := os.MkdirTemp("", "anystore-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "bench.db")

	cfg := &Config{Encryption: enc}
	ctx := context.Background()
	db, err := Open(ctx, path, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	coll, err := db.CreateCollection(ctx, "c")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		docs := make([]*anyenc.Value, 100)
		for j := 0; j < 100; j++ {
			docs[j] = anyenc.MustParseJson(
				fmt.Sprintf(`{"id":"%d-%d","name":"bench","payload":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`, i, j),
			)
		}
		if err := coll.Insert(ctx, docs...); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAnystore_Find exercises the read path under each cipher.
func BenchmarkAnystore_Find_Plain(b *testing.B) {
	benchAnystoreFind(b, EncryptionConfig{})
}

func BenchmarkAnystore_Find_AES256GCM(b *testing.B) {
	benchAnystoreFind(b, EncryptionConfig{
		Passphrase: []byte("bench-pw"), KDFIterations: 1000, CipherType: CipherAES256GCM,
	})
}

func BenchmarkAnystore_Find_ChaCha20Poly1305(b *testing.B) {
	benchAnystoreFind(b, EncryptionConfig{
		Passphrase: []byte("bench-pw"), KDFIterations: 1000, CipherType: CipherChaCha20Poly1305,
	})
}

func BenchmarkAnystore_Find_XChaCha20Poly1305(b *testing.B) {
	benchAnystoreFind(b, EncryptionConfig{
		Passphrase: []byte("bench-pw"), KDFIterations: 1000, CipherType: CipherXChaCha20Poly1305,
	})
}

func benchAnystoreFind(b *testing.B, enc EncryptionConfig) {
	b.Helper()
	dir, err := os.MkdirTemp("", "anystore-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "bench.db")

	ctx := context.Background()
	db, err := Open(ctx, path, &Config{Encryption: enc})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	coll, err := db.CreateCollection(ctx, "c")
	if err != nil {
		b.Fatal(err)
	}
	const n = 1000
	docs := make([]*anyenc.Value, n)
	for i := 0; i < n; i++ {
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":"%d","v":%d}`, i, i))
	}
	if err := coll.Insert(ctx, docs...); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("%d", i%n)
		if _, err := coll.FindId(ctx, id); err != nil {
			b.Fatal(err)
		}
	}
}
