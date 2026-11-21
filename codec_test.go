package anystore

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cipher round-trips and the passphrase-derived key path behind Config.
// The end-to-end encryption behaviour these drive is codec.go's public
// surface: NewAESCodec, NewChaCha20Poly1305Codec, NewXChaCha20Poly1305Codec
// and DeriveKey.

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

func TestEncryption_WrongPassphraseFails(t *testing.T) {
	dir, err := os.MkdirTemp("", "anystore-enc-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "enc.db")

	cfg := &Config{
		Encryption: EncryptionConfig{
			Passphrase:    []byte("right"),
			KDFIterations: 1000,
		},
	}
	db, err := Open(context.Background(), path, cfg)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	cfg.Encryption.Passphrase = []byte("wrong")
	db2, err := Open(context.Background(), path, cfg)
	// Either Open itself or the first query fails — both acceptable.
	if err == nil {
		defer db2.Close()
		_, openErr := db2.OpenCollection(context.Background(), "any")
		assert.Error(t, openErr, "expected failure when opening collection with wrong key")
	}
}

func TestEncryption_MissingPassphraseFails(t *testing.T) {
	dir, err := os.MkdirTemp("", "anystore-enc-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "enc.db")

	cfg := &Config{
		Encryption: EncryptionConfig{
			Passphrase:    []byte("pw"),
			KDFIterations: 1000,
		},
	}
	db, err := Open(context.Background(), path, cfg)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Reopen without passphrase.
	_, err = Open(context.Background(), path, &Config{})
	assert.Error(t, err)
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

func TestEncryption_BringYourOwnCodec(t *testing.T) {
	// BYO codec path: user constructs the codec themselves with an
	// externally-derived 32-byte key. Skips Passphrase/KDFIterations.
	dir, err := os.MkdirTemp("", "anystore-enc-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "enc.db")

	// Derive a key however you like — here we use DeriveKey exposed at
	// the anystore level; an HSM-backed codec would compute this differently.
	salt := []byte("sixteen-byte-slt")
	require.Len(t, salt, 16)
	key := DeriveKey([]byte("my-pass"), salt, 1000)
	codec, err := NewChaCha20Poly1305Codec(key)
	require.NoError(t, err)

	cfg := &Config{
		Encryption: EncryptionConfig{Codec: codec},
	}
	db, err := Open(context.Background(), path, cfg)
	require.NoError(t, err)
	coll, err := db.CreateCollection(context.Background(), "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(context.Background(), anyenc.MustParseJson(`{"id":"k","v":"hello"}`)))
	require.NoError(t, db.Close())

	// Reopen with the same codec (user reconstructs it from the same key).
	codec2, err := NewChaCha20Poly1305Codec(key)
	require.NoError(t, err)
	cfg.Encryption = EncryptionConfig{Codec: codec2}
	db2, err := Open(context.Background(), path, cfg)
	require.NoError(t, err)
	defer db2.Close()
	coll2, err := db2.OpenCollection(context.Background(), "c")
	require.NoError(t, err)
	got, err := coll2.FindId(context.Background(), "k")
	require.NoError(t, err)
	assert.Equal(t, `{"id":"k","v":"hello"}`, got.Value().String())
}

// sharedCodecDoc is the document number i of a shared-codec test, padded so
// that a few of them fill a page.
func sharedCodecDoc(prefix string, i int) *anyenc.Value {
	return anyenc.MustParseJson(fmt.Sprintf(`{"id":"%s-%d","pad":"%0300d"}`, prefix, i, i))
}

// requireSharedCodecDocs reads every document of collection "c" in the
// database at path, which must hold the ids prefix-0..prefix-(n-1) and,
// only when moreOK, other documents besides.
func requireSharedCodecDocs(t *testing.T, path string, cfg *Config, prefix string, n int, moreOK bool) {
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
		require.Equal(t, sharedCodecDoc(prefix, i).String(), got.Value().String())
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

// One codec object in Config.Encryption.Codec serving two databases that
// write at the same time. Each database encrypts its pages through it from
// its own writer, so the codec must not keep per-encrypt state of its own
// (a shared nonce source repeats nonces under one key). Run it with -race.
func TestEncryption_SharedCodecTwoDatabases(t *testing.T) {
	ctx := context.Background()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	codec, err := NewAESCodec(key)
	require.NoError(t, err)
	cfg := &Config{Encryption: EncryptionConfig{Codec: codec}}

	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")}
	prefixes := []string{"a", "b"}
	const docs = 300

	colls := make([]Collection, len(paths))
	dbs := make([]DB, len(paths))
	for i, path := range paths {
		db, err := Open(ctx, path, cfg)
		require.NoError(t, err)
		dbs[i] = db
		colls[i], err = db.CreateCollection(ctx, "c")
		require.NoError(t, err)
	}

	start := make(chan struct{})
	errs := make([]error, len(paths))
	var wg sync.WaitGroup
	for i := range dbs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// One commit per document: every commit encrypts its frames.
			for j := 0; j < docs; j++ {
				if err := colls[i].Insert(ctx, sharedCodecDoc(prefixes[i], j)); err != nil {
					errs[i] = err
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	for i, db := range dbs {
		require.NoError(t, errs[i])
		require.NoError(t, db.Close())
	}

	for i, path := range paths {
		requireSharedCodecDocs(t, path, cfg, prefixes[i], docs, false)
	}
}

// A database with a caller-supplied codec backed up while it is written to.
// Backup opens the destination with the source's options, so the
// destination's writer encrypts through the same codec object as the
// source's writer, at the same time. Run it with -race.
//
// The config is a raw-key AES codec with the default page size and no
// checksum mode: the destination then has the source's page geometry and
// needs no KDF salt, so the copy is readable with the same codec.
func TestEncryption_SharedCodecBackupWhileWriting(t *testing.T) {
	ctx := context.Background()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	codec, err := NewAESCodec(key)
	require.NoError(t, err)
	// Uncompressed, so that the documents below fill hundreds of pages.
	cfg := &Config{Encryption: EncryptionConfig{Codec: codec}, DisableCompression: true}

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
		batch = append(batch, sharedCodecDoc("pre", i))
		if len(batch) == cap(batch) {
			require.NoError(t, coll.Insert(ctx, batch...))
			batch = batch[:0]
		}
	}

	// The writer commits one document at a time for the whole backup.
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
			if err := coll.Insert(ctx, sharedCodecDoc("w", i)); err != nil {
				writerErr = err
				return
			}
			if i == 10 {
				close(running)
			}
		}
	}()
	select {
	case <-running:
	case <-writerDone:
		require.NoError(t, writerErr)
	}

	// A writer that commits in a tight loop can keep every Step of a Backup
	// busy until the busy timeout ends it with ErrBusy; that happens on a
	// loaded machine with or without a shared codec. The test is about the
	// race between the two writers, so a busy Backup is run again.
	var backupErr error
	for attempt := 0; attempt < 5; attempt++ {
		if backupErr = db.Backup(ctx, dstPath); !errors.Is(backupErr, ErrBusy) {
			break
		}
	}
	close(stop)
	<-writerDone
	require.NoError(t, writerErr)
	require.NoError(t, backupErr)
	require.NoError(t, db.Close())
	// Backup copies 256 pages per Step.
	info, err := os.Stat(dstPath)
	require.NoError(t, err)
	require.Greater(t, info.Size()/4096, int64(256), "the copy must take more than one Step")

	requireSharedCodecDocs(t, dstPath, cfg, "pre", pre, true)
}

// One codec object in Config.Encryption.Codec serving three databases: A
// and B each with an OnIntegrityError of its own, C with none. A damaged
// page is reported to the callback of the database it belongs to and to no
// other, whatever order the databases were opened in: the callback is the
// database's, not the codec's.
func TestEncryption_SharedCodecIntegrityCallbackPerDatabase(t *testing.T) {
	ctx := context.Background()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	codec, err := NewAESCodec(key)
	require.NoError(t, err)
	enc := EncryptionConfig{Codec: codec}

	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.db")
	pathB := filepath.Join(dir, "b.db")
	pathC := filepath.Join(dir, "c.db")
	for _, path := range []string{pathA, pathB, pathC} {
		writeAndCloseIntegrityDB(t, path, &Config{Encryption: enc})
	}
	// Damage page 2 of A and of C; B stays intact.
	const pageSize, damaged = 4096, 2
	for _, path := range []string{pathA, pathC} {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		data[(damaged-1)*pageSize+200] ^= 0x01
		require.NoError(t, os.WriteFile(path, data, 0o644))
	}

	type hits struct {
		calls atomic.Int32
		page  atomic.Uint32
		kind  atomic.Int32
	}
	var hitA, hitB hits
	record := func(h *hits) func(IntegrityError) {
		return func(e IntegrityError) {
			h.page.Store(e.PageNo)
			h.kind.Store(int32(e.Kind))
			h.calls.Add(1)
		}
	}
	// Opened in the order A, B, C: B's callback is the last one given, and
	// C gives none. Open reads page 2 (the index-format record lives there)
	// and goes on past a failure, so the opens of A and C already hit their
	// damaged page once.
	dbA, err := Open(ctx, pathA, &Config{Encryption: enc, OnIntegrityError: record(&hitA)})
	require.NoError(t, err)
	defer dbA.Close()
	dbB, err := Open(ctx, pathB, &Config{Encryption: enc, OnIntegrityError: record(&hitB)})
	require.NoError(t, err)
	defer dbB.Close()
	dbC, err := Open(ctx, pathC, &Config{Encryption: enc})
	require.NoError(t, err)
	defer dbC.Close()
	assert.Zero(t, hitB.calls.Load(), "opening A and C must not reach B's callback")

	// A read of A's damaged page, through the read path and through the
	// sweep, goes to A's callback only.
	before := hitA.calls.Load()
	_, err = dbA.GetCollectionNames(ctx)
	require.ErrorIs(t, err, ErrPageIntegrity)
	assert.Greater(t, hitA.calls.Load(), before, "A's read failure must reach A's callback")
	before = hitA.calls.Load()
	rep, err := dbA.VerifyIntegrity(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, rep.Errors, "A's damaged page must fail verification")
	assert.Greater(t, hitA.calls.Load(), before, "A's sweep failure must reach A's callback")
	assert.Equal(t, uint32(damaged), hitA.page.Load())
	assert.Equal(t, IntegrityAEADAuthFail, IntegrityErrorKind(hitA.kind.Load()))
	assert.Zero(t, hitB.calls.Load(), "A's failures must not reach B's callback")

	// C has no callback: its failures reach no one.
	before = hitA.calls.Load()
	_, err = dbC.GetCollectionNames(ctx)
	require.ErrorIs(t, err, ErrPageIntegrity)
	rep, err = dbC.VerifyIntegrity(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, rep.Errors, "C's damaged page must fail verification")
	assert.Equal(t, before, hitA.calls.Load(), "C's failures must not reach A's callback")
	assert.Zero(t, hitB.calls.Load(), "C's failures must not reach B's callback")

	rep, err = dbB.VerifyIntegrity(ctx)
	require.NoError(t, err)
	require.Empty(t, rep.Errors)
	assert.Zero(t, hitB.calls.Load())
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
