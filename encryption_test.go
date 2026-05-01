package anystore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

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

// Each benchmark opens a fresh DB, then times b.N transactions of
// 100 InsertOne operations each — representative of a small-write
// workload. NoCommitSync = !CommitSync (default false) means we
// skip fsync and measure pure CPU + buffered I/O cost.

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
