package anystore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/anyproto/any-store/anyenc"
)

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
