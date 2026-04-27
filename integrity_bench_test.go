package anystore

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/anyproto/any-store/anyenc"
)

// benchInsert inserts b.N documents into a fresh DB built with cfg and
// reports throughput. The doc payload is small but realistic — enough
// to push 4 KB pages into split territory after a few hundred inserts,
// so the codec hot paths are exercised.
func benchInsert(b *testing.B, cfg *Config) {
	dir := b.TempDir()
	path := filepath.Join(dir, "db")
	db, err := Open(context.Background(), path, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	coll, err := db.CreateCollection(context.Background(), "x")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "i":%d, "pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`, i, i))
		if err := coll.UpsertOne(context.Background(), doc); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIntegrity_Insert_Plain   — baseline, no codec.
// BenchmarkIntegrity_Insert_Cksum   — XXH3-128 trailer per page.
// BenchmarkIntegrity_Insert_AES     — AES-256-GCM (existing).
// BenchmarkIntegrity_Insert_ChaCha  — ChaCha20-Poly1305 (existing).
//
// Run with: go test -bench BenchmarkIntegrity_Insert_ -benchtime=2s -count=3 .
func BenchmarkIntegrity_Insert_Plain(b *testing.B) {
	benchInsert(b, &Config{})
}

func BenchmarkIntegrity_Insert_Cksum(b *testing.B) {
	cfg := &Config{}
	cfg.Integrity.PageChecksums = true
	benchInsert(b, cfg)
}

func BenchmarkIntegrity_Insert_AES(b *testing.B) {
	cfg := &Config{}
	cfg.Encryption.Passphrase = []byte("p")
	cfg.Encryption.KDFIterations = 1000
	benchInsert(b, cfg)
}

func BenchmarkIntegrity_Insert_ChaCha(b *testing.B) {
	cfg := &Config{}
	cfg.Encryption.Passphrase = []byte("p")
	cfg.Encryption.KDFIterations = 1000
	cfg.Encryption.CipherType = CipherChaCha20Poly1305
	benchInsert(b, cfg)
}
