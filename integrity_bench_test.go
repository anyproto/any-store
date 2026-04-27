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

// BenchmarkIntegrity_Insert_Default — &Config{} → XXH3-128 trailer per page
//                                     (the default since checksums are on
//                                     for non-encrypted DBs).
// BenchmarkIntegrity_Insert_AES     — AES-256-GCM.
// BenchmarkIntegrity_Insert_ChaCha  — ChaCha20-Poly1305.
//
// Run with: go test -bench BenchmarkIntegrity_Insert_ -benchtime=2s -count=3 .
func BenchmarkIntegrity_Insert_Default(b *testing.B) {
	benchInsert(b, &Config{})
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

// benchRead pre-populates a DB with `seedDocs` documents (~100 byte payload),
// closes and reopens it (so reads come from the on-disk file, not the writer
// cache), then iterates over all rows in a tight loop. Reports ns per row
// read.
func benchRead(b *testing.B, cfg *Config) {
	const seedDocs = 5000
	dir := b.TempDir()
	path := filepath.Join(dir, "db")
	{
		db, err := Open(context.Background(), path, cfg)
		if err != nil {
			b.Fatal(err)
		}
		coll, err := db.CreateCollection(context.Background(), "x")
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < seedDocs; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "i":%d, "pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`, i, i))
			if err := coll.UpsertOne(context.Background(), doc); err != nil {
				b.Fatal(err)
			}
		}
		if err := db.Flush(context.Background(), 0, FlushModeCheckpointFull); err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}

	db, err := Open(context.Background(), path, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	coll, err := db.OpenCollection(context.Background(), "x")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	rows := 0
	for i := 0; i < b.N; i++ {
		iter, err := coll.Find(nil).Iter(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		for iter.Next() {
			doc, err := iter.Doc()
			if err != nil {
				b.Fatal(err)
			}
			_ = doc
			rows++
		}
		_ = iter.Close()
	}
	b.StopTimer()
	b.ReportMetric(float64(rows)/float64(b.Elapsed().Seconds())/1e6, "Mrows/s")
}

// BenchmarkIntegrity_Read_*: full-table scans over a pre-loaded fixture DB.
// Run with: go test -bench BenchmarkIntegrity_Read_ -benchtime=2s -count=3 .
func BenchmarkIntegrity_Read_Default(b *testing.B) {
	benchRead(b, &Config{})
}

func BenchmarkIntegrity_Read_AES(b *testing.B) {
	cfg := &Config{}
	cfg.Encryption.Passphrase = []byte("p")
	cfg.Encryption.KDFIterations = 1000
	benchRead(b, cfg)
}

func BenchmarkIntegrity_Read_ChaCha(b *testing.B) {
	cfg := &Config{}
	cfg.Encryption.Passphrase = []byte("p")
	cfg.Encryption.KDFIterations = 1000
	cfg.Encryption.CipherType = CipherChaCha20Poly1305
	benchRead(b, cfg)
}
