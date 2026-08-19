package btree

import (
	"encoding/binary"
	"math/rand/v2"
	"testing"
)

// BenchmarkSeekKey compares AppendSeekKey (single-shot traversal) vs
// Cursor Seek+Key (allocates cursor with stack) for point lookups.
//
// Three sub-benchmarks per approach:
//   - Random: uniformly random keys (cache-unfriendly, deep traversal)
//   - Sequential: keys accessed in order (cache-friendly)
//   - Missing: keys beyond the max (rightmost-leaf fast path for AppendSeekKey)

func BenchmarkSeekKey(b *testing.B) {
	const N = 100_000
	const valSize = 100

	dir := b.TempDir()
	db, err := Open(dir+"/bench.db", DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create namespace and insert N keys.
	tx, err := db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	ns, err := tx.CreateNamespace("bench")
	if err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	tx, err = db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= N; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		if err := tx.Put(ns, key, make([]byte, valSize)); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	// Checkpoint so reads come from the main DB file.
	if err := db.Checkpoint(CheckpointFull); err != nil {
		b.Fatal(err)
	}

	// Pre-generate random keys for the Random sub-benchmark.
	randomKeys := make([][]byte, N)
	for i := range randomKeys {
		randomKeys[i] = binary.BigEndian.AppendUint32(nil, uint32(rand.IntN(N)+1))
	}

	// Pre-generate missing keys (beyond max).
	missingKeys := make([][]byte, N)
	for i := range missingKeys {
		missingKeys[i] = binary.BigEndian.AppendUint32(nil, uint32(N+1+i))
	}

	b.Run("Cursor/Random", func(b *testing.B) {
		rtx, err := db.BeginRead()
		if err != nil {
			b.Fatal(err)
		}
		defer rtx.Rollback()
		cur := rtx.NewCursor(ns)
		defer cur.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := randomKeys[i%N]
			if err := cur.Seek(key); err != nil {
				b.Fatal(err)
			}
			if cur.Valid() {
				if _, err := cur.Key(); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("AppendSeekKey/Random", func(b *testing.B) {
		rtx, err := db.BeginRead()
		if err != nil {
			b.Fatal(err)
		}
		defer rtx.Rollback()
		buf := make([]byte, 0, 64)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := randomKeys[i%N]
			buf, _ = rtx.AppendSeekKey(ns, key, buf[:0])
		}
	})

	b.Run("Cursor/Sequential", func(b *testing.B) {
		rtx, err := db.BeginRead()
		if err != nil {
			b.Fatal(err)
		}
		defer rtx.Rollback()
		cur := rtx.NewCursor(ns)
		defer cur.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i%N+1))
			if err := cur.Seek(key); err != nil {
				b.Fatal(err)
			}
			if cur.Valid() {
				if _, err := cur.Key(); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("AppendSeekKey/Sequential", func(b *testing.B) {
		rtx, err := db.BeginRead()
		if err != nil {
			b.Fatal(err)
		}
		defer rtx.Rollback()
		buf := make([]byte, 0, 64)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i%N+1))
			buf, _ = rtx.AppendSeekKey(ns, key, buf[:0])
		}
	})

	b.Run("Cursor/Missing", func(b *testing.B) {
		rtx, err := db.BeginRead()
		if err != nil {
			b.Fatal(err)
		}
		defer rtx.Rollback()
		cur := rtx.NewCursor(ns)
		defer cur.Close()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := missingKeys[i%N]
			_ = cur.Seek(key)
		}
	})

	b.Run("AppendSeekKey/Missing", func(b *testing.B) {
		rtx, err := db.BeginRead()
		if err != nil {
			b.Fatal(err)
		}
		defer rtx.Rollback()
		buf := make([]byte, 0, 64)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := missingKeys[i%N]
			buf, _ = rtx.AppendSeekKey(ns, key, buf[:0])
		}
	})
}
