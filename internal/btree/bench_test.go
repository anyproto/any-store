package btree

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

// benchDB creates a temporary on-disk DB for benchmarks with default options.
func benchDB(b *testing.B) *DB {
	b.Helper()
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.InProcess = true
	opts.NoCommitSync = true
	db, err := Open(filepath.Join(dir, "bench.db"), opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

// benchPopulate inserts n keys with the given value size into namespace "bench".
// Returns the namespace handle.
func benchPopulate(b *testing.B, db *DB, n int, valSize int) *Namespace {
	b.Helper()
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

	val := make([]byte, valSize)
	const batchSize = 1000
	for start := 1; start <= n; start += batchSize {
		tx, err = db.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		end := start + batchSize
		if end > n+1 {
			end = n + 1
		}
		for i := start; i < end; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			if err := tx.Put(ns, key, val); err != nil {
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	return ns
}

// BenchmarkGet measures single-key read throughput.
func BenchmarkGet(b *testing.B) {
	const numKeys = 10000
	const valSize = 100
	db := benchDB(b)
	ns := benchPopulate(b, db, numKeys, valSize)

	// Pre-generate random keys within the populated range.
	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = binary.BigEndian.AppendUint32(nil, uint32(i+1))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginRead()
		if err != nil {
			b.Fatal(err)
		}
		key := keys[i%numKeys]
		_, err = tx.Get(ns, key)
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetBatch measures batched read throughput (many reads per transaction).
func BenchmarkGetBatch(b *testing.B) {
	const numKeys = 10000
	const valSize = 100
	const batchSize = 100
	db := benchDB(b)
	ns := benchPopulate(b, db, numKeys, valSize)

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = binary.BigEndian.AppendUint32(nil, uint32(i+1))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginRead()
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < batchSize; j++ {
			key := keys[rand.Intn(numKeys)]
			_, err = tx.Get(ns, key)
			if err != nil {
				b.Fatal(err)
			}
		}
		if err := tx.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCursorScan measures full cursor iteration throughput.
func BenchmarkCursorScan(b *testing.B) {
	for _, numKeys := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("keys=%d", numKeys), func(b *testing.B) {
			db := benchDB(b)
			ns := benchPopulate(b, db, numKeys, 100)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tx, err := db.BeginRead()
				if err != nil {
					b.Fatal(err)
				}
				cur := tx.NewCursor(ns)
				count := 0
				for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
					_, _ = cur.Key()
					_, _ = cur.Value()
					count++
				}
				cur.Close()
				if err := tx.Rollback(); err != nil {
					b.Fatal(err)
				}
				if count != numKeys {
					b.Fatalf("expected %d keys, got %d", numKeys, count)
				}
			}
		})
	}
}

// BenchmarkPut measures single-key write throughput.
func BenchmarkPut(b *testing.B) {
	db := benchDB(b)
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

	val := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		key := binary.BigEndian.AppendUint32(nil, uint32(i+1))
		if err := tx.Put(ns, key, val); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConcurrentReaders measures parallel read throughput.
func BenchmarkConcurrentReaders(b *testing.B) {
	const numKeys = 10000
	const valSize = 100
	db := benchDB(b)
	ns := benchPopulate(b, db, numKeys, valSize)

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = binary.BigEndian.AppendUint32(nil, uint32(i+1))
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			tx, err := db.BeginRead()
			if err != nil {
				b.Fatal(err)
			}
			key := keys[r.Intn(numKeys)]
			_, err = tx.Get(ns, key)
			if err != nil {
				b.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkWriterWithReaders measures throughput when one writer runs
// concurrently with multiple readers.
func BenchmarkWriterWithReaders(b *testing.B) {
	const numKeys = 10000
	const valSize = 100
	db := benchDB(b)
	ns := benchPopulate(b, db, numKeys, valSize)

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = binary.BigEndian.AppendUint32(nil, uint32(i+1))
	}

	// Start background readers.
	const numReaders = 4
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(rand.Int63()))
			for {
				select {
				case <-stop:
					return
				default:
				}
				tx, err := db.BeginRead()
				if err != nil {
					return
				}
				key := keys[rng.Intn(numKeys)]
				_, _ = tx.Get(ns, key)
				_ = tx.Rollback()
			}
		}()
	}

	val := make([]byte, valSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		key := binary.BigEndian.AppendUint32(nil, uint32(rand.Intn(numKeys)+1))
		if err := tx.Put(ns, key, val); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	close(stop)
	wg.Wait()
}
