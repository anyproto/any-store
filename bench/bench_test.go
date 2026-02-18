package bench

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/anyproto/any-store/internal/btree"
	badger "github.com/dgraph-io/badger/v4"
	bolt "go.etcd.io/bbolt"
)

// engine abstracts the four KV stores under test.
type engine interface {
	Name() string
	Open(dir string) error
	Close() error

	// WriteBatch writes n key-value pairs starting from offset.
	WriteBatch(offset, n int) error

	// Get retrieves value by key index.
	Get(index int) ([]byte, error)

	// IterateAll iterates all keys and returns count.
	IterateAll() (int, error)
}

// --- key / value helpers ---

func makeKey(i int) []byte {
	return fmt.Appendf(nil, "key-%08d", i)
}

func makeValue(i int) []byte {
	return fmt.Appendf(nil, "value-%08d-%08d", i, i)
}

// ============================================================
// BTree engine (internal/btree)
// ============================================================

type btreeEngine struct {
	db *btree.DB
	ns *btree.Namespace
}

func (e *btreeEngine) Name() string { return "btree" }

func (e *btreeEngine) Open(dir string) error {
	opts := btree.DefaultOptions()
	opts.InProcess = true
	opts.NoSync = true                // match SQLite synchronous=normal behavior
	btree.AutoCheckpointThreshold = 0 // disable during benchmarks; checkpoint manually if needed
	db, err := btree.Open(filepath.Join(dir, "bench.db"), opts)
	if err != nil {
		return err
	}
	e.db = db
	tx, err := db.BeginWrite()
	if err != nil {
		return err
	}
	ns, err := tx.CreateNamespace("bench")
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	e.ns = ns
	return nil
}

func (e *btreeEngine) Close() error {
	return e.db.Close()
}

func (e *btreeEngine) WriteBatch(offset, n int) error {
	tx, err := e.db.BeginWrite()
	if err != nil {
		return err
	}
	for i := range n {
		if err := tx.Put(e.ns, makeKey(offset+i), makeValue(offset+i)); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (e *btreeEngine) Get(index int) ([]byte, error) {
	tx, err := e.db.BeginRead()
	if err != nil {
		return nil, err
	}
	val, err := tx.Get(e.ns, makeKey(index))
	_ = tx.Rollback()
	return val, err
}

func (e *btreeEngine) IterateAll() (int, error) {
	tx, err := e.db.BeginRead()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	cur := tx.NewCursor(e.ns)
	count := 0
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		count++
	}
	return count, nil
}

// ============================================================
// BoltDB engine
// ============================================================

type boltEngine struct {
	db *bolt.DB
}

func (e *boltEngine) Name() string { return "bbolt" }

func (e *boltEngine) Open(dir string) error {
	db, err := bolt.Open(filepath.Join(dir, "bench.bolt"), 0600, nil)
	if err != nil {
		return err
	}
	e.db = db
	return db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("bench"))
		return err
	})
}

func (e *boltEngine) Close() error {
	return e.db.Close()
}

func (e *boltEngine) WriteBatch(offset, n int) error {
	return e.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("bench"))
		for i := range n {
			if err := b.Put(makeKey(offset+i), makeValue(offset+i)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (e *boltEngine) Get(index int) ([]byte, error) {
	var result []byte
	err := e.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("bench")).Get(makeKey(index))
		if v != nil {
			result = make([]byte, len(v))
			copy(result, v)
		}
		return nil
	})
	return result, err
}

func (e *boltEngine) IterateAll() (int, error) {
	count := 0
	err := e.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("bench")).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// ============================================================
// Badger engine
// ============================================================

type badgerEngine struct {
	db *badger.DB
}

func (e *badgerEngine) Name() string { return "badger" }

func (e *badgerEngine) Open(dir string) error {
	opts := badger.DefaultOptions(filepath.Join(dir, "badger"))
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	e.db = db
	return nil
}

func (e *badgerEngine) Close() error {
	return e.db.Close()
}

func (e *badgerEngine) WriteBatch(offset, n int) error {
	return e.db.Update(func(txn *badger.Txn) error {
		for i := range n {
			if err := txn.Set(makeKey(offset+i), makeValue(offset+i)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (e *badgerEngine) Get(index int) ([]byte, error) {
	var result []byte
	err := e.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(makeKey(index))
		if err != nil {
			return err
		}
		result, err = item.ValueCopy(nil)
		return err
	})
	return result, err
}

func (e *badgerEngine) IterateAll() (int, error) {
	count := 0
	err := e.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// ============================================================
// Benchmark helpers
// ============================================================

func allEngines() []engine {
	return []engine{
		&btreeEngine{},
		&boltEngine{},
		&badgerEngine{},
	}
}

func setupEngine(b *testing.B, eng engine) string {
	b.Helper()
	dir := b.TempDir()
	if err := eng.Open(dir); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = eng.Close() })
	return dir
}

// preload inserts totalKeys keys into the engine.
func preload(b *testing.B, eng engine, totalKeys int) {
	b.Helper()
	batchSize := 100
	for off := 0; off < totalKeys; off += batchSize {
		n := min(batchSize, totalKeys-off)
		if err := eng.WriteBatch(off, n); err != nil {
			b.Fatal(err)
		}
	}
}

// randomIndices returns n random indices in [0, max).
func randomIndices(n, maxVal int) []int {
	buf := make([]byte, n*4)
	_, _ = rand.Read(buf)
	indices := make([]int, n)
	for i := range n {
		v := int(buf[i*4])<<24 | int(buf[i*4+1])<<16 | int(buf[i*4+2])<<8 | int(buf[i*4+3])
		if v < 0 {
			v = -v
		}
		indices[i] = v % maxVal
	}
	return indices
}

// ============================================================
// Write benchmarks: batch sizes 1, 10, 100, 1000 (1 thread)
// ============================================================

func BenchmarkWrite(b *testing.B) {
	for _, batchSize := range []int{1, 10, 100, 1000} {
		for _, eng := range allEngines() {
			b.Run(fmt.Sprintf("%s/batch-%d", eng.Name(), batchSize), func(b *testing.B) {
				setupEngine(b, eng)
				b.ResetTimer()
				for i := range b.N {
					off := i * batchSize
					if err := eng.WriteBatch(off, batchSize); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
			})
		}
	}
}

// ============================================================
// GetById benchmarks: 1, 4, 10, 32 goroutines
// ============================================================

func BenchmarkGetById(b *testing.B) {
	const totalKeys = 10000

	for _, threads := range []int{1, 4, 10, 32} {
		for _, eng := range allEngines() {
			b.Run(fmt.Sprintf("%s/threads-%d", eng.Name(), threads), func(b *testing.B) {
				setupEngine(b, eng)
				preload(b, eng, totalKeys)

				// Pre-generate random indices for the benchmark.
				indices := randomIndices(b.N+threads*1024, totalKeys)

				b.ResetTimer()
				var counter atomic.Int64
				var wg sync.WaitGroup
				wg.Add(threads)
				for t := range threads {
					go func(threadID int) {
						defer wg.Done()
						for {
							idx := int(counter.Add(1)) - 1
							if idx >= b.N {
								return
							}
							lookupIdx := indices[idx%len(indices)]
							val, err := eng.Get(lookupIdx)
							if err != nil {
								b.Error(err)
								return
							}
							_ = val
						}
					}(t)
				}
				wg.Wait()
				b.StopTimer()
			})
		}
	}
}

// ============================================================
// Iteration benchmarks
// ============================================================

func BenchmarkIterate(b *testing.B) {
	for _, totalKeys := range []int{1000, 10000} {
		for _, eng := range allEngines() {
			b.Run(fmt.Sprintf("%s/keys-%d", eng.Name(), totalKeys), func(b *testing.B) {
				setupEngine(b, eng)
				preload(b, eng, totalKeys)

				b.ResetTimer()
				for range b.N {
					count, err := eng.IterateAll()
					if err != nil {
						b.Fatal(err)
					}
					if count != totalKeys {
						b.Fatalf("expected %d keys, got %d", totalKeys, count)
					}
				}
				b.StopTimer()
			})
		}
	}
}

// ============================================================
// Quick sanity test (not a benchmark)
// ============================================================

func TestEnginesSanity(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.Name(), func(t *testing.T) {
			dir := t.TempDir()
			if err := eng.Open(dir); err != nil {
				t.Fatal(err)
			}
			defer eng.Close()

			// Write 100 keys
			if err := eng.WriteBatch(0, 100); err != nil {
				t.Fatal(err)
			}

			// Read back
			val, err := eng.Get(42)
			if err != nil {
				t.Fatal(err)
			}
			expected := makeValue(42)
			if string(val) != string(expected) {
				t.Fatalf("expected %q, got %q", expected, val)
			}

			// Iterate
			count, err := eng.IterateAll()
			if err != nil {
				t.Fatal(err)
			}
			if count != 100 {
				t.Fatalf("expected 100 keys, got %d", count)
			}
		})
	}
}

// Ensure temp dirs are cleaned up
func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
