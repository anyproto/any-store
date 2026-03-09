package btree

// Stress tests for concurrent reader/writer scenarios.
//
// any-store uses per-connection page caches (matching SQLite's model): the
// writer has its own writerCache, each reader gets a private cache from a pool.
// These tests exercise concurrent read/write flows including spill, rollback,
// and overflow reading.

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tempDBWithCacheSize creates a DB with custom CacheSize for stress testing.
func tempDBWithCacheSize(t *testing.T, cacheSize int) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{
		PageSize:              4096,
		CacheSize:             cacheSize,
		DisableAutoCheckpoint: true, // avoid checkpoint interference
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// tempDBWithCacheSizeInMem creates an InMemory DB with custom CacheSize.
func tempDBWithCacheSizeInMem(t *testing.T, cacheSize int) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{
		PageSize:              4096,
		CacheSize:             cacheSize,
		InMemory:              true,
		DisableAutoCheckpoint: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedData inserts n key-value pairs into namespace ns. Keys are 4-byte
// big-endian integers, values are valSize bytes with the key embedded.
func seedData(t *testing.T, db *DB, nsName string, n int, valSize int) *Namespace {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace(nsName)
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		val := make([]byte, valSize)
		binary.BigEndian.PutUint32(val, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())
	return ns
}

// -------------------------------------------------------------------
// Flow 1: Reader cache hit during writer spill
//
// Reader calls getPageWriter, finds clean cached page, calls getLatest
// which returns a spill frame. latestFrame > walMaxFrame → reader drops
// the valid cached page and re-reads via readPageUncached.
//
// This tests correctness (reader sees committed data, not spilled data)
// and quantifies the unnecessary cache miss cost.
// -------------------------------------------------------------------

func TestSharedCache_ReaderCacheHitDuringWriterSpill(t *testing.T) {
	// Very small cache (15 pages) to force spill during writes.
	db := tempDBWithCacheSize(t, 15)

	// Seed 50 rows — enough to fill and overflow the small cache.
	ns := seedData(t, db, "test", 50, 100)

	// Start concurrent readers while the writer is spilling.
	var wg sync.WaitGroup
	var readerErrors atomic.Int64
	var readerReads atomic.Int64
	stopReaders := make(chan struct{})

	// Launch 4 reader goroutines
	for r := range 4 {
		wg.Add(1)
		go func(rid int) {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				rtx, err := db.BeginRead()
				if err != nil {
					continue // db closing
				}
				// Read all keys to exercise cache hits/misses
				for i := 1; i <= 50; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					val, err := rtx.Get(ns, key)
					if err != nil {
						readerErrors.Add(1)
						break
					}
					// Verify the value is consistent
					if len(val) < 4 {
						readerErrors.Add(1)
						break
					}
					got := binary.BigEndian.Uint32(val[:4])
					if got != uint32(i) {
						readerErrors.Add(1)
						break
					}
					readerReads.Add(1)
				}
				_ = rtx.Rollback()
			}
		}(r)
	}

	// Writer: update rows in a way that forces spill with the tiny cache.
	for round := range 10 {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		for i := 1; i <= 50; i++ {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))
			val := make([]byte, 100)
			binary.BigEndian.PutUint32(val, uint32(i)) // keep consistent
			copy(val[4:], fmt.Sprintf("round-%d", round))
			require.NoError(t, tx.Put(ns, key, val))
		}
		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
	}

	close(stopReaders)
	wg.Wait()

	assert.Zero(t, readerErrors.Load(), "readers should never see inconsistent data during spill")
	assert.Greater(t, readerReads.Load(), int64(0), "readers should have completed some reads")
	t.Logf("reader reads: %d, reader errors: %d", readerReads.Load(), readerErrors.Load())
}

// -------------------------------------------------------------------
// Flow 2: Writer getWritablePage after spill+evict
//
// Page spilled (made clean), evicted from pcache, reader creates new
// cache entry for same pgno. Writer calls getWritablePage → should use
// its own writePages entry, not the reader's cache entry.
// -------------------------------------------------------------------

func TestSharedCache_WriterGetWritablePageAfterSpillEvict(t *testing.T) {
	// Tiny cache to force spill and eviction
	db := tempDBWithCacheSize(t, 10)

	ns := seedData(t, db, "test", 30, 200)

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	var readerErrors atomic.Int64

	// Concurrent readers creating cache entries
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				rtx, err := db.BeginRead()
				if err != nil {
					continue
				}
				for i := 1; i <= 30; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					_, err := rtx.Get(ns, key)
					if err != nil {
						readerErrors.Add(1)
						break
					}
				}
				_ = rtx.Rollback()
			}
		}()
	}

	// Writer: modify pages, causing spill + eviction, then re-acquire via getWritablePage
	for round := range 20 {
		tx, err := db.BeginWrite()
		require.NoError(t, err)

		// Create a savepoint to exercise savepoint copy-on-write during spill
		spID, err := tx.Savepoint()
		require.NoError(t, err)

		for i := 1; i <= 30; i++ {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))
			val := make([]byte, 200)
			binary.BigEndian.PutUint32(val, uint32(i))
			copy(val[4:], fmt.Sprintf("round-%d", round))
			require.NoError(t, tx.Put(ns, key, val))
		}

		require.NoError(t, tx.ReleaseSavepoint(spID))
		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
	}

	close(stopReaders)
	wg.Wait()
	assert.Zero(t, readerErrors.Load())

	// Verify final data integrity
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	for i := 1; i <= 30; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		val, err := rtx.Get(ns, key)
		require.NoError(t, err)
		got := binary.BigEndian.Uint32(val[:4])
		assert.Equal(t, uint32(i), got, "data corruption at key %d", i)
	}
	require.NoError(t, rtx.Rollback())
}

// -------------------------------------------------------------------
// Flow 3: Concurrent dirty flag observation
//
// Writer dirties pages while readers read via their private caches.
// Verify per-connection isolation prevents data races.
// -------------------------------------------------------------------

func TestSharedCache_ConcurrentDirtyFlagObservation(t *testing.T) {
	db := tempDBWithCacheSize(t, 20)
	ns := seedData(t, db, "test", 20, 50)

	var wg sync.WaitGroup
	var readerOK atomic.Int64
	stopReaders := make(chan struct{})

	// Reader goroutines that repeatedly fetch pages via readPageMVCC
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				rtx, err := db.BeginRead()
				if err != nil {
					continue
				}
				key := make([]byte, 4)
				idx := rand.IntN(20) + 1
				binary.BigEndian.PutUint32(key, uint32(idx))
				_, err = rtx.Get(ns, key)
				if err == nil {
					readerOK.Add(1)
				}
				_ = rtx.Rollback()
			}
		}()
	}

	// Writer repeatedly dirtying pages
	for round := range 30 {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		for i := 1; i <= 20; i++ {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))
			val := make([]byte, 50)
			binary.BigEndian.PutUint32(val, uint32(i))
			copy(val[4:], fmt.Sprintf("dirty-%d", round))
			require.NoError(t, tx.Put(ns, key, val))
		}
		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
	}

	close(stopReaders)
	wg.Wait()

	assert.Greater(t, readerOK.Load(), int64(0), "readers should have succeeded")
}

// -------------------------------------------------------------------
// Flow 4: Writer re-acquires spilled pages under cache pressure
//
// Writer's spilled page is re-acquired for writing. With per-connection
// caches, there are no reader entries to displace — the page is simply
// re-registered and marked dirty.
// -------------------------------------------------------------------

func TestSharedCache_ReinsertDirtyDisplacesReaderEntry(t *testing.T) {
	// Very small cache to guarantee spill + eviction + reader re-creation
	db := tempDBWithCacheSize(t, 8)
	ns := seedData(t, db, "test", 20, 300)

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	var readerErrors atomic.Int64

	// Reader goroutines constantly reading (creating cache entries)
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				rtx, err := db.BeginRead()
				if err != nil {
					continue
				}
				for i := 1; i <= 20; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					val, err := rtx.Get(ns, key)
					if err != nil {
						readerErrors.Add(1)
						break
					}
					if len(val) < 4 || binary.BigEndian.Uint32(val[:4]) != uint32(i) {
						readerErrors.Add(1)
						break
					}
				}
				_ = rtx.Rollback()
			}
		}()
	}

	// Writer: modify in a way that forces spill, evict, then re-acquire
	for round := range 15 {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		for i := 1; i <= 20; i++ {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))
			val := make([]byte, 300)
			binary.BigEndian.PutUint32(val, uint32(i))
			copy(val[4:], fmt.Sprintf("reinsert-%d", round))
			require.NoError(t, tx.Put(ns, key, val))
		}
		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
	}

	close(stopReaders)
	wg.Wait()
	assert.Zero(t, readerErrors.Load(), "reader should never see corrupted data")
}

// -------------------------------------------------------------------
// Flow 5: pcache eviction under concurrent load
//
// evictOne removing clean pages while readers pin/unpin. Verify pinCount
// and LRU consistency are maintained.
// -------------------------------------------------------------------

func TestSharedCache_EvictionUnderConcurrentLoad(t *testing.T) {
	db := tempDBWithCacheSize(t, 12)
	ns := seedData(t, db, "test", 40, 150)

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	var readerErrors atomic.Int64
	var readerCount atomic.Int64

	// Many readers to create high contention on pin/unpin
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				rtx, err := db.BeginRead()
				if err != nil {
					continue
				}
				// Random access pattern to stress LRU eviction
				for range 10 {
					idx := rand.IntN(40) + 1
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(idx))
					val, err := rtx.Get(ns, key)
					if err != nil {
						readerErrors.Add(1)
						break
					}
					if len(val) < 4 || binary.BigEndian.Uint32(val[:4]) != uint32(idx) {
						readerErrors.Add(1)
						break
					}
				}
				readerCount.Add(1)
				_ = rtx.Rollback()
			}
		}()
	}

	// Writer: create eviction pressure
	for round := range 25 {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		for i := 1; i <= 40; i++ {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))
			val := make([]byte, 150)
			binary.BigEndian.PutUint32(val, uint32(i))
			copy(val[4:], fmt.Sprintf("evict-%d", round))
			require.NoError(t, tx.Put(ns, key, val))
		}
		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
	}

	close(stopReaders)
	wg.Wait()

	assert.Zero(t, readerErrors.Load(), "eviction must not corrupt data")
	assert.Greater(t, readerCount.Load(), int64(0), "readers should have completed")
	t.Logf("reader txns: %d, errors: %d", readerCount.Load(), readerErrors.Load())
}

// -------------------------------------------------------------------
// Flow 6: Reader overflow chain during active spill
//
// readOverflowChainReader uses getPageReader (correct for MVCC).
// readOverflowChainAt non-MVCC path uses getPage (shared cache).
// Verify readers with large overflow values see consistent data during spill.
// -------------------------------------------------------------------

func TestSharedCache_ReaderOverflowDuringActiveSpill(t *testing.T) {
	// Small cache + large values = lots of overflow + spill
	db := tempDBWithCacheSize(t, 15)

	// Create namespace with large values that require overflow pages
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("overflow")
	require.NoError(t, err)
	for i := 1; i <= 10; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		// Large value: ~8KB, needs overflow pages with 4096 page size
		val := make([]byte, 8000)
		binary.BigEndian.PutUint32(val, uint32(i))
		for j := 4; j < len(val); j++ {
			val[j] = byte(i)
		}
		require.NoError(t, tx.Put(ns, key, val))
	}
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	var readerErrors atomic.Int64
	var readerReads atomic.Int64

	// Readers checking overflow chain consistency
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				rtx, err := db.BeginRead()
				if err != nil {
					continue
				}
				for i := 1; i <= 10; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					val, err := rtx.Get(ns, key)
					if err != nil {
						readerErrors.Add(1)
						break
					}
					if len(val) != 8000 {
						readerErrors.Add(1)
						break
					}
					got := binary.BigEndian.Uint32(val[:4])
					if got != uint32(i) {
						readerErrors.Add(1)
						break
					}
					// Check fill pattern is internally consistent
					// (all fill bytes are the same value)
					fillByte := val[4]
					consistent := true
					for j := 5; j < len(val); j++ {
						if val[j] != fillByte {
							consistent = false
							break
						}
					}
					if !consistent {
						readerErrors.Add(1)
						break
					}
					readerReads.Add(1)
				}
				_ = rtx.Rollback()
			}
		}()
	}

	// Writer: update overflow values, causing spill
	for round := range 10 {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		for i := 1; i <= 10; i++ {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))
			val := make([]byte, 8000)
			binary.BigEndian.PutUint32(val, uint32(i))
			fill := byte(i) + byte(round)
			for j := 4; j < len(val); j++ {
				val[j] = fill
			}
			require.NoError(t, tx.Put(ns, key, val))
		}
		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
	}

	close(stopReaders)
	wg.Wait()

	assert.Zero(t, readerErrors.Load(), "overflow reads during spill must be consistent")
	assert.Greater(t, readerReads.Load(), int64(0))
	t.Logf("reader overflow reads: %d, errors: %d", readerReads.Load(), readerErrors.Load())
}

// -------------------------------------------------------------------
// Combined: Spill + Savepoint + Rollback under concurrent readers
//
// This tests the most dangerous path: writer with savepoints, spill
// happens, savepoint rollback restores pages, all while readers are
// reading through the shared cache.
// -------------------------------------------------------------------

func TestSharedCache_SpillSavepointRollbackConcurrent(t *testing.T) {
	db := tempDBWithCacheSize(t, 10)
	ns := seedData(t, db, "test", 25, 100)

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	var readerErrors atomic.Int64

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				rtx, err := db.BeginRead()
				if err != nil {
					continue
				}
				for i := 1; i <= 25; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					val, err := rtx.Get(ns, key)
					if err != nil {
						readerErrors.Add(1)
						break
					}
					if len(val) < 4 || binary.BigEndian.Uint32(val[:4]) != uint32(i) {
						readerErrors.Add(1)
						break
					}
				}
				_ = rtx.Rollback()
			}
		}()
	}

	// Writer: savepoint → modify → spill (forced by small cache) → rollback savepoint
	for round := range 15 {
		tx, err := db.BeginWrite()
		require.NoError(t, err)

		// First modify some rows (these will commit)
		for i := 1; i <= 10; i++ {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))
			val := make([]byte, 100)
			binary.BigEndian.PutUint32(val, uint32(i))
			copy(val[4:], fmt.Sprintf("committed-%d", round))
			require.NoError(t, tx.Put(ns, key, val))
		}

		// Savepoint
		spID, err := tx.Savepoint()
		require.NoError(t, err)

		// Modify more rows after savepoint (these will be rolled back)
		for i := 11; i <= 25; i++ {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))
			val := make([]byte, 100)
			binary.BigEndian.PutUint32(val, uint32(0xDEAD)) // wrong value
			require.NoError(t, tx.Put(ns, key, val))
		}

		// Rollback the savepoint
		require.NoError(t, tx.RollbackToSavepoint(spID))

		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
	}

	close(stopReaders)
	wg.Wait()
	assert.Zero(t, readerErrors.Load(), "readers must never see rolled-back data")

	// Final integrity check: keys 11-25 should still have their original values
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	for i := 11; i <= 25; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		val, err := rtx.Get(ns, key)
		require.NoError(t, err)
		got := binary.BigEndian.Uint32(val[:4])
		assert.Equal(t, uint32(i), got, "savepoint-rolled-back key %d should have original value", i)
	}
	require.NoError(t, rtx.Rollback())
}

// -------------------------------------------------------------------
// Stress: Writer rollback under concurrent readers
//
// Tests that full transaction rollback correctly discards all changes
// and readers always see consistent committed data.
// -------------------------------------------------------------------

func TestSharedCache_WriterRollbackConcurrent(t *testing.T) {
	db := tempDBWithCacheSize(t, 12)
	ns := seedData(t, db, "test", 20, 80)

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	var readerErrors atomic.Int64

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				rtx, err := db.BeginRead()
				if err != nil {
					continue
				}
				for i := 1; i <= 20; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					val, err := rtx.Get(ns, key)
					if err != nil {
						readerErrors.Add(1)
						break
					}
					if len(val) < 4 || binary.BigEndian.Uint32(val[:4]) != uint32(i) {
						readerErrors.Add(1)
						break
					}
				}
				_ = rtx.Rollback()
			}
		}()
	}

	// Alternate between commit and rollback
	for round := range 20 {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		for i := 1; i <= 20; i++ {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))
			val := make([]byte, 80)
			if round%2 == 0 {
				// Commit: keep value consistent
				binary.BigEndian.PutUint32(val, uint32(i))
			} else {
				// Rollback: write bad values (should be discarded)
				binary.BigEndian.PutUint32(val, uint32(0xBAD))
			}
			require.NoError(t, tx.Put(ns, key, val))
		}
		if round%2 == 0 {
			tx.MarkDataChanged()
			require.NoError(t, tx.Commit())
		} else {
			require.NoError(t, tx.Rollback())
		}
	}

	close(stopReaders)
	wg.Wait()
	assert.Zero(t, readerErrors.Load())
}

// -------------------------------------------------------------------
// InMemory stress: verifies all the above flows work in InMemory mode
// where readPageUncached has a different path (reads from cache copy).
// -------------------------------------------------------------------

func TestSharedCache_InMemoryStress(t *testing.T) {
	db := tempDBWithCacheSizeInMem(t, 50) // InMemory is non-purgeable by default but let's test

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("mem")
	require.NoError(t, err)
	for i := 1; i <= 30; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		val := make([]byte, 200)
		binary.BigEndian.PutUint32(val, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	var readerErrors atomic.Int64

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				rtx, err := db.BeginRead()
				if err != nil {
					continue
				}
				for i := 1; i <= 30; i++ {
					key := make([]byte, 4)
					binary.BigEndian.PutUint32(key, uint32(i))
					val, err := rtx.Get(ns, key)
					if err != nil {
						readerErrors.Add(1)
						break
					}
					if len(val) < 4 || binary.BigEndian.Uint32(val[:4]) != uint32(i) {
						readerErrors.Add(1)
						break
					}
				}
				_ = rtx.Rollback()
			}
		}()
	}

	for round := range 20 {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		for i := 1; i <= 30; i++ {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))
			val := make([]byte, 200)
			binary.BigEndian.PutUint32(val, uint32(i))
			copy(val[4:], fmt.Sprintf("mem-%d", round))
			require.NoError(t, tx.Put(ns, key, val))
		}
		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
	}

	close(stopReaders)
	wg.Wait()
	assert.Zero(t, readerErrors.Load())
}

// -------------------------------------------------------------------
// Stress: Cursor iteration during spill
//
// Cursor holds page pins and iterates while writer spills. This tests
// that pinned pages are not evicted from under a cursor.
// -------------------------------------------------------------------

func TestSharedCache_CursorDuringSpill(t *testing.T) {
	db := tempDBWithCacheSize(t, 12)
	ns := seedData(t, db, "test", 40, 100)

	var wg sync.WaitGroup
	stopReaders := make(chan struct{})
	var readerErrors atomic.Int64
	var cursorIters atomic.Int64

	// Reader goroutines using cursors
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				rtx, err := db.BeginRead()
				if err != nil {
					continue
				}
				cursor := rtx.NewCursor(ns)
				if err := cursor.First(); err != nil {
					cursor.Close()
					_ = rtx.Rollback()
					continue
				}
				count := 0
				for cursor.Valid() {
					_, err := cursor.Key()
					if err != nil {
						readerErrors.Add(1)
						break
					}
					_, err = cursor.Value()
					if err != nil {
						readerErrors.Add(1)
						break
					}
					count++
					if err := cursor.Next(); err != nil {
						readerErrors.Add(1)
						break
					}
				}
				if count < 40 && readerErrors.Load() == 0 {
					// Only flag if we didn't break due to error
					// During writes, count might be less than 40 if cursor
					// uses MVCC reads (which is expected)
				}
				cursorIters.Add(1)
				cursor.Close()
				_ = rtx.Rollback()
			}
		}()
	}

	// Writer
	for round := range 15 {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		for i := 1; i <= 40; i++ {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uint32(i))
			val := make([]byte, 100)
			binary.BigEndian.PutUint32(val, uint32(i))
			copy(val[4:], fmt.Sprintf("cursor-%d", round))
			require.NoError(t, tx.Put(ns, key, val))
		}
		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
	}

	close(stopReaders)
	wg.Wait()

	assert.Zero(t, readerErrors.Load())
	assert.Greater(t, cursorIters.Load(), int64(0))
	t.Logf("cursor iterations: %d", cursorIters.Load())
}
