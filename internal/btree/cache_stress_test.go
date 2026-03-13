package btree

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCacheStressConcurrentReaderAndSpillingWriter tests the shared pcache
// behavior when a writer is actively spilling dirty pages while readers
// are accessing the same pages. It uses a very small CacheSize to force
// frequent spills.
func TestCacheStressConcurrentReaderAndSpillingWriter(t *testing.T) {
	// Small cache to force frequent pagerStress
	opts := Options{
		CacheSize: 10,
		PageSize:  4096,
	}
	db, err := testOpen(t, t.TempDir()+"/stress.db", opts)
	require.NoError(t, err)
	defer db.Close()

	// 1. Prepare data that spans multiple pages
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, db.CreateNamespace(tx, "test"))
	ns, err := tx.GetNamespace("test")
	require.NoError(t, err)

	const numKeys = 200 // Enough to fill many pages
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("val-%05d", i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// 2. Start readers that continuously scan
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var readErrors atomic.Int64
	var cacheMisses atomic.Int64

	const numReaders = 5
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rtx, err := db.BeginRead()
				if err != nil {
					return
				}
				rns, _ := rtx.GetNamespace("test")
				
				// Capture the current round's values (could be initial or some later round)
				// We expect these values to stay consistent throughout this ReadTx.
				getRound := func(v []byte) int {
					if string(v[:4]) == "val-" {
						return -1
					}
					// Extract last part: "new-val-00000-ROUND"
					s := string(v)
					lastDash := 0
					for i := len(s) - 1; i >= 0; i-- {
						if s[i] == '-' {
							lastDash = i
							break
						}
					}
					round := 0
					fmt.Sscanf(s[lastDash+1:], "%d", &round)
					return round
				}

				firstKey := []byte(fmt.Sprintf("key-%05d", 0))
				firstVal, err := rtx.Get(rns, firstKey)
				if err != nil {
					_ = rtx.Rollback()
					continue
				}
				expectedRound := getRound(firstVal)

				// Random lookups within this transaction
				for j := 0; j < 50; j++ {
					k := rand.Intn(numKeys)
					key := []byte(fmt.Sprintf("key-%05d", k))
					val, err := rtx.Get(rns, key)
					if err != nil {
						if readErrors.Add(1) < 10 {
							fmt.Printf("Read error for %s: %v\n", key, err)
						}
						continue
					}
					
					// Every key read in this transaction must belong to the same writer "round"
					if getRound(val) != expectedRound {
						if readErrors.Add(1) < 10 {
							fmt.Printf("Isolation failure for %s: expected round %d, got %d (val=%s)\n", key, expectedRound, getRound(val), val)
						}
					}
				}
				_ = rtx.Rollback()
			}
		}(r)
	}

	// 3. Writer performing large updates to trigger spill
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 20; round++ {
			wtx, err := db.BeginWrite()
			if err != nil {
				return
			}
			wns, _ := wtx.GetNamespace("test")
			for i := 0; i < numKeys; i++ {
				key := []byte(fmt.Sprintf("key-%05d", i))
				val := []byte(fmt.Sprintf("new-val-%05d-%d", i, round))
				if err := wtx.Put(wns, key, val); err != nil {
					break
				}
				// Savepoint stress
				if i%50 == 0 {
					_, _ = wtx.Savepoint()
				}
			}
			// Trigger manual checkpoint to keep WAL small but busy
			require.NoError(t, wtx.Commit())
			_ = db.Checkpoint(CheckpointPassive)
		}
		close(stop)
	}()

	wg.Wait()

	assert.Equal(t, int64(0), readErrors.Load(), "Expected no read errors during concurrent spill")
	t.Logf("Cache misses (estimated): %d", cacheMisses.Load())
}

// TestCacheStressSavepointRollbackAndEviction tests Flow 2 & 4:
// Writer getWritablePage after spill+evict re-registers the page and marks it dirty.
func TestCacheStressSavepointRollbackAndEviction(t *testing.T) {
	opts := Options{
		CacheSize: 5, // Extremely small
		PageSize:  4096,
	}
	db, err := testOpen(t, t.TempDir()+"/savepoint_stress.db", opts)
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, db.CreateNamespace(tx, "test"))
	ns, err := tx.GetNamespace("test")
	require.NoError(t, err)

	// 1. Fill some pages
	for i := 0; i < 20; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%d", i)), []byte("v")))
	}
	require.NoError(t, tx.Commit())

	// 2. Start write transaction
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	wns, _ := wtx.GetNamespace("test")

	// Create savepoint
	spID, err := wtx.Savepoint()
	require.NoError(t, err)

	// Dirty some pages and force spill
	for i := 0; i < 50; i++ {
		require.NoError(t, wtx.Put(wns, []byte(fmt.Sprintf("k%d", i)), []byte("new")))
	}
	// At this point many pages are spilled to WAL and marked clean in pcache.
	
	// 3. Simulate reader polluting pcache with "clean" versions of those pages
	for i := 1; i <= 10; i++ {
		pg, err := db.pager.getPageWriter(uint32(i), wtx.walMaxFrame)
		require.NoError(t, err)
		db.pager.releasePage(pg)
	}

	// 4. Rollback to savepoint
	require.NoError(t, wtx.RollbackToSavepoint(spID))

	// 5. Verify data consistency
	for i := 0; i < 20; i++ {
		val, err := wtx.Get(wns, []byte(fmt.Sprintf("k%d", i)))
		require.NoError(t, err)
		assert.Equal(t, "v", string(val))
	}
	require.NoError(t, wtx.Commit())
}

// TestCacheStressOverflowChains tests Flow 6: Reader overflow chain during active spill.
func TestCacheStressOverflowChains(t *testing.T) {
	opts := Options{
		CacheSize: 10,
		PageSize:  4096,
	}
	db, err := testOpen(t, t.TempDir()+"/overflow_stress.db", opts)
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, db.CreateNamespace(tx, "test"))
	ns, err := tx.GetNamespace("test")
	require.NoError(t, err)

	// Create large overflow value
	largeVal := make([]byte, 20000)
	for i := range largeVal {
		largeVal[i] = byte(i % 256)
	}
	require.NoError(t, tx.Put(ns, []byte("large"), largeVal))
	require.NoError(t, tx.Commit())

	var wg sync.WaitGroup
	stop := make(chan struct{})
	
	// Reader constantly reading the overflow chain
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			rtx, _ := db.BeginRead()
			rns, _ := rtx.GetNamespace("test")
			val, err := rtx.Get(rns, []byte("large"))
			if err == nil {
				if len(val) != 20000 {
					fmt.Printf("Read wrong length: %d\n", len(val))
				}
			}
			_ = rtx.Rollback()
		}
	}()

	// Writer constantly causing spills
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			wtx, _ := db.BeginWrite()
			wns, _ := wtx.GetNamespace("test")
			for j := 0; j < 100; j++ {
				_ = wtx.Put(wns, []byte(fmt.Sprintf("other-%d", j)), []byte("data"))
			}
			_ = wtx.Commit()
		}
		close(stop)
	}()

	wg.Wait()
}
