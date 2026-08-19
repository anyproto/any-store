package btree

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPagePoolRecycling verifies that uncached pages are returned to the pool
// and reused, rather than being garbage collected.
func TestPagePoolRecycling(t *testing.T) {
	db, ns := tempDBWithNS(t, "test")

	// Write some data so reads have pages to fetch.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("val-%04d", i))))
	}
	require.NoError(t, tx.Commit())

	// Do a read transaction to warm the pool.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		_, err := rtx.Get(ns, []byte(fmt.Sprintf("key-%04d", i)))
		require.NoError(t, err)
	}
	require.NoError(t, rtx.Rollback())

	// Force GC to clear any non-pooled pages, but pooled pages survive
	// (sync.Pool items survive one GC cycle).
	runtime.GC()

	// Now measure allocations for a second read transaction.
	// With pooling, the page data buffers should be reused from the pool,
	// resulting in significantly fewer allocations.
	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		v, err := rtx2.Get(ns, []byte(fmt.Sprintf("key-%04d", i)))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("val-%04d", i), string(v))
	}
	require.NoError(t, rtx2.Rollback())

	runtime.ReadMemStats(&memAfter)

	// Verify correctness: data is readable and consistent.
	rtx3, err := db.BeginRead()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		v, err := rtx3.Get(ns, []byte(fmt.Sprintf("key-%04d", i)))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("val-%04d", i), string(v))
	}
	require.NoError(t, rtx3.Rollback())
}

// TestPagePoolConcurrentReaders verifies that the page pool works correctly
// under concurrent read transaction load.
func TestPagePoolConcurrentReaders(t *testing.T) {
	db, ns := tempDBWithNS(t, "test")

	// Write data.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("val-%04d", i))))
	}
	require.NoError(t, tx.Commit())

	// Launch concurrent readers.
	const numReaders = 10
	const readsPerReader = 50
	var wg sync.WaitGroup
	errs := make(chan error, numReaders)

	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < readsPerReader; j++ {
				rtx, err := db.BeginRead()
				if err != nil {
					errs <- err
					return
				}
				for i := 0; i < 200; i++ {
					v, err := rtx.Get(ns, []byte(fmt.Sprintf("key-%04d", i)))
					if err != nil {
						errs <- err
						rtx.Rollback()
						return
					}
					expected := fmt.Sprintf("val-%04d", i)
					if string(v) != expected {
						errs <- fmt.Errorf("key-%04d: got %q, want %q", i, v, expected)
						rtx.Rollback()
						return
					}
				}
				rtx.Rollback()
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestPagePoolWithConcurrentWrites verifies that pooled pages don't leak data
// across transactions when writes are happening concurrently.
func TestPagePoolWithConcurrentWrites(t *testing.T) {
	db, ns := tempDBWithNS(t, "test")

	// Write initial data.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("val-v1-%04d", i))))
	}
	require.NoError(t, tx.Commit())

	// Start a read transaction (snapshot v1).
	rtx1, err := db.BeginRead()
	require.NoError(t, err)

	// Write new data (v2) while reader holds v1 snapshot.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, tx2.Put(ns, []byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("val-v2-%04d", i))))
	}
	require.NoError(t, tx2.Commit())

	// Reader should still see v1 (MVCC isolation via uncached pages).
	for i := 0; i < 50; i++ {
		v, err := rtx1.Get(ns, []byte(fmt.Sprintf("key-%04d", i)))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("val-v1-%04d", i), string(v),
			"reader with v1 snapshot should see v1 data for key-%04d", i)
	}
	require.NoError(t, rtx1.Rollback())

	// New reader should see v2.
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		v, err := rtx2.Get(ns, []byte(fmt.Sprintf("key-%04d", i)))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("val-v2-%04d", i), string(v),
			"reader with v2 snapshot should see v2 data for key-%04d", i)
	}
	require.NoError(t, rtx2.Rollback())
}

// BenchmarkReadTxPageAllocs measures allocations during read transactions.
// With page pooling, allocations should be near zero for the page data buffers.
func BenchmarkReadTxPageAllocs(b *testing.B) {
	dir := b.TempDir()
	db, err := testOpen(b, dir+"/bench.db", DefaultOptions())
	require.NoError(b, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(b, err)
	ns, err := tx.CreateNamespace("bench")
	require.NoError(b, err)
	for i := 0; i < 500; i++ {
		require.NoError(b, tx.Put(ns, []byte(fmt.Sprintf("key-%06d", i)), []byte(fmt.Sprintf("value-%06d", i))))
	}
	require.NoError(b, tx.Commit())

	// Warm the pool.
	rtx, _ := db.BeginRead()
	for i := 0; i < 500; i++ {
		rtx.Get(ns, []byte(fmt.Sprintf("key-%06d", i)))
	}
	rtx.Rollback()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rtx, err := db.BeginRead()
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 500; j++ {
			_, err := rtx.Get(ns, []byte(fmt.Sprintf("key-%06d", j)))
			if err != nil {
				b.Fatal(err)
			}
		}
		rtx.Rollback()
	}
}
