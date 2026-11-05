package btree

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// slabDBWithNS opens a DB on the global page slab. Temp pages then draw from
// one LIFO free list shared by every reader, so a buffer one reader releases
// is the next one another reader fills. Under the default sync.Pool each
// goroutine gets its own buffer back and two readers never share one.
func slabDBWithNS(t *testing.T, nsName string) (*DB, *Namespace) {
	t.Helper()
	opts := DefaultOptions()
	opts.UsePageSlab = true
	globalPageSlab.Reset()
	globalPageSlab.Init(int(opts.PageSize), 1024)
	t.Cleanup(globalPageSlab.Reset)

	db, err := testOpen(t, filepath.Join(t.TempDir(), "test.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.True(t, db.pager.useSlab)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace(nsName)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return db, ns
}

// requireSlabBacked fails when any page buffer came from the heap pool
// instead of the slab: there the readers would not share buffers and the
// test would pass without exercising the release order.
func requireSlabBacked(t *testing.T) {
	t.Helper()
	globalPageSlab.mu.Lock()
	nOverflow := globalPageSlab.nOverflow
	globalPageSlab.mu.Unlock()
	require.Zero(t, nOverflow, "page buffers fell back to the heap pool")
}

// Point lookups on uncached temp pages: releasePage hands the page buffer
// back to the slab, so every byte the lookup returns must be copied out
// before the release. A copy made after the release reads a buffer the other
// reader may already be filling (-race reports the pair, the value check
// catches a torn copy).
func TestAppendValue_UncachedTempPage_CopiesBeforeRelease(t *testing.T) {
	db, ns := slabDBWithNS(t, "test")
	key := func(i int) []byte { return fmt.Appendf(nil, "key-%04d", i) }

	const n = 64
	vals := make([][]byte, n)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := range n {
		vals[i] = bytes.Repeat([]byte{byte(i + 1)}, 48)
		require.NoError(t, tx.Put(ns, key(i), vals[i]))
	}
	require.NoError(t, tx.Commit())

	const iters = 4000
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for g := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// No private pcache: every page read is an uncached temp page.
			bt := &btree{pager: db.pager, rootPage: ns.RootPage()}
			for i := range iters {
				k := (i*7 + g*13) % n
				v, err := bt.Get(key(k))
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(v, vals[k]) {
					errs <- fmt.Errorf("key %d: torn value %x", k, v)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	requireSlabBacked(t)
}

// Seek on uncached temp pages with value-overflow cells: leafFullKey returns
// a slice into the page when only the value spills, so the key must be
// appended before the leaf is released. Prefixes just above each key walk
// every leaf boundary, covering the leftmostKeyAfter path as well.
func TestAppendSeekKey_UncachedTempPage_CopiesBeforeRelease(t *testing.T) {
	db, ns := slabDBWithNS(t, "test")
	key := func(i int) []byte { return fmt.Appendf(nil, "key-%04d", i) }

	const n = 48
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := range n {
		require.NoError(t, tx.Put(ns, key(i), bytes.Repeat([]byte{byte(i + 1)}, 3000)))
	}
	require.NoError(t, tx.Commit())

	const rounds = 60
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := db.BeginRead()
			if err != nil {
				errs <- err
				return
			}
			defer tx.Rollback()
			// No private pcache: every page read is an uncached temp page.
			tx.cache.destroy()
			tx.cache = nil
			for range rounds {
				for i := range n - 1 {
					prefix := append(key(i), '~')
					k, err := tx.AppendSeekKey(ns, prefix, nil)
					if err != nil {
						errs <- err
						return
					}
					if !bytes.Equal(k, key(i+1)) {
						errs <- fmt.Errorf("seek %q: got key %q", prefix, k)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	requireSlabBacked(t)
}
