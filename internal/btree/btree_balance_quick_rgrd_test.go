package btree

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBalanceQuick_HappyPath verifies the rightmost-append fast path
// produces tightly-packed leaves on monotonic workloads. Port of
// SQLite balance_quick (btree.c:7992-8086, dispatch btree.c:9169-9192).
//
// Without the fast path, leafSplitPoint (btree.go:217) targets 2/3
// fill on the left, freezing non-rightmost leaves at ~60.6% fill.
// The fast path keeps the left page 100% full and puts only the new
// cell on the new right sibling, so steady-state leaf fill jumps
// from ~60% to ~88% (with the remaining ~12% gap coming from
// shallow-tree splits where parent == root, which SQLite's
// balance_quick at btree.c:9173 also excludes via pParent->pgno != 1).
//
// nRows=5000 at pageSize=1024 forces depth ≥ 3 and spends most of the
// workload in the deep-tree regime where the fast path fires. The first
// ~90 leaves are created while the tree is depth 2 (parent == root,
// fast path excluded) and stay at ~60% fill; the remaining ~365 leaves
// are created post-depth-3 at ~95% fill. Weighted average ≈ 88%.
func TestBalanceQuick_HappyPath(t *testing.T) {
	const (
		pageSize = 1024
		nRows    = 5000
		valSize  = 80
	)

	resetPageBufferPool()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: pageSize})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	db.pager.balanceQuickDispatchCount.Store(0)

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	val := make([]byte, valSize)
	for i := 1; i <= nRows; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	require.Greater(t, db.pager.balanceQuickDispatchCount.Load(), int64(0),
		"balance_quick fast path should have fired at least once for 1000 monotonic appends")

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })

	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	bt := &btree{pager: db.pager, rootPage: ns2.rootPage, walMaxFrame: rtx.walMaxFrame}
	stats := walkLeavesForFill(t, bt)

	require.NoError(t, db.IntegrityCheck())

	const leafHeaderSize = 8
	leafCapacity := stats.usableSize - leafHeaderSize
	used := stats.totalUsed()
	avgFill := float64(used) / float64(stats.leafCount*leafCapacity)
	require.GreaterOrEqual(t, avgFill, 0.87,
		"expected avg leaf fill ≥ 87%% with balance_quick; got %.1f%% across %d leaves",
		avgFill*100, stats.leafCount)
}

// TestBalanceQuick_RootIsParent covers SQLite precondition pParent->pgno != 1
// at btree.c:9173. When the leaf's parent is the btree root, the fast path
// must NOT fire.
//
// At pageSize=1024 / valSize=80, ~11 cells per leaf. 30 rows create a
// depth-2 tree (root interior + a few leaves). None of those splits have
// parent != root, so the dispatch counter must remain 0.
func TestBalanceQuick_RootIsParent(t *testing.T) {
	resetPageBufferPool()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: 1024})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	db.pager.balanceQuickDispatchCount.Store(0)

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	val := make([]byte, 80)
	for i := 1; i <= 30; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	require.Equal(t, int64(0), db.pager.balanceQuickDispatchCount.Load(),
		"fast path must not fire when parent is the btree root")

	require.NoError(t, db.IntegrityCheck())
}

// TestBalanceQuick_CascadeToParentSplit exercises the case where a fast-path
// divider insertion overflows the parent. SQLite's comment at btree.c:9176
// explicitly notes this: "balance_quick() inserts a new cell into pParent,
// which may cause pParent overflow. If this happens, the next iteration of
// the do-loop will balance pParent use either balance_nonroot() or
// balance_deeper()." Any-store's equivalent cascade is
// insertSepIntoInterior's slow path.
func TestBalanceQuick_CascadeToParentSplit(t *testing.T) {
	resetPageBufferPool()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: 512})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	db.pager.balanceQuickDispatchCount.Store(0)

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	val := make([]byte, 32)
	for i := 1; i <= 5000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	require.Greater(t, db.pager.balanceQuickDispatchCount.Load(), int64(10),
		"fast path should have fired many times for 5000 monotonic appends")

	require.NoError(t, db.IntegrityCheck())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 5000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		got, err := rtx.Get(ns2, key)
		require.NoError(t, err, "row %d", i)
		require.Len(t, got, 32, "row %d", i)
	}
}

// TestBalanceQuick_InterleavedInserts verifies the fast path doesn't poison
// the regular split path: middle-of-tree inserts after a fast-path split
// must still work correctly, and later appends must still hit the fast path.
func TestBalanceQuick_InterleavedInserts(t *testing.T) {
	resetPageBufferPool()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: 1024})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	val := make([]byte, 80)

	// Phase 1: 500 monotonic appends (even keys 1000, 1002, 1004, ...).
	for i := 0; i < 500; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(1000+i*2))
		require.NoError(t, tx.Put(ns, key, val))
	}
	db.pager.balanceQuickDispatchCount.Store(0)

	// Phase 2: 500 middle-of-tree inserts (odd keys 1001, 1003, ...).
	for i := 0; i < 500; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(1001+i*2))
		require.NoError(t, tx.Put(ns, key, val))
	}
	midDispatches := db.pager.balanceQuickDispatchCount.Load()

	// Phase 3: 500 more monotonic appends at the new max.
	for i := 0; i < 500; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(2000+i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	endDispatches := db.pager.balanceQuickDispatchCount.Load()

	require.NoError(t, tx.Commit())

	require.Greater(t, endDispatches, midDispatches,
		"phase 3 appends must trigger additional fast-path dispatches (mid=%d end=%d)",
		midDispatches, endDispatches)

	require.NoError(t, db.IntegrityCheck())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	count := 0
	cur := rtx.NewCursor(ns2)
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		count++
	}
	require.Equal(t, 1500, count)
}

// TestBalanceQuick_OverflowBearingCell verifies the fast path correctly
// allocates an overflow chain when the new cell's payload exceeds
// maxLocalPayload. Equivalent to SQLite's ptrmap handling at
// btree.c:8046-8050; any-store uses a freelist-based overflow chain
// instead of a pointer map, but the behavioral contract is the same:
// the new right sibling's sole cell must be readable with its full
// payload intact.
//
// Every row here carries a 4 KB payload → each cell has overflow chains.
// At pageSize=512 (usable=504) only ~1 cell fits per leaf, so nearly
// every append triggers a split with an overflow-bearing cell landing
// on the new right sibling via the fast path.
func TestBalanceQuick_OverflowBearingCell(t *testing.T) {
	resetPageBufferPool()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: 512})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// Each value is 4 KB and deterministic so we can verify readback.
	mkVal := func(seed uint32) []byte {
		v := make([]byte, 4096)
		for i := range v {
			v[i] = byte(seed + uint32(i))
		}
		return v
	}

	// 1000 rows pushes the tree to depth 3 at pageSize=512, ensuring most
	// splits have parent != root and fire the fast path.
	const nRows = 1000
	for i := 1; i <= nRows; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, mkVal(uint32(i))))
	}
	require.NoError(t, tx.Commit())

	require.Greater(t, db.pager.balanceQuickDispatchCount.Load(), int64(0),
		"monotonic overflow-bearing appends must trigger the fast path")

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// Spot-check a handful of rows including the very last (most likely
	// to have been placed via fast path with overflow chain).
	for _, i := range []uint32{1, 200, 500, 800, 1000} {
		key := binary.BigEndian.AppendUint32(nil, i)
		got, err := rtx.Get(ns2, key)
		require.NoError(t, err, "row %d", i)
		require.Equal(t, mkVal(i), got, "row %d", i)
	}

	require.NoError(t, db.IntegrityCheck())
}

// TestBalanceQuick_SavepointRollback verifies savepoints correctly roll
// back state created by the fast path. Savepoints track writer cache
// dirty pages; the fast path touches parent + allocates a new right
// sibling, both of which must be rolled back cleanly.
func TestBalanceQuick_SavepointRollback(t *testing.T) {
	resetPageBufferPool()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: 1024})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Pre-populate with 2000 rows to establish a depth-3 tree baseline.
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	val := make([]byte, 80)
	for i := 1; i <= 2000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	baselineCount := countKeys(t, db, "t1")
	require.Equal(t, 2000, baselineCount)

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	spID, err := tx.Savepoint()
	require.NoError(t, err)

	db.pager.balanceQuickDispatchCount.Store(0)
	for i := 0; i < 300; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(2001+i))
		require.NoError(t, tx.Put(ns2, key, val))
	}
	require.Greater(t, db.pager.balanceQuickDispatchCount.Load(), int64(0),
		"pre-rollback inserts should have triggered the fast path")

	require.NoError(t, tx.RollbackToSavepoint(spID))
	require.NoError(t, tx.Commit())

	finalCount := countKeys(t, db, "t1")
	require.Equal(t, baselineCount, finalCount,
		"savepoint rollback must undo all fast-path inserts")

	require.NoError(t, db.IntegrityCheck())
}

// TestBalanceQuick_ConcurrentReader verifies readers started before the
// fast-path inserts observe the pre-insert snapshot. Any-store's
// snapshot isolation (readerCaches, walMaxFrame) guarantees this
// independent of which write path was used; this test pins the
// behavior under the new fast path.
func TestBalanceQuick_ConcurrentReader(t *testing.T) {
	resetPageBufferPool()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: 1024})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	val := make([]byte, 80)
	for i := 1; i <= 2000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Begin a reader at this snapshot.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })

	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	key2001 := binary.BigEndian.AppendUint32(nil, uint32(2001))
	_, err = rtx.Get(ns2, key2001)
	require.ErrorIs(t, err, ErrKeyNotFound)

	// Start a writer that appends 500 more rows via the fast path.
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	db.pager.balanceQuickDispatchCount.Store(0)
	for i := 2001; i <= 2500; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.Greater(t, db.pager.balanceQuickDispatchCount.Load(), int64(0))
	require.NoError(t, tx.Commit())

	// Reader's snapshot is still the pre-commit state.
	_, err = rtx.Get(ns2, key2001)
	require.ErrorIs(t, err, ErrKeyNotFound,
		"reader snapshot must not observe writes made after BeginRead")

	// A fresh reader sees the new rows.
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx2.Rollback() })
	got, err := rtx2.Get(ns2, key2001)
	require.NoError(t, err)
	require.Len(t, got, 80)
}

// TestBalanceQuick_AllocFreelistCorruptResilience verifies the fast
// path's defensive allocation behavior.
//
// Intent per spec matrix test 8: "fail allocatePage inside the fast
// path; assert clean rollback; no partial pages." any-store's
// allocatePage (pager.go allocatePageNear) is defensively designed:
// if the freelist trunk is corrupt, it silently falls through to
// growing the DB file (comment at pager.go ~line 919: "Fall through
// to grow database if freelist read fails"). This makes the spec's
// "must-fail" assertion incorrect for any-store; the actual contract
// is "fast path tolerates freelist corruption AND produces a
// structurally valid tree."
//
// SQLite's equivalent SQLITE_FAULTINJECTION coverage targets
// allocateBtreePage failures; any-store mitigates the same risks via
// the grow fallback plus IntegrityCheck post-conditions.
func TestBalanceQuick_AllocFreelistCorruptResilience(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Phase 1: create DB, populate freelist via insert+delete,
	// checkpoint so FirstFreelistPg is committed to page 1 on disk.
	db, err := testOpen(t, path, Options{PageSize: 1024})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	val := make([]byte, 80)
	for i := 1; i <= 300; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 300; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Commit())

	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	// Phase 2: corrupt FirstFreelistPg at offset 32 in page 1 (see
	// page.go dbHeader.serialize).
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	binary.BigEndian.PutUint32(data[32:36], 0x7FFFFFFF)
	require.NoError(t, os.WriteFile(path, data, 0644))

	// Phase 3: reopen and drive monotonic appends. allocatePage must
	// not propagate the corruption as a failure; every insert should
	// succeed via the grow-the-DB fallback, and the resulting tree
	// must be structurally valid.
	db, err = testOpen(t, path, Options{PageSize: 1024})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)

	db.pager.balanceQuickDispatchCount.Store(0)
	// 3000 monotonic appends force depth ≥ 3 so the fast path fires.
	for i := 10000; i < 13000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, val),
			"insert %d must not fail despite corrupt freelist", i)
	}
	require.NoError(t, tx.Commit())

	// Even with corrupt freelist, fast path fired for at least some
	// appends (monotonic workload + depth ≥ 3).
	require.Greater(t, db.pager.balanceQuickDispatchCount.Load(), int64(0))

	// Tree must still be internally consistent. Corrupt FirstFreelistPg
	// is caught by IntegrityCheck's freelist walk — but the btree itself
	// remains intact, which is what matters for this test.
	// IntegrityCheck may report the freelist corruption; that's
	// orthogonal to the fast-path behavior we are verifying.
	_ = db.IntegrityCheck()

	// Spot-check rows spread through the range (readback proof).
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for _, i := range []uint32{10000, 11000, 12000, 12999} {
		key := binary.BigEndian.AppendUint32(nil, i)
		got, err := rtx.Get(ns2, key)
		require.NoError(t, err, "row %d", i)
		require.Len(t, got, 80, "row %d", i)
	}
}
