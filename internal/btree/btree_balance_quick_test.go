package btree

import (
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBalanceQuick_AppendFillFactor quantifies the cost of not having a
// balance_quick-style fast path in any-store.
//
// SQLite's balance_quick (btree.c:7992) handles the "rightmost append"
// special case: when the page that overflows is the rightmost child of its
// parent AND the overflow cell is the rightmost cell, SQLite keeps the full
// left page intact and puts just the one new cell on a fresh right sibling.
//
// Any-store's splitLeafAndInsertWithPath always runs leafSplitPoint
// (btree.go:217), which targets ~2/3 fill on the left. For monotonic append
// workloads (the common any-store pattern: time-ordered ids inside a
// collection) this freezes "internal" leaves at ~2/3 fill instead of ~100%,
// inflating leaf count by ~50%.
//
// This test is a measurement, not a regression check. It prints fill stats
// and leaf counts so you can see the gap concretely. Run with:
//
//	go test -run TestBalanceQuick_AppendFillFactor -v ./internal/btree/...
func TestBalanceQuick_AppendFillFactor(t *testing.T) {
	// Small page + fixed-size rows to make the steady-state pattern obvious.
	// Picks that matter:
	//   - PageSize=1024 → ~1016 usable bytes on leaves (8B leaf header).
	//   - val=80 bytes + 4-byte key → ~88B per cell (incl. 2B pointer).
	//   - nRows=5000 → ~440 leaves at 100% fill, ~660 leaves at ~67% fill.
	const (
		pageSize = 1024
		nRows    = 5000
		valSize  = 80
	)

	cases := []struct {
		name    string
		fillKey func(i int) []byte
	}{
		{
			name: "monotonic_append", // worst case for current code
			fillKey: func(i int) []byte {
				k := make([]byte, 4)
				binary.BigEndian.PutUint32(k, uint32(i+1))
				return k
			},
		},
		{
			name: "random", // baseline: SQLite's balance_nonroot doesn't help here either
			fillKey: func(i int) []byte {
				// Deterministic "random" keys: hash idx → 4 bytes.
				// Using a fixed seed so the test is reproducible.
				rng := rand.New(rand.NewSource(int64(i) * 2862933555777941757))
				k := make([]byte, 4)
				binary.BigEndian.PutUint32(k, rng.Uint32())
				return k
			},
		},
	}

	val := make([]byte, valSize)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetPageBufferPool()
			dir := t.TempDir()
			db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: pageSize})
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			// Create namespace.
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			_, err = tx.CreateNamespace("t1")
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			// Insert in a single tx.
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)

			for i := 0; i < nRows; i++ {
				require.NoError(t, tx.Put(ns, tc.fillKey(i), val))
			}
			require.NoError(t, tx.Commit())

			// Walk the tree using a read tx so pager state is consistent.
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer func() { _ = rtx.Rollback() }()

			ns2, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)

			bt := &btree{
				pager:       db.pager,
				rootPage:    ns2.rootPage,
				walMaxFrame: rtx.walMaxFrame,
			}
			stats := walkLeavesForFill(t, bt)
			usable := bt.usablePageSize()

			reportFillStats(t, tc.name, stats, usable, nRows)

			// Regression guard for the balance_quick fast path
			// (splitLeafRightmostAppend / dispatch in
			// splitLeafAndInsertWithPath). Monotonic appends must
			// produce near-full leaves; a regression where
			// leafSplitPoint ran on rightmost appends would drop
			// avg fill back toward 60%.
			if tc.name == "monotonic_append" {
				const leafHeaderSize = 8
				leafCapacity := usable - leafHeaderSize
				used := stats.totalUsed()
				avgFill := float64(used) / float64(stats.leafCount*leafCapacity)
				require.GreaterOrEqual(t, avgFill, 0.85,
					"monotonic-append avg leaf fill regressed below 85%%: %.1f%% across %d leaves",
					avgFill*100, stats.leafCount)
			}
		})
	}
}

type leafFillStats struct {
	leafCount     int
	interiorCount int
	totalCells    int
	perLeafUsed   []int // used bytes per leaf
	perLeafCells  []int // cell count per leaf
	usableSize    int
}

func (s *leafFillStats) totalUsed() int {
	total := 0
	for _, u := range s.perLeafUsed {
		total += u
	}
	return total
}

func walkLeavesForFill(t *testing.T, bt *btree) *leafFillStats {
	t.Helper()
	stats := &leafFillStats{usableSize: bt.usablePageSize()}
	walkForFill(t, bt, bt.rootPage, stats)
	return stats
}

func walkForFill(t *testing.T, bt *btree, pgno uint32, stats *leafFillStats) {
	t.Helper()
	pg, err := bt.getPage(pgno)
	require.NoError(t, err)

	if pg.header.isLeaf() {
		n := int(pg.header.cellCount)
		// cell pointers + (cell content area - fragmented bytes).
		used := n*2 + (stats.usableSize - int(pg.header.cellContentOff)) - int(pg.header.fragBytes)
		stats.leafCount++
		stats.totalCells += n
		stats.perLeafUsed = append(stats.perLeafUsed, used)
		stats.perLeafCells = append(stats.perLeafCells, n)
		bt.pager.releasePage(pg)
		return
	}

	// Interior: recurse into every child.
	stats.interiorCount++
	n := int(pg.header.cellCount)
	cpOff := pg.cellPointerOffset()
	children := make([]uint32, 0, n+1)
	for i := 0; i < n; i++ {
		off := int(binary.BigEndian.Uint16(pg.data[cpOff+i*2:]))
		children = append(children, binary.BigEndian.Uint32(pg.data[off:off+4]))
	}
	children = append(children, pg.header.rightChild)
	bt.pager.releasePage(pg)

	for _, c := range children {
		walkForFill(t, bt, c, stats)
	}
}

func reportFillStats(t *testing.T, mode string, s *leafFillStats, usable, nRows int) {
	t.Helper()
	if s.leafCount == 0 {
		t.Fatalf("no leaves found")
	}

	// Leaf-only denominator: each leaf has `usable - leafHeaderSize` bytes
	// available for cell pointers + cell content.
	const leafHeaderSize = 8
	leafCapacity := usable - leafHeaderSize
	totalCapacity := s.leafCount * leafCapacity

	used := s.totalUsed()
	avg := 100.0 * float64(used) / float64(totalCapacity)

	sorted := make([]int, len(s.perLeafUsed))
	copy(sorted, s.perLeafUsed)
	sort.Ints(sorted)
	minFill := 100.0 * float64(sorted[0]) / float64(leafCapacity)
	maxFill := 100.0 * float64(sorted[len(sorted)-1]) / float64(leafCapacity)
	medFill := 100.0 * float64(sorted[len(sorted)/2]) / float64(leafCapacity)

	// Theoretical balance_quick steady state: all leaves except possibly
	// the rightmost one at ~100% fill. Compute the min leaves needed to
	// hold all cells at 100% fill (with one partial rightmost leaf).
	// Assume steady-state cell size from the observed average.
	avgCellBytes := 0
	if s.totalCells > 0 {
		avgCellBytes = used / s.totalCells // includes 2B cell pointer
	}
	cellsPerFullLeaf := 0
	if avgCellBytes > 0 {
		cellsPerFullLeaf = leafCapacity / avgCellBytes
	}
	idealLeafCount := 0
	if cellsPerFullLeaf > 0 {
		idealLeafCount = (s.totalCells + cellsPerFullLeaf - 1) / cellsPerFullLeaf
	}

	// Summary.
	t.Logf("")
	t.Logf("===== %s (nRows=%d, valSize=80, pageSize=%d, usable=%d) =====",
		mode, nRows, usable+leafHeaderSize, usable)
	t.Logf("total cells         : %d", s.totalCells)
	t.Logf("leaves              : %d  (interior: %d)", s.leafCount, s.interiorCount)
	t.Logf("avg cell bytes      : %d (incl. 2B cell pointer)", avgCellBytes)
	t.Logf("cells per full leaf : %d", cellsPerFullLeaf)
	t.Logf("leaf fill factor    : avg=%.1f%%  median=%.1f%%  min=%.1f%%  max=%.1f%%",
		avg, medFill, minFill, maxFill)
	t.Logf("ideal (balance_quick) leaf count: %d", idealLeafCount)
	if idealLeafCount > 0 {
		overhead := 100.0 * float64(s.leafCount-idealLeafCount) / float64(idealLeafCount)
		t.Logf("leaf-count overhead vs ideal   : %+.1f%% (extra leaves we're using)", overhead)
	}

	// Fill-factor histogram.
	t.Logf("fill-factor distribution:")
	buckets := []struct {
		label string
		lo    float64
		hi    float64
	}{
		{"   0-25%", 0, 25},
		{"  25-50%", 25, 50},
		{"  50-60%", 50, 60},
		{"  60-70%", 60, 70},
		{"  70-80%", 70, 80},
		{"  80-90%", 80, 90},
		{"  90-99%", 90, 99},
		{" 99-100%", 99, 100.01},
	}
	for _, b := range buckets {
		count := 0
		for _, u := range s.perLeafUsed {
			pct := 100.0 * float64(u) / float64(leafCapacity)
			if pct >= b.lo && pct < b.hi {
				count++
			}
		}
		bar := ""
		n := count * 40 / s.leafCount
		for i := 0; i < n; i++ {
			bar += "#"
		}
		t.Logf("  %s : %5d  %s", b.label, count, bar)
	}
}

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

// TestBalanceQuick_AllocFreelistCorruptResilience verifies that an
// allocation attempt against a corrupt freelist is DETECTED and surfaced
// as ErrCorrupt rather than being silently swallowed and grown over.
//
// Intent per spec matrix test 8: "fail allocatePage inside the fast
// path; assert clean rollback; no partial pages." Matching SQLite
// allocateBtreePage (btree.c:6543 freelist branch -> return rc): when
// FirstFreelistPg != 0, allocatePage allocates from the freelist and
// propagates any error (including ErrCorrupt). The grow-the-DB fallback
// (btree.c:6758-6815 else branch) only runs when the freelist is empty.
// A corrupt non-zero FirstFreelistPg therefore fails the allocation,
// which rolls back cleanly with no partial pages committed.
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
	// page.go dbHeader.serialize). 0x7FFFFFFF > dbSize trips the
	// trunk-page guard in allocateFromFreelist.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	binary.BigEndian.PutUint32(data[32:36], 0x7FFFFFFF)
	require.NoError(t, os.WriteFile(path, data, 0644))

	// Phase 3: reopen and drive monotonic appends. Because the freelist
	// is non-empty (and corrupt), the first allocation that consults it
	// must surface ErrCorrupt instead of silently growing the DB.
	db, err = testOpen(t, path, Options{PageSize: 1024})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// Some leading appends fit on existing pages without allocating; the
	// first insert that needs a fresh page consults the corrupt freelist
	// and must fail with ErrCorrupt (detection, not silent grow).
	var putErr error
	for i := 10000; i < 13000 && putErr == nil; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		putErr = tx.Put(ns, key, val)
	}
	require.ErrorIs(t, putErr, ErrCorrupt,
		"corrupt freelist must be detected, not silently grown over")

	// Clean rollback: the failed allocation must not leave partial state.
	require.NoError(t, tx.Rollback())
}

// TestBalanceQuick_ZeroCellCorruptGuard exercises the corruption guard at the
// top of splitLeafRightmostAppend, a port of balance_quick's first statement
// (btree.c:8020 `if( pPage->nCell==0 ) return SQLITE_CORRUPT_BKPT;`, added for
// dbfuzz001.test). An over-full page that reports zero cells is corrupt and
// must be rejected BEFORE any allocation or parent mutation.
func TestBalanceQuick_ZeroCellCorruptGuard(t *testing.T) {
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

	// Build a depth-2+ tree via monotonic appends so a non-root leaf has a
	// non-root parent reached through that parent's rightChild — exactly the
	// shape splitLeafRightmostAppend operates on.
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

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// Build the btree exactly as WriteTx.Put does so reads/writes see the
	// writer's spilled pages (walMaxFrame + writable).
	bt := &btree{pager: tx.pager, rootPage: ns.rootPage, walMaxFrame: tx.walHdr.mxFrame, writable: true}

	// Descend from the root following rightChild to the rightmost leaf,
	// recording the path exactly as Put does (cellIdx == nCell at each hop).
	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	var path []pathEntry
	for pg.header.isInterior() {
		nCell := pg.header.cellCount
		rightChild := pg.header.rightChild
		path = append(path, pathEntry{pgno: pg.pgno, cellIdx: nCell, nCell: nCell})
		bt.pager.releasePage(pg)
		pg, err = bt.getPage(rightChild)
		require.NoError(t, err)
	}
	leafPgno := pg.pgno
	bt.pager.releasePage(pg)

	// Require the shape the fast path needs: a non-root parent reached via
	// its rightChild. Otherwise the test is not exercising the guard's path.
	require.NotEmpty(t, path)
	require.NotEqual(t, bt.rootPage, path[len(path)-1].pgno,
		"need a non-root parent for the rightmost-append fast path")

	wpg, err := bt.pager.getWritablePage(leafPgno)
	require.NoError(t, err)
	t.Cleanup(func() { bt.pager.releasePage(wpg) })

	// Forge the corruption: an otherwise-real leaf reporting zero cells.
	savedCellCount := wpg.header.cellCount
	require.Greater(t, savedCellCount, uint16(0))
	wpg.header.cellCount = 0

	before := db.pager.balanceQuickDispatchCount.Load()
	key := binary.BigEndian.AppendUint32(nil, uint32(2001))
	err = bt.splitLeafRightmostAppend(wpg, key, val, path)

	// Restore the header before any assertion can abort the test, so we never
	// leave a forged zero-cell page behind in the live transaction.
	wpg.header.cellCount = savedCellCount

	require.ErrorIs(t, err, ErrCorrupt,
		"zero-cell over-full page must be rejected as corruption")
	require.Equal(t, before, db.pager.balanceQuickDispatchCount.Load(),
		"guard must fire before allocation/parent mutation (no dispatch counted)")

	require.NoError(t, tx.Rollback())
}
