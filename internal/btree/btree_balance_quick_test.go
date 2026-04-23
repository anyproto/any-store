package btree

import (
	"encoding/binary"
	"math/rand"
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
