package btree

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// assertDescentNeverHitsZeroCellInterior pins the read-path invariant behind
// drift-11 (docs/btree/NOTES.md#drift-11-movetochild-child-page-ncell-greater-than-equal-one-descent-):
// on a VALID tree, no cursor/seek descent step ever lands on a zero-cell
// (nCell<1) interior page. SQLite enforces this with moveToChild's
// `pPage->nCell<1 -> SQLITE_CORRUPT_PGNO` guard (btree.c:5477-5482, inlined at
// btree.c:6253-6258); the Go descent paths carry no such per-child guard, so the
// guard is "kept by-design" (drift-11) ONLY because a valid any-store tree never
// produces a descendable zero-cell interior. This helper replays every descent
// path the cursor uses — First (cell-0 child), Last (rightChild), and Seek
// (searchInterior) — over a set of probe keys and asserts cellCount>=1 on every
// interior page it touches, including the root, mirroring the exact child-pointer
// arithmetic in Cursor.First/Last/Seek (btree.go:3344-3366, 3391-3405,
// 3430-3447) and searchInterior (btree.go:936-989).
func assertDescentNeverHitsZeroCellInterior(t *testing.T, bt *btree, probeKeys [][]byte) {
	t.Helper()

	// checkInterior fails if pg is an interior page with 0 cells: that is exactly
	// the page SQLite's moveToChild would reject, and the page searchInterior
	// would mis-read (n==0 skips the loop, then dereferences the cleared first
	// cell-pointer slot as a child pointer instead of routing to rightChild).
	checkInterior := func(pg *page, via string) {
		t.Helper()
		if pg.header.isInterior() {
			require.NotZero(t, int(pg.header.cellCount),
				"%s descent reached a zero-cell interior page %d (drift-11 invariant violated)", via, pg.pgno)
		}
	}

	// First: descend through the cell-0 child pointer to the leftmost leaf.
	descendFirst := func() {
		pg, err := bt.getPage(bt.rootPage)
		require.NoError(t, err)
		for pg.header.isInterior() {
			checkInterior(pg, "First")
			off := int(pg.getCellOffset(0))
			require.LessOrEqual(t, off+4, len(pg.data))
			child := binary.BigEndian.Uint32(pg.data[off : off+4])
			bt.pager.releasePage(pg)
			pg, err = bt.descendChild(child)
			require.NoError(t, err)
		}
		bt.pager.releasePage(pg)
	}

	// Last: descend through rightChild to the rightmost leaf.
	descendLast := func() {
		pg, err := bt.getPage(bt.rootPage)
		require.NoError(t, err)
		for pg.header.isInterior() {
			checkInterior(pg, "Last")
			child := pg.header.rightChild
			bt.pager.releasePage(pg)
			pg, err = bt.descendChild(child)
			require.NoError(t, err)
		}
		bt.pager.releasePage(pg)
	}

	// Seek(key): descend via searchInterior (the binary-search router used by
	// Cursor.Seek) to the target leaf, checking every interior page on the way.
	descendSeek := func(key []byte) {
		pg, err := bt.getPage(bt.rootPage)
		require.NoError(t, err)
		for pg.header.isInterior() {
			checkInterior(pg, "Seek")
			child, _, serr := bt.searchInterior(pg, key)
			require.NoError(t, serr)
			bt.pager.releasePage(pg)
			pg, err = bt.descendChild(child)
			require.NoError(t, err)
		}
		bt.pager.releasePage(pg)
	}

	descendFirst()
	descendLast()
	for _, k := range probeKeys {
		descendSeek(k)
	}
}

// TestMoveToChild_DescentNeverHitsZeroCellInterior pins the drift-11 read-path
// invariant: on a valid any-store tree, cursor/seek descent never lands on a
// zero-cell (nCell<1) interior page, so the missing moveToChild nCell>=1 guard
// (btree.c:5477-5482) is unreachable by-design rather than unsafe.
//
// It builds a multi-level tree, then drives a heavy delete workload. Deletes are
// the only operations that can transiently produce a zero-cell non-root interior
// (finishParentRemoval, btree.go), and the delete-rebalance cascade
// (completeMergeUpward) must eliminate every such node before commit. The test
// asserts the invariant three ways after EACH committed mutation phase:
//
//  1. structural: no non-root interior page persists with 0 dividers
//     (assertNoDegenerateInterior — a descendable zero-cell interior can only
//     exist if such a degenerate node survived);
//  2. descent-path: replaying First/Last/Seek descent over probe keys never
//     touches a zero-cell interior (assertDescentNeverHitsZeroCellInterior);
//  3. behavioral: the real Cursor.First/Next/Last/Seek agree with the expected
//     survivor set, proving no descent silently bailed at a zero-cell interior.
func TestMoveToChild_DescentNeverHitsZeroCellInterior(t *testing.T) {
	// 512-byte pages (minimum) give a low fanout, so the tree is multi-level and
	// deletes empty whole leaves / parents — the case that can transiently create
	// a zero-cell interior the cascade must remove before commit.
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	const total = 2000
	putN(t, db, "t1", total, 40)
	require.NoError(t, db.IntegrityCheck(), "integrity after insert")

	rootOf := func() uint32 {
		ns, nerr := db.getNamespaceLocked("t1")
		require.NoError(t, nerr)
		return ns.rootPage
	}

	keyOf := func(i int) []byte { return binary.BigEndian.AppendUint32(nil, uint32(i)) }

	// checkInvariant builds a read-snapshot btree (the same snapshot a cursor
	// uses) and asserts the structural + descent-path invariants, then drives the
	// real cursor against the expected survivor set.
	checkInvariant := func(phase string, survivors [][]byte) {
		t.Helper()
		require.NoError(t, db.IntegrityCheck(), "integrity %s", phase)

		rtx, rerr := db.BeginRead()
		require.NoError(t, rerr)
		defer func() { _ = rtx.Rollback() }()
		ns, nerr := db.getNamespaceLocked("t1")
		require.NoError(t, nerr)
		bt := &btree{pager: db.pager, cache: rtx.cache, rootPage: ns.rootPage, walMaxFrame: rtx.walMaxFrame}

		// (1) No degenerate single-child non-root interior survived a commit.
		assertNoDegenerateInterior(t, bt, rootOf(), true)

		// (2) Descent never lands on a zero-cell interior. Probe with the
		// survivor keys plus boundary keys (below-min, above-max, and gaps).
		probes := make([][]byte, 0, len(survivors)+3)
		probes = append(probes, keyOf(0), keyOf(total+1))
		probes = append(probes, survivors...)
		// Also probe between survivors (deleted-key gaps) to exercise the
		// "descend to first key >= probe" router on absent keys.
		for i := 1; i <= total; i += 3 {
			probes = append(probes, keyOf(i))
		}
		assertDescentNeverHitsZeroCellInterior(t, bt, probes)

		// (3) Behavioral: real cursor forward scan == sorted survivor set, and
		// Seek/SeekExact land correctly — proving no descent silently bailed.
		cur := rtx.NewCursor(ns)
		fwd := make([][]byte, 0, len(survivors))
		for cerr := cur.First(); cerr == nil && cur.Valid(); cerr = cur.Next() {
			k, kerr := cur.Key()
			require.NoError(t, kerr)
			fwd = append(fwd, append([]byte(nil), k...))
		}
		require.Equal(t, len(survivors), len(fwd), "%s: forward scan count", phase)
		for i := range survivors {
			require.True(t, bytes.Equal(survivors[i], fwd[i]),
				"%s: forward scan mismatch at %d", phase, i)
		}
		// Each survivor is reachable by Seek (descent succeeded to its leaf).
		for _, k := range survivors {
			require.NoError(t, cur.SeekExact(k), "%s: SeekExact survivor", phase)
			require.True(t, cur.Valid(), "%s: cursor valid after SeekExact", phase)
		}
		// Last lands on the max survivor.
		if len(survivors) > 0 {
			require.NoError(t, cur.Last())
			require.True(t, cur.Valid())
			lk, lerr := cur.Key()
			require.NoError(t, lerr)
			require.True(t, bytes.Equal(survivors[len(survivors)-1], lk),
				"%s: Last == max survivor", phase)
		}
	}

	sortedSurvivors := func(keep func(int) bool) [][]byte {
		s := make([][]byte, 0, total)
		for i := 1; i <= total; i++ {
			if keep(i) {
				s = append(s, keyOf(i))
			}
		}
		sort.Slice(s, func(a, b int) bool { return bytes.Compare(s[a], s[b]) < 0 })
		return s
	}

	// Phase 0: full tree, all keys present.
	checkInvariant("after-insert", sortedSurvivors(func(int) bool { return true }))

	// Phase 1: delete a sparse subset (every 2nd key) — thins many leaves and
	// empties some, forcing rebalancing/merges that transiently create and then
	// cascade away zero-cell interiors.
	keep1 := func(i int) bool { return i%2 == 0 }
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= total; i++ {
		if !keep1(i) {
			require.NoError(t, tx.Delete(ns, keyOf(i)))
		}
	}
	require.NoError(t, tx.Commit())
	checkInvariant("after-delete-half", sortedSurvivors(keep1))

	// Phase 2: aggressive delete — keep only every 7th of the ORIGINAL keys
	// (i.e. drop almost everything still present), driving deep upward cascades
	// that repeatedly empty parents. keep7 is a subset of keep1's survivors.
	keep7 := func(i int) bool { return i%2 == 0 && i%7 == 0 }
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= total; i++ {
		if keep1(i) && !keep7(i) {
			require.NoError(t, tx.Delete(ns, keyOf(i)))
		}
	}
	require.NoError(t, tx.Commit())
	checkInvariant("after-delete-aggressive", sortedSurvivors(keep7))
}
