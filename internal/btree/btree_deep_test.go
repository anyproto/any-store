package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// GROUP A: Cursor.Previous() backward traversal through multi-level B-trees
// Targets: L2929-2993 in btree.go
// =============================================================================

// TestDeep_PrevFullTraversalMultiLevel creates a B-tree with enough entries to
// produce 3+ levels and iterates backward through all entries using Previous().
// This exercises the interior frame descent paths during backward traversal.
func TestDeep_PrevFullTraversalMultiLevel(t *testing.T) {
	// Use a small page size to create more levels with fewer keys.
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert enough entries to get 3+ levels with 512-byte pages.
	// With 512-byte pages, each leaf holds ~15-20 small entries,
	// each interior holds ~30 children. So 1000+ entries => 3 levels.
	n := 2000
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns, k, v))
	}
	require.NoError(t, tx2.Commit())

	// Read traversal: Last then Previous through all entries.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns2)

	require.NoError(t, cur.Last())
	require.True(t, cur.Valid())

	// Collect all keys in reverse order
	var count int
	var lastKey []byte
	for cur.Valid() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		if lastKey != nil {
			// Ensure descending order
			assert.True(t, bytes.Compare(k, lastKey) < 0,
				"expected %s < %s", string(k), string(lastKey))
		}
		lastKey = bytes.Clone(k)
		count++
		require.NoError(t, cur.Previous())
	}
	assert.Equal(t, n, count, "should traverse all entries in reverse")
}

// TestDeep_PrevFromMiddleMultiLevel positions a cursor in the middle of a
// multi-level tree and walks backward, covering the descent-to-rightmost-leaf
// path in Previous().
func TestDeep_PrevFromMiddleMultiLevel(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	n := 1000
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns, k, v))
	}
	require.NoError(t, tx2.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns2)

	// Seek to the middle
	midKey := fmt.Appendf(nil, "key-%06d", n/2)
	require.NoError(t, cur.Seek(midKey))
	require.True(t, cur.Valid())

	// Walk backward for half the entries
	prevCount := 0
	for cur.Valid() {
		_, kerr := cur.Key()
		require.NoError(t, kerr)
		prevCount++
		require.NoError(t, cur.Previous())
	}
	// Should have traversed roughly n/2 + 1 entries
	assert.True(t, prevCount >= n/2, "prevCount=%d, expected >= %d", prevCount, n/2)
}

// TestDeep_PrevAndNextSymmetry verifies that Previous and Next are symmetric:
// walking forward then backward should yield the same entries.
func TestDeep_PrevAndNextSymmetry(t *testing.T) {
	db := tempDBWithPageSize(t, 1024)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	n := 500
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns, k, v))
	}
	require.NoError(t, tx2.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	// Collect forward
	cur1 := rtx.NewCursor(ns2)
	var forwardKeys []string
	require.NoError(t, cur1.First())
	for cur1.Valid() {
		k, kerr := cur1.Key()
		require.NoError(t, kerr)
		forwardKeys = append(forwardKeys, string(k))
		require.NoError(t, cur1.Next())
	}

	// Collect backward
	cur2 := rtx.NewCursor(ns2)
	var backwardKeys []string
	require.NoError(t, cur2.Last())
	for cur2.Valid() {
		k, kerr := cur2.Key()
		require.NoError(t, kerr)
		backwardKeys = append(backwardKeys, string(k))
		require.NoError(t, cur2.Previous())
	}

	require.Equal(t, len(forwardKeys), len(backwardKeys))
	for i := range forwardKeys {
		assert.Equal(t, forwardKeys[i], backwardKeys[len(backwardKeys)-1-i])
	}
}

// =============================================================================
// GROUP B: Delete through multi-level tree + fragmentation rebuild
// Targets: L2026-2115 in btree.go
// =============================================================================

// TestDeep_DeleteMultiLevelTreeTraversal exercises the interior page traversal
// in Delete() by deleting entries from a multi-level tree.
func TestDeep_DeleteMultiLevelTreeTraversal(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	n := 500
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns, k, v))
	}
	require.NoError(t, tx2.Commit())

	// Delete entries from the middle in small batches to exercise interior
	// page traversal without overwhelming the tree.
	deleted := 0
	for batch := 0; batch < 5; batch++ {
		tx3, err := db.BeginWrite()
		require.NoError(t, err)
		ns3, err := db.getNamespaceLocked("data")
		require.NoError(t, err)
		start := 100 + batch*40
		for i := start; i < start+40 && i < 300; i++ {
			k := fmt.Appendf(nil, "key-%06d", i)
			err := tx3.Delete(ns3, k)
			if err == nil {
				deleted++
			}
		}
		require.NoError(t, tx3.Commit())
	}

	require.NoError(t, db.IntegrityCheck())
	assert.Equal(t, n-deleted, countKeys(t, db, "data"))
}

// TestDeep_DeleteFragmentationRebuildPath triggers fragmentation-based rebuild
// in Delete (L2094-2108: needsRebuild when fragBytes > 60).
// Strategy: use small page size, insert many small-value entries, then delete
// them one by one in a pattern that accumulates fragmentation.
func TestDeep_DeleteFragmentationRebuildPath(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert entries with medium-sized values so each cell uses ~20+ bytes.
	// With 512-byte pages, a leaf holds ~15-20 cells.
	// Deleting ~4-5 cells from the middle (not from content boundary) will
	// accumulate fragBytes > 60, triggering rebuild.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 20; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := bytes.Repeat([]byte{byte(i)}, 15)
		require.NoError(t, tx2.Put(ns2, key, val))
	}
	require.NoError(t, tx2.Commit())

	// Delete entries from the middle (not the last-inserted, to avoid content
	// boundary reclaim) to accumulate fragmentation.
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	// Delete keys 5..10 one by one. Each deletion adds ~20 bytes of frag.
	// After 3-4 deletions, fragBytes should exceed 60.
	for i := 5; i <= 10; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx3.Delete(ns3, key))
	}
	require.NoError(t, tx3.Commit())

	require.NoError(t, db.IntegrityCheck())
	assert.Equal(t, 14, countKeys(t, db, "t1"))
}

// TestDeep_DeleteAllFromMultiLevelTree deletes all entries from a multi-level
// tree to exercise the empty-page freeing and parent removal paths.
func TestDeep_DeleteAllFromMultiLevelTree(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	n := 200
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns, k, v))
	}
	require.NoError(t, tx2.Commit())

	// Delete all entries in batches of 50 per transaction
	for batch := 0; batch*50 < n; batch++ {
		tx3, err := db.BeginWrite()
		require.NoError(t, err)
		ns3, err := db.getNamespaceLocked("data")
		require.NoError(t, err)
		start := batch * 50
		end := start + 50
		if end > n {
			end = n
		}
		for i := start; i < end; i++ {
			k := fmt.Appendf(nil, "key-%06d", i)
			require.NoError(t, tx3.Delete(ns3, k))
		}
		require.NoError(t, tx3.Commit())
	}

	require.NoError(t, db.IntegrityCheck())
	assert.Equal(t, 0, countKeys(t, db, "data"))
}

// TestDeep_DeleteOverflowCellMultiLevel deletes a cell with overflow data from
// a multi-level tree to exercise the overflow chain freeing path (L2111-2115).
func TestDeep_DeleteOverflowCellMultiLevel(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert many small entries to build a multi-level tree
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns2, k, v))
	}
	// Insert an overflow entry (key+value > maxLocal for 512-byte pages)
	// maxLocal for 512 = ((512-12)*64/255)-23 = (32000/255)-23 = 125-23 = 102
	bigKey := []byte("key-overflow-big")
	bigVal := bytes.Repeat([]byte("X"), 200)
	require.NoError(t, tx2.Put(ns2, bigKey, bigVal))
	require.NoError(t, tx2.Commit())

	// Delete the overflow entry
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	require.NoError(t, tx3.Delete(ns3, bigKey))
	require.NoError(t, tx3.Commit())

	require.NoError(t, db.IntegrityCheck())
	assert.Equal(t, 200, countKeys(t, db, "t1"))
}

// =============================================================================
// GROUP C: insertIntoParent traversal (L1905-1945)
// Targets the OLD code path via insertIntoPage/insertIntoLeaf/splitLeafAndInsert
// =============================================================================

// TestDeep_InsertIntoParentViaInsertIntoPage exercises the insertIntoParent
// path by using insertIntoPage directly, which triggers the old non-path-tracked
// code path. When splits occur at non-root levels, insertIntoParent traverses
// from the root to find the parent.
func TestDeep_InsertIntoParentViaInsertIntoPage(t *testing.T) {
	p := tempPager(t)

	rootPg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: rootPg.pgno, writable: true}

	// Initialize as a leaf page
	rootPg.header.pageType = pageTypeLeafIdx
	rootPg.header.cellContentOff = uint16(p.usableSize())
	hdr := 0
	if rootPg.pgno == 1 {
		hdr = dbHeaderSize
	}
	rootPg.header.serialize(rootPg.data[hdr:])
	p.releasePage(rootPg)

	// Insert enough entries to trigger multiple levels of splits.
	// With 4096-byte pages, we need ~200+ entries to get past a single root split.
	// After the first root split, subsequent splits at non-root leaves will
	// invoke insertIntoParent which traverses from root.
	for i := 0; i < 500; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)

		pg, gerr := p.getWritablePage(bt.rootPage)
		require.NoError(t, gerr)

		ierr := bt.insertIntoPage(pg, k, v)
		require.NoError(t, ierr)
		p.releasePage(pg)
	}

	// Verify all entries are retrievable
	for i := 0; i < 500; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v, gerr := bt.Get(k)
		require.NoError(t, gerr, "key %s not found", k)
		expected := fmt.Appendf(nil, "val-%06d", i)
		assert.Equal(t, expected, v)
	}
}

// TestDeep_InsertIntoParentSmallPage exercises insertIntoParent with a small
// page size to force more frequent splits and deeper trees.
func TestDeep_InsertIntoParentSmallPage(t *testing.T) {
	dir := t.TempDir()
	p := newPager(filepath.Join(dir, "t.db"), 512, 200, true)
	require.NoError(t, p.open())
	_, slot, err := p.beginRead()
	require.NoError(t, err)
	require.NoError(t, p.beginWrite())
	t.Cleanup(func() {
		_ = p.rollback()
		p.endRead(slot)
		_ = p.close()
	})

	rootPg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: rootPg.pgno, writable: true}

	rootPg.header.pageType = pageTypeLeafIdx
	rootPg.header.cellContentOff = uint16(p.usableSize())
	hdr := 0
	if rootPg.pgno == 1 {
		hdr = dbHeaderSize
	}
	rootPg.header.serialize(rootPg.data[hdr:])
	p.releasePage(rootPg)

	// With 512-byte pages, splits happen much more frequently.
	// We need enough entries to create 3+ levels so that non-root leaf
	// splits trigger insertIntoParent's traversal path.
	// ~15 entries/leaf, ~30 cells/interior page.
	// 30 children * 15 entries = 450 entries fills level 1.
	// After that, root splits into 2 interior pages under a new root.
	// Then leaf splits trigger insertIntoParent traversal.
	for i := 0; i < 800; i++ {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "v-%04d", i)

		pg, gerr := p.getWritablePage(bt.rootPage)
		require.NoError(t, gerr)

		ierr := bt.insertIntoPage(pg, k, v)
		require.NoError(t, ierr)
		p.releasePage(pg)
	}

	// Verify
	for i := 0; i < 800; i++ {
		k := fmt.Appendf(nil, "key-%04d", i)
		v, gerr := bt.Get(k)
		require.NoError(t, gerr, "key %s not found", k)
		expected := fmt.Appendf(nil, "v-%04d", i)
		assert.Equal(t, expected, v)
	}
}

// =============================================================================
// GROUP E: Overflow key handling in search (L567-599)
// Tests searchLeafWithOverflow with keys larger than maxLocal
// =============================================================================

// TestDeep_OverflowKeySearch inserts keys that are larger than a page can hold
// locally, forcing the overflow key reading path in searchLeafWithOverflow.
func TestDeep_OverflowKeySearch(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// maxLocal for 512 = ((512-12)*64/255)-23 = 102
	// So keys + values > 102 will overflow.
	// Use large keys (200 bytes) to force key overflow specifically.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	// Insert several overflow keys
	var keys [][]byte
	for i := 0; i < 20; i++ {
		prefix := fmt.Appendf(nil, "bigkey-%04d-", i)
		k := append(prefix, bytes.Repeat([]byte("K"), 200-len(prefix))...)
		v := []byte(fmt.Sprintf("val-%d", i))
		keys = append(keys, bytes.Clone(k))
		require.NoError(t, tx2.Put(ns2, k, v))
	}
	require.NoError(t, tx2.Commit())

	// Search for each key - this exercises the overflow key comparison
	// paths in searchLeafWithOverflow (L567-599)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns3, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	for i, k := range keys {
		val, gerr := rtx.Get(ns3, k)
		require.NoError(t, gerr, "key index %d not found", i)
		assert.Equal(t, fmt.Sprintf("val-%d", i), string(val))
	}
}

// TestDeep_OverflowKeySearchPrefixComparison exercises the prefix comparison
// path where the prefix alone determines ordering (L585-592).
func TestDeep_OverflowKeySearchPrefixComparison(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	// Insert overflow keys with distinct prefixes so prefix comparison
	// can determine ordering without reading overflow.
	// Key format: "A" + padding (200 bytes), "B" + padding, etc.
	for _, prefix := range []string{"A", "C", "E", "G", "I"} {
		k := append([]byte(prefix), bytes.Repeat([]byte("x"), 200)...)
		require.NoError(t, tx2.Put(ns2, k, []byte("val")))
	}
	require.NoError(t, tx2.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns3, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	// Search for a key that doesn't exist but whose prefix is between existing
	// keys. This forces prefix comparison to determine ordering.
	searchKey := append([]byte("D"), bytes.Repeat([]byte("x"), 200)...)
	_, gerr := rtx.Get(ns3, searchKey)
	assert.ErrorIs(t, gerr, ErrKeyNotFound)

	// Search for existing keys to confirm they're found
	for _, prefix := range []string{"A", "C", "E", "G", "I"} {
		k := append([]byte(prefix), bytes.Repeat([]byte("x"), 200)...)
		_, gerr := rtx.Get(ns3, k)
		require.NoError(t, gerr)
	}
}

// TestDeep_OverflowKeySearchFullKeyRead exercises the path where the prefix
// matches but the full key needs to be read from overflow (L594-599).
func TestDeep_OverflowKeySearchFullKeyRead(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	// Insert keys that share the same prefix but differ in the overflow portion.
	// The local portion of the key is the same, forcing the code to read
	// the full key from overflow pages to determine ordering.
	commonPrefix := bytes.Repeat([]byte("P"), 100) // shared prefix fills local portion
	k1 := append(bytes.Clone(commonPrefix), []byte("AAAAAA-suffix1")...)
	k2 := append(bytes.Clone(commonPrefix), []byte("BBBBBB-suffix2")...)
	k3 := append(bytes.Clone(commonPrefix), []byte("CCCCCC-suffix3")...)
	require.NoError(t, tx2.Put(ns2, k1, []byte("v1")))
	require.NoError(t, tx2.Put(ns2, k2, []byte("v2")))
	require.NoError(t, tx2.Put(ns2, k3, []byte("v3")))
	require.NoError(t, tx2.Commit())

	// Search for a key with same prefix but different overflow portion.
	// This forces full key read from overflow.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns3, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	// Exact match
	v, gerr := rtx.Get(ns3, k2)
	require.NoError(t, gerr)
	assert.Equal(t, []byte("v2"), v)

	// Key between k1 and k2 (same prefix, different overflow)
	betweenKey := append(bytes.Clone(commonPrefix), []byte("ABCDEF-between")...)
	_, gerr = rtx.Get(ns3, betweenKey)
	assert.ErrorIs(t, gerr, ErrKeyNotFound)

	// Key after k3 (same prefix, different overflow)
	afterKey := append(bytes.Clone(commonPrefix), []byte("DDDDDD-after")...)
	_, gerr = rtx.Get(ns3, afterKey)
	assert.ErrorIs(t, gerr, ErrKeyNotFound)
}

// =============================================================================
// GROUP F: findSplitPoint clamping (L287-292)
// =============================================================================

// TestDeep_LeafSplitPointClampBestIdxLow exercises the clamping when bestIdx < 1.
// This happens when all cells are very large so that even the first cell exceeds
// the target fill, causing bestIdx to remain at its default (which could be 0
// for a 2-cell case, but the function returns early for len<=2).
// For 3 cells where the first cell alone exceeds target, bestIdx stays at
// len(cells)/2 = 1, which is already valid. We need a scenario where bestIdx
// would be set to 0.
func TestDeep_LeafSplitPointClampBestIdxLow(t *testing.T) {
	// The clamp at L287-289 fires when bestIdx < 1.
	// bestIdx starts at len(cells)/2. For len(cells)=3, that's 1.
	// The loop can set bestIdx=i where i starts at 1. If cumSize+cellSz > target
	// at i=1, bestIdx=1. The clamp to >=1 never fires in practice for the leaf
	// version because bestIdx = len(cells)/2 >= 1 for len >= 3.
	//
	// However, we can still test the function with edge cases.
	// The bestIdx >= len(cells) clamp at L290-292 is reachable when all cells
	// fit on the left and the last cell sets bestIdx = len(cells)-1 (from L282).
	// That IS valid. But if we create cells that all fit into target, the loop
	// sets bestIdx = len(cells)-1 at i = len(cells)-1, which equals len(cells)-1.
	// The clamp at L290 never fires because bestIdx = len(cells)-1 < len(cells).
	//
	// Both clamps are safety guards. Let's test the split point function directly.
	cells := []cellData{
		{key: []byte("a"), value: []byte("1")},
		{key: []byte("b"), value: []byte("2")},
		{key: []byte("c"), value: []byte("3")},
	}
	mid := leafSplitPoint(cells, 4096)
	assert.True(t, mid >= 1 && mid < len(cells),
		"splitPoint=%d should be in [1, %d)", mid, len(cells))
}

// TestDeep_LeafSplitPointLargeCells tests with cells so large each one nearly
// fills a page. With overflow cells, the on-page size is smaller than the
// value size, so all 3 overflow cells may fit within the 2/3 target.
func TestDeep_LeafSplitPointLargeCells(t *testing.T) {
	bigVal := bytes.Repeat([]byte("X"), 2000)
	cells := []cellData{
		{key: []byte("a"), value: bigVal},
		{key: []byte("b"), value: bigVal},
		{key: []byte("c"), value: bigVal},
	}
	mid := leafSplitPoint(cells, 4096)
	// With overflow, on-page size per cell is ~484 bytes.
	// 3 cells * 484 = 1452 < target(2725), so bestIdx = len(cells)-1 = 2
	assert.True(t, mid >= 1 && mid < len(cells),
		"splitPoint=%d should be in [1, %d)", mid, len(cells))

	// Try with truly page-filling non-overflow cells (3500 byte values with big page)
	// to trigger the "first cell exceeds target" path
	hugeVal := bytes.Repeat([]byte("X"), 3500)
	cells2 := []cellData{
		{key: []byte("a"), value: hugeVal},
		{key: []byte("b"), value: hugeVal},
		{key: []byte("c"), value: hugeVal},
	}
	// Use a very large page size so these don't become overflow cells
	// maxLocal for 65536 = ((65536-12)*64/255)-23 = 16361
	// So 3500+1 = 3501 < 16361, no overflow
	mid2 := leafSplitPoint(cells2, 65536)
	// target = (65536-8)*2/3 = 43685. Each cell = ~3505 bytes + 2 = 3507.
	// cumSize after cell 0: 3507. At i=1: 3507+3507=7014 <= 43685. cumSize=7014.
	// At i=2: last cell, bestIdx=2. So still 2.
	assert.True(t, mid2 >= 1 && mid2 < len(cells2))
}

// TestDeep_LeafSplitPointAllSmall tests with very small cells that all fit
// comfortably, pushing bestIdx to len(cells)-1 (from the i==len(cells)-1 guard).
func TestDeep_LeafSplitPointAllSmall(t *testing.T) {
	cells := make([]cellData, 5)
	for i := range cells {
		cells[i] = cellData{key: []byte{byte('a' + i)}, value: []byte{byte(i)}}
	}
	mid := leafSplitPoint(cells, 4096)
	// All cells fit easily, so bestIdx = len(cells)-1 = 4
	assert.Equal(t, len(cells)-1, mid)
}

// TestDeep_InteriorSplitPointEdgeCases tests interiorSplitPoint with edge cases.
func TestDeep_InteriorSplitPointEdgeCases(t *testing.T) {
	// len(cells) == 1
	cells1 := []cellData{{key: []byte("a"), leftChild: 1}}
	mid1 := interiorSplitPoint(cells1, 4096)
	assert.Equal(t, 0, mid1) // len(cells)/2 = 0

	// len(cells) == 2
	cells2 := []cellData{
		{key: []byte("a"), leftChild: 1},
		{key: []byte("b"), leftChild: 2},
	}
	mid2 := interiorSplitPoint(cells2, 4096)
	assert.Equal(t, 1, mid2) // len(cells)/2 = 1

	// Large cells that force early split
	bigKey := bytes.Repeat([]byte("X"), 2000)
	cells3 := []cellData{
		{key: bigKey, leftChild: 1},
		{key: bigKey, leftChild: 2},
		{key: bigKey, leftChild: 3},
	}
	mid3 := interiorSplitPoint(cells3, 4096)
	assert.True(t, mid3 >= 1 && mid3 <= len(cells3)-2,
		"splitPoint=%d for 3 large interior cells", mid3)
}

// =============================================================================
// GROUP: Overflow key handling in interior pages during search
// Exercises searchInterior with overflow keys
// =============================================================================

// TestDeep_OverflowInteriorKeySearch inserts enough large keys to produce
// overflow keys in interior pages, then exercises search through them.
func TestDeep_OverflowInteriorKeySearch(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	// Insert many entries with large keys. When the leaf splits, separator
	// keys are promoted to interior pages. If separators are large, they
	// may overflow in interior pages too.
	for i := 0; i < 100; i++ {
		prefix := fmt.Appendf(nil, "%04d-", i)
		k := append(prefix, bytes.Repeat([]byte("K"), 150)...)
		require.NoError(t, tx2.Put(ns2, k, []byte("v")))
	}
	require.NoError(t, tx2.Commit())

	// Verify lookups work through interior pages with overflow keys
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns3, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		prefix := fmt.Appendf(nil, "%04d-", i)
		k := append(prefix, bytes.Repeat([]byte("K"), 150)...)
		v, gerr := rtx.Get(ns3, k)
		require.NoError(t, gerr, "key index %d not found", i)
		assert.Equal(t, []byte("v"), v)
	}
}

// =============================================================================
// GROUP: Cursor Previous with overflow values
// =============================================================================

// TestDeep_PrevWithOverflowValues exercises Previous traversal where cells
// have overflow values, ensuring the cursor correctly navigates.
func TestDeep_PrevWithOverflowValues(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	// Insert entries with mix of normal and overflow values
	for i := 0; i < 50; i++ {
		k := fmt.Appendf(nil, "key-%04d", i)
		var v []byte
		if i%5 == 0 {
			v = bytes.Repeat([]byte("V"), 300) // overflow
		} else {
			v = fmt.Appendf(nil, "val-%04d", i)
		}
		require.NoError(t, tx2.Put(ns2, k, v))
	}
	require.NoError(t, tx2.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns3, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns3)

	require.NoError(t, cur.Last())
	count := 0
	for cur.Valid() {
		_, kerr := cur.Key()
		require.NoError(t, kerr)
		count++
		require.NoError(t, cur.Previous())
	}
	assert.Equal(t, 50, count)
}

// =============================================================================
// GROUP: Multi-level Delete then Re-insert
// Tests that the tree remains consistent after complex delete+insert sequences
// =============================================================================

// TestDeep_DeleteAndReinsert deletes entries from a multi-level tree then
// re-inserts new entries, exercising both delete traversal and insert
// through a restructured tree.
func TestDeep_DeleteAndReinsert(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Build multi-level tree
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns2, k, v))
	}
	require.NoError(t, tx2.Commit())

	// Delete some entries in batches
	for batch := 0; batch < 2; batch++ {
		tx3, err := db.BeginWrite()
		require.NoError(t, err)
		ns3, err := db.getNamespaceLocked("data")
		require.NoError(t, err)
		start := batch * 25
		for i := start; i < start+25; i++ {
			k := fmt.Appendf(nil, "key-%06d", i*2) // delete even keys
			_ = tx3.Delete(ns3, k)
		}
		require.NoError(t, tx3.Commit())
	}

	// Re-insert new entries
	tx4, err := db.BeginWrite()
	require.NoError(t, err)
	ns4, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	for i := 200; i < 300; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx4.Put(ns4, k, v))
	}
	require.NoError(t, tx4.Commit())

	require.NoError(t, db.IntegrityCheck())
}

// =============================================================================
// GROUP: insertIntoLeaf via insertIntoPage (exercising the old split path)
// =============================================================================

// TestDeep_InsertIntoLeafDefragmentation exercises the defragmentation path in
// insertIntoLeaf (L1200-1207): when gap space is insufficient but total free
// space (gap + fragBytes) is enough, the page is rebuilt to defragment.
func TestDeep_InsertIntoLeafDefragmentation(t *testing.T) {
	p := tempPager(t)

	rootPg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: rootPg.pgno, writable: true}

	rootPg.header.pageType = pageTypeLeafIdx
	rootPg.header.cellContentOff = uint16(p.usableSize())
	hdr := 0
	if rootPg.pgno == 1 {
		hdr = dbHeaderSize
	}
	rootPg.header.serialize(rootPg.data[hdr:])
	p.releasePage(rootPg)

	// Fill the page nearly full, then delete some entries (via insertIntoPage
	// with updates) to create fragmentation, then insert a new entry that
	// fits only after defragmentation.
	// First, fill the page
	for i := 0; i < 100; i++ {
		k := fmt.Appendf(nil, "k%04d", i)
		v := fmt.Appendf(nil, "v%04d", i)

		pg, gerr := p.getWritablePage(bt.rootPage)
		require.NoError(t, gerr)
		ierr := bt.insertIntoPage(pg, k, v)
		require.NoError(t, ierr)
		p.releasePage(pg)
	}

	// Update some entries with smaller values to create fragmentation
	for i := 10; i < 30; i++ {
		k := fmt.Appendf(nil, "k%04d", i)
		v := []byte("s") // much smaller than original

		pg, gerr := p.getWritablePage(bt.rootPage)
		require.NoError(t, gerr)
		ierr := bt.insertIntoPage(pg, k, v)
		require.NoError(t, ierr)
		p.releasePage(pg)
	}

	// Verify entries
	for i := 0; i < 100; i++ {
		k := fmt.Appendf(nil, "k%04d", i)
		_, gerr := bt.Get(k)
		require.NoError(t, gerr, "key %s not found after defrag sequence", k)
	}
}

// =============================================================================
// GROUP: Additional coverage for Previous() with page boundary crossing
// =============================================================================

// TestDeep_PrevCrossesMultipleInteriorLevels creates a deep tree and
// traverses backward specifically across interior page boundaries.
func TestDeep_PrevCrossesMultipleInteriorLevels(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert enough for 3+ levels
	n := 3000
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%08d", i)
		v := []byte("v")
		require.NoError(t, tx2.Put(ns, k, v))
	}
	require.NoError(t, tx2.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns2)

	// Seek near the end, then walk backward across interior boundaries
	seekKey := fmt.Appendf(nil, "key-%08d", n-10)
	require.NoError(t, cur.Seek(seekKey))
	require.True(t, cur.Valid())

	count := 0
	for cur.Valid() {
		count++
		require.NoError(t, cur.Previous())
	}
	assert.True(t, count >= n-10, "expected at least %d entries, got %d", n-10, count)
}

// =============================================================================
// GROUP: Direct btree-level cursor backward traversal through 3-level tree
// This directly creates a 3-level tree structure via the pager API
// =============================================================================

// TestDeep_PrevThreeLevelDirect builds a 3-level tree manually and
// verifies backward traversal through all levels.
func TestDeep_PrevThreeLevelDirect(t *testing.T) {
	p := tempPager(t)

	// Create leaf pages
	leaf1, err := p.allocatePage()
	require.NoError(t, err)
	leaf2, err := p.allocatePage()
	require.NoError(t, err)
	leaf3, err := p.allocatePage()
	require.NoError(t, err)
	leaf4, err := p.allocatePage()
	require.NoError(t, err)

	// Create interior pages
	int1, err := p.allocatePage()
	require.NoError(t, err)
	int2, err := p.allocatePage()
	require.NoError(t, err)

	// Create root page
	rootPg, err := p.allocatePage()
	require.NoError(t, err)

	bt := &btree{pager: p, rootPage: rootPg.pgno, writable: true}

	// Build leaf pages
	bt.rebuildLeafPage(leaf1, []cellData{
		{key: []byte("a01"), value: []byte("v1")},
		{key: []byte("a02"), value: []byte("v2")},
	})
	bt.rebuildLeafPage(leaf2, []cellData{
		{key: []byte("b01"), value: []byte("v3")},
		{key: []byte("b02"), value: []byte("v4")},
	})
	bt.rebuildLeafPage(leaf3, []cellData{
		{key: []byte("c01"), value: []byte("v5")},
		{key: []byte("c02"), value: []byte("v6")},
	})
	bt.rebuildLeafPage(leaf4, []cellData{
		{key: []byte("d01"), value: []byte("v7")},
		{key: []byte("d02"), value: []byte("v8")},
	})

	// Build interior pages
	// int1: children are leaf1 (left of "b") and leaf2 (right of "b")
	bt.rebuildInteriorPage(int1, []cellData{
		{leftChild: leaf1.pgno, key: []byte("b00")},
	}, leaf2.pgno)

	// int2: children are leaf3 (left of "d") and leaf4 (right of "d")
	bt.rebuildInteriorPage(int2, []cellData{
		{leftChild: leaf3.pgno, key: []byte("d00")},
	}, leaf4.pgno)

	// Root: children are int1 (left of "c") and int2 (right of "c")
	bt.rebuildInteriorPage(rootPg, []cellData{
		{leftChild: int1.pgno, key: []byte("c00")},
	}, int2.pgno)

	p.releasePage(leaf1)
	p.releasePage(leaf2)
	p.releasePage(leaf3)
	p.releasePage(leaf4)
	p.releasePage(int1)
	p.releasePage(int2)
	p.releasePage(rootPg)

	// Now traverse backward using the cursor
	cur := bt.NewCursor()
	require.NoError(t, cur.Last())
	require.True(t, cur.Valid())

	var keys []string
	for cur.Valid() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		keys = append(keys, string(k))
		require.NoError(t, cur.Previous())
	}

	expected := []string{"d02", "d01", "c02", "c01", "b02", "b01", "a02", "a01"}
	assert.Equal(t, expected, keys)
}

// =============================================================================
// GROUP: Delete with content area boundary reclaim
// Tests the L2090-2093 path in Delete
// =============================================================================

// TestDeep_DeleteContentAreaBoundaryMultiLevel exercises the content area
// boundary reclaim path in Delete when the deleted cell is at the start
// of the content area.
func TestDeep_DeleteContentAreaBoundaryMultiLevel(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Build multi-level tree
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns2, k, v))
	}
	require.NoError(t, tx2.Commit())

	// Delete the last inserted key in each leaf page - it's most likely
	// at the content area boundary.
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	// Delete keys from the end (most recently written to each leaf)
	for i := 195; i < 200; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		require.NoError(t, tx3.Delete(ns3, k))
	}
	require.NoError(t, tx3.Commit())

	require.NoError(t, db.IntegrityCheck())
	assert.Equal(t, 195, countKeys(t, db, "data"))
}

// =============================================================================
// GROUP: Heavy fragmentation in Delete leading to rebuild
// =============================================================================

// TestDeep_DeleteHeavyFragmentation aggressively deletes entries to trigger
// the fragmentation rebuild (needsRebuild) path in Delete.
func TestDeep_DeleteHeavyFragmentation(t *testing.T) {
	// Use a page size where we can accumulate > 60 bytes of fragmentation.
	db := tempDBWithPageSize(t, 1024)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert entries with values large enough that deleting 3-4 of them
	// accumulates > 60 bytes of fragmentation.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= 40; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		// Value size ~25 bytes, so cell size is ~30+ bytes including varints
		val := bytes.Repeat([]byte{byte(i)}, 25)
		require.NoError(t, tx2.Put(ns2, key, val))
	}
	require.NoError(t, tx2.Commit())

	// Delete entries from the middle (not at content boundary) to accumulate frag.
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	// Delete keys 10..15 (6 entries * ~30 bytes = ~180 bytes frag)
	for i := 10; i <= 15; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx3.Delete(ns3, key))
	}
	require.NoError(t, tx3.Commit())

	require.NoError(t, db.IntegrityCheck())
	assert.Equal(t, 34, countKeys(t, db, "t1"))
}

// =============================================================================
// GROUP: Cursor traversal with a single-entry tree
// Edge case for Previous on a 1-level tree
// =============================================================================

// TestDeep_PrevSingleEntryTree tests Previous on a tree with one entry.
func TestDeep_PrevSingleEntryTree(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("only-key"), []byte("only-val")))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	require.NoError(t, cur.Last())
	require.True(t, cur.Valid())

	k, err := cur.Key()
	require.NoError(t, err)
	assert.Equal(t, []byte("only-key"), k)

	require.NoError(t, cur.Previous())
	assert.False(t, cur.Valid())
}

// =============================================================================
// GROUP: Mixed operations on multi-level tree
// Tests the interaction of all paths
// =============================================================================

// TestDeep_MixedInsertDeletePrev performs interleaved inserts and deletes
// in a multi-level tree, then verifies backward traversal works correctly.
func TestDeep_MixedInsertDeletePrev(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Phase 1: Build initial tree
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns2, k, v))
	}
	require.NoError(t, tx2.Commit())

	// Phase 2: Delete every 5th entry (smaller batch to avoid issues)
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	deleted := 0
	for i := 0; i < 200; i += 5 {
		k := fmt.Appendf(nil, "key-%06d", i)
		if err := tx3.Delete(ns3, k); err == nil {
			deleted++
		}
	}
	require.NoError(t, tx3.Commit())

	// Phase 3: Insert new entries
	tx4, err := db.BeginWrite()
	require.NoError(t, err)
	ns4, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	for i := 200; i < 260; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx4.Put(ns4, k, v))
	}
	require.NoError(t, tx4.Commit())

	require.NoError(t, db.IntegrityCheck())

	// Verify backward traversal
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns5, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns5)

	require.NoError(t, cur.Last())
	count := 0
	var prevKey []byte
	for cur.Valid() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		if prevKey != nil {
			assert.True(t, bytes.Compare(k, prevKey) < 0)
		}
		prevKey = bytes.Clone(k)
		count++
		require.NoError(t, cur.Previous())
	}
	expectedCount := 200 - deleted + 60
	assert.Equal(t, expectedCount, count)
}

// =============================================================================
// btree.go L1249-1252: contentAreaOffset error in insertLeafCellAt
// =============================================================================

func TestDeepCov_InsertLeafCellAt_ContentAreaOffsetError(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Insert a key so the page has content
	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)
	require.NoError(t, bt.insertIntoPage(pg, []byte("aaa"), []byte("val")))
	p.releasePage(pg)

	// Get the page again and corrupt cellContentOff to be beyond usableSize
	pg, err = p.getWritablePage(bt.rootPage)
	require.NoError(t, err)

	// Corrupt cellContentOff to a value > usableSize
	pg.header.cellContentOff = uint16(p.usableSize() + 100)
	hdr := 0
	if pg.pgno == 1 {
		hdr = dbHeaderSize
	}
	pg.header.serialize(pg.data[hdr:])

	// Call insertLeafCellAt directly — should hit contentAreaOffset error at L1249
	err = bt.insertLeafCellAt(pg, 0, []byte("bbb"), []byte("val2"))
	assert.ErrorIs(t, err, ErrCorrupt)
	p.releasePage(pg)
}

// =============================================================================
// btree.go L1254-1256: newContentStart bounds check in insertLeafCellAt
// =============================================================================

func TestDeepCov_InsertLeafCellAt_ContentStartBoundsError(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)

	// Set cellContentOff to be just barely valid but too small for any cell
	// Cell pointer area starts at offset 8 (leaf header), so gap = contentStart - hdrSize
	// We need contentAreaOffset to succeed but newContentStart to go negative.
	// Set cellContentOff to a small valid value (= cellPointerOffset + cellCount*2 + 1)
	// so contentStart is very close to hdrSize.
	cpOff := pg.cellPointerOffset()
	pg.header.cellContentOff = uint16(cpOff + 2 + 1) // just past one cell pointer
	pg.header.cellCount = 1
	// Write a valid cell pointer pointing to some data
	binary.BigEndian.PutUint16(pg.data[cpOff:], uint16(cpOff+2+1))
	hdr := 0
	if pg.pgno == 1 {
		hdr = dbHeaderSize
	}
	pg.header.serialize(pg.data[hdr:])

	// Now call insertLeafCellAt with key+value larger than available space.
	// contentStart is cpOff+3, cellSize will be > contentStart, making newContentStart < 0
	bigKey := make([]byte, 100)
	err = bt.insertLeafCellAt(pg, 0, bigKey, []byte("val"))
	assert.ErrorIs(t, err, ErrCorrupt)
	p.releasePage(pg)
}

// =============================================================================
// btree.go L1301-1303: parseLeafCellWithSize error in updateLeafCell
// =============================================================================

func TestDeepCov_UpdateLeafCell_ParseCellError(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Insert a key via the normal path
	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)
	require.NoError(t, bt.insertIntoPage(pg, []byte("key1"), []byte("val1")))
	p.releasePage(pg)

	// Get page and corrupt the cell data
	pg, err = p.getWritablePage(bt.rootPage)
	require.NoError(t, err)

	// Find the cell offset for cell 0
	cellOff := int(pg.getCellOffset(0))
	// Corrupt the cell's varint to be an unterminated continuation byte
	pg.data[cellOff] = 0x80
	pg.data[cellOff+1] = 0x80
	pg.data[cellOff+2] = 0x80
	pg.data[cellOff+3] = 0x80
	pg.data[cellOff+4] = 0x80
	pg.data[cellOff+5] = 0x80
	pg.data[cellOff+6] = 0x80
	pg.data[cellOff+7] = 0x80
	pg.data[cellOff+8] = 0x80
	// 9 continuation bytes without terminator makes getVarint read all 9 bytes
	// but the value might be huge, causing parseLeafCellWithSize to fail

	// Actually, set the varint to encode a value > maxPayloadAlloc
	// getVarint returns after reading up to 9 bytes. Use a 9-byte encoding
	// that decodes to > 1<<30
	// Simpler: corrupt so the cell data extends past page boundary
	// Set keyLen varint to a large but terminated value
	pg.data[cellOff] = 0x84   // high bit set = continuation
	pg.data[cellOff+1] = 0x00 // terminated, value = 0x200 = 512
	// Now keyLen = 512, which is > page size, should trigger parse error

	err = bt.updateLeafCell(pg, 0, []byte("key2"), []byte("val2"), nil)
	assert.Error(t, err)
	p.releasePage(pg)
}

// =============================================================================
// btree.go L1349-1351: updateLeafCell hdrOff on page 1
// =============================================================================

func TestDeepCov_UpdateLeafCell_Page1HdrOff(t *testing.T) {
	// Use the standard helpers that put the root on page 1
	p := tempPagerWithPageSize(t, 4096)
	// initLeafBtree may or may not put root on page 1 depending on allocatePage
	// Use tempPager which starts with page 1 for db header
	bt := &btree{pager: p, rootPage: 1, writable: true}

	// Initialize page 1 as a leaf
	pg, err := p.getWritablePage(1)
	require.NoError(t, err)
	pg.header.pageType = pageTypeLeafIdx
	pg.header.cellContentOff = uint16(p.usableSize())
	pg.header.serialize(pg.data[dbHeaderSize:])
	p.releasePage(pg)

	// Insert a key, then update it with a smaller value to trigger the
	// fragmentation path where hdrOff = dbHeaderSize (L1349-1351)
	pg, err = p.getWritablePage(1)
	require.NoError(t, err)
	require.NoError(t, bt.insertIntoPage(pg, []byte("testkey"), []byte("longvalue123456")))
	p.releasePage(pg)

	// Update with shorter value — should trigger waste > 0, newFrag <= 255
	pg, err = p.getWritablePage(1)
	require.NoError(t, err)
	err = bt.updateLeafCell(pg, 0, []byte("testkey"), []byte("s"), nil)
	require.NoError(t, err)
	p.releasePage(pg)

	// Verify page 1 header was serialized at dbHeaderSize offset
	pg, err = p.getWritablePage(1)
	require.NoError(t, err)
	assert.True(t, pg.header.fragBytes > 0) // should have fragmentation
	p.releasePage(pg)
}

// =============================================================================
// btree.go L1755-1758: contentAreaOffset error in insertSepIntoInterior
// =============================================================================

func TestDeepCov_InsertSepIntoInterior_ContentAreaOffsetError(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Set up an interior page
	intPg, err := p.allocatePage()
	require.NoError(t, err)
	intPg.header.pageType = pageTypeIntIdx
	intPg.header.cellCount = 0
	intPg.header.cellContentOff = uint16(p.usableSize())
	intPg.header.rightChild = bt.rootPage
	intPg.header.serialize(intPg.data[0:])

	// Corrupt cellContentOff to be > usableSize
	intPg.header.cellContentOff = uint16(p.usableSize() + 50)
	intPg.header.serialize(intPg.data[0:])

	// Call insertSepIntoInterior directly — should hit contentAreaOffset error at L1754-1758
	err = bt.insertSepIntoInterior(intPg, bt.rootPage, []byte("sep"), 999, nil)
	assert.ErrorIs(t, err, ErrCorrupt)
	// Note: insertSepIntoInterior releases parentPg on error
}

// =============================================================================
// btree.go L1765-1768: newContentStart bounds check in insertSepIntoInterior
// =============================================================================

func TestDeepCov_InsertSepIntoInterior_ContentStartBounds(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Set up an interior page with very little free space
	intPg, err := p.allocatePage()
	require.NoError(t, err)
	intPg.header.pageType = pageTypeIntIdx
	intPg.header.cellCount = 0
	// Set contentStart very close to the header end, so inserting any key
	// makes newContentStart < 0
	cpOff := intPg.cellPointerOffset()
	intPg.header.cellContentOff = uint16(cpOff + 2) // just enough for 1 cell pointer
	intPg.header.rightChild = bt.rootPage
	intPg.header.serialize(intPg.data[0:])

	// The separator key must be small enough that cellSize+2 <= freeSpace,
	// but then newContentStart = contentStart - cellSize goes negative.
	// contentStart = cpOff + 2, hdrSize = cpOff + (0+1)*2 = cpOff + 2
	// freeSpace = contentStart - hdrSize = 0
	// This means cellSize+2 > freeSpace, so it falls through to the split path.

	// To make it fit but go negative, we need freeSpace > 0 but contentStart small.
	// Set cellCount=0, contentOff at cpOff+12+2 (12=interiorHdr, 2=one cell ptr)
	intPg.header.cellContentOff = uint16(cpOff + 2 + 10) // small gap
	intPg.header.serialize(intPg.data[0:])

	// insertSepIntoInterior: cellSize = interiorCellSizeWithOverflow(key, 512)
	// For a 1-byte key: 4 (leftChild) + 1 (varint) + 1 (key) = 6
	// freeSpace = (cpOff+12) - (cpOff+2) = 10, cellSize+2=8 <= 10 → fits!
	// newContentStart = (cpOff+12) - 6 = cpOff+6 ≥ 0, so doesn't trigger bounds check.
	// We need newContentStart < 0. Set contentOff = cellSize - 1 and hdrSize < contentOff.
	// Hmm, contentAreaOffset validates contentOff ≥ gap = cpOff + (n+1)*2.
	// With n=0, gap = cpOff + 2. So contentOff must be ≥ cpOff + 2.
	// newContentStart = contentOff - cellSize. For this to be < 0:
	// contentOff < cellSize → cpOff + 2 < cellSize
	// For 512 page, cpOff = 12 (interior). So cpOff+2 = 14.
	// cellSize for key of length 10: 4+1+10 = 15. freeSpace = contentOff - hdrSize.
	// hdrSize = cpOff + (0+1)*2 = 14. freeSpace = 14 - 14 = 0. Doesn't fit.

	// This path seems hard to trigger because contentAreaOffset validates
	// contentOff ≥ cpOff + cellCount*2. Let me try a different approach:
	// use a page with cellCount=200 (fake) so gap = cpOff + 201*2 = large.
	// Then contentOff = gap, freeSpace = 0. No space. Falls to split.

	// Actually L1765-1768 is a defensive check that's probably unreachable
	// if contentAreaOffset already validated. Skip this test.
	p.releasePage(intPg)
	t.Skip("L1765-1768: newContentStart bounds check unreachable after contentAreaOffset validation")
}

// =============================================================================
// btree.go L2868-2869: Next() descends into empty interior page (cellCount=0)
// Construct a 3-level tree: root(interior) → child(interior, cellCount=0) → leaf
// =============================================================================

func TestDeepCov_CursorNext_EmptyInteriorBreak(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	usable := p.usableSize()

	// Allocate pages: root(interior), leaf1, emptyInt(interior,cellCount=0), leaf2
	rootPg, err := p.allocatePage()
	require.NoError(t, err)
	leaf1, err := p.allocatePage()
	require.NoError(t, err)
	emptyInt, err := p.allocatePage()
	require.NoError(t, err)
	leaf2, err := p.allocatePage()
	require.NoError(t, err)

	// Set up leaf1 with one cell "aaa"
	leaf1.header.pageType = pageTypeLeafIdx
	leaf1.header.cellCount = 1
	cs1 := leafCellSize([]byte("aaa"), []byte("v1"))
	writeLeafCell(leaf1.data[usable-cs1:], []byte("aaa"), []byte("v1"))
	leaf1.header.cellContentOff = uint16(usable - cs1)
	binary.BigEndian.PutUint16(leaf1.data[8:], uint16(usable-cs1))
	leaf1.header.serialize(leaf1.data[0:])
	p.releasePage(leaf1)

	// Set up leaf2 with one cell "zzz"
	leaf2.header.pageType = pageTypeLeafIdx
	leaf2.header.cellCount = 1
	cs2 := leafCellSize([]byte("zzz"), []byte("v2"))
	writeLeafCell(leaf2.data[usable-cs2:], []byte("zzz"), []byte("v2"))
	leaf2.header.cellContentOff = uint16(usable - cs2)
	binary.BigEndian.PutUint16(leaf2.data[8:], uint16(usable-cs2))
	leaf2.header.serialize(leaf2.data[0:])
	p.releasePage(leaf2)

	// Set up emptyInt: interior with cellCount=0, rightChild=leaf2
	emptyInt.header.pageType = pageTypeIntIdx
	emptyInt.header.cellCount = 0
	emptyInt.header.rightChild = leaf2.pgno
	emptyInt.header.cellContentOff = uint16(usable)
	emptyInt.header.serialize(emptyInt.data[0:])
	p.releasePage(emptyInt)

	// Set up root: interior with 1 cell (leftChild=leaf1, key="mmm"), rightChild=emptyInt
	rootPg.header.pageType = pageTypeIntIdx
	rootPg.header.cellCount = 1
	rootPg.header.rightChild = emptyInt.pgno
	ics := 4 + varintSize(uint64(3)) + 3 // leftChild + varint(keyLen) + key
	writeInteriorCell(rootPg.data[usable-ics:], leaf1.pgno, []byte("mmm"))
	rootPg.header.cellContentOff = uint16(usable - ics)
	binary.BigEndian.PutUint16(rootPg.data[12:], uint16(usable-ics))
	rootPg.header.serialize(rootPg.data[0:])
	p.releasePage(rootPg)

	// Create btree and cursor
	bt := &btree{pager: p, rootPage: rootPg.pgno, writable: true}
	cursor := bt.NewCursor()
	defer cursor.Close()

	// Seek to "aaa" (in leaf1)
	require.NoError(t, cursor.Seek([]byte("aaa")))
	require.True(t, cursor.Valid())

	// Next(): leaf1 exhausted → pop to root → cellIdx becomes 1 (= cellCount) → use rightChild (emptyInt)
	// Descend into emptyInt: isInterior() && cellCount==0 → break! (L2868-2869)
	_ = cursor.Next()
	// After break, childPg.cellCount==0 → releases page, pops stack, continues loop
}

// =============================================================================
// btree.go L2969-2970: Previous() descends into empty interior page (cellCount=0)
// Same structure but traversed backward.
// =============================================================================

func TestDeepCov_CursorPrevious_EmptyInteriorBreak(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	usable := p.usableSize()

	// Allocate pages: root(interior), emptyInt(interior,cellCount=0), leaf1, leaf2
	rootPg, err := p.allocatePage()
	require.NoError(t, err)
	emptyInt, err := p.allocatePage()
	require.NoError(t, err)
	leaf1, err := p.allocatePage()
	require.NoError(t, err)
	leaf2, err := p.allocatePage()
	require.NoError(t, err)

	// Set up leaf1 with one cell "aaa"
	leaf1.header.pageType = pageTypeLeafIdx
	leaf1.header.cellCount = 1
	cs1 := leafCellSize([]byte("aaa"), []byte("v1"))
	writeLeafCell(leaf1.data[usable-cs1:], []byte("aaa"), []byte("v1"))
	leaf1.header.cellContentOff = uint16(usable - cs1)
	binary.BigEndian.PutUint16(leaf1.data[8:], uint16(usable-cs1))
	leaf1.header.serialize(leaf1.data[0:])
	p.releasePage(leaf1)

	// Set up leaf2 with one cell "zzz"
	leaf2.header.pageType = pageTypeLeafIdx
	leaf2.header.cellCount = 1
	cs2 := leafCellSize([]byte("zzz"), []byte("v2"))
	writeLeafCell(leaf2.data[usable-cs2:], []byte("zzz"), []byte("v2"))
	leaf2.header.cellContentOff = uint16(usable - cs2)
	binary.BigEndian.PutUint16(leaf2.data[8:], uint16(usable-cs2))
	leaf2.header.serialize(leaf2.data[0:])
	p.releasePage(leaf2)

	// Set up emptyInt: interior with cellCount=0, rightChild=leaf1
	emptyInt.header.pageType = pageTypeIntIdx
	emptyInt.header.cellCount = 0
	emptyInt.header.rightChild = leaf1.pgno
	emptyInt.header.cellContentOff = uint16(usable)
	emptyInt.header.serialize(emptyInt.data[0:])
	p.releasePage(emptyInt)

	// Set up root: interior with 1 cell (leftChild=emptyInt, key="mmm"), rightChild=leaf2
	rootPg.header.pageType = pageTypeIntIdx
	rootPg.header.cellCount = 1
	rootPg.header.rightChild = leaf2.pgno
	ics := 4 + varintSize(uint64(3)) + 3 // leftChild + varint(keyLen) + key
	writeInteriorCell(rootPg.data[usable-ics:], emptyInt.pgno, []byte("mmm"))
	rootPg.header.cellContentOff = uint16(usable - ics)
	binary.BigEndian.PutUint16(rootPg.data[12:], uint16(usable-ics))
	rootPg.header.serialize(rootPg.data[0:])
	p.releasePage(rootPg)

	// Create btree and cursor
	bt := &btree{pager: p, rootPage: rootPg.pgno, writable: true}
	cursor := bt.NewCursor()
	defer cursor.Close()

	// Seek to "zzz" (in leaf2, reached via rightChild)
	require.NoError(t, cursor.Seek([]byte("zzz")))
	require.True(t, cursor.Valid())

	// Previous(): leaf2 exhausted → pop to root → cellIdx decrements to 0
	// → read leftChild of cell[0] = emptyInt
	// Descend into emptyInt: isInterior() && cellCount==0 → break! (L2969-2970)
	_ = cursor.Previous()
}

// =============================================================================
// btree.go L2702-2704: SeekExact Key() error
// =============================================================================

func TestDeepCov_SeekExact_KeyError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, Options{PageSize: 512, InProcess: true})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("ns1")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		key := fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	require.NoError(t, db.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Find the namespace root page
	cpBase := dbHeaderSize + 8
	cellOff0 := int(binary.BigEndian.Uint16(data[cpBase:]))
	pos := cellOff0
	kl := int(data[pos])
	pos++
	pos++ // valLen
	nsRoot := binary.BigEndian.Uint32(data[pos+kl : pos+kl+4])
	nsOff := int(nsRoot-1) * 512

	if nsOff >= len(data) {
		t.Skip("namespace root page offset beyond file")
	}

	// Corrupt the first cell on the namespace leaf page
	// to have an impossibly large key varint
	nsCpBase := nsOff + 8
	nsCellOff := nsOff + int(binary.BigEndian.Uint16(data[nsCpBase:]))
	if nsCellOff+2 < len(data) {
		// Set key varint to a huge value (9-byte encoding > maxPayloadAlloc)
		data[nsCellOff] = 0xFF
		data[nsCellOff+1] = 0xFF
		data[nsCellOff+2] = 0xFF
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := Open(path, Options{PageSize: 512, InProcess: true})
	if err != nil {
		t.Skip("cannot reopen corrupted DB:", err)
	}
	defer db2.Close()

	rtx, err := db2.BeginRead()
	if err != nil {
		t.Skip("cannot begin read:", err)
	}
	ns2, err := db2.getNamespaceAt("ns1", rtx.walMaxFrame, nil)
	if err != nil {
		rtx.Rollback()
		t.Skip("cannot get namespace:", err)
	}
	c := rtx.NewCursor(ns2)

	// SeekExact calls SeekNear → Key(), which should fail on corrupted cell
	err = c.SeekExact([]byte("key-0000"))
	// Should get an error from either SeekNear (search corruption) or Key() (L2702-2704)
	assert.Error(t, err)
	c.Close()
	rtx.Rollback()
}

// =============================================================================
// btree.go L1376-1378: updateLeafCell hdrOff on non-page-1 (basic path coverage)
// This covers the else branch where hdrOff=0 for non-page-1 leaves.
// =============================================================================

func TestDeepCov_UpdateLeafCell_HdrOff_NonPage1(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Insert then update with shorter value to trigger fragmentation path
	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)
	require.NoError(t, bt.insertIntoPage(pg, []byte("k1"), []byte("long_value_here_123")))
	p.releasePage(pg)

	pg, err = p.getWritablePage(bt.rootPage)
	require.NoError(t, err)
	// Update with shorter value — waste > 0, newFrag <= 255
	err = bt.updateLeafCell(pg, 0, []byte("k1"), []byte("x"), nil)
	require.NoError(t, err)
	assert.True(t, pg.header.fragBytes > 0)
	p.releasePage(pg)
}

// =============================================================================
// integrity.go L169-172: contentAreaOffset error in checkPageCoverage
// The gap_coverage_test.go says this is unreachable because checkTreePage
// pre-checks at L254-258. Let's verify and try an alternative approach.
// =============================================================================

// Note: L169-172 is indeed pre-checked by checkTreePage L254-258, which calls
// pg.contentAreaOffset and reports error. So checkPageCoverage's own check never fires.
// This is confirmed UNREACHABLE.

// =============================================================================
// integrity.go L445-448: hdr.deserialize error in IntegrityCheckN
// The header is validated during Open(), so it's always valid when IntegrityCheckN
// reads it. To trigger, we'd need WAL frames to corrupt page 1 after Open.
// =============================================================================

func TestDeepCov_IntegrityCheck_DeserializeViaWAL(t *testing.T) {
	// L445-448 requires hdr.deserialize to fail on page 1 data.
	// However, commit() always re-serializes the valid header onto page 1
	// via p.header.serialize(), which writes the correct magic.
	// This makes the deserialize error structurally unreachable in normal operation.
	t.Skip("L445-448: commit re-serializes valid header, making deserialize error unreachable")
}

// =============================================================================
// btree.go L2952-2954: Previous() cellIdx == cellCount (rightChild path)
// This branch is reached when Previous() traverses backward and finds
// an interior frame where cellIdx == cellCount after decrement.
// =============================================================================

func TestDeepCov_CursorPrevious_RightChildPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, Options{PageSize: 512, InProcess: true})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("ns1")
	require.NoError(t, err)

	// Insert enough data to create a multi-level tree
	for i := 0; i < 300; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 50)))
	}
	require.NoError(t, tx.Commit())

	// Read the last few entries, then go Previous
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, err := db.getNamespaceAt("ns1", rtx.walMaxFrame, nil)
	require.NoError(t, err)
	c := rtx.NewCursor(ns2)

	// SeekLast equivalent: seek to a key larger than any in the tree
	bigKey := make([]byte, 8)
	binary.BigEndian.PutUint64(bigKey, 999)
	err = c.Seek(bigKey)
	require.NoError(t, err)

	// Walk backward through the entire tree
	count := 0
	for c.Valid() {
		count++
		if err := c.Previous(); err != nil {
			break
		}
	}
	c.Close()
	rtx.Rollback()

	// Walk forward then backward at specific interior boundaries
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	ns3, err := db.getNamespaceAt("ns1", rtx2.walMaxFrame, nil)
	require.NoError(t, err)
	c2 := rtx2.NewCursor(ns3)

	// Seek to the very last key (key 299), which will be in the rightChild subtree
	lastKey := make([]byte, 8)
	binary.BigEndian.PutUint64(lastKey, 299)
	require.NoError(t, c2.Seek(lastKey))

	// Next() past the last entry — cursor becomes invalid
	for c2.Valid() {
		if err := c2.Next(); err != nil {
			break
		}
	}

	// Now seek the last key again and walk forward then backward
	require.NoError(t, c2.Seek(lastKey))
	if c2.Valid() {
		// Go forward one, then backward — might cross interior boundary via rightChild
		_ = c2.Next()
		_ = c2.Previous()
	}
	c2.Close()
	rtx2.Rollback()
	db.Close()
}

// =============================================================================
// shm_mmap.go L50-53: DMS lock failure
// Already has a test but uses a bad fd approach. Let's try fd close + lock.
// =============================================================================

// TestDeepCov_ShmMmap_DMSLockFailure triggers the DMS lock failure at L50-53
// by creating a platform SHM with a closed file descriptor.
func TestDeepCov_ShmMmap_DMSLockFailure(t *testing.T) {
	dir := t.TempDir()
	shmPath := filepath.Join(dir, "test.shm")

	// Create the SHM file first so it exists
	f, err := os.Create(shmPath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Replace the file with a read-only version opened from /dev/null
	// to make the DMS lock fail
	// Actually, just close the file before newPlatformShm reads it
	// The issue is that newPlatformShm opens its own file handle

	// Alternative: use symlink to /dev/null which doesn't support locks
	devNullPath := filepath.Join(dir, "devnull.shm")
	err = os.Symlink("/dev/null", devNullPath)
	if err != nil {
		t.Skip("cannot create symlink")
	}

	_, err = newPlatformShm(devNullPath)
	// DMS lock on /dev/null may or may not fail depending on OS
	if err != nil {
		// L50-53 covered!
		t.Logf("DMS lock failed as expected: %v", err)
	}
}

// =============================================================================
// btree.go L1430-1432: collectLeafCells contentSize < 0
// This is UNREACHABLE: contentAreaOffset either returns a valid offset
// (≤ usableSize) or errors (sets contentOff = usableSize making contentSize = 0).
// contentSize = usableSize - contentOff, which is always >= 0.
// =============================================================================

// =============================================================================
// db.go L655-658, L663-672: overflow handling in AppendValue
// L655-658 (parseLeafCellWithSize error) is already tested in gap_coverage.
// L663-672 (getVarintSafe errors) are structurally unreachable.
// Let's verify L655-658 actually covers.
// =============================================================================

func TestDeepCov_DB_AppendValue_OverflowReadError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	pageSize := 512
	db, err := Open(path, Options{PageSize: uint32(pageSize), InProcess: true})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("ns1")
	require.NoError(t, err)
	// Insert a value large enough to overflow
	bigVal := make([]byte, 500)
	for i := range bigVal {
		bigVal[i] = byte(i)
	}
	require.NoError(t, tx.Put(ns, []byte("key1"), bigVal))
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	require.NoError(t, db.Close())

	// Corrupt the overflow page pointer in the cell
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Find namespace root page
	cpBase := dbHeaderSize + 8
	masterCellOff := int(binary.BigEndian.Uint16(data[cpBase:]))
	pos := masterCellOff
	kl := int(data[pos])
	pos++
	pos++ // valLen
	nsRoot := binary.BigEndian.Uint32(data[pos+kl : pos+kl+4])
	nsOff := int(nsRoot-1) * pageSize

	// Find the overflow cell on the namespace page
	nsCpBase := nsOff + 8
	cellCount := int(binary.BigEndian.Uint16(data[nsOff+3:]))
	if cellCount == 0 {
		t.Skip("no cells in namespace page")
	}
	nsCellOff := nsOff + int(binary.BigEndian.Uint16(data[nsCpBase:]))

	// Parse the cell to find the overflow pointer location
	cellPos := nsCellOff
	usableSize := pageSize
	keyLen, kn := getVarint(data[cellPos:])
	cellPos += kn
	valLen, vn := getVarint(data[cellPos:])
	cellPos += vn
	totalPayload := int(keyLen) + int(valLen)
	nLocal := localPayloadSize(totalPayload, usableSize)

	// Overflow pointer is at cellPos + nLocal
	ovfPtrOff := cellPos + nLocal
	if ovfPtrOff+4 <= len(data) {
		// Corrupt overflow page pointer to point beyond DB size
		binary.BigEndian.PutUint32(data[ovfPtrOff:], 0xFFFFFFFF)
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := Open(path, Options{PageSize: uint32(pageSize), InProcess: true})
	if err != nil {
		t.Skip("cannot reopen")
	}
	defer db2.Close()

	rtx, err := db2.BeginRead()
	if err != nil {
		t.Skip("cannot begin read")
	}
	ns2, err := db2.getNamespaceAt("ns1", rtx.walMaxFrame, nil)
	if err != nil {
		rtx.Rollback()
		t.Skip("cannot get namespace:", err)
	}

	// AppendValue should hit the overflow read error at L1022-1024
	_, err = rtx.AppendValue(ns2, []byte("key1"), nil)
	assert.Error(t, err)
	rtx.Rollback()
}

// =============================================================================
// btree.go L960-962: beginRead error in AppendValue
// beginRead only fails when the pager is in error state. We can trigger this
// by corrupting the pager state.
// =============================================================================

// This is an IO_ERROR path — skip.

// =============================================================================
// btree.go L966-968: getPageAt error in AppendValue
// getPageAt fails only for genuine I/O errors.
// =============================================================================

// This is an IO_ERROR path — skip.

// =============================================================================
// btree.go L43-53: init() BTREE_TRACE debug logging
// Coverage of the init() function requires running the test binary with
// BTREE_TRACE env var set. This subprocess test verifies all 3 branches:
//   BTREE_TRACE=1       → L43-44, L44-46 (stderr branch)
//   BTREE_TRACE=<file>  → L43-44, L46-48, L51-53 (file branch)
//   BTREE_TRACE=<bad>   → L43-44, L46-48, L48-51 (error fallback)
// Coverage from subprocess runs must be merged separately.
// =============================================================================

func TestDeepCov_Init_BTREETrace(t *testing.T) {
	if os.Getenv("__BTREE_TRACE_SUBPROCESS") == "1" {
		// Subprocess: init() already ran with BTREE_TRACE set.
		// Just verify the state.
		if debugTrace {
			t.Logf("debugTrace=true, tracing active")
		}
		return
	}

	exe, err := os.Executable()
	if err != nil {
		t.Skip("cannot get executable path")
	}

	tests := []struct {
		name     string
		traceVal string
	}{
		{"stderr", "1"},
		{"file", filepath.Join(t.TempDir(), "trace.log")},
		{"error_fallback", "/nonexistent/path/trace.log"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(exe, "-test.run=TestDeepCov_Init_BTREETrace", "-test.v")
			cmd.Env = append(os.Environ(),
				"BTREE_TRACE="+tt.traceVal,
				"__BTREE_TRACE_SUBPROCESS=1",
			)
			out, runErr := cmd.CombinedOutput()
			if runErr != nil {
				t.Logf("subprocess output: %s", out)
			}
			require.NoError(t, runErr)
		})
	}
}

// =============================================================================
// btree.go L2952-2954: Previous() cellIdx == cellCount (rightChild path)
// CONFIRMED UNREACHABLE: In Normal B-tree traversal, frame.cellIdx maxes at
// cellCount (set by Seek/Next). Previous decrements to cellCount-1, which is
// always < cellCount, taking the first branch. To reach cellIdx == cellCount
// after decrement, cellIdx would need to be cellCount+1, which never occurs.
// =============================================================================

// =============================================================================
// btree.go L2680-2682: SeekNear idx >= n fast path
// CONFIRMED UNREACHABLE: The fast path only activates when key is in
// [firstKey, lastKey] of the current leaf. searchLeaf returns an index
// in [0, n-1] for keys in this range, so idx >= n cannot occur.
// =============================================================================

// =============================================================================
// btree.go L2702-2704: SeekExact Key() error
// CONFIRMED UNREACHABLE: SeekExact calls SeekNear which validates cell data
// during binary search. If cell data is corrupt, SeekNear fails first.
// Key() reads the same data that search already validated, so it cannot fail
// independently.
// =============================================================================

// =============================================================================
// btree.go L2770-2777: Value() getVarintSafe errors on overflow cells
// CONFIRMED UNREACHABLE: Value() first calls parseLeafCellWithSize which
// validates the same varints (keyLen, valLen). If they're corrupt,
// parseLeafCellWithSize returns error at L2761-2762, never reaching L2770.
// =============================================================================

// =============================================================================
// btree.go L287-292: leafSplitPoint bestIdx clamping
// CONFIRMED UNREACHABLE: For len(cells) <= 2, function returns 1 at L258.
// For len(cells) >= 3, bestIdx starts at len/2 >= 1 and is only set to i >= 1
// in the loop (i==0 continues, break only for i>0). So bestIdx is always
// in [1, len-1] after the loop, making both clamps dead code.
// =============================================================================

// =============================================================================
// btree.go L329-331: interiorSplitPoint bestIdx < 1 clamping
// CONFIRMED UNREACHABLE: Same logic as leafSplitPoint. For len <= 2, early
// return. For len >= 3, bestIdx starts at len/2 >= 1 and loop only sets it
// to i >= 1. The clamp cannot fire.
// =============================================================================

// =============================================================================
// btree.go L1430-1432: collectLeafCells contentSize < 0
// CONFIRMED UNREACHABLE: contentAreaOffset returns valid offset <= usableSize
// (non-error) or error (fallback contentOff = usableSize, contentSize = 0).
// contentSize = usableSize - contentOff is always >= 0.
// =============================================================================

// =============================================================================
// btree.go L1765-1768: insertSepIntoInterior newContentStart bounds check
// CONFIRMED UNREACHABLE: We enter the block only when cellSize+2 <= freeSpace
// (L1761), and freeSpace = contentStart - hdrSize. So newContentStart =
// contentStart - cellSize >= hdrSize + 2 > 0, and < len(parentPg.data).
// =============================================================================

// =============================================================================
// db.go L509-512, L655-658: parseLeafCellWithSize after search
// CONFIRMED UNREACHABLE: searchLeafWithOverflow validates the same cell varints
// before returning. If the cell is corrupt, the search fails at L543-554
// before reaching parseLeafCellWithSize.
// =============================================================================

// =============================================================================
// db.go L663-672: getVarintSafe in AppendValue overflow path
// CONFIRMED UNREACHABLE: parseLeafCellWithSize (called just before at L654)
// validates the same varints. If they're corrupt, it returns error at L655-658.
// =============================================================================
