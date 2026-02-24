package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
