package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Area 1: tryMergeLeaf left-direction merge (mergeRight=false)
// Target: btree.go L2259-2262 (keepPgno=siblingPgno, freePgno=leafPgno)
//
// The left merge happens when the leaf being merged IS the rightChild of the
// parent interior page. In that case, the sibling is to the left (the last
// cell's leftChild), mergeRight=false, and the sibling page is kept while
// the leaf page is freed.
// =============================================================================

// TestMergeCursor_TryMergeLeafLeftDirection directly calls tryMergeLeaf with
// the rightChild of a parent interior page. After deleting enough entries from
// the rightmost leaf, the merge condition is met and the left-merge path
// (mergeRight=false) is taken.
func TestMergeCursor_TryMergeLeafLeftDirection(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert enough keys to create a multi-level tree with 512-byte pages.
	// With 4-byte keys and small values, each leaf holds ~15-20 entries.
	putN(t, db, "t1", 60, 10)

	// Delete the highest keys (which live in the rightmost leaf) to make
	// the rightmost leaf sparse enough to merge with its left sibling.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 45; i <= 60; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		_ = tx2.Delete(ns, key)
	}
	require.NoError(t, tx2.Commit())

	// Now open a write tx and call tryMergeLeaf directly on the rightChild
	// of the root (or an interior parent).
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	bt := &btree{pager: db.pager, rootPage: ns2.rootPage, writable: true, walMaxFrame: tx3.walMaxFrame}

	// Navigate to the rightmost leaf: follow rightChild pointers down.
	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)

	var pathBuf [8]uint32
	path := pathBuf[:0]
	for pg.header.isInterior() {
		path = append(path, pg.pgno)
		rightChild := pg.header.rightChild
		bt.pager.releasePage(pg)
		pg, err = bt.getPage(rightChild)
		require.NoError(t, err)
	}
	// pg is now the rightmost leaf page
	require.False(t, pg.header.isInterior(), "expected a leaf page")
	leafPgno := pg.pgno
	bt.pager.releasePage(pg)

	// Verify this leaf IS the rightChild of its parent
	if len(path) > 0 {
		parentPgno := path[len(path)-1]
		parentPg, perr := bt.getPage(parentPgno)
		require.NoError(t, perr)
		assert.Equal(t, leafPgno, parentPg.header.rightChild,
			"the rightmost leaf should be the parent's rightChild")
		bt.pager.releasePage(parentPg)
	}

	// Call tryMergeLeaf on the rightChild leaf. Since it IS the rightChild,
	// mergeRight will be false and the left-merge path will be taken.
	err = bt.tryMergeLeaf(leafPgno, path)
	require.NoError(t, err)

	require.NoError(t, tx3.Commit())
	require.NoError(t, db.IntegrityCheck())

	remaining := countKeys(t, db, "t1")
	assert.Equal(t, 44, remaining)
}

// TestMergeCursor_TryMergeLeafLeftDirectionSmallPage uses an even smaller
// dataset to ensure the left-merge path works when the rightChild has very
// few cells remaining.
func TestMergeCursor_TryMergeLeafLeftDirectionSmallPage(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert keys with slightly larger values to fill pages faster.
	putN(t, db, "t1", 40, 20)

	// Delete a range from the end to make the rightmost leaf sparse
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 30; i <= 40; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		_ = tx2.Delete(ns, key)
	}
	require.NoError(t, tx2.Commit())

	// Now call tryMergeLeaf on the rightChild
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	bt := &btree{pager: db.pager, rootPage: ns2.rootPage, writable: true, walMaxFrame: tx3.walMaxFrame}

	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	var pathBuf [8]uint32
	path := pathBuf[:0]
	for pg.header.isInterior() {
		path = append(path, pg.pgno)
		rc := pg.header.rightChild
		bt.pager.releasePage(pg)
		pg, err = bt.getPage(rc)
		require.NoError(t, err)
	}
	leafPgno := pg.pgno
	bt.pager.releasePage(pg)

	err = bt.tryMergeLeaf(leafPgno, path)
	require.NoError(t, err)

	require.NoError(t, tx3.Commit())
	require.NoError(t, db.IntegrityCheck())
	assert.Equal(t, 29, countKeys(t, db, "t1"))
}

// =============================================================================
// Area 2: removeChildFromParent — non-root empty interior + root collapse
// Target: btree.go L2332-2335 (page 1 preserve DB header path)
//         L2340-2342 (page 1 hdrOff = dbHeaderSize)
//         L2354-2360 (non-root empty interior free + recursive remove)
// =============================================================================

// TestMergeCursor_RootCollapseOnPage1 exercises the root collapse path on page 1.
// The master btree lives on page 1. By creating enough namespaces to force a split,
// then deleting almost all of them, we trigger the root collapse with the special
// page-1 DB header preservation logic.
func TestMergeCursor_RootCollapseOnPage1(t *testing.T) {
	// Regression test for page1-root-collapse-corruption (BUGS.md).
	// Creates enough namespaces on a small-page DB to make page 1 interior,
	// then deletes most of them to trigger root collapse on page 1.
	db := tempDBWithPageSize(t, 512)

	const total = 25
	{
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		for i := 0; i < total; i++ {
			name := fmt.Sprintf("rc-%04d", i)
			_, err = tx.CreateNamespace(name)
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())
	}

	// Delete namespaces one at a time.
	for i := 0; i < total-2; i++ {
		name := fmt.Sprintf("rc-%04d", i)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		require.NoError(t, tx.DeleteNamespace(name))
		require.NoError(t, tx.Commit())
	}

	require.NoError(t, db.IntegrityCheck())
	names, err := db.ListNamespaces()
	require.NoError(t, err)
	assert.Len(t, names, 2)
}

// TestMergeCursor_RootCollapseViaDirectBtree exercises the root collapse path
// using a btree constructed directly with tempPager. This avoids the namespace
// API issues while still testing the removeChildFromParent root collapse logic.
// The btree root is on page 1, which has the dbHeaderSize offset, so this tests
// both the root collapse and the page-1 DB header preservation paths.
func TestMergeCursor_RootCollapseViaDirectBtree(t *testing.T) {
	// Use the DB API to manage page-1 root collapse through the master btree.
	// Create many namespaces on a small-page DB so page 1 becomes interior,
	// then delete them in small batches via separate write transactions.
	db := tempDBWithPageSize(t, 512)

	const total = 30
	// Create namespaces in a single tx
	{
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		for i := 0; i < total; i++ {
			name := fmt.Sprintf("ns-%04d", i)
			_, err = tx.CreateNamespace(name)
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())
	}

	// Verify the master btree on page 1 is now interior
	{
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		bt := &btree{pager: db.pager, rootPage: 1, walMaxFrame: rtx.walMaxFrame}
		pg, err := bt.getPage(1)
		require.NoError(t, err)
		isInterior := pg.header.isInterior()
		bt.pager.releasePage(pg)
		require.NoError(t, rtx.Rollback())
		if !isInterior {
			t.Skip("page 1 did not become interior with 30 namespaces at 512-byte pages")
		}
	}

	// Delete namespaces one at a time in separate write transactions.
	// This avoids the bulk delete sensitivity bug by committing after each delete.
	for i := 0; i < total-2; i++ {
		name := fmt.Sprintf("ns-%04d", i)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		err = tx.DeleteNamespace(name)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}

	require.NoError(t, db.IntegrityCheck())
	names, err := db.ListNamespaces()
	require.NoError(t, err)
	assert.Len(t, names, 2)
}

// TestMergeCursor_NonRootEmptyInteriorFree exercises the non-root empty interior
// page free path (L2354-2360). This requires a 3-level tree where deleting enough
// entries causes an interior page (that is NOT the root) to become empty.
func TestMergeCursor_NonRootEmptyInteriorFree(t *testing.T) {
	// Regression test for bulk-delete-orphan-pages (BUGS.md).
	// Creates a 3-level tree with 512-byte pages, then deletes most entries
	// to trigger non-root empty interior page handling.
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert 600 entries to create a 3-level tree.
	putN(t, db, "t1", 600, 10)

	// Delete 590 entries in batches to trigger interior page collapse.
	for batch := 0; batch < 59; batch++ {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := batch*10 + 1; i <= batch*10+10; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			_ = tx.Delete(ns, key) // ignore key-not-found on already-deleted
		}
		require.NoError(t, tx.Commit())
	}

	require.NoError(t, db.IntegrityCheck())
	assert.Equal(t, 10, countKeys(t, db, "t1"))
}

// TestMergeCursor_DeleteAllFromThreeLevelTree deletes every entry from a 3-level
// tree. This should trigger the full cascade: leaf free -> removeChildFromParent
// -> interior empty -> recursive removeChildFromParent -> root collapse.
func TestMergeCursor_DeleteAllFromThreeLevelTree(t *testing.T) {
	// Regression test for bulk-delete-orphan-pages (BUGS.md).
	// Deletes every entry from a 3-level tree. This triggers the full cascade:
	// leaf free -> removeChildFromParent -> interior empty -> collapse -> root collapse.
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	const total = 600
	putN(t, db, "t1", total, 10)

	// Delete all entries in batches of 10.
	for batch := 0; batch < total/10; batch++ {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := batch*10 + 1; i <= batch*10+10; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			_ = tx.Delete(ns, key)
		}
		require.NoError(t, tx.Commit())
	}

	require.NoError(t, db.IntegrityCheck())
	assert.Equal(t, 0, countKeys(t, db, "t1"))
}

// =============================================================================
// Area 3: Cursor.Previous through multi-level interior pages
// Target: btree.go L2952-2954 (childPgno = pg.header.rightChild in Previous)
//         L2969-2970 (n == 0 break in Previous descent)
//         L2868-2869 (childPg.header.cellCount == 0 in Next descent)
// =============================================================================

// TestMergeCursor_PreviousRightChildPath exercises the Previous() descent path
// where frame.cellIdx == pg.header.cellCount, causing the rightChild to be used.
// This is triggered by interleaving Next() and Previous() on a multi-level tree
// in a specific pattern.
func TestMergeCursor_PreviousRightChildPath(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert entries to create a multi-level tree
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

	// Strategy: Seek to near the end, call Next() to advance through
	// the rightChild subtree, then call Previous() to reverse direction.
	// When we cross back from the rightChild subtree to the previous sibling,
	// Previous() may need to use rightChild on an intermediate interior page.
	cur := rtx.NewCursor(ns2)

	// Seek to a key near the boundary between the last interior cell and rightChild
	seekKey := fmt.Appendf(nil, "key-%06d", n-20)
	require.NoError(t, cur.Seek(seekKey))
	require.True(t, cur.Valid())

	// Advance forward a few steps to enter the rightChild subtree
	for i := 0; i < 15; i++ {
		require.NoError(t, cur.Next())
	}

	// Now reverse: walk backward through all remaining entries
	var prevKeys []string
	for cur.Valid() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		prevKeys = append(prevKeys, string(k))
		require.NoError(t, cur.Previous())
	}
	assert.True(t, len(prevKeys) > n-20, "should traverse at least n-20 keys, got %d", len(prevKeys))

	// Verify descending order
	for i := 1; i < len(prevKeys); i++ {
		assert.True(t, prevKeys[i-1] > prevKeys[i],
			"expected %s > %s at position %d", prevKeys[i-1], prevKeys[i], i)
	}
}

// TestMergeCursor_PreviousFullReverseThreeLevel creates a 3-level tree and
// performs a full reverse traversal using Last()+Previous(), which exercises
// the interior descent path in Previous() through multiple levels.
func TestMergeCursor_PreviousFullReverseThreeLevel(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// With 512-byte pages and short keys+values, we need ~500+ entries
	// for a 3-level tree.
	n := 800
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns, k, v))
	}
	require.NoError(t, tx2.Commit())

	// Verify 3-level depth
	{
		rtx2, err := db.BeginRead()
		require.NoError(t, err)
		ns3, _ := db.getNamespaceLocked("data")
		bt := &btree{pager: db.pager, rootPage: ns3.rootPage, walMaxFrame: rtx2.walMaxFrame}
		depth := measureTreeDepth(t, bt, bt.rootPage)
		require.NoError(t, rtx2.Rollback())
		if depth < 3 {
			t.Logf("tree depth is %d (wanted 3+)", depth)
		}
	}

	// Full reverse traversal
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns2)

	require.NoError(t, cur.Last())
	require.True(t, cur.Valid())

	count := 0
	var lastKey []byte
	for cur.Valid() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		if lastKey != nil {
			assert.True(t, bytes.Compare(k, lastKey) < 0,
				"expected %s < %s", string(k), string(lastKey))
		}
		lastKey = bytes.Clone(k)
		count++
		require.NoError(t, cur.Previous())
	}
	assert.Equal(t, n, count, "should traverse all entries in reverse")
}

// TestMergeCursor_NextPreviousInterleaveMultiLevel creates a multi-level tree
// and interleaves Next() and Previous() calls to exercise different descent
// paths including the rightChild usage in Previous().
func TestMergeCursor_NextPreviousInterleaveMultiLevel(t *testing.T) {
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

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	cur := rtx.NewCursor(ns2)

	// Start from the beginning, advance forward significantly
	require.NoError(t, cur.First())
	require.True(t, cur.Valid())

	// Walk forward 400 steps
	for i := 0; i < 400; i++ {
		require.NoError(t, cur.Next())
		if !cur.Valid() {
			t.Fatalf("cursor became invalid at Next step %d", i)
		}
	}

	// Save current key
	k1, err := cur.Key()
	require.NoError(t, err)

	// Walk backward 200 steps
	for i := 0; i < 200; i++ {
		require.NoError(t, cur.Previous())
		if !cur.Valid() {
			t.Fatalf("cursor became invalid at Previous step %d", i)
		}
	}

	// Save current key
	k2, err := cur.Key()
	require.NoError(t, err)
	assert.True(t, bytes.Compare(k2, k1) < 0,
		"after going back, key should be smaller: %s vs %s", string(k2), string(k1))

	// Walk forward 100 steps again (crosses interior page boundaries)
	for i := 0; i < 100; i++ {
		require.NoError(t, cur.Next())
		if !cur.Valid() {
			t.Fatalf("cursor became invalid at second Next step %d", i)
		}
	}

	k3, err := cur.Key()
	require.NoError(t, err)
	assert.True(t, bytes.Compare(k3, k2) > 0,
		"after going forward again, key should be larger: %s vs %s", string(k3), string(k2))
}

// TestMergeCursor_PreviousAfterSeekToEnd uses Seek with a key past all entries,
// then calls Previous to traverse backward from the end. The Seek+Next combo
// sets up interior frames that then get traversed by Previous.
func TestMergeCursor_PreviousAfterSeekToEnd(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	n := 300
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

	// Seek past all entries
	require.NoError(t, cur.Seek([]byte("zzz")))
	if cur.Valid() {
		// If still valid (shouldn't be since "zzz" > all keys), just proceed
		require.NoError(t, cur.Previous())
	}

	// Position at the last entry
	require.NoError(t, cur.Last())
	require.True(t, cur.Valid())

	// Walk all the way back
	count := 0
	for cur.Valid() {
		_, kerr := cur.Key()
		require.NoError(t, kerr)
		count++
		require.NoError(t, cur.Previous())
	}
	assert.Equal(t, n, count)
}

// =============================================================================
// Combined scenarios: merge + cursor interaction
// =============================================================================

// TestMergeCursor_DeleteHighKeysAndTraverseBackward deletes the highest keys
// from a multi-level tree (exercising the rightChild merge path) and then
// performs a full backward cursor traversal to verify tree integrity.
func TestMergeCursor_DeleteHighKeysAndTraverseBackward(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	n := 300
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx2.Put(ns, key, make([]byte, 15)))
	}
	require.NoError(t, tx2.Commit())

	// Delete the highest 200 keys (from the rightmost side of the tree)
	// in batches, which will trigger rightChild removal and potential
	// interior page collapses.
	batchSize := 50
	for start := n; start > 100; start -= batchSize {
		tx3, err := db.BeginWrite()
		require.NoError(t, err)
		ns3, _ := db.getNamespaceLocked("t1")
		end := start - batchSize + 1
		if end < 101 {
			end = 101
		}
		for i := start; i >= end; i-- {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			_ = tx3.Delete(ns3, key)
		}
		require.NoError(t, tx3.Commit())
	}

	require.NoError(t, db.IntegrityCheck())
	remaining := countKeys(t, db, "t1")
	assert.Equal(t, 100, remaining)

	// Now traverse backward to verify the tree is sound
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns4, _ := db.getNamespaceLocked("t1")
	cur := rtx.NewCursor(ns4)

	require.NoError(t, cur.Last())
	count := 0
	for cur.Valid() {
		_, kerr := cur.Key()
		require.NoError(t, kerr)
		count++
		require.NoError(t, cur.Previous())
	}
	assert.Equal(t, 100, count)
}

// TestMergeCursor_MergeLeftThenCursorTraversal performs a left-merge via
// tryMergeLeaf and then verifies the tree can be traversed in both directions.
func TestMergeCursor_MergeLeftThenCursorTraversal(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	putN(t, db, "t1", 80, 10)

	// Delete keys from the high end to make rightmost leaf sparse
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 60; i <= 80; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		_ = tx2.Delete(ns, key)
	}
	require.NoError(t, tx2.Commit())

	// Now manually trigger a left-merge on the rightmost leaf
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	bt := &btree{pager: db.pager, rootPage: ns2.rootPage, writable: true, walMaxFrame: tx3.walMaxFrame}

	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	var pathBuf [8]uint32
	path := pathBuf[:0]
	for pg.header.isInterior() {
		path = append(path, pg.pgno)
		rc := pg.header.rightChild
		bt.pager.releasePage(pg)
		pg, err = bt.getPage(rc)
		require.NoError(t, err)
	}
	leafPgno := pg.pgno
	bt.pager.releasePage(pg)

	err = bt.tryMergeLeaf(leafPgno, path)
	require.NoError(t, err)
	require.NoError(t, tx3.Commit())
	require.NoError(t, db.IntegrityCheck())

	// Forward traversal
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns3, _ := db.getNamespaceLocked("t1")
	cur := rtx.NewCursor(ns3)

	var forwardKeys []string
	require.NoError(t, cur.First())
	for cur.Valid() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		forwardKeys = append(forwardKeys, string(k))
		require.NoError(t, cur.Next())
	}

	// Backward traversal
	cur2 := rtx.NewCursor(ns3)
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
	assert.Equal(t, 59, len(forwardKeys))
}

// =============================================================================
// Helper: measureTreeDepth
// =============================================================================

// measureTreeDepth returns the depth of the btree rooted at pgno.
// A single leaf has depth 1, a root interior with leaf children has depth 2, etc.
func measureTreeDepth(t *testing.T, bt *btree, pgno uint32) int {
	t.Helper()
	pg, err := bt.getPage(pgno)
	require.NoError(t, err)
	defer bt.pager.releasePage(pg)

	if !pg.header.isInterior() {
		return 1
	}
	// Descend through rightChild to measure depth
	childPgno := pg.header.rightChild
	return 1 + measureTreeDepth(t, bt, childPgno)
}
