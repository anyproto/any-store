package btree

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// === Basic integrity check tests ===

func TestIntegrityCheckEmpty(t *testing.T) {
	db := tempDB(t)
	require.NoError(t, db.IntegrityCheck())
}

func TestIntegrityCheckBasic(t *testing.T) {
	db, ns := tempDBWithNS(t, "test")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("value-%04d", i)
		require.NoError(t, tx.Put(ns, []byte(key), []byte(val)))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())
}

func TestIntegrityCheckAfterDeletes(t *testing.T) {
	db, ns := tempDBWithNS(t, "test")

	// Insert enough keys to create multiple pages and a freelist
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("value-%04d", i)
		require.NoError(t, tx.Put(ns, []byte(key), []byte(val)))
	}
	require.NoError(t, tx.Commit())

	// Delete half to populate the freelist
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 200; i += 2 {
		key := fmt.Sprintf("key-%04d", i)
		require.NoError(t, tx.Delete(ns, []byte(key)))
	}
	require.NoError(t, tx.Commit())

	require.NoError(t, db.IntegrityCheck())
}

func TestIntegrityCheckMultipleNamespaces(t *testing.T) {
	db := tempDB(t)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns1, err := tx.CreateNamespace("alpha")
	require.NoError(t, err)
	ns2, err := tx.CreateNamespace("beta")
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k%04d", i)
		require.NoError(t, tx.Put(ns1, []byte(key), []byte("val1")))
		require.NoError(t, tx.Put(ns2, []byte(key), []byte("val2")))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())
}

func TestIntegrityCheckOverflow(t *testing.T) {
	db, ns := tempDBWithNS(t, "test")

	// Insert enough overflow values to trigger leaf splits.
	// Overflow chains must be preserved through collect/rebuild (no orphan pages).
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("big-%04d", i)
		val := make([]byte, 5000) // > maxLocalPayload, forces overflow
		for j := range val {
			val[j] = byte(i)
		}
		require.NoError(t, tx.Put(ns, []byte(key), val))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())
}

func TestIntegrityCheckAfterCheckpoint(t *testing.T) {
	db, ns := tempDBWithNS(t, "test")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		require.NoError(t, tx.Put(ns, []byte(key), []byte("val")))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.IntegrityCheck())
}

// === Corruption detection tests ===

func TestIntegrityCheckCorruptPageType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Corrupt the page type byte
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	pg.data[0] = 7
	pg.header.pageType = 7
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "invalid page type") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'invalid page type' error, got: %v", ie.Errors)
}

func TestIntegrityCheckCorruptCellPointer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Corrupt a cell pointer to point out of range
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	cpOff := pg.cellPointerOffset()
	binary.BigEndian.PutUint16(pg.data[cpOff:], 0xFFFF)
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "out of range") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'out of range' error, got: %v", ie.Errors)
}

func TestIntegrityCheckCorruptFreelist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	// Create a namespace and insert enough data to get multiple pages
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	// Delete the entire namespace to free all its pages to the freelist
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNamespace("test"))
	require.NoError(t, tx.Commit())

	// Verify baseline passes and there IS a freelist now
	require.NoError(t, db.IntegrityCheck())
	require.True(t, db.pager.header.TotalFreelistPgs > 0, "expected freelist pages after namespace deletion")

	// Now corrupt: set wrong TotalFreelistPgs in both the pager header and page 1
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	actualTotal := db.pager.header.TotalFreelistPgs
	db.pager.header.TotalFreelistPgs = actualTotal + 5
	binary.BigEndian.PutUint32(pg1.data[36:40], actualTotal+5)
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "freelist") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected freelist error, got: %v", ie.Errors)
}

func TestIntegrityCheckOrphanPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	// Create an orphan page by incrementing DatabaseSize without using the new page
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	oldSize := db.pager.dbSize.Load()
	db.pager.dbSize.Store(oldSize + 1)
	db.pager.header.DatabaseSize = oldSize + 1
	binary.BigEndian.PutUint32(pg1.data[28:32], oldSize+1)
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "never used") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'never used' error, got: %v", ie.Errors)
}

func TestIntegrityCheckKeyOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Swap cell pointers 0 and 1 to break key ordering
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	off0 := pg.getCellOffset(0)
	off1 := pg.getCellOffset(1)
	pg.setCellOffset(0, off1)
	pg.setCellOffset(1, off0)
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "key out of order") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'key out of order' error, got: %v", ie.Errors)
}

// === Test that integrity check works with reopen ===

func TestIntegrityCheckReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("items")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("key-%04d", i)), []byte("value")))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	db2, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db2.Close()
	require.NoError(t, db2.IntegrityCheck())
}

// === Large tree with splits ===

func TestIntegrityCheckLargeTree(t *testing.T) {
	db, ns := tempDBWithNS(t, "test")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val := fmt.Sprintf("value-%06d", i)
		require.NoError(t, tx.Put(ns, []byte(key), []byte(val)))
	}
	require.NoError(t, tx.Commit())

	require.NoError(t, db.IntegrityCheck())
}

// === Integrity check with overflow deletes (triggers rebuild) ===

func TestIntegrityCheckOverflowAfterDeletes(t *testing.T) {
	db, ns := tempDBWithNS(t, "test")

	// Insert overflow entries, then delete some to trigger rebuild.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("big-%04d", i)
		val := make([]byte, 5000)
		for j := range val {
			val[j] = byte(i)
		}
		require.NoError(t, tx.Put(ns, []byte(key), val))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())

	// Delete some entries (triggers rebuild with remaining overflow cells)
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 10; i += 2 {
		key := fmt.Sprintf("big-%04d", i)
		require.NoError(t, tx.Delete(ns, []byte(key)))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())
}

// === IntegrityError formatting ===

func TestIntegrityErrorFormat(t *testing.T) {
	ie := &IntegrityError{Errors: []string{"error 1", "error 2"}}
	assert.Equal(t, "error 1\nerror 2", ie.Error())
}

// TestCheckListOverlargeTrunkContinuesWalk is a regression test for the drift
// where checkList() aborted the entire freelist walk on an over-large trunk
// leaf-count (early return) instead of reporting and continuing to the next
// trunk, as SQLite's checkList() does (btree.c:10749-10778).
//
// It builds a two-trunk freelist:
//
//	trunk A (corrupt): next = trunk B, leafCount field = maxLeaves+1, no real leaves
//	trunk B (valid):   next = 0,       leafCount = 2, leaves = [L1, L2]
//
// Before the fix, checkList reports "leaf count too big" on trunk A and then
// returns — so trunk B, L1 and L2 are never referenced and the orphan loop
// reports three spurious "page N: never used" errors. After the fix, the walk
// continues into trunk B, references B/L1/L2, and the only error is the single
// "leaf count too big" line. The redundant freelist size mismatch is also
// suppressed (SQLite's nErrAtStart==pCheck->nErr guard, btree.c:10781).
func TestCheckListOverlargeTrunkContinuesWalk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	// Put a little data so the DB is non-trivial and page 1 is a real root.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k"), []byte("v")))
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())

	// Construct the two-trunk freelist by hand.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)

	// Allocate four fresh pages: two trunks and two leaves for trunk B.
	trunkA, err := db.pager.allocatePage()
	require.NoError(t, err)
	tAno := trunkA.pgno
	db.pager.releasePage(trunkA)

	trunkB, err := db.pager.allocatePage()
	require.NoError(t, err)
	tBno := trunkB.pgno
	db.pager.releasePage(trunkB)

	leaf1, err := db.pager.allocatePage()
	require.NoError(t, err)
	l1no := leaf1.pgno
	db.pager.releasePage(leaf1)

	leaf2, err := db.pager.allocatePage()
	require.NoError(t, err)
	l2no := leaf2.pgno
	db.pager.releasePage(leaf2)

	maxLeaves := uint32(db.pager.freelistMaxLeaves())

	// Trunk B: valid trunk holding two real leaves, terminates the list.
	wB, err := db.pager.getWritablePage(tBno)
	require.NoError(t, err)
	clear(wB.data)
	binary.BigEndian.PutUint32(wB.data[0:4], 0)     // next trunk = 0 (last)
	binary.BigEndian.PutUint32(wB.data[4:8], 2)     // leaf count
	binary.BigEndian.PutUint32(wB.data[8:12], l1no) // leaf 1
	binary.BigEndian.PutUint32(wB.data[12:16], l2no)
	wB.header = pageHeader{}
	db.pager.releasePage(wB)

	// Trunk A: CORRUPT leaf count (> maxLeaves), no real leaf entries, links
	// to trunk B. The over-large count must NOT abort the walk.
	wA, err := db.pager.getWritablePage(tAno)
	require.NoError(t, err)
	clear(wA.data)
	binary.BigEndian.PutUint32(wA.data[0:4], tBno)        // next trunk = B
	binary.BigEndian.PutUint32(wA.data[4:8], maxLeaves+1) // CORRUPT leaf count
	wA.header = pageHeader{}
	db.pager.releasePage(wA)

	// Point the header at trunk A. The four pages we allocated are all on the
	// freelist now (trunk A, trunk B, L1, L2). We deliberately set a count that
	// does NOT match the walk's found count to prove the mismatch line is
	// suppressed once the "too big" error has been reported.
	db.pager.header.FirstFreelistPg = tAno
	db.pager.header.TotalFreelistPgs = 4
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok, "expected *IntegrityError, got %T: %v", err, err)

	var tooBig, neverUsed, mismatch int
	for _, e := range ie.Errors {
		switch {
		case strings.Contains(e, "leaf count") && strings.Contains(e, "too big"):
			tooBig++
		case strings.Contains(e, "never used"):
			neverUsed++
		case strings.Contains(e, "expected") && strings.Contains(e, "but found"):
			mismatch++
		}
	}

	assert.Equal(t, 1, tooBig, "expected exactly one over-large leaf count report, got: %v", ie.Errors)
	// The key regression assertion: the walk must continue past trunk A and
	// reference trunk B, L1, L2 — so none of them are reported as orphans.
	assert.Equal(t, 0, neverUsed, "walk aborted on over-large trunk: trunk B and its leaves were left as orphans: %v", ie.Errors)
	// And the redundant size-mismatch line must be suppressed.
	assert.Equal(t, 0, mismatch, "redundant freelist size mismatch was not suppressed: %v", ie.Errors)
}
