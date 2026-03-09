package btree

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDebugMVCC(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := Open(dbPath, Options{PageSize: 1024})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create namespace and insert 5 rows
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	pairs := [][2]uint32{{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}}
	for _, p := range pairs {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, intKey(p[0]), intVal(p[1])))
		require.NoError(t, tx.Commit())
	}

	// Read current WAL state
	t.Logf("WAL nFrame after setup: %d", db.pager.wal.nFrame.Load())
	t.Logf("WAL index maxFrame: %d", db.pager.wal.index.maxFrame.Load())
	
	// Dump all page->frame mappings
	db.pager.wal.index.mu.RLock()
	for pgno, frame := range db.pager.wal.index.pageMap {
		t.Logf("  pageMap[%d] = frame %d", pgno, frame)
	}
	db.pager.wal.index.mu.RUnlock()

	// Open reader rtx2
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	t.Logf("Reader walMaxFrame: %d", rtx2.walMaxFrame)

	// Get namespace (uses pager.getPage which uses pager.walMaxFrame)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	t.Logf("Namespace t1 rootPage: %d", ns.rootPage)

	// Count using reader
	cur := rtx2.NewCursor(ns)
	count := 0
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		count++
	}
	t.Logf("Reader sees %d rows (expected 5)", count)

	// Now writer inserts row 6
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, intKey(11), intVal(12)))
	require.NoError(t, wtx.Commit())

	t.Logf("After writer commit:")
	t.Logf("  WAL nFrame: %d", db.pager.wal.nFrame.Load())
	t.Logf("  WAL index maxFrame: %d", db.pager.wal.index.maxFrame.Load())
	
	// Dump updated page->frame mappings
	db.pager.wal.index.mu.RLock()
	for pgno, frame := range db.pager.wal.index.pageMap {
		t.Logf("  pageMap[%d] = frame %d", pgno, frame)
	}
	db.pager.wal.index.mu.RUnlock()

	// Reader rtx2 should still see 5 rows
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	t.Logf("Namespace t1 rootPage (re-fetched): %d", ns.rootPage)
	
	// Test reading pages manually
	rootPg, err := db.pager.getPageWriter(ns.rootPage, rtx2.walMaxFrame)
	if err != nil {
		t.Logf("Error reading root page at walMaxFrame %d: %v", rtx2.walMaxFrame, err)
	} else {
		t.Logf("Root page %d: type=%d cellCount=%d uncached=%v dirty=%v", 
			rootPg.pgno, rootPg.header.pageType, rootPg.header.cellCount,
			rootPg.uncached, rootPg.dirty)
		if rootPg.header.isInterior() {
			t.Logf("  rightChild=%d", rootPg.header.rightChild)
			for i := 0; i < int(rootPg.header.cellCount); i++ {
				off := rootPg.getCellOffset(i)
				childPgno := binary.BigEndian.Uint32(rootPg.data[off:off+4])
				t.Logf("  cell[%d] leftChild=%d", i, childPgno)
			}
		}
		db.pager.releasePage(rootPg)
	}

	cur2 := rtx2.NewCursor(ns)
	count2 := 0
	for err := cur2.First(); err == nil && cur2.Valid(); err = cur2.Next() {
		k, _ := cur2.Key()
		t.Logf("  key: %v", k)
		count2++
	}
	t.Logf("Reader rtx2 sees %d rows (expected 5)", count2)

	// Also check: what does getPageReader get for the root page?
	rootPg2, err := db.pager.getPageReader(ns.rootPage, rtx2.walMaxFrame, nil)
	if err != nil {
		t.Logf("getPageReader error: %v", err)
	} else {
		t.Logf("getPageReader root page: type=%d cellCount=%d",
			rootPg2.header.pageType, rootPg2.header.cellCount)
	}

	// And check page 1
	pg1, err := db.pager.getPageReader(1, rtx2.walMaxFrame, nil)
	if err != nil {
		t.Logf("getPageReader page 1 error: %v", err)
	} else {
		t.Logf("getPageReader page 1: type=%d cellCount=%d", pg1.header.pageType, pg1.header.cellCount)
		// Check if we can find t1 namespace
		idx, found, _ := searchLeafPage(pg1, []byte("t1"))
		t.Logf("  t1 found=%v at idx=%d", found, idx)
		if found {
			off := pg1.getCellOffset(idx)
			cell, _, _ := parseLeafCell(pg1.data, int(off))
			rootPage := binary.BigEndian.Uint32(cell.value)
			t.Logf("  t1 rootPage=%d", rootPage)
		}
	}

	_ = fmt.Sprint("")

	require.NoError(t, rtx2.Rollback())
}
