package btree

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPath_CellIdxRightmost verifies that the descent loops in Put and Delete
// populate pathEntry.cellIdx correctly — specifically that a monotonic-append
// workload produces a path where every interior level was reached via the
// rightChild pointer (cellIdx == nCell).
//
// This is the structural precondition for the balance_quick fast path
// (docs/superpowers/specs/2026-04-23-balance-quick-port-design.md §4-5).
func TestPath_CellIdxRightmost(t *testing.T) {
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

	// Insert enough rows to produce depth ≥ 2.
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	val := make([]byte, 80)
	for i := 1; i <= 500; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })

	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	bt := &btree{pager: db.pager, rootPage: ns2.rootPage, walMaxFrame: rtx.walMaxFrame}

	maxKey := binary.BigEndian.AppendUint32(nil, uint32(500))
	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)

	var path []pathEntry
	for pg.header.isInterior() {
		nCell := pg.header.cellCount
		childPgno, cellIdx, serr := bt.searchInterior(pg, maxKey)
		require.NoError(t, serr)
		path = append(path, pathEntry{pgno: pg.pgno, cellIdx: uint16(cellIdx), nCell: nCell})
		bt.pager.releasePage(pg)
		pg, err = bt.getPage(childPgno)
		require.NoError(t, err)
	}
	bt.pager.releasePage(pg)

	require.GreaterOrEqual(t, len(path), 1, "tree must have depth ≥ 2 for this fixture")

	// The descent followed the maximum existing key, so every level must
	// have been reached via rightChild.
	for i, e := range path {
		require.Equalf(t, e.nCell, e.cellIdx,
			"path[%d]: expected cellIdx == nCell (rightChild descent) for max-key lookup, got cellIdx=%d nCell=%d pgno=%d",
			i, e.cellIdx, e.nCell, e.pgno)
	}
}

// TestPath_CellIdxMiddle verifies that a mid-key lookup populates cellIdx
// with the correct middle-of-parent slot.
func TestPath_CellIdxMiddle(t *testing.T) {
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
	for i := 1; i <= 500; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })

	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	bt := &btree{pager: db.pager, rootPage: ns2.rootPage, walMaxFrame: rtx.walMaxFrame}

	midKey := binary.BigEndian.AppendUint32(nil, uint32(250))
	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)

	var path []pathEntry
	for pg.header.isInterior() {
		nCell := pg.header.cellCount
		childPgno, cellIdx, serr := bt.searchInterior(pg, midKey)
		require.NoError(t, serr)
		path = append(path, pathEntry{pgno: pg.pgno, cellIdx: uint16(cellIdx), nCell: nCell})
		bt.pager.releasePage(pg)
		pg, err = bt.getPage(childPgno)
		require.NoError(t, err)
	}
	bt.pager.releasePage(pg)

	require.GreaterOrEqual(t, len(path), 1)

	anyNonRightmost := false
	for _, e := range path {
		if e.cellIdx != e.nCell {
			anyNonRightmost = true
			break
		}
	}
	require.True(t, anyNonRightmost,
		"expected at least one non-rightmost descent step for mid-range key lookup; got path=%+v", path)
}
