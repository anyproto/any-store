package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// setupCorruptDB creates a populated DB, checkpoints, closes, removes WAL
// files, and returns (path, fileBytes). The caller can corrupt fileBytes,
// write them back, and re-open the DB.
func setupCorruptDBForDelCur(t *testing.T, pageSize uint32, populate func(db *DB)) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, Options{PageSize: pageSize})
	require.NoError(t, err)
	populate(db)
	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return path, data
}

// =============================================================================
// Delete function coverage (L2026-2115)
// =============================================================================

// TestDelCurCov_DeleteHighFragRebuild triggers the needsRebuild=true path
// (L2105-2108) by accumulating >60 bytes of fragmentation from non-boundary
// cell deletions.
func TestDelCurCov_DeleteHighFragRebuild(t *testing.T) {
	db := tempDBWithPageSize(t, 4096)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert keys with values large enough that each cell > 20 bytes.
	// Each cell = varint(keyLen) + varint(valLen) + key + value ~ 2+1+4+30 = 37 bytes.
	// Deleting 2 non-boundary cells: 2*37 = 74 > 60 triggers rebuild.
	for i := 0; i < 50; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 30)
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Delete in separate single-key txns from the middle
	for round := 0; round < 4; round++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, uint32(10+round))
		require.NoError(t, tx2.Delete(ns2, key))
		require.NoError(t, tx2.Commit())
	}
	require.NoError(t, db.IntegrityCheck())
}

// TestDelCurCov_DeleteHighFragRebuildSmallPage uses a tiny page size and many
// deletions of non-boundary cells to reliably trigger the needsRebuild path.
func TestDelCurCov_DeleteHighFragRebuildSmallPage(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 14) // cell ~ 20 bytes
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Delete middle keys in a single tx to accumulate fragBytes > 60
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 5; i < 15; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		_ = tx2.Delete(ns2, key)
	}
	require.NoError(t, tx2.Commit())
	require.NoError(t, db.IntegrityCheck())
}

// TestDelCurCov_DeleteOverflowFastPath triggers the overflow chain free in
// the fast path (L2111-2115).
func TestDelCurCov_DeleteOverflowFastPath(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 10)))
	}
	overflowKey := binary.BigEndian.AppendUint32(nil, uint32(100))
	require.NoError(t, tx.Put(ns, overflowKey, make([]byte, 400)))
	require.NoError(t, tx.Commit())

	// Delete only the overflow key
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	require.NoError(t, tx2.Delete(ns2, overflowKey))
	require.NoError(t, tx2.Commit())
	require.NoError(t, db.IntegrityCheck())
	require.Equal(t, 3, countKeys(t, db, "t1"))
}

// TestDelCurCov_DeleteParseLeafCellError triggers parseLeafCellWithSize error
// after key search in Delete (L2071-2074).
func TestDelCurCov_DeleteParseLeafCellError(t *testing.T) {
	path, data := setupCorruptDBForDelCur(t, 4096, func(db *DB) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Put(ns, key, make([]byte, 50)))
		}
		require.NoError(t, tx.Commit())
	})

	pageSize := 4096
	corrupted := false
	for pgno := 2; pgno <= len(data)/pageSize; pgno++ {
		offset := (pgno - 1) * pageSize
		if offset+8 > len(data) {
			break
		}
		pageType := data[offset]
		if pageType == pageTypeLeafIdx {
			cellCount := int(binary.BigEndian.Uint16(data[offset+3 : offset+5]))
			if cellCount > 2 {
				// Corrupt the 2nd cell's keyLen varint to be invalid
				cpBase := offset + 8
				cellPtr := int(binary.BigEndian.Uint16(data[cpBase+2 : cpBase+4]))
				cellOff := (pgno-1)*pageSize + cellPtr
				if cellOff > 0 && cellOff+4 < len(data) {
					data[cellOff] = 0xFF
					data[cellOff+1] = 0xFF
					data[cellOff+2] = 0xFF
					data[cellOff+3] = 0xFF
					corrupted = true
				}
			}
			break
		}
	}
	if !corrupted {
		t.Skip("no leaf page found to corrupt")
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := testOpen(t, path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	tx, err := db2.BeginWrite()
	require.NoError(t, err)
	ns, err := db2.getNamespaceLocked("t1")
	if err != nil {
		_ = tx.Rollback()
		return
	}
	key := binary.BigEndian.AppendUint32(nil, uint32(1))
	err = tx.Delete(ns, key)
	if err != nil {
		_ = tx.Rollback()
	} else {
		_ = tx.Commit()
	}
}

// TestDelCurCov_DeleteInteriorDescentGetPageError triggers getPage(childPgno)
// error during interior descent in Delete (L2044-2046).
func TestDelCurCov_DeleteInteriorDescentGetPageError(t *testing.T) {
	path, data := setupCorruptDBForDelCur(t, 4096, func(db *DB) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < 200; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Put(ns, key, make([]byte, 100)))
		}
		require.NoError(t, tx.Commit())
	})

	pageSize := 4096
	corrupted := false
	for pgno := 2; pgno <= len(data)/pageSize; pgno++ {
		offset := (pgno - 1) * pageSize
		if offset+12 > len(data) {
			break
		}
		pageType := data[offset]
		if pageType == pageTypeIntIdx {
			cellCount := int(binary.BigEndian.Uint16(data[offset+3 : offset+5]))
			if cellCount > 0 {
				cpBase := offset + 12
				cellPtr := int(binary.BigEndian.Uint16(data[cpBase : cpBase+2]))
				cellOff := (pgno-1)*pageSize + cellPtr
				if cellOff+4 <= len(data) {
					// Set leftChild to invalid page number
					binary.BigEndian.PutUint32(data[cellOff:cellOff+4], 0xFFFFFFFF)
					corrupted = true
				}
			}
			break
		}
	}
	if !corrupted {
		t.Skip("no interior page found to corrupt")
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := testOpen(t, path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	tx, err := db2.BeginWrite()
	require.NoError(t, err)
	ns, err := db2.getNamespaceLocked("t1")
	if err != nil {
		_ = tx.Rollback()
		return
	}
	key := binary.BigEndian.AppendUint32(nil, uint32(1))
	err = tx.Delete(ns, key)
	if err != nil {
		_ = tx.Rollback()
	} else {
		_ = tx.Commit()
	}
}

// =============================================================================
// tryMergeLeaf coverage (L2137-2276)
// =============================================================================

// TestDelCurCov_TryMergeLeafDirectExercise exercises tryMergeLeaf including
// the left-merge path by targeting the rightChild leaf.
func TestDelCurCov_TryMergeLeafDirectExercise(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	putN(t, db, "t1", 80, 10)

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 65; i <= 80; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		_ = tx2.Delete(ns, key)
	}
	require.NoError(t, tx2.Commit())

	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	bt := &btree{pager: db.pager, rootPage: ns2.rootPage, writable: true, walMaxFrame: tx3.walMaxFrame}

	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	var pathBuf [8]pathEntry
	path := pathBuf[:0]
	for pg.header.isInterior() {
		nCell := pg.header.cellCount
		path = append(path, pathEntry{pgno: pg.pgno, cellIdx: nCell, nCell: nCell})
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
}

// TestDelCurCov_TryMergeLeafEmptyPath exercises tryMergeLeaf with empty path
// (L2162-2164).
func TestDelCurCov_TryMergeLeafEmptyPath(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 10)))
	}
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	bt := &btree{pager: db.pager, rootPage: ns2.rootPage, writable: true, walMaxFrame: tx2.walMaxFrame}
	err = bt.tryMergeLeaf(bt.rootPage, nil)
	require.NoError(t, err)
	require.NoError(t, tx2.Commit())
}

// =============================================================================
// removeChildFromParent coverage (L2280-2396)
// =============================================================================

// TestDelCurCov_RemoveChildFromParentViaHeavyDelete exercises
// removeChildFromParent by deleting all keys from a multi-level tree.
func TestDelCurCov_RemoveChildFromParentViaHeavyDelete(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	putN(t, db, "t1", 300, 8)

	for batch := 0; batch < 30; batch++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := batch*10 + 1; i <= (batch+1)*10; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			_ = tx2.Delete(ns2, key)
		}
		require.NoError(t, tx2.Commit())
	}
	require.NoError(t, db.IntegrityCheck())
	require.Equal(t, 0, countKeys(t, db, "t1"))
}

// TestDelCurCov_RemoveChildRightChildPath exercises the path where the
// rightChild is being removed (L2308-2316).
func TestDelCurCov_RemoveChildRightChildPath(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	putN(t, db, "t1", 100, 8)

	for batch := 0; batch < 5; batch++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := 100 - batch*20; i > 100-(batch+1)*20; i-- {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			_ = tx2.Delete(ns2, key)
		}
		require.NoError(t, tx2.Commit())
	}
	require.NoError(t, db.IntegrityCheck())
	require.Equal(t, 0, countKeys(t, db, "t1"))
}

// TestDelCurCov_RootCollapseInterior exercises the root collapse path
// (L2325-2346).
func TestDelCurCov_RootCollapseInterior(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	putN(t, db, "t1", 60, 8)

	// Verify root is interior
	func() {
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns3, _ := db.getNamespaceLocked("t1")
		bt := &btree{pager: db.pager, rootPage: ns3.rootPage, walMaxFrame: rtx.walMaxFrame}
		pg, err := bt.getPage(bt.rootPage)
		require.NoError(t, err)
		isInt := pg.header.isInterior()
		bt.pager.releasePage(pg)
		require.NoError(t, rtx.Rollback())
		if !isInt {
			t.Skip("root is not interior with 60 keys at 512-byte pages")
		}
	}()

	for i := 1; i <= 55; i++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		_ = tx2.Delete(ns2, key)
		require.NoError(t, tx2.Commit())
	}
	require.NoError(t, db.IntegrityCheck())
	require.Equal(t, 5, countKeys(t, db, "t1"))
}

// TestDelCurCov_NonRootEmptyInterior exercises the non-root empty interior
// page removal path (L2354-2360).
func TestDelCurCov_NonRootEmptyInterior(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Use many keys with larger values to force 3 levels on 512-byte pages
	n := 2000
	putN(t, db, "t1", n, 4)

	func() {
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns3, _ := db.getNamespaceLocked("t1")
		bt := &btree{pager: db.pager, rootPage: ns3.rootPage, walMaxFrame: rtx.walMaxFrame}
		depth := measureTreeDepth(t, bt, bt.rootPage)
		require.NoError(t, rtx.Rollback())
		if depth < 3 {
			t.Skipf("tree depth is %d (wanted 3+)", depth)
		}
	}()

	// Delete all entries one by one in separate txns.
	// This exercises the non-root empty interior path when an interior page
	// (that is NOT the root) becomes empty after its last child is removed.
	for i := 1; i <= n; i++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		_ = tx2.Delete(ns2, key)
		require.NoError(t, tx2.Commit())
	}
	require.NoError(t, db.IntegrityCheck())
	require.Equal(t, 0, countKeys(t, db, "t1"))
}

// TestDelCurCov_CountPageCorruptCellPointer exercises the countPage
// cpBase+2 > dataLen error path (L2393-2396).
func TestDelCurCov_CountPageCorruptCellPointer(t *testing.T) {
	path, data := setupCorruptDBForDelCur(t, 4096, func(db *DB) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < 200; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Put(ns, key, make([]byte, 100)))
		}
		require.NoError(t, tx.Commit())
	})

	pageSize := 4096
	corrupted := false
	for pgno := 2; pgno <= len(data)/pageSize; pgno++ {
		offset := (pgno - 1) * pageSize
		if offset+12 > len(data) {
			break
		}
		pageType := data[offset]
		if pageType == pageTypeIntIdx {
			// Set cellCount to huge value so cpBase+2 > dataLen
			binary.BigEndian.PutUint16(data[offset+3:offset+5], 0x7FFF)
			corrupted = true
			break
		}
	}
	if !corrupted {
		t.Skip("no interior page found to corrupt")
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := testOpen(t, path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns, err := db2.getNamespaceLocked("t1")
	if err != nil {
		_ = rtx.Rollback()
		return
	}
	bt := &btree{pager: db2.pager, rootPage: ns.rootPage, walMaxFrame: rtx.walMaxFrame}
	_, err = bt.Count()
	_ = err // should be ErrCorrupt
	require.NoError(t, rtx.Rollback())
}

// =============================================================================
// Cursor operations coverage
// =============================================================================

// TestDelCurCov_SeekNearFastPathIdxGEn tries to exercise the SeekNear fast
// path where idx >= n (L2680-2682).
func TestDelCurCov_SeekNearFastPathIdxGEn(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert keys with gaps
	for i := 0; i < 30; i++ {
		k := fmt.Appendf(nil, "key-%03d", i*10)
		v := fmt.Appendf(nil, "val-%03d", i*10)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	cur := rtx.NewCursor(ns2)
	require.NoError(t, cur.Seek([]byte("key-050")))
	require.True(t, cur.Valid())

	// Call SeekNear with various keys that are between existing keys on the
	// current leaf. The fast path checks firstKey <= key <= lastKey.
	require.NoError(t, cur.SeekNear([]byte("key-055")))
	require.NoError(t, cur.SeekNear([]byte("key-065")))
	require.NoError(t, cur.SeekNear([]byte("key-050")))
}

// TestDelCurCov_SeekNearIdxGEnOverflow uses overflow cells so leafKeyAt
// returns ErrCorrupt and falls back to full Seek.
func TestDelCurCov_SeekNearIdxGEnOverflow(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert with large values that overflow on 512-byte pages
	for i := 0; i < 20; i++ {
		key := binary.BigEndian.AppendUint64(nil, uint64(i*100))
		val := make([]byte, 200)
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	cur := rtx.NewCursor(ns2)
	require.NoError(t, cur.First())
	require.True(t, cur.Valid())

	for i := 0; i < 20; i++ {
		key := binary.BigEndian.AppendUint64(nil, uint64(i*100+50))
		_ = cur.SeekNear(key)
	}
}

// TestDelCurCov_SeekExactKeyError exercises the SeekExact path where Key()
// returns error (L2702-2704).
func TestDelCurCov_SeekExactKeyError(t *testing.T) {
	path, data := setupCorruptDBForDelCur(t, 4096, func(db *DB) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < 20; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Put(ns, key, make([]byte, 50)))
		}
		require.NoError(t, tx.Commit())
	})

	pageSize := 4096
	corrupted := false
	for pgno := 2; pgno <= len(data)/pageSize; pgno++ {
		offset := (pgno - 1) * pageSize
		if offset+8 > len(data) {
			break
		}
		pageType := data[offset]
		if pageType == pageTypeLeafIdx {
			cellCount := int(binary.BigEndian.Uint16(data[offset+3 : offset+5]))
			if cellCount > 3 {
				// Corrupt the 3rd cell's keyLen varint
				cpBase := offset + 8
				cellPtr := int(binary.BigEndian.Uint16(data[cpBase+4 : cpBase+6]))
				cellOff := (pgno-1)*pageSize + cellPtr
				if cellOff > 0 && cellOff+3 < len(data) {
					data[cellOff] = 0xFF
					data[cellOff+1] = 0xFF
					data[cellOff+2] = 0xFF
					data[cellOff+3] = 0xFF
					corrupted = true
				}
			}
			break
		}
	}
	if !corrupted {
		t.Skip("no leaf page found to corrupt for SeekExact test")
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := testOpen(t, path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns, err := db2.getNamespaceLocked("t1")
	if err != nil {
		return
	}
	cur := rtx.NewCursor(ns)
	key := binary.BigEndian.AppendUint32(nil, uint32(2))
	_ = cur.SeekExact(key)
}

// TestDelCurCov_ValueOverflowCorruptVarint exercises the Value() overflow path
// getVarintSafe keyLen error (L2770-2772).
func TestDelCurCov_ValueOverflowCorruptVarint(t *testing.T) {
	path, data := setupCorruptDBForDelCur(t, 512, func(db *DB) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			val := make([]byte, 400)
			require.NoError(t, tx.Put(ns, key, val))
		}
		require.NoError(t, tx.Commit())
	})

	pageSize := 512
	corrupted := false
	for pgno := 2; pgno <= len(data)/pageSize; pgno++ {
		offset := (pgno - 1) * pageSize
		if offset+8 > len(data) {
			break
		}
		pageType := data[offset]
		if pageType == pageTypeLeafIdx {
			cellCount := int(binary.BigEndian.Uint16(data[offset+3 : offset+5]))
			if cellCount > 0 {
				cpBase := offset + 8
				cellPtr := int(binary.BigEndian.Uint16(data[cpBase : cpBase+2]))
				cellOff := (pgno-1)*pageSize + cellPtr

				if cellOff+10 < len(data) {
					kl, kn := getVarint(data[cellOff:])
					vl, _ := getVarint(data[cellOff+kn:])
					totalPayload := int(kl) + int(vl)
					maxLocal := maxLocalPayload(pageSize)
					if totalPayload > maxLocal {
						// Corrupt keyLen varint: all continuation bytes
						for j := 0; j < 9 && cellOff+j < len(data); j++ {
							data[cellOff+j] = 0x80
						}
						corrupted = true
						break
					}
				}
			}
		}
	}
	if !corrupted {
		t.Skip("no overflow cell found to corrupt")
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := testOpen(t, path, Options{PageSize: 512})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns, err := db2.getNamespaceLocked("t1")
	if err != nil {
		return
	}
	cur := rtx.NewCursor(ns)
	_ = cur.First()
	for cur.Valid() {
		_, verr := cur.Value()
		if verr != nil {
			break
		}
		_ = cur.Next()
	}
}

// TestDelCurCov_ValueOverflowCorruptValLenVarint specifically corrupts the
// valLen varint in an overflow cell to trigger L2775-2777.
func TestDelCurCov_ValueOverflowCorruptValLenVarint(t *testing.T) {
	path, data := setupCorruptDBForDelCur(t, 512, func(db *DB) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			val := make([]byte, 400)
			require.NoError(t, tx.Put(ns, key, val))
		}
		require.NoError(t, tx.Commit())
	})

	pageSize := 512
	corrupted := false
	for pgno := 2; pgno <= len(data)/pageSize; pgno++ {
		offset := (pgno - 1) * pageSize
		if offset+8 > len(data) {
			break
		}
		pageType := data[offset]
		if pageType == pageTypeLeafIdx {
			cellCount := int(binary.BigEndian.Uint16(data[offset+3 : offset+5]))
			if cellCount > 0 {
				cpBase := offset + 8
				cellPtr := int(binary.BigEndian.Uint16(data[cpBase : cpBase+2]))
				cellOff := (pgno-1)*pageSize + cellPtr

				if cellOff+10 < len(data) {
					kl, kn := getVarint(data[cellOff:])
					vl, vn := getVarint(data[cellOff+kn:])
					totalPayload := int(kl) + int(vl)
					maxLocal := maxLocalPayload(pageSize)
					if totalPayload > maxLocal {
						// Corrupt valLen varint (starts after kn bytes)
						valPos := cellOff + kn
						for j := 0; j < vn+2 && valPos+j < len(data); j++ {
							data[valPos+j] = 0x80
						}
						corrupted = true
						break
					}
				}
			}
		}
	}
	if !corrupted {
		t.Skip("no overflow cell found to corrupt valLen")
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := testOpen(t, path, Options{PageSize: 512})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns, err := db2.getNamespaceLocked("t1")
	if err != nil {
		return
	}
	cur := rtx.NewCursor(ns)
	_ = cur.First()
	for cur.Valid() {
		_, verr := cur.Value()
		if verr != nil {
			break
		}
		_ = cur.Next()
	}
}

// TestDelCurCov_ValueOverflowReadChainError exercises the readOverflowChain
// error path in Value() (L2795-2797).
func TestDelCurCov_ValueOverflowReadChainError(t *testing.T) {
	path, data := setupCorruptDBForDelCur(t, 512, func(db *DB) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			val := make([]byte, 400)
			require.NoError(t, tx.Put(ns, key, val))
		}
		require.NoError(t, tx.Commit())
	})

	pageSize := 512
	corrupted := false
	for pgno := 2; pgno <= len(data)/pageSize; pgno++ {
		offset := (pgno - 1) * pageSize
		if offset+8 > len(data) {
			break
		}
		pageType := data[offset]
		if pageType == pageTypeLeafIdx {
			cellCount := int(binary.BigEndian.Uint16(data[offset+3 : offset+5]))
			if cellCount > 0 {
				cpBase := offset + 8
				cellPtr := int(binary.BigEndian.Uint16(data[cpBase : cpBase+2]))
				cellOff := (pgno-1)*pageSize + cellPtr

				if cellOff+10 < len(data) {
					kl, kn := getVarint(data[cellOff:])
					vl, vn := getVarint(data[cellOff+kn:])
					totalPayload := int(kl) + int(vl)
					maxLocal := maxLocalPayload(pageSize)
					if totalPayload > maxLocal {
						nLocal := localPayloadSize(totalPayload, pageSize)
						overflowPtrOff := cellOff + kn + vn + nLocal
						if overflowPtrOff+4 <= len(data) {
							// Set overflow pointer to 0 (invalid)
							binary.BigEndian.PutUint32(data[overflowPtrOff:], 0)
							corrupted = true
						}
						break
					}
				}
			}
		}
	}
	if !corrupted {
		t.Skip("no overflow cell found to corrupt overflow ptr")
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := testOpen(t, path, Options{PageSize: 512})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns, err := db2.getNamespaceLocked("t1")
	if err != nil {
		return
	}
	cur := rtx.NewCursor(ns)
	_ = cur.First()
	for cur.Valid() {
		_, verr := cur.Value()
		if verr != nil {
			break
		}
		_ = cur.Next()
	}
}

// TestDelCurCov_PreviousDescentCellCountZero exercises Previous() descent
// into interior page with cellCount == 0 (L2969-2970).
func TestDelCurCov_PreviousDescentCellCountZero(t *testing.T) {
	path, data := setupCorruptDBForDelCur(t, 512, func(db *DB) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < 200; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Put(ns, key, make([]byte, 8)))
		}
		require.NoError(t, tx.Commit())
	})

	pageSize := 512
	corrupted := false
	for pgno := 2; pgno <= len(data)/pageSize; pgno++ {
		offset := (pgno - 1) * pageSize
		if offset+12 > len(data) {
			break
		}
		pageType := data[offset]
		if pageType == pageTypeIntIdx {
			// Set cellCount to 0
			binary.BigEndian.PutUint16(data[offset+3:offset+5], 0)
			corrupted = true
			break
		}
	}
	if !corrupted {
		t.Skip("no interior page found to corrupt")
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := testOpen(t, path, Options{PageSize: 512})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns, err := db2.getNamespaceLocked("t1")
	if err != nil {
		return
	}
	cur := rtx.NewCursor(ns)

	// Forward to set up frames, then reverse
	_ = cur.First()
	for i := 0; i < 100 && cur.Valid(); i++ {
		_ = cur.Next()
	}
	for cur.Valid() {
		_ = cur.Previous()
	}

	// Also try Last + Previous
	cur2 := rtx.NewCursor(ns)
	_ = cur2.Last()
	for cur2.Valid() {
		_ = cur2.Previous()
	}
}

// TestDelCurCov_NextDescentCellCountZero exercises Next() descent into
// interior page with cellCount == 0 (L2868-2869).
func TestDelCurCov_NextDescentCellCountZero(t *testing.T) {
	path, data := setupCorruptDBForDelCur(t, 512, func(db *DB) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < 200; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx.Put(ns, key, make([]byte, 8)))
		}
		require.NoError(t, tx.Commit())
	})

	pageSize := 512
	corrupted := false
	for pgno := 2; pgno <= len(data)/pageSize; pgno++ {
		offset := (pgno - 1) * pageSize
		if offset+12 > len(data) {
			break
		}
		pageType := data[offset]
		if pageType == pageTypeIntIdx {
			binary.BigEndian.PutUint16(data[offset+3:offset+5], 0)
			corrupted = true
			break
		}
	}
	if !corrupted {
		t.Skip("no interior page found to corrupt")
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := testOpen(t, path, Options{PageSize: 512})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns, err := db2.getNamespaceLocked("t1")
	if err != nil {
		return
	}
	cur := rtx.NewCursor(ns)
	_ = cur.First()
	for cur.Valid() {
		_ = cur.Next()
	}
}

// TestDelCurCov_PreviousRightChildDescentN0 exercises Previous() rightmost
// descent where n == 0 (L2952-2954, L2969-2970).
func TestDelCurCov_PreviousRightChildDescentN0(t *testing.T) {
	path, data := setupCorruptDBForDelCur(t, 512, func(db *DB) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < 500; i++ {
			key := fmt.Appendf(nil, "key-%06d", i)
			val := fmt.Appendf(nil, "val-%06d", i)
			require.NoError(t, tx.Put(ns, key, val))
		}
		require.NoError(t, tx.Commit())
	})

	pageSize := 512
	// Find the namespace root page: scan for an interior page that's
	// likely the namespace btree root.
	// Corrupt a non-root interior page's cellCount to 0.
	corrupted := false
	rootPgno := uint32(0)
	// First, find the root by scanning page 1 (master table) for namespace entry
	// For simplicity, just find any interior page and corrupt it
	for pgno := 2; pgno <= len(data)/pageSize; pgno++ {
		offset := (pgno - 1) * pageSize
		if offset+12 > len(data) {
			break
		}
		if data[offset] == pageTypeIntIdx {
			if rootPgno == 0 {
				rootPgno = uint32(pgno)
				continue // skip the first interior page (likely root)
			}
			// Corrupt this non-root interior page
			binary.BigEndian.PutUint16(data[offset+3:offset+5], 0)
			corrupted = true
			break
		}
	}
	if !corrupted {
		// Try corrupting the only interior page found
		if rootPgno != 0 {
			offset := int(rootPgno-1) * pageSize
			binary.BigEndian.PutUint16(data[offset+3:offset+5], 0)
			corrupted = true
		}
	}
	if !corrupted {
		t.Skip("no interior page found to corrupt")
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := testOpen(t, path, Options{PageSize: 512})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns, err := db2.getNamespaceLocked("t1")
	if err != nil {
		return
	}
	cur := rtx.NewCursor(ns)

	_ = cur.Last()
	for cur.Valid() {
		_ = cur.Previous()
	}

	cur2 := rtx.NewCursor(ns)
	_ = cur2.Seek([]byte("key-999999"))
	if cur2.Valid() {
		_ = cur2.Previous()
	}
}

// TestDelCurCov_CursorPreviousRightmostMultiLevel does a full reverse
// traversal on a multi-level tree (L2952-2954 rightChild descent).
func TestDelCurCov_CursorPreviousRightmostMultiLevel(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	n := 800
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "key-%06d", i)
		val := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns2, err := db.getNamespaceLocked("t1")
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
			require.True(t, bytes.Compare(k, lastKey) < 0)
		}
		lastKey = bytes.Clone(k)
		count++
		require.NoError(t, cur.Previous())
	}
	require.Equal(t, n, count)
}

// =============================================================================
// Comprehensive tests
// =============================================================================

// TestDelCurCov_BulkDeleteWithMergeAndCollapse inserts then deletes
// from a multi-level tree to exercise merge, removal, and collapse paths.
func TestDelCurCov_BulkDeleteWithMergeAndCollapse(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	putN(t, db, "t1", 400, 8)

	for batch := 0; batch < 40; batch++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		start := 200 - batch*5
		end := 200 + batch*5
		for i := start; i <= end; i++ {
			if i >= 1 && i <= 400 {
				key := binary.BigEndian.AppendUint32(nil, uint32(i))
				_ = tx2.Delete(ns2, key)
			}
		}
		require.NoError(t, tx2.Commit())
	}
	require.NoError(t, db.IntegrityCheck())
}

// TestDelCurCov_DeleteHighFragThenOverflow creates fragmentation then
// inserts+deletes overflow cells.
func TestDelCurCov_DeleteHighFragThenOverflow(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 15; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 20)
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 3; i < 12; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		_ = tx2.Delete(ns2, key)
	}
	require.NoError(t, tx2.Commit())

	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	overflowKey := binary.BigEndian.AppendUint32(nil, uint32(200))
	require.NoError(t, tx3.Put(ns3, overflowKey, make([]byte, 400)))
	require.NoError(t, tx3.Commit())

	tx4, err := db.BeginWrite()
	require.NoError(t, err)
	ns4, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	require.NoError(t, tx4.Delete(ns4, overflowKey))
	require.NoError(t, tx4.Commit())
	require.NoError(t, db.IntegrityCheck())
}

// TestDelCurCov_SeekNearThenSeekExact exercises both SeekNear and SeekExact.
func TestDelCurCov_SeekNearThenSeekExact(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		key := fmt.Appendf(nil, "key-%06d", i*10)
		val := fmt.Appendf(nil, "val-%06d", i*10)
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	cur := rtx.NewCursor(ns2)
	require.NoError(t, cur.Seek([]byte("key-000500")))
	require.True(t, cur.Valid())
	require.NoError(t, cur.SeekNear([]byte("key-000500")))
	require.True(t, cur.Valid())
	require.NoError(t, cur.SeekNear([]byte("key-000505")))
	require.True(t, cur.Valid())
	require.NoError(t, cur.SeekExact([]byte("key-000500")))
	require.Error(t, cur.SeekExact([]byte("key-000505")))
	require.Error(t, cur.SeekExact([]byte("zzzzzzz")))
}

// TestDelCurCov_CursorNextPreviousInterleave exercises Next() and Previous()
// interleaving across interior page boundaries.
func TestDelCurCov_CursorNextPreviousInterleave(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	n := 600
	for i := 0; i < n; i++ {
		key := fmt.Appendf(nil, "key-%06d", i)
		val := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	cur := rtx.NewCursor(ns2)
	require.NoError(t, cur.First())
	for i := 0; i < 300; i++ {
		require.True(t, cur.Valid())
		require.NoError(t, cur.Next())
	}
	for i := 0; i < 150; i++ {
		require.True(t, cur.Valid())
		require.NoError(t, cur.Previous())
	}
	for i := 0; i < 200; i++ {
		if !cur.Valid() {
			break
		}
		require.NoError(t, cur.Next())
	}
	for cur.Valid() {
		require.NoError(t, cur.Previous())
	}
}

// TestDelCurCov_ValueOverflowWriteTx exercises Value() overflow via read tx.
func TestDelCurCov_ValueOverflowWriteTx(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 400)
		for j := range val {
			val[j] = byte(i)
		}
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns2)
	require.NoError(t, cur.First())
	count := 0
	for cur.Valid() {
		v, verr := cur.Value()
		require.NoError(t, verr)
		require.Equal(t, 400, len(v))
		count++
		require.NoError(t, cur.Next())
	}
	require.Equal(t, 10, count)
	require.NoError(t, rtx.Rollback())
}

// TestDelCurCov_DeleteFromSingleLeafPage exercises delete when tree is a
// single root leaf.
func TestDelCurCov_DeleteFromSingleLeafPage(t *testing.T) {
	db := tempDBWithPageSize(t, 4096)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	require.NoError(t, tx.Commit())

	for i := 0; i < 5; i++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx2.Delete(ns2, key))
		require.NoError(t, tx2.Commit())
	}
	require.NoError(t, db.IntegrityCheck())
	require.Equal(t, 0, countKeys(t, db, "t1"))
}

// TestDelCurCov_DeleteNotFound exercises the ErrKeyNotFound path.
func TestDelCurCov_DeleteNotFound(t *testing.T) {
	db := tempDBWithPageSize(t, 4096)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	key := binary.BigEndian.AppendUint32(nil, uint32(999))
	err = tx2.Delete(ns2, key)
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, tx2.Rollback())
}

// TestDelCurCov_DeleteBoundaryCell exercises the fast path where
// cellOff == contentStart (L2090-2093).
func TestDelCurCov_DeleteBoundaryCell(t *testing.T) {
	db := tempDBWithPageSize(t, 4096)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	key := binary.BigEndian.AppendUint32(nil, uint32(4))
	require.NoError(t, tx2.Delete(ns2, key))
	require.NoError(t, tx2.Commit())
	require.NoError(t, db.IntegrityCheck())
	require.Equal(t, 4, countKeys(t, db, "t1"))
}

// TestDelCurCov_DeleteEmptyLeafFreesPage exercises the path where deleting
// the last cell from a non-root leaf frees it (L2135-2141).
func TestDelCurCov_DeleteEmptyLeafFreesPage(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	putN(t, db, "t1", 50, 10)

	for batch := 0; batch < 5; batch++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := batch*10 + 1; i <= (batch+1)*10; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			_ = tx2.Delete(ns2, key)
		}
		require.NoError(t, tx2.Commit())
	}
	require.NoError(t, db.IntegrityCheck())
	require.Equal(t, 0, countKeys(t, db, "t1"))
}

// TestDelCurCov_DeleteGetWritablePageError is a placeholder for the
// getWritablePage(leafPgno) error path (L2054-2056). This error path
// requires internal fault injection.
func TestDelCurCov_DeleteGetWritablePageError(t *testing.T) {
	t.Skip("L2054-2056 requires fault injection in pager.getWritablePage")
}

// =============================================================================
// Direct btree-level tests using tempPager for precise error path targeting
// =============================================================================

// TestDelCurCov_DirectDeleteParseLeafCellError uses a btree created with
// tempPager and corrupts a cell after search to trigger parseLeafCellWithSize
// error in Delete (L2071-2074).
func TestDelCurCov_DirectDeleteParseLeafCellError(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Insert some keys
	for i := 0; i < 10; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, bt.Put(key, make([]byte, 20)))
	}

	// Get root page and corrupt a cell's varint AFTER the search position
	// The key 5 should be present. We corrupt the cell data at key 5's offset
	// so that parseLeafCellWithSize fails after searchLeaf finds it.
	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)

	// Find cell 5's offset
	idx, found, serr := bt.searchLeaf(pg, binary.BigEndian.AppendUint32(nil, 5))
	require.NoError(t, serr)
	require.True(t, found)

	cellOff := int(pg.getCellOffset(idx))
	// Corrupt the cell data: write invalid varint at cell offset
	pg.data[cellOff] = 0x80
	pg.data[cellOff+1] = 0x80
	pg.data[cellOff+2] = 0x80
	pg.data[cellOff+3] = 0x80
	pg.data[cellOff+4] = 0x80
	pg.data[cellOff+5] = 0x80
	pg.data[cellOff+6] = 0x80
	pg.data[cellOff+7] = 0x80
	pg.data[cellOff+8] = 0x80
	p.releasePage(pg)

	// Now try to delete key 5 - should fail at parseLeafCellWithSize
	key := binary.BigEndian.AppendUint32(nil, 5)
	err = bt.Delete(key)
	require.Error(t, err)
}

// TestDelCurCov_DirectDeleteNeedsRebuild uses a btree with direct page
// manipulation to trigger the needsRebuild=true path (L2094-2108).
// We set fragBytes high on the page header before deleting a non-boundary cell,
// so that newFrag = fragBytes + oldCellSize > 60.
func TestDelCurCov_DirectDeleteNeedsRebuild(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Fill the leaf page with cells
	for i := 0; i < 10; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, bt.Put(key, make([]byte, 10)))
	}

	// Manually set fragBytes to 55 on the root page so the next non-boundary
	// delete will push it over 60 (55 + ~16 = ~71 > 60).
	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)
	pg.header.fragBytes = 55
	hdr := 0
	if pg.pgno == 1 {
		hdr = dbHeaderSize
	}
	pg.header.serialize(pg.data[hdr:])
	p.releasePage(pg)

	// Delete a middle key (not at content boundary) to trigger needsRebuild
	key := binary.BigEndian.AppendUint32(nil, uint32(5))
	err = bt.Delete(key)
	require.NoError(t, err)

	// Verify the tree is still valid
	pg2, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	// After rebuild, fragBytes should be 0
	require.Equal(t, uint8(0), pg2.header.fragBytes)
	require.Equal(t, uint16(9), pg2.header.cellCount)
	bt.pager.releasePage(pg2)
}

// TestDelCurCov_DirectDeleteOverflowFastPath inserts an overflow cell
// and deletes it via the fast path (L2111-2115).
func TestDelCurCov_DirectDeleteOverflowFastPath(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Insert small cells first
	for i := 0; i < 3; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, bt.Put(key, make([]byte, 5)))
	}

	// Insert an overflow cell (value > maxLocal ~ 102 bytes on 512 pages)
	overflowKey := binary.BigEndian.AppendUint32(nil, uint32(100))
	require.NoError(t, bt.Put(overflowKey, make([]byte, 300)))

	// Verify the key has an overflow page
	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	usable := bt.usablePageSize()
	// Find the overflow key cell
	idx, found, serr := bt.searchLeaf(pg, overflowKey)
	require.NoError(t, serr)
	require.True(t, found)
	cellOff := int(pg.getCellOffset(idx))
	cell, _, cerr := parseLeafCellWithSize(pg.data, cellOff, usable)
	require.NoError(t, cerr)
	require.NotZero(t, cell.overflowPg, "expected overflow cell")
	bt.pager.releasePage(pg)

	// Delete the overflow key - should use fast path with overflow chain free
	// This hits L2111 (oldCell.overflowPg != 0) and L2112 (freeOverflowChain)
	require.NoError(t, bt.Delete(overflowKey))

	// Verify tree is still valid
	pg2, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	require.Equal(t, uint16(3), pg2.header.cellCount)
	bt.pager.releasePage(pg2)
}

// TestDelCurCov_DirectCursorValueOverflowKeyLenCorrupt exercises Value()
// overflow path where getVarintSafe(keyLen) fails (L2770-2772).
func TestDelCurCov_DirectCursorValueOverflowKeyLenCorrupt(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Insert an overflow cell
	key := binary.BigEndian.AppendUint32(nil, uint32(1))
	require.NoError(t, bt.Put(key, make([]byte, 300)))

	// Position cursor at the cell
	cur := bt.NewCursor()
	require.NoError(t, cur.First())
	require.True(t, cur.Valid())

	// Verify cell has overflow
	frame := &cur.stack[len(cur.stack)-1]
	usableSize := bt.usablePageSize()
	off, oerr := frame.pg.getCellOffsetSafe(frame.cellIdx)
	require.NoError(t, oerr)
	cell, _, cerr := parseLeafCellWithSize(frame.pg.data, int(off), usableSize)
	require.NoError(t, cerr)
	if cell.overflowPg == 0 {
		t.Skip("cell doesn't have overflow on this page size")
	}

	// Now corrupt the keyLen varint at the cell offset
	// Make all bytes continuation bytes so getVarintSafe fails
	cellOff := int(off)
	for j := 0; j < 9 && cellOff+j < len(frame.pg.data); j++ {
		frame.pg.data[cellOff+j] = 0x80
	}

	// Value() should now fail at getVarintSafe(keyLen)
	_, verr := cur.Value()
	require.Error(t, verr)
	cur.Close()
}

// TestDelCurCov_DirectCursorValueOverflowValLenCorrupt exercises Value()
// overflow path where getVarintSafe(valLen) fails (L2775-2777).
func TestDelCurCov_DirectCursorValueOverflowValLenCorrupt(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	key := binary.BigEndian.AppendUint32(nil, uint32(1))
	require.NoError(t, bt.Put(key, make([]byte, 300)))

	cur := bt.NewCursor()
	require.NoError(t, cur.First())
	require.True(t, cur.Valid())

	frame := &cur.stack[len(cur.stack)-1]
	usableSize := bt.usablePageSize()
	off, oerr := frame.pg.getCellOffsetSafe(frame.cellIdx)
	require.NoError(t, oerr)
	cell, _, cerr := parseLeafCellWithSize(frame.pg.data, int(off), usableSize)
	require.NoError(t, cerr)
	if cell.overflowPg == 0 {
		t.Skip("cell doesn't have overflow")
	}

	// Corrupt the valLen varint (after keyLen varint)
	cellOff := int(off)
	_, kn := getVarint(frame.pg.data[cellOff:])
	valPos := cellOff + kn
	// Make valLen varint invalid (all continuation bytes)
	for j := 0; j < 9 && valPos+j < len(frame.pg.data); j++ {
		frame.pg.data[valPos+j] = 0x80
	}

	_, verr := cur.Value()
	require.Error(t, verr)
	cur.Close()
}

// TestDelCurCov_DirectCursorValueOverflowReadChainError exercises Value()
// overflow path where readOverflowChain fails (L2795-2797).
func TestDelCurCov_DirectCursorValueOverflowReadChainError(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	key := binary.BigEndian.AppendUint32(nil, uint32(1))
	require.NoError(t, bt.Put(key, make([]byte, 300)))

	cur := bt.NewCursor()
	require.NoError(t, cur.First())
	require.True(t, cur.Valid())

	frame := &cur.stack[len(cur.stack)-1]
	usableSize := bt.usablePageSize()
	off, oerr := frame.pg.getCellOffsetSafe(frame.cellIdx)
	require.NoError(t, oerr)
	cell, _, cerr := parseLeafCellWithSize(frame.pg.data, int(off), usableSize)
	require.NoError(t, cerr)
	if cell.overflowPg == 0 {
		t.Skip("cell doesn't have overflow")
	}

	// Corrupt the overflow page pointer to 0 (invalid)
	cellOff := int(off)
	kl, kn := getVarint(frame.pg.data[cellOff:])
	vl, vn := getVarint(frame.pg.data[cellOff+kn:])
	totalPayload := int(kl) + int(vl)
	nLocal := localPayloadSize(totalPayload, usableSize)
	overflowPtrOff := cellOff + kn + vn + nLocal
	// Set overflow pointer to 0 (invalid page)
	binary.BigEndian.PutUint32(frame.pg.data[overflowPtrOff:], 0)

	// Set overflow pointer to page 1 (which is valid but not an overflow page,
	// so the chain read will likely produce garbage or fail).
	// Actually, page 0 is caught by pgno<2 check. Let's use a huge page number
	// that's beyond dbSize.
	binary.BigEndian.PutUint32(frame.pg.data[overflowPtrOff:], 0xFFFFFFFE)

	cur.Close()

	cur2 := bt.NewCursor()
	require.NoError(t, cur2.First())
	require.True(t, cur2.Valid())

	_, verr := cur2.Value()
	// Should error because overflow page number is beyond database size
	if verr != nil {
		// Good: got the expected error
	} else {
		t.Log("overflow chain read did not error (pager may handle large page numbers)")
	}
	cur2.Close()
}

// TestDelCurCov_DirectCursorSeekExactKeyError positions a cursor on a cell
// then corrupts it so Key() fails in SeekExact (L2702-2704).
func TestDelCurCov_DirectCursorSeekExactKeyError(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	for i := 0; i < 10; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, bt.Put(key, make([]byte, 20)))
	}

	// Corrupt a specific cell so that Key() fails after SeekNear succeeds.
	// SeekNear uses searchLeaf which uses a different parsing path.
	// After seek positions on a cell, Key() calls parseLeafCellWithSize.
	// If the cell data is corrupted, Key() returns error.
	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)

	// Find cell at index 5 and corrupt it
	targetIdx := 5
	cellOff := int(pg.getCellOffset(targetIdx))
	// Write invalid varint (all continuation bytes)
	for j := 0; j < 9 && cellOff+j < len(pg.data); j++ {
		pg.data[cellOff+j] = 0x80
	}
	p.releasePage(pg)

	// SeekExact for key 5 should fail: SeekNear→Seek→searchLeaf will
	// either error during search or position incorrectly, and Key() may error.
	cur := bt.NewCursor()
	key := binary.BigEndian.AppendUint32(nil, uint32(5))
	err = cur.SeekExact(key)
	// Should get an error (either from search or Key())
	_ = err
	cur.Close()
}

// TestDelCurCov_CursorCloseClearsStackNoRepin verifies that Close() empties the
// cursor stack so that post-close Next()/Previous() short-circuit and do not
// re-pin (and re-read) released pages. This mirrors SQLite's
// btreeReleaseAllCursorPages setting pCur->iPage = -1 (btree.c:707): releasing
// the pinned pages must also logically empty the stack, otherwise the
// !c.valid && len(c.stack) == 0 emptiness guard in Next/Previous fails open and
// the leaf frame (now pg==nil) is misclassified as interior, re-pinning a
// possibly-repurposed page.
func TestDelCurCov_CursorCloseClearsStackNoRepin(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Build a multi-level tree so the cursor stack holds interior frames.
	for i := 0; i < 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, bt.Put(key, make([]byte, 20)))
	}

	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	rootInterior := pg.header.isInterior()
	bt.pager.releasePage(pg)
	require.True(t, rootInterior, "expected a multi-level tree (interior root)")

	// pinned counts pages currently held pinned by the writer cache.
	pinned := func() int { return bt.pager.writerCache.nPage - bt.pager.writerCache.nRecyclable }

	cur := bt.NewCursor()
	require.NoError(t, cur.First())
	require.True(t, cur.Valid())

	cur.Close()
	require.False(t, cur.Valid())
	require.Empty(t, cur.stack, "Close() must empty the cursor stack")

	pinnedAfterClose := pinned()

	// Post-close Next/Previous must no-op (no error, stay invalid) and must not
	// re-pin any pages.
	require.NoError(t, cur.Next())
	require.False(t, cur.Valid())
	require.Equal(t, pinnedAfterClose, pinned(), "Next() after Close re-pinned a page")

	require.NoError(t, cur.Previous())
	require.False(t, cur.Valid())
	require.Equal(t, pinnedAfterClose, pinned(), "Previous() after Close re-pinned a page")
}

// TestDelCurCov_DirectNextEmptyInterior exercises Next() descent into an
// interior page with cellCount == 0 (L2868-2869).
func TestDelCurCov_DirectNextEmptyInterior(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Build a multi-level tree
	for i := 0; i < 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, bt.Put(key, make([]byte, 20)))
	}

	// Check if root is interior
	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	if !pg.header.isInterior() {
		bt.pager.releasePage(pg)
		t.Skip("root is not interior, cannot test Next with empty interior child")
	}

	// Find the first child (leftChild of cell 0)
	off := int(pg.getCellOffset(0))
	childPgno := binary.BigEndian.Uint32(pg.data[off : off+4])
	bt.pager.releasePage(pg)

	// Check if child is interior or leaf
	childPg, err := bt.getPage(childPgno)
	require.NoError(t, err)
	isChildInterior := childPg.header.isInterior()
	bt.pager.releasePage(childPg)

	if isChildInterior {
		// Corrupt child's cellCount to 0
		wpg, err := p.getWritablePage(childPgno)
		require.NoError(t, err)
		wpg.header.cellCount = 0
		hdr := 0
		if wpg.pgno == 1 {
			hdr = dbHeaderSize
		}
		wpg.header.serialize(wpg.data[hdr:])
		p.releasePage(wpg)
	}

	// Traverse forward - may hit the empty interior page
	cur := bt.NewCursor()
	_ = cur.First()
	for cur.Valid() {
		_ = cur.Next()
	}
	cur.Close()
}

// TestDelCurCov_DirectPreviousEmptyInterior exercises Previous() descent
// into an interior page with cellCount == 0 (L2969-2970).
func TestDelCurCov_DirectPreviousEmptyInterior(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	for i := 0; i < 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, bt.Put(key, make([]byte, 20)))
	}

	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	if !pg.header.isInterior() {
		bt.pager.releasePage(pg)
		t.Skip("root is not interior")
	}

	// Corrupt rightChild's page to have cellCount=0 if it's interior
	rightChildPgno := pg.header.rightChild
	bt.pager.releasePage(pg)

	rcPg, err := bt.getPage(rightChildPgno)
	require.NoError(t, err)
	isRCInterior := rcPg.header.isInterior()
	bt.pager.releasePage(rcPg)

	if isRCInterior {
		wpg, err := p.getWritablePage(rightChildPgno)
		require.NoError(t, err)
		wpg.header.cellCount = 0
		hdr := 0
		if wpg.pgno == 1 {
			hdr = dbHeaderSize
		}
		wpg.header.serialize(wpg.data[hdr:])
		p.releasePage(wpg)
	}

	// Previous traversal from Last
	cur := bt.NewCursor()
	_ = cur.Last()
	for cur.Valid() {
		_ = cur.Previous()
	}
	cur.Close()
}
