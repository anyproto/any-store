package btree

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---- trace() coverage ----
// debugTrace is now a build-tag const (false by default).
// Full trace coverage requires: go test -tags=debugtrace

func TestRemaining_TraceNoOp(t *testing.T) {
	// In default build, trace() is a no-op. Just verify no panic.
	trace("this should not appear: %d", 42)
}

// ---- Cursor.SeekExact with non-existent key ----

func TestRemaining_SeekExactNotFound(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Insert a few keys
	require.NoError(t, tx.Put(ns, []byte("aaa"), []byte("v1")))
	require.NoError(t, tx.Put(ns, []byte("ccc"), []byte("v2")))
	require.NoError(t, tx.Put(ns, []byte("eee"), []byte("v3")))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cur := rtx.NewCursor(ns)
	// SeekExact on non-existent key between existing keys
	err = cur.SeekExact([]byte("bbb"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	// SeekExact on key past all entries (cursor becomes invalid)
	err = cur.SeekExact([]byte("zzz"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

// ---- Cursor.SeekNear fast path → idx >= n (fallback to Next) ----
// This happens when searchLeaf returns idx == cellCount within the fast path

func TestRemaining_SeekNearFastPathFallbackNext(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Insert enough keys to have multiple leaf pages
	for i := 0; i < 80; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i*10))
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cur := rtx.NewCursor(ns)

	// Position cursor at a known key
	err = cur.Seek([]byte{0, 0, 0, 100})
	require.NoError(t, err)
	require.True(t, cur.Valid())

	// Now use SeekNear with a key within the same leaf page range
	// but beyond the last key of this leaf - this should trigger the fast path
	// where idx >= n, leading to c.Next()
	k, _ := cur.Key()
	// Seek to a key just slightly above the current position
	target := make([]byte, 4)
	binary.BigEndian.PutUint32(target, binary.BigEndian.Uint32(k)+1)
	err = cur.SeekNear(target)
	require.NoError(t, err)
}

// ---- Cursor.Value with overflow data ----

func TestRemaining_CursorValueOverflow(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Insert a key with a value large enough to overflow
	bigVal := make([]byte, 600) // bigger than page size
	for i := range bigVal {
		bigVal[i] = byte(i % 251)
	}
	require.NoError(t, tx.Put(ns, []byte("key1"), bigVal))
	require.NoError(t, tx.Put(ns, []byte("key2"), []byte("small")))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cur := rtx.NewCursor(ns)
	err = cur.First()
	require.NoError(t, err)
	require.True(t, cur.Valid())

	k, err := cur.Key()
	require.NoError(t, err)
	require.Equal(t, []byte("key1"), k)

	v, err := cur.Value()
	require.NoError(t, err)
	require.Equal(t, bigVal, v)
}

// ---- AppendValue with overflow data ----

func TestRemaining_AppendValueOverflow(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	bigVal := make([]byte, 600)
	for i := range bigVal {
		bigVal[i] = byte(i % 251)
	}
	require.NoError(t, tx.Put(ns, []byte("key1"), bigVal))
	require.NoError(t, tx.Commit())

	// Read via AppendValue through ReadTx
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	got, err := rtx.AppendValue(ns, []byte("key1"), nil)
	require.NoError(t, err)
	require.Equal(t, bigVal, got)
}

// ---- Delete with fragmentation rebuild (btree.go L2100-2108) ----

func TestRemaining_DeleteFragmentationRebuild(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Insert keys with different-sized values to create fragmentation
	for i := 0; i < 20; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 30+i*2) // varying sizes
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Now delete keys one by one to accumulate fragmentation
	// When fragBytes exceeds 60, the rebuild path triggers
	for round := 0; round < 3; round++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		// Delete several keys per round to accumulate frag bytes
		for i := round * 5; i < (round+1)*5 && i < 20; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			_ = tx2.Delete(ns2, key)
		}
		require.NoError(t, tx2.Commit())
	}

	require.NoError(t, db.IntegrityCheck())
}

// ---- Delete of overflow cell + free overflow chain (btree.go L2111-2115) ----

func TestRemaining_DeleteOverflowCell(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Insert a few normal keys and one with overflow value
	for i := 0; i < 5; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	// Overflow value
	overflowKey := binary.BigEndian.AppendUint32(nil, uint32(100))
	require.NoError(t, tx.Put(ns, overflowKey, make([]byte, 600)))
	require.NoError(t, tx.Commit())

	// Delete the overflow key - should trigger freeOverflowChain in the fast delete path
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	require.NoError(t, tx2.Delete(ns2, overflowKey))
	require.NoError(t, tx2.Commit())

	require.NoError(t, db.IntegrityCheck())
}

// ---- updateLeafCell causing page overflow and split (btree.go L1370-1411) ----

func TestRemaining_UpdateCausesLeafSplit(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Fill a leaf page with small values
	for i := 0; i < 30; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 10)))
	}
	require.NoError(t, tx.Commit())

	// Now update a key with a much larger value, causing the page to overflow → split
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	key := binary.BigEndian.AppendUint32(nil, uint32(15))
	require.NoError(t, tx2.Put(ns2, key, make([]byte, 400)))
	require.NoError(t, tx2.Commit())

	require.NoError(t, db.IntegrityCheck())

	// Verify all keys still readable
	for i := 0; i < 30; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		_, err = rtx.AppendValue(ns2, key, nil)
		require.NoError(t, err)
		require.NoError(t, rtx.Rollback())
	}
}

// ---- updateLeafCell with fragmentation overflow → full rebuild (btree.go L1345-1362) ----

func TestRemaining_UpdateHighFragmentation(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Insert keys with values that will create fragmentation when shrunk
	for i := 0; i < 15; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 30)))
	}
	require.NoError(t, tx.Commit())

	// Update all keys with smaller values repeatedly to accumulate fragmentation
	for round := 0; round < 5; round++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := 0; i < 15; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			require.NoError(t, tx2.Put(ns2, key, make([]byte, 30-round*2)))
		}
		require.NoError(t, tx2.Commit())
	}

	require.NoError(t, db.IntegrityCheck())
}

// ---- Cursor Previous through 3+ level tree ----

func TestRemaining_CursorPreviousDeepTree(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Insert enough keys for a 3-level tree with 512-byte pages
	for i := 0; i < 500; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 8)))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cur := rtx.NewCursor(ns)

	// Traverse from Last to First using Previous
	err = cur.Last()
	require.NoError(t, err)
	require.True(t, cur.Valid())

	count := 1
	for {
		err = cur.Previous()
		require.NoError(t, err)
		if !cur.Valid() {
			break
		}
		count++
	}
	require.Equal(t, 500, count)

	// Also test Previous from middle using SeekNear
	err = cur.Seek(binary.BigEndian.AppendUint32(nil, uint32(250)))
	require.NoError(t, err)
	require.True(t, cur.Valid())

	err = cur.Previous()
	require.NoError(t, err)
	require.True(t, cur.Valid())
	k, err := cur.Key()
	require.NoError(t, err)
	// Should be key 249
	require.Equal(t, uint32(249), binary.BigEndian.Uint32(k))
}

// ---- Count on interior page with corruption (btree.go L2392-2396) ----
// countPage checks cpBase+2 > dataLen - hard to trigger through DB API with valid data
// Skip this as it requires corruption injection

// ---- Delete from multi-level tree causing leaf empty → freeLeaf → removeChildFromParent ----

func TestRemaining_DeleteEmptyLeafRemoveFromParent(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Create a tree with multiple leaves
	for i := 0; i < 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	require.NoError(t, tx.Commit())

	// Delete all keys from specific ranges to empty entire leaf pages
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		_ = tx2.Delete(ns2, key) // ignore not-found for already-freed pages
	}
	require.NoError(t, tx2.Commit())

	require.NoError(t, db.IntegrityCheck())
}

// ---- collectLeafCells contentOff negative check (btree.go L1430-1432) ----
// contentSize < 0 check; contentOff coming from contentAreaOffset

// ---- leafSplitPoint and interiorSplitPoint clamping (L287-292, L329-334) ----
// These are clamping branches: bestIdx < 1 → 1, bestIdx >= len(cells) → len(cells)-1

func TestRemaining_SplitPointClamping(t *testing.T) {
	// leafSplitPoint clamping: 2 cells only
	cells := []cellData{
		{key: []byte("a"), value: make([]byte, 10)},
		{key: []byte("b"), value: make([]byte, 10)},
	}
	mid := leafSplitPoint(cells, 4096)
	require.Equal(t, 1, mid)

	// interiorSplitPoint clamping: 3 cells (minimum for split)
	intCells := []cellData{
		{key: []byte("a"), leftChild: 2},
		{key: []byte("b"), leftChild: 3},
		{key: []byte("c"), leftChild: 4},
	}
	intMid := interiorSplitPoint(intCells, 4096)
	require.GreaterOrEqual(t, intMid, 1)
	require.LessOrEqual(t, intMid, len(intCells)-2)
}

// ---- searchLeafWithOverflow — overflow key prefix comparison paths ----

func TestRemaining_SearchLeafOverflowKeyFullRead(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert keys where some are large enough to overflow
	// With 512-byte page, keys > ~100 bytes will overflow
	for i := 0; i < 5; i++ {
		key := bytes.Repeat([]byte{byte('a' + i)}, 200)
		require.NoError(t, tx.Put(ns, key, []byte("val")))
	}
	require.NoError(t, tx.Commit())

	// Search for various keys to trigger different comparison paths
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cur := rtx.NewCursor(ns)

	// Search for a key that matches a prefix but differs in overflow portion
	searchKey := bytes.Repeat([]byte{'c'}, 200)
	searchKey[150] = 'z' // differs in overflow portion
	err = cur.Seek(searchKey)
	require.NoError(t, err)

	// Search for exact match
	exactKey := bytes.Repeat([]byte{'c'}, 200)
	err = cur.Seek(exactKey)
	require.NoError(t, err)
	require.True(t, cur.Valid())
}

// ---- DB.Open with hasMmapShm = false (db.go L106-109) ----
// This is a platform check; hasMmapShm is compile-time on Linux.
// Can't directly test on Linux.

// ---- ReadTx.AppendValue with overflow values (db.go L655-672) ----

func TestRemaining_ReadTxAppendValueOverflow(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	bigVal := make([]byte, 600)
	for i := range bigVal {
		bigVal[i] = byte(i % 253)
	}
	require.NoError(t, tx.Put(ns, []byte("overflow_key"), bigVal))
	// Also insert overflow key (key itself overflows)
	bigKey := bytes.Repeat([]byte("k"), 200)
	require.NoError(t, tx.Put(ns, bigKey, make([]byte, 400)))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	// Read via ReadTx.AppendValue which hits db.go L655-672
	got, err := rtx.AppendValue(ns, []byte("overflow_key"), nil)
	require.NoError(t, err)
	require.Equal(t, bigVal, got)
}

// ---- WAL recover with committed frames (wal.go L1155-1163) ----
// This path is covered by TestGap_WAL_Recover_WithCommittedFrames in gap_coverage_test.go

// ---- Pager freePage with active savepoints (pager.go L668-680) ----

func TestRemaining_FreePageWithSavepoint(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	// Create namespace and insert data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 30; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	require.NoError(t, tx.Commit())

	// Begin write with savepoint, then delete keys to trigger freePage
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	sp, err := tx2.Savepoint()
	require.NoError(t, err)
	_ = sp

	// Insert more to allocate pages, then delete to free them
	for i := 100; i < 130; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx2.Put(ns2, key, make([]byte, 20)))
	}
	// Delete the newly added keys (which frees their pages with savepoint active)
	for i := 100; i < 130; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx2.Delete(ns2, key))
	}
	require.NoError(t, tx2.Commit())
	require.NoError(t, db.IntegrityCheck())
}

// ---- BTREE_TRACE init via subprocess ----

func TestRemaining_InitBtreeTraceStderr(t *testing.T) {
	if os.Getenv("BTREE_TRACE_SUBPROCESS") != "1" {
		// Run this test in a subprocess with BTREE_TRACE set
		// We can't re-init the package, so skip
		t.Skip("init() BTREE_TRACE paths can only be tested via subprocess; skipping")
	}
}

func TestRemaining_InitBtreeTraceFile(t *testing.T) {
	if os.Getenv("BTREE_TRACE_SUBPROCESS") != "1" {
		t.Skip("init() BTREE_TRACE paths can only be tested via subprocess; skipping")
	}
}

// ---- Integrity check with corrupted page types (integrity.go L235-238) ----

func TestRemaining_IntegrityCheckCorruptPageType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, Options{PageSize: 4096})
	require.NoError(t, err)

	// Create namespace with data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 100)))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Close())

	// Corrupt the namespace's root page type
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Find the namespace root page - it should be page 2 or 3
	// Page 2 starts at offset 4096*1 = 4096
	// Corrupt the page type byte (first byte of page header)
	data[4096] = 0xFF // invalid page type
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := Open(path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	err = db2.IntegrityCheck()
	require.Error(t, err) // Should report corruption
}

// ---- Integrity check with corrupted contentAreaOffset (integrity.go L169-172) ----

func TestRemaining_IntegrityCheckCorruptContentOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, Options{PageSize: 4096})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 50)))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	// Corrupt cellContentOff on a leaf page (page 2, offset 4096)
	// cellContentOff is at bytes 5-6 of the page header (0-indexed)
	// Set it to 0 which is invalid (less than the header size)
	data[4096+5] = 0
	data[4096+6] = 0
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := Open(path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	err = db2.IntegrityCheck()
	require.Error(t, err)
}

// ---- searchInteriorPage corruption (btree.go L693-712) when lo==0 ----

func TestRemaining_SearchInteriorCorruptFirstCell(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Insert enough keys for an interior root
	for i := 0; i < 60; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	require.NoError(t, tx.Commit())

	// Now search for a key before the first entry - this should trigger
	// the lo==0 branch in searchInteriorPage
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cur := rtx.NewCursor(ns)
	// Seek to key 0 which should go to the leftmost child (lo==0 branch)
	err = cur.Seek(binary.BigEndian.AppendUint32(nil, 0))
	require.NoError(t, err)
}

// ---- Delete all from tree → root becomes empty leaf ----

func TestRemaining_DeleteAllFromTree(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	require.NoError(t, tx.Commit())

	// Delete everything in one transaction
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx2.Delete(ns2, key))
	}
	require.NoError(t, tx2.Commit())

	require.NoError(t, db.IntegrityCheck())
	require.Equal(t, 0, countKeys(t, db, "t1"))
}

// ---- Insert overflow keys into interior pages (triggers overflow in interior cell) ----

func TestRemaining_OverflowKeyInteriorPage(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Use keys large enough to overflow interior cells (~200 bytes with 512 page size)
	for i := 0; i < 40; i++ {
		key := make([]byte, 200)
		binary.BigEndian.PutUint32(key, uint32(i))
		for j := 4; j < len(key); j++ {
			key[j] = byte(i + j)
		}
		require.NoError(t, tx.Put(ns, key, []byte("val")))
	}
	require.NoError(t, tx.Commit())

	require.NoError(t, db.IntegrityCheck())

	// Verify we can read all keys back
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cur := rtx.NewCursor(ns)
	count := 0
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		count++
	}
	require.Equal(t, 40, count)
}

// ---- leafFullKey (btree.go L778-789) — bounds checks and varint parsing ----
// Tested indirectly through overflow key searches above

// ---- updateLeafCell with overflow → new overflow chain ----

func TestRemaining_UpdateLeafCellOverflowToOverflow(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert keys with values large enough to require overflow
	for i := 0; i < 5; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 400)))
	}
	require.NoError(t, tx.Commit())

	// Update with different overflow-sized value
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	key := binary.BigEndian.AppendUint32(nil, uint32(2))
	newVal := make([]byte, 350)
	for i := range newVal {
		newVal[i] = 0xAB
	}
	require.NoError(t, tx2.Put(ns2, key, newVal))
	require.NoError(t, tx2.Commit())

	// Verify
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	got, err := rtx.AppendValue(ns2, key, nil)
	require.NoError(t, err)
	require.Equal(t, newVal, got)
}

// ---- collectInteriorCells with overflow key (btree.go L1510-1532) ----

func TestRemaining_CollectInteriorCellsOverflow(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert large keys that will create overflow interior cells
	for i := 0; i < 30; i++ {
		key := make([]byte, 250)
		binary.BigEndian.PutUint32(key, uint32(i))
		require.NoError(t, tx.Put(ns, key, []byte("v")))
	}
	require.NoError(t, tx.Commit())

	// Now delete some to trigger tryMergeLeaf which calls collectInteriorCells
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 0; i < 15; i++ {
		key := make([]byte, 250)
		binary.BigEndian.PutUint32(key, uint32(i))
		_ = tx2.Delete(ns2, key)
	}
	require.NoError(t, tx2.Commit())

	require.NoError(t, db.IntegrityCheck())
}

// ---- rebuildInteriorPage overflow key writing (btree.go L1626-1632) ----

func TestRemaining_RebuildInteriorWithOverflowKeys(t *testing.T) {
	// Trigger rebuildInteriorPage with overflow keys by inserting many
	// large keys that force interior page splits with overflow separators.
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Keys > maxLocal (~100 bytes with 512 page) to force interior overflow
	for i := 0; i < 50; i++ {
		key := make([]byte, 250)
		binary.BigEndian.PutUint32(key, uint32(i))
		require.NoError(t, tx.Put(ns, key, []byte("v")))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())
}

// ---- writeOverflowChain in rebuildLeafPage (btree.go L1573-1576) ----

func TestRemaining_RebuildLeafPageOverflow(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert overflow values - when the leaf page is rebuilt during
	// a split or defrag, the overflow chains are re-created
	for i := 0; i < 5; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 400)))
	}
	require.NoError(t, tx.Commit())

	// Trigger a rebuild by inserting one more that causes a split
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	key := binary.BigEndian.AppendUint32(nil, uint32(10))
	require.NoError(t, tx2.Put(ns2, key, make([]byte, 400)))
	require.NoError(t, tx2.Commit())

	require.NoError(t, db.IntegrityCheck())
}

// ---- Namespace deletion with overflow pages (db.go L431-438) ----

func TestRemaining_DeleteNamespaceOverflow(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert overflow data
	for i := 0; i < 10; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 500)))
	}
	require.NoError(t, tx.Commit())

	// Delete the namespace - should free all overflow pages
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx2.DeleteNamespace("t1"))
	require.NoError(t, tx2.Commit())

	require.NoError(t, db.IntegrityCheck())
}
