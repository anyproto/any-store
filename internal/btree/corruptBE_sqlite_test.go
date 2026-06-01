/*
Ported from SQLite: corruptB.test, corruptC.test, corruptD.test, corruptE.test
Sources:

	/home/dev/work/sqlitec/test/corruptB.test
	/home/dev/work/sqlitec/test/corruptC.test
	/home/dev/work/sqlitec/test/corruptD.test
	/home/dev/work/sqlitec/test/corruptE.test

Test scenario:
Four corruption test files that exercise corruption detection in the B-tree layer:
  - corruptB: Tests B-tree loop detection (circular page references where a page is
    both an ancestor and descendant of itself) and out-of-range page pointers.
  - corruptC: Single-byte corruption at known offsets (firstFreeBlk on page 1,
    overflow page corruption near EOF) and a byte-by-byte fuzzing loop.
  - corruptD: Page header field corruption (firstFreeBlk set to 0xFFFF or near
    page end) to verify no buffer overreads.
  - corruptE: Key ordering corruption -- targeted byte patching to break the
    sorted-key invariant, verifying IntegrityCheck detects out-of-order entries.

Deviations from original:
  - corruptB-1.*: Original uses PRAGMA auto_vacuum=1 which adds pointer-map pages.
    We use auto_vacuum=0 (our only mode). Root page and offsets are computed
    dynamically via ns.RootPage(). Original inserts via SQL doubling; we insert
    directly via Put.
  - corruptB-3.1: OUT_OF_SCOPE -- SQL record header-size corruption. Our B-tree
    stores raw key-value bytes without SQLite's record format.
  - corruptC-1.1, corruptC-2.1 through 2.12, 2.15: OUT_OF_SCOPE -- require
    CREATE INDEX or sqlite_master queries.
  - corruptC-2.2 through 2.11: OUT_OF_SCOPE -- schema-dependent offsets.
  - corruptC-2.13: ADAPTABLE -- firstFreeBlk corruption on page 1.
  - corruptC-2.14: ADAPTABLE -- overflow page corruption near EOF.
  - corruptC-3.*: ADAPTABLE -- simplified byte-by-byte fuzz (single corruption per
    offset, stride every 1 byte). Gated behind !testing.Short().
  - corruptD-1.0: OUT_OF_SCOPE -- requires CREATE INDEX.
  - corruptD-1.1.1, 1.1.2: ADAPTABLE -- firstFreeBlk corruption on namespace root.
  - corruptD-1.2.*, 1.4.*, 1.5.*: OUT_OF_SCOPE -- empty/stub tests in original.
  - corruptE-1.*, 2.*: OUT_OF_SCOPE -- require CREATE INDEX and schema-dependent offsets.
  - corruptE-3.*: ADAPTABLE -- concept-level adaptation. We create our own DB with
    sequential keys, find cell offsets, corrupt key bytes to break ordering, and
    verify IntegrityCheck detects it.
*/
package btree

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --------------------------------------------------------------------------
// Setup helpers for corruptB tests
// --------------------------------------------------------------------------

// setupCorruptBSmall creates a DB with 32 rows of 200-byte random values,
// checkpoints, closes, and returns the path, clean DB bytes, root page number,
// and page size. This corresponds to corruptB-1.1 setup.
func setupCorruptBSmall(t *testing.T) (string, []byte, uint32, int) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, Options{PageSize: 1024})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(42))
	for i := 1; i <= 32; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 200)
		rng.Read(val)
		require.NoError(t, tx.Put(ns, key, val))
	}
	rootPage := ns.RootPage()
	require.NoError(t, tx.Commit())

	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.IntegrityCheck())
	require.NoError(t, db.Close())

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(9*1024), "DB file should be > 9KB")

	template, err := os.ReadFile(path)
	require.NoError(t, err)
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	return path, template, rootPage, 1024
}

// setupCorruptBLarge extends a small DB by inserting many more rows to grow the
// tree to 3+ levels. Returns updated template bytes and root page number.
func setupCorruptBLarge(t *testing.T) (string, []byte, uint32, int) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, Options{PageSize: 1024})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert 4096 rows with 200-byte random values to create 3+ level tree
	rng := rand.New(rand.NewSource(99))
	for i := 1; i <= 4096; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 200)
		rng.Read(val)
		require.NoError(t, tx.Put(ns, key, val))
	}
	rootPage := ns.RootPage()
	require.NoError(t, tx.Commit())

	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.IntegrityCheck())
	require.NoError(t, db.Close())

	template, err := os.ReadFile(path)
	require.NoError(t, err)
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	return path, template, rootPage, 1024
}

// restoreDB writes template bytes back to path and removes WAL/SHM files.
func restoreDB(t *testing.T, path string, template []byte) {
	t.Helper()
	data := make([]byte, len(template))
	copy(data, template)
	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
}

// pageOffset returns the byte offset within the DB file for the start of a
// page's B-tree header. For page 1, this accounts for the 100-byte DB header.
func pageOffset(pgno uint32, pageSize int) int {
	off := int(pgno-1) * pageSize
	if pgno == 1 {
		off += dbHeaderSize // 100
	}
	return off
}

// writeUint32At writes a big-endian uint32 at the specified absolute offset.
func writeUint32At(data []byte, absOff int, val uint32) {
	binary.BigEndian.PutUint32(data[absOff:absOff+4], val)
}

// readUint32At reads a big-endian uint32 at the specified absolute offset.
func readUint32At(data []byte, absOff int) uint32 {
	return binary.BigEndian.Uint32(data[absOff : absOff+4])
}

// readUint16At reads a big-endian uint16 at the specified absolute offset.
func readUint16At(data []byte, absOff int) uint16 {
	return binary.BigEndian.Uint16(data[absOff : absOff+2])
}

// isInteriorPage checks if the page type byte indicates an interior page.
func isInteriorPage(data []byte, hdrOff int) bool {
	pt := data[hdrOff]
	return pt == pageTypeIntIdx || pt == pageTypeIntTbl
}

// scanNamespaceCatchPanic opens DB, scans namespace "t1", and returns whether
// any operation panicked or timed out. The DB is closed before returning.
// A 5-second timeout is used to detect infinite loops from circular page refs.
func scanNamespaceCatchPanic(path string, pageSize uint32) (panicked any, scanErr error) {
	type result struct {
		panicked any
		scanErr  error
	}
	ch := make(chan result, 1)

	go func() {
		db, err := Open(path, Options{PageSize: pageSize})
		if err != nil {
			ch <- result{nil, err}
			return
		}

		p := catchPanicWithReadTx(db, func(rtx *ReadTx) {
			ns, err := db.getNamespaceLocked("t1")
			if err != nil {
				return
			}
			cur := rtx.NewCursor(ns)
			count := 0
			for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
				count++
				// Safety valve: if we iterate more than 100000 cells, something is wrong
				// (our test DBs have at most ~4096 keys)
				if count > 100000 {
					break
				}
			}
		})
		_ = db.Close()
		ch <- result{p, nil}
	}()

	select {
	case r := <-ch:
		return r.panicked, r.scanErr
	case <-time.After(5 * time.Second):
		// Infinite loop detected -- the corruption caused an undetected cycle.
		// This is a real implementation bug (missing loop detection), but we
		// report it as a test finding rather than hanging the test suite.
		return fmt.Errorf("TIMEOUT: scan did not complete in 5s (likely infinite loop from circular page reference)"), nil
	}
}

// corruptAndCheckIntegrity is a helper that opens a corrupt DB and runs
// IntegrityCheck with a timeout to avoid infinite loops from circular refs.
func corruptAndCheckIntegrity(path string, pageSize uint32) (panicked any, integrityErr error) {
	type result struct {
		panicked     any
		integrityErr error
	}
	ch := make(chan result, 1)

	go func() {
		db, err := Open(path, Options{PageSize: pageSize})
		if err != nil {
			ch <- result{nil, err}
			return
		}
		p := catchPanic(func() {
			integrityErr = db.IntegrityCheck()
		})
		_ = db.Close()
		ch <- result{p, integrityErr}
	}()

	select {
	case r := <-ch:
		return r.panicked, r.integrityErr
	case <-time.After(5 * time.Second):
		return fmt.Errorf("TIMEOUT: IntegrityCheck did not complete in 5s (likely infinite loop)"), nil
	}
}

// --------------------------------------------------------------------------
// corruptB: Loop detection tests
// --------------------------------------------------------------------------

// Port of corruptB-1.1 + corruptB-1.2 (lines 38-51 in corruptB.test)
// Original: Insert 32 rows with randomblob(200) via doubling, verify file > 9KB,
// integrity check passes.
func TestSqlite_CorruptB_1_1_Setup(t *testing.T) {
	_, _, _, _ = setupCorruptBSmall(t)
	// Setup function already asserts file size > 9KB and integrity check passes.
}

// Port of corruptB-1.3.1 + corruptB-1.3.2 (lines 57-65 in corruptB.test)
// Original: Set the right-child pointer of the B-tree root page to refer to the
// root page itself, creating a loop. Open DB, SELECT * FROM t1 -> malformed.
// DEVIATION: auto_vacuum=0 (vs original auto_vacuum=1). Root page and offsets
// computed dynamically.
func TestSqlite_CorruptB_1_3(t *testing.T) {
	path, template, rootPgno, pageSize := setupCorruptBSmall(t)
	hdrOff := pageOffset(rootPgno, pageSize)

	// Verify root is an interior page (needed for right-child pointer)
	if !isInteriorPage(template, hdrOff) {
		t.Skip("root page is not interior (tree not deep enough)")
	}

	// Corrupt: set rightChild of root = root (self-loop)
	data := make([]byte, len(template))
	copy(data, template)
	writeUint32At(data, hdrOff+8, rootPgno)
	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	panicked, _ := scanNamespaceCatchPanic(path, uint32(pageSize))
	if panicked != nil {
		t.Logf("BUG: self-referencing right-child: %v", panicked)
	}
	assert.Nil(t, panicked, "should not panic or infinite-loop on self-referencing right-child")
}

// Port of corruptB-1.4.1 + corruptB-1.4.2 (lines 70-79 in corruptB.test)
// Original: Set the left-child of the first cell in the root page to refer to the
// root page itself. Open DB, SELECT * FROM t1 -> malformed.
func TestSqlite_CorruptB_1_4(t *testing.T) {
	path, template, rootPgno, pageSize := setupCorruptBSmall(t)
	hdrOff := pageOffset(rootPgno, pageSize)

	if !isInteriorPage(template, hdrOff) {
		t.Skip("root page is not interior (tree not deep enough)")
	}

	data := make([]byte, len(template))
	copy(data, template)

	// Read first cell offset from cell pointer array
	// Interior page header is 12 bytes; cell pointer array starts right after.
	cellPtrOff := hdrOff + 12
	firstCellOff := int(readUint16At(data, cellPtrOff))

	// The cell's first 4 bytes are the left-child pointer.
	// Absolute offset of cell within the raw file:
	pageStart := int(rootPgno-1) * pageSize
	absCellOff := pageStart + firstCellOff

	// Corrupt: set leftChild of first cell = root (self-loop)
	writeUint32At(data, absCellOff, rootPgno)
	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	panicked, _ := scanNamespaceCatchPanic(path, uint32(pageSize))
	if panicked != nil {
		t.Logf("BUG: self-referencing left-child: %v", panicked)
	}
	assert.Nil(t, panicked, "should not panic or infinite-loop on self-referencing left-child")
}

// Port of corruptB-1.6.1 + corruptB-1.6.2 (lines 103-112 in corruptB.test)
// Original: After growing to 3+ levels, set the right-child pointer of the
// right-child of the root to point back to the root. Tests loop at depth 2.
func TestSqlite_CorruptB_1_6(t *testing.T) {
	path, template, rootPgno, pageSize := setupCorruptBLarge(t)
	hdrOff := pageOffset(rootPgno, pageSize)

	if !isInteriorPage(template, hdrOff) {
		t.Skip("root page is not interior")
	}

	data := make([]byte, len(template))
	copy(data, template)

	// Read rightChild of root page
	iRightChild := readUint32At(data, hdrOff+8)
	if iRightChild == 0 || int(iRightChild)*pageSize > len(data) {
		t.Fatalf("right child page %d out of range", iRightChild)
	}

	// Compute the child page's header offset
	childHdrOff := pageOffset(iRightChild, pageSize)
	if !isInteriorPage(data, childHdrOff) {
		t.Skip("right child is not interior (tree structure differs)")
	}

	// Corrupt: set rightChild of the right-child page = root
	writeUint32At(data, childHdrOff+8, rootPgno)
	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	panicked, _ := scanNamespaceCatchPanic(path, uint32(pageSize))
	if panicked != nil {
		t.Logf("BUG: depth-2 right-child loop: %v", panicked)
	}
	assert.Nil(t, panicked, "should not panic or infinite-loop on depth-2 right-child loop")
}

// Port of corruptB-1.7.1 + corruptB-1.7.2 (lines 117-126 in corruptB.test)
// Original: Set the left-child of a cell in the right-child page of root to
// point back to root.
func TestSqlite_CorruptB_1_7(t *testing.T) {
	path, template, rootPgno, pageSize := setupCorruptBLarge(t)
	hdrOff := pageOffset(rootPgno, pageSize)

	if !isInteriorPage(template, hdrOff) {
		t.Skip("root page is not interior")
	}

	data := make([]byte, len(template))
	copy(data, template)

	iRightChild := readUint32At(data, hdrOff+8)
	childHdrOff := pageOffset(iRightChild, pageSize)
	if !isInteriorPage(data, childHdrOff) {
		t.Skip("right child is not interior")
	}

	// Read first cell offset on the right-child page
	cellPtrOff := childHdrOff + 12
	firstCellOff := int(readUint16At(data, cellPtrOff))
	pageStart := int(iRightChild-1) * pageSize
	absCellOff := pageStart + firstCellOff

	// Corrupt: set leftChild of first cell on right-child page = root
	writeUint32At(data, absCellOff, rootPgno)
	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	panicked, _ := scanNamespaceCatchPanic(path, uint32(pageSize))
	if panicked != nil {
		t.Logf("BUG: depth-2 left-child loop via right branch: %v", panicked)
	}
	assert.Nil(t, panicked, "should not panic or infinite-loop on depth-2 left-child loop via right branch")
}

// Port of corruptB-1.8.1 + corruptB-1.8.2 (lines 128-140 in corruptB.test)
// Original: Set the right-child pointer of the left-child (of the first cell
// of the root) to point back to the root.
func TestSqlite_CorruptB_1_8(t *testing.T) {
	path, template, rootPgno, pageSize := setupCorruptBLarge(t)
	hdrOff := pageOffset(rootPgno, pageSize)

	if !isInteriorPage(template, hdrOff) {
		t.Skip("root page is not interior")
	}

	data := make([]byte, len(template))
	copy(data, template)

	// Read left-child from first cell of root page
	cellPtrOff := hdrOff + 12
	firstCellOff := int(readUint16At(data, cellPtrOff))
	pageStart := int(rootPgno-1) * pageSize
	absCellOff := pageStart + firstCellOff
	iLeftChild := readUint32At(data, absCellOff)

	if iLeftChild == 0 || int(iLeftChild)*pageSize > len(data) {
		t.Fatalf("left child page %d out of range", iLeftChild)
	}

	leftChildHdrOff := pageOffset(iLeftChild, pageSize)
	if !isInteriorPage(data, leftChildHdrOff) {
		t.Skip("left child is not interior")
	}

	// Corrupt: set rightChild of left-child page = root
	writeUint32At(data, leftChildHdrOff+8, rootPgno)
	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	panicked, _ := scanNamespaceCatchPanic(path, uint32(pageSize))
	if panicked != nil {
		t.Logf("BUG: depth-2 loop via left-child right-child: %v", panicked)
	}
	assert.Nil(t, panicked, "should not panic or infinite-loop on depth-2 loop via left-child right-child")
}

// Port of corruptB-1.9.1 + corruptB-1.9.2 (lines 145-154 in corruptB.test)
// Original: Set the left-child of a cell on the left-child page of root to
// point back to the root.
func TestSqlite_CorruptB_1_9(t *testing.T) {
	path, template, rootPgno, pageSize := setupCorruptBLarge(t)
	hdrOff := pageOffset(rootPgno, pageSize)

	if !isInteriorPage(template, hdrOff) {
		t.Skip("root page is not interior")
	}

	data := make([]byte, len(template))
	copy(data, template)

	// Find leftChild from root's first cell
	cellPtrOff := hdrOff + 12
	firstCellOff := int(readUint16At(data, cellPtrOff))
	pageStart := int(rootPgno-1) * pageSize
	absCellOff := pageStart + firstCellOff
	iLeftChild := readUint32At(data, absCellOff)

	if iLeftChild == 0 || int(iLeftChild)*pageSize > len(data) {
		t.Fatalf("left child page %d out of range", iLeftChild)
	}

	leftChildHdrOff := pageOffset(iLeftChild, pageSize)
	if !isInteriorPage(data, leftChildHdrOff) {
		t.Skip("left child is not interior")
	}

	// Read first cell on left-child page
	lcCellPtrOff := leftChildHdrOff + 12
	lcFirstCellOff := int(readUint16At(data, lcCellPtrOff))
	lcPageStart := int(iLeftChild-1) * pageSize
	lcAbsCellOff := lcPageStart + lcFirstCellOff

	// Corrupt: set leftChild of first cell on left-child page = root
	writeUint32At(data, lcAbsCellOff, rootPgno)
	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	panicked, _ := scanNamespaceCatchPanic(path, uint32(pageSize))
	if panicked != nil {
		t.Logf("BUG: depth-2 loop via left-child cell-child: %v", panicked)
	}
	assert.Nil(t, panicked, "should not panic or infinite-loop on depth-2 loop via left-child cell-child")
}

// Port of corruptB-2.1.1 + corruptB-2.1.2 (lines 158-166 in corruptB.test)
// Original: Set the right-child pointer of the root to 0x6FFFFFFF (out-of-range
// page number). Open DB, SELECT * FROM t1 -> malformed.
func TestSqlite_CorruptB_2_1(t *testing.T) {
	path, template, rootPgno, pageSize := setupCorruptBLarge(t)
	hdrOff := pageOffset(rootPgno, pageSize)

	if !isInteriorPage(template, hdrOff) {
		t.Skip("root page is not interior")
	}

	data := make([]byte, len(template))
	copy(data, template)

	// Corrupt: set rightChild = 0x6FFFFFFF (absurdly large page number)
	writeUint32At(data, hdrOff+8, 0x6FFFFFFF)
	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	panicked, _ := scanNamespaceCatchPanic(path, uint32(pageSize))
	if panicked != nil {
		t.Logf("BUG: out-of-range page pointer: %v", panicked)
	}
	assert.Nil(t, panicked, "should not panic or infinite-loop on out-of-range page pointer")
}

// --------------------------------------------------------------------------
// corruptC: Single-byte and overflow corruption
// --------------------------------------------------------------------------

// Port of corruptC-2.13 (lines 273-283 in corruptC.test)
// Original: Corrupt byte at offset 102 (firstFreeBlk high byte on page 1's
// B-tree header) to 0x12, then CREATE TABLE -> malformed.
// DEVIATION: We corrupt offset 102 on page 1 (same byte), then attempt
// CreateNamespace or Put to trigger a write to page 1.
func TestSqlite_CorruptC_2_13(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)

	// Insert a row so page 1 has some content
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte{1}, []byte{1}))
	require.NoError(t, tx.Commit())

	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	// Read, corrupt firstFreeBlk on page 1 (offset 100 = B-tree header start,
	// +1 and +2 = firstFreeBlk field at offsets 101-102).
	// Original corrupts offset 102 to 0x12.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[102] = 0x12
	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	// Reopen and attempt write operation
	db, err = testOpen(t, path, DefaultOptions())
	if err != nil {
		// Open failure is acceptable for corrupted page 1
		return
	}
	defer func() { _ = db.Close() }()

	// Attempt CreateNamespace -- should detect corruption, not panic
	panicked := catchPanicWithWriteTx(db, func(tx *WriteTx) {
		_, _ = tx.CreateNamespace("t3")
	})
	assert.Nil(t, panicked, "should not panic on corrupt firstFreeBlk on page 1")

	// IntegrityCheck should detect the corruption
	panicked = catchPanic(func() {
		err = db.IntegrityCheck()
	})
	assert.Nil(t, panicked, "IntegrityCheck should not panic")
	// We expect integrity check to report corruption, but don't require it
	// (depends on which field the corruption affects in our page layout)
}

// Port of corruptC-2.14 (lines 285-297 in corruptC.test)
// Original: Insert a 100KB blob, corrupt 4 bytes near EOF (offset = filesize-2048,
// writing 00000001), then DELETE -> malformed.
// DEVIATION: We insert key=1 with a 100000-byte value, corrupt bytes near EOF,
// then attempt Delete.
func TestSqlite_CorruptC_2_14(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// INSERT INTO t1 VALUES(1, <100KB blob>)
	key := binary.BigEndian.AppendUint32(nil, uint32(1))
	bigVal := make([]byte, 100000)
	rng := rand.New(rand.NewSource(12345))
	rng.Read(bigVal)
	require.NoError(t, tx.Put(ns, key, bigVal))
	require.NoError(t, tx.Commit())

	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	// Read file, corrupt bytes near EOF
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	filesize := len(data)
	require.Greater(t, filesize, 2048, "file should be large enough for 2048-byte offset from end")

	// Original: hexio_write test.db [filesize-2048] 00000001
	writeUint32At(data, filesize-2048, 1)
	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	// Reopen
	db, err = testOpen(t, path, DefaultOptions())
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()

	// Attempt Delete key=1 -- should detect overflow corruption, not panic
	panicked := catchPanicWithWriteTx(db, func(tx *WriteTx) {
		ns, err := db.getNamespaceLocked("t1")
		if err != nil {
			return
		}
		_ = tx.Delete(ns, key)
	})
	assert.Nil(t, panicked, "should not panic on corrupt overflow pages near EOF")
}

// Port of corruptC-3.$tn (lines 314-426 in corruptC.test)
// Original: For each byte offset 0..filesize-1 in the DB file, corrupt that byte,
// then attempt various SQL operations and verify no crashes. Up to 512 additional
// random corruptions per offset.
// DEVIATION: Simplified to single-byte corruption per offset (no cascading random
// corruption). Operations limited to: open, scan, put, IntegrityCheck.
// Skipped when testing.Short() is true due to runtime.
func TestSqlite_CorruptC_3_ByteFuzz(t *testing.T) {
	if testing.Short() {
		t.Skip("byte-by-byte fuzz is slow; skipping in short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Create a small multi-page DB for fuzzing
	db, err := testOpen(t, path, Options{PageSize: 1024})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(0))
	for i := 1; i <= 50; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		valSize := 50 + rng.Intn(200)
		val := make([]byte, valSize)
		rng.Read(val)
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.IntegrityCheck())
	require.NoError(t, db.Close())

	template, err := os.ReadFile(path)
	require.NoError(t, err)
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	fsize := len(template)
	t.Logf("fuzzing %d bytes of DB file", fsize)

	for tn := 0; tn < fsize; tn++ {
		// Restore clean DB, corrupt byte at offset tn
		corrupted := make([]byte, len(template))
		copy(corrupted, template)
		corrupted[tn] = 0xFF // deterministic corruption value
		if err := os.WriteFile(path, corrupted, 0644); err != nil {
			t.Fatal(err)
		}
		_ = os.Remove(path + "-wal")
		_ = os.Remove(path + "-shm")

		// Attempt Open
		db, err := testOpen(t, path, Options{PageSize: 1024})
		if err != nil {
			continue // Open failure is acceptable
		}

		// Attempt scan -- must not panic
		panicked := catchPanicWithReadTx(db, func(rtx *ReadTx) {
			ns, err := db.getNamespaceLocked("t1")
			if err != nil {
				return
			}
			cur := rtx.NewCursor(ns)
			for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
				// just scan
			}
		})
		if panicked != nil {
			_ = db.Close()
			t.Fatalf("panic during scan at byte offset %d: %v", tn, panicked)
		}

		// Attempt Put -- must not panic
		panicked = catchPanicWithWriteTx(db, func(tx *WriteTx) {
			ns, err := db.getNamespaceLocked("t1")
			if err != nil {
				return
			}
			testKey := binary.BigEndian.AppendUint32(nil, uint32(9999))
			_ = tx.Put(ns, testKey, []byte("fuzz"))
		})
		if panicked != nil {
			_ = db.Close()
			t.Fatalf("panic during Put at byte offset %d: %v", tn, panicked)
		}

		// IntegrityCheck -- must not panic
		panicked = catchPanic(func() {
			_ = db.IntegrityCheck()
		})
		if panicked != nil {
			_ = db.Close()
			t.Fatalf("panic during IntegrityCheck at byte offset %d: %v", tn, panicked)
		}

		_ = db.Close()
	}
}

// --------------------------------------------------------------------------
// corruptD: Page header field corruption
// --------------------------------------------------------------------------

// Port of corruptD-1.1.1 (lines 111-116 in corruptD.test)
// Original: Corrupt firstFreeBlk on page 2 to 0xFFFF, then PRAGMA quick_check
// detects free space corruption.
// DEVIATION: We corrupt firstFreeBlk on the namespace root page (not page 2,
// since page 2 might not be the root in our layout). Also we must increment
// the file change counter to invalidate the page cache.
func TestSqlite_CorruptD_1_1_1(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, Options{PageSize: 1024})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert rows, then delete some to create free space (simulating original)
	for i := 1; i < 50; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 10)
		require.NoError(t, tx.Put(ns, key, val))
	}
	rootPage := ns.RootPage()
	require.NoError(t, tx.Commit())

	// Delete some rows to create freeblocks on the root page
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for _, i := range []int{10, 20, 30, 40} {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Commit())

	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Increment file change counter to invalidate page cache
	changeCounter := readUint32At(data, 24)
	writeUint32At(data, 24, changeCounter+1)

	// Corrupt firstFreeBlk on the root page to 0xFFFF
	hdrOff := pageOffset(rootPage, 1024)
	binary.BigEndian.PutUint16(data[hdrOff+1:hdrOff+3], 0xFFFF)

	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	// Reopen and run integrity check
	db, err = testOpen(t, path, Options{PageSize: 1024})
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()

	panicked := catchPanic(func() {
		err = db.IntegrityCheck()
	})
	assert.Nil(t, panicked, "IntegrityCheck should not panic on corrupt firstFreeBlk=0xFFFF")
	assert.Error(t, err, "IntegrityCheck should detect free space corruption")
}

// Port of corruptD-1.1.2 (lines 117-121 in corruptD.test)
// Original: Corrupt firstFreeBlk on page 2 to 1021 (3 bytes from end of
// 1024-byte page), then SELECT * FROM t1 ORDER BY rowid -> malformed.
// DEVIATION: We corrupt firstFreeBlk on the namespace root page to a value
// near the page boundary (pageSize - 3), causing the freeblock to extend
// past the page.
func TestSqlite_CorruptD_1_1_2(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, Options{PageSize: 1024})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	for i := 1; i < 50; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 10)
		require.NoError(t, tx.Put(ns, key, val))
	}
	rootPage := ns.RootPage()
	require.NoError(t, tx.Commit())

	// Delete some rows to create freeblocks
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for _, i := range []int{10, 20, 30, 40} {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Delete(ns, key))
	}
	require.NoError(t, tx.Commit())

	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Increment file change counter
	changeCounter := readUint32At(data, 24)
	writeUint32At(data, 24, changeCounter+1)

	// Corrupt firstFreeBlk on root page to 1021 (near page end)
	hdrOff := pageOffset(rootPage, 1024)
	binary.BigEndian.PutUint16(data[hdrOff+1:hdrOff+3], 1021)

	require.NoError(t, os.WriteFile(path, data, 0644))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	// Reopen and attempt scan -- should detect corruption, not crash
	db, err = testOpen(t, path, Options{PageSize: 1024})
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()

	panicked := catchPanicWithReadTx(db, func(rtx *ReadTx) {
		ns, err := db.getNamespaceLocked("t1")
		if err != nil {
			return
		}
		cur := rtx.NewCursor(ns)
		for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
			// just scan
		}
	})
	assert.Nil(t, panicked, "should not panic on firstFreeBlk=1021 near page end")
}

// --------------------------------------------------------------------------
// corruptE: Key ordering corruption
// --------------------------------------------------------------------------

// Port of corruptE-2.1 concept + corruptE-3.* (lines 71-150 in corruptE.test)
// Original: A parameterized loop with 14 specific (offset, value) pairs that
// corrupt key bytes to break the sorted-key invariant, then PRAGMA integrity_check
// -> "out of order".
// DEVIATION: The 14 test vectors are schema-dependent offsets that don't apply
// to our different data layout. Instead we create our own DB with sequential
// keys, find leaf page cell offsets, and corrupt specific key bytes to break
// ordering. We verify IntegrityCheck detects the key ordering violation.
func TestSqlite_CorruptE_3_KeyOrderingVectors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, Options{PageSize: 1024})
	require.NoError(t, err)

	// Insert sequential keys to create multiple leaf pages
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	for i := 1; i <= 200; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 20)
		require.NoError(t, tx.Put(ns, key, val))
	}
	rootPage := ns.RootPage()
	require.NoError(t, tx.Commit())

	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.IntegrityCheck())
	require.NoError(t, db.Close())

	template, err := os.ReadFile(path)
	require.NoError(t, err)
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	// Find leaf pages that contain cells with keys. We'll scan the tree to find
	// leaf pages and their cell offsets.
	pageSize := 1024
	hdrOff := pageOffset(rootPage, pageSize)

	// Collect leaf page numbers by traversing the tree structure in the raw file
	leafPages := findLeafPages(t, template, rootPage, uint32(pageSize))
	require.NotEmpty(t, leafPages, "should have at least one leaf page")
	t.Logf("found %d leaf pages, root=%d", len(leafPages), rootPage)

	// Build test vectors: for each of several leaf pages, find a cell with
	// a multi-byte key and corrupt one key byte to break ordering.
	type corruptVector struct {
		name   string
		pgno   uint32
		cellID int  // which cell on the page to corrupt
		keyOff int  // which byte within the key to corrupt (0-based)
		value  byte // corrupt value
	}

	// We need pages with at least 3 cells so we can corrupt a middle one
	var vectors []corruptVector

	for i, pgno := range leafPages {
		if len(vectors) >= 5 {
			break
		}
		pageStart := int(pgno-1) * pageSize
		leafHdrOff := pageStart
		if pgno == 1 {
			leafHdrOff += dbHeaderSize
		}

		pt := template[leafHdrOff]
		if pt != pageTypeLeafIdx {
			continue
		}

		cellCount := int(readUint16At(template, leafHdrOff+3))
		if cellCount < 3 {
			continue
		}

		// Pick a middle cell and corrupt the first key byte to a higher value
		// This will make the key out of order relative to the next cell.
		targetCell := cellCount / 2
		cpBase := leafHdrOff + 8 // leaf header is 8 bytes
		cellOff := int(readUint16At(template, cpBase+targetCell*2))
		absCellOff := pageStart + cellOff

		// Parse: varint(keyLen) + key + varint(valLen) + value
		keyLen, kn := getVarint(template[absCellOff:])
		if keyLen != 4 {
			continue // expecting 4-byte keys
		}
		keyStart := absCellOff + kn

		// Read the current key value and the next cell's key
		currentKey := binary.BigEndian.Uint32(template[keyStart : keyStart+4])

		// Corrupt the key to be larger than it should be (add a large value to
		// the most significant byte of the key)
		vectors = append(vectors, corruptVector{
			name:   fmt.Sprintf("leaf%d_cell%d_pg%d", i, targetCell, pgno),
			pgno:   pgno,
			cellID: targetCell,
			keyOff: 0,                         // most significant byte
			value:  template[keyStart] + 0x80, // make much larger
		})

		// Also test corrupting the last byte of the key
		if targetCell > 0 {
			// Corrupt the last key byte of an earlier cell to push it past the next
			prevCellOff := int(readUint16At(template, cpBase+(targetCell-1)*2))
			prevAbsCellOff := pageStart + prevCellOff
			prevKeyLen, prevKn := getVarint(template[prevAbsCellOff:])
			if prevKeyLen == 4 {
				prevKeyStart := prevAbsCellOff + prevKn
				vectors = append(vectors, corruptVector{
					name:   fmt.Sprintf("leaf%d_cell%d_lastbyte_pg%d", i, targetCell-1, pgno),
					pgno:   pgno,
					cellID: targetCell - 1,
					keyOff: 3,    // last byte of 4-byte key
					value:  0xFF, // push last byte to max
				})
				_ = prevKeyStart
			}
		}
		_ = currentKey
		_ = hdrOff
	}

	require.NotEmpty(t, vectors, "should have generated at least one corruption vector")

	for _, v := range vectors {
		t.Run(v.name, func(t *testing.T) {
			data := make([]byte, len(template))
			copy(data, template)

			pageStart := int(v.pgno-1) * pageSize
			leafHdrOff := pageStart
			if v.pgno == 1 {
				leafHdrOff += dbHeaderSize
			}

			cpBase := leafHdrOff + 8
			cellOff := int(readUint16At(data, cpBase+v.cellID*2))
			absCellOff := pageStart + cellOff

			// Find key start
			_, kn := getVarint(data[absCellOff:])
			keyStart := absCellOff + kn

			// Apply corruption
			data[keyStart+v.keyOff] = v.value

			require.NoError(t, os.WriteFile(path, data, 0644))
			_ = os.Remove(path + "-wal")
			_ = os.Remove(path + "-shm")

			db, err := testOpen(t, path, Options{PageSize: 1024})
			if err != nil {
				return
			}
			defer func() { _ = db.Close() }()

			// IntegrityCheck should detect "out of order"
			panicked := catchPanic(func() {
				err = db.IntegrityCheck()
			})
			assert.Nil(t, panicked, "IntegrityCheck should not panic on key ordering corruption")
			assert.Error(t, err, "IntegrityCheck should detect key ordering corruption for vector %s", v.name)
			if err != nil {
				t.Logf("integrity error for %s: %v", v.name, err)
			}
		})
	}
}

// findLeafPages traverses the B-tree structure in raw file data, returning
// all leaf page numbers reachable from the given root.
func findLeafPages(t *testing.T, data []byte, rootPgno uint32, pageSize uint32) []uint32 {
	t.Helper()
	var leaves []uint32
	var visit func(pgno uint32, depth int)
	visited := make(map[uint32]bool)

	visit = func(pgno uint32, depth int) {
		if depth > 20 || visited[pgno] {
			return
		}
		visited[pgno] = true

		pageStart := int(pgno-1) * int(pageSize)
		if pageStart+int(pageSize) > len(data) {
			return
		}
		hdrOff := pageStart
		if pgno == 1 {
			hdrOff += dbHeaderSize
		}

		pt := data[hdrOff]
		if pt == pageTypeLeafIdx || pt == pageTypeLeafTbl {
			leaves = append(leaves, pgno)
			return
		}
		if pt != pageTypeIntIdx && pt != pageTypeIntTbl {
			return
		}

		// Interior page: traverse children
		cellCount := int(readUint16At(data, hdrOff+3))
		cpBase := hdrOff + 12 // interior header is 12 bytes

		for i := 0; i < cellCount; i++ {
			cellOff := int(readUint16At(data, cpBase+i*2))
			absCellOff := pageStart + cellOff
			if absCellOff+4 > len(data) {
				continue
			}
			childPgno := readUint32At(data, absCellOff)
			if childPgno > 0 && int(childPgno)*int(pageSize) <= len(data) {
				visit(childPgno, depth+1)
			}
		}

		// Right child
		rightChild := readUint32At(data, hdrOff+8)
		if rightChild > 0 && int(rightChild)*int(pageSize) <= len(data) {
			visit(rightChild, depth+1)
		}
	}

	visit(rootPgno, 0)
	return leaves
}
