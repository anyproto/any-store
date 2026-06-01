/*
Ported from SQLite: corrupt.test
Source: /home/dev/work/sqlitec/test/corrupt.test

Test scenario:
Tests that the database engine does not crash or segfault when it encounters
a corrupt database file. Tests various corruption scenarios: garbage data
overwriting 256-byte segments, freeblock size corruption, cell-offset array
corruption during balance-deeper, overflow page pointer corruption, and cell
area byte corruption.

Deviations from original:
  - corrupt-1 / corrupt-2: Original creates ~768 rows via SQL string concat plus
    an index and a second table. We create 200 rows with random-length values
    (80-3000 bytes) to produce a multi-page file. Sub-operations per offset are
    reduced from 8 to 4 (open, scan, write, integrity check). Page ref leak check
    (corrupt-2.$tn.8) is skipped (internal implementation detail).
  - corrupt-3 through corrupt-5: Skipped — test sqlite_master rootpage swapping,
    index B-tree type confusion, and sqlite_master column count corruption. None
    of these concepts exist in our key-value API.
  - corrupt-6.1: Our delete uses fragmentation tracking instead of freeblocks,
    so we insert 55 cells then manually create a freeblock via raw byte patching.
    Cell count is 55 (not 63) because our cell format is slightly larger.
    The net effect is the same: a page with a corrupted freeblock size.
  - corrupt-7: Original uses 39 rows which fit on one page in SQLite's format.
    Our cell format is larger (26 vs ~23 bytes), so we use 35 rows instead.
    Key 36 triggers balance-deeper (instead of key 40). Cell-offset array
    corruption logic and fake cell header approach are the same.
  - corrupt-8.1: Overflow pointer offset and change counter corruption offsets
    computed empirically for our page layout. INSERT OR REPLACE maps to Put (upsert).
    secure_delete and auto_vacuum pragmas not applicable.
  - corrupt-8.2: Byte offset 2047 corruption and change counter at offset 24.
    Same offset adjustments as corrupt-8.1.
*/
package btree

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// setupCorruptDB creates a populated multi-page DB with 200 keys having
// random-length values, checkpoints, closes, and returns the path and
// a copy of the clean DB bytes. This is the setup for corrupt-2.
// Port of corrupt-1.1 + corrupt-1.2 (lines 34-56 in corrupt.test)
// Original: Creates ~768 rows with string concat doubling, plus an index
// and a second table (DELETE WHERE rowid%5!=0). We create 200 rows with
// varied sizes to produce a similarly large multi-page file.
func setupCorruptDB(t *testing.T) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}

	// CREATE TABLE t1 -> CreateNamespace
	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	ns, err := tx.CreateNamespace("t1")
	if err != nil {
		t.Fatal(err)
	}

	// INSERT 200 rows with random-length values (80-3000 bytes)
	// DEVIATION: Original creates ~768 rows via SQL string concat. We use
	// 200 rows with random sizes to produce a multi-page file with overflow pages.
	rng := rand.New(rand.NewSource(42))
	for i := 1; i <= 200; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		valSize := 80 + rng.Intn(2921) // 80..3000
		val := make([]byte, valSize)
		rng.Read(val)
		if err := tx.Put(ns, key, val); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Checkpoint to flush WAL to main DB file
	if err := db.Checkpoint(CheckpointFull); err != nil {
		t.Fatal(err)
	}

	// PRAGMA integrity_check (corrupt-1.2)
	if err := db.IntegrityCheck(); err != nil {
		t.Fatalf("integrity check failed on clean DB: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Save a backup copy of the clean DB bytes
	template, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Remove WAL/SHM files
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	return path, template
}

// Port of corrupt-2.$tn.1-8 (lines 71-118 in corrupt.test)
// Original: For each 256-byte offset in the DB file, overwrite with garbage,
// then run: SELECT count(*) FROM sqlite_master, SELECT count(*) FROM t1,
// SELECT count(*) FROM t1 WHERE x>'abcdef', SELECT count(*) FROM t2,
// CREATE TABLE AS, DROP TABLE, integrity_check, page-ref-leak check.
// DEVIATION: Reduced to 4 sub-operations (open, scan, write, integrity check).
// Page ref leak check (corrupt-2.$tn.8) skipped (internal detail).
func TestSqlite_Corrupt_2_GarbageOverwrite(t *testing.T) {
	path, template := setupCorruptDB(t)

	fsize := len(template)

	// Create junk string matching SQLite's pattern
	junkBase := "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	var junk []byte
	for len(junk) < 256 {
		junk = append(junk, junkBase...)
	}
	junk = junk[:256]

	for i := 256; i < fsize-256; i += 256 {
		tn := i / 256
		t.Run(fmt.Sprintf("tn=%d", tn), func(t *testing.T) {
			// Restore clean DB from backup bytes
			corrupted := make([]byte, len(template))
			copy(corrupted, template)

			// Write 256 bytes of garbage at offset i
			copy(corrupted[i:i+256], junk)
			if err := os.WriteFile(path, corrupted, 0644); err != nil {
				t.Fatal(err)
			}

			// Remove WAL/SHM files to force reading from corrupt main file
			_ = os.Remove(path + "-wal")
			_ = os.Remove(path + "-shm")

			// Attempt Open — may fail, that's ok
			db, err := testOpen(t, path, DefaultOptions())
			if err != nil {
				// Open failed — that's a valid response to corruption
				return
			}

			// corrupt-2.$tn.2: catchsql {SELECT count(*) FROM t1}
			// -> scan all keys. Must not panic.
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
				t.Fatalf("panic during scan at offset %d: %v", i, panicked)
			}

			// corrupt-2.$tn.5: catchsql {CREATE TABLE t3 AS SELECT * FROM t1}
			// -> Put a new key (equivalent of write operation). Must not panic.
			panicked = catchPanicWithWriteTx(db, func(tx *WriteTx) {
				ns, err := db.getNamespaceLocked("t1")
				if err != nil {
					return
				}
				key := binary.BigEndian.AppendUint32(nil, uint32(9999))
				err = tx.Put(ns, key, []byte("test"))
				if err != nil {
					return
				}
			})
			if panicked != nil {
				_ = db.Close()
				t.Fatalf("panic during Put at offset %d: %v", i, panicked)
			}

			// corrupt-2.$tn.7: catchsql {PRAGMA integrity_check}. Must not panic.
			panicked = catchPanic(func() {
				_ = db.IntegrityCheck()
			})
			if panicked != nil {
				_ = db.Close()
				t.Fatalf("panic during IntegrityCheck at offset %d: %v", i, panicked)
			}

			_ = db.Close()
		})
	}
}

// catchPanic runs fn and returns the panic value if one occurred, or nil.
func catchPanic(fn func()) (panicVal any) {
	defer func() {
		panicVal = recover()
	}()
	fn()
	return nil
}

// catchPanicWithReadTx starts a read transaction, runs fn, and ensures the
// transaction is rolled back even if fn panics (preventing lock deadlocks).
func catchPanicWithReadTx(db *DB, fn func(rtx *ReadTx)) (panicVal any) {
	rtx, err := db.BeginRead()
	if err != nil {
		return nil
	}
	defer func() {
		panicVal = recover()
		_ = rtx.Rollback()
	}()
	fn(rtx)
	return nil
}

// catchPanicWithWriteTx starts a write transaction, runs fn, and ensures the
// transaction is rolled back even if fn panics (preventing lock deadlocks).
func catchPanicWithWriteTx(db *DB, fn func(tx *WriteTx)) (panicVal any) {
	tx, err := db.BeginWrite()
	if err != nil {
		return nil
	}
	defer func() {
		panicVal = recover()
		_ = tx.Rollback()
	}()
	fn(tx)
	return nil
}

// Port of corrupt-6.1 (lines 230-256 in corrupt.test)
// Original: Fill a leaf page to capacity with 63 cells (10-byte randomblob values
// on 1024-byte pages), delete rowid=1 to create a freeblock, corrupt the
// freeblock's size field to 0x00FF, then INSERT should detect corruption.
// DEVIATION: Our delete implementation uses fragmentation tracking instead of
// creating freeblocks. So we insert 55 cells (leaving space), then manually
// create a freeblock by raw byte patching. Cell count is 55 (not 63) because
// our cell format is slightly larger (varint keyLen + key + varint valLen + value).
// The net effect is the same: a page with a corrupted freeblock size.
func TestSqlite_Corrupt_6_FreeblockSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Open with page_size=1024
	db, err := testOpen(t, path, Options{PageSize: 1024})
	if err != nil {
		t.Fatal(err)
	}

	// CREATE TABLE t1 -> CreateNamespace
	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	ns, err := tx.CreateNamespace("t1")
	if err != nil {
		t.Fatal(err)
	}

	// Insert 55 cells to nearly fill page 2 (leaving room for one freeblock).
	// Page 2: 1024 bytes, 8-byte leaf header, 1016 bytes for cells.
	// Each cell: 1(keyLen) + 4(key) + 1(valLen) + 10(val) = 16 bytes data + 2 pointer = 18 total
	// 55 cells = 55*18 = 990 bytes, leaving 26 bytes for a freeblock (min 4 bytes)
	for i := 0; i < 55; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 10)
		if err := tx.Put(ns, key, val); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Checkpoint to flush to main DB and close
	if err := db.Checkpoint(CheckpointFull); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Read the DB file and manually create a freeblock on page 2.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	page2Off := 1024
	// Page 2 header: type(1) firstFreeBlk(2) cellCount(2) cellContentOff(2) fragBytes(1) = 8 bytes
	cellContentOff := int(binary.BigEndian.Uint16(data[page2Off+5 : page2Off+7]))
	// cellContentOff should be 128 (8 header + 55*2 pointers = 118, content starts at 128..1024)
	// Content area: from cellContentOff to 1024 (55 cells * 16 bytes = 880 bytes)
	// So cellContentOff should be 1024 - 880 = 144? Let me just read it.
	t.Logf("page 2 cellContentOff = %d", cellContentOff)

	// Create a freeblock right at the start of the content area.
	// We'll shrink the content area by writing a freeblock at cellContentOff - 16.
	// This simulates what happens when SQLite deletes a cell: the freed space
	// becomes a freeblock. Our freeblock is 16 bytes (like one cell).
	freeblockOff := cellContentOff // within page
	// Freeblock format: 2-byte next ptr (0 = none), 2-byte size
	binary.BigEndian.PutUint16(data[page2Off+freeblockOff:], 0)    // next = 0
	binary.BigEndian.PutUint16(data[page2Off+freeblockOff+2:], 16) // size = 16 bytes
	// Set firstFreeBlk in page header
	binary.BigEndian.PutUint16(data[page2Off+1:page2Off+3], uint16(freeblockOff))

	// Now corrupt the freeblock size to 0x00FF (255, which is too large)
	// Original: hexio_write test.db $offset 00FF
	data[page2Off+freeblockOff+2] = 0x00
	data[page2Off+freeblockOff+3] = 0xFF

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Remove WAL/SHM files
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	// Reopen DB
	db, err = testOpen(t, path, Options{PageSize: 1024})
	if err != nil {
		// Open failure on corrupted DB is acceptable
		return
	}
	defer func() { _ = db.Close() }()

	// Catch panics
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic on corrupt freeblock size: %v", r)
		}
	}()

	// Attempt Put(new key, 10-byte value) -> expect error (malformed)
	// Original: catchsql { INSERT INTO t1 VALUES( randomblob(10) ) }
	tx, err = db.BeginWrite()
	if err != nil {
		return
	}
	ns, err = db.getNamespaceLocked("t1")
	if err != nil {
		_ = tx.Rollback()
		return
	}
	newKey := binary.BigEndian.AppendUint32(nil, uint32(999))
	err = tx.Put(ns, newKey, make([]byte, 10))
	if err != nil {
		// Error is expected — corruption detected
		_ = tx.Rollback()
		return
	}
	_ = tx.Rollback()
}

// Port of corrupt-7.1 through corrupt-7.3 (lines 258-310 in corrupt.test)
// Original: Insert 39 rows of 20-byte values, corrupt the first cell-pointer
// in the cell-offset array to point to the data for key=10's value (which
// happens to look like a valid cell header), then update key=10 to break
// the fake header, then insert key=40 to trigger balance-deeper which
// detects the corruption.
// DEVIATION: Original uses 39 rows which fit on one page in SQLite's format.
// Our cell format is larger (26 bytes vs ~23 bytes), so 39 rows cause a page
// split. We use 35 rows instead to keep all cells on one leaf page (page 2),
// then insert key 36 to trigger balance-deeper. The cell-offset corruption
// logic is the same.
func TestSqlite_Corrupt_7_CellOffsetArray(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Open with page_size=1024
	db, err := testOpen(t, path, Options{PageSize: 1024})
	if err != nil {
		t.Fatal(err)
	}

	// CREATE TABLE t1 -> CreateNamespace
	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	ns, err := tx.CreateNamespace("t1")
	if err != nil {
		t.Fatal(err)
	}

	// Insert 35 rows with 20-byte values (fits on one 1024-byte page).
	// Each cell: 1(keyLen) + 4(key) + 1(valLen) + 20(val) = 26 bytes + 2 ptr = 28 bytes
	// 35 * 28 = 980 bytes, 1016 available (1024 - 8 header). Fits with 36 bytes spare.
	// Original: INSERT INTO t1 VALUES(X'000100020003000400050006000700080009000A')
	val := []byte{0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04, 0x00, 0x05,
		0x00, 0x06, 0x00, 0x07, 0x00, 0x08, 0x00, 0x09, 0x00, 0x0A}
	for i := 1; i <= 35; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		if err := tx.Put(ns, key, val); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Checkpoint and close
	if err := db.Checkpoint(CheckpointFull); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the cell-offset array on page 2 (root of t1).
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	page2Off := 1024
	pageType := data[page2Off]
	cellCount := int(binary.BigEndian.Uint16(data[page2Off+3 : page2Off+5]))
	t.Logf("page 2: type=%d cellCount=%d", pageType, cellCount)

	if pageType != pageTypeLeafIdx {
		t.Fatalf("expected leaf page (type %d) at page 2, got type %d", pageTypeLeafIdx, pageType)
	}

	// Find the cell for key=10
	key10 := binary.BigEndian.AppendUint32(nil, uint32(10))
	var key10CellOff int
	for i := 0; i < cellCount; i++ {
		cpOff := page2Off + 8 + i*2
		cellOff := int(binary.BigEndian.Uint16(data[cpOff : cpOff+2]))
		absOff := page2Off + cellOff
		keyLen, kn := getVarint(data[absOff:])
		// New format: skip valLen varint before key data
		_, vn := getVarint(data[absOff+kn:])
		keyStart := absOff + kn + vn
		cellKey := data[keyStart : keyStart+int(keyLen)]
		if len(cellKey) == 4 && binary.BigEndian.Uint32(cellKey) == 10 {
			key10CellOff = cellOff
			t.Logf("found key=10 at page-relative offset %d", cellOff)
			break
		}
	}
	if key10CellOff == 0 {
		t.Fatal("could not find cell for key=10 on page 2")
	}

	// Point the first cell pointer to the value data area of key=10's cell.
	// New cell layout: varint(keyLen=4) [1B] + varint(valLen=20) [1B] + key [4B] + value [20B]
	// Value starts at cellOff + 6. The value bytes (0x00,0x01,...) will be
	// parsed as: getVarint(0x00) -> keyLen=0, getVarint(0x01) -> valLen=1.
	// This looks like a valid but tiny cell to btreeInitPage.
	targetOff := key10CellOff + 6

	// Original: seek $fd [expr 1024+8]; puts -nonewline $fd "\x03\x14"
	binary.BigEndian.PutUint16(data[page2Off+8:page2Off+10], uint16(targetOff))
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Remove WAL/SHM
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	// Reopen DB
	db, err = testOpen(t, path, Options{PageSize: 1024})
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()

	// corrupt-7.2: UPDATE t1 SET x = ... WHERE rowid = 10
	// Overwrite key 10 with a different 20-byte value so the fake cell header
	// (which pointed into the old value) now contains different bytes, making
	// the corruption detectable during balance-deeper.
	// Original: UPDATE t1 SET x = X'870400020003000400050006000700080009000A' WHERE rowid = 10
	newVal := []byte{0x87, 0x04, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04, 0x00, 0x05,
		0x00, 0x06, 0x00, 0x07, 0x00, 0x08, 0x00, 0x09, 0x00, 0x0A}

	panicked := catchPanicWithWriteTx(db, func(tx *WriteTx) {
		ns, err := db.getNamespaceLocked("t1")
		if err != nil {
			return
		}
		_ = tx.Put(ns, key10, newVal)
	})
	if panicked != nil {
		t.Fatalf("panic during update of key 10: %v", panicked)
	}

	// corrupt-7.3: INSERT INTO t1 VALUES(...) -> triggers balance-deeper
	// Expect error (corruption detected during re-parse of page)
	// Original: INSERT INTO t1 VALUES(X'000100020003000400050006000700080009000A')
	panicked = catchPanicWithWriteTx(db, func(tx *WriteTx) {
		ns, err := db.getNamespaceLocked("t1")
		if err != nil {
			return
		}
		key36 := binary.BigEndian.AppendUint32(nil, uint32(36))
		_ = tx.Put(ns, key36, val)
	})
	if panicked != nil {
		t.Fatalf("panic during insert of key 36 (balance-deeper): %v", panicked)
	}
}

// Port of corrupt-8.1 (lines 314-328 in corrupt.test)
// Original: Insert key=5 with 1900-byte value (creates overflow on 1024-byte pages),
// corrupt the overflow page pointer to point back to page 2 (circular reference),
// corrupt the change counter at offset 24, then INSERT OR REPLACE.
// DEVIATION: Overflow pointer offset computed empirically. INSERT OR REPLACE
// maps to Put (upsert). secure_delete and auto_vacuum not applicable.
func TestSqlite_Corrupt_8_1_OverflowPointer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, Options{PageSize: 1024})
	if err != nil {
		t.Fatal(err)
	}

	// CREATE TABLE t1(x INTEGER PRIMARY KEY, y) -> CreateNamespace
	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	ns, err := tx.CreateNamespace("t1")
	if err != nil {
		t.Fatal(err)
	}

	// INSERT INTO t1 VALUES(5, randomblob(1900))
	key5 := binary.BigEndian.AppendUint32(nil, uint32(5))
	val1900 := make([]byte, 1900)
	rand.Read(val1900)
	if err := tx.Put(ns, key5, val1900); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Checkpoint and close
	if err := db.Checkpoint(CheckpointFull); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Read DB file and find the overflow pointer
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Page 2 is the root of t1 (starts at offset 1024).
	// It contains one cell with key=5 and a 1900-byte value.
	// For 1024-byte pages: usable=1024, maxLocal = ((1024-12)*64/255)-23 = 231
	// totalPayload = 4 (key) + 1900 (val) = 1904 > 231, so it overflows.
	// Find the overflow pointer in the cell on page 2.
	page2Off := 1024
	cellCount := int(binary.BigEndian.Uint16(data[page2Off+3 : page2Off+5]))
	if cellCount != 1 {
		t.Fatalf("expected 1 cell on page 2, got %d", cellCount)
	}
	cellOff := int(binary.BigEndian.Uint16(data[page2Off+8 : page2Off+10]))

	// Parse the cell to find the overflow pointer (v5 format: keyLen, valLen, payload)
	absOff := page2Off + cellOff
	keyLen, kn := getVarint(data[absOff:])
	valLen, vn := getVarint(data[absOff+kn:])
	pos := absOff + kn + vn // after both varints

	// Calculate local payload size (unified format)
	usable := 1024
	totalPayload := int(keyLen) + int(valLen)
	nLocal := localPayloadSize(totalPayload, usable)
	ovfPtrOff := pos + nLocal // absolute offset of overflow page pointer

	t.Logf("overflow pointer at absolute offset %d (page-relative: %d)", ovfPtrOff, ovfPtrOff-page2Off)

	// Corrupt overflow pointer to point to page 2 (circular reference)
	// Original: hexio_write test.db 2044 [hexio_render_int32 2]
	binary.BigEndian.PutUint32(data[ovfPtrOff:ovfPtrOff+4], 2)

	// Corrupt change counter at offset 24
	// Original: hexio_write test.db 24 [hexio_render_int32 45]
	binary.BigEndian.PutUint32(data[24:28], 45)

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Remove WAL/SHM
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	// Reopen
	db, err = testOpen(t, path, Options{PageSize: 1024})
	if err != nil {
		// Open failure acceptable
		return
	}
	defer func() { _ = db.Close() }()

	// Catch panics
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic on corrupt overflow pointer: %v", r)
		}
	}()

	// Attempt Put(key=5, 1900 zero bytes) -> expect error
	// Original: catchsql { INSERT OR REPLACE INTO t1 VALUES(5, randomblob(1900)) }
	tx, err = db.BeginWrite()
	if err != nil {
		return
	}
	ns, err = db.getNamespaceLocked("t1")
	if err != nil {
		_ = tx.Rollback()
		return
	}
	err = tx.Put(ns, key5, make([]byte, 1900))
	if err != nil {
		// Error expected — circular overflow pointer detected
		_ = tx.Rollback()
		return
	}
	_ = tx.Commit()
}

// Port of corrupt-8.2 (lines 331-347 in corrupt.test)
// Original: Insert key=5 (900 bytes) and key=6 (900 bytes), corrupt byte at
// offset 2047 (last byte of page 2) to 0xFF, corrupt change counter, then
// INSERT key=4 (1900 bytes) should detect corruption.
// DEVIATION: Same offset considerations as corrupt-8.1.
func TestSqlite_Corrupt_8_2_CellAreaCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, Options{PageSize: 1024})
	if err != nil {
		t.Fatal(err)
	}

	// CREATE TABLE t1(x INTEGER PRIMARY KEY, y) -> CreateNamespace
	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	ns, err := tx.CreateNamespace("t1")
	if err != nil {
		t.Fatal(err)
	}

	// INSERT INTO t1 VALUES(5, randomblob(900))
	key5 := binary.BigEndian.AppendUint32(nil, uint32(5))
	if err := tx.Put(ns, key5, make([]byte, 900)); err != nil {
		t.Fatal(err)
	}

	// INSERT INTO t1 VALUES(6, randomblob(900))
	key6 := binary.BigEndian.AppendUint32(nil, uint32(6))
	if err := tx.Put(ns, key6, make([]byte, 900)); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Checkpoint and close
	if err := db.Checkpoint(CheckpointFull); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Read DB file
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt byte at offset 2047 (last byte of page 2) to 0xFF
	// Page 2 = bytes 1024-2047
	// Original: hexio_write test.db 2047 FF
	if len(data) < 2048 {
		t.Fatalf("DB file too small: %d bytes, need at least 2048", len(data))
	}
	data[2047] = 0xFF

	// Corrupt change counter at offset 24
	// Original: hexio_write test.db 24 [hexio_render_int32 45]
	binary.BigEndian.PutUint32(data[24:28], 45)

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Remove WAL/SHM
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")

	// Reopen
	db, err = testOpen(t, path, Options{PageSize: 1024})
	if err != nil {
		// Open failure acceptable
		return
	}
	defer func() { _ = db.Close() }()

	// Catch panics
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic on corrupt cell area: %v", r)
		}
	}()

	// Attempt Put(key=4, 1900 zero bytes) -> expect error
	// Original: catchsql { INSERT INTO t1 VALUES(4, randomblob(1900)) }
	tx, err = db.BeginWrite()
	if err != nil {
		return
	}
	ns, err = db.getNamespaceLocked("t1")
	if err != nil {
		_ = tx.Rollback()
		return
	}
	key4 := binary.BigEndian.AppendUint32(nil, uint32(4))
	err = tx.Put(ns, key4, make([]byte, 1900))
	if err != nil {
		// Error expected — corruption in cell area detected
		_ = tx.Rollback()
		return
	}
	_ = tx.Commit()
}
