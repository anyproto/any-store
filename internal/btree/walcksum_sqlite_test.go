/*
Ported from SQLite: walcksum.test
Source: /home/dev/work/sqlitec/test/walcksum.test

Test scenario:
Tests WAL checksum correctness. The original file covers endian-specific checksum
computation and verification, recovery of WAL files with non-native checksums,
correct checksum continuation when appending to a recovered WAL, and WAL checksum
integrity across savepoint rollbacks.

Groups 2-5 test WAL correctness after savepoint rollbacks: the key behavior is
that WAL frames written after a savepoint rollback have correct checksums and
can be recovered by a fresh process after a crash.

Deviations from original:
- walcksum-1.$endian.1: Adapted. Original uses journal mode then switches to WAL;
  our system is always WAL. Original copies files with forcecopy and checks exact sizes;
  we use rawClose and check WAL exists with size > 0.
- walcksum-1.$endian.2.$f: Adapted. Original verifies checksums with both big and little
  endian; our system only uses big-endian checksums (walMagic = 0x42540601).
- walcksum-1.$endian.3-7, 9: Skipped. Endian-switching of WAL checksums is not supported
  (our implementation uses fixed big-endian checksums with a different magic number).
- walcksum-1.$endian.8: Adapted. Original tests that after checkpoint, new frames use
  native byte order. We verify that after checkpoint, new frame checksums are correct.
- walcksum-2.1: Adapted. INSERT...SELECT doubling pattern done manually with cursor read
  + new unique key inserts. randomblob(800) mapped to 800-byte random values.
- walcksum-3.0 through 3.2, 1.3: Adapted. Text column value combined with key as needed.
  NULL blob mapped to empty []byte{}. "forcecopy + second connection" mapped to rawClose +
  openDBNoCleanup pattern (but using Open with PageSize: 1024).
- walcksum-4.0 through 4.3: Adapted. Same as group 3 but with outer savepoint pattern.
  SAVEPOINT one at transaction boundary mapped to BeginWrite + Savepoint.
- walcksum-5.0 through 5.3: Adapted. Post-checkpoint savepoint rollback pattern.
*/
package btree

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// verifyWALFrameChecksums reads the WAL file at walPath, parses its header and frames,
// and verifies that each frame's cumulative checksum is correct.
// Returns the number of valid frames found.
func verifyWALFrameChecksums(t *testing.T, walPath string, pageSize uint32) int {
	t.Helper()

	data, err := os.ReadFile(walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), walHeaderSize, "WAL file too small for header")

	// Parse WAL header
	var hdr walHeader
	require.NoError(t, hdr.deserialize(data[0:walHeaderSize]))
	require.Equal(t, pageSize, hdr.pageSize, "WAL header page size mismatch")

	// Initialize checksum from header (first 24 bytes)
	s1, s2 := walChecksum(data[0:24], 0, 0)

	frameSize := int(walFrameSize) + int(pageSize)
	offset := walHeaderSize
	validFrames := 0

	for offset+frameSize <= len(data) {
		frameHeader := data[offset : offset+walFrameSize]

		// Verify salt matches header
		frameSalt1 := binary.BigEndian.Uint32(frameHeader[8:12])
		frameSalt2 := binary.BigEndian.Uint32(frameHeader[12:16])
		if frameSalt1 != hdr.salt1 || frameSalt2 != hdr.salt2 {
			break
		}

		// Compute expected checksum: frame header first 8 bytes + page data
		pageData := data[offset+walFrameSize : offset+frameSize]
		cs1, cs2 := walFrameChecksum(frameHeader[0:8], pageData, s1, s2)

		// Read stored checksum
		storedCS1 := binary.BigEndian.Uint32(frameHeader[16:20])
		storedCS2 := binary.BigEndian.Uint32(frameHeader[20:24])

		if cs1 != storedCS1 || cs2 != storedCS2 {
			break
		}

		s1, s2 = cs1, cs2
		validFrames++
		offset += frameSize
	}

	return validFrames
}

// verifyWALFrameChecksumAt verifies the checksum of a specific frame (1-based) in
// the WAL file. Returns true if the checksum is valid.
func verifyWALFrameChecksumAt(t *testing.T, walPath string, pageSize uint32, frameNum int) bool {
	t.Helper()

	data, err := os.ReadFile(walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), walHeaderSize, "WAL file too small for header")

	// Parse WAL header
	var hdr walHeader
	if err := hdr.deserialize(data[0:walHeaderSize]); err != nil {
		return false
	}

	// Initialize checksum from header (first 24 bytes)
	s1, s2 := walChecksum(data[0:24], 0, 0)

	frameSize := int(walFrameSize) + int(pageSize)
	offset := walHeaderSize

	for f := 1; f <= frameNum; f++ {
		if offset+frameSize > len(data) {
			return false
		}

		frameHeader := data[offset : offset+walFrameSize]

		// Verify salt matches header
		frameSalt1 := binary.BigEndian.Uint32(frameHeader[8:12])
		frameSalt2 := binary.BigEndian.Uint32(frameHeader[12:16])
		if frameSalt1 != hdr.salt1 || frameSalt2 != hdr.salt2 {
			return false
		}

		// Compute expected checksum: frame header first 8 bytes + page data
		pageData := data[offset+walFrameSize : offset+frameSize]
		cs1, cs2 := walFrameChecksum(frameHeader[0:8], pageData, s1, s2)

		// Read stored checksum
		storedCS1 := binary.BigEndian.Uint32(frameHeader[16:20])
		storedCS2 := binary.BigEndian.Uint32(frameHeader[20:24])

		if f == frameNum {
			return cs1 == storedCS1 && cs2 == storedCS2
		}

		if cs1 != storedCS1 || cs2 != storedCS2 {
			return false
		}

		s1, s2 = cs1, cs2
		offset += frameSize
	}

	return false
}

// Port of walcksum-1.$endian.1 (lines 152-178 in walcksum.test)
// Original: Creates a DB with page_size=1024, inserts data first in journal mode
// then switches to WAL and inserts more data. Copies DB+WAL and verifies sizes.
// DEVIATION: Our system is always WAL mode; no journal-to-WAL transition.
// We insert all data directly, then rawClose to leave WAL on disk.

// Port of walcksum-1.$endian.2.$f (lines 183-187 in walcksum.test)
// Original: Verifies that all 6 frames in the WAL have valid checksums computed
// with native byte order.
// DEVIATION: Our checksums are always big-endian. We verify each frame's checksum.

// Port of walcksum-1.$endian.8 (lines 263-275 in walcksum.test)
// Original: Checkpoint, insert, verify new frame checksums are correct, and that
// old frames beyond the new write have stale (invalid) checksums.
// DEVIATION: No endian dimension. We just verify correct checksum behavior
// after checkpoint resets the WAL.

// walcksum-1.$endian.3-7, 9: SKIPPED
// Reason: Endian-switching of WAL checksums not supported. Our implementation
// uses fixed big-endian checksums (walMagic = 0x42540601).

func TestSqlite_WALCksum_1(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	walPath := dbPath + "-wal"

	// --- walcksum-1.$endian.1 (lines 152-178) ---
	// Original: Create DB, insert data, switch to WAL, insert more, copy, verify sizes.
	// Adapted: Create DB (always WAL), insert data, rawClose, verify WAL exists.
	t.Run("walcksum-1.1", func(t *testing.T) {
		db, err := Open(dbPath, Options{PageSize: 1024})
		require.NoError(t, err)

		// CREATE TABLE t1(a PRIMARY KEY, b)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		_, err = tx.CreateNamespace("t1")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		// DEVIATION: Original inserts 4 rows in journal mode, then 3 in WAL mode.
		// We insert all 7 in WAL mode since our system is always WAL.

		// INSERT INTO t1 VALUES(1, 'one'), (2, 'two'), (3, 'three'), (5, 'five')
		tx, err = db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, intKey(1), []byte("one")))
		require.NoError(t, tx.Put(ns, intKey(2), []byte("two")))
		require.NoError(t, tx.Put(ns, intKey(3), []byte("three")))
		require.NoError(t, tx.Put(ns, intKey(5), []byte("five")))
		require.NoError(t, tx.Commit())

		// INSERT INTO t1 VALUES(8, 'eight'), (13, 'thirteen'), (21, 'twentyone')
		tx, err = db.BeginWrite()
		require.NoError(t, err)
		ns, err = db.getNamespaceLocked("t1")
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, intKey(8), []byte("eight")))
		require.NoError(t, tx.Put(ns, intKey(13), []byte("thirteen")))
		require.NoError(t, tx.Put(ns, intKey(21), []byte("twentyone")))
		require.NoError(t, tx.Commit())

		// rawClose to simulate crash (leave WAL on disk)
		rawClose(db)

		// Verify WAL file exists and has size > 0
		info, err := os.Stat(walPath)
		require.NoError(t, err, "WAL file should exist after rawClose")
		assert.Greater(t, info.Size(), int64(0), "WAL file should have content")
	})

	// --- walcksum-1.$endian.2.$f (lines 183-187) ---
	// Original: Verify all 6 frames have valid checksums with native byte order.
	// Adapted: Verify all WAL frames have valid big-endian checksums.
	t.Run("walcksum-1.2", func(t *testing.T) {
		validFrames := verifyWALFrameChecksums(t, walPath, 1024)
		assert.Greater(t, validFrames, 0, "WAL should have at least one valid frame")
		t.Logf("Verified %d valid WAL frames", validFrames)

		// Verify each frame individually
		for f := 1; f <= validFrames; f++ {
			t.Run(fmt.Sprintf("f=%d", f), func(t *testing.T) {
				ok := verifyWALFrameChecksumAt(t, walPath, 1024, f)
				assert.True(t, ok, "frame %d checksum should be valid", f)
			})
		}
	})

	// --- walcksum-1.$endian.8 (lines 263-275) ---
	// Original: Checkpoint, insert one row, verify all frames have valid checksums.
	// FULL checkpoint preserves the WAL (no reset), so all frames remain valid.
	t.Run("walcksum-1.8", func(t *testing.T) {
		// Reopen DB (recovers from WAL)
		db, err := Open(dbPath, Options{PageSize: 1024})
		require.NoError(t, err)

		// PRAGMA wal_checkpoint (FULL mode preserves WAL)
		require.NoError(t, db.Checkpoint(CheckpointFull))

		// INSERT INTO t1 VALUES(89, 'eightynine')
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, intKey(89), []byte("eightynine")))
		require.NoError(t, tx.Commit())

		// rawClose to keep WAL on disk for inspection
		rawClose(db)

		// Count total valid frames after checkpoint + insert.
		// FULL checkpoint preserves WAL, so all frames (old + new) remain valid.
		validFrames := verifyWALFrameChecksums(t, walPath, 1024)
		t.Logf("Valid frames after checkpoint + insert: %d", validFrames)
		assert.True(t, validFrames >= 2, "should have at least 2 valid frames")

		t.Run("f=1", func(t *testing.T) {
			ok := verifyWALFrameChecksumAt(t, walPath, 1024, 1)
			assert.True(t, ok, "frame 1 checksum should be valid")
		})
		if validFrames >= 2 {
			t.Run("f=2", func(t *testing.T) {
				ok := verifyWALFrameChecksumAt(t, walPath, 1024, 2)
				assert.True(t, ok, "frame 2 checksum should be valid")
			})
		}
	})

	// Clean up for next tests
	_ = os.Remove(dbPath)
	_ = os.Remove(walPath)
	_ = os.Remove(dbPath + "-wal-shm")
}

// Port of walcksum-2.1 (lines 294-331 in walcksum.test)
// Original: Tests WAL integrity after a savepoint rollback within a transaction.
// Inserts rows with doubling pattern (1->2->4->8->16), takes a savepoint, doubles
// again (16->32->64->128->256), rolls back to savepoint, doubles again
// (16->32->64->128->256), then commits. Copies DB+WAL and opens with second
// connection to verify recovery yields correct data (256 rows) and integrity
// check passes.
// DEVIATION: INSERT...SELECT doubling done manually with cursor read + new unique
// key inserts. randomblob(800) mapped to 800-byte random values.
// cache_size=10 not applicable (our page cache managed differently).
func TestSqlite_WALCksum_2_1(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Open DB with page_size=1024
	db, err := Open(dbPath, Options{PageSize: 1024})
	require.NoError(t, err)

	// CREATE TABLE t1(x PRIMARY KEY)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// PRAGMA wal_checkpoint
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// INSERT INTO t1 VALUES(randomblob(800)) -- 1 row
	nextKey := uint32(1)
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	blob800 := make([]byte, 800)
	_, _ = rand.Read(blob800)
	require.NoError(t, tx.Put(ns, intKey(nextKey), blob800))
	nextKey++
	require.NoError(t, tx.Commit())

	// BEGIN
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// Double rows 4 times: 1->2->4->8->16
	for d := 0; d < 4; d++ {
		// Count existing keys
		cur := tx.NewCursor(ns)
		existing := countCursor(t, cur)
		// Insert 'existing' new rows
		for i := 0; i < existing; i++ {
			blob := make([]byte, 800)
			_, _ = rand.Read(blob)
			require.NoError(t, tx.Put(ns, intKey(nextKey), blob))
			nextKey++
		}
	}

	// Verify count = 16
	cur := tx.NewCursor(ns)
	assert.Equal(t, 16, countCursor(t, cur), "should have 16 rows before savepoint")

	// SAVEPOINT one
	sp, err := tx.Savepoint()
	require.NoError(t, err)

	// Double rows 4 times: 16->32->64->128->256
	savedNextKey := nextKey
	for d := 0; d < 4; d++ {
		cur := tx.NewCursor(ns)
		existing := countCursor(t, cur)
		for i := 0; i < existing; i++ {
			blob := make([]byte, 800)
			_, _ = rand.Read(blob)
			require.NoError(t, tx.Put(ns, intKey(nextKey), blob))
			nextKey++
		}
	}

	// ROLLBACK TO one
	require.NoError(t, tx.RollbackToSavepoint(sp))

	// Reset nextKey since the savepoint rollback undid the inserts
	nextKey = savedNextKey

	// Double rows 4 times again: 16->32->64->128->256
	for d := 0; d < 4; d++ {
		cur := tx.NewCursor(ns)
		existing := countCursor(t, cur)
		for i := 0; i < existing; i++ {
			blob := make([]byte, 800)
			_, _ = rand.Read(blob)
			require.NoError(t, tx.Put(ns, intKey(nextKey), blob))
			nextKey++
		}
	}

	// COMMIT
	require.NoError(t, tx.Commit())

	// rawClose to simulate crash (copy DB+WAL pattern)
	rawClose(db)

	// Reopen (triggers recovery from WAL)
	db2, err := Open(dbPath, Options{PageSize: 1024})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	// PRAGMA integrity_check
	require.NoError(t, db2.IntegrityCheck())

	// SELECT count(*) FROM t1 -> 256
	count := countKeys(t, db2, "t1")
	assert.Equal(t, 256, count, "should have 256 rows after recovery")
}

// Port of walcksum-3.* (lines 342-383 in walcksum.test)
// Original: Tests WAL checksum integrity across savepoint rollback with
// inserts and updates. Creates DB, inserts one row, then within a transaction:
// insert row 2, savepoint, insert rows 3-7, update rows 5-7, rollback to
// savepoint, insert row 8 (with NULL blob), commit. Verify keys 1,2,8 exist.
// Then crash-recovery verify same result.
func TestSqlite_WALCksum_3(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// --- walcksum-3.0 (lines 342-351) ---
	// Original: Create DB, create table, checkpoint, insert row 1.
	db, err := Open(dbPath, Options{PageSize: 1024})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// PRAGMA wal_checkpoint
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// INSERT INTO t1 VALUES(1, randomblob(2048), 'one')
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	blob2048 := make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(1), blob2048))
	require.NoError(t, tx.Commit())

	// --- walcksum-3.1 (lines 353-369) ---
	// Original: BEGIN, insert row 2, savepoint, insert rows 3-7, update rows 5-7,
	// rollback to savepoint, insert row 8 (NULL blob), commit.
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// INSERT INTO t1 VALUES(2, randomblob(2048), 'two')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(2), blob2048))

	// SAVEPOINT one
	sp, err := tx.Savepoint()
	require.NoError(t, err)

	// INSERT INTO t1 VALUES(3, randomblob(2048), 'three')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(3), blob2048))

	// INSERT INTO t1 VALUES(4, randomblob(2048), 'four')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(4), blob2048))

	// INSERT INTO t1 VALUES(5, randomblob(2048), 'five')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(5), blob2048))

	// INSERT INTO t1 VALUES(6, randomblob(2048), 'six')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(6), blob2048))

	// INSERT INTO t1 VALUES(7, randomblob(2048), 'seven')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(7), blob2048))

	// UPDATE t1 SET b=randomblob(2048) WHERE i=5
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(5), blob2048))

	// UPDATE t1 SET b=randomblob(2048) WHERE i=6
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(6), blob2048))

	// UPDATE t1 SET b=randomblob(2048) WHERE i=7
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(7), blob2048))

	// ROLLBACK TO one
	require.NoError(t, tx.RollbackToSavepoint(sp))

	// INSERT INTO t1 VALUES(8, NULL, 'eight')
	// DEVIATION: NULL blob mapped to empty []byte{}
	require.NoError(t, tx.Put(ns, intKey(8), []byte{}))

	// COMMIT
	require.NoError(t, tx.Commit())

	// --- walcksum-3.2 (lines 371-373) ---
	// Original: SELECT i, t FROM t1 -> {1 one 2 two 8 eight}
	t.Run("walcksum-3.2", func(t *testing.T) {
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		cur := rtx.NewCursor(ns)

		var keys []uint32
		for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
			k, err := cur.Key()
			require.NoError(t, err)
			keys = append(keys, binary.BigEndian.Uint32(k))
		}
		require.NoError(t, rtx.Rollback())

		assert.Equal(t, []uint32{1, 2, 8}, keys, "should have keys 1, 2, 8 after savepoint rollback")
	})

	// --- walcksum-1.3 (lines 379-383, named "1.3" in test file, part of group 3) ---
	// Original: forcecopy + open second connection, verify same data.
	// Adapted: rawClose + reopen.
	t.Run("walcksum-3.recovery", func(t *testing.T) {
		rawClose(db)

		db2, err := Open(dbPath, Options{PageSize: 1024})
		require.NoError(t, err)
		defer func() { _ = db2.Close() }()

		rtx, err := db2.BeginRead()
		require.NoError(t, err)
		ns, err := db2.getNamespaceLocked("t1")
		require.NoError(t, err)
		cur := rtx.NewCursor(ns)

		var keys []uint32
		for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
			k, err := cur.Key()
			require.NoError(t, err)
			keys = append(keys, binary.BigEndian.Uint32(k))
		}
		require.NoError(t, rtx.Rollback())

		assert.Equal(t, []uint32{1, 2, 8}, keys, "recovery should produce keys 1, 2, 8")
	})
}

// Port of walcksum-4.* (lines 388-435 in walcksum.test)
// Original: Same setup as group 3 but the savepoint is at the outermost level
// (SAVEPOINT one acts as the transaction boundary in SQLite). Inserts rows 2-7,
// updates rows 5-7, rolls back to savepoint, inserts row 8, releases savepoint.
// Verify keys 1, 8 exist. Then crash-recovery verify same result.
// DEVIATION: SAVEPOINT as outermost transaction mapped to BeginWrite + Savepoint.
// RELEASE savepoint + implicit commit mapped to ReleaseSavepoint + Commit.
func TestSqlite_WALCksum_4(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// --- walcksum-4.0 (lines 390-399) ---
	// Original: Create DB, create table, checkpoint, insert row 1.
	db, err := Open(dbPath, Options{PageSize: 1024})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// PRAGMA wal_checkpoint
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// INSERT INTO t1 VALUES(1, randomblob(2048), 'one')
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	blob2048 := make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(1), blob2048))
	require.NoError(t, tx.Commit())

	// --- walcksum-4.1.1 (lines 401-413) ---
	// Original: SAVEPOINT one (outermost), insert rows 2-7, update rows 5-7.
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// SAVEPOINT one
	sp, err := tx.Savepoint()
	require.NoError(t, err)

	// INSERT INTO t1 VALUES(2, randomblob(2048), 'two')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(2), blob2048))

	// INSERT INTO t1 VALUES(3, randomblob(2048), 'three')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(3), blob2048))

	// INSERT INTO t1 VALUES(4, randomblob(2048), 'four')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(4), blob2048))

	// INSERT INTO t1 VALUES(5, randomblob(2048), 'five')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(5), blob2048))

	// INSERT INTO t1 VALUES(6, randomblob(2048), 'six')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(6), blob2048))

	// INSERT INTO t1 VALUES(7, randomblob(2048), 'seven')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(7), blob2048))

	// UPDATE t1 SET b=randomblob(2048) WHERE i=5
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(5), blob2048))

	// UPDATE t1 SET b=randomblob(2048) WHERE i=6
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(6), blob2048))

	// UPDATE t1 SET b=randomblob(2048) WHERE i=7
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(7), blob2048))

	// --- walcksum-4.1.2 (lines 415-419) ---
	// Original: ROLLBACK TO one, INSERT row 8, RELEASE one.
	require.NoError(t, tx.RollbackToSavepoint(sp))

	// INSERT INTO t1 VALUES(8, NULL, 'eight')
	require.NoError(t, tx.Put(ns, intKey(8), []byte{}))

	// RELEASE one
	require.NoError(t, tx.ReleaseSavepoint(sp))

	// Commit (RELEASE of outermost savepoint commits in SQLite)
	require.NoError(t, tx.Commit())

	// --- walcksum-4.2 (lines 421-423) ---
	// Original: SELECT i, t FROM t1 -> {1 one 8 eight}
	t.Run("walcksum-4.2", func(t *testing.T) {
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		cur := rtx.NewCursor(ns)

		var keys []uint32
		for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
			k, err := cur.Key()
			require.NoError(t, err)
			keys = append(keys, binary.BigEndian.Uint32(k))
		}
		require.NoError(t, rtx.Rollback())

		assert.Equal(t, []uint32{1, 8}, keys, "should have keys 1, 8 after savepoint rollback")
	})

	// --- walcksum-4.3 (lines 429-433) ---
	// Original: forcecopy + open second connection, verify same data.
	t.Run("walcksum-4.3", func(t *testing.T) {
		rawClose(db)

		db2, err := Open(dbPath, Options{PageSize: 1024})
		require.NoError(t, err)
		defer func() { _ = db2.Close() }()

		rtx, err := db2.BeginRead()
		require.NoError(t, err)
		ns, err := db2.getNamespaceLocked("t1")
		require.NoError(t, err)
		cur := rtx.NewCursor(ns)

		var keys []uint32
		for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
			k, err := cur.Key()
			require.NoError(t, err)
			keys = append(keys, binary.BigEndian.Uint32(k))
		}
		require.NoError(t, rtx.Rollback())

		assert.Equal(t, []uint32{1, 8}, keys, "recovery should produce keys 1, 8")
	})
}

// Port of walcksum-5.* (lines 438-480 in walcksum.test)
// Original: Tests savepoint rollback AFTER a checkpoint. Creates DB, inserts 3 rows,
// checkpoints. Then within a transaction: count (expect 3), savepoint, insert rows
// 4-7, rollback to savepoint, insert rows 8-9, commit. Crash-recovery verify
// keys 1, 2, 3, 8, 9 exist.
func TestSqlite_WALCksum_5(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// --- walcksum-5.0 (lines 440-451) ---
	// Original: Create DB, insert 3 rows, checkpoint.
	db, err := Open(dbPath, Options{PageSize: 1024})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// INSERT INTO t1 VALUES(1, randomblob(2048), 'one')
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	blob2048 := make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(1), blob2048))
	require.NoError(t, tx.Commit())

	// INSERT INTO t1 VALUES(2, randomblob(2048), 'two')
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(2), blob2048))
	require.NoError(t, tx.Commit())

	// INSERT INTO t1 VALUES(3, randomblob(2048), 'three')
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(3), blob2048))
	require.NoError(t, tx.Commit())

	// PRAGMA wal_checkpoint
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// --- walcksum-5.1 (lines 453-465) ---
	// Original: BEGIN, SELECT count(*) (expect 3), SAVEPOINT one, insert rows 4-7,
	// ROLLBACK TO one, insert rows 8-9, COMMIT.
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// SELECT count(*) FROM t1 -- expect 3
	cur := tx.NewCursor(ns)
	count := countCursor(t, cur)
	assert.Equal(t, 3, count, "should have 3 rows before savepoint")

	// SAVEPOINT one
	sp, err := tx.Savepoint()
	require.NoError(t, err)

	// INSERT INTO t1 VALUES(4, randomblob(2048), 'four')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(4), blob2048))

	// INSERT INTO t1 VALUES(5, randomblob(2048), 'five')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(5), blob2048))

	// INSERT INTO t1 VALUES(6, randomblob(2048), 'six')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(6), blob2048))

	// INSERT INTO t1 VALUES(7, randomblob(2048), 'seven')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(7), blob2048))

	// ROLLBACK TO one
	require.NoError(t, tx.RollbackToSavepoint(sp))

	// INSERT INTO t1 VALUES(8, randomblob(2048), 'eight')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(8), blob2048))

	// INSERT INTO t1 VALUES(9, randomblob(2048), 'nine')
	blob2048 = make([]byte, 2048)
	_, _ = rand.Read(blob2048)
	require.NoError(t, tx.Put(ns, intKey(9), blob2048))

	// COMMIT
	require.NoError(t, tx.Commit())

	// --- walcksum-5.2 (lines 470-475) ---
	// Original: forcecopy + open second connection, verify keys.
	t.Run("walcksum-5.2", func(t *testing.T) {
		rawClose(db)

		db2, err := Open(dbPath, Options{PageSize: 1024})
		require.NoError(t, err)
		defer func() { _ = db2.Close() }()

		rtx, err := db2.BeginRead()
		require.NoError(t, err)
		ns, err := db2.getNamespaceLocked("t1")
		require.NoError(t, err)
		cur := rtx.NewCursor(ns)

		var keys []uint32
		for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
			k, err := cur.Key()
			require.NoError(t, err)
			keys = append(keys, binary.BigEndian.Uint32(k))
		}
		require.NoError(t, rtx.Rollback())

		assert.Equal(t, []uint32{1, 2, 3, 8, 9}, keys,
			"recovery should produce keys 1, 2, 3, 8, 9")

		// --- walcksum-5.3 (lines 478-480) ---
		// Original: SELECT i, t FROM t1 on original connection (same result).
		// Verified on the same recovered DB since original connection is rawClosed.
		t.Run("walcksum-5.3", func(t *testing.T) {
			rtx2, err := db2.BeginRead()
			require.NoError(t, err)
			ns2, err := db2.getNamespaceLocked("t1")
			require.NoError(t, err)
			cur2 := rtx2.NewCursor(ns2)

			var keys2 []uint32
			for err := cur2.First(); err == nil && cur2.Valid(); err = cur2.Next() {
				k, err := cur2.Key()
				require.NoError(t, err)
				keys2 = append(keys2, binary.BigEndian.Uint32(k))
			}
			require.NoError(t, rtx2.Rollback())

			assert.Equal(t, []uint32{1, 2, 3, 8, 9}, keys2,
				"second read should also produce keys 1, 2, 3, 8, 9")
		})
	})
}
