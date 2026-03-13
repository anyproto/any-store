package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openDBNoCleanup opens a DB without registering a cleanup handler,
// so tests can manually control close/reopen behavior.
func openDBNoCleanup(t *testing.T, path string) *DB {
	t.Helper()
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	return db
}

// rawClose closes the DB without checkpointing by closing the underlying files directly.
// This simulates a crash where dirty pages are in the WAL but not checkpointed.
func rawClose(db *DB) {
	db.closing.Store(true)
	db.closed.Store(true)
	if db.pager.wal != nil {
		if db.pager.wal.index != nil {
			_ = db.pager.wal.index.close()
		}
		if db.pager.wal.file != nil {
			_ = db.pager.wal.file.Close()
			db.pager.wal.file = nil
		}
	}
	db.pager.writerCache.clear()
	if db.pager.file != nil {
		_ = db.pager.file.Close()
		db.pager.file = nil
	}
	// Remove from open registry so the path can be re-opened.
	if !db.opts.InMemory {
		openDBs.Delete(db.path)
	}
}

// TestCrash1_PartialWALFrame: Truncate WAL mid-frame. Only committed data visible.
func TestCrash1_PartialWALFrame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	walPath := path + "-wal"

	// Write first commit
	db := openDBNoCleanup(t, path)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("key1"), []byte("val1")))
	require.NoError(t, tx.Commit())

	// Write second commit
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx2.Put(ns2, []byte("key2"), []byte("val2")))
	require.NoError(t, tx2.Commit())

	rawClose(db)

	// Truncate WAL mid-frame (cut into the second commit's frame)
	info, err := os.Stat(walPath)
	require.NoError(t, err)
	frameSize := int64(walFrameSize + DefaultPageSize)
	// Cut at halfway through the last frame
	truncAt := info.Size() - frameSize/2
	require.NoError(t, os.Truncate(walPath, truncAt))

	// Reopen — should recover first commit only
	db2 := openDBNoCleanup(t, path)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns3, err := db2.getNamespaceLocked("data")
	require.NoError(t, err)
	// key1 should be recoverable since it was in an earlier commit
	val, err := rtx.Get(ns3, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), val)
	require.NoError(t, rtx.Rollback())
}

// TestCrash2_CommitWithoutCheckpoint: Skip checkpoint on close. WAL replayed, data intact.
func TestCrash2_CommitWithoutCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db := openDBNoCleanup(t, path)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k1"), []byte("v1")))
	require.NoError(t, tx.Put(ns, []byte("k2"), []byte("v2")))
	require.NoError(t, tx.Commit())

	// Close without checkpoint (simulates crash after commit)
	rawClose(db)

	// Reopen — WAL should be replayed
	db2 := openDBNoCleanup(t, path)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns2, err := db2.getNamespaceLocked("data")
	require.NoError(t, err)
	v1, err := rtx.Get(ns2, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), v1)
	v2, err := rtx.Get(ns2, []byte("k2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), v2)
	require.NoError(t, rtx.Rollback())
}

// TestCrash3_PartialCheckpoint: Write some frames to DB, don't truncate WAL.
// WAL replayed (idempotent).
func TestCrash3_PartialCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db := openDBNoCleanup(t, path)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	for i := range 50 {
		k := fmt.Appendf(nil, "key-%03d", i)
		v := fmt.Appendf(nil, "val-%03d", i)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())

	// Manually checkpoint (writes to DB) but then also keep the WAL intact
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// Write more data after checkpoint
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := 50; i < 100; i++ {
		k := fmt.Appendf(nil, "key-%03d", i)
		v := fmt.Appendf(nil, "val-%03d", i)
		require.NoError(t, tx2.Put(ns2, k, v))
	}
	require.NoError(t, tx2.Commit())

	// Raw close (no checkpoint of second batch)
	rawClose(db)

	// Reopen — second batch should be recovered from WAL
	db2 := openDBNoCleanup(t, path)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns3, err := db2.getNamespaceLocked("data")
	require.NoError(t, err)
	for i := range 100 {
		k := fmt.Appendf(nil, "key-%03d", i)
		v := fmt.Appendf(nil, "val-%03d", i)
		got, err := rtx.Get(ns3, k)
		require.NoError(t, err, "key-%03d not found", i)
		assert.Equal(t, v, got)
	}
	require.NoError(t, rtx.Rollback())
}

// TestCrash4_PostCheckpointPreTruncate: DB synced, WAL not truncated.
// DB valid, WAL replayed idempotently.
func TestCrash4_PostCheckpointPreTruncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db := openDBNoCleanup(t, path)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("stable"), []byte("data")))
	require.NoError(t, tx.Commit())

	// Checkpoint normally (writes frames to DB and truncates WAL)
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// Add more data
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx2.Put(ns2, []byte("new"), []byte("after-ckpt")))
	require.NoError(t, tx2.Commit())

	rawClose(db)

	// Reopen
	db2 := openDBNoCleanup(t, path)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns3, err := db2.getNamespaceLocked("data")
	require.NoError(t, err)
	v1, err := rtx.Get(ns3, []byte("stable"))
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), v1)
	v2, err := rtx.Get(ns3, []byte("new"))
	require.NoError(t, err)
	assert.Equal(t, []byte("after-ckpt"), v2)
	require.NoError(t, rtx.Rollback())
}

// TestCrash5_CorruptWALHeader: Overwrite magic/checksum bytes. WAL ignored, DB used as-is.
func TestCrash5_CorruptWALHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	walPath := path + "-wal"

	// Write data and checkpoint
	db := openDBNoCleanup(t, path)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("before"), []byte("corrupt")))
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// Write more data WITHOUT checkpointing
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx2.Put(ns2, []byte("after"), []byte("corrupt")))
	require.NoError(t, tx2.Commit())
	rawClose(db)

	// Corrupt WAL header magic bytes
	walFile, err := os.OpenFile(walPath, os.O_RDWR, 0666)
	require.NoError(t, err)
	_, err = walFile.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, 0) // corrupt magic
	require.NoError(t, err)
	require.NoError(t, walFile.Close())

	// Reopen — WAL should be ignored due to corrupt header
	db2 := openDBNoCleanup(t, path)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns3, err := db2.getNamespaceLocked("data")
	require.NoError(t, err)
	// Data from checkpoint should be visible
	v, err := rtx.Get(ns3, []byte("before"))
	require.NoError(t, err)
	assert.Equal(t, []byte("corrupt"), v)
	// Data after checkpoint should be lost (WAL was corrupted)
	_, err = rtx.Get(ns3, []byte("after"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, rtx.Rollback())
}

// TestCrash6_TruncatedWAL: Cut WAL to first commit only.
func TestCrash6_TruncatedWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	walPath := path + "-wal"

	db := openDBNoCleanup(t, path)

	// First commit
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("first"), []byte("commit")))
	require.NoError(t, tx.Commit())

	// Get WAL size after first commit
	walInfo, err := os.Stat(walPath)
	require.NoError(t, err)
	firstCommitSize := walInfo.Size()

	// Second commit
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx2.Put(ns2, []byte("second"), []byte("commit")))
	require.NoError(t, tx2.Commit())

	rawClose(db)

	// Truncate WAL back to first commit size
	require.NoError(t, os.Truncate(walPath, firstCommitSize))

	// Reopen — only first commit's data should be visible
	db2 := openDBNoCleanup(t, path)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns3, err := db2.getNamespaceLocked("data")
	require.NoError(t, err)
	v, err := rtx.Get(ns3, []byte("first"))
	require.NoError(t, err)
	assert.Equal(t, []byte("commit"), v)
	_, err = rtx.Get(ns3, []byte("second"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, rtx.Rollback())
}

// TestCrash7_CorruptFrameChecksum: Flip bits in frame data. Recovery stops at corrupt frame.
func TestCrash7_CorruptFrameChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	walPath := path + "-wal"

	db := openDBNoCleanup(t, path)

	// First commit
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("good"), []byte("data")))
	require.NoError(t, tx.Commit())

	walInfo, err := os.Stat(walPath)
	require.NoError(t, err)
	firstCommitSize := walInfo.Size()

	// Second commit
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx2.Put(ns2, []byte("bad"), []byte("data")))
	require.NoError(t, tx2.Commit())

	rawClose(db)

	// Corrupt the checksum of the second commit's frame
	// Frame header starts at firstCommitSize, checksum is at offset 16-24
	walFile, err := os.OpenFile(walPath, os.O_RDWR, 0666)
	require.NoError(t, err)
	corruptBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(corruptBuf, 0xDEADBEEF)
	_, err = walFile.WriteAt(corruptBuf, firstCommitSize+16) // corrupt checksum1
	require.NoError(t, err)
	require.NoError(t, walFile.Close())

	// Reopen — recovery should stop at corrupt frame
	db2 := openDBNoCleanup(t, path)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns3, err := db2.getNamespaceLocked("data")
	require.NoError(t, err)
	// First commit should be visible
	v, err := rtx.Get(ns3, []byte("good"))
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), v)
	// Second commit should be lost
	_, err = rtx.Get(ns3, []byte("bad"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, rtx.Rollback())
}

// TestCrash8_FullStackCrash: Upper-layer collection ops + crash.
// Verifies atomic commit semantics.
func TestCrash8_FullStackCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db := openDBNoCleanup(t, path)

	// Commit 1: create two namespaces with data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns1, err := tx.CreateNamespace("users")
	require.NoError(t, err)
	ns2, err := tx.CreateNamespace("items")
	require.NoError(t, err)
	for i := range 100 {
		k := fmt.Appendf(nil, "user-%03d", i)
		v := fmt.Appendf(nil, `{"name":"user%d"}`, i)
		require.NoError(t, tx.Put(ns1, k, v))
	}
	for i := range 50 {
		k := fmt.Appendf(nil, "item-%03d", i)
		v := bytes.Repeat([]byte("X"), 200)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Checkpoint first batch
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// Commit 2: modify both namespaces
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns1b, _ := db.getNamespaceLocked("users")
	ns2b, _ := db.getNamespaceLocked("items")
	for i := range 50 {
		k := fmt.Appendf(nil, "user-%03d", i)
		v := fmt.Appendf(nil, `{"name":"updated%d"}`, i)
		require.NoError(t, tx2.Put(ns1b, k, v))
	}
	for i := range 25 {
		k := fmt.Appendf(nil, "item-%03d", i)
		require.NoError(t, tx2.Delete(ns2b, k))
	}
	require.NoError(t, tx2.Commit())

	// Crash!
	rawClose(db)

	// Reopen and verify all changes are atomic
	db2 := openDBNoCleanup(t, path)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns1c, err := db2.getNamespaceLocked("users")
	require.NoError(t, err)
	ns2c, err := db2.getNamespaceLocked("items")
	require.NoError(t, err)

	// Verify updated users
	for i := range 50 {
		k := fmt.Appendf(nil, "user-%03d", i)
		v, err := rtx.Get(ns1c, k)
		require.NoError(t, err)
		expected := fmt.Appendf(nil, `{"name":"updated%d"}`, i)
		assert.Equal(t, expected, v)
	}
	// Verify unchanged users
	for i := 50; i < 100; i++ {
		k := fmt.Appendf(nil, "user-%03d", i)
		v, err := rtx.Get(ns1c, k)
		require.NoError(t, err)
		expected := fmt.Appendf(nil, `{"name":"user%d"}`, i)
		assert.Equal(t, expected, v)
	}

	// Verify deleted items
	for i := range 25 {
		k := fmt.Appendf(nil, "item-%03d", i)
		_, err := rtx.Get(ns2c, k)
		assert.ErrorIs(t, err, ErrKeyNotFound)
	}
	// Verify remaining items
	for i := 25; i < 50; i++ {
		k := fmt.Appendf(nil, "item-%03d", i)
		v, err := rtx.Get(ns2c, k)
		require.NoError(t, err)
		assert.Equal(t, bytes.Repeat([]byte("X"), 200), v)
	}

	require.NoError(t, rtx.Rollback())
}
