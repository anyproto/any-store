//go:build vfs

package btree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Reproducer tests for checkpoint backfill corruption on WriteAt failure.
//
// Bug hypothesis: During WAL checkpoint, if WriteAt to the DB file fails
// mid-backfill, some pages may be partially written. If nBackfill is
// advanced past the failure point (or the WAL is truncated despite the
// partial write), data committed to the WAL could be lost on reopen.
//
// These tests inject WriteAt faults via the VFS mechanism and verify
// that all committed data survives close/reopen.
// ============================================================================

// errInjectedIO is the sentinel error for injected I/O faults.
var errInjectedIO = errors.New("injected I/O fault")

// faultFile wraps a File and can inject WriteAt errors on demand.
type faultFile struct {
	File
	mu             sync.Mutex
	failWriteAt    bool
	writeAtCount   int
	failAfterN     int // fail WriteAt after this many successful calls (-1 = disabled)
	totalWriteAts  atomic.Int64
	failedWriteAts atomic.Int64
}

func (f *faultFile) WriteAt(b []byte, off int64) (int, error) {
	f.totalWriteAts.Add(1)
	f.mu.Lock()
	if f.failWriteAt {
		f.writeAtCount++
		if f.failAfterN >= 0 && f.writeAtCount > f.failAfterN {
			f.mu.Unlock()
			f.failedWriteAts.Add(1)
			return 0, errInjectedIO
		}
	}
	f.mu.Unlock()
	return f.File.WriteAt(b, off)
}

func (f *faultFile) enableFault(afterN int) {
	f.mu.Lock()
	f.failWriteAt = true
	f.failAfterN = afterN
	f.writeAtCount = 0
	f.mu.Unlock()
}

func (f *faultFile) disableFault() {
	f.mu.Lock()
	f.failWriteAt = false
	f.writeAtCount = 0
	f.mu.Unlock()
}

// openDBWithFault opens a DB and returns the faultFile for the DB file.
// The faultFile starts with faults disabled.
func openDBWithFault(t *testing.T, dir string, opts Options) (*DB, *faultFile) {
	t.Helper()
	path := filepath.Join(dir, "test.db")

	var dbFF *faultFile
	SetVFS(VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			// Wrap the *.db file (not WAL or SHM)
			if strings.HasSuffix(name, ".db") && !strings.Contains(name, "-wal") && !strings.Contains(name, "-shm") {
				ff := &faultFile{File: f, failAfterN: -1}
				dbFF = ff
				return ff, nil
			}
			return f, nil
		},
	})
	t.Cleanup(ResetVFS)

	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	require.NotNil(t, dbFF, "DB file should have been opened")
	return db, dbFF
}

// writeDocuments writes n documents to a namespace, each with unique key and data.
func writeDocuments(t *testing.T, db *DB, nsName string, start, count int) {
	t.Helper()
	for i := start; i < start+count; i++ {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.GetNamespace(nsName)
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		// Value encodes the document index and has enough data to span btree cells
		val := make([]byte, 200)
		binary.BigEndian.PutUint32(val, uint32(i))
		copy(val[4:], fmt.Sprintf("doc-%06d-payload", i))
		require.NoError(t, tx.Put(ns, key, val))
		require.NoError(t, tx.Commit())
	}
}

// verifyDocuments reads back all documents and checks for corruption.
func verifyDocuments(t *testing.T, db *DB, nsName string, count int) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns, err := rtx.GetNamespace(nsName)
	require.NoError(t, err)

	for i := 0; i < count; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val, err := rtx.Get(ns, key)
		if !assert.NoErrorf(t, err, "doc %d: Get error", i) {
			continue
		}
		if !assert.Equalf(t, 200, len(val), "doc %d: wrong value length", i) {
			continue
		}
		gotIdx := binary.BigEndian.Uint32(val)
		assert.Equalf(t, uint32(i), gotIdx, "doc %d: wrong index in value", i)
	}

	actualCount, err := rtx.Count(ns)
	require.NoError(t, err)
	assert.Equal(t, count, actualCount, "document count mismatch")
}

// TestCheckpointBackfill_WriteAtFailure_NormalCloseReopen verifies that when
// WriteAt fails during an auto-checkpoint, a normal close/reopen preserves
// all committed data.
//
// Scenario:
//  1. Write 50 documents (triggers auto-checkpoint with low threshold)
//  2. Enable WriteAt fault on DB file
//  3. Write more documents (should trigger another auto-checkpoint that fails)
//  4. Disable WriteAt fault
//  5. Close DB (close-time checkpoint should succeed)
//  6. Reopen DB and verify all documents
func TestCheckpointBackfill_WriteAtFailure_NormalCloseReopen(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.AutoCheckpointAfter = 10 // Low threshold to trigger auto-checkpoint frequently
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)

	// Create namespace
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Phase 1: write documents without fault (auto-checkpoints should succeed)
	writeDocuments(t, db, "data", 0, 50)

	// Phase 2: enable fault and write more (auto-checkpoint should fail)
	ff.enableFault(2) // Allow 2 successful writes, then fail
	writeDocuments(t, db, "data", 50, 20)
	t.Logf("After fault phase: total WriteAts=%d, failed=%d",
		ff.totalWriteAts.Load(), ff.failedWriteAts.Load())

	// Phase 3: disable fault and close (close-time checkpoint should succeed)
	ff.disableFault()
	require.NoError(t, db.Close())

	// Phase 4: reopen without fault injection and verify all data
	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	verifyDocuments(t, db2, "data", 70)
}

// TestCheckpointBackfill_WriteAtFailure_CloseWithFault verifies that when
// WriteAt fails during BOTH auto-checkpoint AND close-time checkpoint,
// the WAL is not truncated and all data survives reopen via WAL replay.
//
// Scenario:
//  1. Write documents with auto-checkpoint disabled
//  2. Enable WriteAt fault on DB file
//  3. Close DB (close-time checkpoint fails, WAL should NOT be truncated)
//  4. Reopen DB (WAL recovery replays all frames)
//  5. Verify all documents
func TestCheckpointBackfill_WriteAtFailure_CloseWithFault(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)

	// Create namespace and write data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	writeDocuments(t, db, "data", 0, 100)

	// Enable fault: close-time checkpoint will fail
	ff.enableFault(0) // Fail immediately on any WriteAt

	// Close — checkpoint should fail, WAL preserved
	err = db.Close()
	// Close itself should succeed even if checkpoint fails
	// (checkpointPassive error is handled, WAL is not truncated)
	t.Logf("Close error (expected nil): %v", err)

	ff.disableFault()

	// Reopen: WAL recovery should replay all frames
	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	verifyDocuments(t, db2, "data", 100)
}

// TestCheckpointBackfill_WriteAtFailure_PartialThenSuccessful verifies the
// scenario where a checkpoint partially writes to the DB, fails, then a
// subsequent checkpoint succeeds and overwrites the partial data.
//
// Scenario:
//  1. Write documents
//  2. Trigger manual checkpoint with WriteAt fault (partial backfill)
//  3. Verify nBackfill was NOT advanced
//  4. Trigger manual checkpoint without fault (should succeed)
//  5. Close and reopen
//  6. Verify all documents
func TestCheckpointBackfill_WriteAtFailure_PartialThenSuccessful(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)

	// Create namespace and write enough data to have multiple WAL frames
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	writeDocuments(t, db, "data", 0, 50)

	// Check nBackfill before fault checkpoint
	nBackfillBefore := db.pager.wal.index.nBackfill.Load()
	t.Logf("nBackfill before faulty checkpoint: %d", nBackfillBefore)
	t.Logf("maxFrame: %d", db.pager.wal.index.maxFrame.Load())

	// Trigger checkpoint with fault: should fail mid-backfill.
	// Fault after 2 page writes so the checkpoint is genuinely partial even for
	// the compact tree the index-btree balancer now produces (50 docs / 200B at
	// pageSize 4096 fit in ~5 pages after balance_nonroot's tighter fill, down
	// from 6). A higher threshold could exceed the page count and let the
	// checkpoint complete, defeating the partial-checkpoint assertion.
	ff.enableFault(2) // Allow 2 page writes, then fail
	err = db.Checkpoint(CheckpointFull)
	assert.Error(t, err, "checkpoint should fail due to WriteAt fault")
	t.Logf("Faulty checkpoint error: %v", err)

	// Verify nBackfill was NOT advanced
	nBackfillAfter := db.pager.wal.index.nBackfill.Load()
	t.Logf("nBackfill after faulty checkpoint: %d", nBackfillAfter)
	assert.Equal(t, nBackfillBefore, nBackfillAfter,
		"nBackfill should not advance after failed checkpoint")

	// Data should still be readable via WAL
	verifyDocuments(t, db, "data", 50)

	// Now checkpoint without fault: should succeed
	ff.disableFault()
	require.NoError(t, db.Checkpoint(CheckpointFull))

	nBackfillFinal := db.pager.wal.index.nBackfill.Load()
	t.Logf("nBackfill after successful checkpoint: %d", nBackfillFinal)
	assert.Greater(t, nBackfillFinal, nBackfillBefore,
		"nBackfill should advance after successful checkpoint")

	// Close and reopen
	require.NoError(t, db.Close())

	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	verifyDocuments(t, db2, "data", 50)
}

// TestCheckpointBackfill_WriteAtFailure_MultipleRounds verifies that multiple
// rounds of failed checkpoints followed by successful writes don't cause
// cumulative corruption.
//
// Scenario:
//  1. Write batch 1, fail checkpoint
//  2. Write batch 2, fail checkpoint
//  3. Write batch 3, fail checkpoint
//  4. Successful close-time checkpoint
//  5. Reopen and verify all batches
func TestCheckpointBackfill_WriteAtFailure_MultipleRounds(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.AutoCheckpointAfter = 5 // Very low threshold
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	totalDocs := 0

	// Round 1: write with fault active
	ff.enableFault(3)
	writeDocuments(t, db, "data", totalDocs, 20)
	totalDocs += 20
	t.Logf("Round 1: failed WriteAts=%d", ff.failedWriteAts.Load())

	// Round 2: more writes with fault still active
	ff.enableFault(1) // even more aggressive fault
	writeDocuments(t, db, "data", totalDocs, 20)
	totalDocs += 20
	t.Logf("Round 2: failed WriteAts=%d", ff.failedWriteAts.Load())

	// Round 3: yet more writes
	ff.enableFault(2)
	writeDocuments(t, db, "data", totalDocs, 20)
	totalDocs += 20
	t.Logf("Round 3: failed WriteAts=%d", ff.failedWriteAts.Load())

	// Verify data is readable while DB is still open
	verifyDocuments(t, db, "data", totalDocs)

	// Close with fault disabled — checkpoint should succeed
	ff.disableFault()
	require.NoError(t, db.Close())

	// Reopen and verify
	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	verifyDocuments(t, db2, "data", totalDocs)
}

// TestCheckpointBackfill_WriteAtFailure_LargeValues tests with large values
// that span overflow pages, making corruption more likely to manifest.
func TestCheckpointBackfill_WriteAtFailure_LargeValues(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.AutoCheckpointAfter = 20
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Write documents with large values (spans overflow pages)
	numDocs := 30
	for i := 0; i < numDocs; i++ {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.GetNamespace("data")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		// Large value: ~8KB per document, will span overflow pages with 4KB page size
		val := make([]byte, 8000)
		binary.BigEndian.PutUint32(val, uint32(i))
		for j := 4; j < len(val); j++ {
			val[j] = byte(i + j)
		}
		require.NoError(t, tx.Put(ns, key, val))
		require.NoError(t, tx.Commit())
	}

	// Enable fault and write more large documents
	ff.enableFault(4)
	for i := numDocs; i < numDocs+10; i++ {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.GetNamespace("data")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 8000)
		binary.BigEndian.PutUint32(val, uint32(i))
		for j := 4; j < len(val); j++ {
			val[j] = byte(i + j)
		}
		require.NoError(t, tx.Put(ns, key, val))
		require.NoError(t, tx.Commit())
	}
	numDocs += 10

	// Disable fault and close
	ff.disableFault()
	require.NoError(t, db.Close())

	// Reopen and verify all documents including large values
	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns, err := rtx.GetNamespace("data")
	require.NoError(t, err)

	for i := 0; i < numDocs; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val, err := rtx.Get(ns, key)
		if !assert.NoErrorf(t, err, "doc %d: Get error", i) {
			continue
		}
		if !assert.Equalf(t, 8000, len(val), "doc %d: wrong value length", i) {
			continue
		}
		gotIdx := binary.BigEndian.Uint32(val)
		assert.Equalf(t, uint32(i), gotIdx, "doc %d: wrong index in value", i)
	}
}

// TestCheckpointBackfill_SilentCorruption_WriteAtGarbles tests the scenario
// where WriteAt "succeeds" but writes garbled data (simulating silent disk
// corruption). This is the most dangerous fault: the checkpoint thinks it
// succeeded, advances nBackfill, and truncates the WAL. On reopen, the DB
// file has corrupt pages with no WAL to replay from.
func TestCheckpointBackfill_SilentCorruption_WriteAtGarbles(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	path := filepath.Join(dir, "test.db")

	garbleActive := atomic.Bool{}
	garbleCount := atomic.Int64{}

	SetVFS(VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			if strings.HasSuffix(name, ".db") && !strings.Contains(name, "-wal") && !strings.Contains(name, "-shm") {
				return &garbleFile{
					File:         f,
					garbleActive: &garbleActive,
					garbleCount:  &garbleCount,
				}, nil
			}
			return f, nil
		},
	})
	t.Cleanup(ResetVFS)

	db, err := testOpen(t, path, opts)
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	writeDocuments(t, db, "data", 0, 50)

	// Enable silent garbling: WriteAt will write zeros instead of real data
	garbleActive.Store(true)
	err = db.Checkpoint(CheckpointFull)
	// Checkpoint returns nil because WriteAt "succeeded" (returned nil error)
	// But the DB file now has garbled pages
	t.Logf("Garbled checkpoint error: %v, garbled writes: %d", err, garbleCount.Load())

	garbleActive.Store(false)

	// nBackfill was advanced (checkpoint "succeeded")
	t.Logf("nBackfill after garbled checkpoint: %d", db.pager.wal.index.nBackfill.Load())
	t.Logf("maxFrame: %d", db.pager.wal.index.maxFrame.Load())

	// Data should still be readable via WAL while the DB is open
	// because readers still have their snapshot
	// (though once nBackfill is advanced, new readers read from DB)
	//
	// This is the REAL vulnerability: after checkpoint advances nBackfill,
	// new readers skip WAL frames and read garbled pages from DB.
	//
	// Close and reopen to see the full damage:
	require.NoError(t, db.Close())

	// Verify the WAL was truncated (checkpoint "succeeded")
	walPath := path + "-wal"
	walInfo, err := os.Stat(walPath)
	if err == nil {
		t.Logf("WAL file after close: %d bytes", walInfo.Size())
		assert.Equal(t, int64(0), walInfo.Size(),
			"WAL should be truncated after 'successful' garbled checkpoint")
	} else {
		t.Logf("WAL file gone: %v", err)
	}

	ResetVFS()
	db2, err := testOpen(t, path, opts)
	if err != nil {
		// Expected: garbled page 1 makes the DB unreadable
		t.Logf("CONFIRMED: Open fails after garbled checkpoint+WAL truncate: %v", err)
		t.Log("This demonstrates the silent corruption vulnerability:")
		t.Log("  1. Checkpoint WriteAt 'succeeds' but writes wrong data")
		t.Log("  2. nBackfill advances, WAL is truncated (no recovery data)")
		t.Log("  3. DB file has garbled pages, Open fails")
		t.Log("  Mitigation: page-level checksums would detect on read")
		return
	}
	defer func() { _ = db2.Close() }()

	// If Open somehow succeeded (garble didn't affect page 1), try reading
	rtx, err := db2.BeginRead()
	if err != nil {
		t.Logf("CONFIRMED: BeginRead fails: %v", err)
		return
	}
	defer func() { _ = rtx.Rollback() }()

	ns, err := rtx.GetNamespace("data")
	if err != nil {
		t.Logf("CONFIRMED: GetNamespace fails: %v", err)
		return
	}

	corruptCount := 0
	for i := 0; i < 50; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val, err := rtx.Get(ns, key)
		if err != nil {
			corruptCount++
			continue
		}
		if len(val) != 200 || binary.BigEndian.Uint32(val) != uint32(i) {
			corruptCount++
		}
	}
	t.Logf("SILENT CORRUPTION: %d/%d documents corrupted", corruptCount, 50)
}

// garbleFile wraps a File and silently writes zeros instead of real data.
type garbleFile struct {
	File
	garbleActive *atomic.Bool
	garbleCount  *atomic.Int64
}

func (f *garbleFile) WriteAt(b []byte, off int64) (int, error) {
	if f.garbleActive.Load() {
		f.garbleCount.Add(1)
		// Write zeros instead of real data
		zeros := make([]byte, len(b))
		return f.File.WriteAt(zeros, off)
	}
	return f.File.WriteAt(b, off)
}

// TestCheckpointBackfill_FdatasyncFailure_NoTruncate verifies that when
// fdatasync fails after backfill (all pages written to DB but not synced),
// nBackfill is NOT advanced and WAL is preserved.
func TestCheckpointBackfill_FdatasyncFailure_NoTruncate(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	path := filepath.Join(dir, "test.db")

	// Use fdatasync fault injection
	fdatasyncFail := atomic.Bool{}
	SetVFS(VFS{
		Fdatasync: func(f File) error {
			if fdatasyncFail.Load() {
				return errInjectedIO
			}
			return f.Sync()
		},
	})
	t.Cleanup(ResetVFS)

	db, err := testOpen(t, path, opts)
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	writeDocuments(t, db, "data", 0, 50)

	nBackfillBefore := db.pager.wal.index.nBackfill.Load()

	// Enable fdatasync fault and try checkpoint
	fdatasyncFail.Store(true)
	err = db.Checkpoint(CheckpointFull)
	assert.Error(t, err, "checkpoint should fail due to fdatasync fault")

	nBackfillAfter := db.pager.wal.index.nBackfill.Load()
	assert.Equal(t, nBackfillBefore, nBackfillAfter,
		"nBackfill should not advance after fdatasync failure")

	// Disable fault, verify data still readable
	fdatasyncFail.Store(false)
	verifyDocuments(t, db, "data", 50)

	// Close and reopen
	require.NoError(t, db.Close())

	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	verifyDocuments(t, db2, "data", 50)
}

// --- Crash simulation helpers ---

// cpFile copies src to dst, creating dst if needed.
func cpFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		return // file may not exist
	}
	require.NoError(t, os.WriteFile(dst, data, 0644))
}

// snapshotDB copies the DB, WAL, and SHM files to a snapshot directory.
// The shm file is <db>-wal-shm (pager appends "-wal", the wal appends
// "-shm"); it exists only for multi-process (mmap shm) DBs — for
// InProcess/InMemory DBs the copy is a silent no-op via cpFile.
//
// WARNING: do not call this while THIS process has a multi-process DB
// attached at dbPath — cpFile's os.ReadFile open+closes an fd on the
// -wal-shm inode, and POSIX fcntl record locks (the shm slot locks and the
// DMS byte) are dropped when any fd for the inode is closed in-process.
// Snapshot from a child process (exec cp) in that case.
func snapshotDB(t *testing.T, dbPath, snapDir string) {
	t.Helper()
	cpFile(t, dbPath, filepath.Join(snapDir, "test.db"))
	cpFile(t, dbPath+"-wal", filepath.Join(snapDir, "test.db-wal"))
	cpFile(t, dbPath+"-wal-shm", filepath.Join(snapDir, "test.db-wal-shm"))
}

// TestCheckpointBackfill_CrashAfterPartialCheckpoint simulates a process
// crash right after a partial checkpoint (WriteAt failure mid-backfill).
func TestCheckpointBackfill_CrashAfterPartialCheckpoint(t *testing.T) {
	dir := t.TempDir()
	snapDir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)
	dbPath := filepath.Join(dir, "test.db")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	writeDocuments(t, db, "data", 0, 30)

	// Phase 1: Successful checkpoint + WAL truncate
	require.NoError(t, db.Checkpoint(CheckpointTruncate))

	t.Logf("After successful TRUNCATE: nBackfill=%d maxFrame=%d",
		db.pager.wal.index.nBackfill.Load(),
		db.pager.wal.index.maxFrame.Load())

	// Phase 2: Write MORE data — creates new WAL frames
	writeDocuments(t, db, "data", 30, 30)

	t.Logf("After more writes: nBackfill=%d maxFrame=%d nFrame=%d",
		db.pager.wal.index.nBackfill.Load(),
		db.pager.wal.index.maxFrame.Load(),
		db.pager.wal.nFrame.Load())

	// Phase 3: Inject fault and run checkpoint — partial backfill
	ff.enableFault(5)
	err = db.Checkpoint(CheckpointFull)
	assert.Error(t, err)
	t.Logf("Partial checkpoint error: %v", err)
	t.Logf("After failed checkpoint: nBackfill=%d maxFrame=%d",
		db.pager.wal.index.nBackfill.Load(),
		db.pager.wal.index.maxFrame.Load())

	ff.disableFault()

	// SNAPSHOT: capture on-disk state (this IS the crash point)
	snapshotDB(t, dbPath, snapDir)
	_ = db.Close()

	// Remove SHM (not durable on crash)
	_ = os.Remove(filepath.Join(snapDir, "test.db-wal-shm"))

	walSnap := filepath.Join(snapDir, "test.db-wal")
	walInfo, err := os.Stat(walSnap)
	require.NoError(t, err)
	t.Logf("Snapshot WAL size: %d bytes", walInfo.Size())
	assert.Greater(t, walInfo.Size(), int64(0), "WAL must exist in crash snapshot")

	// Open from snapshot — simulates recovery after crash
	ResetVFS()
	snapPath := filepath.Join(snapDir, "test.db")
	db2, err := testOpen(t, snapPath, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	t.Logf("After recovery: nBackfill=%d maxFrame=%d",
		db2.pager.wal.index.nBackfill.Load(),
		db2.pager.wal.index.maxFrame.Load())

	verifyDocuments(t, db2, "data", 60)
}

// TestCheckpointBackfill_CrashDuringFdatasync simulates a crash DURING a
// checkpoint that has written ALL pages but hasn't yet synced/advanced nBackfill.
func TestCheckpointBackfill_CrashDuringFdatasync(t *testing.T) {
	dir := t.TempDir()
	snapDir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	dbPath := filepath.Join(dir, "test.db")
	snapshotTaken := atomic.Bool{}
	doSnapshot := atomic.Bool{}

	SetVFS(VFS{
		Fdatasync: func(f File) error {
			if doSnapshot.Load() && !snapshotTaken.Load() {
				snapshotTaken.Store(true)
				snapshotDB(t, dbPath, snapDir)
				t.Log("Snapshot taken during fdatasync")
				return errInjectedIO
			}
			return f.Sync()
		},
	})
	t.Cleanup(ResetVFS)

	db, err := testOpen(t, dbPath, opts)
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	writeDocuments(t, db, "data", 0, 40)

	t.Logf("Before checkpoint: nBackfill=%d maxFrame=%d",
		db.pager.wal.index.nBackfill.Load(),
		db.pager.wal.index.maxFrame.Load())

	doSnapshot.Store(true)
	err = db.Checkpoint(CheckpointFull)
	assert.Error(t, err, "should fail due to fdatasync fault")
	t.Logf("Checkpoint error: %v", err)

	doSnapshot.Store(false)
	_ = db.Close()

	_ = os.Remove(filepath.Join(snapDir, "test.db-wal-shm"))

	ResetVFS()
	snapPath := filepath.Join(snapDir, "test.db")
	db2, err := testOpen(t, snapPath, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	t.Logf("After recovery: nBackfill=%d maxFrame=%d",
		db2.pager.wal.index.nBackfill.Load(),
		db2.pager.wal.index.maxFrame.Load())

	verifyDocuments(t, db2, "data", 40)
}

// TestCheckpointBackfill_StressAutoCheckpointWithFaults is an aggressive
// stress test that interleaves writes and faulty auto-checkpoints.
func TestCheckpointBackfill_StressAutoCheckpointWithFaults(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.AutoCheckpointAfter = 8
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)
	dbPath := filepath.Join(dir, "test.db")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	totalDocs := 0
	for round := 0; round < 10; round++ {
		if round%2 == 0 {
			ff.enableFault(round + 1)
		} else {
			ff.disableFault()
		}
		writeDocuments(t, db, "data", totalDocs, 15)
		totalDocs += 15

		if round%3 == 0 {
			verifyDocuments(t, db, "data", totalDocs)
		}
	}

	t.Logf("Stress: %d docs, failed=%d, nBackfill=%d, maxFrame=%d",
		totalDocs, ff.failedWriteAts.Load(),
		db.pager.wal.index.nBackfill.Load(),
		db.pager.wal.index.maxFrame.Load())

	verifyDocuments(t, db, "data", totalDocs)
	ff.disableFault()

	// Snapshot crash state before clean close
	snapDir := t.TempDir()
	snapshotDB(t, dbPath, snapDir)
	require.NoError(t, db.Close())

	// Test 1: clean close + reopen
	ResetVFS()
	db2, err := testOpen(t, dbPath, opts)
	require.NoError(t, err)
	verifyDocuments(t, db2, "data", totalDocs)
	require.NoError(t, db2.Close())

	// Test 2: crash simulation from snapshot
	_ = os.Remove(filepath.Join(snapDir, "test.db-wal-shm"))
	snapPath := filepath.Join(snapDir, "test.db")
	db3, err := testOpen(t, snapPath, opts)
	require.NoError(t, err)
	defer func() { _ = db3.Close() }()
	verifyDocuments(t, db3, "data", totalDocs)
}

// ==========================================================================
// MinFrame filter tests: verify that the new nBackfill-based filter in
// walIndex.get() correctly redirects readers to DB for checkpointed frames
// and to WAL for uncheckpointed frames.
// ==========================================================================

// TestMinFrameFilter_ReadAfterSuccessfulCheckpoint verifies that after a
// successful checkpoint advances nBackfill, new readers correctly read
// data from the DB file (not WAL).
func TestMinFrameFilter_ReadAfterSuccessfulCheckpoint(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, _ := openDBWithFault(t, dir, opts)

	// Create namespace and write data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	writeDocuments(t, db, "data", 0, 30)

	// Checkpoint successfully — nBackfill should advance
	require.NoError(t, db.Checkpoint(CheckpointFull))

	nBackfill := db.pager.wal.index.nBackfill.Load()
	maxFrame := db.pager.wal.index.maxFrame.Load()
	t.Logf("After checkpoint: nBackfill=%d maxFrame=%d", nBackfill, maxFrame)
	assert.Equal(t, maxFrame, nBackfill, "nBackfill should equal maxFrame after full checkpoint")

	// Read data — the minFrame filter should redirect to DB
	verifyDocuments(t, db, "data", 30)

	// Write more data — this creates new WAL frames above nBackfill
	writeDocuments(t, db, "data", 30, 20)

	maxFrame2 := db.pager.wal.index.maxFrame.Load()
	t.Logf("After more writes: nBackfill=%d maxFrame=%d", nBackfill, maxFrame2)
	assert.Greater(t, maxFrame2, nBackfill, "new frames should be above nBackfill")

	// Read ALL data — old docs from DB, new docs from WAL
	verifyDocuments(t, db, "data", 50)

	// Close and reopen
	require.NoError(t, db.Close())
	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	verifyDocuments(t, db2, "data", 50)
}

// TestMinFrameFilter_PartialCheckpointThenRead verifies that after a partial
// checkpoint (WriteAt failure), the minFrame filter correctly directs readers
// to WAL for ALL frames (since nBackfill was not advanced).
func TestMinFrameFilter_PartialCheckpointThenRead(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	writeDocuments(t, db, "data", 0, 50)

	nBackfillBefore := db.pager.wal.index.nBackfill.Load()
	t.Logf("Before checkpoint: nBackfill=%d maxFrame=%d",
		nBackfillBefore, db.pager.wal.index.maxFrame.Load())

	// Partial checkpoint — fail after writing some pages. Fault after 2 writes
	// so the checkpoint stays partial for the compact tree the index-btree
	// balancer now produces (~5 pages for 50 docs / 200B at pageSize 4096, down
	// from 6); a higher threshold could exceed the page count and complete.
	ff.enableFault(2)
	err = db.Checkpoint(CheckpointFull)
	assert.Error(t, err, "checkpoint should fail")
	ff.disableFault()

	nBackfillAfter := db.pager.wal.index.nBackfill.Load()
	t.Logf("After failed checkpoint: nBackfill=%d maxFrame=%d",
		nBackfillAfter, db.pager.wal.index.maxFrame.Load())
	assert.Equal(t, nBackfillBefore, nBackfillAfter, "nBackfill should not advance")

	// Read data — minFrame should be nBackfill+1 = 1, so ALL WAL frames visible
	verifyDocuments(t, db, "data", 50)

	// Close and reopen
	require.NoError(t, db.Close())
	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	verifyDocuments(t, db2, "data", 50)
}

// TestMinFrameFilter_SuccessThenFailThenRead exercises the sequence:
// successful checkpoint → new writes → failed checkpoint → read.
// This is the most critical scenario for the minFrame filter:
// after the first checkpoint advances nBackfill, old frames are invisible
// to readers. If the second (failed) checkpoint corrupts DB pages that
// correspond to old frames, readers would get wrong data from DB.
func TestMinFrameFilter_SuccessThenFailThenRead(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Phase 1: Write batch 1 and checkpoint successfully.
	writeDocuments(t, db, "data", 0, 100)
	require.NoError(t, db.Checkpoint(CheckpointFull))

	nBackfill1 := db.pager.wal.index.nBackfill.Load()
	t.Logf("After successful checkpoint: nBackfill=%d maxFrame=%d",
		nBackfill1, db.pager.wal.index.maxFrame.Load())
	require.Positive(t, nBackfill1, "first checkpoint should advance nBackfill")

	// Phase 2: Write batch 2 (creates new WAL frames above nBackfill).
	writeDocuments(t, db, "data", 100, 100)

	maxFrame2 := db.pager.wal.index.maxFrame.Load()
	t.Logf("After batch 2: nBackfill=%d maxFrame=%d", nBackfill1, maxFrame2)
	require.Greater(t, maxFrame2, nBackfill1, "batch 2 frames must be above nBackfill")

	// Phase 3: Failed checkpoint — backfills several pages, then fails mid-way.
	// The partial checkpoint writes new page versions to the DB that overwrite
	// the CORRECT data from the successful checkpoint. With ~100 new docs the
	// backfill writes >>2 distinct pages, so failing after 2 leaves a genuine
	// partial state. (One WriteAt per distinct page — buildBackfillMap dedup,
	// wal.go:2862; the per-frame thresholds the old test used no longer fire.)
	ff.enableFault(2) // write 2 pages, then fail on the 3rd
	err = db.Checkpoint(CheckpointFull)
	require.Error(t, err, "checkpoint should fail")
	assert.ErrorIs(t, err, errInjectedIO)
	require.Positive(t, ff.failedWriteAts.Load(), "the injected fault must have actually fired")
	t.Logf("Failed checkpoint fired fault: err=%v failedWriteAts=%d", err, ff.failedWriteAts.Load())
	ff.disableFault()

	nBackfill2 := db.pager.wal.index.nBackfill.Load()
	t.Logf("After failed checkpoint: nBackfill=%d maxFrame=%d",
		nBackfill2, db.pager.wal.index.maxFrame.Load())
	assert.Equal(t, nBackfill1, nBackfill2,
		"nBackfill should NOT advance after failed checkpoint")

	// Phase 4: Read ALL data — this is the critical test!
	// Batch 1 docs: their WAL frames are BELOW nBackfill (invisible via minFrame).
	//   Reader reads these from DB. DB should have correct data from phase 1 checkpoint.
	// Batch 2 docs: their WAL frames are ABOVE nBackfill (visible via minFrame).
	//   Reader reads these from WAL.
	verifyDocuments(t, db, "data", 200)

	// Phase 5: Close (with possible re-checkpoint) and reopen
	require.NoError(t, db.Close())
	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	verifyDocuments(t, db2, "data", 200)
}

// TestMinFrameFilter_MultipleCheckpointCycles tests many cycles of
// successful and failed checkpoints to find cumulative issues.
func TestMinFrameFilter_MultipleCheckpointCycles(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.AutoCheckpointAfter = 10
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	totalDocs := 0
	for round := 0; round < 20; round++ {
		// Alternate between fault and no-fault rounds
		if round%3 == 0 {
			ff.enableFault(round/3 + 1)
		} else {
			ff.disableFault()
		}

		// Write a batch
		batchSize := 5 + round%7
		writeDocuments(t, db, "data", totalDocs, batchSize)
		totalDocs += batchSize

		// Explicit checkpoint on some rounds
		if round%4 == 1 {
			ff.disableFault()
			_ = db.Checkpoint(CheckpointFull)
		}

		// Verify ALL data every 5 rounds
		if round%5 == 4 {
			verifyDocuments(t, db, "data", totalDocs)
		}
	}

	// Final verification
	ff.disableFault()
	verifyDocuments(t, db, "data", totalDocs)

	// Close and reopen
	require.NoError(t, db.Close())
	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	verifyDocuments(t, db2, "data", totalDocs)
}

// TestMinFrameFilter_FdatasyncNoop_CrashAfterTruncate tests the scenario
// where fdatasync is a noop (returns nil without syncing) and a crash
// happens after the WAL is truncated. This is the most dangerous scenario:
// checkpoint "succeeds" (fdatasync returns nil), nBackfill advances, WAL
// is truncated on close. If crash happens and OS buffer cache is lost,
// the DB may have incomplete data with no WAL to recover from.
func TestMinFrameFilter_FdatasyncNoop_CrashAfterTruncate(t *testing.T) {
	dir := t.TempDir()
	snapDir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	path := filepath.Join(dir, "test.db")

	// fdatasync is a noop — returns nil without syncing
	SetVFS(VFS{
		Fdatasync: func(f File) error {
			return nil // noop! data stays in OS buffer cache
		},
	})
	t.Cleanup(ResetVFS)

	db, err := testOpen(t, path, opts)
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	writeDocuments(t, db, "data", 0, 50)

	// Checkpoint with noop fdatasync — "succeeds" but data not durable
	require.NoError(t, db.Checkpoint(CheckpointFull))

	nBackfill := db.pager.wal.index.nBackfill.Load()
	t.Logf("After noop-fdatasync checkpoint: nBackfill=%d maxFrame=%d",
		nBackfill, db.pager.wal.index.maxFrame.Load())

	// Snapshot BEFORE close — this captures the "crash" state
	// In a real crash, the OS buffer cache would be lost. We can't simulate
	// that perfectly, but we can verify the WAL behavior.
	snapshotDB(t, path, snapDir)

	// Close normally (which truncates WAL since checkpoint "succeeded")
	require.NoError(t, db.Close())

	// The WAL should be truncated (checkpoint returned nil)
	walPath := path + "-wal"
	if walInfo, err := os.Stat(walPath); err == nil {
		t.Logf("WAL after close: %d bytes", walInfo.Size())
	}

	// The crash snapshot should still have the WAL
	snapWal := filepath.Join(snapDir, "test.db-wal")
	if walInfo, err := os.Stat(snapWal); err == nil {
		t.Logf("Snapshot WAL: %d bytes", walInfo.Size())
	}

	// Open from the snapshot (simulating crash recovery)
	_ = os.Remove(filepath.Join(snapDir, "test.db-wal-shm"))
	ResetVFS()
	snapPath := filepath.Join(snapDir, "test.db")
	db2, err := testOpen(t, snapPath, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	// This should work because the snapshot was taken BEFORE WAL truncation,
	// so WAL recovery can replay all frames.
	verifyDocuments(t, db2, "data", 50)
}

// TestMinFrameFilter_OverwriteCheckpointedPages tests the scenario where
// a failed checkpoint writes NEWER versions of already-checkpointed pages
// to the DB. Since the failed checkpoint doesn't advance nBackfill, the
// next reader should still read from WAL for these pages.
//
// This is a subtle case: page P was written by frame 3 (checkpointed, nBackfill=5).
// A new write creates frame 8 for page P. A failed checkpoint writes frame 8's
// data to page P's offset in DB. nBackfill stays at 5. Reader: minFrame = 6.
// For page P, frame 8 >= 6 → reads from WAL → correct.
// But what if the DB now has frame 8's data for page P, and the reader reads
// from DB for some other reason (cache miss for an uncacheable page)?
func TestMinFrameFilter_OverwriteCheckpointedPages(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Phase 1: Write docs 0-99 and checkpoint successfully.
	writeDocuments(t, db, "data", 0, 100)
	require.NoError(t, db.Checkpoint(CheckpointFull))
	nBackfill1 := db.pager.wal.index.nBackfill.Load()
	t.Logf("Phase 1: nBackfill=%d", nBackfill1)
	require.Positive(t, nBackfill1)

	// Phase 2: UPDATE docs 0-49 (overwrites same keys, creating new WAL frames).
	for i := 0; i < 50; i++ {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.GetNamespace("data")
		require.NoError(t, err)
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 200)
		binary.BigEndian.PutUint32(val, uint32(i))
		copy(val[4:], fmt.Sprintf("UPDATED-doc-%06d-v2", i))
		require.NoError(t, tx.Put(ns, key, val))
		require.NoError(t, tx.Commit())
	}

	maxFrame2 := db.pager.wal.index.maxFrame.Load()
	t.Logf("Phase 2: nBackfill=%d maxFrame=%d", nBackfill1, maxFrame2)
	require.Greater(t, maxFrame2, nBackfill1)

	// Phase 3: Failed checkpoint — writes some updated pages to the DB, then
	// fails. Updating 50 docs dirties several distinct pages, so failing after
	// 2 page-writes leaves a genuine partial overwrite. (One WriteAt per
	// distinct page after the buildBackfillMap dedup at wal.go:2862; the old
	// per-frame threshold of 4 no longer fires on a small DB.)
	ff.enableFault(2)
	err = db.Checkpoint(CheckpointFull)
	require.Error(t, err)
	assert.ErrorIs(t, err, errInjectedIO)
	require.Positive(t, ff.failedWriteAts.Load(), "the injected fault must have actually fired")
	t.Logf("Phase 3: failed checkpoint fired fault, failedWriteAts=%d", ff.failedWriteAts.Load())
	ff.disableFault()

	nBackfill2 := db.pager.wal.index.nBackfill.Load()
	t.Logf("Phase 3: nBackfill=%d (should equal phase 1)", nBackfill2)
	assert.Equal(t, nBackfill1, nBackfill2,
		"nBackfill must NOT advance after a failed checkpoint")

	// Phase 4: Verify docs 0-49 have UPDATED values.
	// nBackfill did not advance, so the update frames (above nBackfill) are
	// found in the WAL by the minFrame filter — even though phase 3 wrote some
	// of those updated pages into the DB file.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns, err := rtx.GetNamespace("data")
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val, err := rtx.Get(ns, key)
		require.NoError(t, err)
		valStr := string(val[4:])
		assert.Contains(t, valStr, "UPDATED",
			"doc %d should have updated value, got: %s", i, valStr[:min(40, len(valStr))])
	}

	// Docs 50-99 (never updated) should still be readable from DB via the filter.
	for i := 50; i < 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val, err := rtx.Get(ns, key)
		require.NoError(t, err)
		gotIdx := binary.BigEndian.Uint32(val)
		assert.Equal(t, uint32(i), gotIdx, "doc %d index mismatch", i)
	}
	_ = rtx.Rollback()

	// Phase 5: Close and reopen
	require.NoError(t, db.Close())
	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	// Verify updated docs survived, and untouched docs are intact.
	rtx2, err := db2.BeginRead()
	require.NoError(t, err)
	ns2, err := rtx2.GetNamespace("data")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val, err := rtx2.Get(ns2, key)
		require.NoError(t, err)
		valStr := string(val[4:])
		assert.Contains(t, valStr, "UPDATED",
			"doc %d should still have updated value after reopen", i)
	}
	for i := 50; i < 100; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val, err := rtx2.Get(ns2, key)
		require.NoError(t, err)
		assert.Equal(t, uint32(i), binary.BigEndian.Uint32(val), "doc %d index mismatch after reopen", i)
	}
	_ = rtx2.Rollback()
}

// TestMinFrameFilter_NonInProcess tests the minFrame filter behavior
// with non-InProcess mode (mmap SHM).
func TestMinFrameFilter_NonInProcess(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = false // mmap SHM mode

	db, ff := openDBWithFault(t, dir, opts)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Write and checkpoint
	writeDocuments(t, db, "data", 0, 30)
	require.NoError(t, db.Checkpoint(CheckpointFull))

	nBackfill := db.pager.wal.index.nBackfill.Load()
	t.Logf("After checkpoint: nBackfill=%d", nBackfill)

	// Write more and do a failed checkpoint
	writeDocuments(t, db, "data", 30, 20)
	ff.enableFault(3)
	_ = db.Checkpoint(CheckpointFull)
	ff.disableFault()

	// Verify all data
	verifyDocuments(t, db, "data", 50)

	// Close and reopen
	require.NoError(t, db.Close())
	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	verifyDocuments(t, db2, "data", 50)
}

// ==========================================================================
// Short-write tests: verify that WriteAt returning (n < len(b), nil)
// — a partial write without error — is correctly detected.
// ==========================================================================

// shortWriteFile wraps a File and silently writes fewer bytes than requested
// without returning an error. This simulates a custom File implementation
// that returns (n < len(b), nil) from WriteAt — a partial write.
type shortWriteFile struct {
	File
	mu          sync.Mutex
	shortWrite  bool
	shortWrites atomic.Int64
}

func (f *shortWriteFile) WriteAt(b []byte, off int64) (int, error) {
	f.mu.Lock()
	short := f.shortWrite
	f.mu.Unlock()
	if short && len(b) > 64 {
		// Write only the first half of the data. The rest of the page
		// at this offset retains stale/zero data.
		f.shortWrites.Add(1)
		half := len(b) / 2
		return f.File.WriteAt(b[:half], off)
	}
	return f.File.WriteAt(b, off)
}

func (f *shortWriteFile) enableShortWrite() {
	f.mu.Lock()
	f.shortWrite = true
	f.mu.Unlock()
}

func (f *shortWriteFile) disableShortWrite() {
	f.mu.Lock()
	f.shortWrite = false
	f.mu.Unlock()
}

// openDBWithShortWrite opens a DB and returns the shortWriteFile for the DB file.
func openDBWithShortWrite(t *testing.T, dir string, opts Options) (*DB, *shortWriteFile) {
	t.Helper()
	path := filepath.Join(dir, "test.db")

	var dbSWF *shortWriteFile
	SetVFS(VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			if strings.HasSuffix(name, ".db") && !strings.Contains(name, "-wal") && !strings.Contains(name, "-shm") {
				sw := &shortWriteFile{File: f}
				dbSWF = sw
				return sw, nil
			}
			return f, nil
		},
	})
	t.Cleanup(ResetVFS)

	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	require.NotNil(t, dbSWF, "DB file should have been opened")
	return db, dbSWF
}

// TestCheckpointBackfill_ShortWrite_NoError demonstrates data loss when
// WriteAt returns (n < len(b), nil) — a partial write without error.
//
// The checkpoint code discards the n return value from WriteAt:
//
//	if _, err := dbFile.WriteAt(pageData, pageOffset); err != nil { ... }
//
// A short write (no error) writes partial page data to the DB. The checkpoint
// "succeeds", nBackfill advances, and the close-time checkpoint truncates the WAL.
// On reopen, readers get corrupt DB pages with no WAL to recover from.
//
// The minFrame filter (walIndex.get()) makes this worse: after nBackfill advances,
// readers skip WAL frames and read directly from the corrupt DB.
func TestCheckpointBackfill_ShortWrite_NoError(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, swf := openDBWithShortWrite(t, dir, opts)

	// Create namespace
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Phase 1: Write batch 1 and checkpoint successfully
	writeDocuments(t, db, "data", 0, 30)
	require.NoError(t, db.Checkpoint(CheckpointFull))

	nBackfill1 := db.pager.wal.index.nBackfill.Load()
	maxFrame1 := db.pager.wal.index.maxFrame.Load()
	t.Logf("After successful checkpoint: nBackfill=%d maxFrame=%d", nBackfill1, maxFrame1)
	require.Equal(t, maxFrame1, nBackfill1, "nBackfill should equal maxFrame after full checkpoint")

	// Phase 2: Write batch 2 (new WAL frames above nBackfill)
	writeDocuments(t, db, "data", 30, 30)

	maxFrame2 := db.pager.wal.index.maxFrame.Load()
	t.Logf("After batch 2: nBackfill=%d maxFrame=%d", nBackfill1, maxFrame2)
	require.Greater(t, maxFrame2, nBackfill1, "new frames should be above nBackfill")

	// Phase 3: Enable short writes on DB file, then checkpoint.
	// The checkpoint "succeeds" (no error) but writes partial pages to DB.
	// nBackfill advances to maxFrame despite the partial writes.
	swf.enableShortWrite()
	err = db.Checkpoint(CheckpointFull)
	swf.disableShortWrite()

	t.Logf("Checkpoint with short writes: err=%v, shortWrites=%d",
		err, swf.shortWrites.Load())

	nBackfill2 := db.pager.wal.index.nBackfill.Load()
	t.Logf("After short-write checkpoint: nBackfill=%d maxFrame=%d",
		nBackfill2, db.pager.wal.index.maxFrame.Load())

	if err == nil && swf.shortWrites.Load() > 0 {
		t.Log("BUG: short writes silently accepted, nBackfill advanced!")
	}

	// Phase 4: Close (WAL truncated since nBackfill >= maxFrame) and reopen.
	// On close: checkpointPassive() → nBackfill >= maxFrame → nil → WAL truncated.
	// On reopen: no WAL → reads from corrupt DB pages → DATA LOSS.
	require.NoError(t, db.Close())

	ResetVFS()
	path := filepath.Join(dir, "test.db")
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	// Phase 5: Verify ALL data — if the bug exists, batch 2 docs (30-59) will be
	// corrupt because partial pages were written to DB and WAL was truncated.
	verifyDocuments(t, db2, "data", 60)
}

// TestCheckpointBackfill_ShortWrite_InlineRead verifies that a SHORT write
// during checkpoint backfill (WriteAt returning n < len(b) with a nil error) is
// detected and handled crash-safely, and that a fresh handle reading the same
// files afterwards never observes the short-written page.
//
// The backfill loop checks the byte count of every page write and converts a
// short write into io.ErrShortWrite (wal.go:3263-3266). That aborts the
// backfill before nBackfill advances (the error path returns at wal.go:3271
// ahead of the nBackfill.Store at wal.go:3285), so the WAL is preserved.
//
// We then exercise the inline-read-after-short-write path by reopening a FRESH
// handle from an on-disk snapshot captured right after the short-write
// checkpoint: the DB file holds one half-written page, but the intact WAL
// shadows it. The reader sees every document correctly — batch-1 pages from the
// DB (written in full by the successful phase-1 checkpoint) and batch-2 pages
// from the WAL. Zero silent corruption.
//
// (Previously this test opened a second in-process handle to the same path
// while the first was still open, which the Bug-16 double-open guard at
// db.go:385 rejects. It is restructured to close the first handle and recover
// from a snapshot.)
func TestCheckpointBackfill_ShortWrite_InlineRead(t *testing.T) {
	dir := t.TempDir()
	snapDir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = false // non-InProcess to exercise mmap SHM path

	db, swf := openDBWithShortWrite(t, dir, opts)
	dbPath := filepath.Join(dir, "test.db")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Phase 1: write batch 1 and checkpoint successfully (these pages are fully
	// and correctly written to the DB file).
	writeDocuments(t, db, "data", 0, 40)
	require.NoError(t, db.Checkpoint(CheckpointFull))

	nBackfill := db.pager.wal.index.nBackfill.Load()
	t.Logf("After successful checkpoint: nBackfill=%d", nBackfill)
	require.Positive(t, nBackfill)

	// Phase 2: write batch 2 (new WAL frames above nBackfill).
	writeDocuments(t, db, "data", 40, 40)
	maxFrame := db.pager.wal.index.maxFrame.Load()
	require.Greater(t, maxFrame, nBackfill)

	// Phase 3: checkpoint while WriteAt silently truncates each page it writes.
	// The first backfilled page triggers io.ErrShortWrite, aborting the backfill.
	swf.enableShortWrite()
	err = db.Checkpoint(CheckpointFull)
	swf.disableShortWrite()

	t.Logf("Short-write checkpoint: err=%v, shortWrites=%d", err, swf.shortWrites.Load())
	require.Error(t, err, "a short write must surface as a checkpoint error")
	assert.ErrorIs(t, err, io.ErrShortWrite, "short write should be reported as io.ErrShortWrite")
	require.Positive(t, swf.shortWrites.Load(), "the short-write fault must have actually fired")

	// nBackfill must NOT advance: the short write was caught before the store.
	nBackfill2 := db.pager.wal.index.nBackfill.Load()
	t.Logf("After short-write checkpoint: nBackfill=%d maxFrame=%d",
		nBackfill2, db.pager.wal.index.maxFrame.Load())
	assert.Equal(t, nBackfill, nBackfill2,
		"nBackfill must not advance after a short-write checkpoint")

	// Snapshot the on-disk state NOW: DB has a half-written page, WAL is intact.
	snapshotDB(t, dbPath, snapDir)

	// The WAL must still hold the committed frames (not truncated by the failure).
	walInfo, err := os.Stat(filepath.Join(snapDir, "test.db-wal"))
	require.NoError(t, err)
	assert.Greater(t, walInfo.Size(), int64(0), "WAL must remain intact after the short write")

	require.NoError(t, db.Close())

	// Fresh handle / crash recovery from the snapshot. With the first handle
	// closed and the registry cleared, opening the snapshot path is allowed.
	_ = os.Remove(filepath.Join(snapDir, "test.db-wal-shm"))
	ResetVFS()
	ResetOpenRegistry()

	snapPath := filepath.Join(snapDir, "test.db")
	db2, err := testOpen(t, snapPath, opts)
	require.NoError(t, err, "snapshot must reopen despite the half-written DB page")
	defer func() { _ = db2.Close() }()

	t.Logf("After recovery: nBackfill=%d maxFrame=%d",
		db2.pager.wal.index.nBackfill.Load(), db2.pager.wal.index.maxFrame.Load())

	// Inline read after the short write: every doc is correct, including the
	// page that was half-written to the DB (shadowed by the intact WAL).
	verifyDocuments(t, db2, "data", 80)
}

// ==========================================================================
// Regression tests: reproduce the EXACT bugs fixed in recent commits.
// These tests simulate the old buggy behavior to verify the fixes work.
// ==========================================================================

// TestRegression_Bug11_CloseUnconditionallyTruncatesWAL reproduces the Bug 11
// scenario (fixed in be6af73): pager.close() unconditionally truncated the WAL
// after a partial checkpoint, destroying uncopied frames.
//
// Old buggy code in pager.close():
//
//	cpErr := p.wal.checkpointPassive(p.file, p.master)
//	_ = cpErr                // BUG: error ignored!
//	p.wal.truncateFile()     // BUG: always truncated, even on failure!
//
// Fixed code:
//
//	cpErr := p.wal.checkpointPassive(p.file, p.master)
//	if cpErr == nil {
//	    p.wal.truncateFile() // Only truncate on success
//	}
func TestRegression_Bug11_CloseUnconditionallyTruncatesWAL(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)
	dbPath := filepath.Join(dir, "test.db")

	// Create namespace and write data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	writeDocuments(t, db, "data", 0, 50)

	// Enable fault: close-time checkpoint will FAIL
	ff.enableFault(0)

	// Close the DB — the fix ensures WAL is NOT truncated
	_ = db.Close()

	// Verify WAL was preserved (NOT truncated)
	walPath := dbPath + "-wal"
	walInfo, err := os.Stat(walPath)
	require.NoError(t, err)
	t.Logf("WAL size after close with fault: %d bytes", walInfo.Size())
	assert.Greater(t, walInfo.Size(), int64(0),
		"BUG 11 REGRESSION: WAL should NOT be truncated when checkpoint fails")

	// Reopen: WAL recovery should replay all frames
	ResetVFS()
	db2, err := testOpen(t, dbPath, opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	verifyDocuments(t, db2, "data", 50)
}

// TestRegression_Bug11_Simulation is the regression test for the Bug 11 FIX
// (pager.close truncate-gate at pager.go:2698; partial-checkpoint handling at
// wal.go:2820-2840 and the backfill error path at wal.go:3271-3274).
//
// Bug 11 (old, buggy behavior) was: a checkpoint that failed PARTWAY through
// backfill still let pager.close unconditionally truncate the WAL, discarding
// the frames that had not yet been copied to the DB file → data loss on reopen.
//
// This test proves the bug stays fixed. It arranges a DB large enough that a
// checkpoint backfills several distinct pages (~17 for 200 docs — see the
// dedup in buildBackfillMap at wal.go:2862), injects a WriteAt fault that fires
// after only 8 of those page-writes, and asserts the engine does the SAFE thing:
//
//   - the checkpoint returns the injected I/O error (backfill aborts);
//   - nBackfill is NOT advanced past the safely-written prefix
//     (it stays at its pre-checkpoint value — the backfill error path returns
//     before the nBackfill.Store at wal.go:3285);
//   - the WAL file is NOT truncated (it still holds every committed frame);
//   - simulating a process crash at that exact point (reopen from an on-disk
//     snapshot, no clean close) recovers ALL data with zero loss, because WAL
//     recovery resets nBackfill to 0 (wal.go:1855) and replays every frame,
//     shadowing the partially-written DB pages.
//
// NOTE: nBackfillAttempted IS expected to advance to mxSafeFrame (wal.go:3133):
// it is only a crash-safety hint and never causes recovery to skip frames.
func TestRegression_Bug11_Simulation(t *testing.T) {
	dir := t.TempDir()
	snapDir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	db, ff := openDBWithFault(t, dir, opts)
	dbPath := filepath.Join(dir, "test.db")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// 200 docs → a CheckpointFull backfills ~17 distinct pages (frames dedup
	// to pages via buildBackfillMap). Enough that failing after 8 page-writes
	// leaves a genuine PARTIAL backfill (some pages written, some not).
	writeDocuments(t, db, "data", 0, 200)

	nBackfillBefore := db.pager.wal.index.nBackfill.Load()
	maxFrame := db.pager.wal.index.maxFrame.Load()
	require.Greater(t, maxFrame, uint32(0))
	t.Logf("Before faulty checkpoint: nBackfill=%d maxFrame=%d", nBackfillBefore, maxFrame)

	// Inject a fault that fails on the 9th WriteAt: 8 pages are written to the
	// DB file, then the backfill aborts mid-stream — a real partial checkpoint.
	ff.enableFault(8)
	err = db.Checkpoint(CheckpointFull)
	require.Error(t, err, "partial checkpoint must surface the backfill error")
	assert.ErrorIs(t, err, errInjectedIO, "error should be the injected I/O fault")
	require.Positive(t, ff.failedWriteAts.Load(), "the injected fault must have actually fired")
	t.Logf("Partial checkpoint fired fault: err=%v failedWriteAts=%d totalWriteAts=%d",
		err, ff.failedWriteAts.Load(), ff.totalWriteAts.Load())
	ff.disableFault()

	// SAFE behavior #1: nBackfill must NOT advance past the written prefix.
	nBackfillAfter := db.pager.wal.index.nBackfill.Load()
	assert.Equal(t, nBackfillBefore, nBackfillAfter,
		"BUG 11 REGRESSION: nBackfill must not advance after a failed checkpoint")
	t.Logf("After partial checkpoint: nBackfill=%d (unchanged), nBackfillAttempted=%d",
		nBackfillAfter, db.pager.wal.index.nBackfillAttempted.Load())

	// SAFE behavior #2: data is still fully readable on the live handle
	// (readers see all frames in the WAL, none of which were discarded).
	verifyDocuments(t, db, "data", 200)

	// Snapshot the on-disk state RIGHT HERE — this is the crash point: a DB file
	// with 8 of ~17 pages backfilled, plus an intact WAL.
	snapshotDB(t, dbPath, snapDir)

	// SAFE behavior #3: the WAL on disk was NOT truncated by the failed checkpoint.
	walSnap := filepath.Join(snapDir, "test.db-wal")
	walInfo, err := os.Stat(walSnap)
	require.NoError(t, err)
	assert.Greater(t, walInfo.Size(), int64(0),
		"BUG 11 REGRESSION: WAL must remain intact after a failed checkpoint")
	t.Logf("Crash-snapshot WAL size: %d bytes", walInfo.Size())

	_ = db.Close()

	// Simulate a crash: drop the volatile SHM and recover purely from the
	// snapshot's DB + WAL (no clean-close checkpoint runs here).
	_ = os.Remove(filepath.Join(snapDir, "test.db-wal-shm"))
	ResetVFS()
	ResetOpenRegistry()

	snapPath := filepath.Join(snapDir, "test.db")
	db2, err := testOpen(t, snapPath, opts)
	require.NoError(t, err, "DB must reopen after a partial-checkpoint crash")
	defer func() { _ = db2.Close() }()

	// Recovery resets nBackfill to 0 and rebuilds the page map from every frame.
	t.Logf("After crash recovery: nBackfill=%d maxFrame=%d",
		db2.pager.wal.index.nBackfill.Load(), db2.pager.wal.index.maxFrame.Load())
	assert.Equal(t, uint32(0), db2.pager.wal.index.nBackfill.Load(),
		"recovery should reset nBackfill so all WAL frames are replayed")

	// SAFE behavior #4: ZERO data loss — every document survives the crash.
	verifyDocuments(t, db2, "data", 200)
}
