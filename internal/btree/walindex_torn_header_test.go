//go:build (linux || darwin) && (amd64 || arm64)

package btree

// A wal-index header left torn by a writer that died between its two header
// copies, while other processes keep the database open. SQLite repairs it
// from whichever connection next reads the header: walIndexReadHdr takes the
// WRITE lock and runs walIndexRecover (wal.c:2640-2715), and walCheckpoint's
// caller reads the header that way before it copies anything (wal.c:4359).
//
// The cross-process tests re-exec the test binary (the pattern of
// multiprocess_dms_test.go): the child is the writer that dies, the parent
// the process that survives it.

import (
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var tornHdrOpts = Options{
	PageSize:              4096,
	CacheSize:             100,
	InProcess:             false,
	DisableAutoCheckpoint: true,
}

const (
	tornHdrSeedRows = 20 // committed by the survivor before the writer attaches
	tornHdrF1End    = 60 // the writer's first commit: rows [20, 60)
	tornHdrF2End    = 90 // the commit whose header publish is torn: rows [60, 90)
)

// tornHeaderChildMain is the dying writer. It commits F1, commits F2, then
// puts header copy 1 back to F1's bytes and exits without closing. Writers
// publish copy 2 first and copy 1 second (walIndex.writeHeader), so the shm
// is left exactly as a crash between the two copies of F2's publish leaves
// it: F2's frames and hash entries are in place, copy 2 is F2's header and
// copy 1 is still F1's.
func tornHeaderChildMain(t *testing.T, dbPath string) {
	db, err := testOpen(t, dbPath, tornHdrOpts)
	require.NoError(t, err)

	dmsPutRange(t, db, "t1", tornHdrSeedRows, tornHdrF1End)
	region, err := db.pager.wal.index.shm.region(0, true)
	require.NoError(t, err)
	var f1Copy [walIndexHdrSize]byte
	copy(f1Copy[:], region[0:walIndexHdrSize])

	dmsPutRange(t, db, "t1", tornHdrF1End, tornHdrF2End)
	copy(region[0:walIndexHdrSize], f1Copy[:])
	_, valid := db.pager.wal.index.readHeader()
	require.False(t, valid, "child setup: the header must be torn")

	// Die here: no Close, no checkpoint, no shm cleanup.
	os.Exit(0)
}

// runTornHeaderWriter opens the database in this process (the survivor),
// commits the seed rows, and runs the dying writer to completion. The
// survivor starts no transaction after its seed commit, so its process-local
// frame count stays at the seed.
func runTornHeaderWriter(t *testing.T, testName, envVar string) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, tornHdrOpts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	dmsPutRange(t, db, "t1", 0, tornHdrSeedRows)
	seedFrame := db.pager.wal.index.mxCommitFrame.LoadLocal()
	require.Greater(t, seedFrame, uint32(0))

	cmd := exec.Command(os.Args[0], "-test.run=^"+testName+"$", "-test.v", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), envVar+"="+path)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "writer process failed:\n%s", out)

	_, valid := db.pager.wal.index.readHeader()
	require.False(t, valid, "setup: the survivor must see a torn header")
	require.Equal(t, seedFrame, db.pager.wal.index.mxCommitFrame.LoadLocal(),
		"setup: the survivor's local frame count must still be the seed's")
	return db, path
}

// requireTornHeaderRowsAfterReopen closes the survivor and checks the rows
// from a fresh open: what the checkpoint left in the database file and the
// WAL is all there is.
func requireTornHeaderRowsAfterReopen(t *testing.T, db *DB, path string) {
	t.Helper()
	require.NoError(t, db.Close())
	db2, err := testOpen(t, path, tornHdrOpts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	requireTornHeaderRows(t, db2)
	require.NoError(t, db2.IntegrityCheck())
}

// requireTornHeaderRows checks that every row the writer committed is
// readable with its content: F1 was acknowledged, and F2's frames reached the
// WAL with their commit frame, so recovery includes it.
func requireTornHeaderRows(t *testing.T, db *DB) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	ns, err := rtx.GetNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < tornHdrF2End; i++ {
		val, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, uint32(i)))
		require.NoError(t, err, "row %d", i)
		require.Len(t, val, 300, "row %d", i)
		require.Equal(t, byte(i), val[0], "row %d", i)
	}
}

// TestTornWalIndexHeader_CheckpointKeepsPeerCommits: the survivor checkpoints
// with TRUNCATE without having read anything since the writer's commits. It
// must rebuild the wal-index and backfill the writer's frames, not take its
// own stale frame count for the end of the log and truncate the rest away.
func TestTornWalIndexHeader_CheckpointKeepsPeerCommits(t *testing.T) {
	const env = "TEST_TORNHDR_CKPT_DB_PATH"
	if dbPath := os.Getenv(env); dbPath != "" {
		tornHeaderChildMain(t, dbPath)
		return
	}
	db, path := runTornHeaderWriter(t, "TestTornWalIndexHeader_CheckpointKeepsPeerCommits", env)

	require.NoError(t, db.Checkpoint(CheckpointTruncate))

	_, valid := db.pager.wal.index.readHeader()
	require.True(t, valid, "the checkpoint must leave a valid header")
	requireTornHeaderRowsAfterReopen(t, db, path)
}

// TestTornWalIndexHeader_PassiveCheckpointKeepsPeerCommits: the same through
// the PASSIVE mode, which does not hold the write lock when it reads the
// header and so takes it for the repair (walIndexReadHdr, wal.c:2685-2697).
func TestTornWalIndexHeader_PassiveCheckpointKeepsPeerCommits(t *testing.T) {
	const env = "TEST_TORNHDR_PASSIVE_DB_PATH"
	if dbPath := os.Getenv(env); dbPath != "" {
		tornHeaderChildMain(t, dbPath)
		return
	}
	db, path := runTornHeaderWriter(t, "TestTornWalIndexHeader_PassiveCheckpointKeepsPeerCommits", env)

	require.NoError(t, db.Checkpoint(CheckpointPassive))

	hdr, valid := db.pager.wal.index.readHeader()
	require.True(t, valid, "the checkpoint must leave a valid header")
	require.Equal(t, hdr.mxFrame, db.pager.wal.index.shmNBackfill(),
		"the checkpoint must backfill through the recovered end of the log")
	requireTornHeaderRows(t, db)
	requireTornHeaderRowsAfterReopen(t, db, path)
}

// TestTornWalIndexHeader_ReaderRecovers: a read transaction in the survivor
// repairs the header itself and reads the committed data, instead of
// retrying until ErrProtocol.
func TestTornWalIndexHeader_ReaderRecovers(t *testing.T) {
	const env = "TEST_TORNHDR_READ_DB_PATH"
	if dbPath := os.Getenv(env); dbPath != "" {
		tornHeaderChildMain(t, dbPath)
		return
	}
	db, _ := runTornHeaderWriter(t, "TestTornWalIndexHeader_ReaderRecovers", env)

	start := time.Now()
	requireTornHeaderRows(t, db)
	require.Less(t, time.Since(start), 5*time.Second, "the read must not sit in the retry loop")

	_, valid := db.pager.wal.index.readHeader()
	require.True(t, valid)

	// The survivor can write on top of the recovered log.
	dmsPutRange(t, db, "t1", tornHdrF2End, tornHdrF2End+10)
	require.Equal(t, tornHdrF2End+10, countKeys(t, db, "t1"))
	require.NoError(t, db.IntegrityCheck())
}

// TestTornWalIndexHeader_WriterRecovers: a write transaction is the first
// thing the survivor does. Its begin goes through the same read path.
func TestTornWalIndexHeader_WriterRecovers(t *testing.T) {
	const env = "TEST_TORNHDR_WRITE_DB_PATH"
	if dbPath := os.Getenv(env); dbPath != "" {
		tornHeaderChildMain(t, dbPath)
		return
	}
	db, _ := runTornHeaderWriter(t, "TestTornWalIndexHeader_WriterRecovers", env)

	dmsPutRange(t, db, "t1", tornHdrF2End, tornHdrF2End+10)
	requireTornHeaderRows(t, db)
	require.Equal(t, tornHdrF2End+10, countKeys(t, db, "t1"))
	require.NoError(t, db.IntegrityCheck())
}

// tearHeaderCopy makes the two header copies of db's wal-index disagree by
// flipping one bit of copy 1's mxFrame.
func tearHeaderCopy(t *testing.T, db *DB) {
	t.Helper()
	region, err := db.pager.wal.index.shm.region(0, true)
	require.NoError(t, err)
	region[16] ^= 0x01
	_, valid := db.pager.wal.index.readHeader()
	require.False(t, valid, "setup: the header must be torn")
}

// TestTornWalIndexHeader_ReaderRetriesWhileWriteLockBusy: the write lock is
// held (a writer, or another connection already repairing), so the reader
// cannot run recovery and retries. Once the lock is released it repairs the
// header and succeeds. The lock is taken on the same handle: the in-process
// lock table refuses a second exclusive hold exactly as fcntl refuses another
// process's.
func TestTornWalIndexHeader_ReaderRetriesWhileWriteLockBusy(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), tornHdrOpts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	dmsPutRange(t, db, "t1", 0, tornHdrSeedRows)

	w := db.pager.wal
	require.NoError(t, w.index.lock(lockWrite, lockExclusive))
	tearHeaderCopy(t, db)

	// With the lock held a single attempt asks for a retry and repairs
	// nothing.
	_, _, _, _, err = w.tryBeginReadHdr()
	require.ErrorIs(t, err, errWALRetry)
	_, valid := w.index.readHeader()
	require.False(t, valid, "a reader must not rebuild the index without the write lock")

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	released := make(chan struct{})
	go func() {
		rtx, err := db.BeginRead()
		if err != nil {
			done <- result{err: err}
			return
		}
		select {
		case <-released:
		default:
			_ = rtx.Rollback()
			done <- result{err: errors.New("read began while the write lock was still held")}
			return
		}
		ns, err := rtx.GetNamespace("t1")
		if err != nil {
			_ = rtx.Rollback()
			done <- result{err: err}
			return
		}
		n := countKeysInReadTx(t, rtx, ns)
		done <- result{n: n, err: rtx.Rollback()}
	}()

	time.Sleep(50 * time.Millisecond)
	close(released)
	require.NoError(t, w.index.unlock(lockWrite, lockExclusive))

	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Equal(t, tornHdrSeedRows, r.n)
	case <-time.After(20 * time.Second):
		t.Fatal("reader did not finish after the write lock was released")
	}
	_, valid = w.index.readHeader()
	require.True(t, valid)
}

// TestTornWalIndexHeader_CheckpointBusyWhenWriteLockHeld: a PASSIVE
// checkpoint that finds the header torn and the write lock taken cannot
// repair it. It must report busy and leave the WAL alone, never fall back to
// its own frame count.
func TestTornWalIndexHeader_CheckpointBusyWhenWriteLockHeld(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), tornHdrOpts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	dmsPutRange(t, db, "t1", 0, tornHdrSeedRows)

	w := db.pager.wal
	walSize := func() int64 {
		info, err := os.Stat(w.path)
		require.NoError(t, err)
		return info.Size()
	}
	before := walSize()
	backfillBefore := w.index.shmNBackfill()

	require.NoError(t, w.index.lock(lockWrite, lockExclusive))
	tearHeaderCopy(t, db)
	err = db.Checkpoint(CheckpointPassive)
	require.NoError(t, w.index.unlock(lockWrite, lockExclusive))

	require.ErrorIs(t, err, ErrBusy)
	require.Equal(t, before, walSize(), "the WAL must be untouched")
	require.Equal(t, backfillBefore, w.index.shmNBackfill(), "nothing may be backfilled")

	// With the lock free the next checkpoint repairs the header and runs.
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	require.Equal(t, tornHdrSeedRows, countKeys(t, db, "t1"))
}
