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

// TestTornWalIndexHeader_CloseKeepsPeerCommits: the survivor closes as the
// last process attached. Its close-time checkpoint runs under the write
// lock, and a WAL truncate follows a complete backfill, so the header has to
// be repaired first there too.
func TestTornWalIndexHeader_CloseKeepsPeerCommits(t *testing.T) {
	const env = "TEST_TORNHDR_CLOSE_DB_PATH"
	if dbPath := os.Getenv(env); dbPath != "" {
		tornHeaderChildMain(t, dbPath)
		return
	}
	db, path := runTornHeaderWriter(t, "TestTornWalIndexHeader_CloseKeepsPeerCommits", env)

	require.NoError(t, db.Close())
	// The close repaired the header, backfilled the whole log and, as the
	// last client, emptied the WAL: the rows are in the database file.
	info, err := os.Stat(path + "-wal")
	require.NoError(t, err)
	require.Zero(t, info.Size(), "the last client's close must checkpoint the recovered log and truncate the WAL")

	db2, err := testOpen(t, path, tornHdrOpts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	requireTornHeaderRows(t, db2)
	require.NoError(t, db2.IntegrityCheck())
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

// TestCheckpointResetUsesSharedBackfill: the survivor's process-local
// nBackfill is left over from an earlier WAL generation and is larger than
// the current log. A peer restarted the WAL and committed into it; a reader
// pins slot 0, so the survivor's TRUNCATE cannot backfill anything. The
// reset decision must read the shared nBackfill, as walCheckpoint does
// (pInfo->nBackfill<pWal->hdr.mxFrame, wal.c:2341): judged on the stale local
// count the log looks fully backfilled and the peer's commits are truncated
// away.
func TestCheckpointResetUsesSharedBackfill(t *testing.T) {
	const env = "TEST_CKPT_SHARED_BACKFILL_DB_PATH"
	const total = 210
	if dbPath := os.Getenv(env); dbPath != "" {
		// The peer: restart the WAL, commit a few frames into the new
		// generation, die.
		db, err := testOpen(t, dbPath, tornHdrOpts)
		require.NoError(t, err)
		require.NoError(t, db.Checkpoint(CheckpointTruncate))
		dmsPutRange(t, db, "t1", 200, total)
		os.Exit(0)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, tornHdrOpts)
	require.NoError(t, err)
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	dmsPutRange(t, db, "t1", 0, 200)
	require.NoError(t, db.Checkpoint(CheckpointFull))
	w := db.pager.wal
	staleLocal := w.index.nBackfill.Load()

	cmd := exec.Command(os.Args[0], "-test.run=^TestCheckpointResetUsesSharedBackfill$", "-test.v", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), env+"="+path)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "peer process failed:\n%s", out)

	hdr, valid := w.index.readHeader()
	require.True(t, valid)
	require.Greater(t, hdr.mxFrame, uint32(0))
	require.Equal(t, uint32(0), w.index.shmNBackfill(), "setup: nothing of the new generation is backfilled")
	require.Equal(t, staleLocal, w.index.nBackfill.Load(), "setup: the local nBackfill is the old generation's")
	require.Greater(t, staleLocal, hdr.mxFrame, "setup: the stale local count must cover the new log")

	// A reader on slot 0 (it began when the WAL was empty).
	require.NoError(t, w.index.lock(lockRead0, lockShared))
	w.busyHandler = nil
	err = db.Checkpoint(CheckpointTruncate)
	require.NoError(t, w.index.unlock(lockRead0, lockShared))
	require.ErrorIs(t, err, ErrBusy, "nothing was backfilled: the checkpoint must not reset the WAL")

	live, valid := w.index.readHeader()
	require.True(t, valid)
	require.Equal(t, hdr, live, "the wal-index header must be untouched")

	closed = true
	require.NoError(t, db.Close())
	db2, err := testOpen(t, path, tornHdrOpts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	require.Equal(t, total, countKeys(t, db2, "t1"), "the peer's commits must survive")
}

// TestWalIndexFutureVersionRefused: both header copies agree and checksum,
// but carry a wal-index version this build does not know. That is another
// build's index, not a torn one: it is refused, never rebuilt
// (walIndexReadHdr, wal.c:2722-2728: SQLITE_CANTOPEN with no recovery).
func TestWalIndexFutureVersionRefused(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), tornHdrOpts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	dmsPutRange(t, db, "t1", 0, tornHdrSeedRows)

	w := db.pager.wal
	hdr, valid := w.index.readHeader()
	require.True(t, valid)
	region, err := w.index.shm.region(0, true)
	require.NoError(t, err)
	var orig [walIndexHdrSize * 2]byte
	copy(orig[:], region[:walIndexHdrSize*2])

	future := hdr
	future.iVersion = walVersion + 1
	future.computeCksum()
	var buf [walIndexHdrSize]byte
	future.serialize(buf[:])
	copy(region[walIndexHdrSize:walIndexHdrSize*2], buf[:])
	copy(region[0:walIndexHdrSize], buf[:])
	var planted [walIndexHdrSize * 2]byte
	copy(planted[:], region[:walIndexHdrSize*2])

	_, err = db.BeginRead()
	require.ErrorIs(t, err, ErrWALVersion)
	_, err = db.BeginWrite()
	require.ErrorIs(t, err, ErrWALVersion)
	require.ErrorIs(t, db.Checkpoint(CheckpointTruncate), ErrWALVersion)

	var now [walIndexHdrSize * 2]byte
	copy(now[:], region[:walIndexHdrSize*2])
	require.Equal(t, planted, now, "an index of another version must not be rewritten")

	// Put the real header back so the close-time checkpoint runs normally.
	copy(region[:walIndexHdrSize*2], orig[:])
	require.Equal(t, tornHdrSeedRows, countKeys(t, db, "t1"))
}

// TestTornWalIndexHeader_BusyRecoveryWhenRecoverLockHeld: the header is
// invalid, the write lock is taken, and so is the RECOVER lock: another
// connection is running recovery. The reader reports ErrBusyRecovery
// (SQLITE_BUSY_RECOVERY, wal.c:3089-3094), which the begin paths hand to the
// busy handler, instead of spinning in the retry loop.
func TestTornWalIndexHeader_BusyRecoveryWhenRecoverLockHeld(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), tornHdrOpts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	dmsPutRange(t, db, "t1", 0, tornHdrSeedRows)

	w := db.pager.wal
	require.NoError(t, w.index.lock(lockWrite, lockExclusive))
	require.NoError(t, w.index.lock(lockRecover, lockExclusive))
	tearHeaderCopy(t, db)

	_, _, _, _, err = w.tryBeginReadHdr()
	require.ErrorIs(t, err, ErrBusyRecovery)

	// With no busy handler the begin returns it at once.
	w.busyHandler = nil
	start := time.Now()
	_, err = db.BeginRead()
	require.ErrorIs(t, err, ErrBusyRecovery)
	require.Less(t, time.Since(start), 2*time.Second, "must not sit in the WAL retry loop")

	// With a busy handler the begin waits for the recoverer: release the
	// locks from the handler and the read goes through (it repairs the
	// header itself, since the "recoverer" here did not).
	var calls int
	w.busyHandler = func(int) bool {
		calls++
		if calls == 1 {
			_ = w.index.unlock(lockRecover, lockExclusive)
			_ = w.index.unlock(lockWrite, lockExclusive)
		}
		return calls < 50
	}
	require.Equal(t, tornHdrSeedRows, countKeys(t, db, "t1"))
	require.GreaterOrEqual(t, calls, 1)
}

// TestTornWalIndexHeader_BusyRecoveryOneBudgetPerBegin: the header is torn
// and the write and RECOVER locks stay held, a recovery that does not
// finish. A begin waits one busy-handler budget and gives up: SQLite runs
// one loop with one count around the whole begin (sqlite3BtreeBeginTrans,
// btree.c:3730-3737; sqlite3InvokeBusyHandler, main.c:1763-1772), never a
// fresh budget per retry.
func TestTornWalIndexHeader_BusyRecoveryOneBudgetPerBegin(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), tornHdrOpts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	dmsPutRange(t, db, "t1", 0, tornHdrSeedRows)

	w := db.pager.wal
	require.NoError(t, w.index.lock(lockWrite, lockExclusive))
	require.NoError(t, w.index.lock(lockRecover, lockExclusive))
	tearHeaderCopy(t, db)

	const budget = 200 * time.Millisecond
	base := DefaultBusyTimeout(budget)
	var calls int
	w.busyHandler = func(n int) bool {
		calls++
		return base(n)
	}

	start := time.Now()
	_, err = db.BeginRead()
	require.ErrorIs(t, err, ErrBusyRecovery)
	require.Less(t, time.Since(start), 4*budget, "BeginRead: one budget")
	readCalls := calls
	require.Greater(t, readCalls, 1)

	calls = 0
	start = time.Now()
	_, err = db.BeginWrite()
	require.ErrorIs(t, err, ErrBusyRecovery)
	require.Less(t, time.Since(start), 4*budget, "BeginWrite: one budget, not one per retry")
	require.Equal(t, readCalls, calls, "BeginWrite: the handler's count runs once through")

	w.busyHandler = DefaultBusyTimeout(5 * time.Second)
	require.NoError(t, w.index.unlock(lockRecover, lockExclusive))
	require.NoError(t, w.index.unlock(lockWrite, lockExclusive))
	require.Equal(t, tornHdrSeedRows, countKeys(t, db, "t1"))
}

// TestTornWalIndexHeader_WALReadBeginWaitsOutRecovery: the busy loop sits at
// the WAL read begin, so a read begun below the pager (open's read of page
// 1) waits out a recovery in progress as BeginRead does.
func TestTornWalIndexHeader_WALReadBeginWaitsOutRecovery(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), tornHdrOpts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	dmsPutRange(t, db, "t1", 0, tornHdrSeedRows)

	w := db.pager.wal
	require.NoError(t, w.index.lock(lockWrite, lockExclusive))
	require.NoError(t, w.index.lock(lockRecover, lockExclusive))
	tearHeaderCopy(t, db)

	var calls int
	w.busyHandler = func(int) bool {
		calls++
		if calls == 1 {
			_ = w.index.unlock(lockRecover, lockExclusive)
			_ = w.index.unlock(lockWrite, lockExclusive)
		}
		return calls < 50
	}
	_, maxFrame, _, slot, err := w.beginReadHdr()
	require.NoError(t, err)
	w.endRead(slot)
	require.Greater(t, maxFrame, uint32(0))
	require.Equal(t, 1, calls)
	_, valid := w.index.readHeader()
	require.True(t, valid, "the read begin repaired the header once the locks were free")
}

// TestBeginWrite_PeerCommitsBeforeTimeoutOneBudget: a peer holds the write
// lock. SQLite tries the write lock once per attempt (walBeginWriteTransaction,
// wal.c:3714-3717) and retries the whole begin from one loop with one busy
// count (btree.c:3730-3737), so the handler sees one count rising 0, 1, 2, ...
// through the begin, and the begin's wait is one budget.
//
//   - The peer commits just before the waiting begin's budget runs out,
//     releases the lock, and has it again when the begin tries next. The
//     begin gives up, or gets in, within one budget; a write-lock wait with a
//     count of its own started a fresh budget after every commit.
//   - The peer releases the lock within the budget and stays away: the begin
//     gets in.
//
// The peer works through this handle's lock table, which refuses the begin's
// write lock as fcntl refuses another process's. It "commits" by publishing
// a header that differs from the begin's snapshot.
func TestBeginWrite_PeerCommitsBeforeTimeoutOneBudget(t *testing.T) {
	const budget = 200 * time.Millisecond
	// A budget of 200 ms is 11 handler calls that sleep.
	const commitAfter = 10

	type peer struct {
		holds  bool
		waited int
		counts []int // the count of every handler call
	}
	setup := func(t *testing.T) (*DB, *peer) {
		dir := t.TempDir()
		db, err := testOpen(t, filepath.Join(dir, "test.db"), tornHdrOpts)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		dmsPutRange(t, db, "t1", 0, tornHdrSeedRows)
		return db, &peer{}
	}
	lock := func(t *testing.T, db *DB, p *peer) {
		require.NoError(t, db.pager.wal.index.lock(lockWrite, lockExclusive))
		p.holds, p.waited = true, 0
	}
	release := func(db *DB, p *peer) {
		if p.holds {
			_ = db.pager.wal.index.unlock(lockWrite, lockExclusive)
			p.holds = false
		}
	}
	commit := func(t *testing.T, db *DB) {
		w := db.pager.wal
		hdr, valid := w.index.readHeader()
		require.True(t, valid)
		hdr.iChange++
		hdr.computeCksum()
		var buf [walIndexHdrSize]byte
		hdr.serialize(buf[:])
		region, err := w.index.shm.region(0, true)
		require.NoError(t, err)
		copy(region[walIndexHdrSize:walIndexHdrSize*2], buf[:])
		copy(region[0:walIndexHdrSize], buf[:])
	}
	// begin runs BeginWrite with the peer's script in the busy handler and
	// returns its error and duration.
	begin := func(t *testing.T, db *DB, p *peer, onCall func()) (error, time.Duration) {
		w := db.pager.wal
		base := DefaultBusyTimeout(budget)
		w.busyHandler = func(n int) bool {
			p.counts = append(p.counts, n)
			ok := base(n)
			onCall()
			return ok
		}
		defer func() { w.busyHandler = DefaultBusyTimeout(5 * time.Second) }()
		start := time.Now()
		wtx, err := db.BeginWrite()
		elapsed := time.Since(start)
		if err == nil {
			_ = wtx.Rollback()
		}
		release(db, p)
		return err, elapsed
	}
	requireOneCount := func(t *testing.T, p *peer) {
		t.Helper()
		require.NotEmpty(t, p.counts, "the begin must wait through the busy handler")
		for i, n := range p.counts {
			require.Equal(t, i, n, "one count for the whole begin, rising by one: %v", p.counts)
		}
	}

	t.Run("peer commits before each timeout", func(t *testing.T) {
		db, p := setup(t)
		testBeginWriteRetryHook = func(error) {
			if !p.holds {
				lock(t, db, p) // the peer is back before the begin's next try
			}
		}
		defer func() { testBeginWriteRetryHook = nil }()
		lock(t, db, p)
		err, elapsed := begin(t, db, p, func() {
			if p.holds {
				if p.waited++; p.waited == commitAfter {
					commit(t, db)
					release(db, p)
				}
			}
		})
		require.Less(t, elapsed, 2*budget, "one busy budget per begin (err=%v)", err)
		require.GreaterOrEqual(t, len(p.counts), commitAfter, "the begin waited through the peer's hold")
		requireOneCount(t, p)
	})

	t.Run("peer releases within the budget", func(t *testing.T) {
		db, p := setup(t)
		lock(t, db, p)
		err, elapsed := begin(t, db, p, func() {
			if p.holds {
				if p.waited++; p.waited == 3 {
					release(db, p)
				}
			}
		})
		require.NoError(t, err, "the write lock came free within the budget")
		require.Less(t, elapsed, budget)
		require.Len(t, p.counts, 3)
		requireOneCount(t, p)
	})
}
