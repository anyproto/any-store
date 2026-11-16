//go:build (linux || darwin) && (amd64 || arm64)

package btree

// This test verifies that the multi-process WAL write safety fix works:
// BUSY_SNAPSHOT check + WAL state re-sync ensures that writes from multiple
// OS processes all survive correctly.

import (
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMultiProcessWALCorruption verifies that when two OS processes open
// the same database file (InProcess=false), writes from both processes
// are correctly preserved thanks to the BUSY_SNAPSHOT check and WAL state
// re-sync in beginWrite.
//
// Timeline:
//  1. Process A opens DB, writes "parent-1", commits  → WAL frames 1..N
//  2. Process B opens DB, writes "child-1/2", commits → WAL frames N+1..M
//  3. Process A writes "parent-2", commits — beginWrite re-syncs WAL state
//     from SHM so frames chain correctly after M
//  4. On reopen + WAL recovery, all data survives
func TestMultiProcessWALCorruption(t *testing.T) {
	dbPath := os.Getenv("TEST_MULTIPROCESS_DB_PATH")
	if dbPath != "" {
		// === CHILD PROCESS ===
		multiProcessChild(t, dbPath)
		return
	}

	// === PARENT PROCESS ===
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	opts := Options{
		PageSize:              4096,
		CacheSize:             100,
		InProcess:             false,
		DisableAutoCheckpoint: true,
	}

	// Step 1: Open DB, write initial data
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("parent-1"), []byte("value-parent-1")))
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())

	t.Logf("parent after first commit: nFrame=%d", db.pager.wal.nFrame.Load())

	// Step 2: Spawn child process to write to the same DB
	cmd := exec.Command(os.Args[0],
		"-test.run=^TestMultiProcessWALCorruption$",
		"-test.v",
		"-test.timeout=30s",
	)
	cmd.Env = append(os.Environ(), "TEST_MULTIPROCESS_DB_PATH="+path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	require.NoError(t, err, "child process failed")

	// Step 3: Parent writes after child committed.
	// With the BUSY_SNAPSHOT fix, beginWrite re-syncs WAL state from SHM,
	// so the parent correctly chains onto the child's frames.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := tx2.GetNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx2.Put(ns2, []byte("parent-2"), []byte("value-parent-2")))
	tx2.MarkDataChanged()
	require.NoError(t, tx2.Commit())

	t.Logf("parent after second commit: nFrame=%d cksum1=%08x cksum2=%08x",
		db.pager.wal.nFrame.Load(), db.pager.wal.cksum1, db.pager.wal.cksum2)

	require.NoError(t, db.Close())

	// Step 4: Reopen and verify ALL data survives
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer db2.Close()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	ns3, err := rtx.GetNamespace("test")
	require.NoError(t, err)

	// All 4 keys must survive
	val, err := rtx.Get(ns3, []byte("parent-1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-parent-1"), val)

	val, err = rtx.Get(ns3, []byte("child-1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-child-1"), val)

	val, err = rtx.Get(ns3, []byte("child-2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-child-2"), val)

	val, err = rtx.Get(ns3, []byte("parent-2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-parent-2"), val)

	t.Logf("all data survived: parent-1, child-1, child-2, parent-2")
}

func multiProcessChild(t *testing.T, dbPath string) {
	opts := Options{
		PageSize:              4096,
		CacheSize:             100,
		InProcess:             false,
		DisableAutoCheckpoint: true,
	}

	db, err := testOpen(t, dbPath, opts)
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)

	ns, err := tx.GetNamespace("test")
	require.NoError(t, err)

	require.NoError(t, tx.Put(ns, []byte("child-1"), []byte("value-child-1")))
	require.NoError(t, tx.Put(ns, []byte("child-2"), []byte("value-child-2")))
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())

	// Verify child can read its own data before closing
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	val, err := rtx.Get(ns, []byte("child-1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-child-1"), val)
	require.NoError(t, rtx.Rollback())

	t.Logf("child: nFrame=%d after commit", db.pager.wal.nFrame.Load())
	require.NoError(t, db.Close())
}

// closeRescuePeerRows is how many rows the peer process commits, keys after
// the parent's.
const closeRescuePeerRows = 50

// closeRescuePeerOpts are the options of both processes of the close-rescue
// peer tests.
var closeRescuePeerOpts = Options{
	CacheSize:             closeRescueCache,
	InProcess:             false,
	DisableAutoCheckpoint: true,
}

// closeRescuePeer is the peer process of a close-rescue test, which the
// parent starts with runCloseRescuePeer: one commit of closeRescuePeerRows
// rows, then close. It reports whether this process is the peer.
func closeRescuePeer(t *testing.T) bool {
	path := os.Getenv("TEST_CLOSE_RESCUE_PEER_DB")
	if path == "" {
		return false
	}
	db, err := testOpen(t, path, closeRescuePeerOpts)
	require.NoError(t, err)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("t")
	require.NoError(t, err)
	for i := 1; i <= closeRescuePeerRows; i++ {
		require.NoError(t, tx.Put(ns, closeRescueKey(closeRescueRows+i), make([]byte, closeRescueValSize)))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Close())
	return true
}

// runCloseRescuePeer runs t's test again in a child process, as the peer of
// the database at path, and waits for its commit.
func runCloseRescuePeer(t *testing.T, path string) {
	t.Helper()
	cmd := exec.Command(os.Args[0],
		"-test.run=^"+t.Name()+"$",
		"-test.v",
		"-test.timeout=60s",
	)
	cmd.Env = append(os.Environ(), "TEST_CLOSE_RESCUE_PEER_DB="+path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run(), "peer process failed")
}

// A writer that DB.Close rescued must not spill into a log another process
// now writes. Close rewinds the log to the transaction's base frame and
// releases the WAL write lock; a peer then appends its commit from that
// frame on, and a spill by the abandoned writer would land on the peer's
// frames.
func TestCloseRescue_SpillSparesPeerWriterFrames(t *testing.T) {
	if closeRescuePeer(t) {
		return
	}
	opts := closeRescuePeerOpts

	path := filepath.Join(t.TempDir(), "test.db")
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	createNS(t, db, "t")
	putN(t, db, "t", closeRescueRows, closeRescueValSize)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("t")
	require.NoError(t, err)
	p := db.pager
	base := p.wal.nFrame.Load()
	fillWriterCacheDirty(t, p)
	rescueAsClose(db)
	runCloseRescuePeer(t, path)

	frameSize := int64(walFrameSize) + int64(p.pageSize)
	peerFrom := int64(walHeaderSize) + int64(base)*frameSize
	walBefore, err := os.ReadFile(path + "-wal")
	require.NoError(t, err)
	require.Greater(t, int64(len(walBefore)), peerFrom, "the peer's commit is not in the log after the base frame")

	// The abandoned writer reads on; every miss meets a cache full of dirty
	// pages.
	for i := 1; i <= closeRescueRows; i++ {
		_, err := tx.Get(ns, closeRescueKey(i))
		require.NoError(t, err)
	}

	walAfter, err := os.ReadFile(path + "-wal")
	require.NoError(t, err)
	require.Equal(t, len(walBefore), len(walAfter))
	require.True(t, string(walBefore[peerFrom:]) == string(walAfter[peerFrom:]), "the peer's frames were overwritten")
	require.ErrorIs(t, tx.Put(ns, closeRescueKey(1), []byte("x")), ErrClosed)

	require.NoError(t, db.Close())
	db, err = testOpen(t, path, opts)
	require.NoError(t, err)
	require.Equal(t, closeRescueRows+closeRescuePeerRows, countKeys(t, db, "t"))
	require.NoError(t, db.IntegrityCheck())
}

// The same with the rescue and the peer's commit coming after the spill has
// passed pagerStress's guard and before it writes: the spill must find the
// write transaction ended and write nothing, not its page over the peer's
// first frame.
func TestCloseRescue_SpillPastGuardSparesPeerWriterFrames(t *testing.T) {
	if closeRescuePeer(t) {
		return
	}
	opts := closeRescuePeerOpts

	path := filepath.Join(t.TempDir(), "test.db")
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	createNS(t, db, "t")
	putN(t, db, "t", closeRescueRows, closeRescueValSize)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("t")
	require.NoError(t, err)
	p := db.pager
	base := p.wal.nFrame.Load()
	fillWriterCacheDirty(t, p)

	var spills, rescues atomic.Int32
	var walBefore []byte
	testAfterSpillHook = func() { spills.Add(1) }
	testStressGuardPassedHook = func() {
		if rescues.Add(1) != 1 {
			return
		}
		rescueAsClose(db)
		runCloseRescuePeer(t, path)
		var err error
		walBefore, err = os.ReadFile(path + "-wal")
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		testAfterSpillHook = nil
		testStressGuardPassedHook = nil
	})

	// The first miss on the full cache spills; the rescue and the peer's
	// commit run once the spill has passed the guard.
	for i := 1; i <= closeRescueRows; i++ {
		_, err := tx.Get(ns, closeRescueKey(i))
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), rescues.Load(), "no spill passed the guard")

	frameSize := int64(walFrameSize) + int64(p.pageSize)
	peerFrom := int64(walHeaderSize) + int64(base)*frameSize
	require.Greater(t, int64(len(walBefore)), peerFrom, "the peer's commit is not in the log after the base frame")
	walAfter, err := os.ReadFile(path + "-wal")
	require.NoError(t, err)
	require.Equal(t, len(walBefore), len(walAfter))
	require.True(t, string(walBefore[peerFrom:]) == string(walAfter[peerFrom:]), "the peer's frames were overwritten")
	require.Zero(t, spills.Load(), "a page was spilled after Close ended the transaction")
	require.Equal(t, base, p.wal.nFrame.Load())
	require.Equal(t, base, p.wal.index.maxFrame.Load())
	require.ErrorIs(t, tx.Put(ns, closeRescueKey(1), []byte("x")), ErrClosed)

	require.NoError(t, db.Close())
	db, err = testOpen(t, path, opts)
	require.NoError(t, err)
	require.Equal(t, closeRescueRows+closeRescuePeerRows, countKeys(t, db, "t"))
	require.NoError(t, db.IntegrityCheck())
}
