//go:build (linux || darwin) && (amd64 || arm64)

package btree

// The change state (walchange.go): what counts as another process's change,
// what does not, and which snapshots are exact for an epoch. The peers are
// real processes (re-exec, as in multiprocess_dms_test.go).

import (
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	changePeerEnvPath = "TEST_WAL_CHANGE_PEER_DB_PATH"
	changePeerEnvMode = "TEST_WAL_CHANGE_PEER_MODE"
	changePeerEnvFill = "TEST_WAL_CHANGE_PEER_FILL"
)

// changePeerChild is a peer process. Modes:
//
//	commit:    write rows [0, 60) with the fill byte, exit without closing
//	           ("commit-all", "torn-all": rows [0, backupRows) instead)
//	truncate:  a TRUNCATE checkpoint, exit without closing
//	torn:      commit as above, then put header copy 1 back to what it was
//	           before the commit and exit: a crash between the two copies
func changePeerChild(t *testing.T) bool {
	path := os.Getenv(changePeerEnvPath)
	if path == "" {
		return false
	}
	db, err := testOpen(t, path, restartModes[0].opts)
	require.NoError(t, err)
	fill, _ := strconv.Atoi(os.Getenv(changePeerEnvFill))
	rows := 60
	mode := os.Getenv(changePeerEnvMode)
	if len(mode) > 4 && mode[len(mode)-4:] == "-all" { // every row of the backup tests
		mode, rows = mode[:len(mode)-4], backupRows
	}
	switch mode {
	case "commit":
		restartPut(t, db, 0, rows, byte(fill))
	case "truncate":
		require.NoError(t, db.Checkpoint(CheckpointTruncate))
	case "torn":
		region, err := db.pager.wal.index.shm.region(0, true)
		require.NoError(t, err)
		var before [walIndexHdrSize]byte
		copy(before[:], region[0:walIndexHdrSize])
		restartPut(t, db, 0, rows, byte(fill))
		copy(region[0:walIndexHdrSize], before[:])
		_, valid := db.pager.wal.index.readHeader()
		require.False(t, valid, "child setup: the header must be torn")
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	if t.Failed() {
		return true
	}
	os.Exit(0) // no Close: no close-time checkpoint under the parent
	return true
}

func runChangePeer(t *testing.T, path, mode string, fill byte) {
	t.Helper()
	name := t.Name()
	if i := indexByte(name, '/'); i >= 0 {
		name = name[:i]
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.v", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), changePeerEnvPath+"="+path, changePeerEnvMode+"="+mode,
		changePeerEnvFill+"="+strconv.Itoa(int(fill)))
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "peer (%s) failed:\n%s", mode, out)
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// changeEpochOf begins and ends a read transaction and returns what it saw.
func changeEpochOf(t *testing.T, db *DB) (epoch uint64, exact bool) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	epoch, exact = rtx.ChangeEpoch()
	require.NoError(t, rtx.Rollback())
	return epoch, exact
}

func openChangeDB(t *testing.T) (*DB, string) {
	t.Helper()
	db, path := openRestartDB(t, restartModes[0])
	restartPut(t, db, 0, 60, 1)
	return db, path
}

// A process's own commits, WAL restarts and TRUNCATE checkpoints are not
// changes; another process's commit is one, counted once, by whichever
// transaction begins first after it.
func TestChangeEpoch_OwnAndForeign(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	db, path := openChangeDB(t)
	e0, exact := changeEpochOf(t, db)
	require.True(t, exact)

	// Own: commits, a restart inside a commit, a TRUNCATE.
	restartPut(t, db, 0, 2, 2)
	require.NoError(t, db.Checkpoint(CheckpointPassive))
	n := db.pager.wal.index.mxCommitFrame.LoadLocal()
	restartPut(t, db, 0, 2, 3)
	require.Less(t, db.pager.wal.index.mxCommitFrame.LoadLocal(), n, "setup: the commit started the log over")
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	e, exact := changeEpochOf(t, db)
	require.Equal(t, e0, e, "own commits, restarts and resets are not changes")
	require.True(t, exact)

	// Foreign: counted by the first begin, of any kind, and only by it.
	runChangePeer(t, path, "commit", 4)
	wtx, err := db.BeginWrite() // a write begin is the first to see it
	require.NoError(t, err)
	e, exact = wtx.ChangeEpoch()
	require.Equal(t, e0+1, e, "the peer's commit is one change")
	require.True(t, exact)
	require.NoError(t, wtx.Rollback())
	e, exact = changeEpochOf(t, db)
	require.Equal(t, e0+1, e, "and it is counted once")
	require.True(t, exact)
	restartCheck(t, db, 0, 60, 4)

	// A peer's TRUNCATE changes the header and no data: one step, as in
	// SQLite (walRestartHdr publishes a new header, wal.c:2377).
	runChangePeer(t, path, "truncate", 0)
	e, _ = changeEpochOf(t, db)
	require.Equal(t, e0+2, e)
}

// A reader that takes its snapshot after an own commit's header is in shared
// memory, and before the committing goroutine has settled the change state,
// must not take the commit for another process's: the header is announced
// before its first byte is written.
func TestChangeEpoch_ReaderInsideOwnPublication(t *testing.T) {
	db, _ := openChangeDB(t)
	e0, _ := changeEpochOf(t, db)

	// ("Between the two copies" is not a reader's to see: the header is torn
	// there and a begin waits for the writer.)
	for _, step := range []string{"stored"} {
		var got uint64
		var exact bool
		ran := false
		testPublishHeaderHook = func(s string) {
			if s != step || ran {
				return
			}
			ran = true
			var wg sync.WaitGroup
			wg.Add(1)
			go func() { // a reader of this process, inside the publication
				defer wg.Done()
				got, exact = changeEpochOf(t, db)
			}()
			wg.Wait()
		}
		restartPut(t, db, 0, 2, 5)
		testPublishHeaderHook = nil
		require.True(t, ran, step)
		require.Equal(t, e0, got, "%s: an own commit is not a change", step)
		if step == "stored" {
			require.True(t, exact, "a snapshot of the pending own header is exact")
		}
		e, exact := changeEpochOf(t, db)
		require.Equal(t, e0, e)
		require.True(t, exact)
	}
}

// While an own publication that owes a step is pending, a snapshot of the
// header being replaced is not exact: the epoch already counts a change that
// snapshot does not hold. Only the pending header's snapshot is exact. After
// the publication the old snapshot stays inexact, and a consumer that
// recorded the old epoch sees it moved.
//
// The publication is a recovery, which runs beside live readers: a peer died
// between the header copies of a commit, and the next begin of this process
// rebuilds the wal-index.
func TestChangeEpoch_ExactOnBothSidesOfAPublication(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	db, path := openChangeDB(t)
	old, err := db.BeginRead() // at H0, held across the publication
	require.NoError(t, err)
	defer func() { _ = old.Rollback() }()
	e0, exact := old.ChangeEpoch()
	require.True(t, exact)

	runChangePeer(t, path, "torn", 6)

	var during, duringNew uint64
	var duringExact, duringNewExact bool
	ran := false
	testPublishHeaderHook = func(s string) {
		if s != "stored" || ran {
			return
		}
		ran = true
		during, duringExact = old.ChangeEpoch()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { // a begin that lands on the pending header
			defer wg.Done()
			duringNew, duringNewExact = changeEpochOf(t, db)
		}()
		wg.Wait()
	}
	defer func() { testPublishHeaderHook = nil }()
	e, exact := changeEpochOf(t, db) // this begin runs the recovery
	testPublishHeaderHook = nil
	require.True(t, ran, "setup: the begin rebuilt the wal-index")
	require.Equal(t, e0+1, e)
	require.True(t, exact)

	require.Equal(t, e0+1, during, "the owed step is in the epoch before the header is settled")
	require.False(t, duringExact, "a snapshot of the retained header is not exact during the publication")
	require.Equal(t, e0+1, duringNew)
	require.True(t, duringNewExact, "a snapshot of the pending header is exact")

	e, exact = old.ChangeEpoch()
	require.Equal(t, e0+1, e)
	require.False(t, exact, "after the publication the old snapshot is not exact")
}

// A reader whose snapshot is older than the change state's header, and which
// loaded the state before an own commit moved it, cannot tell its snapshot's
// order: it counts a change and leaves the header where it is. The header
// never goes back to an older value.
func TestChangeEpoch_DelayedReaderNeverMovesTheHeaderBack(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	db, path := openChangeDB(t)
	w := db.pager.wal
	e0, _ := changeEpochOf(t, db)

	runChangePeer(t, path, "commit", 8) // F: unseen
	// A delayed begin: it loaded the state (H0), took its snapshot at F, and
	// is preempted before it swaps. Meanwhile an own writer consumes F and
	// commits H2.
	pre := w.chg.Load()
	snap, valid := w.index.readHeader()
	require.True(t, valid)
	restartPut(t, db, 0, 2, 9)
	h2 := w.chg.Load().hdr
	e1 := w.chg.Load().epoch
	require.Equal(t, e0+1, e1, "the writer's begin counted the peer's commit")
	require.NotEqual(t, snap, h2)

	w.noteSnapshot(pre, &snap) // the delayed begin goes on
	cur := w.chg.Load()
	require.Equal(t, h2, cur.hdr, "the header did not go back to the older snapshot")
	require.Equal(t, e1+1, cur.epoch, "order unknown: counted")
	e, exact := changeEpochOf(t, db)
	require.Equal(t, e1+1, e)
	require.True(t, exact)
}

// A publication that unwinds before any byte, between the two header copies,
// or after both, leaves no pending header behind, and the change state
// agrees with shared memory.
func TestChangeEpoch_PublicationUnwinds(t *testing.T) {
	for _, step := range []string{"announced", "between", "stored"} {
		t.Run(step, func(t *testing.T) {
			db, path := openChangeDB(t)
			w := db.pager.wal
			e0, _ := changeEpochOf(t, db)

			testPublishHeaderHook = func(s string) {
				if s == step {
					testPublishHeaderHook = nil
					panic("injected: in the header publication")
				}
			}
			defer func() { testPublishHeaderHook = nil }()
			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 0, 2, 2)
			require.Panics(t, func() { _ = wtx.Commit() })

			cur := w.chg.Load()
			require.Nil(t, cur.pending, "no pending header survives the publication")
			live, valid := w.index.readHeader()
			switch step {
			case "stored":
				require.True(t, valid)
				require.Equal(t, live, cur.hdr, "the published header is the state's")
				require.Equal(t, e0, cur.epoch)
			default:
				// Not published, or torn: nothing a reader saw is certified.
				require.Equal(t, e0+1, cur.epoch)
			}

			// Readers go on, and the state settles on what they find. With
			// no byte of the header written the commit was never visible
			// and the next writer writes over its frames; a torn header is
			// rebuilt from the WAL, in which the commit is durable.
			want := byte(2)
			if step == "announced" {
				want = 1
			}
			restartCheck(t, db, 0, 2, want)
			restartCheck(t, db, 2, 60, 1)
			_, exact := changeEpochOf(t, db)
			require.True(t, exact)
			restartPut(t, db, 0, 3, 3)
			restartRecoverCopy(t, path, "crash", restartModes[0], func(cdb *DB) {
				restartCheck(t, cdb, 0, 3, 3)
				restartCheck(t, cdb, 3, 60, 1)
			})
		})
	}
}

// A rebuild of the wal-index is a change to the process that runs it,
// whatever the rebuilt header's bytes (*pChanged = 1 after walIndexRecover,
// wal.c:2724-2725). iChange counts commits (wal.c:4245), survives a reset by
// a process that never wrote (walRestartHdr keeps it, wal.c:2150-2154), and
// is 0 after a recovery (wal.c:1408).
func TestChangeEpoch_RecoveryAndIChange(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	db, path := openChangeDB(t)
	w := db.pager.wal
	hdr, valid := w.index.readHeader()
	require.True(t, valid)
	base := hdr.iChange
	restartPut(t, db, 0, 2, 2)
	restartPut(t, db, 0, 2, 3)
	hdr, _ = w.index.readHeader()
	require.Equal(t, base+2, hdr.iChange, "iChange counts commits")

	// A peer attaches, which rebuilds the wal-index (iChange 0), and commits
	// once. This process, which did not write since, resets the log:
	// iChange is the peer's, kept.
	runChangePeer(t, path, "commit", 4)
	peerHdr, valid := w.index.readHeader()
	require.True(t, valid)
	require.Equal(t, uint32(1), peerHdr.iChange)
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	hdr, _ = w.index.readHeader()
	require.Zero(t, hdr.mxFrame)
	require.Equal(t, peerHdr.iChange, hdr.iChange, "a reset keeps iChange")
	require.Equal(t, peerHdr.nPage, hdr.nPage)

	// A peer dies between the header copies of a commit. The reader that
	// repairs the header takes a step for the rebuild alone.
	e0, _ := changeEpochOf(t, db)
	runChangePeer(t, path, "torn", 5)
	_, valid = w.index.readHeader()
	require.False(t, valid, "setup: the header is torn")
	e, exact := changeEpochOf(t, db)
	require.Equal(t, e0+1, e, "the rebuild is a change")
	require.True(t, exact)
	hdr, valid = w.index.readHeader()
	require.True(t, valid)
	require.Zero(t, hdr.iChange, "iChange is 0 after a recovery")
	restartCheck(t, db, 0, 60, 5)

	// The same rebuild again, of a WAL that did not change: the header's
	// bytes come out the same, and it is a change all the same.
	region, err := w.index.shm.region(0, true)
	require.NoError(t, err)
	region[16] ^= 0x01
	_, valid = w.index.readHeader()
	require.False(t, valid)
	e2, exact := changeEpochOf(t, db)
	rebuilt, valid := w.index.readHeader()
	require.True(t, valid)
	require.Equal(t, hdr, rebuilt, "setup: the rebuilt header is byte-identical")
	require.Equal(t, e+1, e2, "an identical rebuild is still a change")
	require.True(t, exact)
}

// Salts of zero are a valid log generation. Whether a database has other
// processes is its mode, not its salts.
func TestChangeEpoch_ZeroSaltsStillDetect(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	db, path := openChangeDB(t)
	w := db.pager.wal
	require.NoError(t, db.Checkpoint(CheckpointPassive))
	require.NoError(t, w.index.lock(lockWrite, lockExclusive))
	for i := 1; i <= 4; i++ {
		require.NoError(t, w.index.lock(lockRead0+i, lockExclusive))
	}
	w.mu.Lock()
	hdr, _ := w.index.readHeader()
	w.restartHeaderStateLocked(true, hdr.nPage, 0, &hdr) // salt1 0
	w.header.salt2 = 0
	var buf [walHeaderSize]byte
	w.header.serialize(buf[:])
	w.cksum1, w.cksum2 = walChecksum(buf[0:24], 0, 0)
	w.genSeed1.Store(w.cksum1)
	w.genSeed2.Store(w.cksum2)
	require.NoError(t, w.publishHeader(&w.index.hdr, false, hdr.iChange, 0, hdr.nPage,
		[2]uint32{w.cksum1, w.cksum2}, [2]uint32{0, 0}))
	w.mu.Unlock()
	for i := 1; i <= 4; i++ {
		require.NoError(t, w.index.unlock(lockRead0+i, lockExclusive))
	}
	require.NoError(t, w.index.unlock(lockWrite, lockExclusive))
	live, valid := w.index.readHeader()
	require.True(t, valid)
	require.Equal(t, [2]uint32{0, 0}, live.aSalt, "setup: a log generation with zero salts")

	e0, _ := changeEpochOf(t, db)
	runChangePeer(t, path, "commit", 6)
	e, exact := changeEpochOf(t, db)
	require.Equal(t, e0+1, e, "a peer's commit in a zero-salt log is a change")
	require.True(t, exact)
	restartCheck(t, db, 0, 60, 6)
}

// In-process and in-memory databases have no other process: no change state,
// the epoch never moves and every snapshot is exact.
func TestChangeEpoch_HeapModes(t *testing.T) {
	for _, m := range restartModes[1:] {
		t.Run(m.name, func(t *testing.T) {
			db, _ := openRestartDB(t, m)
			require.Nil(t, db.pager.wal.chg.Load())
			restartPut(t, db, 0, 60, 1)
			old, err := db.BeginRead()
			require.NoError(t, err)
			restartPut(t, db, 0, 2, 2)
			e, exact := old.ChangeEpoch()
			require.Zero(t, e)
			require.True(t, exact)
			require.NoError(t, old.Rollback())
		})
	}
}
