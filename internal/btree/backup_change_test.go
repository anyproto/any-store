//go:build (linux || darwin) && (amd64 || arm64)

package btree

// An online backup beside commits: the source's own (their pages are sent to
// the backup, spilled ones included, and the backup goes on), another
// process's (the backup starts over), and transactions that do not commit.
// Every test ends by checking that the destination is one committed state of
// the source: all rows of one fill.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const backupRows = 400

// backupFixture is a source with backupRows rows of fill 1, a destination,
// and a backup that has copied a few pages.
type backupFixture struct {
	src, dst *DB
	srcPath  string
	dstPath  string
	b        *Backup
}

func newBackupFixture(t *testing.T, m restartMode) *backupFixture {
	t.Helper()
	f := &backupFixture{}
	f.src, f.srcPath = openRestartDB(t, m)
	restartPut(t, f.src, 0, backupRows, 1)
	f.dst, f.dstPath = openRestartDB(t, restartModes[0])
	b, err := f.dst.BackupInit(f.src)
	require.NoError(t, err)
	f.b = b
	t.Cleanup(func() { _ = b.Finish() })
	require.NoError(t, b.Step(context.Background(), 4))
	require.Greater(t, f.src.DatabaseSize(), uint32(12), "setup: most pages are not copied yet")
	return f
}

func (f *backupFixture) next(t *testing.T) uint32 {
	t.Helper()
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	return f.b.iNext
}

// finish steps the backup to its end, finishes it, and checks the
// destination, live and reopened, against rows of one fill.
func (f *backupFixture) finish(t *testing.T, fill byte) {
	t.Helper()
	for {
		err := f.b.Step(context.Background(), 16)
		if errors.Is(err, ErrBackupDone) {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, f.b.Finish())
	restartCheck(t, f.dst, 0, backupRows, fill)
	f.dst = restartReopen(t, f.dst, f.dstPath, restartModes[0])
	restartCheck(t, f.dst, 0, backupRows, fill)
	require.NoError(t, f.dst.IntegrityCheck())
}

// The source's own commits do not restart a backup: the pages they change
// reach it as updates (sqlite3BackupUpdate, pager.c:3219-3225).
func TestBackupChange_OwnCommits(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		f := newBackupFixture(t, m)
		prev := f.next(t)
		for i := 0; i < 6; i++ {
			restartPut(t, f.src, 0, backupRows, byte(2+i))
			require.NoError(t, f.b.Step(context.Background(), 1))
			next := f.next(t)
			require.Greater(t, next, prev, "the backup goes on after an own commit")
			prev = next
		}
		f.finish(t, 7)
	})
}

// A commit whose changed pages were all spilled before it: the spills send
// their pages to the backup too (pagerStress → pagerWalFrames, pager.c:4649),
// so the backup goes on and still ends as the committed state. The
// transaction does not mark the data changed: the page-1 counter stays.
func TestBackupChange_OwnSpilledCommit(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		f := newBackupFixture(t, m)
		before := f.next(t)
		wtx, err := f.src.BeginWrite()
		require.NoError(t, err)
		restartPutTx(t, wtx, 0, backupRows, 2)
		restartSpillAll(t, f.src)
		require.NoError(t, wtx.Commit())
		require.Equal(t, before, f.next(t), "a spilled commit does not restart the backup")
		f.finish(t, 2)
	})
}

// A transaction that sent the backup spilled pages and then did not commit
// restarts it: by a rollback (pagerUndoCallback, pager.c:3139), and by a
// savepoint rollback followed by a commit, whose restored pages are logged
// and sent again.
func TestBackupChange_SpillThenRollback(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		f := newBackupFixture(t, m)
		wtx, err := f.src.BeginWrite()
		require.NoError(t, err)
		restartPutTx(t, wtx, 0, backupRows, 2)
		restartSpillAll(t, f.src)
		require.NoError(t, wtx.Rollback())
		require.Equal(t, uint32(1), f.next(t), "a rollback restarts the backup")
		f.finish(t, 1)
	})
}

func TestBackupChange_SpillThenSavepointRollback(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		f := newBackupFixture(t, m)
		wtx, err := f.src.BeginWrite()
		require.NoError(t, err)
		sp, err := wtx.Savepoint()
		require.NoError(t, err)
		restartPutTx(t, wtx, 0, backupRows, 2)
		restartSpillAll(t, f.src)
		require.NoError(t, wtx.RollbackToSavepoint(sp))
		restartPutTx(t, wtx, 0, 1, 1) // the same content: a commit of what was there
		require.NoError(t, wtx.Commit())
		f.finish(t, 1)
	})
}

// A commit that is published and then unwinds before it sent the backup its
// pages, or partway through sending them, leaves the backup with pages of the
// state before the commit. The backup starts over.
func TestBackupChange_CommitPanicsAfterPublication(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		for _, at := range []string{"before-dispatch", "mid-dispatch"} {
			f := newBackupFixture(t, m)
			wtx, err := f.src.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 0, backupRows, 2) // no spill: every page waits for the commit
			if at == "before-dispatch" {
				testCommitPostPublishHook = func() {
					testCommitPostPublishHook = nil
					panic("injected: after the commit was published")
				}
			} else {
				sent := 0
				testBackupDispatchHook = func(pgno uint32) {
					if pgno == 0 {
						return
					}
					if sent++; sent == 3 {
						testBackupDispatchHook = nil
						panic("injected: while the commit's pages are sent")
					}
				}
			}
			require.Panics(t, func() { _ = wtx.Commit() }, at)
			testCommitPostPublishHook, testBackupDispatchHook = nil, nil
			restartCheck(t, f.src, 0, backupRows, 2)
			require.Equal(t, uint32(1), f.next(t), "%s: the backup starts over", at)
			f.finish(t, 2)
			require.NoError(t, f.src.Close())
		}
	})
}

// A Step does nothing and returns ErrBusy, which is not kept, while this
// process has a write transaction open on the source (sqlite3_backup_step's
// SQLITE_BUSY, backup.c:340-341) and when no reader slot is free. The caller
// may be the one holding either: a Step that waited would wait for itself.
func TestBackupChange_StepBusy(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		f := newBackupFixture(t, m)
		before := f.next(t)

		wtx, err := f.src.BeginWrite()
		require.NoError(t, err)
		require.ErrorIs(t, f.b.Step(context.Background(), 4), ErrBusy)
		require.ErrorIs(t, f.b.Step(context.Background(), 4), ErrBusy, "busy is not sticky")
		require.Equal(t, before, f.next(t), "a busy Step copies nothing")
		restartPutTx(t, wtx, 0, backupRows, 2)
		require.NoError(t, wtx.Commit())
		require.NoError(t, f.b.Step(context.Background(), 1))

		// Every reader slot held.
		var held []*ReadTx
		for {
			rtx, err := f.src.beginRead(context.Background(), false, false)
			if err != nil {
				require.ErrorIs(t, err, ErrReadersBusy)
				break
			}
			held = append(held, rtx)
		}
		require.ErrorIs(t, f.b.Step(context.Background(), 1), ErrBusy)
		for _, rtx := range held {
			require.NoError(t, rtx.Rollback())
		}

		// A ctx that has ended: nothing is copied, nothing is kept.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		at := f.next(t)
		require.ErrorIs(t, f.b.Step(ctx, 4), context.Canceled)
		require.Equal(t, at, f.next(t))

		f.finish(t, 2)
	})
}

// Two backups in opposite directions hold each other's source as their
// destination. Neither hangs: each Step is ErrBusy until the other backup is
// finished.
func TestBackupChange_ReciprocalBackupsAreBusy(t *testing.T) {
	a, _ := openRestartDB(t, restartModes[0])
	restartPut(t, a, 0, 60, 1)
	b, _ := openRestartDB(t, restartModes[0])
	restartPut(t, b, 0, 60, 2)

	ab, err := b.BackupInit(a) // a → b
	require.NoError(t, err)
	ba, err := a.BackupInit(b) // b → a
	require.NoError(t, err)

	// a → b takes b's write lock for its destination transaction and copies.
	require.NoError(t, ab.Step(context.Background(), 1))
	// b → a takes a's as its destination, then finds b's (its source) held.
	require.ErrorIs(t, ba.Step(context.Background(), 1), ErrBusy)
	// a → b now finds a's (its source) held by the other's destination.
	require.ErrorIs(t, ab.Step(context.Background(), 1), ErrBusy)

	// Finish rolls a's destination transaction back and returns what the
	// backup's last Step did (sqlite3_backup_finish returns p->rc,
	// backup.c:603).
	require.ErrorIs(t, ba.Finish(), ErrBusy)
	for {
		err := ab.Step(context.Background(), -1)
		if errors.Is(err, ErrBackupDone) {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, ab.Finish())
	restartCheck(t, b, 0, 60, 1)
	restartCheck(t, a, 0, 60, 1)
}

// A source commit copies the list of backups and calls each outside the
// list's lock. A backup finished in between is not written to: its
// destination may be in a transaction of its owner's by then.
func TestBackupChange_UpdateAfterFinishIsDropped(t *testing.T) {
	f := newBackupFixture(t, restartModes[0])
	var dstTx *WriteTx
	var dirty int
	testBackupDispatchHook = func(pgno uint32) {
		if pgno != 0 {
			return
		}
		testBackupDispatchHook = nil
		// The list is captured. The backup is abandoned, and the
		// destination's owner starts a transaction of its own.
		require.NoError(t, f.b.Finish())
		var err error
		dstTx, err = f.dst.BeginWrite()
		require.NoError(t, err)
		restartPutTx(t, dstTx, 0, 5, 9)
		dirty = f.dst.pager.writerCache.nDirty
	}
	defer func() { testBackupDispatchHook = nil }()
	restartPut(t, f.src, 0, backupRows, 2) // its pages include ones the backup had copied
	require.NotNil(t, dstTx, "setup: the hook ran")
	require.Equal(t, dirty, f.dst.pager.writerCache.nDirty, "the finished backup wrote nothing into the destination's transaction")
	require.NoError(t, dstTx.Commit())
	restartCheck(t, f.dst, 0, 5, 9)
	require.NoError(t, f.dst.IntegrityCheck())
}

// Another process's commit restarts the backup: there is no telling which of
// the pages copied so far it changed (sqlite3BackupRestart from pager_reset,
// pager.c:1774). The commit does not move the page-1 counter (the peer
// marks nothing), so the wal-index header is all that shows it.
func TestBackupChange_ForeignCommits(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	f := newBackupFixture(t, restartModes[0])
	for i := 0; i < 4; i++ {
		before := f.next(t)
		require.Greater(t, before, uint32(1))
		runChangePeer(t, f.srcPath, "commit-all", byte(2+i))
		require.NoError(t, f.b.Step(context.Background(), 4))
		require.LessOrEqual(t, f.next(t), uint32(5), "the backup started over after the peer's commit")
		require.NoError(t, f.b.Step(context.Background(), 2))
		require.Greater(t, f.next(t), uint32(5), "and goes on while nothing else changes")
	}
	f.finish(t, 5)
}

// The peer's commit is first seen by another transaction of this process,
// not by the backup's Step: the Step's own begin finds nothing new. The
// epoch that transaction raised still restarts the backup.
func TestBackupChange_ForeignCommitSeenByAnotherReader(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	for _, first := range []string{"read", "write", "failed-begin"} {
		t.Run(first, func(t *testing.T) {
			f := newBackupFixture(t, restartModes[0])
			runChangePeer(t, f.srcPath, "commit-all", 2)
			switch first {
			case "read":
				restartCheck(t, f.src, 0, backupRows, 2)
			case "write":
				wtx, err := f.src.BeginWrite()
				require.NoError(t, err)
				require.NoError(t, wtx.Rollback())
			case "failed-begin":
				// The begin takes its snapshot, which counts the change,
				// and then fails to read the page-1 counters from the WAL.
				errRead := errors.New("injected WAL read failure")
				setWalReadFrameFaultHook(func(uint32) error { return errRead })
				_, err := f.src.BeginRead()
				setWalReadFrameFaultHook(nil)
				require.ErrorIs(t, err, errRead)
			}
			require.NoError(t, f.b.Step(context.Background(), 4))
			require.LessOrEqual(t, f.next(t), uint32(5), "the backup started over")
			f.finish(t, 2)
		})
	}
}

// The final Step lands inside an own publication that owes a step: this
// process resets the WAL (a TRUNCATE checkpoint, which does not take the
// write lock of the process) over a peer's commit nothing here had seen. The
// Step takes its snapshot at the reset header before the checkpoint has
// settled the change state. It must not finish a copy whose first pages are
// from before the peer's commit.
func TestBackupChange_FinalStepDuringResetPublication(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	f := newBackupFixture(t, restartModes[0])
	runChangePeer(t, f.srcPath, "commit-all", 2)

	var stepErr error
	ran := false
	testPublishHeaderHook = func(step string) {
		if step != "stored" || ran {
			return
		}
		ran = true
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			stepErr = f.b.Step(context.Background(), -1)
		}()
		wg.Wait()
	}
	defer func() { testPublishHeaderHook = nil }()
	require.NoError(t, f.src.Checkpoint(CheckpointTruncate))
	testPublishHeaderHook = nil
	require.True(t, ran, "setup: the Step ran inside the reset's publication")
	if stepErr != nil {
		require.ErrorIs(t, stepErr, ErrBackupDone)
	}
	f.finish(t, 2)
}

// The same with a recovery as the publication: a peer died between the header
// copies of a commit, and the Step's own begin rebuilds the wal-index.
func TestBackupChange_StepRunsRecovery(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	f := newBackupFixture(t, restartModes[0])
	runChangePeer(t, f.srcPath, "torn-all", 2)
	_, valid := f.src.pager.wal.index.readHeader()
	require.False(t, valid, "setup: the header is torn")
	require.NoError(t, f.b.Step(context.Background(), 4))
	require.LessOrEqual(t, f.next(t), uint32(5), "the backup started over")
	f.finish(t, 2)
}

// A backup beside a writer of the same process that never pauses for long:
// it is busy while a transaction is open, advances in the gaps, and is never
// restarted by the writer's commits.
func TestBackupChange_FinishesBesideOwnWriter(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		f := newBackupFixture(t, m)
		stop := make(chan struct{})
		var wg sync.WaitGroup
		var last byte = 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				fill := byte(2 + i%200)
				restartPut(t, f.src, 0, backupRows, fill)
				last = fill
				time.Sleep(time.Millisecond)
			}
		}()

		deadline := time.Now().Add(20 * time.Second)
		prev, busy := f.next(t), 0
		for {
			require.True(t, time.Now().Before(deadline), "the backup did not finish beside the writer (%d busy Steps)", busy)
			err := f.b.Step(context.Background(), 8)
			if errors.Is(err, ErrBackupDone) {
				break
			}
			if errors.Is(err, ErrBusy) {
				busy++
				time.Sleep(200 * time.Microsecond)
				continue
			}
			require.NoError(t, err)
			next := f.next(t)
			require.Greater(t, next, prev, "an own commit never restarts the backup")
			prev = next
		}
		close(stop)
		wg.Wait()
		// Commits after the final Step are not in the copy; the copy is the
		// state at that Step. Finish it and compare with the source's last
		// commit by a fresh backup of nothing: one fill throughout.
		require.NoError(t, f.b.Finish())
		rtx, err := f.dst.BeginRead()
		require.NoError(t, err)
		ns, err := rtx.GetNamespace("t1")
		require.NoError(t, err)
		val, err := rtx.Get(ns, []byte{0, 0, 0, 0})
		require.NoError(t, err)
		require.NoError(t, rtx.Rollback())
		restartCheck(t, f.dst, 0, backupRows, val[0])
		require.NoError(t, f.dst.IntegrityCheck())
		_ = last
	})
}

// Close while a Step holds the write lock of the process waits for the Step;
// it does not run a write transaction's teardown on it.
func TestBackupChange_CloseDuringStep(t *testing.T) {
	f := newBackupFixture(t, restartModes[0])
	var closeErr error
	closed := make(chan struct{})
	sawRegistered := true
	testCloseWriterCheckedHook = func(registered bool) { sawRegistered = registered }
	defer func() { testCloseWriterCheckedHook = nil }()
	ran := false
	testReadSlotClaimedHook = func() { // inside the Step, the lock held
		if ran {
			return
		}
		ran = true
		go func() {
			closeErr = f.src.Close()
			close(closed)
		}()
		select {
		case <-closed:
			t.Error("Close returned while the Step held the write lock")
		case <-time.After(100 * time.Millisecond):
		}
	}
	defer func() { testReadSlotClaimedHook = nil }()
	_ = f.b.Step(context.Background(), 4)
	testReadSlotClaimedHook = nil
	require.True(t, ran)
	<-closed
	require.NoError(t, closeErr)
	require.False(t, sawRegistered, "Close found the lock held by something that is not a write transaction")
	_ = f.b.Finish()
}

// Close against a BeginWrite that has taken every lock of a write
// transaction and has not registered it yet: the begin finds the database
// closing and backs out with ErrClosed; Close rescues nothing and returns.
func TestClose_BeginWriteNotYetRegistered(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, _ := openRestartDB(t, m)
		restartPut(t, db, 0, 60, 1)

		var closeErr error
		closed := make(chan struct{})
		checked := make(chan bool, 1)
		testCloseWriterCheckedHook = func(registered bool) { checked <- registered }
		defer func() { testCloseWriterCheckedHook = nil }()
		testWriterRegisterHook = func() {
			testWriterRegisterHook = nil
			go func() {
				closeErr = db.Close()
				close(closed)
			}()
			require.False(t, <-checked, "Close found no registered write transaction")
		}
		defer func() { testWriterRegisterHook = nil }()

		wtx, err := db.BeginWrite()
		require.ErrorIs(t, err, ErrClosed)
		require.Nil(t, wtx)
		select {
		case <-closed:
		case <-time.After(10 * time.Second):
			t.Fatal("Close did not return")
		}
		require.NoError(t, closeErr)
	})
}

// A backup that is done stays done: a further Step copies nothing and
// returns ErrBackupDone again (isFatalError(SQLITE_DONE) is true,
// backup.c:217-219, so sqlite3_backup_step returns at once, backup.c:329-330).
// The source's commits after that are not sent to the backup either, so a
// Step that went on would copy the pages the source grew by next to pages of
// the state before: Finish must commit the snapshot the backup completed at.
func TestBackupChange_DoneIsTerminal(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		f := newBackupFixture(t, m)
		for {
			err := f.b.Step(context.Background(), 64)
			if errors.Is(err, ErrBackupDone) {
				break
			}
			require.NoError(t, err)
		}
		done := f.src.DatabaseSize()

		// The source changes every row and grows.
		wtx, err := f.src.BeginWrite()
		require.NoError(t, err)
		restartPutTx(t, wtx, 0, 3*backupRows, 2)
		wtx.MarkDataChanged()
		require.NoError(t, wtx.Commit())
		require.Greater(t, f.src.DatabaseSize(), done, "setup: the source grew")

		require.ErrorIs(t, f.b.Step(context.Background(), -1), ErrBackupDone)
		require.ErrorIs(t, f.b.Step(context.Background(), 4), ErrBackupDone)
		require.NoError(t, f.b.Finish())

		check := func(db *DB) {
			restartCheck(t, db, 0, backupRows, 1)
			require.Equal(t, done, db.DatabaseSize(), "the copy is the snapshot the backup completed at")
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			ns, err := rtx.GetNamespace("t1")
			require.NoError(t, err)
			n, err := rtx.Count(ns)
			require.NoError(t, err)
			require.NoError(t, rtx.Rollback())
			require.Equal(t, backupRows, n)
			require.NoError(t, db.IntegrityCheck())
		}
		check(f.dst)
		f.dst = restartReopen(t, f.dst, f.dstPath, restartModes[0])
		check(f.dst)
	})
}
