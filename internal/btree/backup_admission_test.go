//go:build (linux || darwin) && (amd64 || arm64)

package btree

// What a backup Step returns when its transaction begins are held up, and
// what a consumer of the change epoch does with a snapshot that is not
// exact. The Steps are what the public Backup loop retries or gives up on:
// a busy result is not kept and the same Backup goes on once the cause is
// gone; ErrProtocol is kept.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// peerPublish writes a wal-index header that differs from the live one, as a
// commit of another process leaves it: both copies, iChange one up.
func peerPublish(t *testing.T, db *DB) {
	t.Helper()
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

// The wal-index is being rebuilt by another process when a Step begins: the
// Step returns ErrBusyRecovery, keeps nothing, and the same Backup goes on
// once the rebuild is over. A ctx that ends, or a Close, while the Step is
// waiting takes effect when the Step returns: the Step's wait is the busy
// handler's, as any begin's.
func TestBackupAdmission_RecoveryInProgress(t *testing.T) {
	f := newBackupFixture(t, restartModes[0])
	w := f.src.pager.wal
	w.busyHandler = DefaultBusyTimeout(300 * time.Millisecond)
	before := f.next(t)

	// A peer holds the write lock and the recover lock over a header it is
	// rebuilding. (Through this handle's lock table, which refuses the
	// Step's begin as fcntl refuses another process's.)
	region, err := w.index.shm.region(0, true)
	require.NoError(t, err)
	region[16] ^= 0x01
	require.NoError(t, w.index.lock(lockWrite, lockExclusive))
	require.NoError(t, w.index.lock(lockRecover, lockExclusive))
	release := func() {
		region[16] ^= 0x01 // the peer's rebuild put the header back
		require.NoError(t, w.index.unlock(lockRecover, lockExclusive))
		require.NoError(t, w.index.unlock(lockWrite, lockExclusive))
	}

	start := time.Now()
	err = f.b.Step(context.Background(), 4)
	require.ErrorIs(t, err, ErrBusyRecovery)
	require.GreaterOrEqual(t, time.Since(start), 250*time.Millisecond, "the begin waited through the busy handler")
	require.Less(t, time.Since(start), 5*time.Second)
	require.Equal(t, before, f.next(t), "a busy Step copies nothing")

	// A ctx that ends during the wait: the Step still returns its own
	// result; the next Step returns the ctx's error. Neither is kept.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	require.ErrorIs(t, f.b.Step(ctx, 4), ErrBusyRecovery)
	require.ErrorIs(t, f.b.Step(ctx, 4), context.Canceled)

	// The write lock of the process, which the Step held while it waited,
	// is free again.
	require.True(t, f.src.tryLockWriter(), "a busy Step released the process's write lock")
	f.src.unlockWriter()

	release()
	require.NoError(t, f.b.Step(context.Background(), 4))
	require.Greater(t, f.next(t), before, "the same backup goes on after the rebuild")
	f.finish(t, 1)
}

// Close while a Step waits for a rebuild: Close waits for the Step, which
// ends busy, and then closes; it does not take the Step for a write
// transaction.
func TestBackupAdmission_CloseDuringRecoveryWait(t *testing.T) {
	f := newBackupFixture(t, restartModes[0])
	w := f.src.pager.wal
	w.busyHandler = DefaultBusyTimeout(400 * time.Millisecond)
	region, err := w.index.shm.region(0, true)
	require.NoError(t, err)
	region[16] ^= 0x01
	require.NoError(t, w.index.lock(lockWrite, lockExclusive))
	require.NoError(t, w.index.lock(lockRecover, lockExclusive))

	sawRegistered := true
	testCloseWriterCheckedHook = func(registered bool) { sawRegistered = registered }
	defer func() { testCloseWriterCheckedHook = nil }()

	var stepErr, closeErr error
	var wg sync.WaitGroup
	wg.Add(2)
	stepStarted := make(chan struct{})
	go func() {
		defer wg.Done()
		close(stepStarted)
		stepErr = f.b.Step(context.Background(), 4)
	}()
	<-stepStarted
	time.Sleep(100 * time.Millisecond) // the Step is in its begin's wait
	closeStart := time.Now()
	go func() {
		defer wg.Done()
		// The peer goes away while Close waits.
		time.AfterFunc(500*time.Millisecond, func() {
			region[16] ^= 0x01
			_ = w.index.unlock(lockRecover, lockExclusive)
			_ = w.index.unlock(lockWrite, lockExclusive)
		})
		closeErr = f.src.Close()
	}()
	wg.Wait()
	require.ErrorIs(t, stepErr, ErrBusyRecovery)
	require.NoError(t, closeErr)
	require.GreaterOrEqual(t, time.Since(closeStart), 200*time.Millisecond, "Close waited for the Step")
	require.False(t, sawRegistered, "Close found no write transaction to rescue")
	_ = f.b.Finish()
}

// A publisher in another process is between the two copies of a header,
// holding the write lock, with no rebuild running. A begin retries for about
// ten seconds (the WAL protocol's allowance) and then fails with
// ErrProtocol. A pause shorter than that is waited out inside the Step; a
// longer one ends the backup: ErrProtocol is kept, as SQLITE_PROTOCOL is
// (backup.c:217-219).
func TestBackupAdmission_PausedPublisher(t *testing.T) {
	t.Run("short", func(t *testing.T) {
		f := newBackupFixture(t, restartModes[0])
		w := f.src.pager.wal
		before := f.next(t)
		region, err := w.index.shm.region(0, true)
		require.NoError(t, err)
		region[16] ^= 0x01
		require.NoError(t, w.index.lock(lockWrite, lockExclusive))
		time.AfterFunc(300*time.Millisecond, func() {
			region[16] ^= 0x01
			_ = w.index.unlock(lockWrite, lockExclusive)
		})
		start := time.Now()
		require.NoError(t, f.b.Step(context.Background(), 4))
		require.GreaterOrEqual(t, time.Since(start), 250*time.Millisecond, "the Step waited the pause out")
		require.Greater(t, f.next(t), before)
		f.finish(t, 1)
	})
	t.Run("long", func(t *testing.T) {
		if testing.Short() {
			t.Skip("waits out the WAL protocol's allowance, about ten seconds")
		}
		f := newBackupFixture(t, restartModes[0])
		w := f.src.pager.wal
		region, err := w.index.shm.region(0, true)
		require.NoError(t, err)
		region[16] ^= 0x01
		require.NoError(t, w.index.lock(lockWrite, lockExclusive))
		start := time.Now()
		err = f.b.Step(context.Background(), 4)
		elapsed := time.Since(start)
		region[16] ^= 0x01
		require.NoError(t, w.index.unlock(lockWrite, lockExclusive))
		require.ErrorIs(t, err, ErrProtocol)
		require.Less(t, elapsed, 30*time.Second, "one read attempt's allowance, not more")
		require.ErrorIs(t, f.b.Step(context.Background(), 4), ErrProtocol, "ErrProtocol is kept")
		require.ErrorIs(t, f.b.Finish(), ErrProtocol)
	})
}

// The destination's write begin finds, at every try, that another process
// committed since it took its snapshot. It gives up as ErrBusySnapshot within
// the begin's one busy budget (plus the tight retries); the Step returns it
// and keeps nothing, and the same Backup goes on once the commits stop.
func TestBackupAdmission_DestinationSnapshotInvalidated(t *testing.T) {
	src, _ := openRestartDB(t, restartModes[0])
	restartPut(t, src, 0, backupRows, 1)
	dst, dstPath := openRestartDB(t, restartModes[0])
	restartPut(t, dst, 0, 10, 9) // the destination has a log of its own
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	defer func() { _ = b.Finish() }()

	const budget = 300 * time.Millisecond
	dst.pager.wal.busyHandler = DefaultBusyTimeout(budget)
	invalidations := 0
	// Every snapshot the destination's begin takes is overtaken before the
	// begin has the write lock: the hook of the pager's begin runs between
	// the two.
	dstPager := dst.pager
	testBeforeBeginWriteHook = func(p *pager) {
		if p == dstPager {
			invalidations++
			peerPublish(t, dst)
		}
	}
	defer func() { testBeforeBeginWriteHook = nil }()

	start := time.Now()
	err = b.Step(context.Background(), 4)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, ErrBusySnapshot)
	require.Greater(t, invalidations, busySnapshotInnerRetries, "the begin went through its tight retries and the busy handler")
	require.Less(t, elapsed, budget+2*time.Second, "one busy budget for the whole begin")
	require.ErrorIs(t, b.Step(context.Background(), 4), ErrBusySnapshot, "and nothing is kept: the next Step tries again")

	testBeforeBeginWriteHook = nil
	for {
		err := b.Step(context.Background(), 64)
		if errors.Is(err, ErrBackupDone) {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, b.Finish())
	restartCheck(t, dst, 0, backupRows, 1)
	dst = restartReopen(t, dst, dstPath, restartModes[0])
	restartCheck(t, dst, 0, backupRows, 1)
	require.NoError(t, dst.IntegrityCheck())
}

// A Step whose snapshot is older than what the change epoch counts: it took
// the snapshot, and before it asked for the epoch another process's commit
// was counted by another transaction of this process. The Step's snapshot is
// not exact for the epoch it reads. It starts the copy over at once, and so
// does the next Step, although the epoch has not moved between the two: the
// pages the first of them copied are of a state the epoch says is past.
func TestBackupChange_StepAtAnOlderSnapshotThanTheEpoch(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	f := newBackupFixture(t, restartModes[0])
	require.NoError(t, f.b.Step(context.Background(), 4))
	require.Greater(t, f.next(t), uint32(5))

	ran := false
	testReadSlotClaimedHook = func() { // the Step has its snapshot
		if ran {
			return
		}
		ran = true
		runChangePeer(t, f.srcPath, "commit-all", 2)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { // another transaction of this process counts the commit
			defer wg.Done()
			rtx, err := f.src.beginRead(context.Background(), false, true)
			if err == nil {
				_ = rtx.Rollback()
			}
		}()
		wg.Wait()
	}
	defer func() { testReadSlotClaimedHook = nil }()
	require.NoError(t, f.b.Step(context.Background(), 4))
	testReadSlotClaimedHook = nil
	require.True(t, ran)
	require.LessOrEqual(t, f.next(t), uint32(5), "the Step started over: its snapshot is not exact")
	f.b.mu.Lock()
	epoch, exact := f.b.epoch, f.b.exact
	f.b.mu.Unlock()
	require.False(t, exact)

	require.NoError(t, f.b.Step(context.Background(), 4))
	f.b.mu.Lock()
	epoch2 := f.b.epoch
	f.b.mu.Unlock()
	require.Equal(t, epoch, epoch2, "setup: the epoch did not move between the two Steps")
	require.LessOrEqual(t, f.next(t), uint32(5), "the next Step starts over too: what was copied before it was of the older state")
	f.finish(t, 2)
}

// A backup beside readers of its own process and a writer in another: every
// commit of the writer restarts it, the readers neither hide a commit from
// it nor break it, and it ends as one committed state once the writer is
// done.
func TestBackupChange_BesideReadersAndAForeignWriter(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	f := newBackupFixture(t, restartModes[0])
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var readErr error
	var readMu sync.Mutex
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				rtx, err := f.src.BeginRead()
				if err == nil {
					var ns *Namespace
					if ns, err = rtx.GetNamespace("t1"); err == nil {
						_, err = rtx.Get(ns, []byte{0, 0, 0, 7})
					}
					_ = rtx.Rollback()
				}
				if err != nil {
					readMu.Lock()
					readErr = err
					readMu.Unlock()
					return
				}
			}
		}()
	}

	const commits = 6
	for i := 0; i < commits; i++ {
		// The peer's commit lands between two Steps, with the readers
		// beginning all the while: one of them usually counts it first.
		runChangePeer(t, f.srcPath, "commit-all", byte(2+i))
		for j := 0; j < 3; j++ {
			err := f.b.Step(context.Background(), 2)
			if errors.Is(err, ErrBusy) { // every reader slot taken for an instant
				continue
			}
			require.NoError(t, err)
		}
		require.LessOrEqual(t, f.next(t), uint32(7), "commit %d restarted the backup", i)
	}
	close(stop)
	wg.Wait()
	readMu.Lock()
	require.NoError(t, readErr)
	readMu.Unlock()
	f.finish(t, byte(1+commits))
}
