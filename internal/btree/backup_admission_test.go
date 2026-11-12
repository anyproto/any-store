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

	// The Step's begin calls the busy handler while the rebuild is in
	// progress: the first call says the Step is in its wait.
	inWait := make(chan struct{})
	var once sync.Once
	base := w.busyHandler
	w.busyHandler = func(n int) bool {
		once.Do(func() { close(inWait) })
		return base(n)
	}

	var stepErr error
	stepDone := make(chan struct{})
	go func() {
		defer close(stepDone)
		stepErr = f.b.Step(context.Background(), 4)
	}()
	<-inWait
	closeDone := make(chan error, 1)
	go func() { closeDone <- f.src.Close() }()
	select {
	case <-closeDone:
		t.Fatal("Close returned while the Step held the write lock of the process")
	case <-stepDone: // the Step's budget ran out: Close may go on now
	}
	require.ErrorIs(t, stepErr, ErrBusyRecovery)
	// The peer goes away; Close's own checkpoint needs the locks it held.
	region[16] ^= 0x01
	require.NoError(t, w.index.unlock(lockRecover, lockExclusive))
	require.NoError(t, w.index.unlock(lockWrite, lockExclusive))
	require.NoError(t, <-closeDone)
	require.False(t, sawRegistered, "Close found no write transaction to rescue")
	_ = f.b.Finish()
}

// A ctx that ends, and a Close, while a Step waits out a publisher that is
// between its header copies (the WAL protocol's retry loop, not the busy
// handler): the Step goes on to copy when the publisher is done, the ctx's
// error is the next Step's, and Close waits for the Step.
func TestBackupAdmission_CancelAndCloseDuringProtocolWait(t *testing.T) {
	f := newBackupFixture(t, restartModes[0])
	w := f.src.pager.wal
	before := f.next(t)
	region, err := w.index.shm.region(0, true)
	require.NoError(t, err)

	pause := func(d time.Duration) {
		region[16] ^= 0x01
		require.NoError(t, w.index.lock(lockWrite, lockExclusive))
		time.AfterFunc(d, func() {
			region[16] ^= 0x01
			_ = w.index.unlock(lockWrite, lockExclusive)
		})
	}

	// Cancel during the wait.
	ctx, cancel := context.WithCancel(context.Background())
	pause(300 * time.Millisecond)
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	require.NoError(t, f.b.Step(ctx, 4), "the Step does not look at ctx inside its begin")
	require.GreaterOrEqual(t, time.Since(start), 250*time.Millisecond)
	after := f.next(t)
	require.Greater(t, after, before)
	require.ErrorIs(t, f.b.Step(ctx, 4), context.Canceled)
	require.Equal(t, after, f.next(t))

	// Close during the wait.
	pause(300 * time.Millisecond)
	stepDone := make(chan error, 1)
	go func() { stepDone <- f.b.Step(context.Background(), 4) }()
	time.Sleep(50 * time.Millisecond) // the Step is in the retry loop: nothing to witness it by
	closeStart := time.Now()
	require.NoError(t, f.src.Close())
	require.GreaterOrEqual(t, time.Since(closeStart), 150*time.Millisecond, "Close waited for the Step")
	err = <-stepDone
	if err != nil { // the Step ended before Close set closing, or found it set
		require.ErrorIs(t, err, ErrClosed)
	}
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

// A Step whose snapshot is the header a recovery is replacing, read while
// the recovery's publication is pending: the header is announced and written
// and the change state not settled, so the state still holds the old header
// next to an epoch that already counts the rebuild. The Step's snapshot
// equals that retained header and is not exact: it starts over, and so does
// the next Step, with the epoch unchanged between the two.
func TestBackupChange_StepDuringAPendingPublication(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	f := newBackupFixture(t, restartModes[0])
	require.NoError(t, f.b.Step(context.Background(), 4))
	require.Greater(t, f.next(t), uint32(5))
	w := f.src.pager.wal

	pending := make(chan struct{})
	sampled := make(chan struct{})
	var wg sync.WaitGroup
	var duringEpoch uint64
	var duringPending bool
	ran := false
	testReadSlotClaimedHook = func() { // the Step has its snapshot, at H0
		if ran {
			return
		}
		ran = true
		runChangePeer(t, f.srcPath, "torn-all", 2)
		testPublishHeaderHook = func(step string) {
			if step != "stored" {
				return
			}
			testPublishHeaderHook = nil
			cur := w.chg.Load()
			duringEpoch, duringPending = cur.epoch, cur.pending != nil
			close(pending) // announced and written
			<-sampled      // not settled until the Step has read the state
		}
		wg.Add(1)
		go func() { // another begin of this process rebuilds the wal-index
			defer wg.Done()
			rtx, err := f.src.beginRead(context.Background(), false, true)
			if err == nil {
				_ = rtx.Rollback()
			}
		}()
		<-pending
	}
	testBackupSampledHook = func() {
		testBackupSampledHook = nil
		close(sampled)
	}
	defer func() { testReadSlotClaimedHook, testPublishHeaderHook, testBackupSampledHook = nil, nil, nil }()

	require.NoError(t, f.b.Step(context.Background(), 4))
	wg.Wait()
	testReadSlotClaimedHook = nil
	require.True(t, ran)
	require.True(t, duringPending, "setup: the Step read the state between the announcement and the settle")
	f.b.mu.Lock()
	epoch, exact := f.b.epoch, f.b.exact
	f.b.mu.Unlock()
	require.Equal(t, duringEpoch, epoch, "the epoch the Step read already counts the rebuild")
	require.False(t, exact, "the retained header certifies nothing while a publication is pending")
	require.LessOrEqual(t, f.next(t), uint32(5), "the Step started over")

	require.NoError(t, f.b.Step(context.Background(), 4))
	f.b.mu.Lock()
	epoch2 := f.b.epoch
	f.b.mu.Unlock()
	require.Equal(t, epoch, epoch2, "setup: the epoch did not move between the two Steps")
	require.LessOrEqual(t, f.next(t), uint32(5), "the next Step starts over too")
	f.finish(t, 2)
}

// A backup whose Steps overlap the commits of a writer in another process,
// beside readers of its own: the peer commits in a loop while the backup
// steps without pause. Whatever the interleaving, once the peer is done the
// backup ends as one committed state, and it was restarted on the way.
func TestBackupChange_OverlappingForeignWriter(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	f := newBackupFixture(t, restartModes[0])
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if rtx, err := f.src.BeginRead(); err == nil {
					if ns, err := rtx.GetNamespace("t1"); err == nil {
						_, _ = rtx.Get(ns, []byte{0, 0, 0, 7})
					}
					_ = rtx.Rollback()
				}
			}
		}()
	}

	peer := startChangePeer(t, f.srcPath, "commit-loop-all", 20)
	peerDone := make(chan error, 1)
	go func() { peerDone <- peer.Wait() }()

	restarts, prev := 0, f.next(t)
	running := true
	for running {
		select {
		case err := <-peerDone:
			require.NoError(t, err)
			running = false
		default:
		}
		// One page at a time, slowly: a pass over the source takes longer
		// than the peer's pause between commits, so commits land inside it.
		time.Sleep(300 * time.Microsecond)
		err := f.b.Step(context.Background(), 1)
		if errors.Is(err, ErrBusy) || errors.Is(err, ErrBusySnapshot) || errors.Is(err, ErrBusyRecovery) {
			continue
		}
		if errors.Is(err, ErrBackupDone) {
			// Done between two of the peer's commits: a consistent copy of
			// an earlier state. Start a new backup to go on overlapping.
			require.NoError(t, f.b.Finish())
			f.b, err = f.dst.BackupInit(f.src)
			require.NoError(t, err)
			prev = 1
			continue
		}
		require.NoError(t, err)
		if next := f.next(t); next < prev {
			restarts++
			prev = next
		} else {
			prev = next
		}
	}
	close(stop)
	wg.Wait()
	require.Positive(t, restarts, "the peer's commits restarted the backup while it was stepping")
	f.finish(t, 21) // the peer's last fill: 2 + 19
}

// Two begins find the same change at once. The first to swap the change
// state counts it; the other's swap fails, and on its second look the header
// is its snapshot's: it counts nothing. A change is counted once however
// many begins race for it.
func TestChangeEpoch_RacingBeginsCountOnce(t *testing.T) {
	if changePeerChild(t) {
		return
	}
	db, path := openChangeDB(t)
	w := db.pager.wal
	e0, _ := changeEpochOf(t, db)
	runChangePeer(t, path, "commit", 3)

	swaps := 0
	testChangeSwapHook = func() {
		swaps++
		if swaps > 1 {
			return
		}
		// This begin is about to swap. Another begin gets there first.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			rtx, err := db.beginRead(context.Background(), false, true)
			if err == nil {
				_ = rtx.Rollback()
			}
		}()
		wg.Wait()
	}
	defer func() { testChangeSwapHook = nil }()
	e, exact := changeEpochOf(t, db)
	testChangeSwapHook = nil
	require.Equal(t, 2, swaps, "setup: two begins went for the swap, and the loser did not try again")
	require.Equal(t, e0+1, e, "the change is counted once")
	require.True(t, exact)
	require.Nil(t, w.chg.Load().pending)
}
