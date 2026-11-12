//go:build (linux || darwin) && (amd64 || arm64)

package anystore_test

// Backup as its caller sees it: what it returns, matched from outside the
// package, and how long it takes to return when the transactions its copy
// begins are held up. The hold-ups are made through the btree's test seam
// on the handle itself, whose lock table refuses the handle's own
// transactions as fcntl refuses another process's.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

func openPublicDB(t *testing.T, conf *anystore.Config) anystore.DB {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "src.db"), conf)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%0200d"}`, i, i))))
	}
	return db
}

func requireBackupOf(t *testing.T, path string, docs int) {
	t.Helper()
	ctx := context.Background()
	bk, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { _ = bk.Close() }()
	coll, err := bk.OpenCollection(ctx, "c")
	require.NoError(t, err)
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, docs, n)
}

// The busy budgets of these tests: the destination's and the source's begins
// wait budget through their busy handler, and Backup retries busy Steps for
// the same time.
const budget = 300 * time.Millisecond

// handlerCalls is how many times a busy handler of the given timeout agrees
// to retry: SQLite's delays[] (main.c:1717-1720), 12 steps covering 328 ms,
// then one per 100 ms.
func handlerCalls(timeout time.Duration) int {
	delays := []int64{1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100}
	var total int64
	n := 0
	for ; total < timeout.Milliseconds(); n++ {
		d := delays[len(delays)-1]
		if n < len(delays) {
			d = delays[n]
		}
		total += d
	}
	return n
}

// shortBudgets gives the source, Backup's retry loop and (once it is open)
// the destination the test budget, and returns the destination's path.
func shortBudgets(t *testing.T, src anystore.DB) string {
	t.Helper()
	anystore.BtreeOf(src).TestSeam().SetBusyTimeout(budget)
	restore := anystore.SetBackupBusyTimeout(budget)
	t.Cleanup(restore)
	anystore.SetBackupOpenHook(func(dst *btree.DB) {
		if dst != nil {
			dst.TestSeam().SetBusyTimeout(budget)
		}
	})
	t.Cleanup(func() { anystore.SetBackupOpenHook(nil) })
	return filepath.Join(t.TempDir(), "dst.db")
}

// Every busy error a Backup can end with matches anystore.ErrBusy from
// outside the package: the source in a write transaction, no free reader
// slot, a rebuild of the wal-index in progress, and the destination's
// snapshot overtaken.
func TestBackupPublic_EveryBusyMatchesErrBusy(t *testing.T) {
	ctx := context.Background()

	t.Run("write-tx", func(t *testing.T) {
		db := openPublicDB(t, nil)
		path := shortBudgets(t, db)
		wtx, err := db.WriteTx(ctx)
		require.NoError(t, err)
		defer func() { _ = wtx.Rollback() }()
		err = db.Backup(ctx, path)
		require.True(t, errors.Is(err, anystore.ErrBusy), "got %v", err)
	})

	t.Run("reader-slots", func(t *testing.T) {
		db := openPublicDB(t, &anystore.Config{ReadConcurrency: 2})
		path := shortBudgets(t, db)
		var held []anystore.ReadTx
		for i := 0; i < 2; i++ {
			rtx, err := db.ReadTx(ctx)
			require.NoError(t, err)
			held = append(held, rtx)
		}
		err := db.Backup(ctx, path)
		for _, rtx := range held {
			require.NoError(t, rtx.Commit())
		}
		require.True(t, errors.Is(err, anystore.ErrBusy), "got %v", err)
		require.NoError(t, db.Backup(ctx, path), "and it works once a slot is free")
		requireBackupOf(t, path, 200)
	})

	t.Run("recovery", func(t *testing.T) {
		db := openPublicDB(t, nil)
		path := shortBudgets(t, db)
		seam := anystore.BtreeOf(db).TestSeam()
		restore := seam.TearHeader()
		unlockWrite, err := seam.Lock("write")
		require.NoError(t, err)
		unlockRecover, err := seam.Lock("recover")
		require.NoError(t, err)
		start := time.Now()
		err = db.Backup(ctx, path)
		elapsed := time.Since(start)
		restore()
		unlockRecover()
		unlockWrite()
		require.True(t, errors.Is(err, anystore.ErrBusy), "got %v", err)
		require.True(t, errors.Is(err, btree.ErrBusyRecovery), "the cause is kept: %v", err)
		// One public window, plus the wait of the last Step's source begin.
		require.Less(t, elapsed, 2*budget+3*time.Second)
		require.NoError(t, db.Backup(ctx, path), "and it works once the rebuild is over")
		requireBackupOf(t, path, 200)
	})

	t.Run("destination-snapshot", func(t *testing.T) {
		db := openPublicDB(t, nil)
		path := shortBudgets(t, db)
		attempts := 0
		btree.SetTestBeforeBeginWriteHook(func(s btree.TestSeam) {
			if s.Path() == path {
				attempts++
				s.PeerPublish() // the snapshot this begin took is overtaken
			}
		})
		defer btree.SetTestBeforeBeginWriteHook(nil)
		start := time.Now()
		err := db.Backup(ctx, path)
		elapsed := time.Since(start)
		btree.SetTestBeforeBeginWriteHook(nil)
		require.True(t, errors.Is(err, anystore.ErrBusy), "got %v", err)
		require.True(t, errors.Is(err, btree.ErrBusySnapshot), "the cause is kept: %v", err)
		// One write begin: at most 1 + 3 + H(T) read attempts, each a
		// write-lock try, inside one busy budget.
		require.LessOrEqual(t, attempts, 1+3+handlerCalls(budget))
		require.Greater(t, attempts, 3)
		require.Less(t, elapsed, budget+3*time.Second)
		require.NoError(t, db.Backup(ctx, path))
		requireBackupOf(t, path, 200)
	})
}

// A publisher in another process is between the two copies of the source's
// wal-index header. A pause shorter than the WAL protocol's allowance is
// waited out inside the copy, and Backup succeeds; a longer one ends it with
// the protocol error, which is not a busy error, after about the allowance.
func TestBackupPublic_PausedPublisher(t *testing.T) {
	ctx := context.Background()
	t.Run("short", func(t *testing.T) {
		db := openPublicDB(t, nil)
		path := shortBudgets(t, db)
		seam := anystore.BtreeOf(db).TestSeam()
		restore := seam.TearHeader()
		unlock, err := seam.Lock("write")
		require.NoError(t, err)
		const pause = 400 * time.Millisecond
		time.AfterFunc(pause, func() {
			restore()
			unlock()
		})
		start := time.Now()
		require.NoError(t, db.Backup(ctx, path))
		require.GreaterOrEqual(t, time.Since(start), pause-50*time.Millisecond, "the pause, longer than the busy budget, was waited out")
		requireBackupOf(t, path, 200)
	})
	t.Run("long", func(t *testing.T) {
		if testing.Short() {
			t.Skip("waits out the WAL protocol's allowance, about ten seconds")
		}
		db := openPublicDB(t, nil)
		path := shortBudgets(t, db)
		seam := anystore.BtreeOf(db).TestSeam()
		restore := seam.TearHeader()
		unlock, err := seam.Lock("write")
		require.NoError(t, err)
		start := time.Now()
		err = db.Backup(ctx, path)
		elapsed := time.Since(start)
		restore()
		unlock()
		require.ErrorIs(t, err, btree.ErrProtocol)
		require.False(t, errors.Is(err, anystore.ErrBusy))
		// One read attempt's allowance (about 10 s), not a multiple.
		require.Greater(t, elapsed, 8*time.Second)
		require.Less(t, elapsed, 20*time.Second)
		require.NoError(t, db.Backup(ctx, path), "a new Backup works once the publisher is gone")
	})
}

// The schedule the documented bound is about: every read attempt of the
// destination's write begin first waits for a publisher that is between its
// header copies, for a pause just short enough to be waited out, and then
// finds its snapshot overtaken before it has the write lock. Each attempt
// gets a fresh protocol allowance, so the begin takes far longer than its
// busy budget; it still ends within
//
//	T + N_w × (pause + one write-lock try),  N_w = 1 + 3 + H(T),
//
// and Backup returns a busy error.
func TestBackupPublic_PausedPublisherAndSnapshotInvalidation(t *testing.T) {
	ctx := context.Background()
	db := openPublicDB(t, nil)
	path := shortBudgets(t, db)

	const pause = 60 * time.Millisecond
	attempts := 0
	btree.SetTestBeforeBeginWriteHook(func(s btree.TestSeam) {
		if s.Path() != path {
			return
		}
		attempts++
		s.PeerPublish()
		// The publisher of that commit goes on to its next one and stops
		// between the header copies, the write lock held.
		restore := s.TearHeader()
		unlock, err := s.Lock("write")
		if err != nil {
			restore()
			return
		}
		time.AfterFunc(pause, func() {
			restore()
			unlock()
		})
	})
	defer btree.SetTestBeforeBeginWriteHook(nil)

	start := time.Now()
	err := db.Backup(ctx, path)
	elapsed := time.Since(start)
	btree.SetTestBeforeBeginWriteHook(nil)
	time.Sleep(2 * pause) // the last publisher lets go before the cleanup closes
	require.True(t, errors.Is(err, anystore.ErrBusy), "got %v", err)

	nw := 1 + 3 + handlerCalls(budget)
	require.LessOrEqual(t, attempts, nw)
	const slack = 150 * time.Millisecond // scheduling, per attempt
	bound := budget + time.Duration(nw)*(pause+slack)
	require.Less(t, elapsed, bound+2*time.Second, "within the documented bound (%v for %d attempts)", bound, nw)
	require.Greater(t, elapsed, budget, "and beyond the busy budget alone: each attempt waited for the publisher")
}

const publicPeerEnv = "ANYSTORE_TEST_PUBLIC_PEER_DST"

// Two processes and one destination path. This process finds the path
// absent; before it opens it, another process creates a database there and
// completes a backup into it; this process then opens the file and its
// backup fails. The file is the other process's backup, untouched.
func TestBackupPublic_DestinationCreatedByAnotherProcess(t *testing.T) {
	ctx := context.Background()
	if dst := os.Getenv(publicPeerEnv); dst != "" {
		// The other process: a backup of a database of its own to dst.
		src, err := anystore.Open(ctx, filepath.Join(filepath.Dir(dst), "peer-src.db"), nil)
		require.NoError(t, err)
		coll, err := src.CreateCollection(ctx, "peer")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
		require.NoError(t, src.Backup(ctx, dst))
		require.NoError(t, src.Close())
		return
	}

	db := openPublicDB(t, nil)
	restore := anystore.SetBackupBusyTimeout(100 * time.Millisecond)
	defer restore()
	path := filepath.Join(t.TempDir(), "dst.db")

	anystore.SetBackupOpenHook(func(dst *btree.DB) {
		if dst != nil {
			return
		}
		// About to open. The path is still absent; the peer takes it.
		_, err := os.Stat(path)
		require.True(t, os.IsNotExist(err), "setup: the destination is absent when this process looks")
		cmd := exec.Command(os.Args[0], "-test.run=^TestBackupPublic_DestinationCreatedByAnotherProcess$", "-test.timeout=60s")
		cmd.Env = append(os.Environ(), publicPeerEnv+"="+path)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "peer failed:\n%s", out)
	})
	defer anystore.SetBackupOpenHook(nil)

	// This process's backup cannot copy: its source is in a write
	// transaction of another goroutine's.
	wtx, err := db.WriteTx(ctx)
	require.NoError(t, err)
	defer func() { _ = wtx.Rollback() }()
	err = db.Backup(ctx, path)
	require.True(t, errors.Is(err, anystore.ErrBusy), "got %v", err)

	bk, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { _ = bk.Close() }()
	names, err := bk.GetCollectionNames(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"peer"}, names, "the other process's backup is intact")
}

// Close while Backup waits to retry a busy Step: the write transaction that
// made the source busy is abandoned, Close rescues it, and Backup ends with
// the closed error.
func TestBackupPublic_CloseDuringBusyWait(t *testing.T) {
	ctx := context.Background()
	db := openPublicDB(t, nil)
	_, err := db.WriteTx(ctx) // abandoned
	require.NoError(t, err)

	waiting := make(chan struct{}, 1)
	anystore.SetBackupBusyWaitHook(func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	})
	defer anystore.SetBackupBusyWaitHook(nil)
	done := make(chan error, 1)
	go func() { done <- db.Backup(ctx, filepath.Join(t.TempDir(), "b.db")) }()
	select {
	case <-waiting: // Backup is in its busy wait
	case <-time.After(10 * time.Second):
		t.Fatal("Backup never met the busy source")
	}
	require.NoError(t, db.Close())
	select {
	case err := <-done:
		require.ErrorIs(t, err, anystore.ErrDBIsClosed)
	case <-time.After(10 * time.Second):
		t.Fatal("Backup did not return after Close")
	}
}
