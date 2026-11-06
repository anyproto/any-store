package btree

import (
	"context"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The write lock and the reader slots are taken with a ctx: the wait ends
// with the resource, with the ctx, or with Close.

func TestBeginWriteContext_DeadlineWhileHeld(t *testing.T) {
	db := tempDB(t)
	holder, err := db.BeginWrite()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = db.BeginWriteContext(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 5*time.Second)

	// The holder is untouched and the lock is not lost.
	require.NoError(t, holder.Commit())
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.False(t, db.HasOpenTransaction())
}

func TestBeginWriteContext_CancelWhileWaiting(t *testing.T) {
	db := tempDB(t)
	holder, err := db.BeginWrite()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := db.BeginWriteContext(ctx)
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatalf("waiter returned before cancel: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not return after cancel")
	}
	require.NoError(t, holder.Commit())
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
}

func TestBeginWriteContext_PreCancelledTakesNoLock(t *testing.T) {
	db := tempDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 1000; i++ {
		_, err := db.BeginWriteContext(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}
	require.True(t, db.tryLockWriter(), "the lock must be free")
	db.unlockWriter()
}

func TestBeginWriteContext_BackgroundWaitsForHolder(t *testing.T) {
	db := tempDB(t)
	holder, err := db.BeginWrite()
	require.NoError(t, err)
	txCh := make(chan *WriteTx, 1)
	go func() {
		tx, err := db.BeginWriteContext(context.Background())
		if err != nil {
			txCh <- nil
			return
		}
		txCh <- tx
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-txCh:
		t.Fatal("waiter must block while the lock is held")
	default:
	}
	require.NoError(t, holder.Rollback())
	select {
	case tx := <-txCh:
		require.NotNil(t, tx)
		require.NoError(t, tx.Rollback())
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not get the lock")
	}
}

func TestBeginWriteContext_SetClosingUnblocksWaiter(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{})
	require.NoError(t, err)
	holder, err := db.BeginWrite()
	require.NoError(t, err)
	errCh := make(chan error, 1)
	go func() {
		_, err := db.BeginWriteContext(context.Background())
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	db.SetClosing()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("waiter was not unblocked by SetClosing")
	}
	require.NoError(t, holder.Rollback())
	require.NoError(t, db.Close())
}

func TestBeginReadContext_DeadlineWhenSlotsHeld(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{MaxReaders: 1})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	holder, err := db.BeginRead()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = db.BeginReadContext(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = db.BeginReadFastContext(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	waitCtx, cancelWait := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := db.BeginReadContext(waitCtx)
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancelWait()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not return after cancel")
	}

	require.NoError(t, holder.Rollback())
	require.Equal(t, 0, len(db.readerSem), "a wait that ended with its ctx holds no slot")
	tx, err := db.BeginReadContext(context.Background())
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
}

func TestBeginReadContext_PreCancelledTakesNoSlot(t *testing.T) {
	db := tempDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 1000; i++ {
		_, err := db.BeginReadContext(ctx)
		require.ErrorIs(t, err, context.Canceled)
		_, err = db.BeginReadFastContext(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Equal(t, 0, len(db.readerSem))
	require.False(t, db.HasOpenTransaction())
}

func TestUnlockWrite_PanicsWhenNotHeld(t *testing.T) {
	db := tempDB(t)
	require.Panics(t, func() { db.unlockWriter() })
}

func TestClose_AbandonedWriterReleasesWaiters(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{})
	require.NoError(t, err)
	_, err = db.BeginWrite() // abandoned: never committed nor rolled back
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 2)
	go func() {
		_, err := db.BeginWriteContext(ctx)
		errs <- err
	}()
	go func() {
		_, err := db.BeginWriteContext(context.Background())
		errs <- err
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, db.Close())
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			require.ErrorIs(t, err, ErrClosed)
		case <-time.After(5 * time.Second):
			t.Fatal("a waiter did not return after Close")
		}
	}
}

// A cancel racing the handoff of the lock or of a slot never loses it.
func TestWriteLock_CancelRacingHandoffLosesNothing(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{MaxReaders: 2})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	const goroutines, rounds = 32, 300
	var wg sync.WaitGroup
	var writes, reads atomic.Int64
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for j := 0; j < rounds; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.Intn(200))*time.Microsecond)
				if j%2 == 0 {
					tx, err := db.BeginWriteContext(ctx)
					if err == nil {
						writes.Add(1)
						if err := tx.Rollback(); err != nil {
							t.Error(err)
						}
					}
				} else {
					tx, err := db.BeginReadContext(ctx)
					if err == nil {
						reads.Add(1)
						if err := tx.Rollback(); err != nil {
							t.Error(err)
						}
					}
				}
				cancel()
			}
		}(int64(i))
	}
	wg.Wait()
	require.Positive(t, writes.Load())
	require.Positive(t, reads.Load())
	tx, err := db.BeginWrite()
	require.NoError(t, err, "the write lock must not be lost to a cancelled waiter")
	require.NoError(t, tx.Rollback())
	require.Equal(t, 0, len(db.readerSem), "no reader slot may be lost to a cancelled waiter")
	require.False(t, db.HasOpenTransaction())
}
