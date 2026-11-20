package btree

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// DB.Close ends a write transaction it finds, whatever its goroutine is
// doing. These tests pin how: Close waits for the transaction's call in
// flight (writerGate), then rolls back and tears down, and every later call
// of the transaction is refused with ErrClosed before it touches a page.

// closeFromCall starts db.Close on its own goroutine from inside a call of
// the write transaction and returns once Close has closed the gate, found
// the call in flight and is about to wait for it. The returned channel
// yields Close's result.
func closeFromCall(t *testing.T, db *DB) chan error {
	t.Helper()
	waiting := make(chan struct{})
	testWriterGateWaitingHook = func() { close(waiting) }
	done := make(chan error, 1)
	go func() { done <- db.Close() }()
	select {
	case <-waiting:
	case err := <-done:
		t.Fatalf("Close returned (%v) with a call of the write transaction in flight", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not reach the wait at the writer gate")
	}
	testWriterGateWaitingHook = nil
	return done
}

// holdCloseAtDrain makes db.Close stop once it has closed the writer gate
// and found it empty, before the forced rollback. It returns a channel that
// is closed when Close stands there, and the function that lets it go on.
func holdCloseAtDrain(t *testing.T) (drained chan struct{}, release func()) {
	t.Helper()
	drained = make(chan struct{})
	hold := make(chan struct{})
	testWriterGateDrainedHook = func() {
		close(drained)
		<-hold
	}
	var once sync.Once
	release = func() { once.Do(func() { close(hold) }) }
	t.Cleanup(func() {
		release()
		testWriterGateDrainedHook = nil
	})
	return drained, release
}

func TestClose_WaitsForWriterCallInFlight(t *testing.T) {
	for _, mode := range []struct {
		name string
		opts Options
		// arm installs a hook that calls fire once, inside a Put of tx.
		arm func(t *testing.T, db *DB, fire func())
	}{
		{
			// Inside rebuildLeafPage, before it writes the page: the point at
			// which a torn-down cache made the resumed Put panic.
			name: "in a page rebuild",
			opts: Options{DisableAutoCheckpoint: true},
			arm: func(t *testing.T, db *DB, fire func()) {
				setRebuildPageFaultHook(func(*page) error { fire(); return nil })
				t.Cleanup(func() { setRebuildPageFaultHook(nil) })
			},
		},
		{
			name: "in a page rebuild, InMemory",
			opts: Options{InMemory: true},
			arm: func(t *testing.T, db *DB, fire func()) {
				setRebuildPageFaultHook(func(*page) error { fire(); return nil })
				t.Cleanup(func() { setRebuildPageFaultHook(nil) })
			},
		},
		{
			// Inside pagerStress, a spill about to write to the log: the
			// point at which the resumed Put reported a healthy file corrupt.
			name: "in a spill",
			opts: Options{CacheSize: closeRescueCache, DisableAutoCheckpoint: true},
			arm: func(t *testing.T, db *DB, fire func()) {
				fillWriterCacheDirty(t, db.pager)
				testStressGuardPassedHook = fire
				t.Cleanup(func() { testStressGuardPassedHook = nil })
			},
		},
	} {
		t.Run(mode.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "a.db")
			db, err := testOpen(t, path, mode.opts)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			createNS(t, db, "t")
			putN(t, db, "t", closeRescueRows, closeRescueValSize)

			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.GetNamespace("t")
			require.NoError(t, err)
			cur := tx.NewCursor(ns)
			require.NoError(t, cur.First())

			var (
				fired    bool
				closed   chan error
				returned bool // Close returned while the call was in flight
			)
			mode.arm(t, db, func() {
				if fired {
					return
				}
				fired = true
				// closeFromCall returns with Close in the gate's wait, which
				// only this call's return ends.
				closed = closeFromCall(t, db)
				select {
				case <-closed:
					returned = true
				default:
				}
			})

			// The call in which the hook fires runs to its end, undisturbed:
			// no panic, no error.
			for i := closeRescueRows + 1; i <= closeRescueRows+200 && !fired; i++ {
				require.NoError(t, tx.Put(ns, closeRescueKey(i), bytes.Repeat([]byte{0xEE}, 1500)))
			}
			require.True(t, fired, "the hook did not fire")
			require.False(t, returned, "Close returned while a call of the write transaction was in flight")

			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("Close did not return after the call in flight ended")
			}

			// Every later call of the transaction is refused, reads included,
			// and none of them panics on the cache Close freed.
			require.ErrorIs(t, tx.Put(ns, closeRescueKey(1), []byte("x")), ErrClosed)
			require.ErrorIs(t, tx.Delete(ns, closeRescueKey(1)), ErrClosed)
			_, err = tx.Get(ns, closeRescueKey(1))
			require.ErrorIs(t, err, ErrClosed)
			_, err = tx.Has(ns, closeRescueKey(1))
			require.ErrorIs(t, err, ErrClosed)
			_, err = tx.SeekKey(ns, closeRescueKey(1))
			require.ErrorIs(t, err, ErrClosed)
			_, err = tx.Count(ns)
			require.ErrorIs(t, err, ErrClosed)
			_, err = tx.NamespaceSize(ns)
			require.ErrorIs(t, err, ErrClosed)
			_, err = tx.GetNamespace("t")
			require.ErrorIs(t, err, ErrClosed)
			_, err = tx.CreateNamespace("u")
			require.ErrorIs(t, err, ErrClosed)
			require.ErrorIs(t, tx.DeleteNamespace("t"), ErrClosed)
			require.ErrorIs(t, tx.RenameNamespace("t", "v"), ErrClosed)
			_, err = tx.Savepoint()
			require.ErrorIs(t, err, ErrClosed)
			require.ErrorIs(t, tx.RollbackToSavepoint(0), ErrClosed)
			require.ErrorIs(t, tx.ReleaseSavepoint(0), ErrClosed)
			require.False(t, tx.HasChanges())

			require.ErrorIs(t, cur.Next(), ErrClosed)
			require.ErrorIs(t, cur.Previous(), ErrClosed)
			require.ErrorIs(t, cur.First(), ErrClosed)
			require.ErrorIs(t, cur.Last(), ErrClosed)
			require.ErrorIs(t, cur.Seek(closeRescueKey(1)), ErrClosed)
			require.ErrorIs(t, cur.SeekNear(closeRescueKey(1)), ErrClosed)
			require.ErrorIs(t, cur.SeekExact(closeRescueKey(1)), ErrClosed)
			require.ErrorIs(t, cur.Skip(1), ErrClosed)
			require.ErrorIs(t, cur.SkipBackward(1), ErrClosed)
			_, err = cur.Key()
			require.ErrorIs(t, err, ErrClosed)
			_, err = cur.Value()
			require.ErrorIs(t, err, ErrClosed)
			_, err = cur.AppendValue(nil)
			require.ErrorIs(t, err, ErrClosed)
			_, err = cur.AppendValueByKey(closeRescueKey(1), nil)
			require.ErrorIs(t, err, ErrClosed)
			_, err = cur.CountUntil(nil, false)
			require.ErrorIs(t, err, ErrClosed)
			_, err = cur.RangeFraction(nil, nil)
			require.ErrorIs(t, err, ErrClosed)
			cur.Close()
			late := tx.NewCursor(ns)
			require.ErrorIs(t, late.First(), ErrClosed)
			late.Close()

			require.ErrorIs(t, tx.Commit(), ErrClosed)

			if mode.opts.InMemory {
				return
			}
			// The transaction was rolled back, spilled pages and all.
			db, err = testOpen(t, path, Options{})
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			require.Equal(t, closeRescueRows, countKeys(t, db, "t"))
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// Between two calls of its transaction the writer's goroutine is not in the
// gate: Close called there, by that goroutine or another, does not wait.
func TestClose_ByWriterGoroutineBetweenCalls(t *testing.T) {
	db, err := testOpen(t, filepath.Join(t.TempDir(), "a.db"), Options{})
	require.NoError(t, err)
	createNS(t, db, "t")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("t")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, closeRescueKey(1), []byte("x")))
	cur := tx.NewCursor(ns)
	require.NoError(t, cur.First())

	require.NoError(t, db.Close())

	require.ErrorIs(t, tx.Put(ns, closeRescueKey(2), []byte("x")), ErrClosed)
	cur.Close()
	require.NoError(t, tx.Rollback())
}

// A call that panics leaves the gate: Close after it does not wait for a
// call that will never return.
func TestClose_AfterWriterCallPanicked(t *testing.T) {
	db, err := testOpen(t, filepath.Join(t.TempDir(), "a.db"), Options{})
	require.NoError(t, err)
	createNS(t, db, "t")
	putN(t, db, "t", 200, 500)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("t")
	require.NoError(t, err)

	setRebuildPageFaultHook(func(*page) error { panic("page rebuild") })
	func() {
		defer func() {
			setRebuildPageFaultHook(nil)
			require.Equal(t, "page rebuild", recover())
		}()
		for i := 1000; i < 1200; i++ {
			_ = tx.Put(ns, closeRescueKey(i), bytes.Repeat([]byte{0xEE}, 1500))
		}
	}()
	require.Zero(t, db.pager.wgate.n.Load(), "the panicked call is still counted")

	done := make(chan error, 1)
	go func() { done <- db.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close waits for a call that panicked")
	}
}

func TestWriterGate(t *testing.T) {
	t.Run("open: calls nest and leave nothing behind", func(t *testing.T) {
		g := newWriterGate()
		require.NoError(t, g.enter())
		require.NoError(t, g.enter())
		g.leave()
		g.leave()
		require.Zero(t, g.n.Load())
		require.False(t, g.closed())
	})
	t.Run("close with nothing in flight returns and refuses", func(t *testing.T) {
		g := newWriterGate()
		g.close()
		require.True(t, g.closed())
		require.ErrorIs(t, g.enter(), ErrClosed)
		require.ErrorIs(t, g.enter(), ErrClosed)
		require.Equal(t, int32(writerGateClosed), g.n.Load(), "a refused call is not counted")
	})
	t.Run("close waits for the call in flight, calls it makes included", func(t *testing.T) {
		g := newWriterGate()
		require.NoError(t, g.enter())
		done := make(chan struct{})
		go func() { g.close(); close(done) }()
		for !g.closed() {
			time.Sleep(time.Millisecond)
		}
		// The call in flight goes on to make another: let in, not refused.
		require.NoError(t, g.enter())
		g.leave()
		select {
		case <-done:
			t.Fatal("close returned with a call in flight")
		case <-time.After(50 * time.Millisecond):
		}
		g.leave()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("close did not return after the last call left")
		}
		require.ErrorIs(t, g.enter(), ErrClosed)
	})
}

// Close races a writer that puts, reads and scans without a pause. Run under
// the race detector, this is the test of the gate: before it, pcache.destroy
// raced the writer's page accesses, and without -race the writer panicked on
// a nil page buffer or returned ErrCorrupt.
func TestClose_DuringRunningWriter(t *testing.T) {
	for _, mode := range []struct {
		name string
		opts Options
	}{
		{"file", Options{CacheSize: closeRescueCache}},
		{"InMemory", Options{InMemory: true}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			iters := 100
			if testing.Short() {
				iters = 10
			}
			dir := t.TempDir()
			for it := 0; it < iters; it++ {
				db, err := testOpen(t, filepath.Join(dir, "a.db"), mode.opts)
				require.NoError(t, err)
				tx, err := db.BeginWrite()
				require.NoError(t, err)
				ns, err := tx.CreateNamespace("t")
				if errors.Is(err, ErrNamespaceExists) {
					ns, err = tx.GetNamespace("t")
				}
				require.NoError(t, err)

				var (
					wg       sync.WaitGroup
					started  = make(chan struct{})
					panicked atomic.Value
					opErr    error
				)
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() {
						if r := recover(); r != nil {
							panicked.Store(r)
						}
					}()
					var once sync.Once
					defer once.Do(func() { close(started) })
					cur := tx.NewCursor(ns)
					defer cur.Close()
					val := bytes.Repeat([]byte{0xAB}, 700)
					// failed reports an error other than a key the loop's own
					// deletes removed.
					failed := func(err error) bool {
						opErr = err
						return err != nil && !errors.Is(err, ErrKeyNotFound)
					}
					for i := 0; ; i++ {
						if i == 20 {
							once.Do(func() { close(started) })
						}
						if failed(tx.Put(ns, closeRescueKey(i), val)) {
							return
						}
						if _, err := tx.Get(ns, closeRescueKey(i/2)); failed(err) {
							return
						}
						if failed(cur.Seek(closeRescueKey(i / 3))) {
							return
						}
						if _, err := cur.Key(); failed(err) {
							return
						}
						if i%64 == 0 && failed(tx.Delete(ns, closeRescueKey(i/4))) {
							return
						}
					}
				}()
				<-started
				time.Sleep(time.Duration(it%5) * 200 * time.Microsecond)
				require.NoError(t, db.Close())
				wg.Wait()
				require.Nil(t, panicked.Load(), "the writer panicked")
				require.ErrorIs(t, opErr, ErrClosed)
				require.ErrorIs(t, tx.Commit(), ErrClosed)
			}
		})
	}
}

// Closing the gate is the one decision that ends the write transaction:
// once a call of it has been refused, its Commit is refused too. Close is
// held between the gate and the forced rollback, where the pager is still
// in the writer state; a statement of the layer above that failed there has
// its savepoint rollback refused, and a parent Commit that got through
// would publish the half of the statement that was written.
func TestClose_CommitRefusedOnceGateClosed(t *testing.T) {
	for _, end := range []string{"Commit", "Rollback"} {
		t.Run(end, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "a.db")
			db, err := testOpen(t, path, Options{DisableAutoCheckpoint: true})
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			createNS(t, db, "t")
			putN(t, db, "t", 10, 100)

			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.GetNamespace("t")
			require.NoError(t, err)
			// The statement: a savepoint, and the first half of its writes.
			sp, err := tx.Savepoint()
			require.NoError(t, err)
			require.NoError(t, tx.Delete(ns, closeRescueKey(1)))

			drained, release := holdCloseAtDrain(t)
			closed := make(chan error, 1)
			go func() { closed <- db.Close() }()
			select {
			case <-drained:
			case <-time.After(10 * time.Second):
				t.Fatal("Close did not reach the forced rollback")
			}
			require.Equal(t, pagerWriter, pagerState(db.pager.state.Load()))

			// The statement fails and cannot be undone...
			require.ErrorIs(t, tx.Put(ns, closeRescueKey(1), []byte("x")), ErrClosed)
			require.ErrorIs(t, tx.RollbackToSavepoint(sp), ErrClosed)
			// ...so the transaction cannot be committed either.
			if end == "Commit" {
				require.ErrorIs(t, tx.Commit(), ErrClosed)
			} else {
				require.NoError(t, tx.Rollback())
			}
			require.ErrorIs(t, tx.Commit(), ErrTxClosed)

			release()
			require.NoError(t, <-closed)

			db, err = testOpen(t, path, Options{})
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			require.Equal(t, 10, countKeys(t, db, "t"), "the refused transaction's delete was committed")
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// The call in flight leaves and its goroutine goes straight on to Commit:
// the gate was closed while the call ran, so the Commit is refused, whoever
// comes first to writerOpMu.
func TestClose_CommitRightAfterCallInFlight(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.db")
	db, err := testOpen(t, path, Options{DisableAutoCheckpoint: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	createNS(t, db, "t")
	putN(t, db, "t", closeRescueRows, closeRescueValSize)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("t")
	require.NoError(t, err)

	// Close is held after the gate so that the Commit below is the first
	// to writerOpMu.
	_, release := holdCloseAtDrain(t)
	var (
		fired  bool
		closed chan error
	)
	setRebuildPageFaultHook(func(*page) error {
		if !fired {
			fired = true
			closed = closeFromCall(t, db)
		}
		return nil
	})
	t.Cleanup(func() { setRebuildPageFaultHook(nil) })
	for i := closeRescueRows + 1; i <= closeRescueRows+200 && !fired; i++ {
		require.NoError(t, tx.Put(ns, closeRescueKey(i), bytes.Repeat([]byte{0xEE}, 1500)))
	}
	require.True(t, fired, "the hook did not fire")
	require.ErrorIs(t, tx.Commit(), ErrClosed)
	release()
	require.NoError(t, <-closed)

	db, err = testOpen(t, path, Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.Equal(t, closeRescueRows, countKeys(t, db, "t"), "a Commit after the gate closed was published")
	require.NoError(t, db.IntegrityCheck())
}

// Close waits for a Commit in flight to its end, the unwind of a panic
// included. The panic strikes after the commit is published, with the pager
// still in the writer state and writerOpMu released by the unwinding: a
// forced rollback there would rewind a commit other connections have seen.
func TestClose_WaitsForCommitThatPanicsAfterPublish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.db")
	db, err := testOpen(t, path, Options{DisableAutoCheckpoint: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	createNS(t, db, "t")
	putN(t, db, "t", 10, 100)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("t")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, closeRescueKey(100), []byte("committed")))

	var closed chan error
	testCommitPostPublishHook = func() {
		testCommitPostPublishHook = nil
		closed = closeFromCall(t, db)
		panic("after publish")
	}
	t.Cleanup(func() { testCommitPostPublishHook = nil })
	func() {
		defer func() { require.Equal(t, "after publish", recover()) }()
		_ = tx.Commit()
	}()
	require.NoError(t, <-closed)

	db, err = testOpen(t, path, Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.Equal(t, 11, countKeys(t, db, "t"), "the published commit was rewound")
	require.NoError(t, db.IntegrityCheck())
}

// A failed spill ends the transaction's hold on the pager before Close
// comes. The Commit after Close still reports the close: the gate refuses
// it, whatever state the pager was left in.
func TestClose_CommitAfterFailedSpillAndClose(t *testing.T) {
	db, err := testOpen(t, filepath.Join(t.TempDir(), "a.db"), Options{})
	require.NoError(t, err)
	createNS(t, db, "t")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("t")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, closeRescueKey(1), []byte("x")))
	// What pagerStress does when its write to the log fails.
	db.pager.pagerError()

	require.NoError(t, db.Close())
	require.ErrorIs(t, tx.Commit(), ErrClosed)
}
