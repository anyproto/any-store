package btree

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A read begin takes the page-1 counters of its snapshot from the counters
// memo, and reads page 1 only when it pins a snapshot the memo does not
// hold. These tests hold the counters a reader reports to the content of
// its snapshot, in the three modes, across what moves the committed state
// or the frame numbers: own commits, a reader pinned before one, WAL
// restarts that end on the same frame number, checkpoints, a rollback and
// a commit that fails. The package's TestMain turns on the check of every
// memo-served begin against page 1 (checkSnapshotCounters), so each begin
// here is also held to a page-1 read within its own bounds.

// counterModes runs fn on a database of each mode.
func counterModes(t *testing.T, fn func(t *testing.T, db *DB)) {
	for _, mode := range []string{"multiprocess", "inprocess", "inmemory"} {
		t.Run(mode, func(t *testing.T) {
			opts := DefaultOptions()
			path := filepath.Join(t.TempDir(), "db")
			switch mode {
			case "inprocess":
				opts.InProcess = true
			case "inmemory":
				opts.InMemory = true
				path = ""
			}
			db, err := testOpen(t, path, opts)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			fn(t, db)
		})
	}
}

// counterDDL commits a schema change: namespace name is created.
func counterDDL(t *testing.T, db *DB, name string) {
	t.Helper()
	w, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = w.CreateNamespace(name)
	require.NoError(t, err)
	w.MarkDataChanged()
	w.MarkSchemaChanged()
	require.NoError(t, w.Commit())
}

// counterCheck holds an open read transaction to its snapshot: the cookie
// it reports is want, page 1 read within its bounds says the same, and the
// namespaces it resolves are exactly has.
func counterCheck(t *testing.T, rtx *ReadTx, want uint32, has, hasNot []string) {
	t.Helper()
	require.Equal(t, want, rtx.SnapshotSchemaCookie())
	require.Equal(t, want, rtx.DiskSchemaCookie(), "a read transaction has one pair")
	fcc, sc, err := rtx.pager.headerCountersAt(rtx.walHdr.mxFrame, rtx.cache.minFrame, false)
	require.NoError(t, err)
	gotFCC, gotSC, err := rtx.SnapshotHeaderCounters()
	require.NoError(t, err)
	require.Equal(t, [2]uint32{fcc, sc}, [2]uint32{gotFCC, gotSC}, "page 1 at the snapshot's bounds")
	for _, name := range has {
		_, err := rtx.GetNamespace(name)
		require.NoError(t, err, "namespace %q is in the snapshot", name)
	}
	for _, name := range hasNot {
		_, err := rtx.GetNamespace(name)
		require.Error(t, err, "namespace %q is not in the snapshot", name)
	}
}

// counterNow is counterCheck on a transaction of its own; it returns the
// frame the snapshot ends at.
func counterNow(t *testing.T, db *DB, want uint32, has, hasNot []string) uint32 {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	counterCheck(t, rtx, want, has, hasNot)
	return rtx.WalMaxFrame()
}

func counterCookie(t *testing.T, db *DB) uint32 {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	return rtx.SnapshotSchemaCookie()
}

// A reader that begins after a commit of this process reports the commit's
// cookie; one pinned before it keeps its own, while the memo moves on to
// the newer snapshot and back.
func TestReadBeginCounters_OwnCommitAndPinnedReader(t *testing.T) {
	counterModes(t, func(t *testing.T, db *DB) {
		counterDDL(t, db, "a")
		c := counterCookie(t, db)
		counterNow(t, db, c, []string{"a"}, []string{"b"})

		pinned, err := db.BeginRead()
		require.NoError(t, err)
		counterDDL(t, db, "b")
		counterCheck(t, pinned, c, []string{"a"}, []string{"b"})
		counterNow(t, db, c+1, []string{"a", "b"}, nil)
		counterNow(t, db, c+1, []string{"a", "b"}, nil)

		// A second commit under the pinned reader, and a reader pinned at
		// the one between.
		middle, err := db.BeginRead()
		require.NoError(t, err)
		counterDDL(t, db, "c")
		counterNow(t, db, c+2, []string{"a", "b", "c"}, nil)
		counterCheck(t, middle, c+1, []string{"a", "b"}, []string{"c"})
		counterCheck(t, pinned, c, []string{"a"}, []string{"b", "c"})
		require.NoError(t, middle.Rollback())
		require.NoError(t, pinned.Rollback())

		// A commit that changes no schema leaves the cookie.
		w, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := w.GetNamespace("a")
		require.NoError(t, err)
		require.NoError(t, w.Put(ns, []byte("k"), []byte("v")))
		w.MarkDataChanged()
		require.NoError(t, w.Commit())
		counterNow(t, db, c+2, []string{"a", "b", "c"}, nil)
	})
}

// A WAL restart, then a commit that ends on the frame number and the page
// count the commit before the restart ended on: the reader after it reports
// the new cookie, not the one remembered for that position.
func TestReadBeginCounters_RestartEndsOnTheSamePosition(t *testing.T) {
	counterModes(t, func(t *testing.T, db *DB) {
		counterDDL(t, db, "seed")
		require.NoError(t, db.Checkpoint(CheckpointRestart))
		c := counterCookie(t, db)
		// bump is a schema change that allocates nothing: one row rewritten,
		// the cookie moved.
		bump := func(i int) {
			w, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := w.GetNamespace("seed")
			require.NoError(t, err)
			require.NoError(t, w.Put(ns, []byte("k"), fmt.Appendf(nil, "v%d", i)))
			w.MarkDataChanged()
			w.MarkSchemaChanged()
			require.NoError(t, w.Commit())
		}
		// position checks a reader of generation i and returns where its
		// snapshot ends.
		position := func(i int, want uint32) [2]uint32 {
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer func() { require.NoError(t, rtx.Rollback()) }()
			counterCheck(t, rtx, want, []string{"seed"}, nil)
			ns, err := rtx.GetNamespace("seed")
			require.NoError(t, err)
			v, err := rtx.Get(ns, []byte("k"))
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("v%d", i), string(v))
			return [2]uint32{rtx.walHdr.mxFrame, rtx.walHdr.nPage}
		}
		var ends [][2]uint32
		for i := 0; i < 4; i++ {
			bump(i)
			c++
			ends = append(ends, position(i, c))
			require.NotZero(t, ends[i][0], "setup: the commit is in the WAL")
			require.NoError(t, db.Checkpoint(CheckpointRestart))
			position(i, c)
		}
		for i := 2; i < len(ends); i++ {
			require.Equal(t, ends[i-1], ends[i], "setup: two generations end on the same frame number and page count: %v", ends)
		}
	})
}

// A checkpoint of any kind changes where page 1 is read from and, for
// RESTART and TRUNCATE, the frame numbers; never the counters.
func TestReadBeginCounters_Checkpoints(t *testing.T) {
	modes := map[string]CheckpointMode{
		"passive":  CheckpointPassive,
		"full":     CheckpointFull,
		"restart":  CheckpointRestart,
		"truncate": CheckpointTruncate,
	}
	for name, ckpt := range modes {
		t.Run(name, func(t *testing.T) {
			counterModes(t, func(t *testing.T, db *DB) {
				counterDDL(t, db, "a")
				c := counterCookie(t, db)
				counterNow(t, db, c, []string{"a"}, []string{"b"})
				require.NoError(t, db.Checkpoint(ckpt))
				counterNow(t, db, c, []string{"a"}, []string{"b"})

				// A reader pinned across the next commit and a checkpoint
				// that does not wait for it.
				pinned, err := db.BeginRead()
				require.NoError(t, err)
				counterDDL(t, db, "b")
				require.NoError(t, db.Checkpoint(CheckpointPassive))
				counterCheck(t, pinned, c, []string{"a"}, []string{"b"})
				counterNow(t, db, c+1, []string{"a", "b"}, nil)
				require.NoError(t, pinned.Rollback())

				require.NoError(t, db.Checkpoint(ckpt))
				counterNow(t, db, c+1, []string{"a", "b"}, []string{"c"})
				counterDDL(t, db, "c")
				counterNow(t, db, c+2, []string{"a", "b", "c"}, nil)
			})
		})
	}
}

// A rolled-back schema change leaves the counters, and the commit after it
// takes the cookie the rolled-back one would have.
func TestReadBeginCounters_Rollback(t *testing.T) {
	counterModes(t, func(t *testing.T, db *DB) {
		counterDDL(t, db, "a")
		c := counterCookie(t, db)

		w, err := db.BeginWrite()
		require.NoError(t, err)
		_, err = w.CreateNamespace("gone")
		require.NoError(t, err)
		w.MarkDataChanged()
		w.MarkSchemaChanged()
		// A reader that begins under the open write transaction.
		counterNow(t, db, c, []string{"a"}, []string{"gone"})
		require.NoError(t, w.Rollback())
		counterNow(t, db, c, []string{"a"}, []string{"gone"})

		counterDDL(t, db, "b")
		counterNow(t, db, c+1, []string{"a", "b"}, []string{"gone"})
	})
}

// A commit that fails once its frames are written, before it is published,
// is in no snapshot: the readers after it report the counters from before,
// and the next commit — which ends on the frames the failed one wrote —
// its own.
func TestReadBeginCounters_FailedCommit(t *testing.T) {
	counterModes(t, func(t *testing.T, db *DB) {
		counterDDL(t, db, "a")
		c := counterCookie(t, db)
		counterNow(t, db, c, []string{"a"}, []string{"failed"})

		w, err := db.BeginWrite()
		require.NoError(t, err)
		_, err = w.CreateNamespace("failed")
		require.NoError(t, err)
		w.MarkDataChanged()
		w.MarkSchemaChanged()
		failPublication(t)
		require.ErrorIs(t, w.Commit(), errInjectedPublish)
		testCommitPublishErrHook = nil
		counterNow(t, db, c, []string{"a"}, []string{"failed"})
		counterNow(t, db, c, []string{"a"}, []string{"failed"})

		counterDDL(t, db, "b")
		counterNow(t, db, c+1, []string{"a", "b"}, []string{"failed"})
	})
}

// A begin at the snapshot the memo holds reads no page 1: with every WAL
// frame read failing it still begins, where a begin at a snapshot the memo
// does not hold fails on its page-1 read.
func TestReadBeginCounters_RememberedSnapshotReadsNothing(t *testing.T) {
	counterModes(t, func(t *testing.T, db *DB) {
		counterDDL(t, db, "a")
		c := counterCookie(t, db)

		errFrame := errors.New("injected frame read failure")
		reads := 0
		setWalReadFrameFaultHook(func(uint32) error { reads++; return errFrame })
		defer setWalReadFrameFaultHook(nil)

		rtx, err := db.BeginRead()
		require.NoError(t, err)
		require.Equal(t, c, rtx.SnapshotSchemaCookie())
		require.NoError(t, rtx.Rollback())
		require.Zero(t, reads, "the begin read a WAL frame")

		// Another snapshot remembered in its place: page 1 is read, from the
		// frame the commit wrote it to.
		db.snapCounters.put(0, &WalIndexHdr{aSalt: [2]uint32{1, 1}}, 0, 0)
		_, err = db.BeginRead()
		require.ErrorIs(t, err, errFrame)
		require.Equal(t, 1, reads)

		setWalReadFrameFaultHook(nil)
		counterNow(t, db, c, []string{"a"}, nil)
	})
}

// The test-build check: counters remembered under a snapshot's key that are
// not its page 1's fail the begin.
func TestReadBeginCounters_CheckCatchesAWrongMemo(t *testing.T) {
	counterModes(t, func(t *testing.T, db *DB) {
		counterDDL(t, db, "a")
		c := counterCookie(t, db)

		m := &db.snapCounters
		good := m.counters.Load()
		poison := func(v uint64) {
			m.mu.Lock()
			m.seq.Add(1)
			m.counters.Store(v)
			m.seq.Add(1)
			m.mu.Unlock()
		}
		poison(good + 1)
		_, err := db.BeginRead()
		require.ErrorIs(t, err, errPage1Invariant)
		require.Zero(t, len(db.readerSem), "the failed begin left its reader slot held")

		poison(good)
		counterNow(t, db, c, []string{"a"}, nil)
	})
}

// Readers begin while a writer commits schema changes and restarts the
// WAL: each reader's cookie names the newest namespace its snapshot holds,
// and (the test-build check) is the one on its snapshot's page 1. The writer
// lets every reader in before its first commit and waits for further reads
// after each one, so the readers run through every phase whatever the
// scheduler does.
func TestReadBeginCounters_ConcurrentWithCommitsAndRestarts(t *testing.T) {
	counterModes(t, func(t *testing.T, db *DB) {
		counterDDL(t, db, "seed")
		base := counterCookie(t, db)
		name := func(cookie uint32) string { return fmt.Sprintf("c%d", cookie-base) }

		const commits = 60
		stop := make(chan struct{})
		errs := make(chan error, 1<<16)
		var wg sync.WaitGroup
		var gate sync.RWMutex
		const readers = 4
		var reads atomic.Int64
		// waitReads waits until n reads have completed. It gives up when a
		// reader has failed (the errors fail the test) or after a deadline.
		waitReads := func(n int64) bool {
			deadline := time.Now().Add(30 * time.Second)
			for reads.Load() < n {
				if len(errs) > 0 || time.Now().After(deadline) {
					return false
				}
				time.Sleep(50 * time.Microsecond)
			}
			return true
		}
		var started sync.WaitGroup
		started.Add(readers)
		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				first := true
				defer func() {
					if first {
						started.Done()
					}
				}()
				for {
					select {
					case <-stop:
						return
					default:
					}
					gate.RLock()
					rtx, err := db.BeginRead()
					if err != nil {
						gate.RUnlock()
						errs <- err
						return
					}
					c := rtx.SnapshotSchemaCookie()
					if c != base {
						if _, err := rtx.GetNamespace(name(c)); err != nil {
							errs <- fmt.Errorf("cookie %d (base %d): its namespace: %w", c, base, err)
						}
					}
					if _, err := rtx.GetNamespace(name(c + 1)); err == nil {
						errs <- fmt.Errorf("cookie %d (base %d): the snapshot holds the next commit", c, base)
					}
					err = rtx.Rollback()
					gate.RUnlock()
					if err != nil {
						errs <- err
						return
					}
					reads.Add(1)
					if first {
						first = false
						started.Done()
					}
				}
			}()
		}
		started.Wait()
		starved := false
		for i := 1; i <= commits && !starved; i++ {
			counterDDL(t, db, name(base+uint32(i)))
			// At least one read per reader after this commit before the next.
			starved = !waitReads(reads.Load() + readers)
			switch i % 6 {
			case 2:
				_ = db.Checkpoint(CheckpointPassive)
			case 5:
				// A restart waits for the readers to leave: none is let in.
				gate.Lock()
				err := db.Checkpoint(CheckpointRestart)
				gate.Unlock()
				require.NoError(t, err)
			}
		}
		close(stop)
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		require.False(t, starved, "the readers stopped completing reads (%d reads)", reads.Load())
		counterNow(t, db, base+commits, []string{name(base + commits)}, nil)
	})
}
