package btree

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// rescueAsClose runs what DB.Close does to a registered write transaction it
// finds in flight, rollbackForClose under writerOpMu, and stops there. Close
// goes on to release the writer's locks and close the pager without waiting
// for the abandoned writer, and has no seam between the two steps; the test
// keeps the handle and its WAL open instead, which is the window in which
// the abandoned writer can still fetch pages.
func rescueAsClose(db *DB) {
	db.pager.writerOpMu.Lock()
	if pagerState(db.pager.state.Load()) == pagerWriter {
		db.pager.rollbackForClose()
	}
	db.pager.writerOpMu.Unlock()
}

// fillWriterCacheDirty dirties pages 2, 3, ... in the open write transaction
// until the writer's cache holds nothing it can evict: every page in it is
// dirty or pinned, so the next cache miss calls the stress callback. Page 1
// is left alone, as it is never a spill victim. Fails if a page spilled on
// the way.
func fillWriterCacheDirty(t *testing.T, p *pager) {
	t.Helper()
	base := p.wal.nFrame.Load()
	pc := p.writerCache
	for pgno := uint32(2); pc.nPage < pc.maxPages || pc.nRecyclable > 0; pgno++ {
		require.LessOrEqual(t, pgno, p.dbSize.Load(), "database too small to fill the writer cache")
		pg, err := p.getWritablePage(pgno)
		require.NoError(t, err)
		p.releasePage(pg)
	}
	require.NotNil(t, pc.findSpillVictim(), "no spill victim in the full cache")
	require.Equal(t, base, p.wal.nFrame.Load(), "a page spilled while the cache filled")
}

// closeRescueRows and closeRescueValSize make a tree of a few hundred pages:
// far more than the writer cache below holds.
const (
	closeRescueRows    = 2000
	closeRescueValSize = 500
	closeRescueCache   = 32
)

func closeRescueKey(i int) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(i))
}

// A write transaction that DB.Close rescued (rollbackForClose) is over: the
// WAL write lock is released and the log is rewound to the transaction's
// base frame. Its goroutine can still be inside an operation, and a cache
// miss there must not spill a dirty page into the log: the frame would land
// where the next writer appends.
func TestCloseRescue_AbandonedWriterDoesNotSpill(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := testOpen(t, path, Options{CacheSize: closeRescueCache, DisableAutoCheckpoint: true})
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

	var spills atomic.Int32
	testAfterSpillHook = func() { spills.Add(1) }
	t.Cleanup(func() { testAfterSpillHook = nil })

	rescueAsClose(db)
	require.Equal(t, base, p.wal.nFrame.Load())
	walInfo, err := os.Stat(path + "-wal")
	require.NoError(t, err)

	// The abandoned writer reads on. Reads of a write transaction check no
	// writer state, and every miss now meets a cache full of dirty pages.
	for i := 1; i <= closeRescueRows; i++ {
		_, err := tx.Get(ns, closeRescueKey(i))
		require.NoError(t, err)
	}

	require.Zero(t, spills.Load(), "a page was spilled after Close ended the transaction")
	require.Equal(t, base, p.wal.nFrame.Load())
	after, err := os.Stat(path + "-wal")
	require.NoError(t, err)
	require.Equal(t, walInfo.Size(), after.Size())

	// The writer's next write ends with Close's error.
	require.ErrorIs(t, tx.Put(ns, closeRescueKey(1), []byte("x")), ErrClosed)

	require.NoError(t, db.Close())
	db, err = testOpen(t, path, Options{})
	require.NoError(t, err)
	require.Equal(t, closeRescueRows, countKeys(t, db, "t"))
	require.NoError(t, db.IntegrityCheck())
}

// A spill that has passed pagerStress's guard when Close rescues the
// transaction writes nothing. The hook runs the rescue at that point, after
// the guard and before the log write: writeFrames tests under the WAL mutex
// what the rescue sets there before it rewinds the log, so the page stays
// dirty and no frame lands past the base. (An InMemory database never
// spills: its writer cache is not purgeable.)
func TestCloseRescue_SpillPastGuardWritesNothing(t *testing.T) {
	for _, mode := range []struct {
		name string
		opts Options
	}{
		{"shared wal-index", Options{}},
		{"InProcess", Options{InProcess: true}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.opts
			opts.CacheSize = closeRescueCache
			opts.DisableAutoCheckpoint = true
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
			walSize := func() int64 {
				info, err := os.Stat(path + "-wal")
				require.NoError(t, err)
				return info.Size()
			}
			sizeAtBase := walSize()

			var spills, rescues atomic.Int32
			testAfterSpillHook = func() { spills.Add(1) }
			testStressGuardPassedHook = func() {
				if rescues.Add(1) == 1 {
					rescueAsClose(db)
				}
			}
			t.Cleanup(func() {
				testAfterSpillHook = nil
				testStressGuardPassedHook = nil
			})

			// The first miss on the full cache calls pagerStress, and the
			// rescue runs once the spill has passed the guard. Every later
			// miss is refused at the guard.
			for i := 1; i <= closeRescueRows; i++ {
				_, err := tx.Get(ns, closeRescueKey(i))
				require.NoError(t, err)
			}
			require.Equal(t, int32(1), rescues.Load(), "no spill passed the guard")
			require.Zero(t, spills.Load(), "a page was spilled after Close ended the transaction")
			require.Equal(t, base, p.wal.nFrame.Load())
			require.Equal(t, base, p.wal.index.maxFrame.Load())
			require.Equal(t, sizeAtBase, walSize())
			require.ErrorIs(t, tx.Put(ns, closeRescueKey(1), []byte("x")), ErrClosed)

			require.NoError(t, db.Close())
			db, err = testOpen(t, path, Options{InProcess: opts.InProcess})
			require.NoError(t, err)
			require.Equal(t, closeRescueRows, countKeys(t, db, "t"))
			require.NoError(t, db.IntegrityCheck())
		})
	}
}
