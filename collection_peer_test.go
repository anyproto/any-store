//go:build (linux || darwin) && (amd64 || arm64)

package anystore

// The sketches of an open collection beside another process that commits to
// the same file. The peer is a real process (re-exec, see changePeer).

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// sketchBucketsOf is the stored row of an index on a that one process
// leaves after it inserted openSketchDB's 50 documents, the peer's one
// (a=7) and own more with a=3: what the row of a file two processes wrote
// has to be, count by count.
func sketchBucketsOf(t *testing.T, own int) []uint64 {
	t.Helper()
	ctx := context.Background()
	d, err := Open(ctx, filepath.Join(t.TempDir(), "twin.db"), nil)
	require.NoError(t, err)
	defer func() { _ = d.Close() }()
	coll, err := d.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := 0; i < 50; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))))
	}
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1000,"a":7}`)))
	for i := 0; i < own; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":3}`, 100+i))))
	}
	return storedSketch(t, d, coll).Buckets
}

// A write of this process goes on top of the sketch row another process
// committed, whatever this process did between that commit and the write. A
// reader that plans first loads the row into the published sketch only; the
// writer still has to load it into the live one, or its commit stores the
// counts from before the other process's commit over that row, for good.
//
// 50 documents, one more from the peer, then the steps of each case: the
// stored row, the published sketch and the sketch a fresh open loads all
// count every document, each under its value.
func TestSketch_WriterLoadsThePeersRowAfterAReader(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":3}`, id))
	}
	find := func(t *testing.T, ctx context.Context, coll Collection) {
		t.Helper()
		_, err := coll.Find(`{"a":1}`).Count(ctx)
		require.NoError(t, err)
	}
	stats := func(t *testing.T, coll Collection, want uint64) {
		t.Helper()
		docs, sketch := statsDocCounts(t, ctx, coll)
		require.Equal(t, int(want), docs)
		require.Equal(t, want, sketch)
	}
	cases := []struct {
		name string
		// late: the handle is opened after the peer's commit.
		late bool
		// written: this process committed through the handle before the
		// peer's commit, so the handle's live sketches are a writer's.
		written bool
		// steps returns the number of documents it committed.
		steps func(t *testing.T, d DB, coll Collection) int
	}{
		{name: "write", steps: func(t *testing.T, d DB, coll Collection) int {
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "find, write", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "stats, write", steps: func(t *testing.T, d DB, coll Collection) int {
			stats(t, coll, 51)
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "two readers, write", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			require.Equal(t, uint64(51), sketchDocCount(t, coll))
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "handle opened after the commit, find, write", late: true, steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "a writer's handle, find, write", written: true, steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			require.NoError(t, coll.Insert(ctx, doc(101)))
			return 2
		}},
		{name: "a writer's handle, stats, write", written: true, steps: func(t *testing.T, d DB, coll Collection) int {
			stats(t, coll, 52)
			require.NoError(t, coll.Insert(ctx, doc(101)))
			return 2
		}},
		{name: "write, stats, write", steps: func(t *testing.T, d DB, coll Collection) int {
			require.NoError(t, coll.Insert(ctx, doc(100)))
			stats(t, coll, 52)
			require.Equal(t, uint64(52), sketchDocCount(t, coll))
			require.NoError(t, coll.Insert(ctx, doc(101)))
			return 2
		}},
		{name: "find, a write tx that plans between its writes", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			wtx, err := d.WriteTx(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wtx.Rollback() })
			require.NoError(t, coll.Insert(wtx.Context(), doc(100)))
			find(t, wtx.Context(), coll)
			require.NoError(t, coll.Insert(wtx.Context(), doc(101)))
			require.NoError(t, wtx.Commit())
			return 2
		}},
		{name: "find, a write tx rolled back, write", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			wtx, err := d.WriteTx(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wtx.Rollback() })
			require.NoError(t, coll.Insert(wtx.Context(), doc(100), doc(101)))
			require.NoError(t, wtx.Rollback())
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "find, a statement rolled back in the write tx", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			wtx, err := d.WriteTx(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wtx.Rollback() })
			// The first use of the collection in the transaction is inside
			// the statement's savepoint, and the statement fails on its
			// last document.
			require.ErrorIs(t, coll.Insert(wtx.Context(), doc(100), doc(101), doc(1)), ErrDocExists)
			require.NoError(t, coll.Insert(wtx.Context(), doc(100)))
			require.NoError(t, wtx.Commit())
			return 1
		}},
		{name: "find, write, own writes after", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			for i := 0; i < 5; i++ {
				require.NoError(t, coll.Insert(ctx, doc(100+i)))
				find(t, ctx, coll)
			}
			return 5
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, path := openSketchDB(t)
			var coll Collection
			var err error
			if !tc.late {
				coll, err = d.OpenCollection(ctx, "c")
				require.NoError(t, err)
				require.Equal(t, uint64(50), sketchDocCount(t, coll))
			}
			if tc.written {
				require.NoError(t, coll.Insert(ctx, doc(100)))
				require.Equal(t, uint64(51), sketchDocCount(t, coll))
			}
			runChangePeer(t, path)
			if tc.late {
				coll, err = d.OpenCollection(ctx, "c")
				require.NoError(t, err)
			}

			own := tc.steps(t, d, coll)
			want := uint64(51 + own)

			n, err := coll.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, int(want), n, "setup: the documents")
			stored := storedSketch(t, d, coll)
			require.Equal(t, want, stored.DocCount, "the stored row holds the peer's counts and this process's")
			require.Equal(t, want, sketchDocCount(t, coll), "the published sketch")
			stats(t, coll, want)
			// The counts by value, not the total alone: the peer's document
			// under its value and this process's under theirs, as one
			// process that wrote them all counts them.
			require.True(t, slices.Equal(sketchBucketsOf(t, own), stored.Buckets), "the stored row's counts by value")
			pub := coll.(*collection).loadIndexes()[0].loadPubSketch()
			require.True(t, slices.Equal(stored.Buckets, pub.Snapshot(nil)[:len(stored.Buckets)]), "the published sketch has the stored row's counts by value")

			require.NoError(t, d.Close())
			d2, err := Open(ctx, path, nil)
			require.NoError(t, err)
			defer func() { _ = d2.Close() }()
			coll2, err := d2.OpenCollection(ctx, "c")
			require.NoError(t, err)
			require.Equal(t, want, sketchDocCount(t, coll2), "the sketch a fresh open loads")
			pub2 := coll2.(*collection).loadIndexes()[0].loadPubSketch()
			require.True(t, slices.Equal(stored.Buckets, pub2.Snapshot(nil)[:len(stored.Buckets)]), "a fresh open loads the stored row's counts by value")
		})
	}
}

// The change epoch can move while a write transaction is open: a reader of
// this process that took its snapshot at an older commit of the peer and is
// only now counting it. Nothing committed under the writer, and it loaded
// the peer's row before its first write to the collection: it does not load
// the row again over its own deltas.
func TestSketch_EpochMovesUnderAWriteTx(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	d, path := openSketchDB(t)
	bt := d.(*db).btreeDB
	coll, err := d.OpenCollection(ctx, "c")
	require.NoError(t, err)
	require.Equal(t, uint64(50), sketchDocCount(t, coll))
	epoch := func() uint64 {
		rtx, err := bt.BeginRead()
		require.NoError(t, err)
		e, _ := rtx.ChangeEpoch()
		require.NoError(t, rtx.Rollback())
		return e
	}

	runChangePeer(t, path) // the peer's first commit
	// A reader takes its snapshot there and stops before it counts it.
	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	btree.SetTestChangeSwapHook(func() {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	})
	delayed := make(chan error, 1)
	go func() {
		rtx, err := bt.BeginRead()
		if err == nil {
			err = rtx.Rollback()
		}
		delayed <- err
	}()
	// However the test ends, the reader is let go and has ended before the
	// database closes (the cleanups run last in, first out).
	var once sync.Once
	var delayedErr error
	letGo := func() error {
		once.Do(func() {
			close(release)
			delayedErr = <-delayed
			btree.SetTestChangeSwapHook(nil)
		})
		return delayedErr
	}
	t.Cleanup(func() { _ = letGo() })
	select {
	case <-entered:
	case err := <-delayed:
		delayed <- err
		t.Fatalf("setup: the reader ended before it counted the peer's commit: %v", err)
	}
	runChangePeer(t, path) // the peer's second commit

	wtx, err := d.WriteTx(ctx) // counts the second commit
	require.NoError(t, err)
	t.Cleanup(func() { _ = wtx.Rollback() })
	require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":100,"a":3}`)))
	e1 := epoch()
	require.NoError(t, letGo())
	require.Equal(t, e1+1, epoch(), "setup: the delayed reader moved the epoch under the write transaction")
	// The next uses of the collection in the transaction: a plan and a write.
	_, err = coll.Find(`{"a":1}`).Count(wtx.Context())
	require.NoError(t, err)
	require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":101,"a":3}`)))
	docs, sketch := statsDocCounts(t, wtx.Context(), coll)
	require.Equal(t, 54, docs)
	require.Equal(t, uint64(54), sketch, "the transaction's Stats count its own documents")
	require.NoError(t, wtx.Commit())

	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 54, n, "setup: the documents")
	require.Equal(t, uint64(54), storedSketch(t, d, coll).DocCount, "both of the transaction's documents are in the stored row")
	require.Equal(t, uint64(54), sketchDocCount(t, coll))
}

// A read of the stored row that fails is not a load, after another
// process's commit as at an open (TestSketch_RowReadFailsAtOpen). A writer
// that cannot load the row fails the operation before it writes: its deltas
// would go on counts that miss the peer's, and be stored over the peer's
// row. A reader plans with what is published and loads on its next use.
func TestSketch_RowReadFailsAfterAPeersCommit(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()

	t.Run("writer", func(t *testing.T) {
		d, path := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":90,"a":3}`))) // a writer's handle
		runChangePeer(t, path)
		require.Equal(t, uint64(52), sketchDocCount(t, coll)) // a reader loaded the row

		failed, injected := failSketchReadOnce(t)
		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"a":3}`))
		require.Equal(t, 1, *failed, "setup: the writer read the row")
		require.ErrorIs(t, err, injected, "the write fails before it puts its deltas on unloaded counts")
		require.Equal(t, uint64(52), storedSketch(t, d, coll).DocCount)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"a":3}`)), "the next write loads the row")
		require.Equal(t, uint64(53), storedSketch(t, d, coll).DocCount)
		require.Equal(t, uint64(53), sketchDocCount(t, coll))
	})

	t.Run("reader", func(t *testing.T) {
		d, path := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		require.Equal(t, uint64(50), sketchDocCount(t, coll))
		runChangePeer(t, path)

		failed, _ := failSketchReadOnce(t)
		require.Equal(t, uint64(50), sketchDocCount(t, coll), "the plan was made with the published sketch as it was")
		require.Equal(t, 1, *failed, "setup: the reader read the row")
		require.Equal(t, uint64(51), sketchDocCount(t, coll), "the next reader loads the row")
	})
}

// A write transaction that rolled back after another process's commit: the
// counts it put on the live sketch are not stored by the next one, and Stats
// in between reports the stored row, which agrees with the documents.
func TestSketch_RollbackAfterAPeersCommit(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	d, path := openSketchDB(t)
	coll, err := d.OpenCollection(ctx, "c")
	require.NoError(t, err)
	runChangePeer(t, path)
	require.Equal(t, uint64(51), sketchDocCount(t, coll))

	wtx, err := d.WriteTx(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wtx.Rollback() })
	require.NoError(t, coll.Insert(wtx.Context(),
		anyenc.MustParseJson(`{"id":100,"a":3}`), anyenc.MustParseJson(`{"id":101,"a":3}`)))
	docs, sketch := statsDocCounts(t, wtx.Context(), coll)
	require.Equal(t, 53, docs)
	require.Equal(t, uint64(53), sketch, "inside the transaction, with its own writes")
	require.NoError(t, wtx.Rollback())

	docs, sketch = statsDocCounts(t, ctx, coll)
	require.Equal(t, 51, docs)
	require.Equal(t, uint64(51), sketch, "no count of the rolled-back transaction")

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"a":3}`)))
	require.Equal(t, uint64(52), storedSketch(t, d, coll).DocCount)
	require.Equal(t, uint64(52), sketchDocCount(t, coll))
}
