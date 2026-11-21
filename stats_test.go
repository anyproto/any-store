package anystore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// statsDoc builds a document whose marshaled size comfortably exceeds
// anyenc.CompressMinSize and whose payload is highly compressible.
func statsDoc(id int) *anyenc.Value {
	filler := strings.Repeat("compressible-", 64)
	return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d,"filler":%q}`, id, id%10, filler))
}

func TestCollection_Stats(t *testing.T) {
	const docCount = 200

	t.Run("compression enabled", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "byK", Fields: []string{"k"}}))

		for i := 0; i < docCount; i++ {
			require.NoError(t, coll.Insert(ctx, statsDoc(i)))
		}

		st, err := coll.Stats(ctx)
		require.NoError(t, err)

		assert.Equal(t, "test", st.Name)
		assert.Equal(t, docCount, st.DocCount)
		assert.True(t, st.CompressionEnabled)
		// Highly compressible filler ⇒ stored form is much smaller.
		assert.Less(t, st.StoredDocsBytes, st.UncompressedDocsBytes)
		assert.Greater(t, st.CompressionRatio, 1.0)

		// Physical sizes are non-zero and the total is internally consistent.
		assert.Greater(t, st.DocsSizeBytes, 0)
		assert.Greater(t, st.IndexesSizeBytes, 0)
		assert.Equal(t, st.DocsSizeBytes+st.IndexesSizeBytes, st.TotalSizeBytes)

		require.Len(t, st.Indexes, 1)
		idx := st.Indexes[0]
		assert.Equal(t, "byK", idx.Name)
		assert.Equal(t, []string{"k"}, idx.Fields)
		assert.Equal(t, docCount, idx.EntryCount)
		assert.Greater(t, idx.SizeBytes, 0)
		assert.Greater(t, idx.PayloadBytes, 0)
		assert.Equal(t, uint64(docCount), idx.SketchDocCount)
		assert.Greater(t, idx.SketchSize, 0)
		// 10 distinct values for "k" ⇒ at most 10 non-empty buckets.
		assert.GreaterOrEqual(t, idx.SketchDistribution.NonEmptyBuckets, 1)
		assert.LessOrEqual(t, idx.SketchDistribution.NonEmptyBuckets, 10)
		assert.Greater(t, idx.SketchDistribution.MaxBucket, uint64(0))
	})

	t.Run("compression disabled", func(t *testing.T) {
		fx := newFixture(t, &Config{DisableCompression: true})
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		for i := 0; i < docCount; i++ {
			require.NoError(t, coll.Insert(ctx, statsDoc(i)))
		}

		st, err := coll.Stats(ctx)
		require.NoError(t, err)
		assert.False(t, st.CompressionEnabled)
		// Nothing compressed ⇒ stored equals uncompressed, ratio is exactly 1.
		assert.Equal(t, st.StoredDocsBytes, st.UncompressedDocsBytes)
		assert.Equal(t, 1.0, st.CompressionRatio)
	})

	// Stats runs concurrently with inserts: under -race this covers the
	// reads Stats makes while writers commit. It checks only that no call
	// fails and the counts once the writer is done; that Stats reports the
	// row of its own snapshot is TestCollection_Stats_SketchOfItsTransaction.
	t.Run("concurrent with writes", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "byK", Fields: []string{"k"}}))

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < docCount; i++ {
				require.NoError(t, coll.Insert(ctx, statsDoc(i)))
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_, sErr := coll.Stats(ctx)
				require.NoError(t, sErr)
			}
		}()
		wg.Wait()

		st, err := coll.Stats(ctx)
		require.NoError(t, err)
		assert.Equal(t, docCount, st.DocCount)
		require.Len(t, st.Indexes, 1)
		assert.Equal(t, uint64(docCount), st.Indexes[0].SketchDocCount)
	})

	t.Run("empty collection", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "empty")
		require.NoError(t, err)

		st, err := coll.Stats(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, st.DocCount)
		assert.Equal(t, 0, st.StoredDocsBytes)
		assert.Equal(t, 1.0, st.CompressionRatio)
		assert.Empty(t, st.Indexes)
		assert.Equal(t, st.DocsSizeBytes+st.IndexesSizeBytes, st.TotalSizeBytes)
	})
}

// The sketch figures of Stats are the stored row as the call's transaction
// sees it, so they agree with the documents the same call counts. The
// published sketch, which plans are made with, is shared by transactions at
// different snapshots and holds the counts of a write transaction until the
// next one begins, committed or not; Stats does not report it.
func TestCollection_Stats_SketchOfItsTransaction(t *testing.T) {
	ctx := context.Background()
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":3}`, id))
	}
	check := func(t *testing.T, ctx context.Context, coll Collection, want int, msg string) {
		t.Helper()
		docs, sketch := statsDocCounts(t, ctx, coll)
		require.Equal(t, want, docs, "the documents: %s", msg)
		require.Equal(t, uint64(want), sketch, "the sketch: %s", msg)
	}

	t.Run("after a rollback", func(t *testing.T) {
		d, _ := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, doc(99)))

		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		require.NoError(t, coll.Insert(wtx.Context(), doc(100), doc(101)))
		check(t, wtx.Context(), coll, 53, "inside the transaction, with its own writes")
		check(t, ctx, coll, 51, "beside the open transaction")
		require.NoError(t, wtx.Rollback())
		check(t, ctx, coll, 51, "no count of the rolled-back transaction")

		require.NoError(t, coll.Insert(ctx, doc(100)))
		check(t, ctx, coll, 52, "after the next commit")
		require.Equal(t, uint64(52), storedSketch(t, d, coll).DocCount)
	})

	// The commit fails inside the btree, after the sketches went into it:
	// no rollback of the write transaction ran.
	t.Run("after a commit that failed", func(t *testing.T) {
		d, _ := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)

		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		require.NoError(t, coll.Insert(wtx.Context(), doc(100), doc(101)))
		testHookBeforeBtreeCommit = func(btx *btree.WriteTx) { require.NoError(t, btx.Rollback()) }
		err = wtx.Commit()
		testHookBeforeBtreeCommit = nil
		require.ErrorIs(t, err, btree.ErrTxClosed)
		check(t, ctx, coll, 50, "no count of the transaction whose commit failed")

		require.NoError(t, coll.Insert(ctx, doc(100)))
		check(t, ctx, coll, 51, "after the next commit")
	})

	// The next write transaction's begin rebases the sketches a rollback
	// left; here its read of the row fails, and the transaction writes to
	// another collection.
	t.Run("after a rollback and a rebase that could not read the row", func(t *testing.T) {
		d, _ := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		other, err := d.CreateCollection(ctx, "other")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, doc(99)))

		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		require.NoError(t, coll.Insert(wtx.Context(), doc(100), doc(101)))
		require.NoError(t, wtx.Rollback())

		failed, _ := failSketchReadOnce(t)
		wtx, err = d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		require.Equal(t, 1, *failed, "setup: the begin read the row")
		require.NoError(t, other.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":1}`)))
		check(t, ctx, coll, 51, "while the transaction is open")
		require.NoError(t, wtx.Commit())
		check(t, ctx, coll, 51, "after its commit")
		require.Equal(t, uint64(51), storedSketch(t, d, coll).DocCount, "the commit stored nothing of the rolled-back transaction")

		require.NoError(t, coll.Insert(ctx, doc(100)))
		check(t, ctx, coll, 52, "after the next write to the collection")
		require.Equal(t, uint64(52), storedSketch(t, d, coll).DocCount)
	})

	// In memory every snapshot passes for the newest one. A read
	// transaction older than a commit reports its own snapshot, and what it
	// read does not become what later transactions report.
	t.Run("in memory, an older read transaction after a rollback", func(t *testing.T) {
		d, err := Open(ctx, ":memory:", &Config{InMemory: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = d.Close() })
		coll, err := d.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(ctx, doc(i)))
		}
		old, err := d.ReadTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = old.Commit() })
		for i := 0; i < 10; i++ {
			require.NoError(t, coll.Insert(ctx, doc(100+i)))
		}
		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		require.NoError(t, coll.Insert(wtx.Context(), doc(200)))
		require.NoError(t, wtx.Rollback())

		check(t, old.Context(), coll, 50, "the older transaction, first after the rollback")
		check(t, ctx, coll, 60, "a transaction that began after it")
		require.NoError(t, old.Commit())
		check(t, ctx, coll, 60, "once the older transaction has ended")
	})

	// A rollback, then Stats, then a write transaction that inserts: its
	// Stats count the insert in every index, also in one the rolled-back
	// transaction did not write to.
	t.Run("inside a write transaction, after a rollback that wrote one index of two", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}, IndexInfo{Fields: []string{"b"}}))
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%5, i%7))))
		}
		both := func(ctx context.Context, want int, msg string) {
			t.Helper()
			st, err := coll.Stats(ctx)
			require.NoError(t, err)
			require.Equal(t, want, st.DocCount, msg)
			require.Len(t, st.Indexes, 2)
			for _, is := range st.Indexes {
				require.Equal(t, uint64(want), is.SketchDocCount, "%s: index %s", msg, is.Name)
			}
		}

		wtx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		require.NoError(t, coll.UpdateOne(wtx.Context(), anyenc.MustParseJson(`{"id":1,"a":9,"b":1}`)))
		require.NoError(t, wtx.Rollback())
		both(ctx, 50, "after the rollback")

		wtx, err = fx.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":100,"a":3,"b":3}`)))
		both(wtx.Context(), 51, "inside the transaction that inserted")
		both(ctx, 50, "beside it")
		require.NoError(t, wtx.Commit())
		both(ctx, 51, "after its commit")
	})

	t.Run("a row that cannot be read", func(t *testing.T) {
		d, _ := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		failed, injected := failSketchReadOnce(t)
		_, err = coll.Stats(ctx)
		require.Equal(t, 1, *failed, "setup: Stats read the row")
		require.ErrorIs(t, err, injected, "Stats reports no sketch it did not read")
		check(t, ctx, coll, 50, "the next call")
	})

	t.Run("no row", func(t *testing.T) {
		d, _ := openSketchDB(t)
		dbi := d.(*db)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, dbi.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			return tx.Delete(dbi.systemNS, sketchKey("c", "a"))
		}))
		st, err := coll.Stats(ctx)
		require.NoError(t, err)
		require.Equal(t, 50, st.DocCount)
		require.Zero(t, st.Indexes[0].SketchDocCount, "no statistics are stored")
		require.Positive(t, st.Indexes[0].SketchSize)
	})
}
