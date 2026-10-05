package anystore

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestDb_WriteTx(t *testing.T) {
	t.Run("err other instance", func(t *testing.T) {
		fx := newFixture(t)
		fx2 := newFixture(t)

		tx, err := fx2.WriteTx(ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = fx.CreateCollection(tx.Context(), "test")
		assert.ErrorIs(t, err, ErrTxOtherInstance)
	})
	t.Run("err tx been used", func(t *testing.T) {
		fx := newFixture(t)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		_, err = fx.CreateCollection(tx.Context(), "test")
		assert.ErrorIs(t, err, ErrTxIsUsed)
	})
	t.Run("rollback + savepoint rollback", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)

		require.NoError(t, coll.Insert(tx.Context(),
			anyenc.MustParseJson(`{"id":1,"a":1}`),
			anyenc.MustParseJson(`{"id":2,"a":2}`),
			anyenc.MustParseJson(`{"id":3,"a":3}`),
		))
		assertCollCountInTx(tx.Context(), t, coll, 3)

		// this insert will fail because id:1 already exists, and should rollback to savepoint
		require.Error(t, coll.Insert(tx.Context(),
			anyenc.MustParseJson(`{"id":4,"a":4}`),
			anyenc.MustParseJson(`{"id":5,"a":5}`),
			anyenc.MustParseJson(`{"id":1,"a":6}`),
		))
		assertCollCountInTx(tx.Context(), t, coll, 3)

		require.NoError(t, tx.Rollback())
		assertCollCount(t, coll, 0)
	})
	// A savepoint ends with its transaction: finishing it afterwards fails
	// and leaves the state of the next writer alone.
	t.Run("savepoint outlives its tx", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))

		for i, finish := range []func(WriteTx) error{WriteTx.Commit, WriteTx.Rollback} {
			tx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			sp, err := fx.WriteTx(tx.Context())
			require.NoError(t, err)
			assert.False(t, sp.Done())
			require.NoError(t, tx.Commit())
			assert.True(t, sp.Done())

			// The next writer has postings buffered when the stale handle is used.
			next, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			require.NoError(t, coll.Insert(next.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"body":"word%d"}`, i, i))))
			assert.ErrorIs(t, finish(sp), ErrTxIsUsed)
			require.NoError(t, next.Commit())

			n, err := coll.Find(fmt.Sprintf(`{"$text":{"$search":"word%d"}}`, i)).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
		}
	})
	// A savepoint ends with the savepoint that encloses it: finishing it
	// afterwards fails and undoes nothing the transaction has done since.
	t.Run("savepoint outlives its enclosing savepoint", func(t *testing.T) {
		ends := map[string]func(WriteTx) error{"commit": WriteTx.Commit, "rollback": WriteTx.Rollback}
		for outerName, endOuter := range ends {
			for innerName, endInner := range ends {
				t.Run(outerName+" then "+innerName, func(t *testing.T) {
					fx := newFixture(t)
					dropped, err := fx.CreateCollection(ctx, "dropped")
					require.NoError(t, err)

					tx, err := fx.WriteTx(ctx)
					require.NoError(t, err)
					outer, err := fx.WriteTx(tx.Context())
					require.NoError(t, err)
					// A schema change leaves the undo log and the publication
					// list at different lengths for the inner savepoint to mark.
					_, err = fx.CreateCollection(tx.Context(), "inOuter")
					require.NoError(t, err)
					inner, err := fx.WriteTx(tx.Context())
					require.NoError(t, err)
					assert.False(t, inner.Done())
					require.NoError(t, endOuter(outer))
					assert.True(t, inner.Done())

					// A savepoint opened now takes the btree savepoint id the
					// inner one had.
					fresh, err := fx.WriteTx(tx.Context())
					require.NoError(t, err)
					created, err := fx.CreateCollection(tx.Context(), "created")
					require.NoError(t, err)
					require.NoError(t, created.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1}`)))
					require.NoError(t, dropped.Drop(tx.Context()))

					assert.ErrorIs(t, endInner(inner), ErrTxIsUsed)
					assert.False(t, fresh.Done())
					require.NoError(t, fresh.Commit())
					require.NoError(t, tx.Commit())

					assertCollCount(t, created, 1)
					_, err = dropped.Count(ctx)
					assert.ErrorIs(t, err, ErrCollectionClosed)
					_, err = fx.OpenCollection(ctx, "dropped")
					assert.ErrorIs(t, err, ErrCollectionNotFound)
					_, err = fx.OpenCollection(ctx, "inOuter")
					if outerName == "commit" {
						require.NoError(t, err)
					} else {
						assert.ErrorIs(t, err, ErrCollectionNotFound)
					}
					require.NoError(t, fx.IntegrityCheck(ctx))
				})
			}
		}
	})
	t.Run("rollback - commit race", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		var wg sync.WaitGroup

		var insertFunc = func() {
			tx, txErr := coll.WriteTx(ctx)
			require.NoError(t, txErr)
			defer func() {
				assert.NoError(t, tx.Rollback())
			}()
			assert.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":"%s", "data": %d}`, anyenc.NewObjectID().Hex(), rand.Int()))))
			assert.NoError(t, tx.Commit())
		}

		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 100 {
					insertFunc()
				}
			}()
		}
		wg.Wait()
	})
}
