package anystore

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/objectid"
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
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)

		require.NoError(t, coll.Insert(tx.Context(),
			anyenc.MustParseJson(`{"id":1,"a":1}`),
			anyenc.MustParseJson(`{"id":2,"a":2}`),
			anyenc.MustParseJson(`{"id":3,"a":3}`),
		))
		assertCollCountCtx(tx.Context(), t, coll, 3)

		// this insert will be failed and should rollback to savepoint
		require.Error(t, coll.Insert(tx.Context(),
			anyenc.MustParseJson(`{"id":4,"a":4}`),
			anyenc.MustParseJson(`{"id":5,"a":5}`),
			anyenc.MustParseJson(`{"id":6,"a":1}`),
		))
		assertCollCountCtx(tx.Context(), t, coll, 3)

		require.NoError(t, tx.Rollback())
		assertCollCount(t, coll, 0)
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
			assert.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":"%s", "data": %d}`, objectid.NewObjectID().Hex(), rand.Int()))))
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
