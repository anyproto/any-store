package iterator

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/sort"
)

func TestSortIterator_Next(t *testing.T) {
	fx := newFixture(t)
	defer fx.Finish(t)
	fillTestData(t, fx, 5)
	t.Run("desc", func(t *testing.T) {
		var expected = []float64{4, 3, 2, 1, 0}
		var actual []float64
		require.NoError(t, fx.DB.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			sorts, err := sort.ParseSort("-a")
			require.NoError(t, err)
			scIt := NewScanIterator(fx.QCtx, nil, nil, false)
			it := NewSortIterator(fx.QCtx, scIt, sorts)
			for it.Next() {
				id := toAny(t, it.CurrentId())
				require.NoError(t, it.CurrentValue(func(v *fastjson.Value) error {
					av, _ := v.Get("a").Float64()
					assert.Equal(t, id, av)
					return nil
				}))
				actual = append(actual, id.(float64))
			}
			return it.Close()
		}))
		assert.Equal(t, expected, actual)
	})
}
