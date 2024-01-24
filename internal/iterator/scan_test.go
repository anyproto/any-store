package iterator

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/query"
)

func TestValueIterator_Next(t *testing.T) {
	fx := newFixture(t)
	defer fx.Finish(t)
	fillTestData(t, fx, 5)

	t.Run("full scan", func(t *testing.T) {
		var expected = []float64{0, 1, 2, 3, 4}
		var actual []float64
		require.NoError(t, fx.DB.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			it := NewScanIterator(fx.QCtx, nil, nil, false)
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

	t.Run("bound + filter", func(t *testing.T) {
		var expected = []float64{1, 3, 4}
		var actual []float64
		require.NoError(t, fx.DB.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			cond, err := query.ParseCondition(`{"a":{"$ne":2}}`)
			if err != nil {
				return err
			}
			it := NewScanIterator(fx.QCtx, cond, query.Bounds{
				{
					Start:        fx.QCtx.DataNS.GetKey().AppendAny(1),
					StartInclude: true,
				},
			}, false)
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
