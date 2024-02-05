package iterator

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/query"
)

func TestIndexIterator_Next(t *testing.T) {
	fx := newFixture(t)
	defer fx.Finish(t)
	fillTestIndex(t, fx, 5)

	t.Run("no bound direct", func(t *testing.T) {
		var expected = []float64{0, 1, 2, 3, 4}
		var actual []float64
		require.NoError(t, fx.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			idx := NewIndexIterator(fx.QCtx, fx.indexNS, nil, false)
			for idx.Next() {
				actual = append(actual, toAny(t, idx.CurrentId()).(float64))
			}
			return idx.Close()
		}))
		assert.Equal(t, expected, actual)
	})
	t.Run("no bound reverse", func(t *testing.T) {
		var expected = []float64{4, 3, 2, 1, 0}
		var actual []float64
		require.NoError(t, fx.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			idx := NewIndexIterator(fx.QCtx, key.NewNS("testIndex"), nil, true)
			for idx.Next() {
				actual = append(actual, toAny(t, idx.CurrentId()).(float64))
			}
			return idx.Close()
		}))
		assert.Equal(t, expected, actual)
	})
	t.Run("one bound direct; lte gte", func(t *testing.T) {
		var expected = []float64{1, 2, 3}
		var actual []float64
		require.NoError(t, fx.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			idx := NewIndexIterator(fx.QCtx, key.NewNS("testIndex"), query.Bounds{{
				Start:        encoding.AppendAnyValue(nil, 1),
				StartInclude: true,
				End:          encoding.AppendAnyValue(nil, 3),
				EndInclude:   true,
			}}, false)
			for idx.Next() {
				actual = append(actual, toAny(t, idx.CurrentId()).(float64))
			}
			return idx.Close()
		}))
		assert.Equal(t, expected, actual)
	})
	t.Run("one bound reverse; lte gte", func(t *testing.T) {
		var expected = []float64{3, 2, 1}
		var actual []float64
		require.NoError(t, fx.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			idx := NewIndexIterator(fx.QCtx, key.NewNS("testIndex"), query.Bounds{{
				Start:        encoding.AppendAnyValue(nil, 1),
				StartInclude: true,
				End:          encoding.AppendAnyValue(nil, 3),
				EndInclude:   true,
			}}, true)
			for idx.Next() {
				actual = append(actual, toAny(t, idx.CurrentId()).(float64))
			}
			return idx.Close()
		}))
		assert.Equal(t, expected, actual)
	})
	t.Run("one bound direct; lt gt", func(t *testing.T) {
		var expected = []float64{2, 3}
		var actual []float64
		require.NoError(t, fx.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			idx := NewIndexIterator(fx.QCtx, key.NewNS("testIndex"), query.Bounds{{
				Start: encoding.AppendAnyValue(nil, 1),
				End:   encoding.AppendAnyValue(nil, 4),
			}}, false)
			for idx.Next() {
				actual = append(actual, toAny(t, idx.CurrentId()).(float64))
			}
			return idx.Close()
		}))
		assert.Equal(t, expected, actual)
	})
	t.Run("one bound reverse; lt gt", func(t *testing.T) {
		var expected = []float64{3, 2}
		var actual []float64
		require.NoError(t, fx.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			idx := NewIndexIterator(fx.QCtx, key.NewNS("testIndex"), query.Bounds{{
				Start: encoding.AppendAnyValue(nil, 1),
				End:   encoding.AppendAnyValue(nil, 4),
			}}, true)
			for idx.Next() {
				actual = append(actual, toAny(t, idx.CurrentId()).(float64))
			}
			return idx.Close()
		}))
		assert.Equal(t, expected, actual)
	})
	t.Run("one bound reverse; lt", func(t *testing.T) {
		var expected = []float64{3, 2, 1, 0}
		var actual []float64
		require.NoError(t, fx.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			idx := NewIndexIterator(fx.QCtx, key.NewNS("testIndex"), query.Bounds{{
				End: encoding.AppendAnyValue(nil, 4),
			}}, true)
			for idx.Next() {
				actual = append(actual, toAny(t, idx.CurrentId()).(float64))
			}
			return idx.Close()
		}))
		assert.Equal(t, expected, actual)
	})
	t.Run("in bounds direct", func(t *testing.T) {
		var expected = []float64{1, 3, 4}
		var actual []float64
		require.NoError(t, fx.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			idx := NewIndexIterator(fx.QCtx, key.NewNS("testIndex"), query.Bounds{
				{
					Start:        encoding.AppendAnyValue(nil, 1),
					StartInclude: true,
					End:          encoding.AppendAnyValue(nil, 1),
					EndInclude:   true,
				},
				{
					Start:        encoding.AppendAnyValue(nil, 3),
					StartInclude: true,
					End:          encoding.AppendAnyValue(nil, 3),
					EndInclude:   true,
				},
				{
					Start:        encoding.AppendAnyValue(nil, 4),
					StartInclude: true,
					End:          encoding.AppendAnyValue(nil, 4),
					EndInclude:   true,
				},
			}, false)
			for idx.Next() {
				actual = append(actual, toAny(t, idx.CurrentId()).(float64))
			}
			return idx.Close()
		}))
		assert.Equal(t, expected, actual)
	})
	t.Run("in bounds reverse", func(t *testing.T) {
		var expected = []float64{4, 3, 1}
		var actual []float64
		require.NoError(t, fx.View(func(txn *badger.Txn) error {
			fx.QCtx.Txn = txn
			idx := NewIndexIterator(fx.QCtx, key.NewNS("testIndex"), query.Bounds{
				{
					Start:        encoding.AppendAnyValue(nil, 1),
					StartInclude: true,
					End:          encoding.AppendAnyValue(nil, 1),
					EndInclude:   true,
				},
				{
					Start:        encoding.AppendAnyValue(nil, 3),
					StartInclude: true,
					End:          encoding.AppendAnyValue(nil, 3),
					EndInclude:   true,
				},
				{
					Start:        encoding.AppendAnyValue(nil, 4),
					StartInclude: true,
					End:          encoding.AppendAnyValue(nil, 4),
					EndInclude:   true,
				},
			}, true)
			for idx.Next() {
				actual = append(actual, toAny(t, idx.CurrentId()).(float64))
			}
			return idx.Close()
		}))
		assert.Equal(t, expected, actual)
	})
}
