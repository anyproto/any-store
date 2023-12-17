package iterator

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/testdb"
	"github.com/anyproto/any-store/query"
)

func TestIndexIterator_Next(t *testing.T) {
	fx := testdb.NewFixture(t)
	defer fx.Finish(t)
	ns := key.NewNS("/ns")
	keys := make([]key.Key, 10)
	for i := range keys {
		keys[i] = ns.GetKey().AppendAny(i).AppendAny(fmt.Sprint(i)).AppendAny(i)
	}
	fillNs(t, fx.DB, keys...)
	t.Run("all", func(t *testing.T) {
		require.NoError(t, fx.View(func(txn *badger.Txn) error {
			it := &IndexIterator{
				IndexNs: ns,
				Txn:     txn,
			}
			var i int
			for it.Next() {
				expected := []any{
					float64(i),
					fmt.Sprint(i),
					float64(i),
				}
				require.Equal(t, expected, toAnyVals(t, it.Values()))
				i++
			}
			assert.Equal(t, 10, i)
			return it.Close()
		}))
	})
	t.Run("filters", func(t *testing.T) {
		require.NoError(t, fx.View(func(txn *badger.Txn) error {
			it := &IndexIterator{
				IndexNs: ns,
				Txn:     txn,
				Filters: []query.Filter{
					&query.Comp{EqValue: encoding.AppendAnyValue(nil, 5), CompOp: query.CompOpEq},
				},
			}
			var i int
			for it.Next() {
				expected := []any{
					float64(5),
					"5",
					float64(5),
				}
				require.Equal(t, expected, toAnyVals(t, it.Values()))
				i++
			}
			assert.Equal(t, 1, i)
			return it.Close()
		}))
	})
}

func fillNs(t *testing.T, db *badger.DB, keys ...key.Key) {
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		for _, k := range keys {
			if err := txn.Set(k, nil); err != nil {
				return err
			}
		}
		return nil
	}))
}

func toAnyVals(t *testing.T, values [][]byte) []any {
	var anyVals = make([]any, len(values))
	var err error
	for i, v := range values {
		anyVals[i], _, err = encoding.DecodeToAny(v)
		require.NoError(t, err)
	}
	return anyVals
}
