package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIterator_Doc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	docs := []any{`{"id":1,"a":"a"}`, `{"id":2,"a":"b"}`, `{"id":3,"a":"c"}`}
	require.NoError(t, coll.Insert(ctx, docs...))
	t.Run("error", func(t *testing.T) {
		iter, err := coll.Find("not valid").Iter(ctx)
		assert.Error(t, err)
		assert.Nil(t, iter)
	})
	t.Run("ok", func(t *testing.T) {
		iter, err := coll.Find(nil).Sort("id").Iter(ctx)
		require.NoError(t, err)
		var d Doc
		var i int
		for iter.Next() {
			d, err = iter.Doc()
			require.NoError(t, err)
			assert.Equal(t, docs[i], d.Value().String())
			i++
		}
		require.NoError(t, iter.Err())
		require.NoError(t, iter.Close())
	})
}
