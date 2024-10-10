package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestIterator_Doc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	docs := []*anyenc.Value{anyenc.MustParseJson(`{"id":1,"a":"a"}`), anyenc.MustParseJson(`{"id":2,"a":"b"}`), anyenc.MustParseJson(`{"id":3,"a":"c"}`)}
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
			assert.Equal(t, docs[i].String(), d.Value().String())
			i++
		}
		require.NoError(t, iter.Err())
		require.NoError(t, iter.Close())
	})
}
