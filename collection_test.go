package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollection_Drop(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Drop(ctx))
	// TODO: add indexed and data
	_, err = fx.OpenCollection(ctx, "test")
	assert.ErrorIs(t, err, ErrCollectionNotFound)
}

func TestCollection_Rename(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	const newName = "newName"
	require.NoError(t, err)
	require.NoError(t, coll.Rename(ctx, newName))
	assert.Equal(t, coll.Name(), newName)

	collections, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{newName}, collections)
}

func TestCollection_Insert(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, `{"id":1, "doc":"a"}`, `{"id":2, "doc":"b"}`))
		count, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})
	t.Run("tx success", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)

		require.NoError(t, coll.Insert(tx.Context(), `{"id":1, "doc":"a"}`, `{"id":2, "doc":"b"}`))

		// expect count=2 in tx
		count, err := coll.Count(tx.Context())
		require.NoError(t, err)
		assert.Equal(t, 2, count)

		// expect count=0 outside tx
		count, err = coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		require.NoError(t, tx.Commit())

		// expect count=2 after commit
		count, err = coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})
	t.Run("err doc exists", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		require.NoError(t, coll.Insert(ctx, `{"id":1, "doc":"a"}`, `{"id":2, "doc":"b"}`))

		err = coll.Insert(ctx, `{"id":3, "doc":"c"}`, `{"id":2, "doc":"b"}`)
		assert.ErrorIs(t, err, ErrDocExists)

		count, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})
}
func TestCollection_FindId(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		doc, err := coll.FindId(ctx, 1)
		assert.Nil(t, doc)
		assert.ErrorIs(t, err, ErrDocNotFound)
	})
	t.Run("found", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		const docJson = `{"id":1,"doc":2}`
		require.NoError(t, coll.Insert(ctx, docJson))
		doc, err := coll.FindId(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, docJson, doc.Value().String())
	})
}
