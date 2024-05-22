package anystore

import (
	"context"
	"fmt"
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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Rename(ctx, newName))
	assert.Equal(t, coll.Name(), newName)

	collections, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{newName}, collections)
}

func TestCollection_InsertOne(t *testing.T) {
	t.Run("with id", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(context.Background(), "test")
		require.NoError(t, err)

		id, err := coll.InsertOne(ctx, `{"id":42, "d":"a"}`)
		require.NoError(t, err)
		assert.Equal(t, float64(42), id)
	})
	t.Run("gen id", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(context.Background(), "test")
		require.NoError(t, err)

		id, err := coll.InsertOne(ctx, `{"d":"a"}`)
		require.NoError(t, err)
		idString, ok := id.(string)
		require.True(t, ok)
		assert.NotEmpty(t, idString)
	})
	t.Run("err exists", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(context.Background(), "test")
		require.NoError(t, err)

		_, err = coll.InsertOne(ctx, `{"id":"a"}`)
		require.NoError(t, err)
		_, err = coll.InsertOne(ctx, `{"id":"a"}`)
		assert.ErrorIs(t, err, ErrDocExists)
	})
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

func TestCollection_UpdateOne(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		err = coll.UpdateOne(ctx, `{"id":"notFound", "d":2}`)
		assert.ErrorIs(t, err, ErrDocNotFound)
	})

	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		id, err := coll.InsertOne(ctx, `{"key":"value"}`)
		require.NoError(t, err)

		newDoc := fmt.Sprintf(`{"id":"%s","key":"value2"}`, id)

		err = coll.UpdateOne(ctx, newDoc)
		require.NoError(t, err)

		doc, err := coll.FindId(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, newDoc, doc.Value().String())
	})

	t.Run("doc without id", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		err = coll.UpdateOne(ctx, `{"a":"b"}`)
		assert.ErrorIs(t, err, ErrDocWithoutId)
	})
}

func TestCollection_UpsertOne(t *testing.T) {
	t.Run("insert", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		t.Run("gen id", func(t *testing.T) {
			id, err := coll.UpsertOne(ctx, `{"a":"b"}`)
			require.NoError(t, err)
			assert.NotEmpty(t, id)
		})
		t.Run("with id", func(t *testing.T) {
			id, err := coll.UpsertOne(ctx, `{"id":999, "a":"b"}`)
			require.NoError(t, err)
			assert.Equal(t, float64(999), id)
		})
		t.Run("update", func(t *testing.T) {
			_, err = coll.UpsertOne(ctx, `{"id":"upd","val":1}`)
			require.NoError(t, err)
			newDoc := `{"id":"upd","val":2}`
			_, err = coll.UpsertOne(ctx, newDoc)
			require.NoError(t, err)
			doc, err := coll.FindId(ctx, "upd")
			require.NoError(t, err)
			assert.Equal(t, newDoc, doc.Value().String())
		})
	})
}

func TestCollection_DeleteOne(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	t.Run("not found", func(t *testing.T) {
		err = coll.DeleteOne(ctx, "notFound")
		assert.ErrorIs(t, err, ErrDocNotFound)
	})
	t.Run("success", func(t *testing.T) {
		require.NoError(t, coll.Insert(ctx, `{"id":"toDel", "a":2}`))
		require.NoError(t, coll.DeleteOne(ctx, "toDel"))
		count, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

func TestCollection_EnsureIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, `{"id":1, "doc":"a"}`, `{"id":2, "doc":"b"}`))
	t.Run("err exists", func(t *testing.T) {
		err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"name"}}, IndexInfo{Fields: []string{"name"}})
		assert.ErrorIs(t, err, ErrIndexExists)
	})
	t.Run("success", func(t *testing.T) {
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"doc"}}))
		idxs := coll.GetIndexes()
		require.Len(t, idxs, 1)
		assert.Equal(t, "doc", idxs[0].Info().Name)
		count, err := idxs[0].Len(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})
}

func TestCollection_DropIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.DropIndex(ctx, "a"))
	assert.Len(t, coll.GetIndexes(), 0)
	assert.ErrorIs(t, coll.DropIndex(ctx, "a"), ErrIndexNotFound)
}
