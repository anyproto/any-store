package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func TestCollection_InsertOne(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish()

		coll, err := fx.Collection("test")
		require.NoError(t, err)
		docId, err := coll.InsertOne(map[string]string{
			"id":  "myId",
			"key": "value",
		})
		require.NoError(t, err)
		assert.Equal(t, "myId", docId)
	})
	t.Run("duplicate error", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish()

		coll, err := fx.Collection("test")
		require.NoError(t, err)
		docId, err := coll.InsertOne(map[string]string{
			"id":  "myId",
			"key": "value",
		})
		require.NoError(t, err)
		assert.Equal(t, "myId", docId)

		_, err = coll.InsertOne(map[string]string{
			"id":  "myId",
			"key": "value",
		})
		assert.ErrorIs(t, err, ErrDuplicatedId)
	})
}

func TestCollection_InsertMany(t *testing.T) {

	t.Run("small", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish()

		coll, err := fx.Collection("test")
		require.NoError(t, err)

		var docs = make([]any, 100)
		for i := range docs {
			docs[i] = map[string]int{"num": i}
		}
		res, err := coll.InsertMany(docs...)
		require.NoError(t, err)
		assert.Equal(t, len(docs), res.AffectedRows)
		assertCount(t, coll, len(docs))
	})
	t.Run("big", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish()

		coll, err := fx.Collection("test")
		require.NoError(t, err)

		var docs = make([]any, 100000)
		for i := range docs {
			docs[i] = newSmallIntObject(i)
		}
		res, err := coll.InsertMany(docs...)
		require.NoError(t, err)
		assert.Equal(t, len(docs), res.AffectedRows)
		assertCount(t, coll, len(docs))
	})
	t.Run("duplicate", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish()

		coll, err := fx.Collection("test")
		require.NoError(t, err)

		var docs = make([]any, 10)
		for i := range docs {
			docs[i] = map[string]int{"id": i}
		}
		docs[9].(map[string]int)["id"] = 2
		_, err = coll.InsertMany(docs...)
		assert.ErrorIs(t, err, ErrDuplicatedId)
		assertCount(t, coll, 0)
	})
}

func TestCollection_UpsertOne(t *testing.T) {
	t.Run("insert", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish()

		coll, err := fx.Collection("test")
		require.NoError(t, err)
		docId, err := coll.UpsertOne(map[string]string{
			"id":  "myId",
			"key": "value",
		})
		require.NoError(t, err)
		assert.Equal(t, "myId", docId)
		assertCount(t, coll, 1)
	})

	t.Run("update", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish()

		coll, err := fx.Collection("test")
		require.NoError(t, err)
		docId, err := coll.UpsertOne(map[string]string{
			"id":  "myId",
			"key": "value",
		})
		require.NoError(t, err)
		assert.Equal(t, "myId", docId)
		docId, err = coll.UpsertOne(map[string]string{
			"id":  "myId",
			"key": "value2",
		})
		require.NoError(t, err)
		assert.Equal(t, "myId", docId)
		assertCount(t, coll, 1)

		// TODO: check findId
	})
	t.Run("big", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish()

		coll, err := fx.Collection("test")
		require.NoError(t, err)

		var docs = make([]any, 100000)
		for i := range docs {
			docs[i] = newSmallIntObject(i)
		}
		res, err := coll.UpsertMany(docs...)
		require.NoError(t, err)
		assert.Equal(t, len(docs), res.AffectedRows)
		assertCount(t, coll, len(docs))
	})

}

func TestCollection_UpsertMany(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		defer fx.finish()

		coll, err := fx.Collection("test")
		require.NoError(t, err)

		var docs = make([]any, 10)
		for i := range docs {
			docs[i] = map[string]any{"id": i, "val": fmt.Sprint(i)}
		}

		res, err := coll.UpsertMany(docs...)
		require.NoError(t, err)
		assert.Equal(t, len(docs), res.AffectedRows)
		assertCount(t, coll, len(docs))

		for i := range docs {
			docs[i] = map[string]any{"id": i + 5, "val": fmt.Sprint(i)}
		}

		res, err = coll.UpsertMany(docs...)
		require.NoError(t, err)
		assert.Equal(t, len(docs), res.AffectedRows)
		assertCount(t, coll, len(docs)+5)
	})
}

func assertCount(t *testing.T, coll *Collection, expected int) {
	count, err := coll.Count(nil)
	require.NoError(t, err)
	assert.Equal(t, expected, count)
}

func newSmallIntObject(i int) *fastjson.Value {
	a := arenaPool.Get()
	defer arenaPool.Put(a)
	val := a.NewObject()
	obj, _ := val.Object()
	obj.Set("i", a.NewNumberInt(i))
	return val
}
