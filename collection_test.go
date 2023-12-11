package anystore

import (
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
	assertCount := func(coll *Collection, res Result, expected int) {
		assert.Equal(t, expected, res.AffectedRows)
		count, err := coll.Count(nil)
		require.NoError(t, err)
		assert.Equal(t, expected, count)
	}

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
		assertCount(coll, res, len(docs))
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
		assertCount(coll, res, len(docs))
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
		assertCount(coll, Result{}, 0)
	})
}

func newSmallIntObject(i int) *fastjson.Value {
	a := arenaPool.Get()
	defer arenaPool.Put(a)
	val := a.NewObject()
	obj, _ := val.Object()
	obj.Set("i", a.NewNumberInt(i))
	return val
}
