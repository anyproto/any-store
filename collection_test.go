package anystore

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/query"
)

func assertCollCount(t testing.TB, c Collection, expected int) bool {
	return assertCollCountCtx(ctx, t, c, expected)
}

func assertCollCountCtx(ctx context.Context, t testing.TB, c Collection, expected int) bool {
	count, err := c.Count(ctx)
	require.NoError(t, err)
	return assert.Equal(t, expected, count)
}

func TestCollection_Drop(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Drop(ctx))
	_, err = fx.OpenCollection(ctx, "test")
	assert.ErrorIs(t, err, ErrCollectionNotFound)

	stats, err := fx.Stats(ctx)
	require.NoError(t, err)
	assert.Empty(t, stats.IndexesCount)
	assert.Empty(t, stats.CollectionsCount)
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
		assertCollCount(t, coll, 2)
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
		assertCollCount(t, coll, 0)

		require.NoError(t, tx.Commit())

		// expect count=2 after commit
		assertCollCount(t, coll, 2)
	})
	t.Run("err doc exists", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		require.NoError(t, coll.Insert(ctx, `{"id":1, "doc":"a"}`, `{"id":2, "doc":"b"}`))

		err = coll.Insert(ctx, `{"id":3, "doc":"c"}`, `{"id":2, "doc":"b"}`)
		assert.ErrorIs(t, err, ErrDocExists)

		assertCollCount(t, coll, 2)
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

func TestCollection_UpdateId(t *testing.T) {
	mod := query.MustParseModifier(`{"$inc":{"a":1}}`)
	t.Run("not found", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		res, err := coll.UpdateId(ctx, "notFound", mod)
		assert.ErrorIs(t, err, ErrDocNotFound)
		assert.Empty(t, res)
	})

	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		id, err := coll.InsertOne(ctx, `{"key":"value"}`)
		require.NoError(t, err)

		res, err := coll.UpdateId(ctx, id, mod)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Modified)

		doc, err := coll.FindId(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, float64(1), doc.Value().GetFloat64("a"))
	})
	t.Run("not modified", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		id, err := coll.InsertOne(ctx, `{"key":"value"}`)
		require.NoError(t, err)

		res, err := coll.UpdateId(ctx, id, query.MustParseModifier(`{"$set":{"key":"value"}}`))
		require.NoError(t, err)
		assert.Equal(t, 0, res.Modified)
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

func TestCollection_UpsertId(t *testing.T) {
	mod := query.MustParseModifier(`{"$inc":{"a":1}}`)
	t.Run("insert", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		res, err := coll.UpsertId(ctx, 1, mod)
		require.NoError(t, err)
		assert.Equal(t, 0, res.Matched)
		assert.Equal(t, 1, res.Modified)

		doc, err := coll.FindId(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, float64(1), doc.Value().GetFloat64("a"))
	})
	t.Run("update", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, `{"id":1, "a":2}`))
		res, err := coll.UpsertId(ctx, 1, mod)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Matched)
		assert.Equal(t, 1, res.Modified)

		doc, err := coll.FindId(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, float64(3), doc.Value().GetFloat64("a"))
	})
}

func TestCollection_DeleteOne(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	t.Run("not found", func(t *testing.T) {
		err = coll.DeleteId(ctx, "notFound")
		assert.ErrorIs(t, err, ErrDocNotFound)
	})
	t.Run("success", func(t *testing.T) {
		require.NoError(t, coll.Insert(ctx, `{"id":"toDel", "a":2}`))
		require.NoError(t, coll.DeleteId(ctx, "toDel"))
		assertCollCount(t, coll, 0)
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

func BenchmarkCollection_Insert(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		require.NoError(b, coll.Insert(ctx, `{"some":"document"}`))
	}
}

func BenchmarkCollection_UpdateId(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)

	require.NoError(b, coll.Insert(ctx, `{"id":1, "v":0}`))
	mod := query.MustParseModifier(`{"$inc":{"v":1}}`)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, err = coll.UpdateId(ctx, 1, mod)
		require.NoError(b, err)
	}
}

func BenchmarkCollection_FindId(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)

	require.NoError(b, coll.Insert(ctx, `{"id":1, "v":0}`))

	b.Run("no parser", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, err = coll.FindId(ctx, 1)
			require.NoError(b, err)
		}
	})
	b.Run("with parser", func(b *testing.B) {
		p := &fastjson.Parser{}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, err = coll.FindIdWithParser(ctx, p, 1)
			require.NoError(b, err)
		}
	})
}

func BenchmarkCollection_Find(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)
	tx, err := coll.WriteTx(ctx)
	require.NoError(b, err)
	for i := range 1000 {
		require.NoError(b, coll.Insert(tx.Context(), fmt.Sprintf(`{"id":%d, "a":%d, "b":%d}`, i, i, rand.Int())))
	}
	require.NoError(b, tx.Commit())

	b.Run("count", func(b *testing.B) {
		for range b.N {
			b.ReportAllocs()
			_, _ = coll.Find(nil).Count(ctx)
		}
	})
	b.Run("count by filter", func(b *testing.B) {
		var f = query.MustParseCondition(`{"a":{"$gt":900}}`)
		for range b.N {
			b.ReportAllocs()
			_, _ = coll.Find(f).Count(ctx)
		}
	})

}
