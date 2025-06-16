package anystore

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/objectid"
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

func TestCollection_Insert(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))
		assertCollCount(t, coll, 2)
	})
	t.Run("tx success", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)

		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))

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

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":3, "doc":"c"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`))
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
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(docJson)))
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

		err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":"notFound", "d":2}`))
		assert.ErrorIs(t, err, ErrDocNotFound)
	})

	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"333","key":"value"}`))
		require.NoError(t, err)

		newDoc := `{"id":"333","key":"value2"}`

		err = coll.UpdateOne(ctx, anyenc.MustParseJson(newDoc))
		require.NoError(t, err)

		doc, err := coll.FindId(ctx, "333")
		require.NoError(t, err)
		assert.Equal(t, newDoc, doc.Value().String())
	})

	t.Run("doc without id", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"a":"b"}`))
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

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":333,"key":"value"}`))
		require.NoError(t, err)
		id := 333

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

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "key":"value"}`))
		require.NoError(t, err)

		res, err := coll.UpdateId(ctx, 1, query.MustParseModifier(`{"$set":{"key":"value"}}`))
		require.NoError(t, err)
		assert.Equal(t, 0, res.Modified)
	})
}

func TestCollection_UpsertOne(t *testing.T) {
	t.Run("insert", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		t.Run("update", func(t *testing.T) {
			err = coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":"upd","val":1}`))
			require.NoError(t, err)
			newDoc := `{"id":"upd","val":2}`
			err = coll.UpsertOne(ctx, anyenc.MustParseJson(newDoc))
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
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "a":2}`)))
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
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"toDel", "a":2}`)))
		require.NoError(t, coll.DeleteId(ctx, "toDel"))
		assertCollCount(t, coll, 0)
	})
}

func TestCollection_CreateIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))
	t.Run("err exists", func(t *testing.T) {
		err = coll.CreateIndex(ctx, IndexInfo{Fields: []string{"name"}}, IndexInfo{Fields: []string{"name"}})
		assert.ErrorIs(t, err, ErrIndexExists)
	})
	t.Run("success", func(t *testing.T) {
		require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"doc"}}))
		idxs := coll.GetIndexes()
		require.Len(t, idxs, 1)
		assert.Equal(t, "doc", idxs[0].Info().Name)
		count, err := idxs[0].Len(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})
}

func TestCollection_EnsureIndex(t *testing.T) {
	t.Run("multiple", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))
		err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"name"}}, IndexInfo{Fields: []string{"doc"}}, IndexInfo{Fields: []string{"name"}})
		assert.NoError(t, err, ErrIndexExists)
	})
	t.Run("single index", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		idx := IndexInfo{
			Fields: []string{"a"},
		}
		require.NoError(t, coll.EnsureIndex(ctx, idx))
		require.NoError(t, coll.EnsureIndex(ctx, idx))
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
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		a.Reset()
		doc := a.NewObject()
		doc.Set("id", a.NewString(objectid.NewObjectID().Hex()))
		require.NoError(b, coll.Insert(ctx, doc))
	}
}

func BenchmarkCollection_UpdateId(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)

	require.NoError(b, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "v":0}`)))
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

	require.NoError(b, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "v":0}`)))

	b.Run("no parser", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, err = coll.FindId(ctx, 1)
			require.NoError(b, err)
		}
	})
	b.Run("with parser", func(b *testing.B) {
		p := &anyenc.Parser{}
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
		require.NoError(b, coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "a":%d, "b":%d}`, i, i, rand.Int()))))
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

func BenchmarkCollection_InQuery(b *testing.B) {
	fx := newFixture(b)
	var builder strings.Builder
	builder.Grow(4000)
	builder.WriteString(`{"id":{"$in":[`)
	l := 1001
	for i := 1; i <= l; i++ {
		builder.WriteString(strconv.Itoa(i))
		if i < l {
			builder.WriteString(",")
		}
	}
	builder.WriteString(",400000")
	builder.WriteString("]}}")

	query := query.MustParseCondition(builder.String())
	coll, _ := fx.CreateCollection(ctx, "test_foo")
	coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}})
	vals := make([]*anyenc.Value, 1000000)
	for i := range 1000000 {
		// try to make random
		vals[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "a":%d}`, i+980, i+981))
	}
	b.Log(coll.Find(query).Explain(ctx))
	coll.Insert(ctx, vals...)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		coll.Find(query).Count(ctx)
	}
}
