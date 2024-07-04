package anystore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func mustParseItem(t testing.TB, s any) item {
	it, err := parseItem(&fastjson.Parser{}, &fastjson.Arena{}, s, true)
	require.NoError(t, err)
	return it
}

func assertIdxKeyBuf(t *testing.T, idx *index, keyCase fillKeysCase) {
	var keysStrings = make([]string, len(idx.keysBuf))
	for i, k := range idx.keysBuf {
		keysStrings[i] = k.String()
	}
	require.Equal(t, len(keyCase.expected), len(idx.keysBuf), keyCase.doc, strings.Join(keysStrings, ","))
	for i, k := range keysStrings {
		assert.Equal(t, keyCase.expected[i], k, keyCase.doc)
	}
}

func assertIndexLen(t testing.TB, idx Index, expected int) bool {
	count, err := idx.Len(ctx)
	require.NoError(t, err)
	return assert.Equal(t, expected, count)
}

type fillKeysCaseIndex struct {
	name  string
	info  IndexInfo
	cases []fillKeysCase
}

type fillKeysCase struct {
	doc      string
	expected []string
}

var fillKeysCases = []fillKeysCaseIndex{
	{
		name: "one field",
		info: IndexInfo{Fields: []string{"a"}},
		cases: []fillKeysCase{
			{`{"a":"b"}`, []string{"b"}},
			{`{"a":["b","c"]}`, []string{"b", "c", `["b","c"]`}},
			{`{"a":["a", "a", "b", "c", "b"]}`, []string{"a", "b", "c", `["a","a","b","c","b"]`}},
			{`{}`, []string{"<nil>"}},
			{`{"a":null}`, []string{"<nil>"}},
		},
	},
	{
		name: "one field sparse",
		info: IndexInfo{Fields: []string{"a"}, Sparse: true},
		cases: []fillKeysCase{
			{`{"a":"b"}`, []string{"b"}},
			{`{"a":["b","c"]}`, []string{"b", "c", `["b","c"]`}},
			{`{"a":["a", "a", "b", "c", "b"]}`, []string{"a", "b", "c", `["a","a","b","c","b"]`}},
			{`{}`, []string{}},
			{`{"a":null}`, []string{}},
		},
	},
	{
		name: "reverse",
		info: IndexInfo{Fields: []string{"-a"}},
		cases: []fillKeysCase{
			{`{"a":"b"}`, []string{"b"}},
			{`{"a":["b","c"]}`, []string{"b", "c", `["b","c"]`}},
			{`{"a":["a", "a", "b", "c", "b"]}`, []string{"a", "b", "c", `["a","a","b","c","b"]`}},
		},
	},
	{
		name: "two fields",
		info: IndexInfo{Fields: []string{"a", "b"}},
		cases: []fillKeysCase{
			{`{"a":"1"}`, []string{"1/<nil>"}},
			{`{"a":"1","b":"2"}`, []string{"1/2"}},
			{`{"a":[1,2],"b":"2"}`, []string{"1/2", "2/2", "[1,2]/2"}},
			{`{"a":[1,2,1],"b":[2,1,2]}`, []string{
				"1/2", "1/1", "1/[2,1,2]",
				"2/2", "2/1", "2/[2,1,2]",
				"[1,2,1]/2", "[1,2,1]/1", "[1,2,1]/[2,1,2]"}},
		},
	},
	{
		name: "two fields sparse",
		info: IndexInfo{Fields: []string{"a", "b"}, Sparse: true},
		cases: []fillKeysCase{
			{`{"a":"1"}`, []string{}},
			{`{"b":"2"}`, []string{}},
			{`{"a":[1,2]}`, []string{}},
		},
	},
}

func TestIndex_fillKeysBuf(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	newIdx := func(i IndexInfo) *index {
		i.Name = i.createName()
		idx := &index{info: i, c: coll.(*collection)}
		require.NoError(t, idx.init(ctx))
		return idx
	}
	for _, idxCase := range fillKeysCases[:] {
		t.Run(idxCase.name, func(t *testing.T) {
			idx := newIdx(idxCase.info)
			for _, keyCase := range idxCase.cases {
				idx.fillKeysBuf(mustParseItem(t, keyCase.doc))
				assertIdxKeyBuf(t, idx, keyCase)
			}
		})
	}
}

func TestIndex_Insert(t *testing.T) {
	fx := newFixture(t)
	t.Run("uniq", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_uniq")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

		require.NoError(t, coll.Insert(ctx, `{"a":1}`, `{"a":2}`, `{"a":3}`))
		assert.ErrorIs(t, coll.Insert(ctx, `{"a":2}`), ErrUniqueConstraint)
		assertCollCount(t, coll, 3)
		assertIndexLen(t, coll.GetIndexes()[0], 3)
	})
	t.Run("sparse", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_sparse")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

		require.NoError(t, coll.Insert(ctx, `{"a":1}`, `{"a":2}`, `{"b":3}`))
		assertCollCount(t, coll, 3)
		assertIndexLen(t, coll.GetIndexes()[0], 2)
	})
	t.Run("simple", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_simple")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx, `{"a":1}`, `{"a":1}`, `{"b":3}`))
		assertCollCount(t, coll, 3)
		assertIndexLen(t, coll.GetIndexes()[0], 3)
	})
}

func TestIndex_Update(t *testing.T) {
	fx := newFixture(t)
	t.Run("uniq", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_uniq")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

		require.NoError(t, coll.Insert(ctx, `{"id":1, "a":1}`, `{"id":2, "a":2}`, `{"id":3,"a":3}`))
		require.NoError(t, coll.UpdateOne(ctx, `{"id":2,"a":4}`))
		assert.ErrorIs(t, coll.UpdateOne(ctx, `{"id":2, "a":1}`), ErrUniqueConstraint)
		res, err := coll.FindId(ctx, 2)
		require.NoError(t, err)
		assert.Equal(t, `{"id":2,"a":4}`, res.Value().String())
	})
	t.Run("sparse", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_sparse")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, coll.Close())
		}()
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

		require.NoError(t, coll.Insert(ctx, `{"id":1, "a":1}`, `{"id":2, "a":2}`, `{"id":3, "b":3}`))
		assertIndexLen(t, coll.GetIndexes()[0], 2)
		require.NoError(t, coll.UpdateOne(ctx, `{"id":1, "b":1}`))
		assertIndexLen(t, coll.GetIndexes()[0], 1)
		require.NoError(t, coll.UpdateOne(ctx, `{"id":3, "a":1}`))
		assertIndexLen(t, coll.GetIndexes()[0], 2)
	})
}

func TestIndex_Delete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_simple")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, coll.Close())
	}()
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, `{"id":1, "a":1}`, `{"id":2, "a":1}`, `{"id":3, "b":3}`))
	assertIndexLen(t, coll.GetIndexes()[0], 3)

	require.NoError(t, coll.DeleteId(ctx, 1))
	assertIndexLen(t, coll.GetIndexes()[0], 2)

	require.NoError(t, coll.DeleteId(ctx, 2))
	assertIndexLen(t, coll.GetIndexes()[0], 1)

	require.NoError(t, coll.DeleteId(ctx, 3))
	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

func Benchmark_fillKeysBuf(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)
	newIdx := func(i IndexInfo) *index {
		i.Name = i.createName()
		idx := &index{info: i, c: coll.(*collection)}
		require.NoError(b, idx.init(ctx))
		return idx
	}

	b.Run("simple", func(b *testing.B) {
		idx := newIdx(IndexInfo{Fields: []string{"a"}})
		doc := mustParseItem(b, `{"id":1, "a":"2"}`)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.fillKeysBuf(doc)
		}
	})
	b.Run("array", func(b *testing.B) {
		idx := newIdx(IndexInfo{Fields: []string{"a"}})
		doc := mustParseItem(b, `{"id":1, "a":["1", "2", "3", "2"]}`)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			idx.fillKeysBuf(doc)
		}
	})

}
