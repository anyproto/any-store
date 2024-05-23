package anystore

import (
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
	require.Equal(t, len(keyCase.expected), len(idx.keysBuf), keyCase.doc)
	for i, k := range idx.keysBuf {
		assert.Equal(t, keyCase.expected[i], k.String(), keyCase.doc)
	}
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
			{`{"a":["b","c"]}`, []string{"b", "c"}},
			{`{"a":["a", "a", "b", "c", "b"]}`, []string{"a", "b", "c"}},
			{`{}`, []string{"<nil>"}},
			{`{"a":null}`, []string{"<nil>"}},
		},
	},
	{
		name: "one field sparse",
		info: IndexInfo{Fields: []string{"a"}, Sparse: true},
		cases: []fillKeysCase{
			{`{"a":"b"}`, []string{"b"}},
			{`{"a":["b","c"]}`, []string{"b", "c"}},
			{`{"a":["a", "a", "b", "c", "b"]}`, []string{"a", "b", "c"}},
			{`{}`, []string{}},
			{`{"a":null}`, []string{}},
		},
	},
	{
		name: "reverse",
		info: IndexInfo{Fields: []string{"-a"}},
		cases: []fillKeysCase{
			{`{"a":"b"}`, []string{"b"}},
			{`{"a":["b","c"]}`, []string{"b", "c"}},
			{`{"a":["a", "a", "b", "c", "b"]}`, []string{"a", "b", "c"}},
		},
	},
	{
		name: "two fields",
		info: IndexInfo{Fields: []string{"a", "b"}},
		cases: []fillKeysCase{
			{`{"a":"1"}`, []string{"1/<nil>"}},
			{`{"a":"1","b":"2"}`, []string{"1/2"}},
			{`{"a":[1,2],"b":"2"}`, []string{"1/2", "2/2"}},
			{`{"a":[1,2,1],"b":[2,1,2]}`, []string{"1/2", "1/1", "2/2", "2/1"}},
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
