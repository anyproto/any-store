package anystore

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
	"testing"
)

func mustParseItem(t *testing.T, s any) item {
	it, err := parseItem(&fastjson.Parser{}, &fastjson.Arena{}, s, true)
	require.NoError(t, err)
	return it
}

func assertIdxKeyBuf(t *testing.T, idx *index, expected ...string) {
	require.Equal(t, len(expected), len(idx.keysBuf))
	for i, k := range idx.keysBuf {
		assert.Equal(t, expected[i], k.String())
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
	for _, idxCase := range fillKeysCases {
		t.Run(idxCase.name, func(t *testing.T) {
			idx := newIdx(idxCase.info)
			for _, keyCase := range idxCase.cases {
				idx.fillKeysBuf(mustParseItem(t, keyCase.doc))
				assertIdxKeyBuf(t, idx, keyCase.expected...)
			}
		})
	}
}
