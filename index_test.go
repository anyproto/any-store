package anystore

import (
	"hash/fnv"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/key"
)

func TestIndex_fillKeysBuf(t *testing.T) {
	idx := &index{
		ns:         key.NewNS(key.KeyFromString("/some/namespace")),
		fieldPaths: [][]string{{"a"}, {"b"}},
		bitmap:     roaring.New(),
		sparse:     false,
		hash:       fnv.New32a(),
	}

	idx.fillKeysBuf(fastjson.MustParse(`{"a":"1", "b":"2"}`))
	t.Log("1")
	for _, k := range idx.keysBuf {
		t.Log(k.String())
	}

	idx.fillKeysBuf(fastjson.MustParse(`{"a":["1", "2"], "b":"2"}`))
	t.Log("2")

	for _, k := range idx.keysBuf {
		t.Log(k.String())
	}

	idx.fillKeysBuf(fastjson.MustParse(`{"b":["1", "2"], "a":"2"}`))
	t.Log("3")

	for _, k := range idx.keysBuf {
		t.Log(k.String())
	}

	t.Log("4")
	idx.fillKeysBuf(fastjson.MustParse(`{"a":["1", "2"], "b":["3", "4", "5"]}`))
}

func BenchmarkCollection_AddIndex(b *testing.B) {
	idx := &index{
		ns:         key.NewNS(key.KeyFromString("/some/namespace")),
		fieldPaths: [][]string{{"a"}, {"b"}},
		bitmap:     roaring.New(),
		sparse:     false,
		hash:       fnv.New32a(),
	}
	v := fastjson.MustParse(`{"a":"1", "b":"2"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.fillKeysBuf(v)
	}
}
