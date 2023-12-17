package query

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/valyala/fastjson"
	"golang.org/x/exp/rand"
)

func TestSortField_Compare(t *testing.T) {
	t.Run("asc", func(t *testing.T) {
		sf := &SortField{Path: []string{"a"}}
		assert.Equal(t, 0, sf.Compare(fastjson.MustParse(`{"a":1}`), fastjson.MustParse(`{"a":1}`)))
		assert.Equal(t, -1, sf.Compare(fastjson.MustParse(`{"a":1}`), fastjson.MustParse(`{"a":2}`)))
		assert.Equal(t, 1, sf.Compare(fastjson.MustParse(`{"a":1}`), fastjson.MustParse(`{"a":0}`)))
	})
	t.Run("desc", func(t *testing.T) {
		sf := &SortField{Path: []string{"a"}, Reverse: true}
		assert.Equal(t, 0, sf.Compare(fastjson.MustParse(`{"a":1}`), fastjson.MustParse(`{"a":1}`)))
		assert.Equal(t, 1, sf.Compare(fastjson.MustParse(`{"a":1}`), fastjson.MustParse(`{"a":2}`)))
		assert.Equal(t, -1, sf.Compare(fastjson.MustParse(`{"a":1}`), fastjson.MustParse(`{"a":0}`)))
	})
}

func TestSorts_Compare(t *testing.T) {
	var ss = Sorts{&SortField{Path: []string{"a"}}, &SortField{Path: []string{"b"}}}
	assert.Equal(t, 0, ss.Compare(fastjson.MustParse(`{"a":1}`), fastjson.MustParse(`{"a":1}`)))
	assert.Equal(t, 0, ss.Compare(fastjson.MustParse(`{"a":1, "b":1}`), fastjson.MustParse(`{"a":1, "b":1}`)))
	assert.Equal(t, -1, ss.Compare(fastjson.MustParse(`{"a":1, "b":1}`), fastjson.MustParse(`{"a":1, "b":2}`)))
	assert.Equal(t, 1, ss.Compare(fastjson.MustParse(`{"a":2, "b":1}`), fastjson.MustParse(`{"a":1, "b":2}`)))
}

func TestSortDocs_Sort100k(t *testing.T) {
	var docs = &SortDocs{
		Sort: &SortField{
			Path: []string{"a"},
		},
		Docs: make([]*fastjson.Value, 100000),
	}
	for i := range docs.Docs {
		docs.Docs[i] = fastjson.MustParse(fmt.Sprintf(`{"id":%d, "a": %f}`, i, rand.Float64()))
	}
	st := time.Now()
	sort.Sort(docs)
	t.Log("random", len(docs.Docs), time.Since(st))
	st = time.Now()
	sort.Sort(docs)
	t.Log("sorted", len(docs.Docs), time.Since(st))
}
