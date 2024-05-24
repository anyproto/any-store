package registry

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/sort"
	"github.com/anyproto/any-store/internal/syncpool"
	"github.com/anyproto/any-store/query"
)

func TestFilterRegistry_Filter(t *testing.T) {
	fr := NewFilterRegistry(syncpool.NewSyncPool())
	filter := query.MustParseCondition
	assert.Equal(t, 1, fr.Register(filter(`{"f":0}`)))
	assert.Equal(t, 2, fr.Register(filter(`{"f":1}`)))
	assert.Equal(t, 3, fr.Register(filter(`{"f":2}`)))
	assert.True(t, fr.Filter(2, `{"f":1}`))
	assert.False(t, fr.Filter(3, `{"f":1}`))

	fr.Release(2)
	assert.Equal(t, 2, fr.Register(filter(`{"f":3}`)))
	assert.True(t, fr.Filter(2, `{"f":3}`))
}

func TestSortRegistry_Sort(t *testing.T) {
	sr := NewSortRegistry(syncpool.NewSyncPool())

	const testJson = `{"n0":0, "n1":1, "n2":2}`

	assert.Equal(t, 1, sr.Register(sort.MustParseSort("n0")))
	assert.Equal(t, 2, sr.Register(sort.MustParseSort("n1")))
	assert.Equal(t, 3, sr.Register(sort.MustParseSort("n2")))

	assert.Equal(t, encoding.AppendAnyValue(nil, 1), sr.Sort(2, testJson))

	sr.Release(2)
	assert.Equal(t, 2, sr.Register(sort.MustParseSort("n2")))
	assert.Equal(t, encoding.AppendAnyValue(nil, 2), sr.Sort(2, testJson))
}

func BenchmarkFilterRegistry_Filter(b *testing.B) {
	fr := NewFilterRegistry(syncpool.NewSyncPool())
	id := fr.Register(query.MustParseCondition(`{"f":0}`))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fr.Filter(id, `{"f":1}`)
	}
}

func BenchmarkSortRegistry_Sort(b *testing.B) {
	sr := NewSortRegistry(syncpool.NewSyncPool())
	id := sr.Register(sort.MustParseSort("f"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sr.Sort(id, `{"f":1}`)
	}
}
