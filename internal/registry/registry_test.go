package registry

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-store/encoding"
	"github.com/anyproto/any-store/internal/syncpool"
	"github.com/anyproto/any-store/query"
)

func TestFilterRegistry_Filter(t *testing.T) {
	fr := NewFilterRegistry(syncpool.NewSyncPool(), 4)
	filter := query.MustParseCondition
	assert.Equal(t, 1, fr.Register(filter(`{"f":0}`)))
	assert.Equal(t, 2, fr.Register(filter(`{"f":1}`)))
	assert.Equal(t, 3, fr.Register(filter(`{"f":2}`)))
	assert.True(t, fr.Filter(2, []byte(`{"f":1}`)))
	assert.False(t, fr.Filter(3, []byte(`{"f":1}`)))

	fr.Release(2)
	assert.Equal(t, 2, fr.Register(filter(`{"f":3}`)))
	assert.True(t, fr.Filter(2, []byte(`{"f":3}`)))
}

func TestSortRegistry_Sort(t *testing.T) {
	sr := NewSortRegistry(syncpool.NewSyncPool(), 4)

	var testJson = []byte(`{"n0":0, "n1":1, "n2":2}`)

	assert.Equal(t, 1, sr.Register(query.MustParseSort("n0")))
	assert.Equal(t, 2, sr.Register(query.MustParseSort("n1")))
	assert.Equal(t, 3, sr.Register(query.MustParseSort("n2")))

	assert.Equal(t, encoding.AppendAnyValue(nil, 1), sr.Sort(2, testJson))

	sr.Release(2)
	assert.Equal(t, 2, sr.Register(query.MustParseSort("n2")))
	assert.Equal(t, encoding.AppendAnyValue(nil, 2), sr.Sort(2, testJson))
}

func TestSortRegistryConcurrent(t *testing.T) {
	bufSize := 10
	numWorkers := 20
	sr := NewSortRegistry(syncpool.NewSyncPool(), bufSize)
	var wg sync.WaitGroup
	for j := 0; j < 100; j++ {
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(i int) {
				jsonObj := []byte(fmt.Sprintf(`{"f":%d}`, i))
				id := sr.Register(query.MustParseSort("f"))
				assert.Equal(t, encoding.AppendAnyValue(nil, i), sr.Sort(id, jsonObj))
				sr.Release(id)
				wg.Done()
			}(j*numWorkers + i)
		}
	}
	wg.Wait()
}

func TestFilterRegistryConcurrent(t *testing.T) {
	bufSize := 10
	numWorkers := 20
	fr := NewFilterRegistry(syncpool.NewSyncPool(), bufSize)
	var wg sync.WaitGroup
	for j := 0; j < 100; j++ {
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(i int) {
				jsonObj := fmt.Sprintf(`{"f":%d}`, i)
				id := fr.Register(query.MustParseCondition(jsonObj))
				assert.True(t, fr.Filter(id, []byte(jsonObj)))
				assert.False(t, fr.Filter(id, []byte(`{"f":-1}`)))
				fr.Release(id)
				wg.Done()
			}(j*numWorkers + i)
		}
	}
	wg.Wait()
}

func BenchmarkFilterRegistry_Filter(b *testing.B) {
	fr := NewFilterRegistry(syncpool.NewSyncPool(), 4)
	id := fr.Register(query.MustParseCondition(`{"f":0}`))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fr.Filter(id, []byte(`{"f":1}`))
	}
}

func BenchmarkSortRegistry_Sort(b *testing.B) {
	sr := NewSortRegistry(syncpool.NewSyncPool(), 4)
	id := sr.Register(query.MustParseSort("f"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sr.Sort(id, []byte(`{"f":1}`))
	}
}

func BenchmarkFilterRegistry_FilterRelease(b *testing.B) {
	cond := query.MustParseCondition(`{"f":0}`)
	fr := NewFilterRegistry(syncpool.NewSyncPool(), 4)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fr.Register(cond)
		fr.Filter(id, []byte(`{"f":1}`))
		fr.Release(id)
	}
}

func BenchmarkSortRegistry_SortRelease(b *testing.B) {
	sr := NewSortRegistry(syncpool.NewSyncPool(), 4)
	sort := query.MustParseSort("f")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := sr.Register(sort)
		sr.Sort(id, []byte(`{"f":1}`))
		sr.Release(id)
	}
}
