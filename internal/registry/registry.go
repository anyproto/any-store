package registry

import (
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

type FilterRegistry struct {
	registry *registry[query.Filter]
}

func NewFilterRegistry(sp *syncpool.SyncPool, bufSize int) *FilterRegistry {
	return &FilterRegistry{registry: newRegistry[query.Filter](sp, bufSize)}
}

func (r *FilterRegistry) Register(f query.Filter) int {
	return r.registry.Register(f)
}

func (r *FilterRegistry) Release(id int) {
	r.registry.Release(id)
}

// Filter could be called only between Register and Release calls, so it's safe to use concurrently
func (r *FilterRegistry) Filter(id int, data []byte) bool {
	id -= 1
	v, err := r.registry.entries[id].buf.Parser.Parse(data)
	if err != nil {
		return false
	}
	// TODO: smallbuf or docbuf?
	buf := r.registry.entries[id].buf
	// check allocks here

	ok := r.registry.entries[id].value.Ok(v, buf)

	return ok
}

type SortRegistry struct {
	registry *registry[query.Sort]
}

func NewSortRegistry(sp *syncpool.SyncPool, bufSize int) *SortRegistry {
	return &SortRegistry{registry: newRegistry[query.Sort](sp, bufSize)}
}

func (r *SortRegistry) Register(s query.Sort) int {
	return r.registry.Register(s)
}

func (r *SortRegistry) Release(id int) {
	r.registry.Release(id)
}

func (r *SortRegistry) Sort(id int, data []byte) []byte {
	id -= 1
	v, err := r.registry.entries[id].buf.Parser.Parse(data)
	if err != nil {
		return nil
	}
	buf := r.registry.entries[id].buf.SmallBuf[:0]
	buf = r.registry.entries[id].value.AppendKey(buf, v)
	r.registry.entries[id].buf.SmallBuf = buf
	return buf
}

type registryEntry[T any] struct {
	buf   *syncpool.DocBuffer
	inUse atomic.Bool
	value T
}

type registry[T any] struct {
	usersCh   chan struct{}
	syncPools *syncpool.SyncPool
	entries   []registryEntry[T]
	mu        sync.Mutex
}

func newRegistry[T any](sp *syncpool.SyncPool, bufSize int) *registry[T] {
	return &registry[T]{syncPools: sp, entries: make([]registryEntry[T], bufSize), usersCh: make(chan struct{}, bufSize)}
}

func (r *registry[T]) Register(v T) int {
	r.usersCh <- struct{}{}

	r.mu.Lock()
	defer r.mu.Unlock()

	for i := range r.entries {
		if !r.entries[i].inUse.Load() {
			// We can use separate Load and Store because we have a lock
			r.entries[i].inUse.Store(true)
			r.entries[i].value = v
			r.entries[i].buf = r.syncPools.GetDocBuf()
			return i + 1
		}
	}
	panic("integrity violation")
}

func (r *registry[T]) Release(id int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id -= 1
	r.syncPools.ReleaseDocBuf(r.entries[id].buf)
	r.entries[id].inUse.Store(false)

	<-r.usersCh
}
