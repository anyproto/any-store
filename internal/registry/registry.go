package registry

import (
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/internal/syncpool"
	"github.com/anyproto/any-store/query"
)

func NewFilterRegistry(sp *syncpool.SyncPool, bufSize int) *FilterRegistry {
	return &FilterRegistry{syncPools: sp, filters: make([]filterEntry, bufSize), usersCh: make(chan struct{}, bufSize)}
}

type filterEntry struct {
	buf    *syncpool.DocBuffer
	filter query.Filter
	inUse  atomic.Bool
}

type FilterRegistry struct {
	syncPools *syncpool.SyncPool
	usersCh   chan struct{}
	filters   []filterEntry
	mu        sync.Mutex
}

func (r *FilterRegistry) Register(f query.Filter) int {
	r.usersCh <- struct{}{}

	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < len(r.filters); i++ {
		if !r.filters[i].inUse.Load() {
			// We can use separate Load and Store because we have a lock
			r.filters[i].inUse.Store(true)
			r.filters[i].filter = f
			r.filters[i].buf = r.syncPools.GetDocBuf()
			return i + 1
		}
	}
	panic("integrity violation")
}

func (r *FilterRegistry) Release(id int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id -= 1
	r.syncPools.ReleaseDocBuf(r.filters[id].buf)
	r.filters[id].inUse.Store(false)

	<-r.usersCh
}

// Filter could be called only between Register and Release calls, so it's safe to use concurrently
func (r *FilterRegistry) Filter(id int, data string) bool {
	id -= 1
	v, err := r.filters[id].buf.Parser.Parse(data)
	if err != nil {
		return false
	}
	return r.filters[id].filter.Ok(v)
}

type sortEntry struct {
	sort  query.Sort
	buf   *syncpool.DocBuffer
	inUse atomic.Bool
}

type SortRegistry struct {
	usersCh   chan struct{}
	syncPools *syncpool.SyncPool
	sorts     []sortEntry
	mu        sync.Mutex
}

func NewSortRegistry(sp *syncpool.SyncPool, bufSize int) *SortRegistry {
	return &SortRegistry{syncPools: sp, sorts: make([]sortEntry, bufSize), usersCh: make(chan struct{}, bufSize)}
}

func (r *SortRegistry) Register(s query.Sort) int {
	r.usersCh <- struct{}{}

	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < len(r.sorts); i++ {
		if !r.sorts[i].inUse.Load() {
			// We can use separate Load and Store because we have a lock
			r.sorts[i].inUse.Store(true)
			r.sorts[i].sort = s
			r.sorts[i].buf = r.syncPools.GetDocBuf()
			return i + 1
		}
	}
	panic("integrity violation")
}

func (r *SortRegistry) Release(id int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id -= 1
	r.syncPools.ReleaseDocBuf(r.sorts[id].buf)
	r.sorts[id].inUse.Store(false)

	<-r.usersCh
}

func (r *SortRegistry) Sort(id int, data string) []byte {
	id -= 1
	v, err := r.sorts[id].buf.Parser.Parse(data)
	if err != nil {
		return nil
	}
	return r.sorts[id].sort.AppendKey(r.sorts[id].buf.SmallBuf[:0], v)
}
