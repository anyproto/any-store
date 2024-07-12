package registry

import (
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/internal/syncpool"
	"github.com/anyproto/any-store/query"
)

func NewFilterRegistry(sp *syncpool.SyncPool, readConnections int) *FilterRegistry {
	return &FilterRegistry{syncPools: sp, filters: make([]filterEntry, readConnections*2)}
}

type filterEntry struct {
	buf    *syncpool.DocBuffer
	filter query.Filter
	inUse  atomic.Bool
}

type FilterRegistry struct {
	syncPools *syncpool.SyncPool
	filters   []filterEntry
	mu        sync.Mutex
}

func (r *FilterRegistry) Register(f query.Filter) int {
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
	r.filters = append(r.filters, filterEntry{filter: f, buf: r.syncPools.GetDocBuf(), inUse: atomic.Bool{}})
	r.filters[len(r.filters)-1].inUse.Store(true)
	return len(r.filters)
}

func (r *FilterRegistry) Release(id int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id -= 1
	r.syncPools.ReleaseDocBuf(r.filters[id].buf)
	r.filters[id].inUse.Store(false)
}

func (r *FilterRegistry) Filter(id int, data string) bool {
	id -= 1
	v, err := r.filters[id].buf.Parser.Parse(data)
	if err != nil {
		return false
	}
	return r.filters[id].filter.Ok(v)
}

func NewSortRegistry(sp *syncpool.SyncPool, readConnections int) *SortRegistry {
	return &SortRegistry{syncPools: sp, sorts: make([]sortEntry, readConnections+1)}
}

type sortEntry struct {
	sort  query.Sort
	buf   *syncpool.DocBuffer
	inUse atomic.Bool
}

type SortRegistry struct {
	syncPools *syncpool.SyncPool
	sorts     []sortEntry
	mu        sync.Mutex
}

func (r *SortRegistry) Register(s query.Sort) int {
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
	r.sorts = append(r.sorts, sortEntry{sort: s, buf: r.syncPools.GetDocBuf(), inUse: atomic.Bool{}})
	r.sorts[len(r.sorts)-1].inUse.Store(true)
	return len(r.sorts)
}

func (r *SortRegistry) Release(id int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	id -= 1
	r.syncPools.ReleaseDocBuf(r.sorts[id].buf)
	r.sorts[id].inUse.Store(false)
}

func (r *SortRegistry) Sort(id int, data string) []byte {
	id -= 1
	v, err := r.sorts[id].buf.Parser.Parse(data)
	if err != nil {
		return nil
	}
	return r.sorts[id].sort.AppendKey(r.sorts[id].buf.SmallBuf[:0], v)
}
