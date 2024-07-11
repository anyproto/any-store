package registry

import (
	"sync"

	"github.com/anyproto/any-store/internal/syncpool"
	"github.com/anyproto/any-store/query"
)

func NewFilterRegistry(sp *syncpool.SyncPool) *FilterRegistry {
	return &FilterRegistry{syncPools: sp}
}

type filterEntry struct {
	buf    *syncpool.DocBuffer
	filter query.Filter
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
		if r.filters[i].filter == nil {
			r.filters[i] = filterEntry{filter: f, buf: r.syncPools.GetDocBuf()}
			return i + 1
		}
	}
	r.filters = append(r.filters, filterEntry{filter: f, buf: r.syncPools.GetDocBuf()})
	return len(r.filters)
}

func (r *FilterRegistry) Release(id int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id -= 1
	r.syncPools.ReleaseDocBuf(r.filters[id].buf)
	r.filters[id].buf = nil
	r.filters[id].filter = nil
}

func (r *FilterRegistry) Filter(id int, data string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	id -= 1
	v, err := r.filters[id].buf.Parser.Parse(data)
	if err != nil {
		return false
	}
	return r.filters[id].filter.Ok(v)
}

func NewSortRegistry(sp *syncpool.SyncPool) *SortRegistry {
	return &SortRegistry{syncPools: sp}
}

type sortEntry struct {
	sort query.Sort
	buf  *syncpool.DocBuffer
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
		if r.sorts[i].sort == nil {
			r.sorts[i] = sortEntry{sort: s, buf: r.syncPools.GetDocBuf()}
			return i + 1
		}
	}
	r.sorts = append(r.sorts, sortEntry{sort: s, buf: r.syncPools.GetDocBuf()})
	return len(r.sorts)
}

func (r *SortRegistry) Release(id int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id -= 1
	r.syncPools.ReleaseDocBuf(r.sorts[id].buf)
	r.sorts[id].buf = nil
	r.sorts[id].sort = nil
}

func (r *SortRegistry) Sort(id int, data string) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	id -= 1
	v, err := r.sorts[id].buf.Parser.Parse(data)
	if err != nil {
		return nil
	}
	return r.sorts[id].sort.AppendKey(r.sorts[id].buf.SmallBuf[:0], v)
}
