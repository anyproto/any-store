package registry

import (
	"sync"

	"github.com/anyproto/any-store/internal/sort"
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
			return i
		}
	}
	r.filters = append(r.filters, filterEntry{filter: f, buf: r.syncPools.GetDocBuf()})
	return len(r.filters) - 1
}

func (r *FilterRegistry) Release(id int) {
	r.syncPools.ReleaseDocBuf(r.filters[id].buf)
	r.filters[id].buf = nil
	r.filters[id].filter = nil
}

func (r *FilterRegistry) Filter(id int, data string) bool {
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
	sort sort.Sort
	buf  *syncpool.DocBuffer
}

type SortRegistry struct {
	syncPools *syncpool.SyncPool
	sorts     []sortEntry
	mu        sync.Mutex
}

func (r *SortRegistry) Register(s sort.Sort) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < len(r.sorts); i++ {
		if r.sorts[i].sort == nil {
			r.sorts[i] = sortEntry{sort: s, buf: r.syncPools.GetDocBuf()}
			return i
		}
	}
	r.sorts = append(r.sorts, sortEntry{sort: s, buf: r.syncPools.GetDocBuf()})
	return len(r.sorts) - 1
}

func (r *SortRegistry) Release(id int) {
	r.syncPools.ReleaseDocBuf(r.sorts[id].buf)
	r.sorts[id].buf = nil
	r.sorts[id].sort = nil
}

func (r *SortRegistry) Sort(id int, data string) []byte {
	v, err := r.sorts[id].buf.Parser.Parse(data)
	if err != nil {
		return nil
	}
	return r.sorts[id].sort.AppendKey(r.sorts[id].buf.SmallBuf[:0], v)
}
