package anystore

import (
	"sync"

	"github.com/anyproto/any-store/query"
)

var qbPool = &sync.Pool{
	New: func() any {
		return &queryBuilder{}
	},
}

func newQueryBuilder() *queryBuilder {
	return qbPool.Get().(*queryBuilder)
}

// queryBuilder is kept for compatibility with the index weight calculation code.
// With btree backend, we always do full scan with in-memory filtering.
type queryBuilder struct {
	coll     *collection
	idBounds query.Bounds
	filterId int
	sortId   int
	limit    int
	offset   int
}

func (qb *queryBuilder) Close() {
	if qb != nil {
		qb.coll = nil
		qb.idBounds = qb.idBounds[:0]
		qb.filterId = 0
		qb.sortId = 0
		qbPool.Put(qb)
	}
}
