package anystore

import (
	"context"
	"errors"
	"strings"

	"github.com/anyproto/any-store/internal/sort"
	"github.com/anyproto/any-store/query"
)

type Query interface {
	Cond(filter any) Query
	Limit(limit uint) Query
	Offset(offset uint) Query
	Sort(sort ...any) Query
	Iter() Iterator
	Count(ctx context.Context) (count int, err error)
	Update(ctx context.Context, modifier any) error
	Delete(ctx context.Context) (err error)
}

type collQuery struct {
	c *collection

	cond query.Filter
	sort sort.Sort

	limit, offset uint

	sqlQuery string

	err error
}

func (q *collQuery) Cond(filter any) Query {
	var err error
	if q.cond, err = query.ParseCondition(filter); err != nil {
		q.err = errors.Join(err)
	}
	return q
}

func (q *collQuery) Limit(limit uint) Query {
	q.limit = limit
	return q
}

func (q *collQuery) Offset(offset uint) Query {
	q.offset = offset
	return q
}

func (q *collQuery) Sort(sorts ...any) Query {
	var err error
	if q.sort, err = sort.ParseSort(sorts...); err != nil {
		q.err = errors.Join(err)
	}
	return q
}

func (q *collQuery) Iter() Iterator {
	//TODO implement me
	panic("implement me")
}

func (q *collQuery) Update(ctx context.Context, modifier any) error {
	//TODO implement me
	panic("implement me")
}

func (q *collQuery) Delete(ctx context.Context) (err error) {
	//TODO implement me
	panic("implement me")
}

func (q *collQuery) Count(ctx context.Context) (count int, err error) {
	return
}

func (q *collQuery) makeQuery(needValues bool) (err error) {
	if q.err != nil {
		return err
	}

}

type queryIndex struct {
	idx       *index
	filter    query.Filter
	bounds    query.Bounds
	reverse   bool
	exactSort bool
}

type queryBuilder struct {
	tableName string
	joins     []string
	buf       *strings.Builder
}
