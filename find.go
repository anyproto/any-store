package anystore

import (
	"errors"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/iterator"
	"github.com/anyproto/any-store/internal/qcontext"
	"github.com/anyproto/any-store/internal/qplan"
	"github.com/anyproto/any-store/internal/sort"
	"github.com/anyproto/any-store/query"
)

type FindQuery interface {
	Limit(limit uint) FindQuery
	Offset(offset uint) FindQuery
	Cond(cond any) FindQuery
	Sort(sort ...any) FindQuery
	IndexHint(indexNames ...string) FindQuery
	Count() (count int, err error)
	Err() error

	Iter() (Iterator, error)
}

type findQuery struct {
	coll *Collection

	cond          query.Filter
	sort          sort.Sort
	indexHint     []string
	limit, offset uint

	err error
}

func (f *findQuery) Limit(limit uint) FindQuery {
	f.limit = limit
	return f
}

func (f *findQuery) Offset(offset uint) FindQuery {
	f.offset = offset
	return f
}

func (f *findQuery) Cond(cond any) FindQuery {
	var err error
	if f.cond, err = query.ParseCondition(cond); err != nil {
		f.err = errors.Join(f.err, err)
	}
	return f
}

func (f *findQuery) Sort(sorts ...any) FindQuery {
	var err error
	if f.sort, err = sort.ParseSort(sorts...); err != nil {
		f.err = errors.Join(f.err, err)
	}
	return f
}

func (f *findQuery) IndexHint(indexNames ...string) FindQuery {
	f.indexHint = indexNames
	return f
}

func (f *findQuery) Err() error {
	return f.err
}

func (f *findQuery) Iter() (Iterator, error) {
	if f.err != nil {
		return nil, f.err
	}

	qCtx := &qcontext.QueryContext{
		Txn:    f.coll.db.db.NewTransaction(false),
		DataNS: f.coll.dataNS,
		Parser: &fastjson.Parser{},
	}

	return &itemIterator{
		qCtx:          qCtx,
		ValueIterator: f.makeIterator(qCtx, true),
		limit:         f.limit,
		offset:        f.offset,
	}, nil
}

func (f *findQuery) Count() (count int, err error) {
	qCtx := &qcontext.QueryContext{
		Txn:    f.coll.db.db.NewTransaction(false),
		DataNS: f.coll.dataNS,
		Parser: &fastjson.Parser{},
	}
	iter := f.makeIterator(qCtx, false)
	for iter.Next() {
		count++
	}
	return count, iter.Close()
}

func (f *findQuery) makeIterator(qCtx *qcontext.QueryContext, needValues bool) iterator.ValueIterator {
	plan := qplan.QPlan{
		Indexes:   f.coll.indexes,
		Condition: f.cond,
		Sort:      f.sort,
	}
	if len(f.indexHint) != 0 {
		plan.Hint = f.indexHint[0]
	}

	return plan.Make(qCtx, needValues)
}
