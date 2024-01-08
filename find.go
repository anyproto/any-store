package anystore

import (
	"errors"

	"github.com/dgraph-io/badger/v4"

	"github.com/anyproto/any-store/internal/sort"
	"github.com/anyproto/any-store/query"
)

type FindQuery interface {
	Limit(limit uint) FindQuery
	Offset(offset uint) FindQuery
	Cond(cond any) FindQuery
	Sort(sort ...any) FindQuery
	Fields(fields ...string) FindQuery
	ExcludeFields(fields ...string) FindQuery
	IndexHint(indexNames ...string) FindQuery
	Err() error

	Iter() (Iterator, error)
}

type findQuery struct {
	coll *Collection

	cond          query.Filter
	sort          sort.Sort
	fields        []string
	excludeField  []string
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

func (f *findQuery) Fields(fields ...string) FindQuery {
	f.fields = fields
	return f
}

func (f *findQuery) ExcludeFields(fields ...string) FindQuery {
	f.excludeField = fields
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

	if f.cond == nil {
		f.cond = query.All{}
	}

	iter := newDirectIterator()
	iter.txn = f.coll.db.db.NewTransaction(false)
	iter.it = iter.txn.NewIterator(badger.IteratorOptions{
		PrefetchSize:   100,
		PrefetchValues: true,
		Prefix:         f.coll.dataNS.Bytes(),
	})
	iter.it.Rewind()
	iter.filter = f.cond
	return iter, nil
}
