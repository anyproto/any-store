package iterator

import (
	"bytes"
	"slices"

	"github.com/dgraph-io/badger/v4"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/query"
)

type IndexIterator struct {
	Txn      *badger.Txn
	IndexNs  *key.NS
	Filters  []query.Filter
	StartKey key.Key
	EndKey   key.Key
	Reverse  bool

	it *badger.Iterator

	a *fastjson.Arena
	p *fastjson.Parser

	values [][]byte
	err    error
	valid  bool
}

func (ii *IndexIterator) makeBadgerIter() {
	opts := badger.IteratorOptions{
		PrefetchSize: 100,
		Reverse:      ii.Reverse,
	}
	if ii.StartKey != nil {
		opts.Prefix = ii.StartKey
	} else {
		opts.Prefix = ii.IndexNs.Bytes()
	}
	ii.it = ii.Txn.NewIterator(opts)
	ii.valid = true
	ii.it.Rewind()
}

func (ii *IndexIterator) Next() bool {
	if ii.it == nil {
		ii.makeBadgerIter()
	}
	if !ii.valid {
		return false
	}
	for ii.it.Valid() {
		k := key.Key(ii.it.Item().Key())
		ii.values = ii.values[:0]
		var vi int
		if ii.err = k.ReadByteValues(ii.IndexNs, func(b []byte) error {
			ii.values = slices.Grow(ii.values, vi+1)[:vi+1]
			ii.values[vi] = append(ii.values[vi][:0], b...)
			vi++
			return nil
		}); ii.err != nil {
			break
		}
		if ii.EndKey != nil {
			cmp := bytes.Compare(k, ii.EndKey)
			if (ii.Reverse && cmp == -1) || (!ii.Reverse && cmp == 1) {
				break
			}
		}
		if ii.FiltersOk() {
			ii.it.Next()
			return true
		} else {
			ii.it.Next()
		}
	}

	ii.valid = false
	return false
}

func (ii *IndexIterator) FiltersOk() bool {
	if len(ii.Filters) == 0 {
		return true
	}

	for i, v := range ii.values {
		if len(ii.Filters) == i {
			break
		}
		if !ii.Filters[i].OkBytes(v) {
			return false
		}
	}
	return true
}

func (ii *IndexIterator) Valid() bool {
	return ii.valid
}

func (ii *IndexIterator) Values() [][]byte {
	return ii.values
}

func (ii *IndexIterator) Close() (err error) {
	if ii.it != nil {
		ii.it.Close()
	}
	return ii.err
}
