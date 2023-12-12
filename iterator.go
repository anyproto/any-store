package anystore

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/query"
)

type Iterator interface {
	Next() (ok bool)
	Item() Item
	Close()
}

func newDirectIterator() *directIterator {
	return &directIterator{
		p: parserPool.Get(),
	}
}

type directIterator struct {
	it       *badger.Iterator
	txn      *badger.Txn
	filter   query.Filter
	nextItem Item
	p        *fastjson.Parser
}

func (d *directIterator) Next() (ok bool) {
	for d.it.Valid() {
		itm, err := d.itemIfOk()
		if err != nil {
			panic(fmt.Errorf("iterator error: %w", err))
		}
		d.it.Next()
		if itm != nil {
			d.nextItem = itm
			return true
		}
	}
	return false
}

func (d *directIterator) Item() Item {
	return d.nextItem
}

func (d *directIterator) Close() {
	d.it.Close()
	d.txn.Discard()
	parserPool.Put(d.p)
}

func (d *directIterator) itemIfOk() (itm Item, err error) {
	if err = d.it.Item().Value(func(val []byte) error {
		jval, e := d.p.ParseBytes(val)
		if e != nil {
			return e
		}
		if d.filter.Ok(jval) {
			itm = item{val: jval}
		}
		return nil
	}); err != nil {
		return
	}
	return
}
