package anystore

import (
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/iterator"
	"github.com/anyproto/any-store/internal/qcontext"
)

type Iterator interface {
	Next() (ok bool)
	Item() Item
	Close() (err error)
}

type itemIterator struct {
	qCtx          *qcontext.QueryContext
	limit, offset uint
	curr          uint
	iterator.ValueIterator
}

func (i *itemIterator) Next() bool {
	for ; i.curr < i.offset; i.curr++ {
		if !i.ValueIterator.Next() {
			return false
		}
	}

	if i.limit > 0 && i.curr >= (i.offset+i.limit) {
		return false
	}

	i.curr++
	return i.ValueIterator.Next()
}

func (i *itemIterator) Item() (it Item) {
	var err error
	err = i.CurrentValue(func(v *fastjson.Value) error {
		it, err = newItem(v, nil, false)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return
}

func (i *itemIterator) Close() (err error) {
	if err = i.ValueIterator.Close(); err != nil {
		i.qCtx.Txn.Discard()
		return
	}
	return i.qCtx.Txn.Commit()
}
