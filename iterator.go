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
	qCtx *qcontext.QueryContext
	iterator.ValueIterator
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
