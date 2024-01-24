package iterator

import (
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/qcontext"
	"github.com/anyproto/any-store/query"
)

// NewFetchIterator creates ValueIterator that uses IdIterator as source.
func NewFetchIterator(qCtx *qcontext.QueryContext, source IdIterator, cond query.Filter) ValueIterator {
	return &fetchIterator{
		qCtx:       qCtx,
		IdIterator: source,
		cond:       cond,
	}
}

type fetchIterator struct {
	qCtx *qcontext.QueryContext
	IdIterator

	cond query.Filter

	currentValue *fastjson.Value
	err          error
}

func (f *fetchIterator) Next() bool {
	for f.IdIterator.Next() {
		ok, err := f.checkFilter()
		if err != nil {
			f.err = err
			return false
		}
		if ok {
			return true
		}
	}
	return false
}

func (f *fetchIterator) CurrentValue(onValue func(v *fastjson.Value) error) (err error) {
	if f.err != nil {
		return f.err
	}
	return onValue(f.currentValue)
}

func (f *fetchIterator) checkFilter() (ok bool, err error) {
	err = f.qCtx.Fetch(f.CurrentId(), func(val []byte) (err error) {
		if f.currentValue, err = f.qCtx.Parser.ParseBytes(val); err != nil {
			return err
		}
		if f.cond == nil {
			ok = true
		} else {
			ok = f.cond.Ok(f.currentValue)
		}
		return
	})
	return
}
