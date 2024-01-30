package iterator

import (
	"fmt"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/qcontext"
	"github.com/anyproto/any-store/internal/sort"
)

func NewSortIterator(qCtx *qcontext.QueryContext, source ValueIterator, sorts sort.Sort) ValueIterator {
	return &sortIterator{
		qCtx:          qCtx,
		c:             sort.NewContainer(sorts),
		ValueIterator: source,
	}
}

type sortIterator struct {
	ValueIterator

	qCtx  *qcontext.QueryContext
	c     *sort.Container
	err   error
	idx   int
	ready bool
}

func (i *sortIterator) Next() bool {
	if !i.ready {
		i.collect()
	}
	if i.c.Len()-1 > i.idx {
		i.idx++
		return true
	} else {
		return false
	}
}

func (i *sortIterator) Valid() bool {
	return i.idx >= 0 && i.idx < i.c.Len()
}

func (i *sortIterator) CurrentId() []byte {
	var currId []byte
	key.Key(i.c.Data[i.idx]).ReadByteValues(i.c.NS, func(b []byte) error {
		currId = b
		return nil
	})
	return currId
}

func (i *sortIterator) CurrentValue(onValue func(val *fastjson.Value) error) (err error) {
	// we don't cache parsed value in iterator, but maybe it's not needed
	return i.qCtx.Fetch(i.CurrentId(), func(b []byte) error {
		val, err := i.qCtx.Parser.ParseBytes(b)
		if err != nil {
			return err
		}
		return onValue(val)
	})
}

func (i *sortIterator) collect() {
	for i.ValueIterator.Next() {
		if i.err = i.ValueIterator.CurrentValue(func(v *fastjson.Value) error {
			i.c.Collect(v)
			return nil
		}); i.err != nil {
			return
		}
	}
	i.c.Sort()
	i.idx = -1
	i.ready = true
}

func (i *sortIterator) Close() error {
	if err := i.ValueIterator.Close(); err != nil {
		return err
	}
	return i.err
}

func (i *sortIterator) String() string {
	return fmt.Sprintf("SORT(%s)", i.ValueIterator.String())
}
