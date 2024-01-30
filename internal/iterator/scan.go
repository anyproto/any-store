package iterator

import (
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/qcontext"
	"github.com/anyproto/any-store/query"
)

// NewScanIterator creates ValueIterator that used to iterate over the data with optional filtering and sorting by id field
func NewScanIterator(qCtx *qcontext.QueryContext, cond query.Filter, bounds query.Bounds, reverse bool) ValueIterator {
	ii := NewIndexIterator(qCtx, qCtx.DataNS, bounds, reverse).(*uniqIterator).IdIterator.(*indexIterator)
	return &scanIterator{
		condition:     cond,
		indexIterator: ii,
	}
}

type scanIterator struct {
	condition query.Filter

	currentValue *fastjson.Value

	*indexIterator
}

func (v *scanIterator) Next() bool {
	for v.indexIterator.Next() {
		var ok bool
		if ok, v.err = v.checkFilter(); v.err != nil {
			return false
		}
		if ok {
			return true
		}
	}
	return false
}

func (v *scanIterator) CurrentValue(onValue func(v *fastjson.Value) error) (err error) {
	if v.err != nil {
		return v.err
	}
	if v.currentValue == nil {
		return v.currentItem.Value(func(val []byte) error {
			if v.currentValue, err = v.qCtx.Parser.ParseBytes(val); err != nil {
				return err
			}
			return onValue(v.currentValue)
		})
	}
	return onValue(v.currentValue)
}
func (v *scanIterator) CurrentId() []byte {
	return v.currentId
}

func (v *scanIterator) checkFilter() (ok bool, err error) {
	if v.condition == nil {
		v.currentValue = nil
		return true, nil
	}
	if err = v.currentItem.Value(func(val []byte) error {
		if v.currentValue, err = v.qCtx.Parser.ParseBytes(val); err != nil {
			return err
		}
		if v.condition.Ok(v.currentValue) {
			ok = true
		} else {
			v.currentItem = nil
			v.currentValue = nil
		}
		return nil
	}); err != nil {
		return
	}
	return
}

func (v *scanIterator) String() string {
	indexName := "id"
	var result = "SCAN(" + indexName
	boundsToString := v.boundsToString()
	if len(boundsToString) > 0 {
		result += ", " + boundsToString.String()
	}
	if v.isReverse {
		result += ", rev"
	}
	return result + ")"

}
