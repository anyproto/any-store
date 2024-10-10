package anystore

import (
	"github.com/anyproto/any-store/anyenc"
)

func newItem(val *anyenc.Value) (item, error) {
	objVal, err := val.Object()
	if err != nil {
		return item{}, err
	}

	if idVal := objVal.Get("id"); idVal == nil {
		return item{}, ErrDocWithoutId
	}
	it := item{
		val: val,
	}
	return it, nil
}

type item struct {
	val *anyenc.Value
}

func (i item) appendId(b []byte) []byte {
	idVal := i.val.Get("id")
	if idVal == nil {
		panic("document without id")
	}
	return idVal.MarshalTo(b)
}

func (i item) Value() *anyenc.Value {
	return i.val
}
