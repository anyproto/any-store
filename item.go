package anystore

import (
	"encoding/json"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/objectid"
	"github.com/anyproto/any-store/internal/parser"
)

type Item interface {
	Decode(v any) (err error)
	Value() *anyenc.Value
}

func newItem(val *anyenc.Value, a *anyenc.Arena, autoId bool) (item, error) {
	objVal, err := val.Object()
	if err != nil {
		return item{}, err
	}

	if idVal := objVal.Get("id"); idVal == nil {
		if autoId {
			id := objectid.NewObjectID().Hex()
			objVal.Set("id", a.NewString(id))
		} else {
			return item{}, ErrDocWithoutId
		}
	}
	it := item{
		val: val,
	}
	return it, nil
}

func parseItem(a *anyenc.Arena, doc any, autoId bool) (it item, err error) {
	docJ, err := parser.Parse(doc)
	if err != nil {
		return item{}, err
	}
	return newItem(docJ, a, autoId)
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

func (i item) Decode(v any) (err error) {
	bytes := i.val.MarshalTo(nil)
	return json.Unmarshal(bytes, v)
}

func (i item) Value() *anyenc.Value {
	return i.val
}
