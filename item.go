package anystore

import (
	"encoding/json"
	"fmt"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/objectid"
)

type Item interface {
	Decode(v any) (err error)
	DecodeFastJSON(func(v *fastjson.Value) error) error
}

func newItem(val *fastjson.Value, withId bool) (item, error) {
	objVal, err := val.Object()
	if err != nil {
		return item{}, err
	}

	var id []byte

	if idVal := objVal.Get("id"); idVal != nil {
		id = encoding.AppendJSONValue(nil, idVal)
	}
	if id == nil {
		if withId {
			return item{}, fmt.Errorf("document doesn't contain an identifier")
		} else {
			id = encoding.AppendAnyValue(nil, objectid.NewObjectID().Hex())
		}
	}
	it := item{
		val: val,
	}
	return it, nil
}

type item struct {
	val *fastjson.Value
}

func (i item) appendId(b []byte) []byte {
	idVal := i.val.Get("id")
	if idVal == nil {
		panic("document without id")
	}
	return encoding.AppendJSONValue(b, idVal)
}
func (i item) Decode(v any) (err error) {
	bytes := i.val.MarshalTo(nil)
	return json.Unmarshal(bytes, v)
}

func (i item) DecodeFastJSON(f func(v *fastjson.Value) error) error {
	return f(i.val)
}
