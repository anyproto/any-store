package anystore

import (
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
		id:  id,
		val: val,
	}
	objVal.Del("id")
	return it, nil
}

type item struct {
	id  []byte
	val *fastjson.Value
}
