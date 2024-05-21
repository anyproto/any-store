package anystore

import (
	"encoding/json"
	"fmt"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/objectid"
	"github.com/anyproto/any-store/internal/parser"
)

type Item interface {
	Decode(v any) (err error)
	Value() *fastjson.Value
}

func newItem(val *fastjson.Value, a *fastjson.Arena, autoId bool) (item, error) {
	objVal, err := val.Object()
	if err != nil {
		return item{}, err
	}

	if idVal := objVal.Get("id"); idVal == nil {
		if autoId {
			id := objectid.NewObjectID().Hex()
			objVal.Set("id", a.NewString(id))
		} else {
			return item{}, fmt.Errorf("document without id")
		}
	}
	it := item{
		val: val,
	}
	return it, nil
}

func parseItem(p *fastjson.Parser, a *fastjson.Arena, doc any, autoId bool) (it item, err error) {
	docJ, err := parser.AnyToJSON(p, doc)
	if err != nil {
		return item{}, err
	}
	return newItem(docJ, a, autoId)
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

func (i item) Value() *fastjson.Value {
	return i.val
}
