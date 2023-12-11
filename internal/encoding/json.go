package encoding

import (
	"fmt"

	"github.com/valyala/fastjson"
)

var arenaPool = &fastjson.ArenaPool{}

func AppendJSONValue(b []byte, v *fastjson.Value) []byte {
	if v == nil {
		return append(b, uint8(TypeNull))
	}

	switch v.Type() {
	case fastjson.TypeString:
		stringBytes, _ := v.StringBytes()
		b = append(b, uint8(TypeString))
		b = append(b, stringBytes...)
	case fastjson.TypeNumber:
		f, _ := v.Float64()
		b = append(b, uint8(TypeNumber))
		b = AppendFloat64(b, f)
	case fastjson.TypeNull:
		b = append(b, uint8(TypeNull))
	case fastjson.TypeFalse:
		b = append(b, uint8(TypeFalse))
	case fastjson.TypeTrue:
		b = append(b, uint8(TypeTrue))
	case fastjson.TypeObject:
		b = append(b, uint8(TypeObject))
		b = v.MarshalTo(b)
	case fastjson.TypeArray:
		b = append(b, uint8(TypeArray))
		b = v.MarshalTo(b)
	default:
		panic(fmt.Errorf("unknown fastjson type: %v", v.Type()))
	}
	return b
}

func DecodeToJSON(p *fastjson.Parser, a *fastjson.Arena, b []byte) (v *fastjson.Value, err error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("can't decode, bytes is empty")
	}
	switch Type(b[0]) {
	case TypeObject, TypeArray:
		return p.ParseBytes(b[1:])
	case TypeString:
		return a.NewStringBytes(b[1:]), nil
	case TypeNumber:
		if len(b[1:]) != 8 {
			return nil, fmt.Errorf("unexpected number encoding")
		}
		return a.NewNumberFloat64(BytesToFloat64(b[1:])), nil
	case TypeNull:
		return a.NewNull(), nil
	case TypeTrue:
		return a.NewTrue(), nil
	case TypeFalse:
		return a.NewFalse(), nil
	default:
		return nil, fmt.Errorf("unexpected binary type: %v", Type(b[0]))
	}
}
