package encoding

import (
	"fmt"

	"github.com/valyala/fastjson"
)

func AppendJSONValue(b []byte, v *fastjson.Value) []byte {
	if v == nil {
		return append(b, uint8(TypeNull))
	}

	switch v.Type() {
	case fastjson.TypeString:
		stringBytes, _ := v.StringBytes()
		b = append(b, uint8(TypeString))
		b = append(b, stringBytes...)
		b = append(b, EOS)
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
		b = append(b, EOS)
	case fastjson.TypeArray:
		b = append(b, uint8(TypeArray))
		b = v.MarshalTo(b)
		b = append(b, EOS)
	default:
		panic(fmt.Errorf("unknown fastjson type: %v", v.Type()))
	}
	return b
}

func AppendInvertedJSON(b []byte, v *fastjson.Value) []byte {
	if v == nil {
		return append(b, uint8(iTypeNull))
	}

	switch v.Type() {
	case fastjson.TypeString:
		stringBytes, _ := v.StringBytes()
		b = append(b, uint8(iTypeString))
		for _, sb := range stringBytes {
			b = append(b, ^sb)
		}
		b = append(b, EOS)
	case fastjson.TypeNumber:
		b = append(b, uint8(iTypeNumber))
		f, _ := v.Float64()
		b = AppendFloat64(b, -f)
	case fastjson.TypeNull:
		b = append(b, uint8(iTypeNull))
	case fastjson.TypeFalse:
		b = append(b, uint8(iTypeFalse))
	case fastjson.TypeTrue:
		b = append(b, uint8(iTypeTrue))
	case fastjson.TypeObject:
		b = append(b, uint8(iTypeObject))
		l := len(b)
		b = v.MarshalTo(b)
		for i, sb := range b[l:] {
			b[i+l] = ^sb
		}
		b = append(b, EOS)
	case fastjson.TypeArray:
		b = append(b, uint8(iTypeArray))
		l := len(b)
		b = v.MarshalTo(b)
		for i, sb := range b[l:] {
			b[i+l] = ^sb
		}
		b = append(b, EOS)
	default:
		panic(fmt.Errorf("unknown fastjson type: %v", v.Type()))
	}
	return b

}

func DecodeToJSON(p *fastjson.Parser, a *fastjson.Arena, b []byte) (v *fastjson.Value, n int, err error) {
	if len(b) == 0 {
		return nil, 0, fmt.Errorf("can't decode, bytes is empty")
	}
	var t = Type(b[0])
	switch t {
	case TypeObject, TypeArray, TypeString:
		var end int
		for i := range b {
			if b[i] == EOS {
				end = i
				break
			}
		}
		if end == 0 {
			return nil, 0, fmt.Errorf("can't decode string: end of string not found")
		}
		if t == TypeString {
			v = a.NewStringBytes(b[1:end])
		} else {
			if v, err = p.ParseBytes(b[1:end]); err != nil {
				return nil, 0, err
			}
		}
		return v, end + 1, nil

	case TypeNumber:
		if len(b) < 9 {
			return nil, 0, fmt.Errorf("unexpected number encoding")
		}
		return a.NewNumberFloat64(BytesToFloat64(b[1:])), 9, nil
	case TypeNull:
		return a.NewNull(), 1, nil
	case TypeTrue:
		return a.NewTrue(), 1, nil
	case TypeFalse:
		return a.NewFalse(), 1, nil
	default:
		return nil, 0, fmt.Errorf("unexpected binary type: %v", Type(b[0]))
	}
}
