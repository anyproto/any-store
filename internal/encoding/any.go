package encoding

import (
	"fmt"
)

func AppendAnyValue(b []byte, v any) []byte {
	if v == nil {
		return append(b, uint8(TypeNull))
	}

	switch tv := v.(type) {
	case string:
		b = append(b, uint8(TypeString))
		b = append(b, []byte(tv)...)
	case []byte:
		b = append(b, uint8(TypeString))
		b = append(b, tv...)
	case uint:
		b = append(b, uint8(TypeNumber))
		b = AppendNumber(b, tv)
	case uint8:
		b = append(b, uint8(TypeNumber))
		b = AppendNumber(b, tv)
	case uint16:
		b = append(b, uint8(TypeNumber))
		b = AppendNumber(b, tv)
	case uint32:
		b = append(b, uint8(TypeNumber))
		b = AppendNumber(b, tv)
	case uint64:
		b = append(b, uint8(TypeNumber))
		b = AppendNumber(b, tv)
	case int:
		b = append(b, uint8(TypeNumber))
		b = AppendNumber(b, tv)
	case int8:
		b = append(b, uint8(TypeNumber))
		b = AppendNumber(b, tv)
	case int16:
		b = append(b, uint8(TypeNumber))
		b = AppendNumber(b, tv)
	case int32:
		b = append(b, uint8(TypeNumber))
		b = AppendNumber(b, tv)
	case int64:
		b = append(b, uint8(TypeNumber))
		b = AppendNumber(b, tv)
	case float32:
		b = append(b, uint8(TypeNumber))
		b = AppendNumber(b, tv)
	case float64:
		b = append(b, uint8(TypeNumber))
		b = AppendFloat64(b, tv)
	case bool:
		if tv {
			b = append(b, uint8(TypeTrue))
		} else {
			b = append(b, uint8(TypeFalse))
		}
	default:
		panic(fmt.Sprintf("TODO: make other types: %T", v))
	}
	return b
}

func DecodeToAny(b []byte) (v any, err error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("can't decode, bytes is empty")
	}
	switch Type(b[0]) {
	case TypeObject, TypeArray, TypeString:
		return string(b[1:]), nil
	case TypeNumber:
		if len(b[1:]) != 8 {
			return nil, fmt.Errorf("unexpected number encoding")
		}
		return BytesToFloat64(b[1:]), nil
	case TypeNull:
		return nil, nil
	case TypeTrue:
		return true, nil
	case TypeFalse:
		return false, nil
	default:
		return nil, fmt.Errorf("unexpected binary type: %v", Type(b[0]))
	}
}
