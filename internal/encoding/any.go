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
		b = append(b, EOS)
	case []byte:
		b = append(b, uint8(TypeString))
		b = append(b, tv...)
		b = append(b, EOS)
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

func DecodeToAny(b []byte) (v any, n int, err error) {
	if len(b) == 0 {
		return nil, 0, fmt.Errorf("can't decode, bytes is empty")
	}
	switch Type(b[0]) {
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
		return string(b[1:end]), end + 1, nil
	case TypeNumber:
		if len(b) < 9 {
			return nil, 0, fmt.Errorf("unexpected number encoding")
		}
		return BytesToFloat64(b[1:]), 9, nil
	case TypeNull:
		return nil, 1, nil
	case TypeTrue:
		return true, 1, nil
	case TypeFalse:
		return false, 1, nil
	case iTypeObject, iTypeArray, iTypeString:
		var end int
		for i := range b {
			if b[i] == EOS {
				end = i
				break
			} else if i != 0 {
				b[i] = 255 - b[i]
			}
		}
		if end == 0 {
			return nil, 0, fmt.Errorf("can't decode string: end of string not found")
		}
		return string(b[1:end]), end + 1, nil
	case iTypeNumber:
		if len(b) < 9 {
			return nil, 0, fmt.Errorf("unexpected number encoding")
		}
		return -BytesToFloat64(b[1:]), 9, nil
	case iTypeNull:
		return nil, 1, nil
	case iTypeTrue:
		return true, 1, nil
	case iTypeFalse:
		return false, 1, nil
	default:
		return nil, 0, fmt.Errorf("toAny: unexpected binary type: %v: %v", Type(b[0]), b)
	}
}
