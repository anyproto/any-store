package anyenc

import (
	"fmt"

	"github.com/valyala/fastjson"
)

const EOS = byte(0)

type Type uint8

const (
	TypeNull   = Type(1)
	TypeNumber = Type(2)
	TypeString = Type(3)
	TypeFalse  = Type(4)
	TypeTrue   = Type(5)
	TypeArray  = Type(6)
	TypeObject = Type(7)
	TypeBinary = Type(8)

	// inverted types
	iTypeNull    = Type(108)
	iTypeNumber  = Type(107)
	iTypeString  = Type(106)
	iTypeFalse   = Type(105)
	iTypeTrue    = Type(104)
	iTypeArray   = Type(103)
	iTypeObject  = Type(102)
	iTypeOBinary = Type(101)
)

func FastJSONTypeToType(t fastjson.Type) Type {
	switch t {
	case fastjson.TypeNumber:
		return TypeNumber
	case fastjson.TypeObject:
		return TypeObject
	case fastjson.TypeFalse:
		return TypeFalse
	case fastjson.TypeTrue:
		return TypeTrue
	case fastjson.TypeString:
		return TypeString
	case fastjson.TypeArray:
		return TypeArray
	case fastjson.TypeNull:
		return TypeNull
	default:
		panic(fmt.Errorf("unexpected fastjson type: %v", t))
	}
}

func (t Type) String() string {
	switch t {
	case TypeNull:
		return "null"
	case TypeNumber:
		return "number"
	case TypeString:
		return "string"
	case TypeFalse:
		return "false"
	case TypeTrue:
		return "true"
	case TypeArray:
		return "array"
	case TypeObject:
		return "object"
	case TypeBinary:
		return "binary"
	default:
		return fmt.Sprintf("unknown type: %d", t)
	}
}
