package encoding

import (
	"fmt"

	"github.com/valyala/fastjson"
)

type Type uint8

const (
	TypeNull   = Type(0)
	TypeNumber = Type(1)
	TypeString = Type(2)
	TypeFalse  = Type(3)
	TypeTrue   = Type(4)
	TypeArray  = Type(5)
	TypeObject = Type(6)
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
