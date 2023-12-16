package encoding

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
