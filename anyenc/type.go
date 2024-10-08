package anyenc

import (
	"fmt"
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
)

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
