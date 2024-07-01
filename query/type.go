package query

import (
	"github.com/anyproto/any-store/encoding"
)

type Type encoding.Type

const (
	TypeNull   = Type(encoding.TypeNull)
	TypeNumber = Type(encoding.TypeNumber)
	TypeString = Type(encoding.TypeString)
	TypeFalse  = Type(encoding.TypeFalse)
	TypeTrue   = Type(encoding.TypeTrue)
	TypeArray  = Type(encoding.TypeArray)
	TypeObject = Type(encoding.TypeObject)
)

var stringToType = map[string]Type{}

func init() {
	for i, ts := range typeString {
		if i != 0 {
			stringToType[ts] = Type(i)
		}
	}
}

var typeString = []string{
	"", "null", "number", "string", "false", "true", "array", "object",
}

func (t Type) String() string {
	if int(t) >= len(typeString) || t <= 0 {
		return ""
	}
	return typeString[t]
}
