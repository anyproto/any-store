package query

import (
	"github.com/anyproto/any-store/anyenc"
)

type Type anyenc.Type

const (
	TypeNull   = Type(anyenc.TypeNull)
	TypeNumber = Type(anyenc.TypeNumber)
	TypeString = Type(anyenc.TypeString)
	TypeFalse  = Type(anyenc.TypeFalse)
	TypeTrue   = Type(anyenc.TypeTrue)
	TypeArray  = Type(anyenc.TypeArray)
	TypeObject = Type(anyenc.TypeObject)
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
