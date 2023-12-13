package query

import "github.com/anyproto/any-store/internal/encoding"

type Type encoding.Type

const (
	TypeNull   = Type(0)
	TypeNumber = Type(1)
	TypeString = Type(2)
	TypeFalse  = Type(3)
	TypeTrue   = Type(4)
	TypeArray  = Type(5)
	TypeObject = Type(6)
)

var stringToType = map[string]Type{}

func init() {
	for i, ts := range typeString {
		stringToType[ts] = Type(i)
	}
}

var typeString = []string{
	"null", "number", "string", "false", "true", "array", "object",
}

func (t Type) String() string {
	if int(t) >= len(typeString) {
		return ""
	}
	return typeString[t]
}
