package query

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

type Type anyenc.Type

const (
	TypeNull     = Type(anyenc.TypeNull)
	TypeNumber   = Type(anyenc.TypeNumber)
	TypeString   = Type(anyenc.TypeString)
	TypeFalse    = Type(anyenc.TypeFalse)
	TypeTrue     = Type(anyenc.TypeTrue)
	TypeArray    = Type(anyenc.TypeArray)
	TypeObject   = Type(anyenc.TypeObject)
	TypeObjectID = Type(anyenc.TypeObjectID)
)

var stringToType = map[string]Type{}

func init() {
	for i, ts := range typeString {
		if i != 0 && ts != "" {
			stringToType[ts] = Type(i)
		}
	}
}

// typeString is indexed by the anyenc type value. The gaps at binary(8),
// compressedObjectS2(9) and vectorF32(10) are intentionally empty — they are not
// queryable via $type — and are skipped when building stringToType. objectId(11)
// is queryable.
var typeString = []string{
	"", "null", "number", "string", "false", "true", "array", "object",
	"", "", "", "objectId",
}

func (t Type) String() string {
	if int(t) >= len(typeString) || t <= 0 {
		return ""
	}
	return typeString[t]
}
