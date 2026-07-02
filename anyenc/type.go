package anyenc

import (
	"fmt"
)

const EOS = byte(0)

type Type uint8

const (
	TypeNull               = Type(1)
	TypeNumber             = Type(2)
	TypeString             = Type(3)
	TypeFalse              = Type(4)
	TypeTrue               = Type(5)
	TypeArray              = Type(6)
	TypeObject             = Type(7)
	TypeBinary             = Type(8)
	TypeCompressedObjectS2 = Type(9)
	// TypeVectorF32 stores a packed little-endian []float32 embedding as a single
	// length-prefixed blob (4 bytes/dim) instead of a generic number array — far
	// smaller on disk and decoded zero-copy. Vectors are not orderable, so they
	// never appear in range/sort index keys (no inverted form needed).
	TypeVectorF32 = Type(10)
	// TypeObjectID stores a 12-byte ObjectID as a fixed-width, memcmp-orderable
	// value: the tag followed by the 12 raw big-endian bytes (no length prefix),
	// 13 bytes total. Unlike vectors, ObjectIDs are orderable and appear in
	// index keys, so an inverted form (iTypeObjectID) exists for reverse indexes.
	TypeObjectID = Type(11)

	iTypeNull               = ^Type(1)
	iTypeNumber             = ^Type(2)
	iTypeString             = ^Type(3)
	iTypeFalse              = ^Type(4)
	iTypeTrue               = ^Type(5)
	iTypeArray              = ^Type(6)
	iTypeObject             = ^Type(7)
	iTypeBinary             = ^Type(8)
	iTypeCompressedObjectS2 = ^Type(9)
	// iTypeObjectID (^Type(11) = 0xF4) is the inverted tag for reverse index
	// keys. Note ^Type(10) = 0xF5 (inverted vectorF32) is deliberately skipped:
	// vectors are never index-keyed, so 0xF5 must stay an "unknown type".
	iTypeObjectID = ^Type(11)
)

const (
	emptyKey = 0x1F
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
	case TypeCompressedObjectS2:
		return "compressedObjectS2"
	case TypeVectorF32:
		return "vectorF32"
	case TypeObjectID:
		return "objectID"
	default:
		return fmt.Sprintf("unknown type: %d", t)
	}
}
