package encoding

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
