package anystore

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

// item wraps a document value. Key derivation and validation are collection
// responsibilities (see collection.newItem / collection.appendId), because the
// primary-key field name is per-collection.
type item struct {
	val *anyenc.Value
}

func (i item) Value() *anyenc.Value {
	return i.val
}
