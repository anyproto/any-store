package anystore

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

// Doc represents a document in the collection.
type Doc interface {
	// Value returns the document as a *anyenc.Value.
	// Important: When used in an iterator, the returned value is valid only until the next call to Next.
	Value() *anyenc.Value
}
