package anystore

import "github.com/valyala/fastjson"

// Doc represents a document in the collection.
type Doc interface {
	// Decode decodes the document into the provided variable.
	// The variable should be a pointer to the appropriate type.
	// Returns an error if decoding fails.
	Decode(v any) error

	// Value returns the document as a *fastjson.Value.
	// Important: When used in an iterator, the returned value is valid only until the next call to Next.
	Value() *fastjson.Value
}
