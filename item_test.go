package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestNewItem(t *testing.T) {
	t.Run("with id", func(t *testing.T) {
		doc := `{"id":"myId","key":"value"}`
		val, _ := anyenc.ParseJson(doc)
		it, err := newItem(val)
		require.NoError(t, err)
		assert.Equal(t, doc, it.val.String())
	})
	t.Run("missing id", func(t *testing.T) {
		val, _ := anyenc.ParseJson(`{"key":"value"}`)
		_, err := newItem(val)
		require.Error(t, err)
	})
}

// newItem is a TEST-ONLY helper that wraps a value as an item keyed by the
// default "id" field (the pre-configurable-primary-key contract). It lets
// low-level index/iterator tests build items without a collection. Production
// derives keys via collection.newItem / collection.appendId, which honor the
// per-collection primary key.
func newItem(val *anyenc.Value) (item, error) {
	objVal, err := val.Object()
	if err != nil {
		return item{}, err
	}
	if objVal.Get("id") == nil {
		return item{}, ErrDocWithoutId
	}
	return item{val: val}, nil
}

// appendId is a TEST-ONLY mirror of collection.appendId for the default "id" key.
func (i item) appendId(dst []byte) []byte {
	return i.val.Get("id").MarshalTo(dst)
}
