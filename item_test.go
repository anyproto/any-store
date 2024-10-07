package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestNewItem(t *testing.T) {
	var a = &anyenc.Arena{}
	t.Run("with id", func(t *testing.T) {
		doc := `{"id":"myId","key":"value"}`
		val, _ := anyenc.ParseJson(doc)
		it, err := newItem(val, a, false)
		require.NoError(t, err)
		assert.Equal(t, doc, it.val.String())
	})
	t.Run("missing id", func(t *testing.T) {
		val, _ := anyenc.ParseJson(`{"key":"value"}`)
		_, err := newItem(val, a, false)
		require.Error(t, err)
	})
	t.Run("auto id", func(t *testing.T) {
		val, _ := anyenc.ParseJson(`{"key":"value"}`)
		it, err := newItem(val, a, true)
		require.NoError(t, err)
		assert.NotEmpty(t, it.val.String())
	})
	t.Run("not object", func(t *testing.T) {
		val, _ := anyenc.ParseJson(`[]`)
		_, err := newItem(val, a, false)
		require.Error(t, err)
	})
}
