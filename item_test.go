package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func TestNewItem(t *testing.T) {
	var (
		p = &fastjson.Parser{}
	)
	t.Run("with id", func(t *testing.T) {
		doc := `{"id":"myId","key":"value"}`
		val, _ := p.Parse(doc)
		it, err := newItem(val, true)
		require.NoError(t, err)
		assert.Equal(t, doc, it.val.String())
	})
	t.Run("missing id", func(t *testing.T) {
		val, _ := p.Parse(`{"key":"value"}`)
		_, err := newItem(val, true)
		require.Error(t, err)
	})
	t.Run("auto id", func(t *testing.T) {
		val, _ := p.Parse(`{"key":"value"}`)
		it, err := newItem(val, false)
		require.NoError(t, err)
		assert.NotEmpty(t, it.val.String())
	})
	t.Run("not object", func(t *testing.T) {
		val, _ := p.Parse(`[]`)
		_, err := newItem(val, true)
		require.Error(t, err)
	})
}
