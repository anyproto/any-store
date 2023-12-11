package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
)

func TestNewItem(t *testing.T) {
	var (
		a = &fastjson.Arena{}
		p = &fastjson.Parser{}
	)
	t.Run("with id", func(t *testing.T) {
		val, _ := p.Parse(`{"id":"myId","key":"value"}`)
		it, err := newItem(val, true)
		require.NoError(t, err)
		assert.Equal(t, encoding.AppendJSONValue(nil, a.NewString("myId")), it.id)
		assert.Equal(t, `{"key":"value"}`, it.val.String())
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
		assert.NotEmpty(t, it.id)
		assert.Equal(t, `{"key":"value"}`, it.val.String())
	})
	t.Run("not object", func(t *testing.T) {
		val, _ := p.Parse(`[]`)
		_, err := newItem(val, true)
		require.Error(t, err)
	})
}
