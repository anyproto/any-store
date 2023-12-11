package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func TestAnyToJSON(t *testing.T) {
	var p = &fastjson.Parser{}
	t.Run("string", func(t *testing.T) {
		v, err := AnyToJSON(p, `{"some":"doc"}`)
		require.NoError(t, err)
		assert.Equal(t, fastjson.TypeObject, v.Type())
	})
	t.Run("bytes", func(t *testing.T) {
		v, err := AnyToJSON(p, []byte(`{"some":"doc"}`))
		require.NoError(t, err)
		assert.Equal(t, fastjson.TypeObject, v.Type())
	})
	t.Run("fastjson", func(t *testing.T) {
		v, err := AnyToJSON(p, []byte(`{"some":"doc"}`))
		require.NoError(t, err)
		v, err = AnyToJSON(p, v)
		require.NoError(t, err)
		assert.Equal(t, fastjson.TypeObject, v.Type())
	})
	t.Run("go type", func(t *testing.T) {
		v, err := AnyToJSON(p, map[string]string{"some": "doc"})
		require.NoError(t, err)
		assert.Equal(t, `{"some":"doc"}`, v.String())
	})
}
