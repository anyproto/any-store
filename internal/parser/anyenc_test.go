package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestParse(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		v, err := Parse(`{"some":"doc"}`)
		require.NoError(t, err)
		assert.Equal(t, anyenc.TypeObject, v.Type())
	})
	t.Run("bytes", func(t *testing.T) {
		a := anyenc.Arena{}
		v, err := Parse(a.NewString("string").MarshalTo(nil))
		require.NoError(t, err)
		assert.Equal(t, anyenc.TypeString, v.Type())
	})
	t.Run("fastjson", func(t *testing.T) {
		v, err := Parse(`{"some":"doc"}`)
		require.NoError(t, err)
		assert.Equal(t, anyenc.TypeObject, v.Type())
		v, err = Parse(v)
		require.NoError(t, err)
		assert.Equal(t, anyenc.TypeObject, v.Type())
	})
	t.Run("go type", func(t *testing.T) {
		v, err := Parse(map[string]string{"some": "doc"})
		require.NoError(t, err)
		assert.Equal(t, `{"some":"doc"}`, v.String())
	})
}
