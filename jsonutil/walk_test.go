package jsonutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func TestWalk(t *testing.T) {
	var a = &fastjson.Arena{}
	t.Run("set single key ", func(t *testing.T) {
		v := fastjson.MustParse(`{}`)
		require.NoError(t, Walk(a, v, []string{"a"}, true, func(prevValue *fastjson.Value, value *fastjson.Value) (res *fastjson.Value, err error) {
			assert.Equal(t, `{}`, prevValue.String())
			assert.Nil(t, value)
			return a.NewTrue(), nil
		}))
		assert.Equal(t, `{"a":true}`, v.String())
	})
	t.Run("set path key", func(t *testing.T) {
		v := fastjson.MustParse(`{}`)
		require.NoError(t, Walk(a, v, []string{"a", "b"}, true, func(prevValue *fastjson.Value, value *fastjson.Value) (res *fastjson.Value, err error) {
			assert.Equal(t, `{}`, prevValue.String())
			assert.Nil(t, value)
			return a.NewTrue(), nil
		}))
		assert.Equal(t, `{"a":{"b":true}}`, v.String())
	})
	t.Run("rewrite key", func(t *testing.T) {
		v := fastjson.MustParse(`{"a":1}`)
		require.NoError(t, Walk(a, v, []string{"a"}, true, func(prevValue *fastjson.Value, value *fastjson.Value) (res *fastjson.Value, err error) {
			assert.Equal(t, `{"a":1}`, prevValue.String())
			assert.Equal(t, `1`, value.String())
			return a.NewTrue(), nil
		}))
		assert.Equal(t, `{"a":true}`, v.String())
	})
	t.Run("delete key", func(t *testing.T) {
		v := fastjson.MustParse(`{"a":1}`)
		require.NoError(t, Walk(a, v, []string{"a"}, true, func(prevValue *fastjson.Value, value *fastjson.Value) (res *fastjson.Value, err error) {
			assert.Equal(t, `{"a":1}`, prevValue.String())
			assert.Equal(t, `1`, value.String())
			return nil, nil
		}))
		assert.Equal(t, `{}`, v.String())
	})
	t.Run("set to array", func(t *testing.T) {
		v := fastjson.MustParse(`{"a":{"b":[1,2,3]}}`)
		require.NoError(t, Walk(a, v, []string{"a", "b", "1"}, true, func(prevValue *fastjson.Value, value *fastjson.Value) (res *fastjson.Value, err error) {
			assert.Equal(t, `[1,2,3]`, prevValue.String())
			assert.Equal(t, `2`, value.String())
			return a.NewNumberFloat64(22), nil
		}))
		assert.Equal(t, `{"a":{"b":[1,22,3]}}`, v.String())
	})
	t.Run("set to object inside array", func(t *testing.T) {
		v := fastjson.MustParse(`{"a":[{"b":2}]}`)
		require.NoError(t, Walk(a, v, []string{"a", "0", "b"}, true, func(prevValue *fastjson.Value, value *fastjson.Value) (res *fastjson.Value, err error) {
			assert.Equal(t, `{"b":2}`, prevValue.String())
			assert.Equal(t, `2`, value.String())
			return a.NewNumberFloat64(22), nil
		}))
		assert.Equal(t, `{"a":[{"b":22}]}`, v.String())
	})
	t.Run("error create", func(t *testing.T) {
		var errorCases = [][]string{
			{
				`{"a":1}`,
				"a", "b",
			},
			{
				`{"a":[1,2]}`,
				"a", "b",
			},
			{
				`{"a":[1,2]}`,
				"a", "b", "c",
			},
		}

		for _, ec := range errorCases {
			v := fastjson.MustParse(ec[0])
			if !assert.Error(t, Walk(a, v, ec[1:], true, func(prevValue *fastjson.Value, value *fastjson.Value) (res *fastjson.Value, err error) {
				t.Log(prevValue.String())
				return a.NewTrue(), nil
			})) {
				t.Log(v.String())
			}

		}
	})
}
