package anyenc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func TestValue_Getters(t *testing.T) {
	a := &Arena{}
	js := `{"id":12345,"key":"value","int":42,"array":[1,2,3],"object":{"float":33.2,"null":null,"true":true,"false":false}}`
	val := MustParseJson(js)
	val.Set("bin", a.NewBinary([]byte{1, 2, 3}))

	t.Run("null", func(t *testing.T) {
		assert.Equal(t, TypeNull, val.Get("object", "null").Type())
		assert.Equal(t, TypeNull, val.Get("object", "non exists").Type())
	})
	t.Run("string", func(t *testing.T) {
		assert.Equal(t, TypeString, val.Get("key").Type())
		assert.Equal(t, "value", val.GetString("key"))
		assert.Equal(t, []byte("value"), val.GetStringBytes("key"))

		assert.Equal(t, "", val.GetString("int"))
		assert.Nil(t, val.GetStringBytes("int"))

		v, err := val.Get("key").StringBytes()
		require.NoError(t, err)
		assert.Equal(t, []byte("value"), v)

		_, err = val.Get("int").StringBytes()
		assert.Error(t, err)
	})
	t.Run("number", func(t *testing.T) {
		assert.Equal(t, TypeNumber, val.Get("array", "1").Type())
		assert.Equal(t, 33.2, val.GetFloat64("object", "float"))
		assert.Equal(t, 42, val.GetInt("int"))

		assert.Equal(t, float64(0), val.GetFloat64("key"))
		assert.Equal(t, 0, val.GetInt("key"))

		v, err := val.Get("int").Int()
		require.NoError(t, err)
		assert.Equal(t, 42, v)
		_, err = val.Get("key").Int()
		assert.Error(t, err)

		fv, err := val.Get("object", "float").Float64()
		require.NoError(t, err)
		assert.Equal(t, 33.2, fv)

		_, err = val.Get("key").Float64()
		assert.Error(t, err)
	})
	t.Run("bool", func(t *testing.T) {
		assert.True(t, val.GetBool("object", "true"))
		assert.False(t, val.GetBool("object", "false"))
		assert.False(t, val.GetBool("key"))
		assert.False(t, val.GetBool("1"))

		v, err := val.Get("object", "true").Bool()
		require.NoError(t, err)
		assert.True(t, v)

		v, err = val.Get("object", "false").Bool()
		require.NoError(t, err)
		assert.False(t, v)

		_, err = val.Get("id").Bool()
		require.Error(t, err)
	})
	t.Run("binary", func(t *testing.T) {
		assert.Equal(t, TypeBinary, val.Get("bin").Type())
		assert.Equal(t, []byte{1, 2, 3}, val.GetBytes("bin"))

		assert.Nil(t, val.GetBytes("int"))

		v, err := val.Get("bin").Bytes()
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3}, v)

		v, err = val.Get("bin").AppendBytes(nil)
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3}, v)

		_, err = val.Get("int").Bytes()
		assert.Error(t, err)
	})
	t.Run("array", func(t *testing.T) {
		assert.Equal(t, TypeArray, val.Get("array").Type())
		assert.Len(t, val.GetArray("array"), 3)

		assert.Nil(t, val.GetArray("int"))

		v, err := val.Get("array").Array()
		require.NoError(t, err)
		assert.Len(t, v, 3)

		_, err = val.Get("int").Array()
		assert.Error(t, err)
	})
	t.Run("object", func(t *testing.T) {
		assert.Equal(t, TypeObject, val.Get("object").Type())
		assert.Equal(t, 4, val.GetObject("object").Len())

		assert.Nil(t, val.GetObject("int"))

		v, err := val.Get("object").Object()
		require.NoError(t, err)
		assert.NotNil(t, v)
		_, err = val.Get("int").Object()
		assert.Error(t, err)
	})
}

func BenchmarkValue_MarshalTo(b *testing.B) {
	js := `{"id":12345,"key":"value","int":42,"array":[1,2,3],"object":{"float":33.2,"null":null,"true":true,"false":false}}`
	a := &Arena{}
	fv := fastjson.MustParse(js)
	v := a.NewFromFastJson(fv)
	b.Run("anyenc", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		var buf []byte
		for range b.N {
			buf = v.MarshalTo(buf[:0])
		}
	})
	b.Run("fastjson", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		var buf []byte
		for range b.N {
			buf = fv.MarshalTo(buf[:0])
		}
	})
}

func TestValue_Set(t *testing.T) {
	a := &Arena{}
	val := MustParseJson(`{"a":{"b":[1,2,3],"c":3}}`)

	val.Get("c").Set("44", a.NewFalse())

	val.Set("d", a.NewTrue())
	assert.True(t, val.GetBool("d"))

	val.Set("c", a.NewNumberInt(5))
	assert.Equal(t, 5, val.GetInt("c"))

	valB := val.Get("a", "b")
	valB.Set("0", a.NewNumberInt(-1))
	assert.Equal(t, -1, val.GetInt("a", "b", "0"))

	valB.Set("4", a.NewNumberInt(33))
	assert.Equal(t, `[-1,2,3,null,33]`, val.Get("a", "b").String())
}

func TestValue_Del(t *testing.T) {
	val := MustParseJson(`{"a":{"b":[1,2,3],"c":3}}`)
	val.Get("a", "b").Del("1")
	val.Get("a").Del("c")
	assert.Equal(t, `{"a":{"b":[1,3]}}`, val.String())
}
