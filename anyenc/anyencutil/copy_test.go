package anyencutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestCopy(t *testing.T) {
	t.Run("all types round-trip", func(t *testing.T) {
		var cases = []string{
			`null`,
			`true`,
			`false`,
			`0`,
			`-1.5`,
			`"str"`,
			`""`,
			`[]`,
			`[0,"1",2.2,null,true]`,
			`{}`,
			`{"a":1,"b":"2","c":[1,2,{"d":null}],"e":{"f":{"g":false}}}`,
		}
		a := &anyenc.Arena{}
		for _, src := range cases {
			orig := anyenc.MustParseJson(src)
			cp := Copy(a, orig)
			assert.True(t, Equal(orig, cp), src)
		}
	})

	t.Run("binary and vector", func(t *testing.T) {
		ba := &anyenc.Arena{}
		orig := ba.NewObject()
		orig.Set("bin", ba.NewBinary([]byte{1, 2, 3}))
		orig.Set("vec", ba.NewVectorF32([]float32{1.5, -2.5, 3}))

		a := &anyenc.Arena{}
		cp := Copy(a, orig)
		assert.True(t, Equal(orig, cp))
	})

	t.Run("nil", func(t *testing.T) {
		a := &anyenc.Arena{}
		assert.Nil(t, Copy(a, nil))
	})

	t.Run("no shared memory with source", func(t *testing.T) {
		p := &anyenc.Parser{}
		src, err := p.Parse(anyenc.MustParseJson(`{"a":"text","arr":[1,2]}`).MarshalTo(nil))
		require.NoError(t, err)

		a := &anyenc.Arena{}
		cp := Copy(a, src)

		// Reuse the parser: the source value's memory is invalidated.
		_, err = p.Parse(anyenc.MustParseJson(`{"x":"overwritten value 123456789"}`).MarshalTo(nil))
		require.NoError(t, err)

		assert.Equal(t, "text", cp.GetString("a"))
		assert.Equal(t, float64(2), cp.GetFloat64("arr", "1"))
	})

	t.Run("copy survives further arena use, dies on reset", func(t *testing.T) {
		a := &anyenc.Arena{}
		cp := Copy(a, anyenc.MustParseJson(`{"a":1}`))
		_ = a.NewString("more")
		assert.Equal(t, float64(1), cp.GetFloat64("a"))
	})
}
