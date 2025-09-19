package anyenc

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

var encDecData = []string{
	`"a"`,
	`22.32`,
	`true`,
	`false`,
	`null`,
	`["1","2","3"]`,
	`{"key":"value"}`,
	`{"id":12345,"key":"value","int":42,"array":[1,2,3],"object":{"float":33.2,"null":null,"true":true,"false":false}}`,
	`[{},{"":[{"":1},{}]},{"a":"b"}]`,
}

func TestEncodeDecode(t *testing.T) {
	var a = &Arena{}
	var fa = &fastjson.Arena{}
	var p = &Parser{}
	var buf []byte
	for _, js := range encDecData {
		jv := fastjson.MustParse(js)
		anyVal := a.NewFromFastJson(jv)
		buf = anyVal.MarshalTo(buf[:0])
		assert.NotEmpty(t, buf, js)
		decVal, err := p.Parse(buf)
		require.NoError(t, err, js)
		require.NotNil(t, decVal, js)
		res := decVal.FastJson(fa).String()
		assert.Equal(t, js, res)
		t.Logf("type: %s; json len: %d; anyenc len: %d", decVal.Type(), len(js), len(buf))
	}
}

func TestBinary(t *testing.T) {
	a := &Arena{}
	p := &Parser{}
	payload := []byte{1, 2, 3}
	bin := a.NewBinary(payload)
	buf := bin.MarshalTo(nil)
	val, err := p.Parse(buf)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(payload, val.GetBytes()))
}

func BenchmarkParser_Parse(b *testing.B) {
	a := &Arena{}
	js := `{"id":12345,"key":"value","int":42,"array":[1,2,3],"object":{"float":33.2,"null":null,"true":true,"false":false}}`
	data := a.NewFromFastJson(fastjson.MustParse(js)).MarshalTo(nil)
	p := &Parser{}
	fp := &fastjson.Parser{}
	b.Run("anyenc", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = p.Parse(data)
		}
	})
	b.Run("fastjson", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = fp.Parse(js)
		}
	})

}
