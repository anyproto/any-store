package anyenc

import (
	"testing"

	"github.com/valyala/fastjson"
)

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
