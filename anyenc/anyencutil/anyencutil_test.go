package anyencutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-store/anyenc"
)

func TestEqual(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		var equalList = [][2]string{
			{`0`, `0`},
			{`"1"`, `"1"`},
			{`true`, `true`},
			{`false`, `false`},
			{`null`, `null`},
			{`[0,"1",2.2,null]`, `[0,"1",2.2,null]`},
			{`{"a":1, "b":2}`, `{"a":1, "b":2}`},
			{`{"b":2, "a":1}`, `{"a":1, "b":2}`},
		}
		for _, v := range equalList {
			assert.True(t, Equal(anyenc.MustParseJson(v[0]), anyenc.MustParseJson(v[1])), v[0], v[1])
		}
	})
	t.Run("not equal", func(t *testing.T) {
		var equalList = [][2]string{
			{`0`, `"0"`},
			{`1`, `2`},
			{`true`, `false`},
			{`[0]`, `[0,1]`},
			{`[0,2]`, `[0,1]`},
			{`{"a":1, "b":2}`, `{"a":1, "b":2, "c": 3}`},
			{`{"a":2, "b":2}`, `{"a":1, "b":2}`},
			{`{"a1":2, "b":2}`, `{"a":1, "b":2}`},
			{`{}`, `{"a":1}`},
		}
		for _, v := range equalList {
			assert.False(t, Equal(anyenc.MustParseJson(v[0]), anyenc.MustParseJson(v[1])), v[0], v[1])
		}
	})
}

func BenchmarkEqual(b *testing.B) {
	var ja = anyenc.MustParseJson(`{"a":"b", "c":["d", "e"], "f":1, "g": {"a":true, "b":null}}`)
	var jb = anyenc.MustParseJson(`{"a":"b", "c":["d", "e"], "f":1, "g": {"a":true, "b":null}}`)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Equal(ja, jb)
	}
}
