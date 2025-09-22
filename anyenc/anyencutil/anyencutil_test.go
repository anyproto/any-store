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
	t.Run("bytes type", func(t *testing.T) {
		arena := &anyenc.Arena{}

		bytes1 := []byte{1, 2, 3, 4, 5}
		bytes2 := []byte{1, 2, 3, 4, 5}
		bytes3 := []byte{1, 2, 3, 4, 6}

		val1 := arena.NewBinary(bytes1)
		val2 := arena.NewBinary(bytes2)
		val3 := arena.NewBinary(bytes3)

		assert.True(t, Equal(val1, val2))
		assert.False(t, Equal(val1, val3))

		emptyBytes1 := arena.NewBinary([]byte{})
		emptyBytes2 := arena.NewBinary([]byte{})
		assert.True(t, Equal(emptyBytes1, emptyBytes2))
		assert.False(t, Equal(emptyBytes1, val1))
	})
	t.Run("bytes vs other types", func(t *testing.T) {
		arena := &anyenc.Arena{}

		bytesVal := arena.NewBinary([]byte{1, 2, 3})
		strVal := anyenc.MustParseJson(`"123"`)
		numVal := anyenc.MustParseJson(`123`)

		assert.False(t, Equal(bytesVal, strVal))
		assert.False(t, Equal(bytesVal, numVal))
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
