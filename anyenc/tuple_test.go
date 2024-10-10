package anyenc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func testTuple() (tp Tuple) {
	tp = tp.Append(MustParseJson(`6`))
	tp = tp.Append(MustParseJson(`"a"`))
	tp = tp.Append(MustParseJson(`true`))
	tp = tp.Append(MustParseJson(`false`))
	tp = tp.Append(MustParseJson(`null`))
	tp = tp.Append(MustParseJson(`[1,2,3]`))
	tp = tp.Append(MustParseJson(`{"a":{"b":4}}`))
	return
}

func TestTuple_String(t *testing.T) {
	tp := testTuple()
	var exp = `6/"a"/true/false/null/[1,2,3]/{"a":{"b":4}}`
	assert.Equal(t, exp, tp.String())
	t.Log(TypeNull, iTypeNull)
}

func BenchmarkTuple_ReadBytes(b *testing.B) {
	b.ReportAllocs()
	tp := testTuple()
	b.ResetTimer()
	for range b.N {
		_ = tp.ReadBytes(func(b []byte) error {
			return nil
		})
	}
}
