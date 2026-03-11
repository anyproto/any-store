package anyenc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestTuple_AppendInverted_OffsetAfter_Copy(t *testing.T) {
	var regular Tuple
	var inverted Tuple
	val1 := MustParseJson(`"abc"`)
	val2 := MustParseJson(`123`)

	regular = regular.Append(val1)
	regular = regular.Append(val2)

	inverted = inverted.AppendInverted(val1)
	inverted = inverted.AppendInverted(val2)
	require.Len(t, inverted, len(regular))
	for i := range regular {
		assert.Equal(t, ^regular[i], inverted[i])
	}

	off0, err := regular.OffsetAfter(0)
	require.NoError(t, err)
	assert.Equal(t, 0, off0)

	off1, err := regular.OffsetAfter(1)
	require.NoError(t, err)
	assert.Greater(t, off1, 0)
	assert.Less(t, off1, len(regular))

	offAll, err := regular.OffsetAfter(10)
	require.NoError(t, err)
	assert.Equal(t, len(regular), offAll)

	clone := regular.Copy()
	require.Equal(t, []byte(regular), []byte(clone))
	if len(clone) > 0 {
		clone[0] ^= 0xFF
		assert.NotEqual(t, regular[0], clone[0])
	}
}
