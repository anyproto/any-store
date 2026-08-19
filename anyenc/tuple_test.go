package anyenc

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertCustomErr is a sentinel error type for checking callback propagation.
type assertCustomErr struct{ msg string }

func (e assertCustomErr) Error() string { return e.msg }

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

// TestTuple_FieldBytes exercises Tuple.FieldBytes for each encoded field
// position, plus the out-of-range error branch. Asserts that each field's
// bytes equal the individually-marshaled value, and that concatenating the
// per-field bytes reproduces the full tuple — proving FieldBytes carves at
// the correct boundaries.
func TestTuple_FieldBytes(t *testing.T) {
	a := &Arena{}
	var tuple Tuple
	tuple = tuple.Append(a.NewString("hello"))
	tuple = tuple.Append(a.NewNumberInt(42))
	tuple = tuple.Append(a.NewNull())

	// Expected per-field encodings.
	expectedFb0 := a.NewString("hello").MarshalTo(nil)
	expectedFb1 := a.NewNumberInt(42).MarshalTo(nil)
	expectedFb2 := a.NewNull().MarshalTo(nil)

	fb0, err := tuple.FieldBytes(0)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fb0, expectedFb0),
		"field 0 bytes must equal independent MarshalTo of the value: got %x want %x", fb0, expectedFb0)

	fb1, err := tuple.FieldBytes(1)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fb1, expectedFb1),
		"field 1 bytes must equal independent MarshalTo of the value: got %x want %x", fb1, expectedFb1)

	fb2, err := tuple.FieldBytes(2)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fb2, expectedFb2),
		"field 2 bytes must equal independent MarshalTo of the value: got %x want %x", fb2, expectedFb2)

	// Concatenating the per-field bytes must exactly reproduce the tuple.
	var concat []byte
	concat = append(concat, fb0...)
	concat = append(concat, fb1...)
	concat = append(concat, fb2...)
	assert.True(t, bytes.Equal(concat, []byte(tuple)),
		"concat of FieldBytes must reproduce the full tuple: got %x want %x", concat, []byte(tuple))

	// Out of range.
	_, err = tuple.FieldBytes(3)
	require.Error(t, err)
}

// TestTuple_ReadValues_ErrorPaths covers tuple.go ReadValues error branches:
// parse failure, callback error propagation.
func TestTuple_ReadValues_ErrorPaths(t *testing.T) {
	a := &Arena{}
	var tuple Tuple
	tuple = tuple.Append(a.NewString("a"))
	tuple = tuple.Append(a.NewString("b"))

	t.Run("callback_error_propagates", func(t *testing.T) {
		sentinel := assertCustomErr{"stop"}
		err := tuple.ReadValues(&Parser{}, func(v *Value) error {
			return sentinel
		})
		require.ErrorIs(t, err, sentinel)
	})
	t.Run("parse_failure", func(t *testing.T) {
		// Tuple with garbage.
		bad := Tuple{0xff, 0xff, 0xff}
		err := bad.ReadValues(&Parser{}, func(v *Value) error { return nil })
		require.Error(t, err)
	})
}

// TestTuple_ReadBytes_ErrorPaths covers tuple.go ReadBytes error branches.
func TestTuple_ReadBytes_ErrorPaths(t *testing.T) {
	a := &Arena{}
	var tuple Tuple
	tuple = tuple.Append(a.NewString("a"))

	t.Run("callback_error_propagates", func(t *testing.T) {
		sentinel := assertCustomErr{"rb-stop"}
		err := tuple.ReadBytes(func(b []byte) error { return sentinel })
		require.ErrorIs(t, err, sentinel)
	})
	t.Run("parse_failure", func(t *testing.T) {
		bad := Tuple{0xff, 0xff}
		err := bad.ReadBytes(func(b []byte) error { return nil })
		require.Error(t, err)
	})
}

// TestTuple_OffsetAfter covers all branches: n<=0, n in range, n beyond
// field count (clamps to len(t)), and parse failure propagation.
func TestTuple_OffsetAfter(t *testing.T) {
	a := &Arena{}
	var tuple Tuple
	tuple = tuple.Append(a.NewString("a"))
	tuple = tuple.Append(a.NewString("b"))

	t.Run("n_zero", func(t *testing.T) {
		off, err := tuple.OffsetAfter(0)
		require.NoError(t, err)
		assert.Equal(t, 0, off)
	})
	t.Run("n_negative", func(t *testing.T) {
		off, err := tuple.OffsetAfter(-1)
		require.NoError(t, err)
		assert.Equal(t, 0, off)
	})
	t.Run("n_in_range", func(t *testing.T) {
		off, err := tuple.OffsetAfter(1)
		require.NoError(t, err)
		assert.Greater(t, off, 0)
		assert.Less(t, off, len(tuple))
	})
	t.Run("n_beyond_len_clamped", func(t *testing.T) {
		off, err := tuple.OffsetAfter(99)
		require.NoError(t, err)
		assert.Equal(t, len(tuple), off, "n past the end must clamp to len(tuple)")
	})
	t.Run("parse_failure", func(t *testing.T) {
		bad := Tuple{0xff, 0xff, 0xff}
		_, err := bad.OffsetAfter(1)
		require.Error(t, err)
	})
}
