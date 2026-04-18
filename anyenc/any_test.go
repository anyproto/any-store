package anyenc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendAnyValue(t *testing.T) {
	var values = []any{
		"string",
		nil,
		false,
		true,
		uint8(1),
		uint16(2),
		uint32(3),
		uint64(4),
		uint(5),
		int(6),
		int8(7),
		int16(8),
		int32(9),
		int64(10),
		float32(32.32),
		float64(64.64),
		MustParseJson(`{"test":"value"}`),
	}

	for _, v := range values {
		b := AppendAnyValue(nil, v)
		_, err := Parse(b)
		require.NoError(t, err)
	}
}

// TestAppendAnyValue_BytesAndPanic covers the `case []byte` arm at any.go:18-21
// and the `default: panic(...)` arm at any.go:67 for unsupported types.
func TestAppendAnyValue_BytesAndPanic(t *testing.T) {
	// []byte takes the TypeString encoding.
	b := AppendAnyValue(nil, []byte("hi"))
	assert.Equal(t, byte(TypeString), b[0])
	// EOS terminator at the end.
	assert.Equal(t, EOS, b[len(b)-1])

	// Round-trip via MustParse: the emitted bytes must parse back to a
	// string value whose StringBytes is "hi" — proves the []byte arm
	// produced a well-formed TypeString encoding, not just tagged bytes.
	v := MustParse(b)
	sb, err := v.StringBytes()
	require.NoError(t, err)
	assert.Equal(t, []byte("hi"), sb)

	// Unsupported type → panic.
	assert.Panics(t, func() {
		_ = AppendAnyValue(nil, struct{ X int }{X: 1})
	})
}
