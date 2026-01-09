package anyenc

import (
	"testing"

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
