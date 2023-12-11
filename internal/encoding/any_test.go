package encoding

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
	}

	for _, v := range values {
		b := AppendAnyValue(nil, v)
		res, err := DecodeToAny(b)
		require.NoError(t, err)
		assert.EqualValues(t, v, res)
	}
}
