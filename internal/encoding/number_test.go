package encoding

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

type floats struct {
	a, b float64
}

var testData = []floats{
	{1, -1},
	{1.11112, 1.1111111},
	{36.6, 8.8},
	{0, -0.00000001},
	{0.00000001, 0},
}

func TestAppendFloat64ToBytes(t *testing.T) {
	for _, f := range testData {
		assert.Equal(t, 1, bytes.Compare(AppendFloat64(nil, f.a), AppendFloat64(nil, f.b)))
	}
}

func TestBytesToFloat64(t *testing.T) {
	for _, f := range testData {
		assert.Equal(t, f.a, BytesToFloat64(AppendFloat64(nil, f.a)))
		assert.Equal(t, f.b, BytesToFloat64(AppendFloat64(nil, f.b)))
	}
}

func BenchmarkAppendFloat64(b *testing.B) {
	b.Run("pos", func(b *testing.B) {
		var buf = make([]byte, 0, 8)
		for i := 0; i < b.N; i++ {
			_ = AppendFloat64(buf, 4.44444444444)
			buf = buf[:0]
		}
	})
	b.Run("neg", func(b *testing.B) {
		var buf = make([]byte, 0, 8)
		for i := 0; i < b.N; i++ {
			_ = AppendFloat64(buf, -4.44444444444)
			buf = buf[:0]
		}
	})

}
