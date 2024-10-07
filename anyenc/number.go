package anyenc

import (
	"encoding/binary"
	"math"

	"golang.org/x/exp/constraints"
)

// AppendFloat64 encodes float64 as bytes that will be correctly comparable with byte.Compare
func AppendFloat64(b []byte, f float64) []byte {
	bits := math.Float64bits(f)
	n := len(b)
	bytes := binary.BigEndian.AppendUint64(b, bits)
	// handle negative numbers
	if bits&(1<<63) != 0 {
		// Flip all bits for negative numbers
		for i := n; i < len(bytes); i++ {
			bytes[i] = ^bytes[i]
		}
	} else {
		// flip only the sign bit for positive numbers
		bytes[n] ^= 1 << 7
	}
	return bytes
}

func AppendNumber[T constraints.Integer | constraints.Float](b []byte, n T) []byte {
	return AppendFloat64(b, float64(n))
}

// BytesToFloat64 decodes float64 from bytes encoded with AppendFloat64
func BytesToFloat64(b []byte) float64 {
	_ = b[7] // bounds check
	// handle negative numbers
	if b[0]&(1<<7) == 0 {
		// flip all bits for negative numbers
		var cp [8]byte
		for i := range b[:8] {
			cp[i] = ^b[i]
		}
		return math.Float64frombits(binary.BigEndian.Uint64(cp[:]))
	}
	return -math.Float64frombits(binary.BigEndian.Uint64(b))
}
