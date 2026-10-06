package vivf

import (
	"github.com/anyproto/any-store/v2/internal/simd"
)

// normalize returns a unit-length copy of v (zero vector copied through).
func normalize(v []float32) []float32 {
	return normalizeInto(nil, v)
}

// normalizeInto writes the unit-normalized v into dst (reusing dst's capacity) and
// returns it — the alloc-free form for the pooled search path.
func normalizeInto(dst, v []float32) []float32 {
	return simd.NormalizeInto(dst, v)
}

// sqNorm is the squared L2 norm of v (SIMD dot with itself).
func sqNorm(v []float32) float32 {
	return simd.Dot(v, v)
}

func cmpF32(a, b float32) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
