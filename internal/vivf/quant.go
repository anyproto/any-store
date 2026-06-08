package vivf

import (
	"encoding/binary"
	"math"
)

// The re-rank store (:vec) can hold int8-quantized vectors instead of raw f32 —
// ~4× smaller on disk and in the page cache, which is the win on the random
// per-candidate re-rank reads. The format mirrors internal/vindex/quant.go:
// [scale float32 LE][dim int8], a per-vector symmetric scalar quantization. The
// query stays full precision; only the stored side is quantized (~0.5% recall).

func int8RecordSize(dim int) int { return 4 + dim }

// encodeVecInt8 appends [scale][dim int8] for v to dst (reusing dst) and returns it.
func encodeVecInt8(dst []byte, v []float32) []byte {
	var maxAbs float32
	for _, x := range v {
		if x < 0 {
			x = -x
		}
		if x > maxAbs {
			maxAbs = x
		}
	}
	dst = dst[:0]
	var sb [4]byte
	scale := maxAbs / 127
	binary.LittleEndian.PutUint32(sb[:], math.Float32bits(scale))
	dst = append(dst, sb[:]...)
	if maxAbs == 0 {
		return append(dst, make([]byte, len(v))...) // zero vector
	}
	inv := 127 / maxAbs
	for _, x := range v {
		qi := int32(math.Round(float64(x * inv)))
		if qi > 127 {
			qi = 127
		} else if qi < -127 {
			qi = -127
		}
		dst = append(dst, byte(int8(qi)))
	}
	return dst
}

// decodeVecInt8 dequantizes a [scale][dim int8] record into dst (length dim).
func decodeVecInt8(data []byte, dim int, dst []float32) []float32 {
	scale := math.Float32frombits(binary.LittleEndian.Uint32(data[:4]))
	qb := data[4:]
	for i := 0; i < dim; i++ {
		dst[i] = float32(int8(qb[i])) * scale
	}
	return dst
}
