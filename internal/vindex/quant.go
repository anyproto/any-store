package vindex

import (
	"encoding/binary"
	"math"
)

// Quantization selects how vectors are stored in the :vec namespace. It is a
// pure storage choice: vectors are quantized on write and dequantized to
// float32 on read, so the graph, distance functions, and cross-process
// consistency are all unchanged — only the bytes on disk (and thus the page
// cache footprint) shrink.
type Quantization uint8

const (
	// QuantNone stores raw float32 vectors (dim*4 bytes), read zero-copy.
	QuantNone Quantization = iota
	// QuantInt8 stores a per-vector scale + int8 components (4+dim bytes), ~4x
	// smaller, at a small recall cost. Symmetric scalar quantization.
	QuantInt8
)

func (q Quantization) String() string {
	switch q {
	case QuantInt8:
		return "int8"
	default:
		return "none"
	}
}

// vecRecordSize is the byte size of a stored vector record for a given mode.
func vecRecordSize(dim int, q Quantization) int {
	if q == QuantInt8 {
		return 4 + dim
	}
	return dim * 4
}

// int8Bias is the offset-binary bias: a quantized component q ∈ [-127,127] is
// stored as the unsigned byte q+128 ∈ [1,255]. Offset-binary (rather than signed
// two's-complement) lets the UNSIGNED SIMD float×byte kernel compute the signed
// dot via scale·(DotFloatByte(query,u) − bias·Σquery); see internal/simd. The
// format has no on-disk back-compat constraint yet.
//
// Adding 128 mod 256 is exactly flipping the sign bit, so encode is the XOR
// `byte(q) ^ 0x80` (cheaper and self-evidently reversible) and decode subtracts
// the bias back off — the two are inverse on the full byte range.
const int8Bias = 128

// encodeVec appends v's storage bytes to buf (reusing it) and returns the slice.
// For QuantNone it is the raw float32 byte view; for QuantInt8 it is
// [scale float32 LE][dim uint8] using a per-vector symmetric scale and
// offset-binary components (component q stored as byte q+int8Bias).
func encodeVec(buf []byte, v []float32, q Quantization) []byte {
	if q != QuantInt8 {
		return append(buf[:0], f32bytes(v)...)
	}
	var maxAbs float32
	for _, x := range v {
		if x < 0 {
			x = -x
		}
		if x > maxAbs {
			maxAbs = x
		}
	}
	buf = buf[:0]
	var sb [4]byte
	scale := maxAbs / 127
	binary.LittleEndian.PutUint32(sb[:], math.Float32bits(scale))
	buf = append(buf, sb[:]...)
	if maxAbs == 0 {
		// zero vector: q=0 -> byte 0x80 (decode yields 0; scale is 0 anyway).
		for range v {
			buf = append(buf, 0x80)
		}
		return buf
	}
	inv := 127 / maxAbs
	for _, x := range v {
		qi := int32(math.Round(float64(x * inv)))
		if qi > 127 {
			qi = 127
		} else if qi < -127 {
			qi = -127
		}
		buf = append(buf, byte(qi)^0x80) // offset-binary via sign-bit flip
	}
	return buf
}

// decodeVecInto turns a stored vector record into a []float32. For QuantNone it
// returns the zero-copy reinterpretation of data (dst unused); for QuantInt8 it
// dequantizes into dst (which must have length dim) and returns it.
func decodeVecInto(data []byte, dim int, q Quantization, dst []float32) ([]float32, bool) {
	if q != QuantInt8 {
		f := bytesAsF32(data, dim)
		return f, false // not written into dst; caller keeps zero-copy semantics
	}
	if len(data) < 4+dim {
		return nil, false
	}
	scale := math.Float32frombits(binary.LittleEndian.Uint32(data[:4]))
	qb := data[4:]
	for i := 0; i < dim; i++ {
		dst[i] = (float32(qb[i]) - int8Bias) * scale
	}
	return dst, true
}

// int8ScaleBytes splits a QuantInt8 record into its scale and the offset-binary
// component bytes (length dim). Used by the byte-kernel distance path, which
// reads the stored bytes directly instead of dequantizing to float32.
func int8ScaleBytes(data []byte, dim int) (scale float32, comps []byte, ok bool) {
	if len(data) < 4+dim {
		return 0, nil, false
	}
	scale = math.Float32frombits(binary.LittleEndian.Uint32(data[:4]))
	return scale, data[4 : 4+dim], true
}
