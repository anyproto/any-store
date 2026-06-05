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

// encodeVec appends v's storage bytes to buf (reusing it) and returns the slice.
// For QuantNone it is the raw float32 byte view; for QuantInt8 it is
// [scale float32 LE][dim int8] using a per-vector symmetric scale.
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
		return append(buf, make([]byte, len(v))...) // zero vector
	}
	inv := 127 / maxAbs
	for _, x := range v {
		qi := int32(math.Round(float64(x * inv)))
		if qi > 127 {
			qi = 127
		} else if qi < -127 {
			qi = -127
		}
		buf = append(buf, byte(int8(qi)))
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
		dst[i] = float32(int8(qb[i])) * scale
	}
	return dst, true
}
