package vindex

import (
	"encoding/binary"
	"math"

	"github.com/anyproto/any-store/v2/internal/vecf"
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

// int8Bias is the offset-binary bias (see vecf.Int8Bias for the rationale).
// The format has no on-disk back-compat constraint yet.
const int8Bias = vecf.Int8Bias

// encodeVec appends v's storage bytes to buf (reusing it) and returns the slice.
// For QuantNone it is the raw float32 byte view; for QuantInt8 it is
// [scale float32 LE][dim uint8] using a per-vector symmetric scale and
// offset-binary components (component q stored as byte q+int8Bias).
func encodeVec(buf []byte, v []float32, q Quantization) []byte {
	if q != QuantInt8 {
		return append(buf[:0], f32bytes(v)...)
	}
	buf = append(buf[:0], 0, 0, 0, 0) // scale slot, back-patched below
	var scale float32
	buf, scale = vecf.QuantizeInt8(buf, v)
	binary.LittleEndian.PutUint32(buf[:4], math.Float32bits(scale))
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
	vecf.DequantizeInt8(dst[:dim], data[4:], scale)
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
