// Package vecf holds the float32-vector storage helpers shared by
// internal/vindex and internal/vivf: unsafe f32<->byte reinterpretation and
// the int8 offset-binary scalar-quantization core. Byte layouts are the host
// float32 layout (amd64 and arm64 are both little-endian); a cross-endian
// build would need an explicit swap.
package vecf

import (
	"math"
	"unsafe"
)

// F32Bytes reinterprets a []float32 as its host byte view (write path).
func F32Bytes(v []float32) []byte {
	if len(v) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*4)
}

// BytesAsF32 reinterprets a byte slice (length dim*4, 4-byte aligned) as
// []float32 without copying (read path).
func BytesAsF32(b []byte, dim int) []float32 {
	if len(b) < dim*4 {
		return nil
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), dim)
}

// Int8Bias is the offset-binary bias: a quantized component q ∈ [-127,127] is
// stored as the unsigned byte q+128 ∈ [1,255]. Offset-binary (rather than
// signed two's-complement) lets the UNSIGNED SIMD float×byte kernel compute
// the signed dot via scale·(DotFloatByte(query,u) − bias·Σquery); see
// internal/simd. Adding 128 mod 256 is exactly flipping the sign bit, so
// encode is the XOR `byte(q) ^ 0x80` (cheaper and self-evidently reversible)
// and decode subtracts the bias back off — the two are inverse on the full
// byte range.
const Int8Bias = 128

// QuantizeInt8 appends v's offset-binary int8 components to dst using a
// per-vector symmetric scale (maxAbs/127) and returns dst and the scale. The
// caller writes its own record header (scale, optional norm) around the
// component bytes. A zero vector appends bias bytes (0x80, decode yields 0)
// with scale 0.
//
// QuantizeInt8Norm is the same loop additionally returning Σqᵢ² over the
// clamped components — callers storing ‖x‖² multiply it by scale². Two
// concrete variants (not a flag) so the norm-free encode path pays no
// per-component accumulation.
func QuantizeInt8(dst []byte, v []float32) (_ []byte, scale float32) {
	maxAbs := maxAbsF32(v)
	if maxAbs == 0 {
		return appendBias(dst, len(v)), 0
	}
	inv := 127 / maxAbs
	for _, x := range v {
		qi := int32(math.Round(float64(x * inv)))
		if qi > 127 {
			qi = 127
		} else if qi < -127 {
			qi = -127
		}
		dst = append(dst, byte(qi)^0x80) // offset-binary via sign-bit flip
	}
	return dst, maxAbs / 127
}

// QuantizeInt8Norm: see QuantizeInt8. A zero vector yields sumSq 0.
func QuantizeInt8Norm(dst []byte, v []float32) (_ []byte, scale, sumSq float32) {
	maxAbs := maxAbsF32(v)
	if maxAbs == 0 {
		return appendBias(dst, len(v)), 0, 0
	}
	inv := 127 / maxAbs
	for _, x := range v {
		qi := int32(math.Round(float64(x * inv)))
		if qi > 127 {
			qi = 127
		} else if qi < -127 {
			qi = -127
		}
		dst = append(dst, byte(qi)^0x80) // offset-binary via sign-bit flip
		sumSq += float32(qi) * float32(qi)
	}
	return dst, maxAbs / 127, sumSq
}

func maxAbsF32(v []float32) float32 {
	var maxAbs float32
	for _, x := range v {
		if x < 0 {
			x = -x
		}
		if x > maxAbs {
			maxAbs = x
		}
	}
	return maxAbs
}

// appendBias appends n bias bytes (component 0) for the zero vector.
func appendBias(dst []byte, n int) []byte {
	for range n {
		dst = append(dst, 0x80)
	}
	return dst
}

// DequantizeInt8 dequantizes offset-binary component bytes into dst
// (len(dst) components; comps must be at least as long).
func DequantizeInt8(dst []float32, comps []byte, scale float32) {
	for i := range dst {
		dst[i] = (float32(comps[i]) - Int8Bias) * scale
	}
}
