//go:build !noasm && arm64

package simd

import "unsafe"

// dotFloatByteNEON is the arm64 float×byte kernel (unsigned bytes), owned by
// any-store rather than vendored: see dot_float_byte_arm64.s for why. Same
// contract as the pure-Go fallback: the length is len(q), extra bytes in u are
// ignored, and a u shorter than q panics (the kernel never reads past it).
func dotFloatByteNEON(q []float32, u []byte) float32 {
	if len(u) < len(q) {
		panic("simd: DotFloatByte: len(u) < len(q)")
	}
	return dotFloatByteNEONAsm(unsafe.SliceData(q), unsafe.SliceData(u), len(q))
}

// dotFloatByteNEONAsm returns Σ q[i]·float32(u[i]) for i < n.
//
//go:noescape
func dotFloatByteNEONAsm(q *float32, u *byte, n int) float32
