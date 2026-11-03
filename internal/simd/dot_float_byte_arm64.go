//go:build !noasm && arm64

package simd

import "unsafe"

// dotFloatByteNEON is the arm64 float×byte kernel (unsigned bytes), owned by
// any-store rather than vendored: see dot_float_byte_arm64.s for why. The
// length is len(q) and extra bytes in u are ignored. It reads len(q) bytes from
// u unchecked: DotFloatByte panics on a shorter u before calling it.
func dotFloatByteNEON(q []float32, u []byte) float32 {
	return dotFloatByteNEONAsm(unsafe.SliceData(q), unsafe.SliceData(u), len(q))
}

// dotFloatByteNEONAsm returns Σ q[i]·float32(u[i]) for i < n.
//
//go:noescape
func dotFloatByteNEONAsm(q *float32, u *byte, n int) float32
