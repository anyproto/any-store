// Package simd is any-store's single home for vector distance math.
//
// It wraps the vendored, hand-written SIMD kernels in internal/simd/asm
// (dot-product and L2 for amd64 AVX2/AVX512 and arm64 NEON/SVE, float32 and
// int8) behind a small, portable API. Runtime CPU dispatch happens once in an
// arch-specific init (dispatch_*.go); when no SIMD kernel is available — wasm,
// 386, or an x86 without AVX2 (e.g. pre-Haswell Ivy Bridge) — the pure-Go
// fallbacks in fallback.go are used, which beat a naive scalar loop.
//
// This package replaces the former github.com/viterin/vek/vek32 dependency,
// which was amd64+AVX2-only and fell back to slow portable Go on arm64.
package simd

import "math"

// dotImpl and l2Impl are the active kernels, selected once per process by the
// arch-specific init(). l2Impl returns the SQUARED L2 distance.
var (
	dotImpl     = dotScalar
	l2Impl      = l2Scalar
	dotFBImpl   = dotFloatByteScalar
	accelerated bool
	// acceleratedFB reports whether a SIMD float×byte kernel (not the fallback)
	// is active. Separate from accelerated because not every arch that has a
	// float32 kernel also has the byte kernel wired here.
	acceleratedFB bool
)

// DotFloatByte returns the inner product of a float32 vector q and a byte vector
// u, where each byte is interpreted as an UNSIGNED value 0..255 (matching the
// vendored weaviate kernels). Callers using offset-binary int8 storage recover
// the signed dot via scale·(DotFloatByte(q,u) − 128·Σq); see internal/vindex.
func DotFloatByte(q []float32, u []byte) float32 { return dotFBImpl(q, u) }

// AcceleratedFloatByte reports whether a SIMD float×byte kernel is active (false
// on the pure-Go fallback path, and on arches where it is not wired).
func AcceleratedFloatByte() bool { return acceleratedFB }

// Accelerated reports whether a hand-written SIMD kernel (not the pure-Go
// fallback) was selected for this CPU. Mirrors the old vek32.Info().Acceleration.
func Accelerated() bool { return accelerated }

// Dot returns the inner product of two equal-length vectors.
func Dot(a, b []float32) float32 { return dotImpl(a, b) }

// L2Squared returns the squared Euclidean distance (no sqrt).
func L2Squared(a, b []float32) float32 { return l2Impl(a, b) }

// Distance returns the Euclidean distance. Kept as sqrt(L2Squared) so the
// returned/stored distance values stay identical to the previous vek32.Distance.
func Distance(a, b []float32) float32 {
	return float32(math.Sqrt(float64(l2Impl(a, b))))
}

// Norm returns the L2 norm (length) of v.
func Norm(v []float32) float32 {
	return float32(math.Sqrt(float64(dotImpl(v, v))))
}

// CosineSimilarity returns the cosine similarity of a and b. For a zero-length
// operand it returns 0 (so a cosine distance of 1-sim is maximal, matching the
// previous behavior for degenerate vectors).
func CosineSimilarity(a, b []float32) float32 {
	na := dotImpl(a, a)
	nb := dotImpl(b, b)
	if na == 0 || nb == 0 {
		return 0
	}
	return dotImpl(a, b) / float32(math.Sqrt(float64(na))*math.Sqrt(float64(nb)))
}

// MulNumber_Into writes dst = v * s and returns dst (which must have len(v)).
// Drop-in for vek32.MulNumber_Into. Cold path (per-vector normalize), so a plain
// loop the compiler can auto-vectorize is enough.
func MulNumber_Into(dst, v []float32, s float32) []float32 {
	for i := range v {
		dst[i] = v[i] * s
	}
	return dst
}

// Sub_Into writes dst = a - b. Drop-in for vek32.Sub_Into.
func Sub_Into(dst, a, b []float32) {
	for i := range a {
		dst[i] = a[i] - b[i]
	}
}

// NormalizeInto writes the unit-normalized v into dst (reusing dst's capacity)
// and returns it. A zero vector is copied through unchanged (norm 0 -> leave as
// zeros). This is the single shared copy of the per-package normalizeInto that
// previously lived in both internal/vindex and internal/vivf.
func NormalizeInto(dst, v []float32) []float32 {
	if cap(dst) < len(v) {
		dst = make([]float32, len(v))
	} else {
		dst = dst[:len(v)]
	}
	n := Norm(v)
	if n == 0 {
		copy(dst, v)
		return dst
	}
	return MulNumber_Into(dst, v, 1/n)
}
