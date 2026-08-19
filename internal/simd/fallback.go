package simd

// Pure-Go distance kernels. These are the active implementations on platforms
// without a vendored SIMD kernel (wasm, 386, arm without NEON, x86 without
// AVX2). They are 4-way unrolled so the independent accumulators auto-vectorize
// on targets that can, and they measurably beat a naive single-accumulator loop
// (and beat vek's scalar fallback on no-AVX2 x86 — see vector/CROSS_HARDWARE.md).

// dotScalar is a 4-accumulator inner product.
func dotScalar(a, b []float32) float32 {
	var s0, s1, s2, s3 float32
	n := len(a)
	i := 0
	for ; i+4 <= n; i += 4 {
		s0 += a[i] * b[i]
		s1 += a[i+1] * b[i+1]
		s2 += a[i+2] * b[i+2]
		s3 += a[i+3] * b[i+3]
	}
	sum := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		sum += a[i] * b[i]
	}
	return sum
}

// dotFloatByteScalar is the pure-Go float×byte inner product (unsigned bytes).
func dotFloatByteScalar(q []float32, u []byte) float32 {
	var s0, s1, s2, s3 float32
	n := len(q)
	i := 0
	for ; i+4 <= n; i += 4 {
		s0 += q[i] * float32(u[i])
		s1 += q[i+1] * float32(u[i+1])
		s2 += q[i+2] * float32(u[i+2])
		s3 += q[i+3] * float32(u[i+3])
	}
	sum := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		sum += q[i] * float32(u[i])
	}
	return sum
}

// l2Scalar is a 4-accumulator SQUARED Euclidean distance.
func l2Scalar(a, b []float32) float32 {
	var s0, s1, s2, s3 float32
	n := len(a)
	i := 0
	for ; i+4 <= n; i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		s0 += d0 * d0
		s1 += d1 * d1
		s2 += d2 * d2
		s3 += d3 * d3
	}
	sum := s0 + s1 + s2 + s3
	for ; i < n; i++ {
		d := a[i] - b[i]
		sum += d * d
	}
	return sum
}
