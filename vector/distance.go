// Package vector is an experimental vector-search PROTOTYPE for any-store.
//
// Deprecated: this package is NOT the live vector engine and is not imported by
// any-store itself (only by cmd/vectorbench). The production vector index is
// internal/vindex (a btree-resident HNSW). This package is retained only as a
// benchmarking/recall reference and may be removed.
//
// It explores a few ways to land approximate nearest-neighbour (ANN) search on
// top of the embedded btree engine, using github.com/coder/hnsw as the
// algorithmic reference. Three index families are provided:
//
//   - Brute: exact flat scan (ground truth / recall baseline).
//   - HNSW:  map-based in-memory graph, a faithful port of coder/hnsw.
//   - FlatHNSW: an arena/SoA (struct-of-arrays) HNSW that keeps every vector in
//     one contiguous []float32 slab and every adjacency list in flat []uint32
//     slices — no per-node maps, no per-vector allocations. This is the variant
//     wired into the btree for persistence (see BtreeHNSW).
//
// Distance is computed with SIMD (AVX2/AVX-512 via github.com/viterin/vek,
// pure-Go assembly, no CGO — the same library coder/hnsw uses) with scalar and
// hand-unrolled fallbacks kept around purely so the benchmarks can quantify the
// SIMD win.
package vector

import (
	"math"

	"github.com/anyproto/any-store/v2/internal/simd"
)

// Metric selects how the distance between two vectors is measured.
type Metric uint8

const (
	// L2 is the (squared-free) Euclidean distance. Smaller is closer.
	L2 Metric = iota
	// Cosine is 1 - cosine-similarity. Smaller is closer. Inputs need not be
	// normalised; the metric normalises internally.
	Cosine
	// Dot is the negated inner product (so that "smaller is closer" holds for
	// all metrics). Use with pre-normalised vectors for cosine-equivalent
	// ranking at lower cost.
	Dot
)

// DistanceFunc returns the distance between two equal-length vectors.
// Smaller means closer for every metric exposed here.
type DistanceFunc func(a, b []float32) float32

// SIMD reports whether a hand-written SIMD kernel was selected for this CPU.
// When false, the simd package transparently uses a pure-Go fallback.
func SIMD() bool { return simd.Accelerated() }

// DistanceFor returns the best available distance function for the metric on
// this CPU. The simd package selects a SIMD or pure-Go kernel internally (the
// pure-Go L2 is the hand-unrolled loop that beats vek's old fallback on no-AVX2
// x86), so there is nothing CPU-specific to branch on here.
func (m Metric) DistanceFor() DistanceFunc {
	switch m {
	case Cosine:
		return CosineDistanceSIMD
	case Dot:
		return DotDistanceSIMD
	default:
		return L2DistanceSIMD
	}
}

func (m Metric) String() string {
	switch m {
	case L2:
		return "l2"
	case Cosine:
		return "cosine"
	case Dot:
		return "dot"
	default:
		return "unknown"
	}
}

// ---------------------------------------------------------------------------
// SIMD implementations (vek32 -> AVX2/AVX-512 assembly, no CGO).
// ---------------------------------------------------------------------------

// L2DistanceSIMD returns the Euclidean distance using vectorised kernels.
func L2DistanceSIMD(a, b []float32) float32 {
	return simd.Distance(a, b)
}

// CosineDistanceSIMD mirrors coder/hnsw's CosineDistance: 1 - cosineSimilarity.
func CosineDistanceSIMD(a, b []float32) float32 {
	return 1 - simd.CosineSimilarity(a, b)
}

// DotDistanceSIMD returns the negated inner product (smaller = closer).
func DotDistanceSIMD(a, b []float32) float32 {
	return -simd.Dot(a, b)
}

// ---------------------------------------------------------------------------
// Scalar reference implementations. Not used in production paths; kept so the
// benchmark can show what SIMD buys us, and so tests can cross-check the
// vectorised kernels against a trivially-correct version.
// ---------------------------------------------------------------------------

// L2DistanceScalar is a naive Euclidean distance.
func L2DistanceScalar(a, b []float32) float32 {
	var sum float32
	for i := range a {
		d := a[i] - b[i]
		sum += d * d
	}
	return float32(math.Sqrt(float64(sum)))
}

// L2DistanceUnrolled is a 4-way unrolled Euclidean distance. The compiler can
// auto-vectorise the independent accumulators on some targets; this sits
// between the naive scalar and the explicit-SIMD kernels.
func L2DistanceUnrolled(a, b []float32) float32 {
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
	return float32(math.Sqrt(float64(sum)))
}

// CosineDistanceScalar is a naive cosine distance.
func CosineDistanceScalar(a, b []float32) float32 {
	var dot, na, nb float32
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 1
	}
	return 1 - dot/float32(math.Sqrt(float64(na))*math.Sqrt(float64(nb)))
}

// DotDistanceScalar is a naive negated inner product.
func DotDistanceScalar(a, b []float32) float32 {
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return -dot
}
