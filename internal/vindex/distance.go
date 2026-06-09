// Package vindex implements a btree-resident HNSW vector index for any-store.
//
// Unlike the experimental top-level vector package (which keeps an in-memory
// arena), this index owns NO graph state: the HNSW graph lives entirely in btree
// namespaces and every operation runs against a transaction. That is what makes
// it correct across processes — a reader sees a consistent MVCC snapshot of the
// graph, a writer's mutations commit atomically with the document, and there is
// no per-process in-memory layer to invalidate or sync.
package vindex

import (
	"github.com/anyproto/any-store/v2/internal/simd"
)

// Metric selects the distance measure. Smaller is closer for all of them.
type Metric uint8

const (
	L2 Metric = iota
	Cosine
	Dot
)

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

// DistanceFunc returns the distance between two equal-length vectors.
type DistanceFunc func(a, b []float32) float32

// DistanceFor returns the best distance function for the metric on this CPU —
// the same kernel the index uses, so brute-force search ranks identically to ANN.
func DistanceFor(m Metric) DistanceFunc { return distanceFor(m) }

// SIMD reports whether a hand-written SIMD kernel was selected for this CPU.
// False on arm without NEON, x86 without AVX2, and wasm — see vector/ARM.md and
// vector/CROSS_HARDWARE.md.
func SIMD() bool { return simd.Accelerated() }

// distanceFor returns the best distance function for the metric. The L2/Dot/
// Cosine kernels are selected once inside the simd package (SIMD or, on CPUs
// without it, a pure-Go fallback), so there is nothing CPU-specific to branch on
// here.
func distanceFor(m Metric) DistanceFunc {
	switch m {
	case Cosine:
		return cosineDistance
	case Dot:
		return dotDistance
	default:
		return simd.Distance
	}
}

func cosineDistance(a, b []float32) float32 { return 1 - simd.CosineSimilarity(a, b) }
func dotDistance(a, b []float32) float32    { return -simd.Dot(a, b) }

// cosineDotDistance is the cosine kernel for an index that stores unit-normalized
// vectors (and normalizes queries): with both operands unit length, cosine
// similarity is just their dot product, so this returns the SAME value as
// cosineDistance (1 - cos) but via the cheaper Dot kernel (no per-call norms).
func cosineDotDistance(a, b []float32) float32 { return 1 - simd.Dot(a, b) }

// normalizeInto writes the unit-normalized v into dst (reusing dst's capacity)
// and returns it. Thin wrapper over simd.NormalizeInto kept for call-site brevity.
func normalizeInto(dst, v []float32) []float32 {
	return simd.NormalizeInto(dst, v)
}
