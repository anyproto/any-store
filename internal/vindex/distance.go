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
	"math"

	"github.com/viterin/vek/vek32"
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

// SIMD reports whether vek selected a vectorised kernel (amd64 + AVX2). False on
// arm64 and non-AVX2 x86 — see vector/ARM.md and vector/CROSS_HARDWARE.md.
func SIMD() bool { return vek32.Info().Acceleration }

var simdActive = SIMD()

// distanceFor returns the best distance function for the metric on this CPU.
// When SIMD is unavailable, L2 uses the hand-unrolled kernel (measured faster
// than vek's scalar fallback on a no-AVX2 CPU).
func distanceFor(m Metric) DistanceFunc {
	switch m {
	case Cosine:
		return cosineDistance
	case Dot:
		return dotDistance
	default:
		if simdActive {
			return l2SIMD
		}
		return l2Unrolled
	}
}

func l2SIMD(a, b []float32) float32         { return vek32.Distance(a, b) }
func cosineDistance(a, b []float32) float32 { return 1 - vek32.CosineSimilarity(a, b) }
func dotDistance(a, b []float32) float32    { return -vek32.Dot(a, b) }

// l2Unrolled is a 4-accumulator Euclidean distance for CPUs without SIMD.
func l2Unrolled(a, b []float32) float32 {
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
