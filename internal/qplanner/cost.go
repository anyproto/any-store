package qplanner

const (
	// Query Cost Weights — calibrated against 50K-doc benchmarks.
	// Ratio of random fetch to sequential read (~8x) matches measured performance.
	CostIndexSeek = 0.5  // Cost of a B-tree traversal to find a key (per bound/seek)
	CostDocFetch  = 3.0  // Cost of a random point lookup in the data B-tree (index -> data)
	CostSeqRead   = 0.1  // Cost of a sequential cursor read per doc (full scan)
	CostFilter    = 0.5  // Cost of in-memory evaluation of a predicate
	CostSortSwap  = 0.25 // Cost of in-memory sort (includes re-fetch overhead after sorting)

	// Fallback Heuristics
	DefaultRangeSelectivity = 0.5 // Default assumption: a RANGE predicate, or an equality on a NON-indexed field, matches ~50% of the collection

	// MinIndexedEqualitySelectivity is the floor for an equality predicate on an
	// INDEXED field whose per-value frequency cannot be read directly from a
	// sketch (e.g. the leading column of a compound index, before a leading-prefix
	// sketch is available). It is derived from the index's distinct-value estimate
	// (≈ 1/distinct) and clamped into [MinIndexedEqualitySelectivity, DefaultRangeSelectivity]:
	// the upper clamp guarantees the estimate is never LESS selective than the old
	// blind 0.5 default (so no previously-correct plan regresses), and the floor
	// stops a saturated/very-high-cardinality index from estimating zero rows.
	MinIndexedEqualitySelectivity = 1e-4

	// Default sketch size (number of buckets)
	DefaultSketchSize = 1024
)
