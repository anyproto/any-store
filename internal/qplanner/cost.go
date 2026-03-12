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
	DefaultRangeSelectivity = 0.5 // Default assumption: a range predicate matches ~50% of the collection

	// Default sketch size (number of buckets)
	DefaultSketchSize = 1024
)
