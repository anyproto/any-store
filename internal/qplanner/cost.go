package qplanner

const (
	// Query Cost Weights
	CostIndexSeek = 0.5 // Cost of a B-tree traversal to find a key (per bound/seek)
	CostDocFetch  = 2.0 // Cost of a random point lookup in the data B-tree (index -> data)
	CostSeqRead   = 0.1 // Cost of a sequential cursor read per doc (full scan)
	CostFilter    = 0.5 // Cost of in-memory evaluation of a predicate
	CostSortSwap  = 0.5 // Cost of an in-memory swap operation for sorting

	// Fallback Heuristics
	DefaultRangeSelectivity = 0.5 // Default assumption: a range query matches 50% of the collection

	// Default sketch size (number of buckets)
	DefaultSketchSize = 1024
)
