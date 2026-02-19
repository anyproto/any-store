package qplanner

const (
	// Query Cost Weights
	CostIndexSeek = 0.0 // Cost of a B-tree traversal to find a key (negligible for embedded DB)
	CostDocFetch  = 2.0  // Cost of a point lookup in the main collection B-tree
	CostFilter    = 0.5  // Cost of in-memory evaluation of a predicate
	CostSortSwap  = 0.5  // Cost of an in-memory swap operation for sorting

	// Fallback Heuristics
	DefaultRangeSelectivity = 0.5 // Default assumption: a range query matches 50% of the collection

	// Default sketch size (number of buckets)
	DefaultSketchSize = 1024
)
