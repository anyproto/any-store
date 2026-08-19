package qplanner

const (
	// Query Cost Weights — calibrated against 50K-doc benchmarks.
	// Ratio of random fetch to sequential read (~8x) matches measured performance.
	CostIndexSeek = 0.5  // Cost of a B-tree traversal to find a key (per bound/seek)
	CostDocFetch  = 3.0  // Cost of a random point lookup in the data B-tree (index -> data)
	CostSeqRead   = 0.1  // Cost of a sequential cursor read per doc (full scan)
	CostFilter    = 0.5  // Cost of in-memory evaluation of a predicate
	CostSortSwap  = 0.25 // Cost of in-memory sort (includes re-fetch overhead after sorting)

	// CostMaterialize is the per-document cost of buffering a row into the slice a
	// full-scan sort must build before it can order anything. It models what the
	// n*log2(n)*CostSortSwap term does not: Go heap allocation, GC pressure and
	// pointer chasing for materializing the whole result set. It is the linear
	// counterpart that lets an order-providing index scan (which streams, and can
	// early-terminate under LIMIT) win when it should — WITHOUT a multiplicative
	// discount on the scan, which would distort the random-fetch math that keeps a
	// poorly-selective filter from scanning most of an index. Applied only to the
	// full-scan sort path.
	CostMaterialize = 1.0

	// Fallback Heuristics
	DefaultRangeSelectivity = 0.5 // Default assumption: a range predicate matches ~50% of the collection

	// Default sketch size (number of buckets)
	DefaultSketchSize = 1024
)
