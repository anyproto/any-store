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

	// Full-text cost weights — calibrated against the 50k-doc fts_restrict
	// benchmarks (≈0.9 µs per cost unit, anchored by CostDocFetch ≈ 2.7 µs
	// for a 4 KB fetch+parse).
	//
	// CostFtsPosting prices one posting visited by the driver scan: chunk
	// decode + BM25 + amortized rank sort (~340 ns measured).
	CostFtsPosting = 0.4
	// CostFtsProbeTerm prices one per-term postings-chunk point-get during a
	// document probe; CostFtsProbeDoc is the per-document overhead (docmap +
	// docinfo point-gets). A full probe of one doc for a q-term query costs
	// CostFtsProbeDoc + q×CostFtsProbeTerm (~1–3 µs measured for one term).
	CostFtsProbeTerm = 1.5
	CostFtsProbeDoc  = 1.0

	// Vector cost weights. Exact per-candidate scoring reads the vector
	// zero-copy from the already-parsed document, so it costs a dim-scaled
	// SIMD kernel on top of the fetch (256d ≈ 0.2 units). The ANN driver's
	// per-ef-candidate cost covers graph/list traversal + rerank, amortized;
	// backends override it via VectorQuerySpec.SearchCostPerCand.
	CostVecScoreBase     = 0.1
	CostVecScoreDim      = 0.0005
	CostKnnSearchPerCand = 4.0
	// CostKnnBruteDoc is the brute-force backend's per-document overhead on
	// top of the sequential read: the raw-path vector decode (~0.7 µs
	// measured on 20k docs / dim 64).
	CostKnnBruteDoc = 0.8

	// Fallback Heuristics
	DefaultRangeSelectivity = 0.5 // Default assumption: a range predicate matches ~50% of the collection

	// Default sketch size (number of buckets)
	DefaultSketchSize = 1024
)
