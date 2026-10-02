package qplanner

const (
	// Query Cost Weights. CostDocFetch is anchored at ~2.7 µs for a 4 KB
	// fetch+parse on a 50K-doc collection; the full-text and vector weights
	// below are calibrated against it. CostDocFetch is 2.4× CostScanDoc
	// (measured 1–3× by document size, see there); CostSeqRead prices one
	// sequential cursor step over an index entry (the brute-force vector scan
	// adds its own per-document decode on top).
	CostIndexSeek = 0.5  // Cost of a B-tree traversal to find a key (per bound/seek)
	CostDocFetch  = 3.0  // Cost of a random point lookup in the data B-tree (index -> data)
	CostSeqRead   = 0.1  // Cost of a sequential cursor step (index entry)
	CostFilter    = 0.5  // Cost of in-memory evaluation of a predicate
	CostSortSwap  = 0.25 // Cost of in-memory sort (includes re-fetch overhead after sorting)

	// CostScanDoc is the per-document cost of a filtered full scan's read: the
	// sequential page walk plus decompressing and decoding the document far
	// enough to reject it. The work grows with document size, so it sits far
	// above CostSeqRead: measured against the index path on the same data, a
	// random fetch costs about 3× a scanned document at ~700 B, 1.8× at 1 KB
	// with twenty fields and about 1× at 4 KB, so the index keeps winning up to
	// 35–100% selectivity. The value puts the break-even at 50%
	// (CostScanDoc + CostFilter = (CostDocFetch + CostFilter) / 2) and errs
	// toward the index on purpose: a full scan chosen for a range that turns
	// out 5%-selective costs 10–20×, an index chosen for a 50% range costs
	// 1.3–1.6× end to end (700 B down to 80 B documents) and nothing at 1 KB
	// and above — the asymmetry SQLite prices with its 3× full-table-scan
	// penalty.
	CostScanDoc = 1.25

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

	// DefaultRangeSelectivity prices a predicate the planner cannot measure:
	// SQLite's factor for one inequality bound (each bound quarters the search
	// space). B-tree interpolation rates every range over a live index, so
	// this is reached only where nothing measures the predicate — no read tx
	// or an interpolation read error, an equality on a non-leading compound
	// field or without a trusted sketch level, a covering filter with no
	// per-field sketch, and predicates on unindexed fields. Under the
	// CostScanDoc break-even it sends an unmeasured range to the index.
	DefaultRangeSelectivity = 0.25

	// Default sketch size (number of buckets)
	DefaultSketchSize = 1024
)
