package anystore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/internal/vindex"
)

// IndexKind selects the kind of index.
type IndexKind uint8

const (
	// IndexKindRange is the default B-tree range/equality index.
	IndexKindRange IndexKind = iota
	// IndexKindVector is an approximate-nearest-neighbour index over a vector
	// (embedding) field. Queried via Find() with a
	// `{field: {"$knn": {"$query": [...], "$k": N}}}` clause; results carry a
	// synthetic _distance field (see docs/vector-search.md).
	IndexKindVector
	// IndexKindFulltext is an inverted full-text index over one or more text
	// fields, queried with the $text operator and ranked by BM25. See
	// docs/fts/DESIGN.md.
	IndexKindFulltext
)

// VectorMetric selects the distance measure for a vector index.
type VectorMetric uint8

const (
	VectorCosine VectorMetric = iota
	VectorL2
	VectorDot
)

func (m VectorMetric) toVindex() vindex.Metric {
	switch m {
	case VectorL2:
		return vindex.L2
	case VectorDot:
		return vindex.Dot
	default:
		return vindex.Cosine
	}
}

// VectorQuantization selects how the index stores vectors.
type VectorQuantization uint8

const (
	// VectorQuantNone stores full float32 vectors (default).
	VectorQuantNone VectorQuantization = iota
	// VectorQuantInt8 stores scalar-quantized int8 vectors (~4x less storage /
	// page-cache RAM) at a small recall cost.
	VectorQuantInt8
)

func (q VectorQuantization) toVindex() vindex.Quantization {
	if q == VectorQuantInt8 {
		return vindex.QuantInt8
	}
	return vindex.QuantNone
}

// VectorMode selects the index strategy for a vector index.
type VectorMode uint8

const (
	// VectorModeBTree stores the full HNSW graph in the btree (default): lowest
	// RAM, multiprocess-safe writes, approximate search.
	VectorModeBTree VectorMode = iota
	// VectorModeHybrid is VectorModeBTree plus a RAM-resident copy of HNSW layer 0
	// for faster search; the btree remains the source of truth.
	VectorModeHybrid
	// VectorModeBruteForce stores no index data — the index is only a declaration
	// that a field is a vector. Search scans the collection and computes exact
	// distances: ~free writes, ~0 storage, exact (100%) recall, O(N) search.
	VectorModeBruteForce
	// VectorModeIVFPQ is a btree-resident IVF-PQ index (see
	// vector/RESEARCH_IVFPQ_BTREE.md): vectors are partitioned into nlist coarse
	// cells and each is stored as a compact product-quantization code of its
	// residual. Search probes a few cells (contiguous btree range scans — no random
	// graph traversal) and re-ranks the survivors by exact distance. Far smaller hot
	// RAM set (only the coarse centroids) and a sequential read pattern, at the cost
	// of approximate recall recovered by re-rank. See VectorParams NList/NProbe/Closure.
	VectorModeIVFPQ
	// VectorModeIVFSQ is the same IVF partition but stores each vector as an int8
	// scalar-quantized full vector per cell (no product quantization), scanned
	// directly with exact int8 distance — no ADC table, no precomputed table, no
	// re-rank. Versus IVFPQ: much faster builds (no PQ training), higher recall (int8
	// is more faithful than PQ), smaller (no separate re-rank store), and no
	// precompute-table RAM — at higher search cost (it reads full vectors, not codes).
	// Favours Closure=1 with a higher NProbe (replicating full vectors is costly).
	VectorModeIVFSQ
)

func (m VectorMode) String() string {
	switch m {
	case VectorModeHybrid:
		return "hybrid"
	case VectorModeBruteForce:
		return "brute"
	case VectorModeIVFPQ:
		return "ivfpq"
	case VectorModeIVFSQ:
		return "ivfsq"
	default:
		return "btree"
	}
}

// isIVF reports whether this mode is an IVF index (PQ or SQ) — both share the
// internal/vivf backend.
func (m VectorMode) isIVF() bool { return m == VectorModeIVFPQ || m == VectorModeIVFSQ }

// isBruteForce reports whether this mode keeps no on-disk index structure.
func (m VectorMode) isBruteForce() bool { return m == VectorModeBruteForce }

// VectorParams configures a vector (HNSW) index.
type VectorParams struct {
	// Field is the path to the embedding field (an array of numbers).
	Field string `json:"field"`
	// Dim is the embedding dimension.
	Dim int `json:"dim"`
	// Metric is the distance measure (default cosine).
	Metric VectorMetric `json:"metric"`
	// M / EfConstruction / EfSearch tune the HNSW graph; 0 = sensible defaults.
	M              int `json:"m,omitempty"`
	EfConstruction int `json:"efConstruction,omitempty"`
	EfSearch       int `json:"efSearch,omitempty"`
	// Quantization selects the stored vector format (default full float32). For
	// VectorModeIVFPQ it sets the format of the re-rank vector store (the bulk of the
	// index): VectorQuantInt8 makes it ~4× smaller on disk and in the page cache —
	// cutting RAM and speeding the random per-candidate re-rank reads — at ~0.5%
	// recall, and is recommended there.
	Quantization VectorQuantization `json:"quantization,omitempty"`
	// Mode selects the index strategy (default btree). See VectorMode.
	Mode VectorMode `json:"mode,omitempty"`
	// HybridCacheVectors, for VectorModeHybrid, also caches vectors in RAM (the
	// "vector tier") so layer-0 search avoids btree vector reads entirely — the
	// bulk of search cost. RAM ≈ liveVectors × recordSize (use int8 to keep it
	// small). Ignored unless Mode == VectorModeHybrid.
	HybridCacheVectors bool `json:"hybridCacheVectors,omitempty"`

	// CompactRatio enables automatic compaction of the HNSW graph. Deletes and
	// replaces only tombstone nodes; when tombstones reach CompactRatio × live
	// nodes, the index is rebuilt to reclaim them (dropping dead nodes, superseded
	// vectors, and re-densifying labels). 0 (the default) disables auto-compaction
	// — the index is then only compacted via Collection.CompactVectorIndex.
	//
	// Choosing a value (measured — see TestVectorCompactThreshold): tombstoned
	// nodes are still traversed during search, so latency rises roughly linearly
	// with the deleted/live ratio (recall does NOT — it holds/improves as the live
	// set shrinks). Search latency degrades ~30% at a ratio of ~0.2–0.25, ~50% at
	// ~0.5, and ~2x at ~1.0 (steeper at larger N / higher dim). With
	// HybridCacheVectors, tombstones also inflate the RAM vector tier (it tracks
	// the label high-water mark, not the live set), so compaction reclaims RAM too.
	// A rebuild costs ~one re-insert of the live set, so the amortized compaction
	// cost per delete is ~insertCost / CompactRatio — i.e. a smaller ratio caps
	// latency tighter but rebuilds far more often. Balanced default: ~0.5. Use
	// ~0.25 only for read-latency-sensitive, delete-light workloads.
	//
	// Auto-compaction runs synchronously, in its own transaction, right after the
	// self-contained write that crosses the threshold — never inside a
	// caller-managed transaction. The rebuild is O(live) at ~850 nodes/s for dim
	// 768 (measured, see TestVectorCompactTiming): ~23 s to rebuild 19k live, ~45 s
	// for 38k. It surfaces as a latency spike on the triggering write, so for large
	// indexes prefer leaving this 0 and scheduling Collection.CompactVectorIndex in
	// a maintenance window.
	// Ignored for VectorModeBruteForce.
	//
	// For VectorModeIVFPQ, CompactRatio instead bounds *centroid drift*: IVF deletes
	// are physical (no tombstones), but the codebooks are frozen at build, so as the
	// data distribution shifts the partition degrades. CompactRatio triggers an
	// automatic rebuild (re-train from the live set) when the drift score —
	// max(reconstruction-error ratio − 1, churn ratio) — reaches it. ~0.5 rebuilds
	// after the live set roughly doubles or new data fits the centroids ~50% worse;
	// 0 disables auto-rebuild (use Collection.CompactVectorIndex manually).
	CompactRatio float64 `json:"compactRatio,omitempty"`

	// IVF-PQ parameters (Mode == VectorModeIVFPQ only). Zero values pick defaults.
	//
	// NList is the number of coarse cells (k-means centroids); 0 ⇒ ~4·√N at build.
	// More cells = finer partition = fewer vectors scanned per probe, but more
	// centroids to compare and a larger training requirement.
	NList int `json:"nList,omitempty"`
	// NProbe is how many cells a search scans (the recall/speed dial); 0 ⇒ 16.
	// Higher = more recall, more cells scanned.
	NProbe int `json:"nProbe,omitempty"`
	// Closure is the multi-assignment factor: each vector is placed in its Closure
	// nearest cells (with a per-cell residual code) so boundary vectors are found at
	// lower NProbe (SPANN closure). 0/1 = single assignment; ~4 reaches parity at
	// ~4× lower NProbe, costing ~Closure× the on-disk code bytes. M (above) sets the
	// number of PQ subquantizers / code bytes; Dim must be divisible by M.
	Closure int `json:"closure,omitempty"`
	// PrecomputeTableMiB budgets the RAM for the IVF-PQ precomputed ADC table, which
	// removes the per-cell distance-table rebuild from search (measured ~3× faster
	// search at dim 768) at the cost of an always-resident table of nlist·M·1 KiB.
	// 0 = the default budget (128 MiB: on for typical indexes, off for very large
	// ones whose table would exceed it); a negative value disables the table
	// (smallest RAM, slower search); a positive value sets the budget in MiB.
	PrecomputeTableMiB int `json:"precomputeTableMiB,omitempty"`
}

// IndexInfo provides information about an index.
type IndexInfo struct {
	// Name is the name of the index. If empty, it will be generated
	// based on the fields (e.g., "name,-createdDate").
	Name string `json:"name"`

	// Fields are the fields included in the index. Each field can specify
	// ascending (e.g., "name") or descending (e.g., "-createdDate") order.
	Fields []string `json:"fields"`

	// Unique indicates whether the index enforces a unique constraint.
	Unique bool `json:"unique"`

	// Sparse indicates whether the index is sparse, indexing only documents
	// with a present, non-null value for every indexed field.
	//
	// Null is treated the same as missing: a document whose indexed field is
	// explicitly null is NOT indexed (this differs from MongoDB, where a sparse
	// index stores present-but-null values). The choice keeps sparse indexing
	// consistent with query matching, where {field: null} matches both a null
	// and a missing field.
	//
	// Over a path through an array of objects the rule is per document: each
	// indexed field must hold a present value in SOME element. Every key is
	// then written unless all of its fields are null or missing in that
	// element — {"a":[{"b":1},{"c":2}]} under (a.b, a.c) has keys (1, null)
	// and (null, 2) — so a document that matches through different elements
	// is still reachable by the seek on the leading field.
	//
	// As a consequence, the planner only uses a sparse index for a query that
	// guarantees every indexed field is present and non-null; otherwise it would
	// silently drop matching documents the index never stored. In particular a
	// query whose only constraint on a field is {$exists: true} cannot use a
	// sparse index here, because an explicit-null document matches $exists:true
	// yet is absent from the index — such a query falls back to a complete index
	// or a full scan. For a unique sparse index, several documents with a missing
	// or null field coexist freely, since none of them are indexed.
	Sparse bool `json:"sparse"`

	// Kind selects the index type (range by default, full-text, or vector).
	Kind IndexKind `json:"kind,omitempty"`

	// Fulltext configures a full-text index when Kind == IndexKindFulltext.
	Fulltext *FulltextParams `json:"fulltext,omitempty"`

	// Vector configures a vector index when Kind == IndexKindVector.
	Vector *VectorParams `json:"vector,omitempty"`
}

// FulltextParams configures a full-text index. All fields are optional; the
// zero value reproduces the default BM25 scoring. Per-field weights (Weights)
// and analyzer/stemming selection are reserved for a later version. The default
// analyzer (NFKC + case-fold + UAX#29 + CJK bigram) is used for all fields.
type FulltextParams struct {
	// Weights is the per-field BM25F boost, keyed by indexed field name (each key
	// must be one of IndexInfo.Fields). A field absent from the map has weight 1.0.
	// Mirrors MongoDB's text-index weights option, but scoring is BM25F: the
	// per-field term frequencies are combined as Σ weight_f · tf_f and then scored
	// with standard BM25 saturation + length normalization. Set at index creation;
	// changing a weight only changes ranking (no re-index needed — weights are read
	// at query time). Boosting e.g. {"title": 5} ranks title matches above body.
	Weights map[string]float64 `json:"weights,omitempty"`
	// B is the BM25 length-normalization parameter, in [0,1]. 0 ⇒ the default
	// 0.75. Lower values reduce the bias toward short documents (e.g. 0.4 is a
	// reasonable choice for a corpus of mixed-length notes); 0 disables length
	// normalization entirely — but note 0 is read as "use the default", so to
	// approach no normalization use a small value rather than exactly 0.
	B float64 `json:"b,omitempty"`
	// K1 is the BM25 term-frequency saturation parameter (>= 0). 0 ⇒ the default
	// 1.2. Higher values make repeated term occurrences count for more.
	K1 float64 `json:"k1,omitempty"`
}

func (i IndexInfo) createName() string {
	return strings.Join(i.Fields, ",")
}

// Index represents an index on a collection.
type Index interface {
	// Info returns the IndexInfo for this index.
	Info() IndexInfo

	// Len returns the length of the index.
	Len(ctx context.Context) (int, error)
}

func indexNsName(collName, indexName string) string {
	return "ix:" + collName + ":" + indexName
}

func newIndex(c *collection, info IndexInfo, ns *btree.Namespace) (idx *index, err error) {
	// Every construction site (init, reconcile, createIndex) runs on the
	// writer or under c.mu, so reading c.name here is race-free; the captured
	// identity is immutable afterwards (see the field comment).
	idx = &index{
		info:       info,
		c:          c,
		ns:         ns,
		nsName:     indexNsName(c.name, info.Name),
		catalogKey: indexKey(c.name, info.Name),
	}
	if err = idx.init(); err != nil {
		return nil, err
	}
	return
}

type index struct {
	c    *collection
	info IndexInfo
	ns   *btree.Namespace

	fieldNames []string
	fieldPaths [][]string
	reverse    []bool

	// sketch is the WRITER-OWNED live selectivity sketch. It is mutated in place
	// ONLY by the single writer (insertKeys/deleteKeys, serialized by the btree
	// writeMu and the cross-process WAL write lock) and marshaled by
	// persistSketches. No reader ever swaps or mutates it, so a concurrent
	// read-tx reload can never lose the writer's accumulated increments. Inside a
	// write tx it is also the planner's view (reflects this tx's own uncommitted
	// inserts).
	sketch *qplanner.IndexSketch

	// sketchPub is the READER-VISIBLE copy-on-write snapshot — the per-index
	// analog of collection.indexes. The query/Stats path reads it lock-free via
	// loadPubSketch(); the advisory staleness tier publishes a freshly decoded
	// sketch into it on a stale READ (build-fresh, never mutate-in-place); the
	// writer republishes its live object into it at commit (a pointer Store, no
	// clone). Non-nil once the index is published to readers.
	sketchPub atomic.Pointer[qplanner.IndexSketch]

	sketchBuf      []byte
	sketchModified bool

	cboInfo *qplanner.IndexInfo // cached CBO index info, built once during init

	// validFromCookie is the earliest schema cookie at which this handle is
	// KNOWN visible: a reader whose snapshot cookie reaches it is guaranteed
	// the index committed before its snapshot (SchemaCookie bumps exactly once
	// per schema-changing commit, atomically with the DDL — the OP_Transaction
	// analog). A DDL publishing mid-tx stamps its begin cookie + 1; handles
	// built from a committed view (init, reconcile) stamp that view's cookie.
	// An OLDER reader is not necessarily excluded — it falls back to resolving
	// the handle's namespace in its own snapshot (see visibleTo).
	//
	// Set before the CoW publish, immutable after (clones copy it); a rollback
	// discards the handle itself (registerIndexSetRestore). Read lock-free by
	// concurrent planners — a plain load, no atomics needed for an immutable
	// field published via the CoW slice swap.
	validFromCookie uint32
	// nsName and catalogKey are the identity of the generation this handle was
	// built from, captured at construction and immutable: visibleTo's slow
	// path reads them lock-free, so it must never consult c.name, which a
	// concurrent Rename mutates under c.mu. Rename clones carry the renamed
	// identity (cloneWithNs); handles from before a rename keep the old one —
	// correct for the only readers that consult it, whose snapshots predate
	// this handle.
	nsName     string
	catalogKey []byte

	keyBuf  anyenc.Tuple
	keysBuf []anyenc.Tuple
	// keyBoundsBuf is parallel to keysBuf: keyBoundsBuf[k][L] is the byte offset
	// in keysBuf[k] just past field L's encoding, so keysBuf[k][:keyBoundsBuf[k][L]]
	// is the level-L prefix fed to the multi-level sketch.
	keyBoundsBuf [][]int
	// fields is writeValues' per-field scratch, indexed by field position and
	// reset once per document (resetFields).
	fields      []fieldScratch
	rebinds     []rebind        // stack of rebound fields across nested fan-outs
	shared      int             // later fields currently rebound: level dedup is off
	rebound     bool            // some field was rebound for this document: keys dedup whole
	valBuf      []*anyenc.Value // stack of a leaf array's index values
	fullKeyBuf  anyenc.Tuple    // reusable buffer for full keys (key+docId)
	seekBuf     anyenc.Tuple    // reusable buffer for unique constraint seek results
	uniqSeekBuf anyenc.Tuple    // reusable buffer for the padded unique-probe seek key
	mkBuf       []byte          // reusable buffer for the multikey-flag check-and-put read
}

// loadPubSketch returns the published reader snapshot. Lock-free; the returned
// pointer is valid for the caller's transaction (a concurrent reload swaps a new
// object in, leaving this one untouched). Mirrors collection.loadIndexes().
func (idx *index) loadPubSketch() *qplanner.IndexSketch { return idx.sketchPub.Load() }

// storePubSketch publishes a reader snapshot. Callers hold c.mu (publisher
// serialisation, like storeIndexes); readers need no lock.
func (idx *index) storePubSketch(s *qplanner.IndexSketch) { idx.sketchPub.Store(s) }

// visibleTo reports whether the given tx may plan with this handle. Fast
// path: any write-tx view is the single writer's own (which must see its
// uncommitted DDL for same-tx maintenance and queries), and a snapshot cookie
// at or past validFromCookie proves the index committed before the snapshot.
// Slow path: an older reader admits the handle only if its OWN snapshot still
// carries this exact index — the catalog row at catalogKey matches the
// handle's full definition AND the index namespace resolves at the bound
// root. Both checks are required: root equality alone is a defeatable
// identity proxy (freelist reuse can land a recreated tree on the freed old
// root's page number), and definition equality alone does not prove the
// handle's bound root means anything in the reader's snapshot. When both
// hold, the tree at that root IS the reader's own generation of this
// definition, so planning with it is exact even if the handle was built from
// a later state. Anything less → invisible; the planner scans, which is
// always correct. The stamp is a fast-path bound, not the exact commit point.
func (idx *index) visibleTo(tx *btree.ReadTx) bool {
	// Snapshot cookie, not the raised begin-time one — see visibleIndexes.
	if tx.IsWriteTx() || tx.SnapshotSchemaCookie() >= idx.validFromCookie {
		return true
	}
	raw, err := tx.AppendValue(idx.c.db.systemNS, idx.catalogKey, nil)
	if err != nil || !indexDefMatches(raw, idx.info) {
		return false
	}
	if idx.ns == nil {
		return false
	}
	ns, nsErr := tx.GetNamespace(idx.nsName)
	return nsErr == nil && ns.RootPage() == idx.ns.RootPage()
}

// cloneWithNs returns a copy of the index bound to a different namespace
// handle; Rename publishes clones copy-on-write (via storeIndexes) because
// concurrent readers access idx.ns lock-free, so the field can't be mutated
// in place. Field-by-field, not a struct copy (sketchPub is an atomic).
// The live sketch pointer is SHARED with the original: the single writer is
// the only mutator, and after storeIndexes the writer path loads the clone.
// Scratch buffers start zero and regrow lazily.
func (idx *index) cloneWithNs(ns *btree.Namespace, nsName string, catalogKey []byte) *index {
	n := &index{
		c:              idx.c,
		info:           idx.info,
		ns:             ns,
		nsName:         nsName,
		catalogKey:     catalogKey,
		fieldNames:     idx.fieldNames,
		fieldPaths:     idx.fieldPaths,
		reverse:        idx.reverse,
		sketch:         idx.sketch,
		sketchModified: idx.sketchModified,
	}
	n.initScratch()
	// cboInfo embeds the namespace handle — rebuild it around the new one.
	cbo := *idx.cboInfo
	cbo.Ns = ns
	n.cboInfo = &cbo
	n.sketchPub.Store(idx.loadPubSketch())
	// A rename in the same tx as an uncommitted create keeps the pending
	// visibility bound; the field is immutable, so a plain copy suffices.
	n.validFromCookie = idx.validFromCookie
	return n
}

func validateIndexField(s string) (err error) {
	if s == "" || s == "-" {
		return fmt.Errorf("index field is empty")
	}
	if strings.HasPrefix(s, "$") {
		return fmt.Errorf("invalid index field name: %s", s)
	}
	return nil
}

func parseIndexField(s string) (fields []string, reverse bool) {
	if strings.HasPrefix(s, "-") {
		return strings.Split(s[1:], "."), true
	}
	return strings.Split(s, "."), false
}

func (idx *index) init() (err error) {
	for _, field := range idx.info.Fields {
		fields, reverse := parseIndexField(field)
		if slices.Contains(fields, "") {
			return fmt.Errorf("invalid index field: '%s'", field)
		}
		idx.fieldNames = append(idx.fieldNames, strings.Join(fields, "."))
		idx.fieldPaths = append(idx.fieldPaths, fields)
		idx.reverse = append(idx.reverse, reverse)
	}
	idx.initScratch()

	// Build cached CBO index info once (avoids per-query allocation)
	sharedFrom := len(idx.fieldPaths)
	for j := 1; j < len(idx.fieldPaths) && sharedFrom == len(idx.fieldPaths); j++ {
		for i := 0; i < j; i++ {
			if idx.fieldPaths[i][0] == idx.fieldPaths[j][0] {
				sharedFrom = j
				break
			}
		}
	}
	idx.cboInfo = &qplanner.IndexInfo{
		Name:       idx.info.Name,
		FieldNames: idx.fieldNames,
		FieldPaths: idx.fieldPaths,
		Reverse:    idx.reverse,
		Unique:     idx.info.Unique,
		Sparse:     idx.info.Sparse,
		Ns:         idx.ns,
		SharedFrom: sharedFrom,
	}
	return nil
}

func (idx *index) Info() IndexInfo {
	return idx.info
}

func (idx *index) Len(ctx context.Context) (count int, err error) {
	err = idx.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		var txErr error
		count, txErr = tx.Count(idx.ns)
		return txErr
	})
	return
}

// insertKeys inserts index entries for the given item into the index namespace.
// Both unique and non-unique indexes use key=Tuple(fields..., docId).
// For unique indexes, a single-shot SeekKey + prefix check enforces the constraint.
//
// Per-entry value (1 byte bitmask, see qplanner.IndexEntryFlagMultiKey):
//   - len(idx.keysBuf) > 1 → IndexValueMultiKey (this doc has >1 entries here)
//   - len(idx.keysBuf) == 1 → IndexValueScalar  (this is this doc's only entry)
//
// Lets the multi-bound covering-count fast path stream-count scalar entries
// without a hash set; only multi-key entries pay the dedup cost. Reversible
// per-doc: an array shrinking from 3 elements to 1 next time round will see
// its single new entry written with IndexValueScalar.
func (idx *index) insertKeys(tx *btree.WriteTx, it item) error {
	idx.fillKeysBuf(it)
	idKey := idx.c.appendId(nil, it.Value())

	entryValue := qplanner.IndexValueScalar
	if len(idx.keysBuf) > 1 {
		entryValue = qplanner.IndexValueMultiKey
		// This doc fans out (non-empty array at an indexed field): persist
		// the sticky index-level multikey flag in this same tx, so any
		// snapshot that can see these entries sees the flag.
		if err := idx.markMultiKey(tx); err != nil {
			return err
		}
	}

	prevKi := -1
	for ki, key := range idx.keysBuf {
		idx.fullKeyBuf = append(idx.fullKeyBuf[:0], key...)
		idx.fullKeyBuf = append(idx.fullKeyBuf, idKey...)

		if idx.info.Unique {
			// Escape-aware duplicate probe: a bare HasPrefix would match an
			// entry whose last field value extends this key's value with an
			// escaped NUL ("a" vs "a\x00b") and report a false
			// ErrUniqueConstraint; for an inverted tail field those
			// continuation entries (key+0x00...) also sort BEFORE a real
			// duplicate (key+docIdTag, tag >= 0x01) and would shadow it, so
			// the seek starts at key+0x01 to hop over them.
			lastInverted := len(idx.reverse) > 0 && idx.reverse[len(idx.reverse)-1]
			seekKey := key
			if lastInverted {
				idx.uniqSeekBuf = append(append(idx.uniqSeekBuf[:0], key...), 0x01)
				seekKey = idx.uniqSeekBuf
			}
			var err error
			idx.seekBuf, err = tx.AppendSeekKey(idx.ns, seekKey, idx.seekBuf[:0])
			if err == nil && qplanner.HasExactFieldPrefix(idx.seekBuf, key, lastInverted) {
				if !bytes.Equal(idx.seekBuf, idx.fullKeyBuf) {
					return ErrUniqueConstraint
				}
				continue // same doc, idempotent: leave prevKi (sketch already counted it)
			}
		}

		if err := tx.Put(idx.ns, idx.fullKeyBuf, entryValue); err != nil {
			return err
		}
		if idx.sketch != nil {
			idx.applySketch(ki, prevKi, true)
			idx.sketchModified = true
		}
		prevKi = ki
	}
	if idx.sketch != nil {
		idx.sketch.IncrementDocCount()
		idx.sketchModified = true
	}
	return nil
}

// deleteKeys deletes index entries for the given item from the index namespace.
// Both unique and non-unique indexes use key=Tuple(fields..., docId), value=nil.
// markMultiKey persists the sticky one-way index-level multikey flag in the
// SAME write tx that commits this doc's fan-out entries: any snapshot that can
// see the entries sees the flag, which is what lets plan-time tight-bounds
// gating read it from the query's own read tx with no coherence protocol.
//
// Check-and-put against the tx's own view — deliberately NO in-memory latch:
// a commit-time latch goes stale under savepoint partial rollback (the nested
// tx's record write reverts while the outer tx commits) and under peer
// drop+recreate with live-object reuse, silently leaving a scalar record over
// committed fan-out entries. The record itself rolls back with the entries,
// which is exactly the consistent outcome. deleteKeys never clears the flag
// (older snapshots may still hold fanned-out entries); drop+recreate resets.
func (idx *index) markMultiKey(tx *btree.WriteTx) error {
	key := multikeyKey(idx.ns.Name())
	var err error
	idx.mkBuf, err = tx.AppendValue(idx.c.db.systemNS, key, idx.mkBuf[:0])
	if err == nil && len(idx.mkBuf) == 1 && idx.mkBuf[0] == mkValMultiKey[0] {
		return nil // already flagged in this tx's view
	}
	return tx.Put(idx.c.db.systemNS, key, mkValMultiKey)
}

// isScalarProven reports whether this index provably contains no fan-out
// (multi-key) entries IN THE GIVEN SNAPSHOT: an explicit scalar-so-far marker
// written at index creation and never flipped by a fan-out write. An absent
// record (index built before the flag existed) or a multikey value means
// "assume fan-out" — tight seek bounds must not be used. The flag travels in
// the same tx as the entries (markMultiKey), so reading it through the query's
// read tx is exact for that snapshot, including across processes.
func (idx *index) isScalarProven(tx *btree.ReadTx) bool {
	v, err := tx.Get(idx.c.db.systemNS, multikeyKey(idx.ns.Name()))
	return err == nil && len(v) == 1 && v[0] == mkValScalar[0]
}

func (idx *index) deleteKeys(tx *btree.WriteTx, it item) error {
	idx.fillKeysBuf(it)
	idKey := idx.c.appendId(nil, it.Value())
	prevKi := -1
	for ki, key := range idx.keysBuf {
		idx.fullKeyBuf = append(idx.fullKeyBuf[:0], key...)
		idx.fullKeyBuf = append(idx.fullKeyBuf, idKey...)
		if err := tx.Delete(idx.ns, idx.fullKeyBuf); err != nil {
			if !errors.Is(err, btree.ErrKeyNotFound) {
				return err
			}
		}
		if idx.sketch != nil {
			idx.applySketch(ki, prevKi, false)
			idx.sketchModified = true
		}
		prevKi = ki
	}
	if idx.sketch != nil {
		idx.sketch.DecrementDocCount()
		idx.sketchModified = true
	}
	return nil
}

// applySketch increments (inc) or decrements the multi-level sketch for entry ki,
// deduping each prefix level it shares with the previously processed entry prevKi
// (-1 = none). writeValues' DFS emits keys with equal shallow prefixes
// contiguously, so a deeper multikey (array) field that fans one document into
// several entries does not inflate the shallow levels — the level it diverges at
// and below are bumped, the unchanged shallow prefixes are skipped. A missed
// duplicate could only over-count, never under-count, matching the sketch's
// existing collision bias.
func (idx *index) applySketch(ki, prevKi int, inc bool) {
	key := idx.keysBuf[ki]
	bounds := idx.keyBoundsBuf[ki]
	for L := 0; L < len(bounds); L++ {
		if prevKi >= 0 {
			pe := idx.keyBoundsBuf[prevKi][L]
			if pe == bounds[L] && bytes.Equal(key[:bounds[L]], idx.keysBuf[prevKi][:pe]) {
				continue // same level-L prefix as previous entry: already applied
			}
		}
		if inc {
			idx.sketch.Increment(L, key[:bounds[L]])
		} else {
			idx.sketch.Decrement(L, key[:bounds[L]])
		}
	}
}

// writeKey appends the key being built and reports whether it was written:
// a sparse index skips a key whose fields are all null or missing.
func (idx *index) writeKey() bool {
	if idx.info.Sparse {
		any := false
		for i := range idx.fields {
			if idx.fields[i].present {
				any = true
				idx.fields[i].seen = true
			}
		}
		if !any {
			return false
		}
	}
	if idx.rebound {
		for _, k := range idx.keysBuf {
			if bytes.Equal(k, idx.keyBuf) {
				return true
			}
		}
	}
	nl := len(idx.keysBuf) + 1
	idx.keysBuf = slices.Grow(idx.keysBuf, nl)[:nl]
	idx.keysBuf[nl-1] = append(idx.keysBuf[nl-1][:0], idx.keyBuf...)
	idx.keyBoundsBuf = slices.Grow(idx.keyBoundsBuf, nl)[:nl]
	bounds := idx.keyBoundsBuf[nl-1][:0]
	for i := range idx.fields {
		bounds = append(bounds, idx.fields[i].curBound)
	}
	idx.keyBoundsBuf[nl-1] = bounds
	return true
}

// writeValues appends, for field i, one key per value the field stores for
// this document, each continued into field i+1 depth-first, and reports
// whether any full key was written. A sparse index skips a key whose fields
// are all null or missing, and drops the document when some field is null
// or missing in every key (fillKeysBuf).
//
// Field i resolves from fields[i].root at segment .consumed (the whole document
// at segment 0 unless an earlier field rebound it — see resolveField): a
// path through an array of objects fans out one entry per element, like a
// leaf array does, and fields that run through the SAME array iterate it
// together, one key per element, never as a cross product.
func (idx *index) writeValues(i int) bool {
	if i == len(idx.fieldPaths) {
		return idx.writeKey()
	}
	idx.fields[i].keyPrefix = len(idx.keyBuf)
	root, seg := idx.fields[i].root, idx.fields[i].consumed
	// One scalar leaf — the common field — needs no fan-out bookkeeping.
	if leaf, single := root.GetLeaf(idx.fieldPaths[i][seg:]...); single && (leaf == nil || leaf.Type() != anyenc.TypeArray) {
		idx.fields[i].dedup = false
		return idx.emitLeaf(i, anyenc.Leaf{Value: leaf})
	}
	// Several values under one prefix (an array, or fan-out) may repeat —
	// {"a":[{"b":1},{"b":1}]} — and must yield one entry.
	idx.fields[i].dedup = true
	idx.fields[i].uniq = idx.fields[i].uniq[:0]
	return idx.resolveField(i, root, seg)
}

// resolveField walks field i's path from segment seg at v and emits every
// leaf (anyenc.Value.AppendLeaves semantics). At an array met by a
// non-numeric segment, every later field whose path reaches this same array
// is rebound to the element being visited, so its own walk continues inside
// that element — MongoDB's key generation for fields sharing an array.
func (idx *index) resolveField(i int, v *anyenc.Value, seg int) bool {
	path := idx.fieldPaths[i]
	for ; seg < len(path); seg++ {
		if v == nil {
			return idx.emitLeaf(i, anyenc.Leaf{})
		}
		switch v.Type() {
		case anyenc.TypeObject:
			v = v.Get(path[seg])
		case anyenc.TypeArray:
			arr, _ := v.Array()
			if n, ok := anyenc.ParseIndexSegment(path[seg]); ok {
				if n < 0 || n >= len(arr) {
					return idx.emitLeaf(i, anyenc.Leaf{})
				}
				v = arr[n]
				if seg == len(path)-1 {
					return idx.emitLeaf(i, anyenc.Leaf{Value: v, Positional: true})
				}
				continue
			}
			if len(arr) == 0 {
				return idx.emitLeaf(i, anyenc.Leaf{})
			}
			mark := len(idx.rebinds)
			shared := 0
			for j := i + 1; j < len(idx.fieldPaths); j++ {
				if idx.sharesArray(j, i, seg) {
					idx.rebinds = append(idx.rebinds, rebind{j, idx.fields[j].root, idx.fields[j].consumed})
					shared++
				}
			}
			// Field i's own context moves into the element too, so a deeper
			// fan-out compares later fields against where this walk stands.
			idx.rebinds = append(idx.rebinds, rebind{i, idx.fields[i].root, idx.fields[i].consumed})
			idx.shared += shared
			if shared > 0 {
				idx.rebound = true
			}
			wrote := false
			for _, el := range arr {
				for _, rb := range idx.rebinds[mark:] {
					idx.fields[rb.field].root, idx.fields[rb.field].consumed = el, seg
				}
				var ok bool
				if el.Type() == anyenc.TypeObject {
					ok = idx.resolveField(i, el, seg)
				} else {
					ok = idx.emitLeaf(i, anyenc.Leaf{})
				}
				wrote = wrote || ok
			}
			for _, rb := range idx.rebinds[mark:] {
				idx.fields[rb.field].root, idx.fields[rb.field].consumed = rb.root, rb.consumed
			}
			clear(idx.rebinds[mark:])
			idx.rebinds = idx.rebinds[:mark]
			idx.shared -= shared
			return wrote
		default:
			return idx.emitLeaf(i, anyenc.Leaf{})
		}
	}
	return idx.emitLeaf(i, anyenc.Leaf{Value: v})
}

// fieldScratch is one index field's state while writeValues builds a
// document's keys. Laid out widest first — pointer, slice header, ints, then
// the bools — so it packs into 64 bytes with no interior padding.
type fieldScratch struct {
	root      *anyenc.Value  // value the field resolves from (rebound inside a shared array)
	uniq      []anyenc.Tuple // values already written under the current prefix
	consumed  int            // path segments already resolved by root
	keyPrefix int            // len(keyBuf) at this field's level
	curBound  int            // len(keyBuf) past this field's value in the key being built
	dedup     bool           // the values under one prefix may repeat
	present   bool           // the value in the key being built is non-null
	seen      bool           // some key of this document had the field present
}

// rebind records a later field's root before an array fan-out rebinds it to
// the element under visit.
type rebind struct {
	field    int
	root     *anyenc.Value
	consumed int
}

// sharesArray reports whether field j, resolved from the same root and
// segment as field i, reaches the array field i fans out on at segment seg
// and continues into its elements (a numeric segment there would index the
// array instead, and a path ending at the array names the array itself).
func (idx *index) sharesArray(j, i, seg int) bool {
	pj, pi := idx.fieldPaths[j], idx.fieldPaths[i]
	if len(pj) <= seg || idx.fields[j].root != idx.fields[i].root || idx.fields[j].consumed != idx.fields[i].consumed {
		return false
	}
	for k := idx.fields[i].consumed; k < seg; k++ {
		if pj[k] != pi[k] {
			return false
		}
	}
	_, numeric := anyenc.ParseIndexSegment(pj[seg])
	return !numeric
}

// emitLeaf writes field i's index values for one leaf — the value, or each
// element followed by the array itself for a non-positional array leaf
// (anyenc.AppendIndexValues) — each continued into field i+1.
func (idx *index) emitLeaf(i int, l anyenc.Leaf) bool {
	var one [1]*anyenc.Value
	vals := one[:]
	one[0] = l.Value
	vb := len(idx.valBuf)
	if l.Value != nil && l.Value.Type() == anyenc.TypeArray && !l.Positional {
		if arr, _ := l.Value.Array(); len(arr) > 0 {
			idx.valBuf = append(idx.valBuf, arr...)
			idx.valBuf = append(idx.valBuf, l.Value)
			vals = idx.valBuf[vb:]
		}
	}

	// Reverse-flagged fields are stored bitwise-inverted so a single forward
	// index scan yields the field's declared (descending) order; readers skip
	// such fields via the inverted-tag length path in anyenc.parseValue. The
	// docId suffix appended later (insertKeys/deleteKeys) and the per-entry
	// value flag are NEVER inverted. Inversion is a bijection, so the unique
	// dedup (isUnique) and unique-constraint seek still compare correctly.
	reverse := i < len(idx.reverse) && idx.reverse[i]
	k := idx.keyBuf[:idx.fields[i].keyPrefix]
	wrote := false
	for _, v := range vals {
		if reverse {
			idx.keyBuf = anyenc.Tuple(k).AppendInverted(v)
		} else {
			idx.keyBuf = v.MarshalTo(k)
		}
		// A value's subtree is fixed by the value alone only while no later
		// field is rebound (it then resolves from its own root either way);
		// under a rebind two equal values can head different keys, and
		// writeKey dedups the whole key instead.
		if idx.fields[i].dedup && idx.shared == 0 && !idx.isUnique(i, idx.keyBuf) {
			continue
		}
		// Presence is the leaf's: a null element of a present array leaf is
		// still one of its entries.
		idx.fields[i].present = l.Value != nil && l.Value.Type() != anyenc.TypeNull
		idx.fields[i].curBound = len(idx.keyBuf)
		if idx.writeValues(i + 1) {
			wrote = true
		}
	}
	// Clear the frame before cutting it: the buffer outlives the document.
	clear(idx.valBuf[vb:])
	idx.valBuf = idx.valBuf[:vb]
	return wrote
}

// keysBufKeep bounds the entry buffers kept between documents: a document
// that fans out into more entries than this hands its buffers back.
const keysBufKeep = 4096

func (idx *index) fillKeysBuf(it item) {
	if cap(idx.keysBuf) > keysBufKeep {
		idx.keysBuf, idx.keyBoundsBuf = nil, nil
	}
	idx.keysBuf = idx.keysBuf[:0]
	idx.keyBoundsBuf = idx.keyBoundsBuf[:0]
	idx.keyBuf = idx.keyBuf[:0]
	doc := it.Value()
	idx.rebound = false
	idx.resetFields(doc)
	wrote := idx.writeValues(0)
	if wrote && idx.info.Sparse {
		// A sparse index holds a document only when every field is present
		// and non-null somewhere in it.
		for i := range idx.fields {
			wrote = wrote && idx.fields[i].seen
		}
	}
	if !wrote {
		idx.keysBuf = idx.keysBuf[:0]
		idx.keyBoundsBuf = idx.keyBoundsBuf[:0]
	}
	idx.resetFields(nil)
}

// initScratch sizes the per-field scratch (indexed by field position, never
// appended).
func (idx *index) initScratch() {
	idx.fields = make([]fieldScratch, len(idx.fieldPaths))
}

// resetFields starts every field at doc, segment 0, with nothing seen and
// no dedup set; the dedup buffers keep their capacity.
func (idx *index) resetFields(doc *anyenc.Value) {
	for i := range idx.fields {
		f := &idx.fields[i]
		*f = fieldScratch{root: doc, uniq: f.uniq[:0]}
	}
}

func (idx *index) isUnique(i int, k anyenc.Tuple) bool {
	f := &idx.fields[i]
	for _, ek := range f.uniq {
		if bytes.Equal(k, ek) {
			return false
		}
	}
	nl := len(f.uniq) + 1
	f.uniq = slices.Grow(f.uniq, nl)[:nl]
	f.uniq[nl-1] = append(f.uniq[nl-1][:0], k...)
	return true
}

func (idx *index) Close() (err error) {
	return
}
