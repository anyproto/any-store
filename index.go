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
	// IndexKindVector is an HNSW approximate-nearest-neighbour index over a
	// vector (embedding) field. Queried via Find() with a `{field: [vector]}`
	// clause; results carry a synthetic _distance field (see docs/vector-search.md).
	IndexKindVector
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
)

func (m VectorMode) String() string {
	switch m {
	case VectorModeHybrid:
		return "hybrid"
	case VectorModeBruteForce:
		return "brute"
	case VectorModeIVFPQ:
		return "ivfpq"
	default:
		return "btree"
	}
}

// isIVFPQ reports whether this mode is the btree-resident IVF-PQ index.
func (m VectorMode) isIVFPQ() bool { return m == VectorModeIVFPQ }

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
	// Quantization selects the stored vector format (default full float32).
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
	// with the specified fields.
	Sparse bool `json:"sparse"`

	// Kind selects the index type (range by default, or vector).
	Kind IndexKind `json:"kind,omitempty"`

	// Vector configures a vector index when Kind == IndexKindVector.
	Vector *VectorParams `json:"vector,omitempty"`
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
	idx = &index{info: info, c: c, ns: ns}
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

	keyBuf      anyenc.Tuple
	keysBuf     []anyenc.Tuple
	keysBufPrev []anyenc.Tuple
	uniqBuf     [][]anyenc.Tuple
	fullKeyBuf  anyenc.Tuple // reusable buffer for full keys (key+docId)
	seekBuf     anyenc.Tuple // reusable buffer for unique constraint seek results
}

// loadPubSketch returns the published reader snapshot. Lock-free; the returned
// pointer is valid for the caller's transaction (a concurrent reload swaps a new
// object in, leaving this one untouched). Mirrors collection.loadIndexes().
func (idx *index) loadPubSketch() *qplanner.IndexSketch { return idx.sketchPub.Load() }

// storePubSketch publishes a reader snapshot. Callers hold c.mu (publisher
// serialisation, like storeIndexes); readers need no lock.
func (idx *index) storePubSketch(s *qplanner.IndexSketch) { idx.sketchPub.Store(s) }

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
		for _, f := range fields {
			if f == "" {
				return fmt.Errorf("invalid index field: '%s'", field)
			}
		}
		idx.fieldNames = append(idx.fieldNames, strings.Join(fields, "."))
		idx.fieldPaths = append(idx.fieldPaths, fields)
		idx.reverse = append(idx.reverse, reverse)
	}
	idx.uniqBuf = make([][]anyenc.Tuple, len(idx.fieldPaths))

	// Build cached CBO index info once (avoids per-query allocation)
	idx.cboInfo = &qplanner.IndexInfo{
		Name:       idx.info.Name,
		FieldNames: idx.fieldNames,
		FieldPaths: idx.fieldPaths,
		Reverse:    idx.reverse,
		Unique:     idx.info.Unique,
		Sparse:     idx.info.Sparse,
		Ns:         idx.ns,
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
	idKey := it.appendId(nil)

	entryValue := qplanner.IndexValueScalar
	if len(idx.keysBuf) > 1 {
		entryValue = qplanner.IndexValueMultiKey
	}

	for _, key := range idx.keysBuf {
		idx.fullKeyBuf = append(idx.fullKeyBuf[:0], key...)
		idx.fullKeyBuf = append(idx.fullKeyBuf, idKey...)

		if idx.info.Unique {
			var err error
			idx.seekBuf, err = tx.AppendSeekKey(idx.ns, key, idx.seekBuf[:0])
			if err == nil && bytes.HasPrefix(idx.seekBuf, key) {
				if !bytes.Equal(idx.seekBuf, idx.fullKeyBuf) {
					return ErrUniqueConstraint
				}
				continue // same doc, idempotent
			}
		}

		if err := tx.Put(idx.ns, idx.fullKeyBuf, entryValue); err != nil {
			return err
		}
		if idx.sketch != nil {
			idx.sketch.Increment(key)
			idx.sketchModified = true
		}
	}
	if idx.sketch != nil {
		idx.sketch.IncrementDocCount()
		idx.sketchModified = true
	}
	return nil
}

// deleteKeys deletes index entries for the given item from the index namespace.
// Both unique and non-unique indexes use key=Tuple(fields..., docId), value=nil.
func (idx *index) deleteKeys(tx *btree.WriteTx, it item) error {
	idx.fillKeysBuf(it)
	idKey := it.appendId(nil)
	for _, key := range idx.keysBuf {
		idx.fullKeyBuf = append(idx.fullKeyBuf[:0], key...)
		idx.fullKeyBuf = append(idx.fullKeyBuf, idKey...)
		if err := tx.Delete(idx.ns, idx.fullKeyBuf); err != nil {
			if !errors.Is(err, btree.ErrKeyNotFound) {
				return err
			}
		}
		if idx.sketch != nil {
			idx.sketch.Decrement(key)
			idx.sketchModified = true
		}
	}
	if idx.sketch != nil {
		idx.sketch.DecrementDocCount()
		idx.sketchModified = true
	}
	return nil
}

func (idx *index) writeKey() {
	nl := len(idx.keysBuf) + 1
	idx.keysBuf = slices.Grow(idx.keysBuf, nl)[:nl]
	idx.keysBuf[nl-1] = append(idx.keysBuf[nl-1][:0], idx.keyBuf...)
}

func (idx *index) writeValues(d *anyenc.Value, i int) bool {
	if i == len(idx.fieldPaths) {
		idx.writeKey()
		return true
	}
	v := d.Get(idx.fieldPaths[i]...)
	if idx.info.Sparse && (v == nil || v.Type() == anyenc.TypeNull) {
		return false
	}

	// Reverse-flagged fields are stored bitwise-inverted so a single forward
	// index scan yields the field's declared (descending) order; readers skip
	// such fields via the inverted-tag length path in anyenc.parseValue. The
	// docId suffix appended later (insertKeys/deleteKeys) and the per-entry
	// value flag are NEVER inverted. Inversion is a bijection, so the unique
	// dedup (isUnique) and unique-constraint seek still compare correctly.
	reverse := i < len(idx.reverse) && idx.reverse[i]

	k := idx.keyBuf
	if v != nil && v.Type() == anyenc.TypeArray {
		arr, _ := v.Array()
		if len(arr) != 0 {
			idx.uniqBuf[i] = idx.uniqBuf[i][:0]
			for _, av := range arr {
				if reverse {
					idx.keyBuf = anyenc.Tuple(k).AppendInverted(av)
				} else {
					idx.keyBuf = av.MarshalTo(k)
				}
				if idx.isUnique(i, idx.keyBuf) {
					if !idx.writeValues(d, i+1) {
						return false
					}
				}
			}
		}
	}

	if reverse {
		idx.keyBuf = anyenc.Tuple(k).AppendInverted(v)
	} else {
		idx.keyBuf = v.MarshalTo(k)
	}
	return idx.writeValues(d, i+1)
}

func (idx *index) fillKeysBuf(it item) {
	idx.keysBuf = idx.keysBuf[:0]
	idx.keyBuf = idx.keyBuf[:0]
	idx.resetUnique()
	if !idx.writeValues(it.Value(), 0) {
		idx.keysBuf = idx.keysBuf[:0]
	}
}

func (idx *index) resetUnique() {
	for i := range idx.uniqBuf {
		idx.uniqBuf[i] = idx.uniqBuf[i][:0]
	}
}

func (idx *index) isUnique(i int, k anyenc.Tuple) bool {
	for _, ek := range idx.uniqBuf[i] {
		if bytes.Equal(k, ek) {
			return false
		}
	}
	nl := len(idx.uniqBuf[i]) + 1
	idx.uniqBuf[i] = slices.Grow(idx.uniqBuf[i], nl)[:nl]
	idx.uniqBuf[i][nl-1] = append(idx.uniqBuf[i][nl-1][:0], k...)
	return true
}

func (idx *index) Close() (err error) {
	return
}
