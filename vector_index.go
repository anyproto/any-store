package anystore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/internal/vindex"
	"github.com/anyproto/any-store/v2/internal/vivf"
	"github.com/anyproto/any-store/v2/query"
)

// vectorIndex wraps a btree-resident HNSW (internal/vindex) and bridges it to
// any-store documents: it extracts the embedding from the document's vector
// field and keys the graph by the document id. It mutates inside the collection
// write transaction, exactly like the range index's insertKeys/deleteKeys.
type vectorIndex struct {
	c            *collection
	info         IndexInfo
	fieldPath    []string
	dim          int
	mode         VectorMode
	compactRatio float64
	// ix is the btree-resident HNSW graph for btree/hybrid modes; it is nil for
	// brute-force mode (search scans the documents) and for IVF-PQ mode (which uses
	// ivf instead).
	ix *vindex.Index
	// ivf is the btree-resident IVF-PQ index for VectorModeIVFPQ; nil otherwise.
	ivf *vivf.StoreIndex

	// validFromCookie: earliest schema cookie at which this handle is KNOWN
	// visible. Same contract as index.validFromCookie — see there, forTx and
	// visibleIndexes.
	validFromCookie uint32
	// collName and catalogKey are the identity of the generation this handle
	// was built from, captured at construction (bindIdentity) and immutable:
	// forTx's slow path reads them lock-free, so it must never consult
	// c.name, which a concurrent Rename mutates under c.mu. After a rename
	// they intentionally keep the OLD name — the only readers that consult
	// them hold snapshots that predate this handle, i.e. snapshots in which
	// that old name is the correct one.
	collName   string
	catalogKey []byte
	// prev is the handle this one replaced (compaction), still valid in every
	// committed snapshot: while the compacting tx is uncommitted, a concurrent
	// tx searches through prev instead (forTx resolves it by root against the
	// reader's snapshot). Set before the CoW publish; cleared by the commit
	// publication so successive compactions do not chain and pin every
	// predecessor's codebooks/caches for the life of the process — a reader
	// whose snapshot predates the compaction commit is served by a transient
	// rebuild from its own snapshot instead (see forTx). nil for a freshly
	// created index (nothing committed to serve; the reader errors as it did
	// before the DDL began).
	prev atomic.Pointer[vectorIndex]
}

// isIVF reports whether this index uses the IVF backend (PQ or SQ).
func (vi *vectorIndex) isIVF() bool { return vi.mode.isIVF() }

func vectorIndexNsPrefix(collName, indexName string) string {
	return "vix:" + collName + ":" + indexName
}

// vectorIndexNsSuffixes is the union of namespace suffixes across both vector
// backends: HNSW uses :adj, IVF-PQ uses :cb and :cell. Shared by the drop and
// rename sweeps so the two lists can't drift.
var vectorIndexNsSuffixes = []string{":meta", ":vec", ":adj", ":doc", ":lbl", ":cb", ":cell"}

// dropVectorIndexNamespaces deletes all btree namespaces backing a vector index.
// It drops the union of HNSW and IVF-PQ suffixes so it works for either
// backend; absent namespaces are ignored.
func dropVectorIndexNamespaces(tx *btree.WriteTx, collName, indexName string) error {
	prefix := vectorIndexNsPrefix(collName, indexName)
	for _, suf := range vectorIndexNsSuffixes {
		if err := tx.DeleteNamespace(prefix + suf); err != nil && !errors.Is(err, btree.ErrNamespaceNotFound) {
			return err
		}
	}
	return nil
}

func validateVectorParams(p *VectorParams) error {
	if p == nil {
		return fmt.Errorf("vector index requires Vector params")
	}
	if p.Field == "" {
		return fmt.Errorf("vector index requires a Field")
	}
	if p.Dim <= 0 {
		return fmt.Errorf("vector index requires Dim > 0")
	}
	switch p.Mode {
	case VectorModeBTree, VectorModeHybrid, VectorModeBruteForce:
	case VectorModeIVFPQ, VectorModeIVFSQ:
		if p.Metric == VectorDot {
			// vivf has no dot-product ranking path: its distance surface is
			// L2, or cosine via unit-normalization (StoreParams.Normalize).
			// Accepting Dot here would silently rank by L2 — no error,
			// plausible neighbours, wrong order. Refuse until a real MIPS
			// path exists (dot-aware coarse assignment + ADC tables).
			return ErrVectorMetricUnsupported
		}
		m := ivfM(p)
		if p.Dim%m != 0 {
			return fmt.Errorf("vector index: IVF requires Dim (%d) divisible by M (%d)", p.Dim, m)
		}
	default:
		return fmt.Errorf("vector index: unknown mode %d", p.Mode)
	}
	return nil
}

// ErrVectorMetricUnsupported is returned when an index declares a metric the
// selected mode cannot rank by. Validation runs on both create and open, so a
// persisted index with an unsupported combination fails loudly instead of
// silently ranking by the wrong metric.
var ErrVectorMetricUnsupported = errors.New("any-store: vector index: VectorDot is not supported by IVF modes; use VectorModeBTree, VectorModeHybrid or VectorModeBruteForce, or the Cosine/L2 metrics")

// ivfM resolves the PQ subquantizer count: explicit M, else a default that divides
// Dim (prefer 96 → 8-dim subspaces for typical embedding dims, else the largest of
// a small candidate set that divides Dim, else Dim itself).
func ivfM(p *VectorParams) int {
	if p.M > 0 {
		return p.M
	}
	for _, m := range []int{96, 64, 48, 32, 16, 8, 4, 2} {
		if p.Dim%m == 0 && p.Dim/m >= 2 {
			return m
		}
	}
	return p.Dim
}

// ivfNList resolves the coarse cell count: explicit NList, else ~4·√N clamped to
// [16, 65536] (FAISS sizing), with a points-per-centroid floor so tiny collections
// don't over-partition.
func ivfNList(p *VectorParams, n int) int {
	if p.NList > 0 {
		return p.NList
	}
	nl := min(max(4*int(math.Sqrt(float64(n))), 16), 65536)
	if minCells := n / 39; minCells >= 1 && nl > minCells {
		nl = minCells // keep ≥~39 training points per centroid (FAISS floor)
	}
	if n > 0 && nl > n {
		nl = n // can't have more cells than points
	}
	return max(nl, 1)
}

// ivfStoreParams builds the vivf build/open parameters from VectorParams.
func ivfStoreParams(p *VectorParams, n int) vivf.StoreParams {
	closure := max(p.Closure, 1)
	nprobe := p.NProbe
	if nprobe < 1 {
		nprobe = 16
	}
	return vivf.StoreParams{
		Dim:        p.Dim,
		NList:      ivfNList(p, n),
		M:          ivfM(p),
		Assign:     closure,
		NProbe:     nprobe,
		Normalize:  p.Metric == VectorCosine,
		Int8:       p.Quantization == VectorQuantInt8,
		SQ:         p.Mode == VectorModeIVFSQ,
		KMeansPP:   true,
		PrecompMiB: p.PrecomputeTableMiB,
	}
}

func newVectorIndexFromVindex(c *collection, info IndexInfo, ix *vindex.Index) *vectorIndex {
	return &vectorIndex{
		c:            c,
		info:         info,
		fieldPath:    strings.Split(info.Vector.Field, "."),
		dim:          info.Vector.Dim,
		mode:         info.Vector.Mode,
		compactRatio: info.Vector.CompactRatio,
		ix:           ix,
	}
}

func newVectorIndexFromIVF(c *collection, info IndexInfo, ivf *vivf.StoreIndex) *vectorIndex {
	return &vectorIndex{
		c:            c,
		info:         info,
		fieldPath:    strings.Split(info.Vector.Field, "."),
		dim:          info.Vector.Dim,
		mode:         info.Vector.Mode,
		compactRatio: info.Vector.CompactRatio, // IVF: drift threshold for auto-rebuild
		ivf:          ivf,
	}
}

// extractVector reads the embedding from a document, returning ok=false when the
// field is absent or not a numeric array (the document is simply not indexed).
func (vi *vectorIndex) extractVector(it item, buf []float32) ([]float32, bool) {
	v := it.Value().Get(vi.fieldPath...)
	if v == nil {
		return nil, false
	}
	// Packed vector type: zero-copy []float32 view, copied into buf (the parser
	// buffer it points into is reused after this call).
	if v.Type() == anyenc.TypeVectorF32 {
		vec, err := v.VectorF32()
		if err != nil || len(vec) != vi.dim {
			return nil, false
		}
		return append(buf[:0], vec...), true
	}
	if v.Type() != anyenc.TypeArray {
		return nil, false
	}
	arr, err := v.Array()
	if err != nil || len(arr) != vi.dim {
		return nil, false
	}
	buf = buf[:0]
	for _, e := range arr {
		if e.Type() != anyenc.TypeNumber {
			return nil, false
		}
		buf = append(buf, float32(e.GetFloat64()))
	}
	return buf, true
}

// insert indexes the document's vector (no-op if the field is absent/invalid).
// Brute-force mode keeps no index, so there is nothing to maintain.
func (vi *vectorIndex) insert(tx *btree.WriteTx, it item, vbuf []float32) error {
	if vi.isIVF() {
		if vi.ivf == nil {
			return nil
		}
		vec, ok := vi.extractVector(it, vbuf)
		if !ok {
			return nil
		}
		return vi.ivf.Insert(tx, vi.c.appendId(nil, it.Value()), vec)
	}
	if vi.ix == nil {
		return nil
	}
	vec, ok := vi.extractVector(it, vbuf)
	if !ok {
		return nil
	}
	return vi.ix.Insert(tx, vi.c.appendId(nil, it.Value()), vec)
}

// delete removes the document's vector (no-op if it was never indexed).
func (vi *vectorIndex) delete(tx *btree.WriteTx, it item) error {
	if vi.isIVF() {
		if vi.ivf == nil {
			return nil
		}
		_, err := vi.ivf.Delete(tx, vi.c.appendId(nil, it.Value()))
		return err
	}
	if vi.ix == nil {
		return nil
	}
	_, err := vi.ix.Delete(tx, vi.c.appendId(nil, it.Value()))
	return err
}

// update reindexes only when the embedding changed: skip if equal, delete if the
// field went away, otherwise insert (which replaces the old node). This avoids
// re-inserting into the HNSW graph on every unrelated document update.
func (vi *vectorIndex) update(tx *btree.WriteTx, prevIt, it item) error {
	if vi.ix == nil && vi.ivf == nil {
		return nil
	}
	// Fast path: identical stored field → embedding unchanged, nothing to reindex.
	// anyencutil.Equal is a single bytes.Equal memcmp for the packed vector type and
	// alloc-free for arrays, so the common "document updated, embedding untouched"
	// case avoids both vector extractions below.
	newField := it.Value().Get(vi.fieldPath...)
	oldField := prevIt.Value().Get(vi.fieldPath...)
	if newField != nil && oldField != nil && anyencutil.Equal(newField, oldField) {
		return nil
	}
	newVec, newOk := vi.extractVector(it, nil)
	_, oldOk := vi.extractVector(prevIt, nil)
	if vi.isIVF() {
		switch {
		case newOk:
			return vi.ivf.Insert(tx, vi.c.appendId(nil, it.Value()), newVec) // Insert replaces
		case oldOk:
			_, err := vi.ivf.Delete(tx, vi.c.appendId(nil, it.Value()))
			return err
		}
		return nil
	}
	switch {
	case newOk:
		return vi.ix.Insert(tx, vi.c.appendId(nil, it.Value()), newVec)
	case oldOk:
		_, err := vi.ix.Delete(tx, vi.c.appendId(nil, it.Value()))
		return err
	}
	return nil
}

func (vi *vectorIndex) Info() IndexInfo { return vi.info }

// rootUnchanged reports whether the index's on-disk :meta namespace still has the
// btree root page this object was opened against. It returns false after a
// compaction recreated the namespaces (root moved), which means the object's
// handles are stale and it must be reopened. Brute-force indexes (no namespaces)
// are always "unchanged". A transient resolution failure returns true so a
// working index is not dropped over a momentary view.
func (vi *vectorIndex) rootUnchanged(tx *btree.ReadTx, collName string) bool {
	if vi.ix == nil && vi.ivf == nil {
		return true
	}
	ns, err := tx.GetNamespace(vectorIndexNsPrefix(collName, vi.info.Name) + ":meta")
	if err != nil {
		return true
	}
	if vi.isIVF() {
		return ns.RootPage() == vi.ivf.MetaRoot()
	}
	return ns.RootPage() == vi.ix.MetaRoot()
}

// compact rebuilds the HNSW graph from its live vectors, reclaiming tombstones
// and superseded vectors, and returns a fresh *vectorIndex bound to the rebuilt
// namespaces (re-applying the mode's hybrid/vector-cache settings). Brute-force
// has no graph, so it is returned unchanged.
func (vi *vectorIndex) compact(tx *btree.WriteTx, collName string) (*vectorIndex, error) {
	if vi.isIVF() {
		// IVF-PQ has no tombstones to reclaim (deletes are physical); "compaction"
		// here means re-training the codebooks from the live set to clear centroid
		// drift (RESEARCH_IVFPQ_BTREE.md §6). Recreates the namespaces, so the caller
		// MarkSchemaChanged (done by CompactVectorIndex) so peers reopen.
		ivf, err := vivf.Rebuild(tx, vectorIndexNsPrefix(collName, vi.info.Name))
		if err != nil {
			return nil, err
		}
		return newVectorIndexFromIVF(vi.c, vi.info, ivf), nil
	}
	if vi.ix == nil {
		return vi, nil
	}
	prefix := vectorIndexNsPrefix(collName, vi.info.Name)
	ix, err := vindex.Compact(tx, prefix, vectorIndexSeed(collName, vi.info.Name))
	if err != nil {
		return nil, err
	}
	hybrid := vi.info.Vector.Mode == VectorModeHybrid
	ix.SetHybrid(hybrid)
	ix.SetVectorCache(hybrid && vi.info.Vector.HybridCacheVectors)
	return newVectorIndexFromVindex(vi.c, vi.info, ix), nil
}

// overThreshold reports whether tombstones have reached compactRatio × live
// nodes (so an auto-compaction is due). Cheap: one meta read, no namespace walk.
func (vi *vectorIndex) overThreshold(tx *btree.ReadTx) (bool, error) {
	if vi.isIVF() {
		if vi.ivf == nil || vi.compactRatio <= 0 {
			return false, nil
		}
		score, err := vi.ivf.DriftScore(tx)
		if err != nil {
			return false, err
		}
		return score >= vi.compactRatio, nil
	}
	if vi.ix == nil || vi.compactRatio <= 0 {
		return false, nil
	}
	live, deleted, _, err := vi.ix.Counts(tx)
	if err != nil {
		return false, err
	}
	return deleted > 0 && float64(deleted) >= vi.compactRatio*float64(live), nil
}

// loadVectorIndex resolves an existing vector index from persisted info using
// the provided read transaction (no nested read tx).
// bindIdentity stamps the immutable generation identity (see the field
// comments): every constructor funnel calls it before the handle is returned
// or published, so forTx's lock-free slow path never reads c.name.
func (vi *vectorIndex) bindIdentity(collName string) {
	vi.collName = collName
	vi.catalogKey = indexKey(collName, vi.info.Name)
}

func (c *collection) loadVectorIndex(tx *btree.ReadTx, info IndexInfo) (*vectorIndex, error) {
	return c.loadVectorIndexAs(tx, c.name, info)
}

// loadVectorIndexAs opens the index from the given snapshot under collName —
// the collection's name AS THAT SNAPSHOT knows it. forTx's transient rebuild
// passes the handle's captured name, which may legitimately differ from
// c.name (a later rename); init and reconcile go through loadVectorIndex.
func (c *collection) loadVectorIndexAs(tx *btree.ReadTx, collName string, info IndexInfo) (*vectorIndex, error) {
	if err := validateVectorParams(info.Vector); err != nil {
		return nil, err
	}
	vi, err := func() (*vectorIndex, error) {
		if info.Vector.Mode.isBruteForce() {
			// No on-disk graph to open — the index is metadata only.
			return newVectorIndexFromVindex(c, info, nil), nil
		}
		prefix := vectorIndexNsPrefix(collName, info.Name)
		if info.Vector.Mode.isIVF() {
			ivf, e := vivf.OpenTx(tx, prefix)
			if e != nil {
				return nil, e
			}
			return newVectorIndexFromIVF(c, info, ivf), nil
		}
		ix, e := vindex.OpenTx(tx, prefix, vectorIndexSeed(collName, info.Name))
		if e != nil {
			return nil, e
		}
		hybrid := info.Vector.Mode == VectorModeHybrid
		ix.SetHybrid(hybrid)
		ix.SetVectorCache(hybrid && info.Vector.HybridCacheVectors)
		return newVectorIndexFromVindex(c, info, ix), nil
	}()
	if err != nil {
		return nil, err
	}
	vi.bindIdentity(collName)
	// The opened state is visible from this snapshot's cookie on (reconcile
	// calls at tx begin, before any of this tx's writes; forTx's transient
	// rebuild never republishes). A caller whose view may already hold
	// uncommitted DDL must re-stamp begin+1 — init and createIndexes'
	// publish block do.
	vi.validFromCookie = tx.DiskSchemaCookie()
	return vi, nil
}

// createVectorIndex creates a new vector index (namespaces + meta) and builds it
// from the collection's existing documents, all within tx.
func (c *collection) createVectorIndex(tx *btree.WriteTx, info IndexInfo) (*vectorIndex, error) {
	if err := validateVectorParams(info.Vector); err != nil {
		return nil, err
	}
	tx.MarkSchemaChanged()
	if info.Name == "" {
		info.Name = info.Vector.Field
	}
	if err := validateIndexName(info.Name); err != nil {
		return nil, err
	}
	if err := c.db.registerIndex(tx, c.name, info); err != nil {
		return nil, err
	}
	if info.Vector.Mode.isBruteForce() {
		// Brute-force: no namespaces, no graph, nothing to build from documents.
		// The persisted metadata above is the entire index.
		return newVectorIndexFromVindex(c, info, nil), nil
	}
	prefix := vectorIndexNsPrefix(c.name, info.Name)

	// Collect (id, vector) from existing documents, then build the index in RAM
	// and flush it in one bulk pass (vindex.BulkBuild) — far faster than inserting
	// node-by-node through the btree (no per-edge copy-on-write churn). The bulk path
	// produces a graph byte-identical to the per-insert path, so it is a pure speedup.
	// NOTE: this holds every indexed vector in RAM during the build; for very large
	// collections a maintenance_work_mem-style cap + fallback to per-insert is future
	// work (see BUILD_SPEED.md).
	tmpVI := newVectorIndexFromVindex(c, info, nil) // extractVector needs fieldPath/dim
	var ids [][]byte
	var vecs [][]float32
	cursor := tx.NewCursor(c.ns)
	defer cursor.Close()
	if err := cursor.First(); err != nil {
		return nil, err
	}
	p2 := &anyenc.Parser{}
	for cursor.Valid() {
		val, verr := cursor.Value()
		if verr != nil {
			return nil, verr
		}
		doc, perr := p2.Parse(val) // compression-aware
		if perr != nil {
			return nil, perr
		}
		it := item{val: doc}
		if vec, ok := tmpVI.extractVector(it, nil); ok { // nil buf → fresh copy per doc
			ids = append(ids, c.appendId(nil, it.Value()))
			vecs = append(vecs, vec)
		}
		if err := cursor.Next(); err != nil {
			return nil, err
		}
	}

	// IVF-PQ: train the coarse + PQ codebooks and write the inverted lists in one
	// pass (RESEARCH_IVFPQ_BTREE.md §5.1). nlist/closure auto-size from the live
	// count. IVF trains from the existing documents, so the index must be created on
	// a populated collection (the documented bulk-load pattern); creating it empty
	// has no data to learn the quantizers from.
	if info.Vector.Mode.isIVF() {
		if len(vecs) == 0 {
			return nil, fmt.Errorf("vector index: IVF-PQ requires existing documents to train — insert documents before creating the index")
		}
		ivf, err := vivf.BulkBuild(tx, prefix, ivfStoreParams(info.Vector, len(vecs)), ids, vecs)
		if err != nil {
			return nil, err
		}
		return newVectorIndexFromIVF(c, info, ivf), nil
	}

	p := vindex.Params{
		Dim:            info.Vector.Dim,
		Metric:         info.Vector.Metric.toVindex(),
		M:              info.Vector.M,
		EfConstruction: info.Vector.EfConstruction,
		EfSearch:       info.Vector.EfSearch,
		Quantization:   info.Vector.Quantization.toVindex(),
	}
	// Parallel in-RAM build (graph constructed concurrently in RAM, then flushed
	// single-threaded) — ~17x faster than per-insert at scale. threads=0 → GOMAXPROCS.
	// The parallel phase touches only RAM; tx is used single-threaded in the flush.
	ix, err := vindex.BulkBuildParallel(tx, prefix, p, vectorIndexSeed(c.name, info.Name), ids, vecs, 0)
	if err != nil {
		return nil, err
	}
	hybrid := info.Vector.Mode == VectorModeHybrid
	ix.SetHybrid(hybrid)
	ix.SetVectorCache(hybrid && info.Vector.HybridCacheVectors)
	return newVectorIndexFromVindex(c, info, ix), nil
}

// reconcileVectorIndexesLocked rebuilds the vector-index set from on-disk infos
// after a peer committed vector-index DDL. Caller holds c.mu. Tolerant of
// transient resolution failures (keeps the existing object) so a working index
// is never dropped over a stale view.
func (c *collection) reconcileVectorIndexesLocked(tx *btree.ReadTx, infos []IndexInfo) {
	cur := c.loadVectorIndexes()
	byName := make(map[string]*vectorIndex, len(cur))
	for _, vi := range cur {
		byName[vi.info.Name] = vi
	}

	var want int
	for _, info := range infos {
		if info.Kind == IndexKindVector {
			want++
		}
	}
	rebuilt := make([]*vectorIndex, 0, want)
	changed := want != len(cur)
	for _, info := range infos {
		if info.Kind != IndexKindVector {
			continue
		}
		if existing, ok := byName[info.Name]; ok && existing.rootUnchanged(tx, c.name) {
			rebuilt = append(rebuilt, existing)
			continue
		}
		// New index, or a compaction recreated the namespaces under the same name
		// (root page moved) so the existing object's handles are stale — reopen
		// with fresh handles. This mirrors the range-index reconcile's
		// root-moved-via-drop+recreate path.
		vi, err := c.loadVectorIndex(tx, info)
		if err != nil {
			// not resolvable in this snapshot — keep any existing object rather than
			// dropping a working index over a transient view; retry next round.
			if existing, ok := byName[info.Name]; ok {
				rebuilt = append(rebuilt, existing)
			} else {
				changed = true
			}
			continue
		}
		rebuilt = append(rebuilt, vi)
		changed = true
	}
	if changed {
		c.storeVectorIndexes(rebuilt)
	}
}

// ErrMultipleVectorClauses is returned when a query carries more than one $knn
// clause (unsupported).
var ErrMultipleVectorClauses = errors.New("any-store: query has multiple vector clauses")

// ErrInvalidVectorQuery is returned when a $knn clause is malformed: an empty or
// non-finite $query, a $query whose length differs from the index dimension, or
// an out-of-range $k/$ef. It says nothing about non-$knn clauses on a
// vector-indexed field — those are ordinary filters on every verb.
var ErrInvalidVectorQuery = errors.New("any-store: invalid $knn clause")

// ErrNoVectorIndex is returned when a $knn clause targets a field with no vector
// index (or $index names one that doesn't exist). Exact search is an index MODE
// (VectorModeBruteForce), not a fallback.
var ErrNoVectorIndex = errors.New("any-store: no vector index on field")

// ErrAmbiguousVectorIndex is returned when a $knn clause targets a field with
// more than one vector index and no $index to disambiguate. Without it the
// searched index — and therefore the selected documents, on Delete too — would
// depend on index load order.
var ErrAmbiguousVectorIndex = errors.New("any-store: field has multiple vector indexes; name one with $index")

// ErrKnnBadPlacement is returned when a $knn clause sits anywhere other than the
// top level or under $and: a ranked source cannot be a disjunct ($or/$nor), a
// negation ($not — fail-closed Ok would reflect to match-all), or a nested
// sub-filter.
var ErrKnnBadPlacement = errors.New("any-store: $knn is only allowed at the top level or under $and")

// ErrKnnWithText is returned when one query carries both $knn and $text: a query
// has one source.
var ErrKnnWithText = errors.New("any-store: $knn and $text cannot be combined in one query")

// ErrLegacyVectorClause is returned, on every verb, for the pre-$knn ANN
// spelling — a bare equality of a dim-sized numeric array against a
// vector-indexed field. Silent demotion to a literal filter would mean 0 rows
// (packed storage) or 1 row (plain-array storage) with err == nil; a hard error
// turns every migration point into a test failure instead.
var ErrLegacyVectorClause = errors.New(`any-store: field has a vector index; a bare-array equality clause is no longer an ANN query — use {"$knn":{"$query":[...],"$k":N}} (or query.NewKnn) to search it`)

// ErrDistanceWithoutVector is returned when the synthetic _distance field is used
// in a filter or sort but the query has no $knn clause to produce it.
var ErrDistanceWithoutVector = errors.New("any-store: _distance is only available in a vector query")

// detectKnnQuery is a SYNTACTIC operator check (template: detectFtsQuery): it
// finds the $knn clause, validates it, resolves the vector index, and returns
// the planner spec plus the residual filter (the original tree minus the Knn
// node). Returns (nil, original, nil) when the query has no $knn.
//
// It runs on EVERY verb, via compilePlan, and it is AUTHORITATIVE: every rule
// parseKnn enforces is re-checked here, because ParseCondition short-circuits
// on an already-built Filter and the production consumers build their ANN
// filter programmatically — this walk is the only validation they ever see.
// NOTE: the result is deliberately NOT memoized across calls. The spec's
// Search closure captures the resolved *vectorIndex HANDLE, and stale handles
// are reconciled at read-tx begin — a spec detected before the verb's tx
// opens (validateSources runs pre-tx, ahead of the unsatisfiable()
// short-circuit) would search a peer-rebuilt index through its dead gen-0
// namespaces (caught by the multiprocess IVF consistency test). compilePlan
// re-detects after the tx begins, on the handle set the plan executes on.
func (q *collQuery) detectKnnQuery() (*qplanner.VectorQuerySpec, query.Filter, error) {
	if q.cond == nil || !query.ContainsKnn(q.cond) {
		return nil, q.cond, nil
	}
	node, err := findKnnClause(q.cond)
	if err != nil {
		return nil, nil, err
	}
	knn, _ := knnOf(node.Filter)
	// Re-check the parse-time argument rules (query.Knn.Validate is the one
	// shared copy): programmatic NewKnn filters never see the parser, and this
	// walk is the only validation they get. Wrapped in ErrInvalidVectorQuery
	// so consumers have a matchable sentinel.
	if verr := knn.Validate(); verr != nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrInvalidVectorQuery, verr)
	}
	field := strings.Join(node.Path, ".")
	vi, err := resolveKnnIndex(q.c.loadVectorIndexes(), field, knn.Index)
	if err != nil {
		return nil, nil, err
	}
	if len(knn.Query) != vi.dim {
		return nil, nil, fmt.Errorf("%w: $knn on %q: got %d dims, index has %d", ErrInvalidVectorQuery, field, len(knn.Query), vi.dim)
	}

	residual := knnResidualFilter(q.cond)
	// HARD post-condition, not an optimization: a Knn leaking into the residual
	// FilterIter fail-closes every candidate — Iter = 0 rows, Delete = no-op,
	// err == nil, on all verbs. findKnnClause has already rejected every shape
	// the strip cannot handle, so this is unreachable; keep it that way.
	if query.ContainsKnn(residual) {
		return nil, nil, fmt.Errorf("%w: internal: $knn survived residual extraction", ErrKnnBadPlacement)
	}
	hasResidual := residual != nil && !isAllQueryFilter(residual)

	captured := vi
	spec := &qplanner.VectorQuerySpec{
		Query:          knn.Query,
		K:              knn.K,
		IndexName:      vi.info.Name,
		TotallyOrdered: true, // all three backends return (distance, docId) ascending
	}
	switch {
	case vi.isIVF():
		// IVF: probe a few cells (contiguous range scans), re-rank by exact
		// distance. ef is the re-rank depth / candidate count; nprobe is fixed
		// in the index.
		spec.Ef = knnEf(knn.Ef, captured.ivf.NProbe()*8, knn.K, hasResidual)
		spec.Search = func(tx *btree.ReadTx, qv []float32, ef int) ([]qplanner.VectorCandidate, error) {
			svi, err := captured.forTx(tx)
			if err != nil {
				return nil, err
			}
			cands, err := svi.ivf.SearchCandidates(tx, qv, ef)
			if err != nil {
				return nil, err
			}
			out := make([]qplanner.VectorCandidate, len(cands))
			for i, c := range cands {
				out[i] = qplanner.VectorCandidate{DocId: c.DocID, Distance: c.Distance}
			}
			return out, nil
		}
	case vi.ix == nil:
		// Brute-force: the scan computes an exact distance for every document,
		// so it can rank and cut to k itself — but ONLY when no residual will
		// discard candidates downstream (it is the one backend that can
		// guarantee k post-residual survivors, by returning everything).
		topK := 0
		if !hasResidual {
			topK = knn.K
		}
		spec.Search = func(tx *btree.ReadTx, qv []float32, _ int) ([]qplanner.VectorCandidate, error) {
			svi, err := captured.forTx(tx)
			if err != nil {
				return nil, err
			}
			return q.c.bruteVectorCandidates(tx, svi, qv, topK)
		}
	default:
		// HNSW: ef is the beam width.
		spec.Ef = knnEf(knn.Ef, vi.ix.EfSearch(), knn.K, hasResidual)
		spec.Search = func(tx *btree.ReadTx, qv []float32, ef int) ([]qplanner.VectorCandidate, error) {
			svi, err := captured.forTx(tx)
			if err != nil {
				return nil, err
			}
			cands, err := svi.ix.SearchCandidates(tx, qv, ef)
			if err != nil {
				return nil, err
			}
			out := make([]qplanner.VectorCandidate, len(cands))
			for i, c := range cands {
				out[i] = qplanner.VectorCandidate{DocId: c.DocID, Distance: c.Distance}
			}
			return out, nil
		}
	}
	return spec, residual, nil
}

// forTx resolves the handle the given SCAN tx may search through — the
// visibility gate of visibleIndexes, vector-shaped (see index.visibleTo).
// The write-tx view (single-writer: the creator's own) uses the handle as
// resolved. A reader is served by generation INTERVAL, never by root-page
// identity (page numbers are freelist-recycled, so a recreated root can
// collide with the one a stale snapshot still holds): each handle in the
// prev chain was the published handle for cookies [h.validFromCookie, next
// generation), so the first h with cookie >= h.validFromCookie is exactly
// the generation the reader's snapshot contains — the mid-compaction reader
// lands on prev this way. A reader older than every held generation (prev
// is cleared at the compaction's commit; init/reconcile restamp) is served
// by a transient handle opened from its OWN snapshot, but only when the
// snapshot's catalog row still carries this handle's exact definition —
// which also guarantees the backend class, so the spec branch chosen at
// detect time stays valid for the result. A definition the snapshot does
// not carry errors exactly as before the index existed (the SQLITE_SCHEMA
// posture: never serve old data under a new definition).
func (vi *vectorIndex) forTx(tx *btree.ReadTx) (*vectorIndex, error) {
	if tx.IsWriteTx() {
		return vi, nil
	}
	cookie := tx.DiskSchemaCookie()
	for h := vi; h != nil; h = h.prev.Load() {
		if cookie >= h.validFromCookie {
			return h, nil
		}
	}
	raw, err := tx.AppendValue(vi.c.db.systemNS, vi.catalogKey, nil)
	if err == nil && indexDefMatches(raw, vi.info) {
		if svi, lerr := vi.c.loadVectorIndexAs(tx, vi.collName, vi.info); lerr == nil {
			return svi, nil
		}
	}
	return nil, fmt.Errorf("%w: vector index %q", ErrIndexNotFound, vi.info.Name)
}

// knnOf extracts the Knn from a leaf, accepting the pointer form too: every
// composite filter here has value-receiver methods, so a hand-built &Knn{…}
// satisfies query.Filter just like Knn does, and a value-only type test would
// let it slip past detection into the residual (fail-closed → 0 rows / no-op
// writes with err == nil).
func knnOf(f query.Filter) (query.Knn, bool) {
	switch k := f.(type) {
	case query.Knn:
		return k, true
	case *query.Knn:
		return *k, true
	}
	return query.Knn{}, false
}

// findKnnClause walks the WHOLE tree for the single legal $knn placement:
// Key{path, Knn} at the top level or under $and (any nesting; And/*And). Every
// composite is matched in BOTH value and pointer form — a pointer-built
// &Not{Key{v, Knn}} that a value-only switch skipped would evaluate
// !false == match-all on Delete. A second hit is ErrMultipleVectorClauses; a
// Knn under Or/Nor/Not, nested inside a Key's inner filter, or bare (no Key
// naming the field) is ErrKnnBadPlacement. Callers guarantee ContainsKnn(f).
func findKnnClause(f query.Filter) (node query.Key, err error) {
	var found bool
	var walkKey func(ft query.Key)
	var walk func(f query.Filter)
	walkKey = func(ft query.Key) {
		if _, isKnn := knnOf(ft.Filter); isKnn {
			if found {
				err = ErrMultipleVectorClauses
				return
			}
			node, found = ft, true
			return
		}
		// A Knn deeper inside this Key's inner filter (Key{p, And{Knn,…}},
		// Key{p, Not{Knn}}, …) cannot be stripped as a unit.
		if query.ContainsKnn(ft.Filter) {
			err = ErrKnnBadPlacement
		}
	}
	walk = func(f query.Filter) {
		if err != nil {
			return
		}
		switch ft := f.(type) {
		case query.Knn, *query.Knn:
			// A bare Knn names no field: nothing to resolve an index against.
			err = fmt.Errorf("%w (a $knn must name its field: query.Key{Path, Knn})", ErrKnnBadPlacement)
		case query.Key:
			walkKey(ft)
		case *query.Key:
			walkKey(*ft)
		case query.And:
			for _, sub := range ft {
				walk(sub)
			}
		case *query.And:
			for _, sub := range *ft {
				walk(sub)
			}
		default:
			// Or/Nor/Not (any form), or an unknown filter type wrapping a Knn:
			// not a placement the residual builder can strip.
			if query.ContainsKnn(f) {
				err = ErrKnnBadPlacement
			}
		}
	}
	walk(f)
	if err != nil {
		return query.Key{}, err
	}
	if !found {
		return query.Key{}, fmt.Errorf("%w: internal: ContainsKnn true but no clause found", ErrKnnBadPlacement)
	}
	return node, nil
}

// knnResidualFilter returns f minus the (unique — findKnnClause enforced it)
// Key{…, Knn} node, recursively rebuilding And/*And and collapsing an empty
// result to nil, NOT All{}: the ef sizing and the brute-force topK both key off
// "no residual", and an All{} would silently flip them to the ×10
// over-fetch / full-ranking path on every bare $knn.
//
// The strip tests the NODE (is this Key's inner filter the Knn?), never the
// path: And{Key{v, Knn}, Key{v, Comp{Ne,…}}} is legal programmatically, and a
// path-keyed strip would drop the $ne too — widening the residual, so Delete
// would remove documents the filter excluded. Pointer-built *Key/*Knn strip
// identically (findKnnClause accepted them, so the strip must too).
func knnResidualFilter(f query.Filter) query.Filter {
	switch ft := f.(type) {
	case query.Key:
		if _, isKnn := knnOf(ft.Filter); isKnn {
			return nil
		}
		return ft
	case *query.Key:
		if _, isKnn := knnOf(ft.Filter); isKnn {
			return nil
		}
		return ft
	case query.And:
		rest := make(query.And, 0, len(ft))
		for _, sub := range ft {
			if r := knnResidualFilter(sub); r != nil {
				rest = append(rest, r)
			}
		}
		switch len(rest) {
		case 0:
			return nil
		case 1:
			return rest[0]
		default:
			return rest
		}
	case *query.And:
		return knnResidualFilter(query.And(*ft))
	default:
		return f
	}
}

// isAllQueryFilter reports whether f is the match-all filter. Distinct from
// nil: knnResidualFilter never produces All{}, but a programmatic caller can
// hand us And{Key{v,Knn}, All{}}, and All must not count as "has residual".
func isAllQueryFilter(f query.Filter) bool {
	_, isAll := f.(query.All)
	return isAll
}

// resolveKnnIndex picks the vector index a $knn clause searches. With $index
// set, the name must exist and be a vector index on the field; without it, the
// field must carry exactly one vector index — with two, the searched index (and
// therefore the selected documents, on Delete too) would depend on load order.
func resolveKnnIndex(vidxs []*vectorIndex, field, indexName string) (*vectorIndex, error) {
	var match *vectorIndex
	for _, v := range vidxs {
		if v.info.Vector.Field != field {
			continue
		}
		if indexName != "" {
			if v.info.Name == indexName {
				return v, nil
			}
			continue
		}
		if match != nil {
			return nil, fmt.Errorf("%w: field %q", ErrAmbiguousVectorIndex, field)
		}
		match = v
	}
	if match == nil {
		if indexName != "" {
			return nil, fmt.Errorf("%w: %q ($index %q matches no vector index on that field)", ErrNoVectorIndex, field, indexName)
		}
		return nil, fmt.Errorf("%w: %q", ErrNoVectorIndex, field)
	}
	return match, nil
}

// rejectLegacyVectorClause errors on the pre-$knn ANN spelling: an equality of
// a plain dim-sized numeric ARRAY against a vector-indexed field, anywhere in
// the tree (full walk — Or/Nor/Not/Key included, so no placement smuggles one
// past). Scoped to TypeArray only, NEVER TypeVectorF32: $eq against a packed
// {"$vector":[…]} literal is ordinary byte equality (Rule V defines it), and
// catching it would make exact-vector equality unexpressible. The == dim
// requirement means {"v":{"$eq":[]}} is never caught (validateVectorParams
// forbids dim == 0).
func rejectLegacyVectorClause(f query.Filter, vidxs []*vectorIndex) error {
	if f == nil || len(vidxs) == 0 {
		return nil
	}
	return rejectLegacyWalk(f, "", vidxs)
}

func rejectLegacyWalk(f query.Filter, path string, vidxs []*vectorIndex) error {
	each := func(fs []query.Filter) error {
		for _, sub := range fs {
			if err := rejectLegacyWalk(sub, path, vidxs); err != nil {
				return err
			}
		}
		return nil
	}
	switch ft := f.(type) {
	case query.And:
		return each(ft)
	case *query.And:
		return each(*ft)
	case query.Or:
		return each(ft)
	case *query.Or:
		return each(*ft)
	case query.Nor:
		return each(ft)
	case *query.Nor:
		return each(*ft)
	case query.Not:
		return rejectLegacyWalk(ft.Filter, path, vidxs)
	case *query.Not:
		return rejectLegacyWalk(ft.Filter, path, vidxs)
	case query.Key:
		return rejectLegacyKey(ft, path, vidxs)
	case *query.Key:
		return rejectLegacyKey(*ft, path, vidxs)
	case *query.Comp:
		// The legacy trigger is $eq — the old ANN spelling — AND $ne: on
		// packed storage a plain-array operand never byte-equals the stored
		// TypeVectorF32 value, so a dim-sized-array $ne is true for EVERY
		// document (a "delete all but this vector" that removes that vector
		// too, silently). Both are the same ANN-shaped-literal mistake.
		// (Deliberate widening of the design's Eq-only scoping — the design's
		// own rationale, "loud error over two silent wrong answers", applies
		// with more force to the match-all direction.)
		if path == "" || (ft.CompOp != query.CompOpEq && ft.CompOp != query.CompOpNe) {
			return nil
		}
		raw := ft.EqValue
		if len(raw) == 0 || raw[0] != byte(anyenc.TypeArray) {
			return nil
		}
		for _, vi := range vidxs {
			if vi.info.Vector.Field != path {
				continue
			}
			if vec, ok := anyenc.AppendFloat32s(raw, nil); ok && len(vec) == vi.dim {
				return fmt.Errorf("%w (field %q, index %q)", ErrLegacyVectorClause, path, vi.info.Name)
			}
		}
	}
	return nil
}

func rejectLegacyKey(ft query.Key, path string, vidxs []*vectorIndex) error {
	p := strings.Join(ft.Path, ".")
	if path != "" {
		p = path + "." + p
	}
	return rejectLegacyWalk(ft.Filter, p, vidxs)
}

// knnEf resolves the ANN candidate depth. PURE function of the clause (+
// whether a residual filter exists). q.limit / q.offset / q.sort are
// deliberately NOT inputs, so every verb computes the same ef and walks the
// same beam — that is what makes Count == len(Iter) a theorem instead of a
// hope.
func knnEf(explicit, indexDefault, k int, hasResidual bool) int {
	ef := explicit
	if ef == 0 {
		ef = indexDefault
		want := k
		if hasResidual {
			want = k * vectorOverFetch
		}
		if ef < want {
			ef = want
		}
		if c := max(vectorEfCap, k); ef > c {
			ef = c // the cap may NEVER starve k
		}
	}
	if ef < k {
		ef = k
	}
	return ef
}

// filterRefsField reports whether the filter tree references the given field
// path (used to detect _distance outside a vector query). FULL walk —
// And/Or/Nor/Not/Key, value and pointer forms — so no placement hides a
// reference. A nested Key's path is RELATIVE to its parent: the walk descends
// with the remaining suffix, so Key{["a"], Key{["_distance"], …}} refers to
// the stored field "a._distance", not the synthetic top-level "_distance".
func filterRefsField(f query.Filter, field string) bool {
	switch v := f.(type) {
	case query.And:
		return anyRefsField(v, field)
	case *query.And:
		return anyRefsField(*v, field)
	case query.Or:
		return anyRefsField(v, field)
	case *query.Or:
		return anyRefsField(*v, field)
	case query.Nor:
		return anyRefsField(v, field)
	case *query.Nor:
		return anyRefsField(*v, field)
	case query.Not:
		return filterRefsField(v.Filter, field)
	case *query.Not:
		return filterRefsField(v.Filter, field)
	case query.Key:
		return keyRefsField(v, field)
	case *query.Key:
		return keyRefsField(*v, field)
	}
	return false
}

func anyRefsField(fs []query.Filter, field string) bool {
	for _, c := range fs {
		if filterRefsField(c, field) {
			return true
		}
	}
	return false
}

func keyRefsField(v query.Key, field string) bool {
	p := strings.Join(v.Path, ".")
	if p == field {
		return true
	}
	// Nested Keys address sub-paths of this Key: only the matching suffix of
	// field can still be referenced below.
	if suffix, ok := strings.CutPrefix(field, p+"."); ok {
		return filterRefsField(v.Filter, suffix)
	}
	return false
}

// sortRefsField reports whether any sort key is the given field.
func sortRefsField(s query.Sort, field string) bool {
	if s == nil {
		return false
	}
	for _, sf := range s.Fields() {
		if sf.Field == field {
			return true
		}
	}
	return false
}

// bruteVectorCandidates scans the whole collection and returns the documents
// ranked by exact distance to qv (ascending, ties by docId) — the candidate
// source for a brute-force vector index. It uses the same distance kernel as
// the ANN index, so rankings are identical. topK > 0 bounds the result to the
// k closest documents via a max-heap (only valid when the whole set is not
// needed downstream); topK == 0 returns every document, still ranked.
//
// The scan never builds a document tree: the vector field's raw bytes are
// read with anyenc's lazy RawByPath into a reused buffer, and the docId is
// the cursor key itself (the data namespace is keyed by encoded id). All
// per-document state lives in reused buffers; the candidates' ids share one
// arena.
func (c *collection) bruteVectorCandidates(tx *btree.ReadTx, vi *vectorIndex, qv []float32, topK int) ([]qplanner.VectorCandidate, error) {
	dist := vindex.DistanceFor(vi.info.Vector.Metric.toVindex())
	cursor := tx.NewCursor(c.ns)
	defer cursor.Close()
	if err := cursor.First(); err != nil {
		return nil, err
	}
	type cand struct {
		d     float32
		idOff uint32
		idLen uint32
	}
	var (
		p      anyenc.Parser
		vbuf   = make([]float32, 0, vi.dim)
		valBuf []byte
		cands  []cand
		arena  []byte
	)
	idOf := func(cd cand) []byte { return arena[cd.idOff : cd.idOff+cd.idLen] }
	// less orders by distance ascending, ties by docId bytes ascending — the
	// same deterministic order the previous _distance SortIter produced.
	less := func(a, b cand) bool {
		if a.d != b.d {
			return a.d < b.d
		}
		return bytes.Compare(idOf(a), idOf(b)) < 0
	}
	for cursor.Valid() {
		var err error
		valBuf, err = cursor.AppendValue(valBuf[:0])
		if err != nil {
			return nil, err
		}
		vec, ok, err := p.Float32sByPath(valBuf, vbuf[:0], vi.fieldPath...)
		if err != nil {
			return nil, err
		}
		vbuf = vec
		if !ok || len(vec) != vi.dim {
			if err = cursor.Next(); err != nil {
				return nil, err
			}
			continue
		}
		d := dist(qv, vec)
		if topK > 0 && len(cands) == topK && !(d < cands[0].d) {
			// Heap is full and this doc can't beat the current k-th best
			// (on a distance tie the incumbent wins, matching stable order:
			// the incumbent has the smaller docId — ids arrive in ascending
			// cursor order).
			if err = cursor.Next(); err != nil {
				return nil, err
			}
			continue
		}
		key, err := cursor.Key()
		if err != nil {
			return nil, err
		}
		nc := cand{d: d, idOff: uint32(len(arena)), idLen: uint32(len(key))}
		arena = append(arena, key...)
		if topK > 0 {
			// Max-heap by less (root = worst kept candidate).
			if len(cands) == topK {
				cands[0] = nc
				heapSiftDown(cands, 0, less)
			} else {
				cands = append(cands, nc)
				heapSiftUp(cands, len(cands)-1, less)
			}
		} else {
			cands = append(cands, nc)
		}
		if err = cursor.Next(); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(cands, func(a, b cand) int {
		if less(a, b) {
			return -1
		}
		if less(b, a) {
			return 1
		}
		return 0
	})
	out := make([]qplanner.VectorCandidate, len(cands))
	for i, cd := range cands {
		out[i] = qplanner.VectorCandidate{DocId: idOf(cd), Distance: cd.d}
	}
	return out, nil
}

// heapSiftUp/heapSiftDown maintain a max-heap where less(a, b) means a is
// BETTER than b (the root is the worst kept candidate, evicted first).
func heapSiftUp[T any](h []T, i int, less func(a, b T) bool) {
	for i > 0 {
		parent := (i - 1) / 2
		if !less(h[parent], h[i]) {
			return
		}
		h[parent], h[i] = h[i], h[parent]
		i = parent
	}
}

func heapSiftDown[T any](h []T, i int, less func(a, b T) bool) {
	n := len(h)
	for {
		worst := i
		if l := 2*i + 1; l < n && less(h[worst], h[l]) {
			worst = l
		}
		if r := 2*i + 2; r < n && less(h[worst], h[r]) {
			worst = r
		}
		if worst == i {
			return
		}
		h[i], h[worst] = h[worst], h[i]
		i = worst
	}
}

// vectorOverFetch is how many ANN candidates to fetch per requested result when
// a residual filter is present, so post-filtering rarely under-fills the limit
// (akin to MongoDB's numCandidates ≈ 10–20× limit guidance).
const vectorOverFetch = 10

// vectorEfCap bounds the auto-sized candidate list so a huge limit + selective
// filter can't trigger a pathologically wide search. Callers that genuinely need
// more set VectorEf explicitly.
const vectorEfCap = 4096

// CompactVectorIndex rebuilds the named vector index from its live vectors,
// reclaiming the storage and graph quality lost to tombstoned (deleted or
// replaced) nodes and re-densifying labels. It is synchronous and single-writer:
// it holds the write lock for the whole O(live) rebuild, so prefer a maintenance
// window for large indexes. It is a no-op when there is nothing to reclaim or
// for a brute-force index (no graph). Returns ErrIndexNotFound if no vector index
// with that name exists.
func (c *collection) CompactVectorIndex(ctx context.Context, indexName string) error {
	return c.db.doWriteTxW(ctx, func(wtx WriteTx, tx *btree.WriteTx) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		if err := c.alive(); err != nil {
			return err
		}
		cur := c.loadVectorIndexes()
		idx := -1
		for i, vi := range cur {
			if vi.info.Name == indexName {
				idx = i
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("%w: vector index %q", ErrIndexNotFound, indexName)
		}
		vi := cur[idx]
		if vi.ix == nil && vi.ivf == nil {
			return nil // brute-force: no index to compact
		}
		// Recreating the namespaces moves their root pages; MarkSchemaChanged so
		// peers reconcile and reopen the index with fresh handles. (For IVF this
		// re-trains the codebooks from the live set — see vectorIndex.compact.)
		tx.MarkSchemaChanged()
		nvi, err := vi.compact(tx, c.name)
		if err != nil {
			return err
		}
		// This is an index-set publication inside an uncommitted tx, exactly
		// like createIndexes': a rollback (ambient tx, or an error later in
		// this one) reverts the namespace recreation and frees the compacted
		// roots, so the pre-compaction snapshot must be restored — a handle
		// left pointing at freed pages fails every subsequent vector op with
		// "btree: key not found" until reopen. The restore undo also raises
		// indexSetDDLTxs, so a concurrent read tx's reconcile (reacting to
		// the cookie bump above) cannot rebuild the set from its older
		// snapshot mid-tx.
		c.registerIndexSetRestore(wtx)
		// Visibility (see forTx): until commit, the compacted roots exist
		// only in this tx's view. The stamp is the cookie this commit will
		// publish (MarkSchemaChanged above guarantees the bump) — a
		// concurrent reader fails the generation-interval walk on this
		// handle and is served through prev instead.
		nvi.bindIdentity(c.name)
		nvi.validFromCookie = tx.DiskSchemaCookie() + 1
		// prev must be a COMMITTED fallback: with chained same-tx DDL (create
		// then compact, or compact twice) the replaced handle is itself
		// pending this tx's commit (stamped past the begin cookie —
		// single-writer, so only this tx can have published it) and would
		// route concurrent readers onto namespaces that exist only in this
		// tx's view — inherit the chain's committed tail instead (nil when
		// the index was created in this tx: nothing committed to serve, the
		// reader errors as before the DDL began).
		prevTarget := vi
		if vi.validFromCookie > tx.DiskSchemaCookie() {
			prevTarget = vi.prev.Load()
		}
		nvi.prev.Store(prevTarget)
		wtx.onCommitPublish(func() {
			// Committed: readers at the new cookie resolve nvi on the fast
			// path; drop the chain so predecessors are not pinned. A later
			// reader on a pre-compaction snapshot is served by forTx's
			// transient rebuild.
			nvi.prev.Store(nil)
		})
		next := make([]*vectorIndex, len(cur))
		copy(next, cur)
		next[idx] = nvi
		c.storeVectorIndexes(next)
		return nil
	})
}

// maybeAutoCompactVectors runs synchronous auto-compaction for any vector index
// whose tombstone ratio crossed its CompactRatio. It is invoked after a
// self-contained write commits. It MUST NOT run inside a caller-managed
// transaction — compaction needs its own committed tx — so it no-ops when ctx
// already carries one (the caller can compact manually after their tx).
func (c *collection) maybeAutoCompactVectors(ctx context.Context) {
	if ctx.Value(ctxKeyTx) != nil {
		return
	}
	vidxs := c.loadVectorIndexes()
	enabled := false
	for _, vi := range vidxs {
		if (vi.ix != nil || vi.ivf != nil) && vi.compactRatio > 0 {
			enabled = true
			break
		}
	}
	if !enabled {
		return
	}
	// The threshold check only reads each index's meta record (live/deleted
	// counts), so a fast read tx — no checkStale/reconcile/sketch-reload — is
	// enough. A momentarily stale count at worst defers a compaction by one write,
	// which is harmless for this heuristic.
	var due []string
	if rtx, rerr := c.db.btreeDB.BeginReadFast(); rerr == nil {
		for _, vi := range vidxs {
			if over, err := vi.overThreshold(rtx); err == nil && over {
				due = append(due, vi.info.Name)
			}
		}
		_ = rtx.Rollback()
	}
	for _, name := range due {
		// Best-effort: a concurrent writer may have compacted already, in which
		// case Compact is a no-op. Errors here must not fail the user's write.
		_ = c.CompactVectorIndex(ctx, name)
	}
}

// vectorIndexSeed derives a deterministic level-generation seed per index so a
// reopened index generates the same levels as documents are (re)built. The graph
// itself is persisted, so this only matters for newly inserted nodes after open.
// A collection rename changes the seed for subsequently opened handles; only
// new-insert level draws differ — persisted nodes carry their levels in the
// graph, and HNSW needs the right level distribution, not any particular
// sequence.
func vectorIndexSeed(coll, name string) int64 {
	var h int64 = 1469598103934665603
	for _, s := range []string{coll, ":", name} {
		for i := 0; i < len(s); i++ {
			h ^= int64(s[i])
			h *= 1099511628211
		}
	}
	if h < 0 {
		h = -h
	}
	return h
}
