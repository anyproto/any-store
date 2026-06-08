package anystore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

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
}

// isIVF reports whether this index uses the IVF-PQ backend.
func (vi *vectorIndex) isIVF() bool { return vi.mode.isIVFPQ() }

func vectorIndexNsPrefix(collName, indexName string) string {
	return "vix:" + collName + ":" + indexName
}

// dropVectorIndexNamespaces deletes all btree namespaces backing a vector index.
// It drops the union of HNSW (:adj) and IVF-PQ (:cb, :cell) suffixes so it works
// for either backend; DeleteNamespace ignores absent namespaces.
func dropVectorIndexNamespaces(tx *btree.WriteTx, collName, indexName string) error {
	prefix := vectorIndexNsPrefix(collName, indexName)
	for _, suf := range []string{":meta", ":vec", ":adj", ":doc", ":lbl", ":cb", ":cell"} {
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
	case VectorModeIVFPQ:
		m := ivfM(p)
		if p.Dim%m != 0 {
			return fmt.Errorf("vector index: IVF-PQ requires Dim (%d) divisible by M (%d)", p.Dim, m)
		}
	default:
		return fmt.Errorf("vector index: unknown mode %d", p.Mode)
	}
	return nil
}

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
	nl := 4 * int(math.Sqrt(float64(n)))
	if nl < 16 {
		nl = 16
	}
	if nl > 65536 {
		nl = 65536
	}
	if minCells := n / 39; minCells >= 1 && nl > minCells {
		nl = minCells // keep ≥~39 training points per centroid (FAISS floor)
	}
	if n > 0 && nl > n {
		nl = n // can't have more cells than points
	}
	if nl < 1 {
		nl = 1
	}
	return nl
}

// ivfStoreParams builds the vivf build/open parameters from VectorParams.
func ivfStoreParams(p *VectorParams, n int) vivf.StoreParams {
	closure := p.Closure
	if closure < 1 {
		closure = 1
	}
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
		KMeansPP:   true,
		PrecompMiB: p.PrecomputeTableMiB,
	}
}

func newVectorIndexFromVindex(info IndexInfo, ix *vindex.Index) *vectorIndex {
	return &vectorIndex{
		info:         info,
		fieldPath:    strings.Split(info.Vector.Field, "."),
		dim:          info.Vector.Dim,
		mode:         info.Vector.Mode,
		compactRatio: info.Vector.CompactRatio,
		ix:           ix,
	}
}

func newVectorIndexFromIVF(info IndexInfo, ivf *vivf.StoreIndex) *vectorIndex {
	return &vectorIndex{
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
		return vi.ivf.Insert(tx, it.appendId(nil), vec)
	}
	if vi.ix == nil {
		return nil
	}
	vec, ok := vi.extractVector(it, vbuf)
	if !ok {
		return nil
	}
	return vi.ix.Insert(tx, it.appendId(nil), vec)
}

// delete removes the document's vector (no-op if it was never indexed).
func (vi *vectorIndex) delete(tx *btree.WriteTx, it item) error {
	if vi.isIVF() {
		if vi.ivf == nil {
			return nil
		}
		_, err := vi.ivf.Delete(tx, it.appendId(nil))
		return err
	}
	if vi.ix == nil {
		return nil
	}
	_, err := vi.ix.Delete(tx, it.appendId(nil))
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
			return vi.ivf.Insert(tx, it.appendId(nil), newVec) // Insert replaces
		case oldOk:
			_, err := vi.ivf.Delete(tx, it.appendId(nil))
			return err
		}
		return nil
	}
	switch {
	case newOk:
		return vi.ix.Insert(tx, it.appendId(nil), newVec)
	case oldOk:
		_, err := vi.ix.Delete(tx, it.appendId(nil))
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
		return newVectorIndexFromIVF(vi.info, ivf), nil
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
	return newVectorIndexFromVindex(vi.info, ix), nil
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
func (c *collection) loadVectorIndex(tx *btree.ReadTx, info IndexInfo) (*vectorIndex, error) {
	if err := validateVectorParams(info.Vector); err != nil {
		return nil, err
	}
	if info.Vector.Mode.isBruteForce() {
		// No on-disk graph to open — the index is metadata only.
		return newVectorIndexFromVindex(info, nil), nil
	}
	prefix := vectorIndexNsPrefix(c.name, info.Name)
	if info.Vector.Mode.isIVFPQ() {
		ivf, err := vivf.OpenTx(tx, prefix)
		if err != nil {
			return nil, err
		}
		return newVectorIndexFromIVF(info, ivf), nil
	}
	ix, err := vindex.OpenTx(tx, prefix, vectorIndexSeed(c.name, info.Name))
	if err != nil {
		return nil, err
	}
	hybrid := info.Vector.Mode == VectorModeHybrid
	ix.SetHybrid(hybrid)
	ix.SetVectorCache(hybrid && info.Vector.HybridCacheVectors)
	return newVectorIndexFromVindex(info, ix), nil
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
	if err := c.db.registerIndex(tx, c.name, info); err != nil {
		return nil, err
	}
	if info.Vector.Mode.isBruteForce() {
		// Brute-force: no namespaces, no graph, nothing to build from documents.
		// The persisted metadata above is the entire index.
		return newVectorIndexFromVindex(info, nil), nil
	}
	prefix := vectorIndexNsPrefix(c.name, info.Name)

	// Collect (id, vector) from existing documents, then build the index in RAM
	// and flush it in one bulk pass (vindex.BulkBuild) — far faster than inserting
	// node-by-node through the btree (no per-edge copy-on-write churn). The bulk path
	// produces a graph byte-identical to the per-insert path, so it is a pure speedup.
	// NOTE: this holds every indexed vector in RAM during the build; for very large
	// collections a maintenance_work_mem-style cap + fallback to per-insert is future
	// work (see BUILD_SPEED.md).
	tmpVI := newVectorIndexFromVindex(info, nil) // extractVector needs fieldPath/dim
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
			ids = append(ids, it.appendId(nil))
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
	if info.Vector.Mode.isIVFPQ() {
		if len(vecs) == 0 {
			return nil, fmt.Errorf("vector index: IVF-PQ requires existing documents to train — insert documents before creating the index")
		}
		ivf, err := vivf.BulkBuild(tx, prefix, ivfStoreParams(info.Vector, len(vecs)), ids, vecs)
		if err != nil {
			return nil, err
		}
		return newVectorIndexFromIVF(info, ivf), nil
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
	return newVectorIndexFromVindex(info, ix), nil
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

// ErrMultipleVectorClauses is returned when a query constrains more than one
// vector-indexed field (unsupported).
var ErrMultipleVectorClauses = errors.New("any-store: query has multiple vector clauses")

// ErrInvalidVectorQuery is returned when a clause targets a vector-indexed field
// but isn't a valid ANN clause: it must be an equality against a numeric array of
// the index's dimension, e.g. {embedding: [..dim floats..]}.
var ErrInvalidVectorQuery = errors.New("any-store: invalid vector query clause")

// ErrDistanceWithoutVector is returned when the synthetic _distance field is used
// in a filter or sort but the query has no vector clause to produce it.
var ErrDistanceWithoutVector = errors.New("any-store: _distance is only available in a vector query")

// detectVectorQuery inspects the parsed filter for an equality on a
// vector-indexed field (`{vectorField: [..]}`). If found it returns a planner
// spec and the residual filter (the original filter minus the vector clause —
// keeping _distance predicates and any other field filters). Returns
// (nil, original, nil) when this is not a vector query.
func (q *collQuery) detectVectorQuery() (*qplanner.VectorQuerySpec, query.Filter, error) {
	vidxs := q.c.loadVectorIndexes()
	if len(vidxs) == 0 || q.cond == nil {
		// No vector clause is possible, so _distance (in filter or sort) is invalid.
		if filterRefsField(q.cond, qplanner.DistanceField) || sortRefsField(q.sort, qplanner.DistanceField) {
			return nil, nil, ErrDistanceWithoutVector
		}
		return nil, q.cond, nil
	}

	var clauses []query.Filter
	switch f := q.cond.(type) {
	case query.And:
		clauses = f
	default:
		clauses = []query.Filter{q.cond}
	}

	vecIdx := -1
	var vi *vectorIndex
	var qvec []float32
	for i, cl := range clauses {
		k, isKey := cl.(query.Key)
		if !isKey {
			continue
		}
		field := strings.Join(k.Path, ".")
		v := findVectorIndexByField(vidxs, field)
		if v == nil {
			continue // ordinary (non-vector) field clause
		}
		// The clause targets a vector-indexed field, so it must be a valid ANN
		// clause — an equality against a dim-sized numeric array. Anything else
		// (a range op, a scalar, a wrong-dim array) is a mistake, not a silent
		// fall-through to a literal field match.
		comp, isComp := k.Filter.(*query.Comp)
		if !isComp || comp.CompOp != query.CompOpEq {
			return nil, nil, fmt.Errorf("%w: field %q must be an equality against a %d-dim array", ErrInvalidVectorQuery, field, v.dim)
		}
		vec, derr := decodeVectorValue(comp.EqValue, v.dim)
		if derr != nil {
			return nil, nil, fmt.Errorf("%w: field %q: %v", ErrInvalidVectorQuery, field, derr)
		}
		if vecIdx >= 0 {
			return nil, nil, ErrMultipleVectorClauses
		}
		vecIdx, vi, qvec = i, v, vec
	}
	if vecIdx < 0 {
		// Not a vector query: _distance is synthetic and only produced by a vector
		// search, so referencing it in a filter or sort here is an error rather
		// than a silent match-everything / sort-on-nothing.
		if filterRefsField(q.cond, qplanner.DistanceField) || sortRefsField(q.sort, qplanner.DistanceField) {
			return nil, nil, ErrDistanceWithoutVector
		}
		return nil, q.cond, nil
	}

	residual := residualFilter(clauses, vecIdx)
	captured := vi
	if vi.isIVF() {
		// IVF-PQ: probe a few cells (contiguous range scans), re-rank by exact
		// distance. ef is the re-rank depth / candidate count, sized to the page
		// window; nprobe is fixed in the index. SearchCandidates returns them
		// closest-first (Ordered) — measurably faster than letting SortIter sort, as
		// the planner then skips the SortIter for the default distance order and
		// streams straight to LimitIter; an explicit multi-key Sort still uses SortIter.
		ef := chooseEf(int(q.vectorEf), captured.ivf.NProbe()*8, int(q.limit)+int(q.offset), residual != nil)
		spec := &qplanner.VectorQuerySpec{
			Query:   qvec,
			Ef:      ef,
			Ordered: true,
			Search: func(tx *btree.ReadTx, qv []float32, ef int) ([]qplanner.VectorCandidate, error) {
				cands, err := captured.ivf.SearchCandidates(tx, qv, ef)
				if err != nil {
					return nil, err
				}
				out := make([]qplanner.VectorCandidate, len(cands))
				for i, c := range cands {
					out[i] = qplanner.VectorCandidate{DocId: c.DocID, Distance: c.Distance}
				}
				return out, nil
			},
		}
		return spec, residual, nil
	}
	if vi.ix == nil {
		// Brute-force: every document is a candidate. The existing
		// FilterIter -> SortIter -> LimitIter chain then filters, sorts by
		// _distance, and takes top-k — exact, no over-fetch needed.
		spec := &qplanner.VectorQuerySpec{
			Query: qvec,
			Search: func(tx *btree.ReadTx, qv []float32, _ int) ([]qplanner.VectorCandidate, error) {
				return q.c.bruteVectorCandidates(tx, captured, qv)
			},
		}
		return spec, residual, nil
	}
	// Size the candidate list for the whole window the caller will page through
	// (offset+limit), over-fetching when a residual filter will discard some. The
	// limit/offset themselves are still applied downstream by Sort/LimitIter over
	// the post-filter stream — ef only governs how many candidates the ANN yields.
	ef := chooseEf(int(q.vectorEf), vi.ix.EfSearch(), int(q.limit)+int(q.offset), residual != nil)
	spec := &qplanner.VectorQuerySpec{
		Query:   qvec,
		Ef:      ef,
		Ordered: true, // SearchCandidates yields candidates closest-first
		Search: func(tx *btree.ReadTx, qv []float32, ef int) ([]qplanner.VectorCandidate, error) {
			cands, err := captured.ix.SearchCandidates(tx, qv, ef)
			if err != nil {
				return nil, err
			}
			out := make([]qplanner.VectorCandidate, len(cands))
			for i, c := range cands {
				out[i] = qplanner.VectorCandidate{DocId: c.DocID, Distance: c.Distance}
			}
			return out, nil
		},
	}
	return spec, residual, nil
}

// findVectorIndexByField returns the vector index whose embedding field path
// matches field, or nil.
func findVectorIndexByField(vidxs []*vectorIndex, field string) *vectorIndex {
	for _, v := range vidxs {
		if v.info.Vector.Field == field {
			return v
		}
	}
	return nil
}

// filterRefsField reports whether the filter tree references the given field path
// (used to detect _distance outside a vector query). Walks And/Or/Key.
func filterRefsField(f query.Filter, field string) bool {
	switch v := f.(type) {
	case query.And:
		for _, c := range v {
			if filterRefsField(c, field) {
				return true
			}
		}
	case query.Or:
		for _, c := range v {
			if filterRefsField(c, field) {
				return true
			}
		}
	case query.Key:
		return strings.Join(v.Path, ".") == field
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

// decodeVectorValue decodes a marshaled anyenc array into a dim-sized []float32.
func decodeVectorValue(eqValue []byte, dim int) ([]float32, error) {
	var p anyenc.Parser
	v, err := p.Parse(eqValue)
	if err != nil {
		return nil, err
	}
	if v.Type() != anyenc.TypeArray {
		return nil, fmt.Errorf("not an array")
	}
	arr, err := v.Array()
	if err != nil || len(arr) != dim {
		return nil, fmt.Errorf("array length %d != dim %d", len(arr), dim)
	}
	out := make([]float32, dim)
	for i, e := range arr {
		if e.Type() != anyenc.TypeNumber {
			return nil, fmt.Errorf("non-numeric element")
		}
		out[i] = float32(e.GetFloat64())
	}
	return out, nil
}

// bruteVectorCandidates scans the whole collection and returns every document
// with its distance to qv — the candidate source for a brute-force vector index.
// It uses the same distance kernel as the ANN index, so rankings are identical.
func (c *collection) bruteVectorCandidates(tx *btree.ReadTx, vi *vectorIndex, qv []float32) ([]qplanner.VectorCandidate, error) {
	dist := vindex.DistanceFor(vi.info.Vector.Metric.toVindex())
	cursor := tx.NewCursor(c.ns)
	defer cursor.Close()
	if err := cursor.First(); err != nil {
		return nil, err
	}
	var p anyenc.Parser
	buf := make([]float32, 0, vi.dim)
	var out []qplanner.VectorCandidate
	for cursor.Valid() {
		val, err := cursor.Value()
		if err != nil {
			return nil, err
		}
		doc, err := p.Parse(val) // compression-aware
		if err != nil {
			return nil, err
		}
		it := item{val: doc}
		if vec, ok := vi.extractVector(it, buf); ok {
			out = append(out, qplanner.VectorCandidate{DocId: it.appendId(nil), Distance: dist(qv, vec)})
		}
		if err := cursor.Next(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// vectorOverFetch is how many ANN candidates to fetch per requested result when
// a residual filter is present, so post-filtering rarely under-fills the limit
// (akin to MongoDB's numCandidates ≈ 10–20× limit guidance).
const vectorOverFetch = 10

// vectorEfCap bounds the auto-sized candidate list so a huge limit + selective
// filter can't trigger a pathologically wide search. Callers that genuinely need
// more set VectorEf explicitly.
const vectorEfCap = 4096

// chooseEf resolves the ANN candidate-list size (numCandidates). An explicit
// override wins; otherwise start from the index default, ensure it covers the
// limit, and over-fetch when a residual filter will discard candidates.
func chooseEf(explicit, indexDefault, limit int, hasFilter bool) int {
	if explicit > 0 {
		return explicit
	}
	ef := indexDefault
	if limit > 0 {
		want := limit
		if hasFilter {
			want = limit * vectorOverFetch
		}
		if want > ef {
			ef = want
		}
	}
	if ef > vectorEfCap {
		ef = vectorEfCap
	}
	return ef
}

func residualFilter(clauses []query.Filter, skip int) query.Filter {
	rest := make([]query.Filter, 0, len(clauses)-1)
	for i, c := range clauses {
		if i != skip {
			rest = append(rest, c)
		}
	}
	switch len(rest) {
	case 0:
		return nil
	case 1:
		return rest[0]
	default:
		return query.And(rest)
	}
}

// CompactVectorIndex rebuilds the named vector index from its live vectors,
// reclaiming the storage and graph quality lost to tombstoned (deleted or
// replaced) nodes and re-densifying labels. It is synchronous and single-writer:
// it holds the write lock for the whole O(live) rebuild, so prefer a maintenance
// window for large indexes. It is a no-op when there is nothing to reclaim or
// for a brute-force index (no graph). Returns ErrIndexNotFound if no vector index
// with that name exists.
func (c *collection) CompactVectorIndex(ctx context.Context, indexName string) error {
	return c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		c.mu.Lock()
		defer c.mu.Unlock()
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
