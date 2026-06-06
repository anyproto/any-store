package anystore

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/internal/vindex"
	"github.com/anyproto/any-store/v2/query"
)

// VectorHit is a single vector-search result.
type VectorHit struct {
	// DocId is the marshaled document id (as stored by the collection).
	DocId []byte
	// Distance to the query (smaller is closer).
	Distance float32
}

// vectorIndex wraps a btree-resident HNSW (internal/vindex) and bridges it to
// any-store documents: it extracts the embedding from the document's vector
// field and keys the graph by the document id. It mutates inside the collection
// write transaction, exactly like the range index's insertKeys/deleteKeys.
type vectorIndex struct {
	info      IndexInfo
	fieldPath []string
	dim       int
	mode      VectorMode
	// ix is the btree-resident HNSW graph for btree/hybrid modes; it is nil for
	// brute-force mode, which stores no index data (search scans the documents).
	ix *vindex.Index
}

func vectorIndexNsPrefix(collName, indexName string) string {
	return "vix:" + collName + ":" + indexName
}

// dropVectorIndexNamespaces deletes all btree namespaces backing a vector index.
func dropVectorIndexNamespaces(tx *btree.WriteTx, collName, indexName string) error {
	prefix := vectorIndexNsPrefix(collName, indexName)
	for _, suf := range []string{":meta", ":vec", ":adj", ":doc", ":lbl"} {
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
	default:
		return fmt.Errorf("vector index: unknown mode %d", p.Mode)
	}
	return nil
}

func newVectorIndexFromVindex(info IndexInfo, ix *vindex.Index) *vectorIndex {
	return &vectorIndex{
		info:      info,
		fieldPath: strings.Split(info.Vector.Field, "."),
		dim:       info.Vector.Dim,
		mode:      info.Vector.Mode,
		ix:        ix,
	}
}

// extractVector reads the embedding from a document, returning ok=false when the
// field is absent or not a numeric array (the document is simply not indexed).
func (vi *vectorIndex) extractVector(it item, buf []float32) ([]float32, bool) {
	v := it.Value().Get(vi.fieldPath...)
	if v == nil || v.Type() != anyenc.TypeArray {
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
	if vi.ix == nil {
		return nil
	}
	newVec, newOk := vi.extractVector(it, nil)
	oldVec, oldOk := vi.extractVector(prevIt, nil)
	switch {
	case newOk && oldOk:
		if float32sEqual(newVec, oldVec) {
			return nil // unchanged — leave the graph alone
		}
		return vi.ix.Insert(tx, it.appendId(nil), newVec)
	case newOk:
		return vi.ix.Insert(tx, it.appendId(nil), newVec)
	case oldOk:
		_, err := vi.ix.Delete(tx, it.appendId(nil))
		return err
	}
	return nil
}

func float32sEqual(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (vi *vectorIndex) Info() IndexInfo { return vi.info }

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
	ix, err := vindex.OpenTx(tx, prefix, vectorIndexSeed(c.name, info.Name))
	if err != nil {
		return nil, err
	}
	ix.SetHybrid(info.Vector.Mode == VectorModeHybrid)
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
	p := vindex.Params{
		Dim:            info.Vector.Dim,
		Metric:         info.Vector.Metric.toVindex(),
		M:              info.Vector.M,
		EfConstruction: info.Vector.EfConstruction,
		EfSearch:       info.Vector.EfSearch,
		Quantization:   info.Vector.Quantization.toVindex(),
	}
	ix, err := vindex.Create(tx, prefix, p, vectorIndexSeed(c.name, info.Name))
	if err != nil {
		return nil, err
	}
	ix.SetHybrid(info.Vector.Mode == VectorModeHybrid)
	vi := newVectorIndexFromVindex(info, ix)

	// Build from existing documents.
	vbuf := make([]float32, 0, vi.dim)
	cursor := tx.NewCursor(c.ns)
	defer cursor.Close()
	if err = cursor.First(); err != nil {
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
		if err = vi.insert(tx, it, vbuf); err != nil {
			return nil, err
		}
		if err = cursor.Next(); err != nil {
			return nil, err
		}
	}
	return vi, nil
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
		if existing, ok := byName[info.Name]; ok {
			rebuilt = append(rebuilt, existing)
			continue
		}
		vi, err := c.loadVectorIndex(tx, info)
		if err != nil {
			// not resolvable in this snapshot — skip this round
			changed = true
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

// detectVectorQuery inspects the parsed filter for an equality on a
// vector-indexed field (`{vectorField: [..]}`). If found it returns a planner
// spec and the residual filter (the original filter minus the vector clause —
// keeping _distance predicates and any other field filters). Returns
// (nil, original, nil) when this is not a vector query.
func (q *collQuery) detectVectorQuery() (*qplanner.VectorQuerySpec, query.Filter, error) {
	vidxs := q.c.loadVectorIndexes()
	if len(vidxs) == 0 || q.cond == nil {
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
		path, comp, ok := asEqClause(cl)
		if !ok {
			continue
		}
		field := strings.Join(path, ".")
		for _, v := range vidxs {
			if v.info.Vector.Field != field {
				continue
			}
			vec, derr := decodeVectorValue(comp.EqValue, v.dim)
			if derr != nil {
				continue // value isn't a dim-sized numeric array — not an ANN query
			}
			if vecIdx >= 0 {
				return nil, nil, ErrMultipleVectorClauses
			}
			vecIdx, vi, qvec = i, v, vec
		}
	}
	if vecIdx < 0 {
		return nil, q.cond, nil
	}

	residual := residualFilter(clauses, vecIdx)
	captured := vi
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
	ef := chooseEf(int(q.vectorEf), vi.ix.EfSearch(), int(q.limit), residual != nil)
	spec := &qplanner.VectorQuerySpec{
		Query: qvec,
		Ef:    ef,
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

// asEqClause returns the field path and Comp for a `{field: value}` equality
// clause (a query.Key wrapping a CompOpEq), else ok=false.
func asEqClause(cl query.Filter) (path []string, comp *query.Comp, ok bool) {
	k, isKey := cl.(query.Key)
	if !isKey {
		return nil, nil, false
	}
	c, isComp := k.Filter.(*query.Comp)
	if !isComp || c.CompOp != query.CompOpEq {
		return nil, nil, false
	}
	return k.Path, c, true
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

// VectorSearch returns the k nearest documents to query under the named vector
// index. efSearch <= 0 uses the index default.
func (c *collection) VectorSearch(ctx context.Context, indexName string, query []float32, k, efSearch int) (hits []VectorHit, err error) {
	vi := c.findVectorIndex(indexName)
	if vi == nil {
		return nil, fmt.Errorf("%w: vector index %q", ErrIndexNotFound, indexName)
	}
	if vi.ix == nil {
		// Brute-force: scan all docs, sort by distance, take top-k.
		err = c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
			cands, serr := c.bruteVectorCandidates(tx, vi, query)
			if serr != nil {
				return serr
			}
			sort.Slice(cands, func(i, j int) bool { return cands[i].Distance < cands[j].Distance })
			if k > 0 && len(cands) > k {
				cands = cands[:k]
			}
			hits = make([]VectorHit, len(cands))
			for i, cnd := range cands {
				hits[i] = VectorHit{DocId: cnd.DocId, Distance: cnd.Distance}
			}
			return nil
		})
		return hits, err
	}
	err = c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		raw, serr := vi.ix.Search(tx, query, k, efSearch)
		if serr != nil {
			return serr
		}
		hits = make([]VectorHit, len(raw))
		for i, h := range raw {
			hits[i] = VectorHit{DocId: h.DocID, Distance: h.Distance}
		}
		return nil
	})
	return hits, err
}

func (c *collection) findVectorIndex(name string) *vectorIndex {
	for _, vi := range c.loadVectorIndexes() {
		if vi.info.Name == name {
			return vi
		}
	}
	return nil
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
