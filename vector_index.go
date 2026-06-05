package anystore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/vindex"
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
	ix        *vindex.Index
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
	return nil
}

func newVectorIndexFromVindex(info IndexInfo, ix *vindex.Index) *vectorIndex {
	return &vectorIndex{
		info:      info,
		fieldPath: strings.Split(info.Vector.Field, "."),
		dim:       info.Vector.Dim,
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
func (vi *vectorIndex) insert(tx *btree.WriteTx, it item, vbuf []float32) error {
	vec, ok := vi.extractVector(it, vbuf)
	if !ok {
		return nil
	}
	return vi.ix.Insert(tx, it.appendId(nil), vec)
}

// delete removes the document's vector (no-op if it was never indexed).
func (vi *vectorIndex) delete(tx *btree.WriteTx, it item) error {
	_, err := vi.ix.Delete(tx, it.appendId(nil))
	return err
}

// update reindexes only when the embedding changed: skip if equal, delete if the
// field went away, otherwise insert (which replaces the old node). This avoids
// re-inserting into the HNSW graph on every unrelated document update.
func (vi *vectorIndex) update(tx *btree.WriteTx, prevIt, it item) error {
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
	prefix := vectorIndexNsPrefix(c.name, info.Name)
	ix, err := vindex.OpenTx(tx, prefix, vectorIndexSeed(c.name, info.Name))
	if err != nil {
		return nil, err
	}
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
	prefix := vectorIndexNsPrefix(c.name, info.Name)
	p := vindex.Params{
		Dim:            info.Vector.Dim,
		Metric:         info.Vector.Metric.toVindex(),
		M:              info.Vector.M,
		EfConstruction: info.Vector.EfConstruction,
		EfSearch:       info.Vector.EfSearch,
	}
	ix, err := vindex.Create(tx, prefix, p, vectorIndexSeed(c.name, info.Name))
	if err != nil {
		return nil, err
	}
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

// VectorSearch returns the k nearest documents to query under the named vector
// index. efSearch <= 0 uses the index default.
func (c *collection) VectorSearch(ctx context.Context, indexName string, query []float32, k, efSearch int) (hits []VectorHit, err error) {
	vi := c.findVectorIndex(indexName)
	if vi == nil {
		return nil, fmt.Errorf("%w: vector index %q", ErrIndexNotFound, indexName)
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
