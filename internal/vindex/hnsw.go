package vindex

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// Namespace suffixes appended to the index prefix.
const (
	nsMeta = ":meta" // single meta record
	nsVec  = ":vec"  // label -> raw vector            (immutable)
	nsAdj  = ":adj"  // label -> level+flags+neighbours (churns)
	nsDoc  = ":doc"  // docId -> label                 (delete/update by id)
	nsLbl  = ":lbl"  // label -> docId                 (resolve results)
)

// Params configure a freshly created index. Zero fields take defaults.
type Params struct {
	Dim            int
	Metric         Metric
	M              int     // max neighbours on layers >= 1 (default 16)
	EfConstruction int     // candidate list at insert (default 200)
	EfSearch       int     // candidate list at query (default 64)
	Ml             float64 // level factor (default 0.25)
}

func (p *Params) withDefaults() {
	if p.M == 0 {
		p.M = 16
	}
	if p.EfConstruction == 0 {
		p.EfConstruction = 200
	}
	if p.EfSearch == 0 {
		p.EfSearch = 64
	}
	if p.Ml == 0 {
		p.Ml = 0.25
	}
}

// Hit is a search result.
type Hit struct {
	DocID    []byte
	Distance float32
}

// Index is a btree-resident HNSW. It holds no graph state — only the namespace
// handles, the (immutable) parameters, the distance function, and an RNG for
// level generation. Every operation reads/writes through the passed transaction.
type Index struct {
	vmeta, vvec, vadj, vdoc, vlbl *btree.Namespace

	dim int
	m   int
	m0  int
	efC int
	efS int
	ml  float64

	dist DistanceFunc

	rngMu sync.Mutex
	rng   *rand.Rand

	// scratch pools per-operation searchers (heaps, visited set, vector and
	// adjacency buffers) so a steady-state Search/Insert reuses them instead of
	// allocating fresh every call. sync.Pool is safe under concurrent Search.
	scratch sync.Pool
}

// getSearcher returns a pooled searcher bound to rtx/query (or a fresh one).
func (ix *Index) getSearcher(rtx *btree.ReadTx, query []float32) *searcher {
	if s, ok := ix.scratch.Get().(*searcher); ok {
		s.rtx = rtx
		s.query = query
		return s
	}
	return ix.newSearcher(rtx, query)
}

func (ix *Index) putSearcher(s *searcher) {
	s.rtx = nil
	s.query = nil
	ix.scratch.Put(s)
}

func nsNames(prefix string) [5]string {
	return [5]string{prefix + nsMeta, prefix + nsVec, prefix + nsAdj, prefix + nsDoc, prefix + nsLbl}
}

func ensureNS(wtx *btree.WriteTx, name string) (*btree.Namespace, error) {
	ns, err := wtx.GetNamespace(name)
	if err == nil {
		return ns, nil
	}
	if !errors.Is(err, btree.ErrNamespaceNotFound) {
		return nil, err
	}
	return wtx.CreateNamespace(name)
}

// Create initialises a new index: creates the namespaces and writes the initial
// meta record. Idempotent only if the namespaces don't already hold an index.
func Create(wtx *btree.WriteTx, prefix string, p Params, seed int64) (*Index, error) {
	if p.Dim <= 0 {
		return nil, fmt.Errorf("vindex: dim must be > 0")
	}
	p.withDefaults()
	names := nsNames(prefix)
	var ns [5]*btree.Namespace
	for i, name := range names {
		n, err := ensureNS(wtx, name)
		if err != nil {
			return nil, err
		}
		ns[i] = n
	}
	ix := newIndex(ns, p.Dim, p.M, 2*p.M, p.EfConstruction, p.EfSearch, p.Ml, p.Metric, seed)
	mt := &meta{
		dim: p.Dim, metric: p.Metric, m: p.M, m0: 2 * p.M,
		efC: p.EfConstruction, efS: p.EfSearch, ml: p.Ml,
	}
	if err := ix.writeMeta(wtx, mt); err != nil {
		return nil, err
	}
	return ix, nil
}

// Open resolves an existing index, reading its parameters from the meta record.
func Open(db *btree.DB, prefix string, seed int64) (*Index, error) {
	names := nsNames(prefix)
	var ns [5]*btree.Namespace
	for i, name := range names {
		n, err := db.GetNamespace(name)
		if err != nil {
			return nil, err
		}
		ns[i] = n
	}
	rtx, err := db.BeginRead()
	if err != nil {
		return nil, err
	}
	defer rtx.Rollback()
	b, err := rtx.Get(ns[0], metaKey)
	if err != nil {
		return nil, err
	}
	mt, err := decodeMeta(b)
	if err != nil {
		return nil, err
	}
	return newIndex(ns, mt.dim, mt.m, mt.m0, mt.efC, mt.efS, mt.ml, mt.metric, seed), nil
}

// OpenTx resolves an existing index using a caller-provided read transaction
// (no nested BeginRead) — used when opening/reconciling inside another tx.
func OpenTx(rtx *btree.ReadTx, prefix string, seed int64) (*Index, error) {
	names := nsNames(prefix)
	var ns [5]*btree.Namespace
	for i, name := range names {
		n, err := rtx.GetNamespace(name)
		if err != nil {
			return nil, err
		}
		ns[i] = n
	}
	b, err := rtx.Get(ns[0], metaKey)
	if err != nil {
		return nil, err
	}
	mt, err := decodeMeta(b)
	if err != nil {
		return nil, err
	}
	return newIndex(ns, mt.dim, mt.m, mt.m0, mt.efC, mt.efS, mt.ml, mt.metric, seed), nil
}

func newIndex(ns [5]*btree.Namespace, dim, m, m0, efC, efS int, ml float64, metric Metric, seed int64) *Index {
	return &Index{
		vmeta: ns[0], vvec: ns[1], vadj: ns[2], vdoc: ns[3], vlbl: ns[4],
		dim: dim, m: m, m0: m0, efC: efC, efS: efS, ml: ml,
		dist: distanceFor(metric),
		rng:  rand.New(rand.NewSource(seed)),
	}
}

// Dim returns the index dimension.
func (ix *Index) Dim() int { return ix.dim }

func (ix *Index) readMeta(rtx *btree.ReadTx) (*meta, error) {
	b, err := rtx.Get(ix.vmeta, metaKey)
	if err != nil {
		return nil, err
	}
	return decodeMeta(b)
}

func (ix *Index) writeMeta(wtx *btree.WriteTx, mt *meta) error {
	return wtx.Put(ix.vmeta, metaKey, encodeMeta(nil, mt))
}

func (ix *Index) randomLevel() int32 {
	ix.rngMu.Lock()
	defer ix.rngMu.Unlock()
	var l int32
	for ix.rng.Float64() < ix.ml {
		l++
		if l > 30 {
			break
		}
	}
	return l
}

func (ix *Index) maxConn(layer int32) int {
	if layer == 0 {
		return ix.m0
	}
	return ix.m
}

// ---------------------------------------------------------------------------
// Insert
// ---------------------------------------------------------------------------

// Insert adds (or replaces) the vector for docID. If docID is already present
// its old node is tombstoned and a fresh node inserted (HNSW-standard update).
func (ix *Index) Insert(wtx *btree.WriteTx, docID []byte, vec []float32) error {
	if len(vec) != ix.dim {
		return fmt.Errorf("vindex: dim mismatch: got %d want %d", len(vec), ix.dim)
	}
	rtx := &wtx.ReadTx
	mt, err := ix.readMeta(rtx)
	if err != nil {
		return err
	}

	// replace existing
	if old, gerr := rtx.Get(ix.vdoc, docID); gerr == nil {
		oldLabel := binary.LittleEndian.Uint32(old)
		if terr := ix.tombstoneLabel(wtx, oldLabel, mt); terr != nil {
			return terr
		}
	} else if !errors.Is(gerr, btree.ErrKeyNotFound) {
		return gerr
	}

	label := mt.nextLabel
	mt.nextLabel++
	mt.count++
	level := ix.randomLevel()

	var keyArr [4]byte
	key := func(l uint32) []byte {
		binary.BigEndian.PutUint32(keyArr[:], l)
		return keyArr[:]
	}

	if err = wtx.Put(ix.vvec, key(label), f32bytes(vec)); err != nil {
		return err
	}
	var lb [4]byte
	binary.LittleEndian.PutUint32(lb[:], label)
	if err = wtx.Put(ix.vdoc, docID, lb[:]); err != nil {
		return err
	}
	if err = wtx.Put(ix.vlbl, key(label), docID); err != nil {
		return err
	}

	if !mt.hasEntry {
		empty := make([][]uint32, level+1)
		if err = wtx.Put(ix.vadj, key(label), encodeAdj(nil, level, false, empty)); err != nil {
			return err
		}
		mt.hasEntry, mt.entryLabel, mt.topLayer = true, label, level
		return ix.writeMeta(wtx, mt)
	}

	s := ix.getSearcher(rtx, vec)
	defer ix.putSearcher(s)
	ep := mt.entryLabel
	for lc := mt.topLayer; lc > level; lc-- {
		if ep, err = s.greedyClosest(ep, lc); err != nil {
			return err
		}
	}
	start := mt.topLayer
	if level < start {
		start = level
	}

	// Accumulate the new node's neighbours per layer and write its adjacency
	// ONCE after connecting, instead of a read-modify-write per selected
	// neighbour. Only the back-links (neighbour -> new node) need addNeighbor.
	newNbrs := make([][]uint32, level+1)
	for lc := start; lc >= 0; lc-- {
		found, serr := s.searchLayer(ep, ix.efC, lc)
		if serr != nil {
			return serr
		}
		m := ix.maxConn(lc)
		if len(found) > m {
			found = found[:m]
		}
		sel := make([]uint32, len(found))
		for i, c := range found {
			sel[i] = c.label
		}
		newNbrs[lc] = sel
		for _, c := range found {
			if err = ix.addNeighbor(wtx, s, c.label, label, lc); err != nil {
				return err
			}
		}
		if len(found) > 0 {
			ep = found[0].label
		}
	}
	if err = wtx.Put(ix.vadj, key(label), encodeAdj(nil, level, false, newNbrs)); err != nil {
		return err
	}
	if level > mt.topLayer {
		mt.entryLabel, mt.topLayer = label, level
	}
	return ix.writeMeta(wtx, mt)
}

// addNeighbor links b into a's neighbour list at layer, pruning to the farthest
// when the list is full (coder/hnsw's rule). Reads/writes a's adjacency record.
func (ix *Index) addNeighbor(wtx *btree.WriteTx, s *searcher, a, b uint32, layer int32) error {
	rtx := &wtx.ReadTx
	var kbuf []byte
	adjBytes, err := rtx.Get(ix.vadj, labelKey(kbuf, a))
	if err != nil {
		return err
	}
	level, deleted, nbrs, err := decodeAllAdj(adjBytes)
	if err != nil {
		return err
	}
	if layer > level {
		return nil // a doesn't exist at this layer
	}
	list := nbrs[layer]
	for _, x := range list {
		if x == b {
			return nil // already linked
		}
	}
	capn := ix.maxConn(layer)
	if len(list) < capn {
		nbrs[layer] = append(list, b)
	} else {
		// prune: keep the cap closest to a
		aVec, err := s.vec2Of(a)
		if err != nil {
			return err
		}
		bVec, err := s.vecOf(b)
		if err != nil {
			return err
		}
		worstDist := ix.dist(aVec, bVec)
		worstIdx := -1
		for i, x := range list {
			xVec, err := s.vecOf(x)
			if err != nil {
				return err
			}
			if d := ix.dist(aVec, xVec); d > worstDist {
				worstDist, worstIdx = d, i
			}
		}
		if worstIdx >= 0 {
			list[worstIdx] = b
		}
		nbrs[layer] = list
	}
	return wtx.Put(ix.vadj, labelKey(kbuf, a), encodeAdj(nil, level, deleted, nbrs))
}

func (ix *Index) tombstoneLabel(wtx *btree.WriteTx, label uint32, mt *meta) error {
	rtx := &wtx.ReadTx
	var kbuf []byte
	adjBytes, err := rtx.Get(ix.vadj, labelKey(kbuf, label))
	if err != nil {
		return err
	}
	level, deleted, nbrs, err := decodeAllAdj(adjBytes)
	if err != nil {
		return err
	}
	if deleted {
		return nil
	}
	mt.count--
	mt.deletedCount++
	return wtx.Put(ix.vadj, labelKey(kbuf, label), encodeAdj(nil, level, true, nbrs))
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

// Delete tombstones the node for docID. Returns false if docID is not indexed.
func (ix *Index) Delete(wtx *btree.WriteTx, docID []byte) (bool, error) {
	rtx := &wtx.ReadTx
	lb, err := rtx.Get(ix.vdoc, docID)
	if errors.Is(err, btree.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	label := binary.LittleEndian.Uint32(lb)
	mt, err := ix.readMeta(rtx)
	if err != nil {
		return false, err
	}
	if err = ix.tombstoneLabel(wtx, label, mt); err != nil {
		return false, err
	}
	if err = wtx.Delete(ix.vdoc, docID); err != nil {
		return false, err
	}
	return true, ix.writeMeta(wtx, mt)
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

// Search returns the k nearest live documents to query. efSearch <= 0 uses the
// index default.
func (ix *Index) Search(rtx *btree.ReadTx, query []float32, k, efSearch int) ([]Hit, error) {
	if len(query) != ix.dim {
		return nil, fmt.Errorf("vindex: dim mismatch: got %d want %d", len(query), ix.dim)
	}
	mt, err := ix.readMeta(rtx)
	if err != nil {
		return nil, err
	}
	if !mt.hasEntry || k <= 0 {
		return nil, nil
	}
	ef := efSearch
	if ef <= 0 {
		ef = ix.efS
	}
	if k > ef {
		ef = k
	}

	s := ix.getSearcher(rtx, query)
	defer ix.putSearcher(s)
	ep := mt.entryLabel
	for lc := mt.topLayer; lc > 0; lc-- {
		if ep, err = s.greedyClosest(ep, lc); err != nil {
			return nil, err
		}
	}
	found, err := s.searchLayer(ep, ef, 0)
	if err != nil {
		return nil, err
	}
	if k > len(found) {
		k = len(found)
	}
	out := make([]Hit, 0, k)
	var kbuf []byte
	for i := 0; i < k; i++ {
		docID, derr := rtx.Get(ix.vlbl, labelKey(kbuf, found[i].label))
		if derr != nil {
			return nil, derr
		}
		out = append(out, Hit{DocID: append([]byte(nil), docID...), Distance: found[i].dist})
	}
	return out, nil
}
