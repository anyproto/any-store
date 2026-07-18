package vindex

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/simd"
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
	Quantization   Quantization
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

	dim   int
	m     int
	m0    int
	efC   int
	efS   int
	ml    float64
	quant Quantization

	dist DistanceFunc

	// distInt8 is the byte-kernel distance from a float32 query to a stored
	// offset-binary int8 record, for the dot-based metrics (Cosine, Dot). It uses
	// the SIMD float×byte kernel and skips dequantization. nil when not eligible
	// (non-int8 storage, L2 metric, or no SIMD byte kernel); callers then fall
	// back to decode+dist. scale/comps come from the record; qsum is the
	// precomputed Σ of the query components (the offset-binary correction term).
	distInt8 func(q []float32, scale float32, comps []byte, qsum float32) float32

	// normalize is set for the Cosine metric: vectors are unit-normalized on write
	// and queries on read, so dist is the dot-product cosine kernel
	// (cosineDotDistance) — same distance values, cheaper than recomputing norms.
	normalize bool

	// hybrid enables the RAM layer-0 mirror on the read path (set by the caller
	// for VectorModeHybrid). l0pub holds the most recently built mirror and l0base
	// the immutable CSR base both are derived from, as lock-free CAS-to-newest
	// caches; a search only uses a mirror whose gen matches its snapshot. dirty is
	// the in-memory changelog the writer records to so reads can rebuild the mirror
	// incrementally; fullStreak counts consecutive full rebuilds for the thrash
	// valve. See mirror.go.
	hybrid     bool
	l0pub      atomic.Pointer[l0Mirror]
	l0base     atomic.Pointer[l0Base]
	dirty      *dirtyRing
	fullStreak atomic.Uint64

	// vecCache enables the RAM vector tier (hybrid only): layer-0 vector reads are
	// served from RAM instead of the btree. vtier is created lazily.
	vecCache bool
	vtier    *vecTier

	// vcacheCap bounds the per-batch insert vector cache (lever 1), in vectors;
	// 0 disables it. RAM ceiling ≈ vcacheCap*dim*4 bytes per active write searcher.
	// See vecCache (vcache.go) and SetInsertCacheSize.
	vcacheCap int

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
	s.l0 = nil
	s.tierData = nil
	s.vcacheOn = false   // read path must not reuse the insert cache
	s.byteDistOn = false // and the next op re-decides byte-kernel eligibility
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
	ix := newIndex(ns, p.Dim, p.M, 2*p.M, p.EfConstruction, p.EfSearch, p.Ml, p.Metric, p.Quantization, seed)
	mt := &meta{
		dim: p.Dim, metric: p.Metric, m: p.M, m0: 2 * p.M,
		efC: p.EfConstruction, efS: p.EfSearch, ml: p.Ml, quant: p.Quantization,
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
	return newIndex(ns, mt.dim, mt.m, mt.m0, mt.efC, mt.efS, mt.ml, mt.metric, mt.quant, seed), nil
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
	return newIndex(ns, mt.dim, mt.m, mt.m0, mt.efC, mt.efS, mt.ml, mt.metric, mt.quant, seed), nil
}

func newIndex(ns [5]*btree.Namespace, dim, m, m0, efC, efS int, ml float64, metric Metric, quant Quantization, seed int64) *Index {
	ix := &Index{
		vmeta: ns[0], vvec: ns[1], vadj: ns[2], vdoc: ns[3], vlbl: ns[4],
		dim: dim, m: m, m0: m0, efC: efC, efS: efS, ml: ml, quant: quant,
		dist:      distanceFor(metric),
		rng:       rand.New(rand.NewSource(seed)),
		vcacheCap: defaultInsertCacheVecs,
	}
	if metric == Cosine {
		// Store unit-normalized vectors and normalize queries, so every distance is
		// a dot product instead of a cosine (which recomputes both norms per call).
		ix.normalize = true
		ix.dist = cosineDotDistance
	}
	// For int8 storage on a dot-based metric, the byte kernel can compute the
	// query→stored distance straight from the stored bytes (no dequant). The
	// offset-binary identity is dot(q, scale·(comps−bias)) = scale·(Σq·comps −
	// bias·Σq), so both metrics share one signed-dot term and differ only in the
	// final transform. L2 does not factor this way, so it keeps decode+dist.
	if quant == QuantInt8 && simd.AcceleratedFloatByte() {
		switch metric {
		case Cosine:
			ix.distInt8 = func(q []float32, scale float32, comps []byte, qsum float32) float32 {
				return 1 - scale*(simd.DotFloatByte(q, comps)-int8Bias*qsum)
			}
		case Dot:
			ix.distInt8 = func(q []float32, scale float32, comps []byte, qsum float32) float32 {
				return -scale * (simd.DotFloatByte(q, comps) - int8Bias*qsum)
			}
		}
	}
	return ix
}

// defaultInsertCacheVecs is the default per-batch insert vector cache size, in
// vectors. ~16k vectors covers the hot hub set of typical graphs (sweeps show
// build throughput still climbing at this size) while bounding RAM (e.g. ~8 MiB
// at dim 128, ~50 MiB at dim 768). Override per-index with SetInsertCacheSize,
// or globally via the ASV_VCACHE env var (vectors; 0 disables) — handy for
// sweeping the optimal size across a test/benchmark suite.
var defaultInsertCacheVecs = insertCacheDefault()

func insertCacheDefault() int {
	if s := os.Getenv("ASV_VCACHE"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return 16384
}

// Dim returns the index dimension.
func (ix *Index) Dim() int { return ix.dim }

// SetHybrid enables (or disables) the RAM layer-0 mirror on the read path. Call
// once right after Create/Open, before concurrent use. The btree stays the
// source of truth; the mirror is a derived, snapshot-aligned read cache.
func (ix *Index) SetHybrid(h bool) {
	ix.hybrid = h
	if h && ix.dirty == nil {
		ix.dirty = &dirtyRing{}
	}
}

// SetVectorCache enables (or disables) the RAM vector tier (hybrid only): layer-0
// vector reads come from a RAM slab instead of the btree, making a hop pure-RAM.
// It caches the btree's exact :vec bytes (int8 if the index is int8), so results
// are unchanged. RAM cost ≈ nextLabel × vecRecordSize, where nextLabel is the
// label high-water mark — every delete/replace allocates a fresh label, so this
// grows with write churn (NOT just the live set) until a Compact resets it. Use
// int8 to keep it down. Call once right after Create/Open, before concurrent use.
func (ix *Index) SetVectorCache(v bool) {
	ix.vecCache = v
	if v && ix.vtier == nil {
		ix.vtier = newVecTier(vecRecordSize(ix.dim, ix.quant))
	}
}

// SetInsertCacheSize configures the per-batch insert vector cache (lever 1):
// during a write batch, repeatedly-read node vectors (entry point and hubs) are
// served from a capped RAM arena instead of the btree. n is the max cached
// vectors (RAM ≈ n*dim*4 bytes per active write searcher); n <= 0 disables it.
// Call once right after Create/Open, before concurrent use.
func (ix *Index) SetInsertCacheSize(n int) { ix.vcacheCap = n }

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
	mt.l0Gen++ // this insert changes layer 0 (new node + back-links)

	// One searcher for the whole insert: it also owns the normalize buffer, so
	// the normalized vector is stored AND used as the traversal query (one norm).
	s := ix.getSearcher(rtx, vec)
	defer ix.putSearcher(s)
	if ix.normalize {
		s.normBuf = normalizeInto(s.normBuf, vec)
		s.query = s.normBuf // traversal compares against normalized stored vectors
		vec = s.normBuf     // and the stored :vec record is normalized too
	}
	s.beginVecCache(mt.nextLabel) // serve repeated per-insert vector reads from RAM (lever 1)

	// Track the layer-0 labels this insert changes, for the hybrid dirty ring.
	track := ix.hybrid && ix.dirty != nil
	if track {
		s.dirtyL0 = s.dirtyL0[:0]
	}

	// replace existing
	if old, gerr := rtx.Get(ix.vdoc, docID); gerr == nil {
		oldLabel := binary.LittleEndian.Uint32(old)
		if terr := ix.tombstoneLabel(wtx, oldLabel, mt); terr != nil {
			return terr
		}
		if track {
			s.dirtyL0 = append(s.dirtyL0, oldLabel) // its deleted flag flipped
		}
	} else if !errors.Is(gerr, btree.ErrKeyNotFound) {
		return gerr
	}

	label := mt.nextLabel
	mt.nextLabel++
	mt.count++
	level := ix.randomLevel()
	if track {
		s.dirtyL0 = append(s.dirtyL0, label) // the new node's own layer-0 adjacency
	}

	var keyArr [4]byte
	key := func(l uint32) []byte {
		binary.BigEndian.PutUint32(keyArr[:], l)
		return keyArr[:]
	}

	var vbytes []byte
	if ix.quant == QuantNone {
		vbytes = f32bytes(vec) // zero-copy
	} else {
		vbytes = encodeVec(nil, vec, ix.quant)
	}
	if err = wtx.Put(ix.vvec, key(label), vbytes); err != nil {
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
		if track {
			ix.dirty.record(mt.l0Gen, s.dirtyL0)
		}
		return ix.writeMeta(wtx, mt)
	}

	s.checkDeleted = mt.deletedCount > 0
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
		found, err = s.selectNeighborsHeuristic(found, m)
		if err != nil {
			return err
		}
		// Copy labels out before any addNeighbor call: addNeighbor runs the
		// heuristic too and would overwrite s.sel (which `found` aliases).
		sel := make([]uint32, len(found))
		for i, c := range found {
			sel[i] = c.label
		}
		newNbrs[lc] = sel
		if track && lc == 0 {
			s.dirtyL0 = append(s.dirtyL0, sel...) // these neighbours gained a back-link
		}
		for _, lbl := range sel {
			if err = ix.addNeighbor(wtx, s, lbl, label, lc); err != nil {
				return err
			}
		}
		if len(sel) > 0 {
			ep = sel[0]
		}
	}
	if err = wtx.Put(ix.vadj, key(label), encodeAdj(nil, level, false, newNbrs)); err != nil {
		return err
	}
	if level > mt.topLayer {
		mt.entryLabel, mt.topLayer = label, level
	}
	if track {
		ix.dirty.record(mt.l0Gen, s.dirtyL0)
	}
	return ix.writeMeta(wtx, mt)
}

// addNeighbor links b into a's neighbour list at layer, pruning to the farthest
// when the list is full (coder/hnsw's rule). Reads/writes a's adjacency record.
func (ix *Index) addNeighbor(wtx *btree.WriteTx, s *searcher, a, b uint32, layer int32) error {
	rtx := &wtx.ReadTx
	binary.BigEndian.PutUint32(s.naKey[:], a)
	kb := s.naKey[:]
	var err error
	s.naAdj, err = rtx.AppendValue(ix.vadj, kb, s.naAdj[:0])
	if err != nil {
		return err
	}
	level, deleted, nbrs, err := decodeAllAdjInto(s.naAdj, s.naDec)
	if err != nil {
		return err
	}
	s.naDec = nbrs
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
		// Full: re-select capn diverse neighbours from {existing ∪ b} via the
		// heuristic, centred on a — preserves bridges instead of just keeping the
		// closest (which fragments the graph). Centre vector lives in aVecBuf so
		// the heuristic's vec2Of/vecOf scratch can't clobber it.
		av, err := s.vecOf(a)
		if err != nil {
			return err
		}
		s.aVecBuf = append(s.aVecBuf[:0], av...)
		s.naCand = s.naCand[:0]
		for _, x := range list {
			xv, xerr := s.vecOf(x)
			if xerr != nil {
				return xerr
			}
			s.naCand = append(s.naCand, candidate{ix.dist(s.aVecBuf, xv), x})
		}
		bv, berr := s.vecOf(b)
		if berr != nil {
			return berr
		}
		s.naCand = append(s.naCand, candidate{ix.dist(s.aVecBuf, bv), b})
		// Alloc-free sort (sort.Slice uses reflection and allocates); the
		// candidate set is tiny (<= maxConn+1), so insertion sort is ideal.
		insertionSortCands(s.naCand)
		kept, kerr := s.selectNeighborsHeuristic(s.naCand, capn)
		if kerr != nil {
			return kerr
		}
		out := list[:0]
		for _, c := range kept {
			out = append(out, c.label)
		}
		nbrs[layer] = out
	}
	s.naEnc = encodeAdj(s.naEnc[:0], level, deleted, nbrs)
	return wtx.Put(ix.vadj, kb[:], s.naEnc)
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
	if err = wtx.Put(ix.vadj, labelKey(kbuf, label), encodeAdj(nil, level, true, nbrs)); err != nil {
		return err
	}
	// Searches start at the entry point and only route THROUGH tombstones;
	// tombstoning the entry without repointing leaves a dead start — in the
	// degenerate case (entry with no live neighbour, e.g. a one-document
	// collection being updated) every subsequent search returned empty
	// forever (guarded by TestVectorEntryRepoint_SingleDocUpsert).
	if mt.hasEntry && label == mt.entryLabel {
		return ix.repointEntry(wtx, mt, nbrs)
	}
	return nil
}

// repointEntry moves the search entry off a just-tombstoned node onto a live
// one. Preference order: the dead entry's own neighbours, highest layer first
// (they are the natural hubs and cost O(M) adjacency reads); otherwise any
// live document via the vdoc namespace, which excludes tombstones by
// construction. topLayer follows the new entry's level — search descends from
// there, so a lower level is merely less accelerated, never wrong; a later
// higher-level insert raises it again. When no live node remains, hasEntry is
// cleared: searches correctly return nothing and the next Insert re-seeds the
// entry (the single-document update case).
func (ix *Index) repointEntry(wtx *btree.WriteTx, mt *meta, deadNbrs [][]uint32) error {
	mt.hasEntry, mt.entryLabel, mt.topLayer = false, 0, 0
	if mt.count <= 0 {
		return nil
	}
	rtx := &wtx.ReadTx
	var kbuf []byte
	setEntry := func(label uint32) (bool, error) {
		adjBytes, err := rtx.Get(ix.vadj, labelKey(kbuf, label))
		if errors.Is(err, btree.ErrKeyNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		level, deleted, _, derr := decodeAllAdj(adjBytes)
		if derr != nil {
			return false, derr
		}
		if deleted {
			return false, nil
		}
		mt.hasEntry, mt.entryLabel, mt.topLayer = true, label, level
		return true, nil
	}
	for l := len(deadNbrs) - 1; l >= 0; l-- {
		for _, nb := range deadNbrs[l] {
			ok, err := setEntry(nb)
			if err != nil || ok {
				return err
			}
		}
	}
	// Disconnected entry (no live neighbour): fall back to any live doc.
	cur := rtx.NewCursor(ix.vdoc)
	defer cur.Close()
	if err := cur.First(); err != nil {
		return err
	}
	for cur.Valid() {
		lb, verr := cur.Value()
		if verr != nil {
			return verr
		}
		ok, err := setEntry(binary.LittleEndian.Uint32(lb))
		if err != nil || ok {
			return err
		}
		if err = cur.Next(); err != nil {
			return err
		}
	}
	return nil
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
	mt.l0Gen++ // tombstoning this node changes its layer-0 deleted flag
	if err = ix.tombstoneLabel(wtx, label, mt); err != nil {
		return false, err
	}
	if err = wtx.Delete(ix.vdoc, docID); err != nil {
		return false, err
	}
	if ix.hybrid && ix.dirty != nil {
		ix.dirty.record(mt.l0Gen, []uint32{label})
	}
	return true, ix.writeMeta(wtx, mt)
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

// Search returns the k nearest live documents to query, ranked closest-first.
// efSearch <= 0 uses the index default. It is a thin ranking wrapper over
// SearchCandidates (which yields the beam in closest-first order); the live
// query pipeline uses SearchCandidates directly and owns its own sort+limit.
func (ix *Index) Search(rtx *btree.ReadTx, query []float32, k, efSearch int) ([]Hit, error) {
	if k <= 0 {
		return nil, nil
	}
	ef := efSearch
	if ef <= 0 {
		ef = ix.efS
	}
	if k > ef {
		ef = k // candidate list must cover k
	}
	cands, err := ix.SearchCandidates(rtx, query, ef)
	if err != nil {
		return nil, err
	}
	if k > len(cands) {
		k = len(cands)
	}
	out := make([]Hit, k)
	for i := 0; i < k; i++ {
		out[i] = Hit{DocID: cands[i].DocID, Distance: cands[i].Distance}
	}
	return out, nil
}
