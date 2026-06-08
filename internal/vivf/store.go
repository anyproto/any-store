package vivf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/viterin/vek/vek32"
)

// Candidate is one search result: the document id and its distance to the query
// (cosine distance for a normalized index, L2 otherwise). Smaller is closer.
type Candidate struct {
	DocID    []byte
	Distance float32
}

// StoreParams configures a btree-resident IVF-PQ index. Dim must be divisible by M.
type StoreParams struct {
	Dim       int
	NList     int
	M         int
	Assign    int  // closure factor: each vector placed in its Assign nearest cells
	NProbe    int  // default cells scanned per search
	Normalize bool // cosine: store/query unit-normalized vectors
	KMeansPP  bool
	Seed      int64
}

func (p *StoreParams) withDefaults() {
	if p.Assign < 1 {
		p.Assign = 1
	}
	if p.NProbe < 1 {
		p.NProbe = 16
	}
	if p.KMeansPP {
		// no-op; explicit field
	}
}

// StoreIndex is a btree-resident IVF-PQ index. It owns no vector state beyond the
// codebooks (read once into RAM at open); the inverted lists, codes, vectors, and
// label maps all live in btree namespaces and are read at the tx snapshot — so it
// is MVCC-consistent and multiprocess-safe, like internal/vindex.
type StoreIndex struct {
	vmeta, vcb, vcell, vvec, vlbl, vdoc *btree.Namespace

	dim, nlist, m, dsub, nprobe, assign int
	normalize                           bool

	coarse [][]float32   // [nlist][dim]   — RAM-resident (the only hot set)
	pqcb   [][][]float32 // [m][pqK][dsub]

	metaRoot  uint32
	searchers sync.Pool // *searcher — reusable per-query scratch (alloc-free search)
}

// cand is one scanned candidate: a label and its approximate (ADC) distance.
type cand struct {
	label uint32
	dist  float32
}

// cellDist pairs a coarse cell index with the query's distance to its centroid.
type cellDist struct {
	idx  int
	dist float32
}

// searcher holds all per-query scratch so a search allocates nothing beyond its
// result. Pooled on the index and reused across queries.
type searcher struct {
	normBuf []float32
	qr      []float32
	lut     []float32
	cd      []cellDist
	cells   []int
	cands   []cand
	dedup   u32fmap // label -> best ADC, map-free with O(1) reset
	docOff  [][2]int
	vbuf    []byte // reused :vec read buffer (re-rank)
	dbuf    []byte // reused :lbl (docID) read buffer (re-rank)
}

func (ix *StoreIndex) getSearcher() *searcher {
	if v := ix.searchers.Get(); v != nil {
		return v.(*searcher)
	}
	return &searcher{}
}

func (ix *StoreIndex) putSearcher(s *searcher) { ix.searchers.Put(s) }

func storeNsNames(prefix string) [6]string {
	return [6]string{prefix + nsMeta, prefix + nsCB, prefix + nsCell, prefix + nsVec, prefix + nsLbl, prefix + nsDoc}
}

// DropNamespaces deletes every btree namespace backing an IVF-PQ index.
func DropNamespaces(wtx *btree.WriteTx, prefix string) error {
	for _, name := range storeNsNames(prefix) {
		if err := wtx.DeleteNamespace(name); err != nil && !errors.Is(err, btree.ErrNamespaceNotFound) {
			return err
		}
	}
	return nil
}

// BulkBuild trains the IVF-PQ model on (ids, vecs) and writes the whole index in
// one pass: codebooks to :cb, per-cell residual codes to :cell, vectors to :vec,
// and the label↔docID maps. ids[i] is the document id for vecs[i].
func BulkBuild(wtx *btree.WriteTx, prefix string, p StoreParams, ids [][]byte, vecs [][]float32) (*StoreIndex, error) {
	if p.Dim <= 0 || p.M <= 0 || p.Dim%p.M != 0 {
		return nil, fmt.Errorf("vivf: dim %d must be a positive multiple of M %d", p.Dim, p.M)
	}
	p.withDefaults()
	ix := &StoreIndex{dim: p.Dim, nlist: p.NList, m: p.M, dsub: p.Dim / p.M, nprobe: p.NProbe, assign: p.Assign, normalize: p.Normalize}

	names := storeNsNames(prefix)
	ns := make([]*btree.Namespace, len(names))
	for i, name := range names {
		n, err := ensureNS(wtx, name)
		if err != nil {
			return nil, err
		}
		ns[i] = n
	}
	ix.vmeta, ix.vcb, ix.vcell, ix.vvec, ix.vlbl, ix.vdoc = ns[0], ns[1], ns[2], ns[3], ns[4], ns[5]

	// Normalize a working copy (cosine -> unit vectors).
	norm := make([][]float32, len(vecs))
	for i, v := range vecs {
		if p.Normalize {
			norm[i] = normalize(v)
		} else {
			norm[i] = v
		}
	}

	// Train codebooks (shared with the in-RAM prototype) and persist them.
	pp := Params{Dim: p.Dim, NList: p.NList, M: p.M, Seed: p.Seed, KMeansPP: p.KMeansPP, Assign: p.Assign}
	ix.coarse, ix.pqcb = trainModel(norm, pp)
	if err := wtx.Put(ix.vcb, coarseKey, encodeCentroids(ix.coarse)); err != nil {
		return nil, err
	}
	if err := wtx.Put(ix.vcb, pqKey, encodePQ(ix.pqcb)); err != nil {
		return nil, err
	}

	// Encode + place each vector. label == build order (dense 0..n-1). Accumulate the
	// primary-cell reconstruction error to seed the drift baseline.
	var keyBuf, codeBuf []byte
	var reconSum float64
	r := make([]float32, ix.dim)
	for i, x := range norm {
		label := uint32(i)
		if err := ix.writeVecRecords(wtx, label, ids[i], x); err != nil {
			return nil, err
		}
		cells := topNCells(x, ix.coarse, ix.assign)
		if err := ix.putDocCells(wtx, ids[i], label, cells); err != nil {
			return nil, err
		}
		for ci, c := range cells {
			vek32.Sub_Into(r, x, ix.coarse[c])
			if ci == 0 {
				reconSum += float64(sqNorm(r)) // nearest-cell residual = reconstruction error
			}
			keyBuf = cellKey(keyBuf, uint32(c), label)
			codeBuf = encodeResidualInto(codeBuf, r, ix)
			if err := wtx.Put(ix.vcell, keyBuf, codeBuf); err != nil {
				return nil, err
			}
		}
	}

	n := len(vecs)
	reconBase := 0.0
	if n > 0 {
		reconBase = reconSum / float64(n)
	}
	mt := &meta{dim: p.Dim, nlist: p.NList, m: p.M, assign: p.Assign, nprobe: p.NProbe, normalize: p.Normalize, count: int64(n), nextLabel: uint32(n), reconBase: reconBase, buildCount: int64(n)}
	if err := wtx.Put(ix.vmeta, metaKey, encodeMeta(mt)); err != nil {
		return nil, err
	}
	ix.metaRoot = ix.vmeta.RootPage()
	return ix, nil
}

// encodeResidualInto produces the stored :cell value (PQ code) for a residual,
// reusing dst (wtx.Put copies the bytes, so one buffer can be shared across cells).
func encodeResidualInto(dst []byte, r []float32, ix *StoreIndex) []byte {
	dst = dst[:0]
	for mm := 0; mm < ix.m; mm++ {
		lo := mm * ix.dsub
		dst = append(dst, byte(nearestSmall(r[lo:lo+ix.dsub], ix.pqcb[mm])))
	}
	return dst
}

// writeVecRecords stores the (normalized) full vector for re-rank and the
// label↔docID maps.
func (ix *StoreIndex) writeVecRecords(wtx *btree.WriteTx, label uint32, docID []byte, x []float32) error {
	var lk [4]byte
	binary.BigEndian.PutUint32(lk[:], label)
	if err := wtx.Put(ix.vvec, lk[:], f32bytes(x)); err != nil {
		return err
	}
	if err := wtx.Put(ix.vlbl, lk[:], docID); err != nil {
		return err
	}
	return nil
}

// putDocCells records, under docID, the label and the cells it was placed in — so
// delete/update can remove its :cell entries without scanning.
func (ix *StoreIndex) putDocCells(wtx *btree.WriteTx, docID []byte, label uint32, cells []int) error {
	buf := make([]byte, 0, 4+1+4*len(cells))
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], label)
	buf = append(buf, b[:]...)
	buf = append(buf, byte(len(cells)))
	for _, c := range cells {
		binary.LittleEndian.PutUint32(b[:], uint32(c))
		buf = append(buf, b[:]...)
	}
	return wtx.Put(ix.vdoc, docID, buf)
}

func decodeDocCells(data []byte) (label uint32, cells []uint32) {
	label = binary.LittleEndian.Uint32(data)
	n := int(data[4])
	off := 5
	cells = make([]uint32, n)
	for i := 0; i < n; i++ {
		cells[i] = binary.LittleEndian.Uint32(data[off:])
		off += 4
	}
	return label, cells
}

// OpenTx resolves an existing index, reading params and codebooks into RAM.
func OpenTx(rtx *btree.ReadTx, prefix string) (*StoreIndex, error) {
	names := storeNsNames(prefix)
	ns := make([]*btree.Namespace, len(names))
	for i, name := range names {
		n, err := rtx.GetNamespace(name)
		if err != nil {
			return nil, err
		}
		ns[i] = n
	}
	ix := &StoreIndex{}
	ix.vmeta, ix.vcb, ix.vcell, ix.vvec, ix.vlbl, ix.vdoc = ns[0], ns[1], ns[2], ns[3], ns[4], ns[5]

	b, err := rtx.Get(ix.vmeta, metaKey)
	if err != nil {
		return nil, err
	}
	mt, err := decodeMeta(b)
	if err != nil {
		return nil, err
	}
	ix.dim, ix.nlist, ix.m, ix.dsub = mt.dim, mt.nlist, mt.m, mt.dim/mt.m
	ix.nprobe, ix.assign, ix.normalize = mt.nprobe, mt.assign, mt.normalize

	cb, err := rtx.Get(ix.vcb, coarseKey)
	if err != nil {
		return nil, err
	}
	ix.coarse = decodeCentroids(cloneBytes(cb), ix.nlist, ix.dim)
	pq, err := rtx.Get(ix.vcb, pqKey)
	if err != nil {
		return nil, err
	}
	ix.pqcb = decodePQ(cloneBytes(pq), ix.m, ix.dsub)
	ix.metaRoot = ix.vmeta.RootPage()
	return ix, nil
}

// MetaRoot is the :meta namespace root page (staleness check after compaction).
func (ix *StoreIndex) MetaRoot() uint32 { return ix.metaRoot }

// StoreStats reports per-namespace sizes and counts for the index.
type StoreStats struct {
	Dim, NList, M, NProbe, Assign int
	Count                         int64
	Cell, Vec, CB, Lbl, Doc, Meta btree.NamespaceSize
}

// Stats reads the namespace sizes and meta counters at rtx's snapshot.
func (ix *StoreIndex) Stats(rtx *btree.ReadTx) (StoreStats, error) {
	var s StoreStats
	mt, err := ix.readMeta(rtx)
	if err != nil {
		return s, err
	}
	s.Dim, s.NList, s.M, s.NProbe, s.Assign = mt.dim, mt.nlist, mt.m, mt.nprobe, mt.assign
	s.Count = mt.count
	for _, p := range []struct {
		ns  *btree.Namespace
		dst *btree.NamespaceSize
	}{
		{ix.vcell, &s.Cell}, {ix.vvec, &s.Vec}, {ix.vcb, &s.CB},
		{ix.vlbl, &s.Lbl}, {ix.vdoc, &s.Doc}, {ix.vmeta, &s.Meta},
	} {
		sz, err := rtx.NamespaceSize(p.ns)
		if err != nil {
			return s, err
		}
		*p.dst = sz
	}
	return s, nil
}

// NProbe returns the index's default cells-per-search.
func (ix *StoreIndex) NProbe() int { return ix.nprobe }

// DriftScore returns how far the index has drifted from its build-time codebooks,
// as max(reconRatio−1, churnRatio):
//   - reconRatio = mean residual norm of inserts-since-build ÷ the build baseline
//     (how much worse new data fits the frozen centroids), and
//   - churnRatio = writes-since-build ÷ built size (a delete-heavy backstop).
//
// Both are ~0 on a fresh build and grow with drift. A caller rebuilds when the
// score crosses a threshold (the IVF analog of HNSW's tombstone CompactRatio). It
// is O(1): just a meta read.
func (ix *StoreIndex) DriftScore(rtx *btree.ReadTx) (float64, error) {
	mt, err := ix.readMeta(rtx)
	if err != nil {
		return 0, err
	}
	if mt.buildCount <= 0 {
		return 0, nil
	}
	// The reconstruction-error signal only counts once a meaningful fraction (~10%)
	// of the built size has been inserted, so a handful of outliers can't trigger a
	// rebuild — it needs a sustained distribution shift. Below that, only the churn
	// backstop applies.
	var recon float64
	if mt.reconBase > 0 && mt.driftN*10 >= mt.buildCount && mt.driftN > 0 {
		recon = (mt.driftSum / float64(mt.driftN)) / mt.reconBase
	}
	churn := float64(mt.churn) / float64(mt.buildCount)
	score := churn
	if recon-1 > score {
		score = recon - 1
	}
	return score, nil
}

// Rebuild re-trains the codebooks from the live vectors and rewrites the whole
// index, clearing accumulated drift (the IVF analog of compaction). It recreates
// the namespaces (root pages move — the caller MarkSchemaChanged so peers
// reconcile). nlist is re-derived from the current live count, so the partition
// also rescales. Returns a fresh StoreIndex bound to the rebuilt namespaces.
func Rebuild(wtx *btree.WriteTx, prefix string) (*StoreIndex, error) {
	rtx := &wtx.ReadTx
	old, err := OpenTx(rtx, prefix)
	if err != nil {
		return nil, err
	}

	// Collect the live (docID, vector) set into RAM before dropping the namespaces.
	var ids [][]byte
	var vecs [][]float32
	cur := rtx.NewCursor(old.vvec)
	defer cur.Close()
	if err := cur.First(); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
		return nil, err
	}
	for cur.Valid() {
		key, err := cur.Key()
		if err != nil {
			return nil, err
		}
		vb, err := cur.Value()
		if err != nil {
			return nil, err
		}
		docID, err := rtx.Get(old.vlbl, key)
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				if err := cur.Next(); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		ids = append(ids, cloneBytes(docID))
		vecs = append(vecs, append([]float32(nil), bytesAsF32(vb, old.dim)...))
		if err := cur.Next(); err != nil {
			return nil, err
		}
	}
	cur.Close()

	if err := DropNamespaces(wtx, prefix); err != nil {
		return nil, err
	}
	p := StoreParams{
		Dim: old.dim, NList: ivfRebuildNList(old.nlist, len(vecs)), M: old.m,
		Assign: old.assign, NProbe: old.nprobe, Normalize: old.normalize, KMeansPP: true, Seed: 1,
	}
	return BulkBuild(wtx, prefix, p, ids, vecs)
}

// ivfRebuildNList keeps the configured nlist but clamps it to the live count (a
// shrunken index can't have more cells than points).
func ivfRebuildNList(configured, n int) int {
	if n > 0 && configured > n {
		return n
	}
	if configured < 1 {
		return 1
	}
	return configured
}

// SearchCandidates returns up to ef nearest candidates, closest-first. It scans
// nprobe cells (each a contiguous :cell range), scores members code-only via the
// per-cell ADC LUT, truncates to the ef best by approximate distance, then
// re-ranks ONLY those by exact distance on the stored vectors. The expensive
// random :vec reads are thus bounded to ef per query, while the cell scans are
// sequential — the btree-fit read pattern (RESEARCH_IVFPQ_BTREE.md §3).
func (ix *StoreIndex) SearchCandidates(rtx *btree.ReadTx, q []float32, ef int) ([]Candidate, error) {
	if ef < 1 {
		ef = 1
	}
	s := ix.getSearcher()
	defer ix.putSearcher(s)

	qn := q
	if ix.normalize {
		s.normBuf = normalizeInto(s.normBuf, q)
		qn = s.normBuf
	}
	s.cells = topNCellsInto(qn, ix.coarse, ix.nprobe, &s.cd, s.cells)

	// Scan probed cells; dedup labels keeping the best ADC (closure can place one
	// label in several probed cells), map-free via the pooled u32fmap.
	s.lut = ensureF32(s.lut, ix.m*pqK)
	s.qr = ensureF32(s.qr, ix.dim)
	s.dedup.reset()
	cur := rtx.NewCursor(ix.vcell)
	defer cur.Close()
	var seek [8]byte
	for _, c := range s.cells {
		vek32.Sub_Into(s.qr, qn, ix.coarse[c])
		ix.buildLUT(s.qr, s.lut)
		binary.BigEndian.PutUint32(seek[0:], uint32(c))
		binary.BigEndian.PutUint32(seek[4:], 0)
		if err := cur.Seek(seek[:]); err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}
		for cur.Valid() {
			key, err := cur.Key()
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(key[:4], seek[:4]) {
				break // left this cell's contiguous range
			}
			code, err := cur.Value()
			if err != nil {
				return nil, err
			}
			s.dedup.putMin(cellKeyLabel(key), adc(s.lut, code, ix.m))
			if err := cur.Next(); err != nil {
				return nil, err
			}
		}
	}

	// Keep the ef best by ADC before the exact re-rank (bounds :vec reads).
	s.cands = s.dedup.collect(s.cands)
	slices.SortFunc(s.cands, func(a, b cand) int { return cmpF32(a.dist, b.dist) })
	if len(s.cands) > ef {
		s.cands = s.cands[:ef]
	}

	// Re-rank survivors by exact distance, packing docIDs into one backing slice so
	// the result costs ~2 allocations regardless of ef.
	out := make([]Candidate, 0, len(s.cands))
	s.docOff = s.docOff[:0]
	backing := make([]byte, 0, len(s.cands)*12)
	var lk [4]byte
	for _, c := range s.cands {
		binary.BigEndian.PutUint32(lk[:], c.label)
		vb, err := rtx.AppendValue(ix.vvec, lk[:], s.vbuf[:0])
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				continue // tombstoned between scan and re-rank
			}
			return nil, err
		}
		s.vbuf = vb
		dist := ix.exactDist(qn, bytesAsF32(vb, ix.dim))
		docID, err := rtx.AppendValue(ix.vlbl, lk[:], s.dbuf[:0])
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}
		s.dbuf = docID
		start := len(backing)
		backing = append(backing, docID...)
		s.docOff = append(s.docOff, [2]int{start, len(backing)})
		out = append(out, Candidate{Distance: dist})
	}
	// backing is final now — slice docIDs out of it, then sort closest-first. Sorting
	// the ~ef re-ranked survivors here (and marking the spec Ordered) is measurably
	// cheaper than returning them unordered and letting the pipeline's SortIter
	// collect+heap+fetch: with Ordered set the planner skips the SortIter for the
	// default distance order and streams straight to LimitIter (~19% faster end to
	// end). An explicit multi-key Sort still goes through SortIter regardless.
	for i := range out {
		out[i].DocID = backing[s.docOff[i][0]:s.docOff[i][1]]
	}
	slices.SortFunc(out, func(a, b Candidate) int { return cmpF32(a.Distance, b.Distance) })
	return out, nil
}

// ensureF32 returns a length-n float32 slice reusing buf's capacity when possible.
func ensureF32(buf []float32, n int) []float32 {
	if cap(buf) < n {
		return make([]float32, n)
	}
	return buf[:n]
}

// exactDist is the final re-rank distance: cosine (1−dot on unit vectors) for a
// normalized index, L2 otherwise — matching the metric the DB exposes as _distance.
func (ix *StoreIndex) exactDist(qn, x []float32) float32 {
	if ix.normalize {
		return 1 - vek32.Dot(qn, x)
	}
	return vek32.Distance(qn, x)
}

func (ix *StoreIndex) buildLUT(qr, lut []float32) {
	for mm := 0; mm < ix.m; mm++ {
		lo := mm * ix.dsub
		sub := qr[lo : lo+ix.dsub]
		base := mm * pqK
		cb := ix.pqcb[mm]
		for j := 0; j < pqK; j++ {
			lut[base+j] = sqL2(sub, cb[j])
		}
	}
}

// Insert adds (or replaces) docID's vector: assign to its Assign nearest cells and
// write the per-cell codes. A replace first removes the old entries.
func (ix *StoreIndex) Insert(wtx *btree.WriteTx, docID []byte, vec []float32) error {
	if len(vec) != ix.dim {
		return fmt.Errorf("vivf: dim mismatch: got %d want %d", len(vec), ix.dim)
	}
	rtx := &wtx.ReadTx
	mt, err := ix.readMeta(rtx)
	if err != nil {
		return err
	}
	if _, derr := ix.removeDoc(wtx, docID, mt); derr != nil {
		return derr
	}
	x := vec
	if ix.normalize {
		x = normalize(vec)
	}
	label := mt.nextLabel
	mt.nextLabel++
	mt.count++
	if err := ix.writeVecRecords(wtx, label, docID, x); err != nil {
		return err
	}
	cells := topNCells(x, ix.coarse, ix.assign)
	if err := ix.putDocCells(wtx, docID, label, cells); err != nil {
		return err
	}
	var keyBuf, codeBuf []byte
	r := make([]float32, ix.dim)
	for ci, c := range cells {
		vek32.Sub_Into(r, x, ix.coarse[c])
		if ci == 0 {
			// Track how well the frozen centroids fit this new vector (drift signal).
			mt.driftSum += float64(sqNorm(r))
			mt.driftN++
		}
		keyBuf = cellKey(keyBuf, uint32(c), label)
		codeBuf = encodeResidualInto(codeBuf, r, ix)
		if err := wtx.Put(ix.vcell, keyBuf, codeBuf); err != nil {
			return err
		}
	}
	mt.churn++
	return wtx.Put(ix.vmeta, metaKey, encodeMeta(mt))
}

// Delete removes docID from the index, returning whether it was present.
func (ix *StoreIndex) Delete(wtx *btree.WriteTx, docID []byte) (bool, error) {
	rtx := &wtx.ReadTx
	mt, err := ix.readMeta(rtx)
	if err != nil {
		return false, err
	}
	removed, err := ix.removeDoc(wtx, docID, mt)
	if err != nil || !removed {
		return removed, err
	}
	mt.churn++ // a delete also shifts the live distribution away from the centroids
	return true, wtx.Put(ix.vmeta, metaKey, encodeMeta(mt))
}

// removeDoc deletes docID's :cell/:vec/:lbl/:doc records (if present) and updates
// mt.count. It does NOT persist mt (the caller does).
func (ix *StoreIndex) removeDoc(wtx *btree.WriteTx, docID []byte, mt *meta) (bool, error) {
	rtx := &wtx.ReadTx
	dc, err := rtx.Get(ix.vdoc, docID)
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	label, cells := decodeDocCells(dc)
	var keyBuf []byte
	for _, c := range cells {
		keyBuf = cellKey(keyBuf, c, label)
		if derr := wtx.Delete(ix.vcell, keyBuf); derr != nil && !errors.Is(derr, btree.ErrKeyNotFound) {
			return false, derr
		}
	}
	var lk [4]byte
	binary.BigEndian.PutUint32(lk[:], label)
	if derr := wtx.Delete(ix.vvec, lk[:]); derr != nil && !errors.Is(derr, btree.ErrKeyNotFound) {
		return false, derr
	}
	if derr := wtx.Delete(ix.vlbl, lk[:]); derr != nil && !errors.Is(derr, btree.ErrKeyNotFound) {
		return false, derr
	}
	if derr := wtx.Delete(ix.vdoc, docID); derr != nil && !errors.Is(derr, btree.ErrKeyNotFound) {
		return false, derr
	}
	mt.count--
	return true, nil
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

func (ix *StoreIndex) readMeta(rtx *btree.ReadTx) (*meta, error) {
	b, err := rtx.Get(ix.vmeta, metaKey)
	if err != nil {
		return nil, err
	}
	return decodeMeta(b)
}

func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
