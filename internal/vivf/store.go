package vivf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/simd"
)

// Candidate is one search result: the document id and its distance to the query
// (cosine distance for a normalized index, L2 otherwise). Smaller is closer.
type Candidate struct {
	DocID    []byte
	Distance float32
}

// StoreParams configures a btree-resident IVF-SQ index.
type StoreParams struct {
	Dim       int
	NList     int
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
}

// StoreIndex is a btree-resident IVF-SQ index: vectors are partitioned into
// nlist coarse cells and each is stored as an int8 scalar-quantized full vector
// under its cell(s), scanned directly with exact int8 distance. It owns no
// vector state beyond the coarse centroids (read once into RAM at open); the
// inverted lists and label maps live in btree namespaces and are read at the tx
// snapshot — so it is MVCC-consistent and multiprocess-safe, like internal/vindex.
type StoreIndex struct {
	vmeta, vcb, vcell, vvec, vlbl, vdoc *btree.Namespace
	// build is the identity of the build this object was opened for or
	// made by (meta.build).
	build uint64

	dim, nlist, nprobe, assign int
	normalize                  bool

	// byteDist enables the SIMD float×byte distance on the read path: cell
	// records are scored straight from their stored bytes (no dequant). Set
	// when a byte kernel is available for this CPU.
	byteDist bool

	// coarse is the RAM-resident centroid table, held as ONE flat, contiguous
	// float32 view of its :cb blob (an arena) rather than nested slices: opening
	// an index aliases the blob with zero extra allocation, and the hot loops
	// read it contiguously. Row c is coarseRow(c).
	coarse []float32 // nlist·dim

	metaRoot  uint32
	searchers sync.Pool // *searcher — reusable per-query scratch (alloc-free search)
}

// cand is one scanned candidate: a label and its exact distance.
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
	cd      []cellDist
	cells   []int
	cands   []cand
	dedup   u32fmap // label -> best distance, map-free with O(1) reset
	docOff  [][2]int
	vbufF32 []float32 // reused dequant target for a cell record (scalar path)
	dbuf    []byte    // reused :lbl (docID) read buffer

	// byteDist mirrors StoreIndex.byteDist for this (read-only) search; qsum=Σq
	// and qnorm2=‖q‖² are the per-query terms of the offset-binary byte distance.
	byteDist bool
	qsum     float32
	qnorm2   float32
}

// coarseRow returns coarse centroid c (a view into the flat coarse arena).
func (ix *StoreIndex) coarseRow(c int) []float32 {
	return ix.coarse[c*ix.dim : (c+1)*ix.dim]
}

// nearestCellsInto fills out with the n coarse cells nearest (L2) to q, reusing
// the scratch slice. The flat-coarse cell finder for both search and build/insert.
func (ix *StoreIndex) nearestCellsInto(q []float32, n int, scratch *[]cellDist, out []int) []int {
	cd := *scratch
	if cap(cd) < ix.nlist {
		cd = make([]cellDist, ix.nlist)
	} else {
		cd = cd[:ix.nlist]
	}
	for c := 0; c < ix.nlist; c++ {
		cd[c] = cellDist{c, simd.Distance(q, ix.coarseRow(c))}
	}
	slices.SortFunc(cd, func(a, b cellDist) int { return cmpF32(a.dist, b.dist) })
	if n > ix.nlist {
		n = ix.nlist
	}
	out = out[:0]
	for i := 0; i < n; i++ {
		out = append(out, cd[i].idx)
	}
	*scratch = cd
	return out
}

// nearestCells is the allocating form of nearestCellsInto for the build/insert
// paths (not hot — one assignment per vector).
func (ix *StoreIndex) nearestCells(q []float32, n int) []int {
	var cd []cellDist
	return ix.nearestCellsInto(q, n, &cd, nil)
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

// DropNamespaces deletes every btree namespace backing an IVF index.
func DropNamespaces(wtx *btree.WriteTx, prefix string) error {
	for _, name := range storeNsNames(prefix) {
		if err := wtx.DeleteNamespace(name); err != nil && !errors.Is(err, btree.ErrNamespaceNotFound) {
			return err
		}
	}
	return nil
}

// BulkBuild trains the coarse quantizer on (ids, vecs) and writes the whole index
// in one pass: centroids to :cb, per-cell int8 vectors to :cell, and the
// label↔docID maps. ids[i] is the document id for vecs[i].
func BulkBuild(wtx *btree.WriteTx, prefix string, p StoreParams, ids [][]byte, vecs [][]float32) (*StoreIndex, error) {
	if p.Dim <= 0 {
		return nil, fmt.Errorf("vivf: dim %d must be positive", p.Dim)
	}
	if len(vecs) == 0 {
		// k-means needs at least one point; an IVF store cannot exist without
		// trained centroids. (Rebuild handles the emptied-index case itself by
		// keeping the previous centroids.)
		return nil, fmt.Errorf("vivf: cannot build from an empty vector set")
	}
	p.withDefaults()
	ix := &StoreIndex{dim: p.Dim, nlist: p.NList, nprobe: p.NProbe, assign: p.Assign, normalize: p.Normalize}
	ix.byteDist = simd.AcceleratedFloatByte()

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

	// Normalize a working copy (cosine -> unit vectors). The unit vectors land in
	// one contiguous arena (one allocation + better locality) instead of a fresh
	// slice per vector; norm[i] is a view into it.
	norm := make([][]float32, len(vecs))
	if p.Normalize {
		arena := make([]float32, len(vecs)*p.Dim)
		for i, v := range vecs {
			norm[i] = normalizeInto(arena[i*p.Dim:(i+1)*p.Dim:(i+1)*p.Dim], v)
		}
	} else {
		copy(norm, vecs)
	}

	// Train the coarse quantizer. The centroids are persisted and then held as
	// the flat view of that blob (same layout the reader gets), so there is one
	// representation, not two.
	coarseN := kmeans(norm, p.NList, 15, p.Seed, p.KMeansPP)
	coarseBlob := encodeCentroids(coarseN)
	if err := wtx.Put(ix.vcb, coarseKey, coarseBlob); err != nil {
		return nil, err
	}
	ix.coarse = bytesAsF32(coarseBlob, ix.nlist*ix.dim)

	// Place each vector: its int8 record under each of its cells. label == build
	// order (dense 0..n-1). Accumulate the primary-cell reconstruction error to
	// seed the drift baseline.
	var keyBuf, recBuf []byte
	var reconSum float64
	r := make([]float32, ix.dim)
	var cellScratch []cellDist // reused across vectors (was a fresh nlist-slice per call)
	var cellsOut []int
	for i, x := range norm {
		label := uint32(i)
		if err := ix.writeLabel(wtx, label, ids[i]); err != nil {
			return nil, err
		}
		cellsOut = ix.nearestCellsInto(x, ix.assign, &cellScratch, cellsOut)
		cells := cellsOut
		if err := ix.putDocCells(wtx, ids[i], label, cells); err != nil {
			return nil, err
		}
		simd.Sub_Into(r, x, ix.coarseRow(cells[0]))
		reconSum += float64(sqNorm(r)) // nearest-cell residual = drift baseline
		recBuf = encodeVecInt8(recBuf, x, !ix.normalize)
		for _, c := range cells {
			keyBuf = cellKey(keyBuf, uint32(c), label)
			if err := wtx.Put(ix.vcell, keyBuf, recBuf); err != nil {
				return nil, err
			}
		}
	}

	n := len(vecs)
	reconBase := 0.0
	if n > 0 {
		reconBase = reconSum / float64(n)
	}
	mt := &meta{dim: p.Dim, nlist: p.NList, pqM: legacySubquantizers(p.Dim), assign: p.Assign, nprobe: p.NProbe, normalize: p.Normalize, count: int64(n), nextLabel: uint32(n), reconBase: reconBase, buildCount: int64(n), build: newBuild()}
	if err := wtx.Put(ix.vmeta, metaKey, encodeMeta(mt)); err != nil {
		return nil, err
	}
	ix.build = mt.build
	ix.metaRoot = ix.vmeta.RootPage()
	return ix, nil
}

// writeLabel records the label → docID map entry.
func (ix *StoreIndex) writeLabel(wtx *btree.WriteTx, label uint32, docID []byte) error {
	var lk [4]byte
	binary.BigEndian.PutUint32(lk[:], label)
	return wtx.Put(ix.vlbl, lk[:], docID)
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

// OpenTx resolves an existing index, reading params and centroids into RAM.
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
	ix.build = mt.build
	ix.dim, ix.nlist = mt.dim, mt.nlist
	ix.nprobe, ix.assign, ix.normalize = mt.nprobe, mt.assign, mt.normalize
	ix.byteDist = simd.AcceleratedFloatByte()

	cb, err := rtx.Get(ix.vcb, coarseKey)
	if err != nil {
		return nil, err
	}
	// rtx.Get already returns an owned copy (safe to retain after pages release), so
	// the decoded float32 view can alias cb directly — no extra clone.
	ix.coarse = bytesAsF32(cb, ix.nlist*ix.dim) // flat arena view, no per-row headers
	ix.metaRoot = ix.vmeta.RootPage()
	return ix, nil
}

// MetaRoot is the :meta namespace root page (staleness check after compaction).
func (ix *StoreIndex) MetaRoot() uint32 { return ix.metaRoot }

// Build returns the identity of the build this object serves (meta.build).
func (ix *StoreIndex) Build() uint64 { return ix.build }

// Roots returns the root page of every namespace of the index, by the
// namespace's suffix: what a view must have for this object to serve it.
func (ix *StoreIndex) Roots() map[string]uint32 {
	return map[string]uint32{
		":meta": ix.vmeta.RootPage(), ":cb": ix.vcb.RootPage(), ":cell": ix.vcell.RootPage(),
		":vec": ix.vvec.RootPage(), ":lbl": ix.vlbl.RootPage(), ":doc": ix.vdoc.RootPage(),
	}
}

// StoreStats reports per-namespace sizes and counts for the index.
type StoreStats struct {
	Dim, NList, NProbe, Assign    int
	Count                         int64
	Cell, Vec, CB, Lbl, Doc, Meta btree.NamespaceSize // Vec: the empty namespace kept for earlier releases
}

// Stats reads the namespace sizes and meta counters at rtx's snapshot.
func (ix *StoreIndex) Stats(rtx *btree.ReadTx) (StoreStats, error) {
	var s StoreStats
	mt, err := ix.readMeta(rtx)
	if err != nil {
		return s, err
	}
	s.Dim, s.NList, s.NProbe, s.Assign = mt.dim, mt.nlist, mt.nprobe, mt.assign
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

// DriftScore returns how far the index has drifted from its build-time centroids,
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

// Rebuild re-trains the centroids from the live vectors and rewrites the whole
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

	// Collect the live (docID, vector) set into RAM before dropping the
	// namespaces: the int8 records in :cell, replicated by closure → dedup by
	// label (the :cell key's label is its last 4 bytes).
	var ids [][]byte
	var vecs [][]float32
	cur := rtx.NewCursor(old.vcell)
	defer cur.Close()
	if err := cur.First(); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
		return nil, err
	}
	seen := make(map[uint32]struct{})
	for cur.Valid() {
		key, err := cur.Key()
		if err != nil {
			return nil, err
		}
		label := cellKeyLabel(key)
		if _, dup := seen[label]; dup {
			if err := cur.Next(); err != nil {
				return nil, err
			}
			continue
		}
		seen[label] = struct{}{}
		vb, err := cur.Value()
		if err != nil {
			return nil, err
		}
		docID, err := rtx.Get(old.vlbl, key[4:8])
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				if err := cur.Next(); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		ids = append(ids, docID) // rtx.Get returns an owned copy
		v := make([]float32, old.dim)
		decodeVecInt8(vb, old.dim, !old.normalize, v)
		vecs = append(vecs, v)
		if err := cur.Next(); err != nil {
			return nil, err
		}
	}
	cur.Close()

	if len(vecs) == 0 {
		// Every document was deleted since the last build: there is nothing to
		// retrain on (kmeans on an empty set is not defined), and an IVF store
		// cannot exist without centroids (index creation requires documents).
		// Keep the trained centroids, clear the per-vector namespaces, and reset
		// the drift counters. Inserts keep placing into the last trained
		// centroids; the first insert's churn ratio (buildCount=1) then makes the
		// next drift check retrain on real data.
		names := storeNsNames(prefix)
		for _, name := range names[2:] { // :cell, :vec, :lbl, :doc — keep :meta and :cb
			if err := wtx.DeleteNamespace(name); err != nil && !errors.Is(err, btree.ErrNamespaceNotFound) {
				return nil, err
			}
			if _, err := ensureNS(wtx, name); err != nil {
				return nil, err
			}
		}
		b, err := rtx.Get(old.vmeta, metaKey)
		if err != nil {
			return nil, err
		}
		mt, err := decodeMeta(b)
		if err != nil {
			return nil, err
		}
		mt.count, mt.nextLabel = 0, 0
		mt.churn, mt.driftSum, mt.driftN = 0, 0, 0
		mt.buildCount = 1 // churn-ratio denominator; a build never has n=0 otherwise
		mt.build = newBuild()
		if err := wtx.Put(old.vmeta, metaKey, encodeMeta(mt)); err != nil {
			return nil, err
		}
		return OpenTx(rtx, prefix)
	}

	if err := DropNamespaces(wtx, prefix); err != nil {
		return nil, err
	}
	p := StoreParams{
		Dim: old.dim, NList: ivfRebuildNList(old.nlist, len(vecs)),
		Assign: old.assign, NProbe: old.nprobe, Normalize: old.normalize,
		KMeansPP: true, Seed: 1,
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
// nprobe cells (each a contiguous :cell range), scoring every member by exact
// distance on its int8 record, keeps the ef best, and resolves their docIDs.
// The cell scans are sequential — the btree-fit read pattern.
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
	// Per-query terms for the int8 byte-kernel distance (read-only path): Σqn for
	// the offset-binary correction, and ‖qn‖² for L2's squared-distance expansion.
	s.byteDist = ix.byteDist
	if s.byteDist {
		var sum float32
		for _, x := range qn {
			sum += x
		}
		s.qsum = sum
		if !ix.normalize {
			s.qnorm2 = simd.Dot(qn, qn)
		}
	}
	s.cells = ix.nearestCellsInto(qn, ix.nprobe, &s.cd, s.cells)

	// Scan probed cells into s.dedup (label -> best distance).
	s.dedup.reset()
	cur := rtx.NewCursor(ix.vcell)
	defer cur.Close()
	if err := ix.scanCells(s, cur, qn); err != nil {
		return nil, err
	}

	// Keep the ef best by distance. Use quickselect to partition out the ef smallest
	// in O(n) — full-sorting the whole (often thousands-strong) candidate set just to
	// take the top ef was ~23% of search — then sort only those ef.
	s.cands = s.dedup.collect(s.cands)
	if len(s.cands) > ef {
		selectSmallest(s.cands, ef)
		s.cands = s.cands[:ef]
	}
	// Resolve docIDs reading :lbl in LABEL order rather than distance order: the
	// records are keyed by label, so a label-sorted pass walks the btree in
	// ascending key order — leaf-local and prefetch-friendly, sharing upper-tree
	// pages — instead of N random reads that thrash the cache. The result is
	// re-sorted by distance just below.
	slices.SortFunc(s.cands, func(a, b cand) int {
		switch {
		case a.label < b.label:
			return -1
		case a.label > b.label:
			return 1
		default:
			return 0
		}
	})
	out := make([]Candidate, 0, len(s.cands))
	s.docOff = s.docOff[:0]
	backing := make([]byte, 0, len(s.cands)*12)
	var lk [4]byte
	for _, c := range s.cands {
		binary.BigEndian.PutUint32(lk[:], c.label)
		docID, err := rtx.AppendValue(ix.vlbl, lk[:], s.dbuf[:0])
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				continue // defensive: the snapshot has the label's record whenever it has the cell entry
			}
			return nil, err
		}
		s.dbuf = docID
		start := len(backing)
		backing = append(backing, docID...)
		s.docOff = append(s.docOff, [2]int{start, len(backing)})
		out = append(out, Candidate{Distance: c.dist})
	}
	// backing is final now — slice docIDs out of it, then sort (distance, docId)
	// ascending. Sorting the ~ef survivors here (and marking the spec
	// TotallyOrdered) is measurably cheaper than returning them unordered and
	// letting the pipeline's SortIter collect+heap+fetch: the planner skips the
	// SortIter for the default distance order and streams straight to LimitIter
	// (~19% faster end to end). An explicit multi-key Sort still goes through
	// SortIter regardless. The docId tie-break makes the output a TOTAL order, so
	// every verb pages the same sequence through identical ties.
	for i := range out {
		out[i].DocID = backing[s.docOff[i][0]:s.docOff[i][1]]
	}
	slices.SortFunc(out, func(a, b Candidate) int {
		if c := cmpF32(a.Distance, b.Distance); c != 0 {
			return c
		}
		return bytes.Compare(a.DocID, b.DocID)
	})
	return out, nil
}

// candLess orders candidates by (dist, label). The label tie-break is what makes
// the ef-cut deterministic: s.cands arrives from dedup.collect in SLOT order — a
// function of the pooled searcher's table size, i.e. of its history — so a
// distance-only cut selects an arbitrary subset of an exact-tie group straddling
// the boundary, and two verbs of the same query can rank different candidate
// sets. With the tie-break, cs[:ef] is the unique (dist,label)-smallest ef.
func candLess(a, b cand) bool {
	return a.dist < b.dist || (a.dist == b.dist && a.label < b.label)
}

// selectSmallest partitions cs in place so the k smallest by (dist, label)
// occupy cs[:k] (in arbitrary order). Quickselect (Hoare partition,
// median-of-three pivot), O(n) average — used instead of a full sort when only
// the ef best are needed.
func selectSmallest(cs []cand, k int) {
	lo, hi := 0, len(cs)-1
	for lo < hi {
		p := partitionDist(cs, lo, hi)
		switch {
		case p == k:
			return
		case p < k:
			lo = p + 1
		default:
			hi = p - 1
		}
	}
}

// partitionDist partitions cs[lo:hi+1] around a median-of-three pivot by
// (dist, label), returning the final pivot index.
func partitionDist(cs []cand, lo, hi int) int {
	mid := lo + (hi-lo)/2
	// median-of-three (lo, mid, hi) → move median to hi-1 as pivot
	if candLess(cs[mid], cs[lo]) {
		cs[lo], cs[mid] = cs[mid], cs[lo]
	}
	if candLess(cs[hi], cs[lo]) {
		cs[lo], cs[hi] = cs[hi], cs[lo]
	}
	if candLess(cs[hi], cs[mid]) {
		cs[mid], cs[hi] = cs[hi], cs[mid]
	}
	pivot := cs[mid]
	cs[mid], cs[hi-1] = cs[hi-1], cs[mid]
	i := lo
	for j := lo; j < hi-1; j++ {
		if candLess(cs[j], pivot) {
			cs[i], cs[j] = cs[j], cs[i]
			i++
		}
	}
	cs[i], cs[hi-1] = cs[hi-1], cs[i]
	return i
}

// ensureF32 returns a length-n float32 slice reusing buf's capacity when possible.
func ensureF32(buf []float32, n int) []float32 {
	if cap(buf) < n {
		return make([]float32, n)
	}
	return buf[:n]
}

// exactDist is the candidate distance: cosine (1−dot on unit vectors) for a
// normalized index, L2 otherwise — matching the metric the DB exposes as _distance.
func (ix *StoreIndex) exactDist(qn, x []float32) float32 {
	if ix.normalize {
		return 1 - simd.Dot(qn, x)
	}
	return simd.Distance(qn, x)
}

// distBytes is exactDist computed straight from an int8 record's offset-binary
// bytes via the SIMD float×byte kernel — no dequant. comps are the dim component
// bytes, sqnorm = ‖x‖² (L2 only), and qsum=Σqn / qnorm2=‖qn‖² are the per-query
// terms. Returns the SAME distance as exactDist(qn, decode(record)).
//
//	d = dot(qn, x) = scale·(DotFloatByte(qn, comps) − 128·Σqn)
//	cosine: 1 − d         (stored x is unit length)
//	L2:     sqrt(max(0, ‖qn‖² + ‖x‖² − 2·d))
func (ix *StoreIndex) distBytes(qn []float32, scale, sqnorm float32, comps []byte, qsum, qnorm2 float32) float32 {
	d := scale * (simd.DotFloatByte(qn, comps) - int8Bias*qsum)
	if ix.normalize {
		return 1 - d
	}
	v := qnorm2 + sqnorm - 2*d
	if v < 0 {
		v = 0 // float roundoff when qn ≈ x; ‖q−x‖² is never truly negative
	}
	return float32(math.Sqrt(float64(v)))
}

// scanCells scores every member of the probed cells by exact distance on its
// int8 record: straight from the bytes with the byte kernel, else dequantized.
func (ix *StoreIndex) scanCells(s *searcher, cur *btree.Cursor, qn []float32) error {
	s.vbufF32 = ensureF32(s.vbufF32, ix.dim)
	var seek [8]byte
	for _, c := range s.cells {
		binary.BigEndian.PutUint32(seek[0:], uint32(c))
		binary.BigEndian.PutUint32(seek[4:], 0)
		if err := cur.Seek(seek[:]); err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				continue
			}
			return err
		}
		for cur.Valid() {
			key, err := cur.Key()
			if err != nil {
				return err
			}
			if !bytes.Equal(key[:4], seek[:4]) {
				break
			}
			val, err := cur.Value()
			if err != nil {
				return err
			}
			var d float32
			if s.byteDist {
				scale, sqnorm, comps, ok := int8Split(val, ix.dim, !ix.normalize)
				if !ok {
					return fmt.Errorf("vivf: bad int8 cell record len %d", len(val))
				}
				d = ix.distBytes(qn, scale, sqnorm, comps, s.qsum, s.qnorm2)
			} else {
				d = ix.exactDist(qn, decodeVecInt8(val, ix.dim, !ix.normalize, s.vbufF32))
			}
			s.dedup.putMin(cellKeyLabel(key), d)
			if err := cur.Next(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Insert adds (or replaces) docID's vector: assign to its Assign nearest cells and
// write its int8 record under each. A replace first removes the old entries.
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
	if err := ix.writeLabel(wtx, label, docID); err != nil {
		return err
	}
	cells := ix.nearestCells(x, ix.assign)
	if err := ix.putDocCells(wtx, docID, label, cells); err != nil {
		return err
	}
	// Track how well the frozen centroids fit this new vector (drift signal).
	r := make([]float32, ix.dim)
	simd.Sub_Into(r, x, ix.coarseRow(cells[0]))
	mt.driftSum += float64(sqNorm(r))
	mt.driftN++
	var keyBuf []byte
	rec := encodeVecInt8(nil, x, !ix.normalize)
	for _, c := range cells {
		keyBuf = cellKey(keyBuf, uint32(c), label)
		if err := wtx.Put(ix.vcell, keyBuf, rec); err != nil {
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

// removeDoc deletes docID's :cell/:lbl/:doc records (if present) and updates
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
