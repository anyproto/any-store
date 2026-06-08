package vivf

import (
	"encoding/binary"
	"errors"
	"fmt"

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

	metaRoot uint32
}

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

	// Encode + place each vector. label == build order (dense 0..n-1).
	var keyBuf []byte
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
		for _, c := range cells {
			vek32.Sub_Into(r, x, ix.coarse[c])
			keyBuf = cellKey(keyBuf, uint32(c), label)
			if err := wtx.Put(ix.vcell, keyBuf, encodeResidual(r, ix)); err != nil {
				return nil, err
			}
		}
	}

	mt := &meta{dim: p.Dim, nlist: p.NList, m: p.M, assign: p.Assign, nprobe: p.NProbe, normalize: p.Normalize, count: int64(len(vecs)), nextLabel: uint32(len(vecs))}
	if err := wtx.Put(ix.vmeta, metaKey, encodeMeta(mt)); err != nil {
		return nil, err
	}
	ix.metaRoot = ix.vmeta.RootPage()
	return ix, nil
}

// encodeResidual produces the stored :cell value (PQ code) for a residual.
func encodeResidual(r []float32, ix *StoreIndex) []byte {
	code := make([]byte, ix.m)
	for mm := 0; mm < ix.m; mm++ {
		lo := mm * ix.dsub
		code[mm] = byte(nearestSmall(r[lo:lo+ix.dsub], ix.pqcb[mm]))
	}
	return code
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
	qn := q
	if ix.normalize {
		qn = normalize(q)
	}
	cells := topNCells(qn, ix.coarse, ix.nprobe)

	// Scan probed cells; keep each label's best ADC (a label may recur via closure).
	best := make(map[uint32]float32)
	lut := make([]float32, ix.m*pqK)
	qr := make([]float32, ix.dim)
	cur := rtx.NewCursor(ix.vcell)
	defer cur.Close()
	var seek [8]byte
	for _, c := range cells {
		vek32.Sub_Into(qr, qn, ix.coarse[c])
		ix.buildLUT(qr, lut)
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
			if cellKeyList(key) != uint32(c) {
				break // left this cell's contiguous range
			}
			code, err := cur.Value()
			if err != nil {
				return nil, err
			}
			label := cellKeyLabel(key)
			d := adc(lut, code, ix.m)
			if prev, ok := best[label]; !ok || d < prev {
				best[label] = d
			}
			if err := cur.Next(); err != nil {
				return nil, err
			}
		}
	}

	type cand struct {
		label uint32
		dist  float32
	}
	cands := make([]cand, 0, len(best))
	for label, d := range best {
		cands = append(cands, cand{label, d})
	}
	// Truncate to ef best by ADC before the exact re-rank, bounding :vec reads.
	partialSortByDist(cands, func(c cand) float32 { return c.dist }, ef)
	if len(cands) > ef {
		cands = cands[:ef]
	}

	// Re-rank survivors by exact distance, resolving docIDs.
	out := make([]Candidate, 0, len(cands))
	var lk [4]byte
	for _, c := range cands {
		binary.BigEndian.PutUint32(lk[:], c.label)
		vb, err := rtx.Get(ix.vvec, lk[:])
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				continue // tombstoned between scan and re-rank
			}
			return nil, err
		}
		x := bytesAsF32(vb, ix.dim)
		docID, err := rtx.Get(ix.vlbl, lk[:])
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, Candidate{DocID: cloneBytes(docID), Distance: ix.exactDist(qn, x)})
	}
	partialSortByDist(out, func(c Candidate) float32 { return c.Distance }, len(out))
	return out, nil
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
	var keyBuf []byte
	r := make([]float32, ix.dim)
	for _, c := range cells {
		vek32.Sub_Into(r, x, ix.coarse[c])
		keyBuf = cellKey(keyBuf, uint32(c), label)
		if err := wtx.Put(ix.vcell, keyBuf, encodeResidual(r, ix)); err != nil {
			return err
		}
	}
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
