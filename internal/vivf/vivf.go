package vivf

import (
	"runtime"
	"sync"

	"github.com/viterin/vek/vek32"
)

// Params configures an IVF-PQ index. Dim must be divisible by M.
type Params struct {
	Dim   int // vector dimension
	NList int // number of coarse (IVF) cells
	M     int // number of PQ subquantizers; code is M bytes
	Seed  int64

	// KMeansPP seeds k-means with D² (k-means++) sampling instead of random points.
	KMeansPP bool
	// Assign is the multi-assignment / closure factor: each vector is placed in its
	// Assign nearest cells (with a per-cell residual code), so a query finds it at
	// lower nprobe (SPANN closure). 1 = standard single assignment.
	Assign int
}

const pqK = 256 // centroids per PQ subquantizer (1 byte/code)

// cellEntry is one member of an inverted list: a label and its PQ code relative to
// THAT cell's centroid (codes differ per cell under multi-assignment). In the btree
// design this is the :cell record value, keyed listID‖label.
type cellEntry struct {
	label int32
	code  []byte
}

// Index is an in-RAM IVF-PQ index over unit-normalized vectors. Cosine ranking on
// the original vectors equals L2 ranking after normalization, so everything below
// works in L2 on normalized data and matches a cosine ground truth.
//
// In the eventual btree design (RESEARCH_IVFPQ_BTREE.md §4): coarse + pqcb live in
// the :cb namespace; cells become :cell records keyed listID‖label; norm (the
// re-rank store) is the existing :vec namespace.
type Index struct {
	dim, nlist, m, dsub int

	coarse [][]float32   // [nlist][dim]   coarse centroids
	pqcb   [][][]float32 // [m][pqK][dsub] PQ sub-codebooks (trained on residuals)

	cells [][]cellEntry // [nlist] inverted lists
	norm  [][]float32   // [label] -> unit-normalized vector (re-rank store == :vec)
}

// Train builds the index from vecs (label == slice index). It normalizes a copy,
// learns the coarse quantizer, encodes residuals with PQ, and fills the inverted
// lists — the offline build of RESEARCH_IVFPQ_BTREE.md §5.1.
func Train(vecs [][]float32, p Params) *Index {
	if p.Assign < 1 {
		p.Assign = 1
	}
	ix := &Index{dim: p.Dim, nlist: p.NList, m: p.M, dsub: p.Dim / p.M}
	n := len(vecs)

	// 1. Normalize (cosine -> unit vectors).
	ix.norm = make([][]float32, n)
	for i, v := range vecs {
		ix.norm[i] = normalize(v)
	}

	// 2-4. Learn the coarse quantizer and PQ sub-codebooks (shared with the
	//      btree-resident builder in store.go).
	ix.coarse, ix.pqcb = trainModel(ix.norm, p)

	// 5. Place each vector in its Assign nearest cells, encoding the per-cell
	//    residual (closure: a boundary vector lives in several lists).
	ix.cells = make([][]cellEntry, p.NList)
	r := make([]float32, ix.dim)
	for i, x := range ix.norm {
		for _, c := range topNCells(x, ix.coarse, p.Assign) {
			vek32.Sub_Into(r, x, ix.coarse[c])
			ix.cells[c] = append(ix.cells[c], cellEntry{int32(i), ix.encode(r)})
		}
	}
	return ix
}

// trainModel learns the IVF coarse centroids and the M PQ sub-codebooks (on
// primary-assignment residuals). Shared by the in-RAM prototype (Train) and the
// btree-resident builder (BulkBuild in store.go) so both encode identically.
func trainModel(norm [][]float32, p Params) (coarse [][]float32, pqcb [][][]float32) {
	n := len(norm)
	dim := p.Dim
	dsub := p.Dim / p.M

	// Coarse quantizer: k-means with nlist cells.
	coarse, assign := kmeans(norm, p.NList, 15, p.Seed, p.KMeansPP)

	// Primary residuals r = x - coarse[assign] -> train PQ on these.
	resid := make([][]float32, n)
	for i, x := range norm {
		r := make([]float32, dim)
		vek32.Sub_Into(r, x, coarse[assign[i]])
		resid[i] = r
	}

	// PQ: train M independent sub-codebooks on the residual subspaces, in parallel.
	pqcb = make([][][]float32, p.M)
	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	for mm := 0; mm < p.M; mm++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(mm int) {
			defer wg.Done()
			defer func() { <-sem }()
			sub := make([][]float32, n)
			lo := mm * dsub
			for i := range resid {
				sub[i] = resid[i][lo : lo+dsub]
			}
			cb, _ := kmeans(sub, pqK, 12, p.Seed+int64(mm)+1, p.KMeansPP)
			pqcb[mm] = cb
		}(mm)
	}
	wg.Wait()
	return coarse, pqcb
}

// avgReplication is the mean number of cell entries per distinct vector — the
// storage multiplier closure costs (1.0 = single assignment).
func (ix *Index) avgReplication() float64 {
	var entries int
	for _, c := range ix.cells {
		entries += len(c)
	}
	return float64(entries) / float64(len(ix.norm))
}

// encode quantizes a residual to its M-byte PQ code.
func (ix *Index) encode(r []float32) []byte {
	code := make([]byte, ix.m)
	for mm := 0; mm < ix.m; mm++ {
		lo := mm * ix.dsub
		code[mm] = byte(nearestSmall(r[lo:lo+ix.dsub], ix.pqcb[mm]))
	}
	return code
}

// Search returns the labels of the approximate k nearest neighbours of q, using
// nprobe cells and re-ranking the top kFactor·k ADC candidates by exact distance
// (RESEARCH_IVFPQ_BTREE.md §5.2).
func (ix *Index) Search(q []float32, k, nprobe, kFactor int) []int {
	qn := normalize(q)
	cells := topNCells(qn, ix.coarse, nprobe)

	// Scan probed cells, scoring each member code-only via a per-cell ADC LUT.
	// A label may appear in several probed cells (closure); keep its best ADC.
	best := make(map[int32]float32)
	lut := make([]float32, ix.m*pqK)
	qr := make([]float32, ix.dim)
	for _, c := range cells {
		vek32.Sub_Into(qr, qn, ix.coarse[c]) // query residual for this cell
		ix.buildLUT(qr, lut)
		for _, e := range ix.cells[c] {
			d := adc(lut, e.code, ix.m)
			if prev, ok := best[e.label]; !ok || d < prev {
				best[e.label] = d
			}
		}
	}

	type cand struct {
		label int32
		dist  float32
	}
	cands := make([]cand, 0, len(best))
	for label, d := range best {
		cands = append(cands, cand{label, d})
	}

	// Keep the top kFactor·k by approximate distance, then re-rank those by EXACT
	// distance on the stored (normalized) vectors.
	rerankN := kFactor * k
	partialSortByDist(cands, func(c cand) float32 { return c.dist }, rerankN)
	if len(cands) > rerankN {
		cands = cands[:rerankN]
	}
	for i := range cands {
		cands[i].dist = vek32.Distance(qn, ix.norm[cands[i].label])
	}
	partialSortByDist(cands, func(c cand) float32 { return c.dist }, k)
	if len(cands) > k {
		cands = cands[:k]
	}
	out := make([]int, len(cands))
	for i, c := range cands {
		out[i] = int(c.label)
	}
	return out
}

// buildLUT fills lut[m*pqK+j] = ||qr_sub_m - pqcb[m][j]||^2 — the M×256 distance
// table that turns per-candidate scoring into M lookups + adds.
func (ix *Index) buildLUT(qr []float32, lut []float32) {
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

// adc sums the M table entries selected by the code — the asymmetric distance.
func adc(lut []float32, code []byte, m int) float32 {
	var s float32
	for mm := 0; mm < m; mm++ {
		s += lut[mm*pqK+int(code[mm])]
	}
	return s
}
