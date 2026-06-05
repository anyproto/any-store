package vector

import (
	"encoding/binary"
	"unsafe"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// PagedHNSW is a prototype of "Option B" from COMPARISON_pgvector.md: instead of
// holding every vector in a contiguous RAM arena, it keeps only the graph
// *topology* (adjacency + levels + entry point) resident and reads each node's
// vector from the btree on demand during traversal — paging it through the same
// page cache (pcache) the btree already maintains. This is the DiskANN-lite /
// pgvector residency model: RAM scales with the graph, not the vectors.
//
// Memory math (per node): the topology is ~M0*4 = 128 B; a vector is dim*4 B
// (512 B at dim=128, 6 KB at dim=1536). So paging the vectors keeps ~80% (dim
// 128) to ~98% (dim 1536) of the data on disk while the navigable graph stays in
// RAM. The cost is one btree point-lookup per visited node instead of one array
// index — that overhead is exactly what TestPagedVsMemory measures.
//
// This prototype borrows an existing FlatHNSW purely for its in-RAM topology
// (neighbors/level/entry/deleted); the vectors it reads come from the btree, not
// from the FlatHNSW slab. Search is single-threaded (one reusable scratch).
type PagedHNSW struct {
	g     *FlatHNSW // topology source (its vector slab is ignored)
	db    *btree.DB
	vecNS *btree.Namespace
	dim   int
	dist  DistanceFunc

	// zero-copy scratch: scratchB aliases the bytes of scratchF, so a btree read
	// into scratchB[:0] lands directly in scratchF as []float32 (no decode copy).
	scratchF []float32
	scratchB []byte

	gets int // btree point-lookups performed (telemetry)
}

// BuildPagedFromFlat persists g's vectors into a <name>:vec btree namespace and
// returns a PagedHNSW that traverses g's topology while reading vectors from the
// btree. g's own vector slab can be released afterwards (the paged index does
// not use it).
func BuildPagedFromFlat(g *FlatHNSW, db *btree.DB, name string, metric Metric) (*PagedHNSW, error) {
	wtx, err := db.BeginWrite()
	if err != nil {
		return nil, err
	}
	var vecNS *btree.Namespace
	if vecNS, err = wtx.GetNamespace(name + ":vec"); err != nil {
		if vecNS, err = wtx.CreateNamespace(name + ":vec"); err != nil {
			_ = wtx.Rollback()
			return nil, err
		}
	}
	var keyBuf [8]byte
	for id := uint32(0); id < uint32(len(g.keys)); id++ {
		binary.BigEndian.PutUint64(keyBuf[:], uint64(id))
		vb := f32bytes(g.vectorAt(id))
		if err := wtx.Put(vecNS, keyBuf[:], vb); err != nil {
			_ = wtx.Rollback()
			return nil, err
		}
	}
	if err := wtx.Commit(); err != nil {
		return nil, err
	}
	if vecNS, err = db.GetNamespace(name + ":vec"); err != nil {
		return nil, err
	}

	scratchF := make([]float32, g.dim)
	p := &PagedHNSW{
		g:        g,
		db:       db,
		vecNS:    vecNS,
		dim:      g.dim,
		dist:     metric.DistanceFor(),
		scratchF: scratchF,
		scratchB: unsafe.Slice((*byte)(unsafe.Pointer(&scratchF[0])), g.dim*4),
	}
	return p, nil
}

// f32bytes reinterprets a []float32 as its little-endian byte view (x86). For a
// portable on-disk format this would be an explicit encode; the prototype reads
// and writes the host layout to isolate the *paging* cost, not the codec cost.
func f32bytes(v []float32) []byte {
	if len(v) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*4)
}

// Gets returns the cumulative number of btree vector lookups (telemetry).
func (p *PagedHNSW) Gets() int { return p.gets }

// vecOf reads node id's vector from the btree into the reusable scratch and
// returns it as []float32 (zero-copy when the read fits scratch's capacity).
func (p *PagedHNSW) vecOf(rtx *btree.ReadTx, id uint32, keyBuf []byte) []float32 {
	binary.BigEndian.PutUint64(keyBuf, uint64(id))
	p.gets++
	b, err := rtx.AppendValue(p.vecNS, keyBuf, p.scratchB[:0])
	if err != nil || len(b) != p.dim*4 {
		return p.scratchF // (shouldn't happen in the prototype)
	}
	if &b[0] == &p.scratchB[0] {
		return p.scratchF // landed in scratch, no realloc
	}
	// fell back to a fresh buffer: reinterpret it
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), p.dim)
}

// Search runs the HNSW query reading vectors from the btree. It opens one read
// transaction for the query (the realistic unit) so the traversal sees a
// consistent snapshot and benefits from the reader's page cache.
func (p *PagedHNSW) Search(query []float32, k int) ([]SearchResult, error) {
	g := p.g
	if !g.hasEntry || k <= 0 {
		return nil, nil
	}
	rtx, err := p.db.BeginRead()
	if err != nil {
		return nil, err
	}
	defer rtx.Rollback()

	var keyBuf [8]byte
	dist := func(id uint32) float32 {
		return p.dist(query, p.vecOf(rtx, id, keyBuf[:]))
	}

	// descend the upper layers greedily (ef=1)
	ep := g.entryID
	for lc := g.topLayer; lc > 0; lc-- {
		ep = p.greedyClosest(g, ep, lc, dist)
	}
	ef := g.EfSearch
	if k > ef {
		ef = k
	}
	found := p.searchLayer(g, ep, ef, 0, dist)
	if k > len(found) {
		k = len(found)
	}
	out := make([]SearchResult, k)
	for i := 0; i < k; i++ {
		out[i] = SearchResult{Key: g.keys[found[i].id], Distance: found[i].dist}
	}
	return out, nil
}

func (p *PagedHNSW) greedyClosest(g *FlatHNSW, ep uint32, layer int32, dist func(uint32) float32) uint32 {
	best := ep
	bestDist := dist(ep)
	for {
		improved := false
		for _, nb := range g.neighbors(best, layer) {
			if d := dist(nb); d < bestDist {
				bestDist, best, improved = d, nb, true
			}
		}
		if !improved {
			return best
		}
	}
}

// searchLayer mirrors FlatHNSW.searchLayer but pulls vectors through dist() (a
// btree read) and respects tombstones the same way.
func (p *PagedHNSW) searchLayer(g *FlatHNSW, ep uint32, ef int, layer int32, dist func(uint32) float32) []candidate {
	var vis visitedList
	vis.prepare(len(g.keys))
	var cand, res cheap
	cand.reset(false)
	res.reset(true)

	d0 := dist(ep)
	vis.visit(ep)
	cand.push(candidate{d0, ep})
	if !g.deleted[ep] {
		res.push(candidate{d0, ep})
	}

	for cand.len() > 0 {
		cur := cand.pop()
		if res.len() >= ef && cur.dist > res.peek().dist {
			break
		}
		for _, nb := range g.neighbors(cur.id, layer) {
			if !vis.visit(nb) {
				continue
			}
			d := dist(nb)
			admit := res.len() < ef || d < res.peek().dist
			if !admit {
				continue
			}
			cand.push(candidate{d, nb})
			if g.deleted[nb] {
				continue
			}
			res.push(candidate{d, nb})
			if res.len() > ef {
				res.pop()
			}
		}
	}

	out := make([]candidate, 0, res.len())
	for res.len() > 0 {
		out = append(out, res.pop())
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// SearchHybrid models the production answer (DiskANN / pgvector-with-rescore):
// ROUTE the graph using an in-RAM vector copy (here g's slab; in a real build a
// quantized int8/binary slab a quarter/thirtieth the size), then page only the
// final ef candidates' full vectors from the btree to RERANK. This collapses the
// ~1900 btree reads/query of pure paging down to ef reads/query, so latency
// returns to near in-memory while RAM stays at (routing-slab + topology).
//
// Because the prototype reuses the same float32 vectors for routing and rerank,
// the *results* are identical to Search; what it isolates is the latency/Get
// profile of the hybrid (in-RAM routing + ef paged rerank reads).
func (p *PagedHNSW) SearchHybrid(query []float32, k int) ([]SearchResult, error) {
	g := p.g
	if !g.hasEntry || k <= 0 {
		return nil, nil
	}
	// in-RAM routing (no btree reads) — reuse the arena search
	routed := g.Search(query, maxInt(k, g.EfSearch))

	rtx, err := p.db.BeginRead()
	if err != nil {
		return nil, err
	}
	defer rtx.Rollback()
	var keyBuf [8]byte

	// page each routed candidate's full vector and recompute the exact distance
	out := make([]SearchResult, 0, len(routed))
	for _, r := range routed {
		id := g.keyToID[r.Key]
		d := p.dist(query, p.vecOf(rtx, id, keyBuf[:]))
		out = append(out, SearchResult{Key: r.Key, Distance: d})
	}
	sortResultsByDist(out)
	if k < len(out) {
		out = out[:k]
	}
	return out, nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func sortResultsByDist(r []SearchResult) {
	for i := 1; i < len(r); i++ {
		for j := i; j > 0 && r[j].Distance < r[j-1].Distance; j-- {
			r[j], r[j-1] = r[j-1], r[j]
		}
	}
}

// TopologyBytes approximates the resident RAM of the paged index: the graph
// arenas WITHOUT the vector slab (the whole point — vectors live on disk).
func (p *PagedHNSW) TopologyBytes() int {
	g := p.g
	return cap(g.links)*4 + cap(g.counts)*4 + cap(g.keys)*8 +
		cap(g.level)*4 + cap(g.linkOff)*4 + cap(g.countOff)*4 + cap(g.deleted)
}
