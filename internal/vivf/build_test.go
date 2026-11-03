package vivf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/buildpool"
	"github.com/anyproto/any-store/v2/internal/simd"
	"github.com/stretchr/testify/require"
)

// kv is one btree entry.
type kv struct{ k, v []byte }

// dumpIndex reads every entry of the index's namespaces, by suffix, and its
// meta record with the build id cleared (a random value per build).
func dumpIndex(t *testing.T, db *btree.DB, prefix string) (map[string][]kv, meta) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	out := make(map[string][]kv)
	for _, suffix := range []string{nsCB, nsCell, nsVec, nsLbl, nsDoc} {
		ns, err := rtx.GetNamespace(prefix + suffix)
		require.NoError(t, err)
		cur := rtx.NewCursor(ns)
		if err := cur.First(); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
			t.Fatal(err)
		}
		for cur.Valid() {
			k, err := cur.Key()
			require.NoError(t, err)
			v, err := cur.Value()
			require.NoError(t, err)
			out[suffix] = append(out[suffix], kv{bytes.Clone(k), bytes.Clone(v)})
			require.NoError(t, cur.Next())
		}
		cur.Close()
	}
	vmeta, err := rtx.GetNamespace(prefix + nsMeta)
	require.NoError(t, err)
	b, err := rtx.Get(vmeta, metaKey)
	require.NoError(t, err)
	mt, err := decodeMeta(b)
	require.NoError(t, err)
	mt.build = 0
	return out, *mt
}

func requireSameIndex(t *testing.T, want, got *btree.DB) {
	t.Helper()
	wantNS, wantMeta := dumpIndex(t, want, "ivf")
	gotNS, gotMeta := dumpIndex(t, got, "ivf")
	require.Equal(t, encodeMeta(&wantMeta), encodeMeta(&gotMeta), "meta, drift baseline bit for bit")
	for _, suffix := range []string{nsCB, nsCell, nsVec, nsLbl, nsDoc} {
		w, g := wantNS[suffix], gotNS[suffix]
		require.Len(t, g, len(w), "%s entries", suffix)
		for i := range w {
			require.Equal(t, w[i].k, g[i].k, "%s key #%d", suffix, i)
			require.Equal(t, w[i].v, g[i].v, "%s value #%d", suffix, i)
		}
	}
}

// mainNearestCells is the cell finder of earlier releases, copied as they
// shipped it: every centroid scored in index order, then a
// distance-only sort. It is the routing — placement and probing — earlier
// releases use, so the index this code writes and reads must agree with it,
// exact distance ties included.
func mainNearestCells(ix *StoreIndex, q []float32, n int) []int {
	cd := make([]cellDist, ix.nlist)
	for c := 0; c < ix.nlist; c++ {
		cd[c] = cellDist{c, simd.Distance(q, ix.coarseRow(c))}
	}
	slices.SortFunc(cd, func(a, b cellDist) int { return cmpF32(a.dist, b.dist) })
	if n > ix.nlist {
		n = ix.nlist
	}
	out := make([]int, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, cd[i].idx)
	}
	return out
}

// lowestFirstCells ranks exactly equidistant cells by index instead: a tie
// order another sort could produce.
func lowestFirstCells(ix *StoreIndex, q []float32, n int) []int {
	cd := make([]cellDist, ix.nlist)
	for c := 0; c < ix.nlist; c++ {
		cd[c] = cellDist{c, simd.Distance(q, ix.coarseRow(c))}
	}
	slices.SortStableFunc(cd, func(a, b cellDist) int { return cmpF32(a.dist, b.dist) })
	out := make([]int, 0, n)
	for i := 0; i < min(n, ix.nlist); i++ {
		out = append(out, cd[i].idx)
	}
	return out
}

// openIndexNS creates the index namespaces in wtx and returns an index bound to
// them, with p's settings and the given centroids written to :cb.
func openIndexNS(t *testing.T, wtx *btree.WriteTx, p StoreParams, cents [][]float32) *StoreIndex {
	t.Helper()
	p.withDefaults()
	ix := &StoreIndex{dim: p.Dim, nlist: len(cents), nprobe: p.NProbe, assign: p.Assign, normalize: p.Normalize}
	var ns [6]*btree.Namespace
	var err error
	for i, name := range storeNsNames("ivf") {
		ns[i], err = ensureNS(wtx, name)
		require.NoError(t, err)
	}
	ix.vmeta, ix.vcb, ix.vcell, ix.vvec, ix.vlbl, ix.vdoc = ns[0], ns[1], ns[2], ns[3], ns[4], ns[5]
	blob := encodeCentroids(cents)
	require.NoError(t, wtx.Put(ix.vcb, coarseKey, blob))
	ix.coarse = bytesAsF32(blob, ix.nlist*ix.dim)
	return ix
}

func putBuildMeta(t *testing.T, wtx *btree.WriteTx, ix *StoreIndex, p StoreParams, n int, reconSum float64) {
	t.Helper()
	p.withDefaults()
	mt := &meta{dim: p.Dim, nlist: ix.nlist, pqM: legacySubquantizers(p.Dim), assign: p.Assign, nprobe: p.NProbe, normalize: p.Normalize, count: int64(n), nextLabel: uint32(n), reconBase: reconSum / float64(n), buildCount: int64(n), build: newBuild()}
	require.NoError(t, wtx.Put(ix.vmeta, metaKey, encodeMeta(mt)))
}

// serialWrite writes the index the way earlier releases' bulk build does,
// over the given centroids: one vector at a time in label order, its cells from
// cellsOf, its :doc record laid out by hand, every write landing wherever its
// key falls. vecs are already normalized for a cosine index.
func serialWrite(t *testing.T, db *btree.DB, p StoreParams, cents [][]float32, ids [][]byte, vecs [][]float32, cellsOf func(*StoreIndex, []float32, int) []int) {
	t.Helper()
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ix := openIndexNS(t, wtx, p, cents)
	var reconSum float64
	r := make([]float32, ix.dim)
	for i, x := range vecs {
		label := uint32(i)
		var lk [4]byte
		binary.BigEndian.PutUint32(lk[:], label)
		require.NoError(t, wtx.Put(ix.vlbl, lk[:], ids[i]))
		cells := cellsOf(ix, x, ix.assign)
		var b [4]byte
		dc := make([]byte, 0, 4+1+4*len(cells))
		binary.LittleEndian.PutUint32(b[:], label)
		dc = append(dc, b[:]...)
		dc = append(dc, byte(len(cells)))
		for _, c := range cells {
			binary.LittleEndian.PutUint32(b[:], uint32(c))
			dc = append(dc, b[:]...)
		}
		require.NoError(t, wtx.Put(ix.vdoc, ids[i], dc))
		simd.Sub_Into(r, x, ix.coarseRow(cells[0]))
		reconSum += float64(sqNorm(r))
		rec := encodeVecInt8(nil, x, !ix.normalize)
		for _, c := range cells {
			require.NoError(t, wtx.Put(ix.vcell, cellKey(nil, uint32(c), label), rec))
		}
	}
	putBuildMeta(t, wtx, ix, p, len(vecs), reconSum)
	require.NoError(t, wtx.Commit())
}

// mainBuild is the bulk build of earlier releases: the same k-means, then
// serialWrite with their cell finder.
func mainBuild(t *testing.T, db *btree.DB, p StoreParams, ids [][]byte, vecs [][]float32) {
	t.Helper()
	norm := make([][]float32, len(vecs))
	for i, v := range vecs {
		norm[i] = v
		if p.Normalize {
			norm[i] = normalize(v)
		}
	}
	cents := kmeans(norm, p.NList, 15, p.Seed, p.KMeansPP)
	serialWrite(t, db, p, cents, ids, norm, mainNearestCells)
}

// placeWrite writes the index over the given centroids with this code's
// placement: assignUnique, then place.
func placeWrite(t *testing.T, db *btree.DB, p StoreParams, cents [][]float32, ids [][]byte, vecs [][]float32) {
	t.Helper()
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ix := openIndexNS(t, wtx, p, cents)
	reconSum, err := ix.place(wtx, ids, vecs, assignUnique(vecs, cents))
	require.NoError(t, err)
	putBuildMeta(t, wtx, ix, p, len(vecs), reconSum)
	require.NoError(t, wtx.Commit())
}

// shuffledIDs returns n distinct doc ids in an order unrelated to their bytes,
// so :doc's key order differs from label order.
func shuffledIDs(n int, seed int64) [][]byte {
	ids := make([][]byte, n)
	for i, j := range rand.New(rand.NewSource(seed)).Perm(n) {
		ids[i] = bid(j)
	}
	return ids
}

func seqIDs(n int) [][]byte {
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = bid(i)
	}
	return ids
}

func buildWithIDs(t *testing.T, db *btree.DB, p StoreParams, ids [][]byte, vecs [][]float32) {
	t.Helper()
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = BulkBuild(wtx, "ivf", p, ids, vecs)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
}

// alternatingVecs is n 1-D vectors alternating [0], [2]. Built with NList 13,
// k-means leaves the centroids duplicated (several at 0, several at 2), so
// every vector sits at an exact tie between cells.
func alternatingVecs(n int) [][]float32 {
	vecs := make([][]float32, n)
	for i := range vecs {
		vecs[i] = []float32{float32(2 * (i % 2))}
	}
	return vecs
}

// repeatedVecs is 8 distinct dim-16 points, each repeated 25 times: random
// init draws duplicated centroids from them.
func repeatedVecs() [][]float32 {
	base := clusteredVecs(8, 16, 8, 3)
	vecs := make([][]float32, 200)
	for i := range vecs {
		vecs[i] = base[i%len(base)]
	}
	return vecs
}

// TestBulkBuildMatchesMainBuild pins that the parallel, key-ordered build —
// cells from assignUnique or nearestCellsInto, records encoded a
// chunk at a time — writes exactly the index earlier releases write: the
// same :cb, :cell, :lbl, :doc and meta (drift baseline bit for bit), so either
// release reads, inserts into and searches the other's index alike. Fixtures
// with exact distance ties between centroids included.
func TestBulkBuildMatchesMainBuild(t *testing.T) {
	clustered := clusteredVecs(3000, 48, 30, 11)
	for _, tc := range []struct {
		name      string
		vecs      [][]float32
		ids       [][]byte
		p         StoreParams
		chunkRecs int // 0: the default chunk (one chunk at these sizes)
	}{
		{name: "clustered/cosine", vecs: clustered, ids: shuffledIDs(3000, 5), p: StoreParams{Dim: 48, NList: 64, Assign: 1, NProbe: 16, Normalize: true, KMeansPP: true, Seed: 3}},
		{name: "clustered/l2", vecs: clustered, ids: shuffledIDs(3000, 5), p: StoreParams{Dim: 48, NList: 64, Assign: 1, NProbe: 16, KMeansPP: true, Seed: 3}},
		{name: "clustered/cosine/assign3", vecs: clustered, ids: shuffledIDs(3000, 5), p: StoreParams{Dim: 48, NList: 64, Assign: 3, NProbe: 16, Normalize: true, KMeansPP: true, Seed: 3}},
		{name: "clustered/l2/assign2/chunk7", vecs: clustered, ids: shuffledIDs(3000, 5), p: StoreParams{Dim: 48, NList: 64, Assign: 2, NProbe: 16, KMeansPP: true, Seed: 3}, chunkRecs: 7},
		{name: "clustered/cosine/chunk7", vecs: clustered, ids: shuffledIDs(3000, 5), p: StoreParams{Dim: 48, NList: 64, Assign: 1, NProbe: 16, Normalize: true, KMeansPP: true, Seed: 3}, chunkRecs: 7},
		{name: "ties/alternating", vecs: alternatingVecs(26), ids: seqIDs(26), p: StoreParams{Dim: 1, NList: 13, Assign: 1, NProbe: 1, KMeansPP: true, Seed: 0}},
		{name: "ties/alternating/assign2", vecs: alternatingVecs(26), ids: seqIDs(26), p: StoreParams{Dim: 1, NList: 13, Assign: 2, NProbe: 1, KMeansPP: true, Seed: 0}},
		{name: "ties/repeated", vecs: repeatedVecs(), ids: shuffledIDs(200, 9), p: StoreParams{Dim: 16, NList: 16, Assign: 1, NProbe: 4, Seed: 1}},
		{name: "ties/repeated/cosine/assign3", vecs: repeatedVecs(), ids: shuffledIDs(200, 9), p: StoreParams{Dim: 16, NList: 16, Assign: 3, NProbe: 4, Normalize: true, Seed: 1}, chunkRecs: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.chunkRecs > 0 {
				defer func(v int) { placeChunkBytes = v }(placeChunkBytes)
				placeChunkBytes = tc.chunkRecs * int8RecordSize(tc.p.Dim, !tc.p.Normalize)
			}
			want := openMem(t)
			mainBuild(t, want, tc.p, tc.ids, tc.vecs)
			got := openMem(t)
			buildWithIDs(t, got, tc.p, tc.ids, tc.vecs)
			requireSameIndex(t, want, got)
		})
	}
}

// TestBulkBuildIndependentOfBudget: each item of a parallel phase is handled
// by exactly one worker, results are written per item or reduced in a fixed
// order, and no phase splits its work by the budget, so the index is the same
// at any build budget. Across GOMAXPROCS it is the same with k-means++ off:
// k-means++ sums d² per GOMAXPROCS chunk, as earlier releases do, so with it
// on another GOMAXPROCS can round that sum differently and, rarely, seed other
// centroids. The GOMAXPROCS case below therefore runs with k-means++ off.
func TestBulkBuildIndependentOfBudget(t *testing.T) {
	vecs := clusteredVecs(2000, 32, 20, 13)
	ids := shuffledIDs(2000, 7)
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	defer buildpool.Set(buildpool.Current().Size())
	for _, tc := range []struct {
		kpp    bool
		setups []struct{ procs, budget int }
	}{
		{kpp: true, setups: []struct{ procs, budget int }{{8, 8}, {8, 1}, {8, 3}}},
		{kpp: false, setups: []struct{ procs, budget int }{{8, 8}, {1, 1}, {1, 8}, {3, 2}}},
	} {
		p := StoreParams{Dim: 32, NList: 32, Assign: 2, NProbe: 8, Normalize: true, KMeansPP: tc.kpp, Seed: 1}
		var first *btree.DB
		for _, w := range tc.setups {
			runtime.GOMAXPROCS(w.procs)
			buildpool.Set(w.budget)
			db := openMem(t)
			buildWithIDs(t, db, p, ids, vecs)
			if first == nil {
				first = db
				continue
			}
			requireSameIndex(t, first, db)
		}
	}
}

// TestBulkBuildsShareBudgetOfOne: at a budget of one a build's phases run on
// its own goroutine, but under the budget's one token: a build waits while
// another holder has it, and concurrent builds take turns, each coming out as
// it would alone.
func TestBulkBuildsShareBudgetOfOne(t *testing.T) {
	defer buildpool.Set(buildpool.Current().Size())
	buildpool.Set(1)
	p := StoreParams{Dim: 16, NList: 16, Assign: 2, Normalize: true, KMeansPP: true, Seed: 1}
	vecs := clusteredVecs(500, 16, 8, 2)
	want := openMem(t)
	buildWithIDs(t, want, p, seqIDs(len(vecs)), vecs)

	build := func(db *btree.DB) error {
		wtx, err := db.BeginWrite()
		if err != nil {
			return err
		}
		if _, err := BulkBuild(wtx, "ivf", p, seqIDs(len(vecs)), vecs); err != nil {
			_ = wtx.Rollback()
			return err
		}
		return wtx.Commit()
	}
	wait := func(done <-chan error) {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(30 * time.Second):
			t.Fatal("build did not complete")
		}
	}

	b := buildpool.Current()
	b.Acquire()
	held := openMem(t)
	done := make(chan error, 1)
	go func() { done <- build(held) }()
	select {
	case err := <-done:
		t.Fatalf("build ran while the only token was held (err %v)", err)
	case <-time.After(100 * time.Millisecond):
	}
	b.Release()
	wait(done)
	requireSameIndex(t, want, held)

	dbs := []*btree.DB{openMem(t), openMem(t), openMem(t)}
	results := make(chan error, len(dbs))
	for _, db := range dbs {
		go func() { results <- build(db) }()
	}
	for range dbs {
		wait(results)
	}
	for _, db := range dbs {
		requireSameIndex(t, want, db)
	}
}

// mainKmeansppInit is k-means++ seeding as earlier releases run it, without
// the build budget: a goroutine per GOMAXPROCS chunk, the chunk d² sums added in
// chunk order.
func mainKmeansppInit(data [][]float32, k int, rng *rand.Rand) [][]float32 {
	n := len(data)
	cents := [][]float32{slices.Clone(data[rng.Intn(n)])}
	d2 := make([]float64, n)
	for i := range d2 {
		d2[i] = math.Inf(1)
	}
	updateD2 := func(c []float32) (sum float64) {
		nThreads := runtime.GOMAXPROCS(0)
		chunk := (n + nThreads - 1) / nThreads
		part := make([]float64, nThreads)
		for t := 0; t < nThreads; t++ {
			for i := t * chunk; i < min((t+1)*chunk, n); i++ {
				dist := float64(simd.Distance(data[i], c))
				if dd := dist * dist; dd < d2[i] {
					d2[i] = dd
				}
				part[t] += d2[i]
			}
		}
		for _, s := range part {
			sum += s
		}
		return sum
	}
	sum := updateD2(cents[0])
	for len(cents) < k {
		target := rng.Float64() * sum
		pick := n - 1
		var acc float64
		for i := 0; i < n; i++ {
			acc += d2[i]
			if acc >= target {
				pick = i
				break
			}
		}
		cents = append(cents, slices.Clone(data[pick]))
		sum = updateD2(cents[len(cents)-1])
	}
	return cents
}

// TestKMeansPPIndependentOfBudget: k-means++ seeding on the build budget picks
// the centroids earlier releases pick at the same GOMAXPROCS, at any budget —
// its d² sum is reduced over the same GOMAXPROCS chunks in the same order.
func TestKMeansPPIndependentOfBudget(t *testing.T) {
	data := clusteredVecs(3000, 24, 40, 17)
	defer buildpool.Set(buildpool.Current().Size())
	want := mainKmeansppInit(data, 64, rand.New(rand.NewSource(5)))
	for _, budget := range []int{1, 3, 8} {
		buildpool.Set(budget)
		require.Equal(t, want, kmeansppInit(data, 64, rand.New(rand.NewSource(5))), "budget %d", budget)
	}
}

// TestBulkBuildTiesPlaceAsInsert: on exact distance ties between centroids the
// build places a vector in the cells an insert of it gets — nearestCells,
// which ranks a tie as earlier releases do.
func TestBulkBuildTiesPlaceAsInsert(t *testing.T) {
	vecs := repeatedVecs()
	for _, assign := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("assign=%d", assign), func(t *testing.T) {
			db := openMem(t)
			p := StoreParams{Dim: 16, NList: 16, Assign: assign, NProbe: 4, Seed: 1}
			buildStore(t, db, p, vecs)
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer rtx.Rollback()
			ix, err := OpenTx(rtx, "ivf")
			require.NoError(t, err)

			ties := 0
			for i, x := range vecs {
				dc, err := rtx.Get(ix.vdoc, bid(i))
				require.NoError(t, err)
				label, cells := decodeDocCells(dc)
				require.Equal(t, uint32(i), label)
				want := ix.nearestCells(x, assign)
				require.Equal(t, mainNearestCells(ix, x, assign), want)
				require.Len(t, cells, len(want))
				for j := range want {
					require.Equal(t, uint32(want[j]), cells[j], "vector %d cell #%d", i, j)
				}
				d0 := simd.Distance(x, ix.coarseRow(want[0]))
				for c := 0; c < ix.nlist; c++ {
					if c != want[0] && simd.Distance(x, ix.coarseRow(c)) == d0 {
						ties++
						break
					}
				}
			}
			require.Positive(t, ties, "the fixture must exercise exact ties")
		})
	}
}

// probeLabels is how earlier releases search: the labels in the nprobe cells
// mainNearestCells ranks first.
func probeLabels(t *testing.T, db *btree.DB, q []float32, nprobe int) map[uint32]bool {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ix, err := OpenTx(rtx, "ivf")
	require.NoError(t, err)
	if ix.normalize {
		q = normalize(q)
	}
	cur := rtx.NewCursor(ix.vcell)
	defer cur.Close()
	out := make(map[uint32]bool)
	for _, c := range mainNearestCells(ix, q, nprobe) {
		var seek [8]byte
		binary.BigEndian.PutUint32(seek[:], uint32(c))
		if err := cur.Seek(seek[:]); err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				continue
			}
			t.Fatal(err)
		}
		for cur.Valid() {
			key, err := cur.Key()
			require.NoError(t, err)
			if !bytes.Equal(key[:4], seek[:4]) {
				break
			}
			out[cellKeyLabel(key)] = true
			require.NoError(t, cur.Next())
		}
	}
	return out
}

// searchIDs returns the doc ids SearchCandidates returns for q (ef = all).
func searchIDs(t *testing.T, db *btree.DB, q []float32, ef int) map[string]bool {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ix, err := OpenTx(rtx, "ivf")
	require.NoError(t, err)
	cands, err := ix.SearchCandidates(rtx, q, ef)
	require.NoError(t, err)
	out := make(map[string]bool)
	for _, c := range cands {
		out[string(c.DocID)] = true
	}
	return out
}

// requireFindsAll asserts both searches find every doc whose vector equals q,
// with nprobe 1: this code's SearchCandidates, and the earlier releases' probe.
func requireFindsAll(t *testing.T, db *btree.DB, vecs [][]float32, q []float32) {
	t.Helper()
	got := searchIDs(t, db, q, len(vecs))
	old := probeLabels(t, db, q, 1)
	for i, v := range vecs {
		if slices.Equal(v, q) {
			require.True(t, got[string(bid(i))], "this search misses doc %d", i)
			require.True(t, old[uint32(i)], "the earlier releases' search misses doc %d", i)
		}
	}
}

// TestTiedCentroidsAcrossReleases builds indexes whose vectors sit at exact
// ties between duplicated centroids, written by this code and by the earlier
// releases' placement, and searches each with this code's search and the
// earlier releases' probe at nprobe 1. All of them rank a tie with the same
// sort, so each search finds every tied vector in either build; a build that
// ranked the tie another way (lowest index first, say) would put vectors in
// cells neither search reaches.
func TestTiedCentroidsAcrossReleases(t *testing.T) {
	t.Run("alternating", func(t *testing.T) {
		// 26 docs alternating [0]/[2], NList 13: k-means duplicates the centroids.
		vecs := alternatingVecs(26)
		p := StoreParams{Dim: 1, NList: 13, Assign: 1, NProbe: 1, KMeansPP: true, Seed: 0}
		cents := kmeans(vecs, p.NList, 15, p.Seed, p.KMeansPP)
		probe := &StoreIndex{dim: 1, nlist: len(cents), coarse: bytesAsF32(encodeCentroids(cents), len(cents))}
		require.NotEqual(t, lowestFirstCells(probe, []float32{0}, 1), mainNearestCells(probe, []float32{0}, 1),
			"the fixture must rank the [0] tie differently from lowest-first")

		older, newer := openMem(t), openMem(t)
		mainBuild(t, older, p, seqIDs(26), vecs)
		buildWithIDs(t, newer, p, seqIDs(26), vecs)
		for _, db := range []*btree.DB{older, newer} {
			requireFindsAll(t, db, vecs, []float32{0})
			requireFindsAll(t, db, vecs, []float32{2})
		}
	})
	t.Run("fixed centroids", func(t *testing.T) {
		// Cells 1 and 2 tie for [0]; eleven cells tie for [10].
		cents := [][]float32{{10}, {0}, {0}, {10}, {10}, {10}, {10}, {10}, {10}, {10}, {10}, {10}, {10}}
		vecs := make([][]float32, 20)
		for i := range vecs {
			vecs[i] = []float32{float32(10 * (i % 2))}
		}
		p := StoreParams{Dim: 1, Assign: 1, NProbe: 1}
		older, newer := openMem(t), openMem(t)
		serialWrite(t, older, p, cents, seqIDs(20), vecs, mainNearestCells)
		placeWrite(t, newer, p, cents, seqIDs(20), vecs)
		requireSameIndex(t, older, newer)
		for _, db := range []*btree.DB{older, newer} {
			requireFindsAll(t, db, vecs, []float32{0})
			requireFindsAll(t, db, vecs, []float32{10})
		}
	})
	t.Run("NaN", func(t *testing.T) {
		// A NaN distance orders against nothing: the cell is whatever the
		// earlier releases' sort leaves first — for [0] here the centroid at
		// 5, not the one at 0 — so the build asks that sort too.
		nan := float32(math.NaN())
		cents := [][]float32{{5}, {nan}, {0}, {10}}
		vecs := [][]float32{{0}, {10}, {nan}, {5}}
		p := StoreParams{Dim: 1, Assign: 1, NProbe: 1}
		older, newer := openMem(t), openMem(t)
		serialWrite(t, older, p, cents, seqIDs(len(vecs)), vecs, mainNearestCells)
		placeWrite(t, newer, p, cents, seqIDs(len(vecs)), vecs)
		requireSameIndex(t, older, newer)
	})
}

// TestTieOrderGolden pins, as hard-coded cell ids, the order in which
// nearestCellsInto ranks cells at an exact distance tie, and so the cells the
// build places tied vectors in. The other tie tests compare against a copy of
// the same sort, which would move along with it. If this fails after a Go
// upgrade, the toolchain changed how slices.SortFunc orders equal elements:
// routing no longer matches indexes written by earlier releases, which put
// tied vectors in the cells listed here, so their searches miss them and
// theirs miss new ones. Do not update the expected ids; vendor the earlier
// sort for nearestCellsInto instead. The alternating fixture also pins its
// k-means centroids, which a change to training would move as well.
func TestTieOrderGolden(t *testing.T) {
	order := func(ix *StoreIndex, q float32) []int {
		var scratch []cellDist
		return ix.nearestCellsInto([]float32{q}, ix.nlist, &scratch, nil)
	}
	// requirePlaced checks the build's :doc and :cell entries put vector i in
	// cell cellOf(i), and nowhere else.
	requirePlaced := func(t *testing.T, db *btree.DB, n int, cellOf func(i int) uint32) {
		t.Helper()
		got, _ := dumpIndex(t, db, "ivf")
		require.Len(t, got[nsDoc], n)
		for _, e := range got[nsDoc] {
			label, cells := decodeDocCells(e.v)
			require.Equal(t, []uint32{cellOf(int(label))}, cells, "label %d", label)
		}
		var want [][]byte
		for i := 0; i < n; i++ {
			want = append(want, cellKey(nil, cellOf(i), uint32(i)))
		}
		slices.SortFunc(want, bytes.Compare)
		require.Len(t, got[nsCell], n)
		for i, e := range got[nsCell] {
			require.Equal(t, want[i], e.k, ":cell key #%d", i)
		}
	}

	t.Run("alternating", func(t *testing.T) {
		db := openMem(t)
		buildWithIDs(t, db, StoreParams{Dim: 1, NList: 13, Assign: 1, NProbe: 1, KMeansPP: true, Seed: 0}, seqIDs(26), alternatingVecs(26))
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer rtx.Rollback()
		ix, err := OpenTx(rtx, "ivf")
		require.NoError(t, err)
		require.Equal(t, []float32{0, 2, 2, 2, 0, 0, 2, 0, 0, 0, 0, 0, 2}, ix.coarse, "k-means centroids")
		require.Equal(t, []int{8, 11, 10, 0, 4, 5, 9, 7, 3, 6, 2, 1, 12}, order(ix, 0))
		require.Equal(t, []int{6, 1, 2, 3, 12, 4, 5, 0, 7, 8, 9, 10, 11}, order(ix, 2))
		requirePlaced(t, db, 26, func(i int) uint32 {
			if i%2 == 0 {
				return 8 // [0]
			}
			return 6 // [2]
		})
	})
	t.Run("fixed centroids", func(t *testing.T) {
		cents := [][]float32{{10}, {0}, {0}, {10}, {10}, {10}, {10}, {10}, {10}, {10}, {10}, {10}, {10}}
		vecs := make([][]float32, 20)
		for i := range vecs {
			vecs[i] = []float32{float32(10 * (i % 2))}
		}
		db := openMem(t)
		placeWrite(t, db, StoreParams{Dim: 1, Assign: 1, NProbe: 1}, cents, seqIDs(20), vecs)
		ix := &StoreIndex{dim: 1, nlist: len(cents), coarse: bytesAsF32(encodeCentroids(cents), len(cents))}
		require.Equal(t, []int{2, 1, 6, 3, 4, 5, 0, 7, 8, 9, 10, 11, 12}, order(ix, 0))
		require.Equal(t, []int{6, 3, 4, 5, 0, 7, 8, 9, 10, 11, 12, 1, 2}, order(ix, 10))
		requirePlaced(t, db, 20, func(i int) uint32 {
			if i%2 == 0 {
				return 2 // [0]
			}
			return 6 // [10]
		})
	})
}

// TestPlaceRejectsWrongLengthRecord: a vector that encodes to a record of the
// wrong length fails the placement instead of writing a bad :cell record.
// BulkBuild rejects such a vector up front; this reaches place directly.
func TestPlaceRejectsWrongLengthRecord(t *testing.T) {
	cents := [][]float32{{0, 0, 0, 0}, {5, 5, 5, 5}}
	vecs := [][]float32{{1, 0, 0, 0}, {5, 4, 5, 5}, {1, 1, 1}, {0, 1, 0, 0}} // vector 2 has 3 components
	wtx, err := openMem(t).BeginWrite()
	require.NoError(t, err)
	defer wtx.Rollback()
	ix := openIndexNS(t, wtx, StoreParams{Dim: 4, Assign: 1, NProbe: 1}, cents)
	_, err = ix.place(wtx, seqIDs(len(vecs)), vecs, []int32{0, 1, 0, 0})
	require.ErrorContains(t, err, "vector 2 encodes to a 11-byte record, want 12")
	sz, err := wtx.NamespaceSize(ix.vcell)
	require.NoError(t, err)
	require.Zero(t, sz.Entries, "no :cell record written")
}

// TestSearchProbesTieGroup: a vector placed under another tie order — by a
// sort that ranks equal cells lowest index first, say — is still found at
// nprobe 1, as the search also probes the cells exactly as near as its
// farthest probed one.
func TestSearchProbesTieGroup(t *testing.T) {
	cents := [][]float32{{10}, {0}, {0}, {10}, {10}, {10}, {10}, {10}, {10}, {10}, {10}, {10}, {10}}
	vecs := make([][]float32, 20)
	for i := range vecs {
		vecs[i] = []float32{float32(10 * (i % 2))}
	}
	db := openMem(t)
	serialWrite(t, db, StoreParams{Dim: 1, Assign: 1, NProbe: 1}, cents, seqIDs(20), vecs, lowestFirstCells)
	for _, q := range [][]float32{{0}, {10}} {
		got := searchIDs(t, db, q, len(vecs))
		for i, v := range vecs {
			if slices.Equal(v, q) {
				require.True(t, got[string(bid(i))], "query %v misses doc %d", q, i)
			}
		}
	}
}

// TestProbeCellsTieGroupBounded: the cells added for a tie stop at
// maxTieProbes, the lowest-index ones when more tie, however many centroids
// coincide; a query with no exact tie at the boundary probes nprobe cells.
func TestProbeCellsTieGroupBounded(t *testing.T) {
	const nlist = 300
	cents := make([]float32, nlist) // every centroid at 0 but the last
	cents[nlist-1] = 5
	ix := &StoreIndex{dim: 1, nlist: nlist, coarse: cents}
	var scratch []cellDist
	got := ix.probeCellsInto([]float32{0}, 2, &scratch, nil)
	require.Len(t, got, 2+maxTieProbes)
	first := map[int]bool{got[0]: true, got[1]: true}
	var want []int
	for c := 0; len(want) < maxTieProbes; c++ {
		if !first[c] {
			want = append(want, c)
		}
	}
	require.Equal(t, want, got[2:], "the extra cells are the lowest-index rest of the tie")

	got = ix.probeCellsInto([]float32{5}, 1, &scratch, nil)
	require.Equal(t, []int{nlist - 1}, got, "no tie at the boundary: nprobe cells")
	got = ix.probeCellsInto([]float32{4}, 2, &scratch, nil)
	require.Len(t, got, 2+maxTieProbes, "a tie behind a unique nearest cell")
	got = ix.probeCellsInto([]float32{float32(math.NaN())}, 3, &scratch, nil)
	require.Len(t, got, 3, "NaN distances are no tie")
}
