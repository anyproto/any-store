// Command vectorbench is a self-contained benchmark for the experimental
// any-store vector (HNSW) search package. It builds to a single static,
// CGO-free binary you can copy to slow/low-power hardware and run without the
// repo, a network, or the test harness.
//
//	go build -o vectorbench ./cmd/vectorbench      # build for the host
//	GOOS=linux GOARCH=amd64 go build -o vectorbench ./cmd/vectorbench   # cross
//	./vectorbench -n 20000 -dim 768                # run a small sweep
//	./vectorbench -h                               # all flags
//
// It reports CPU/SIMD info, distance-kernel throughput (scalar vs SIMD), HNSW
// build rate, recall vs exact brute force, and query latency / RAM for the
// in-memory arena, the btree-paged index, and the route-in-RAM + paged-rerank
// hybrid — the same numbers as the OPTION_B / mddata measurements, on whatever
// box you run it on.
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/vector"
)

func main() {
	var (
		n       = flag.Int("n", 20000, "number of documents/vectors")
		dim     = flag.Int("dim", 768, "embedding dimension")
		ef      = flag.Int("ef", 64, "efSearch (HNSW candidate-list size)")
		k       = flag.Int("k", 10, "k nearest neighbours per query")
		queries = flag.Int("queries", 200, "queries used for recall + latency")
		seed    = flag.Int64("seed", 2024, "dataset RNG seed")
		metric  = flag.String("metric", "cosine", "distance metric: cosine|l2|dot")
		paged   = flag.Bool("paged", true, "also measure the btree-paged and hybrid indexes")
		recall  = flag.Bool("recall", true, "measure recall vs exact brute force (O(n) per query)")
		dbPath  = flag.String("db", "", "on-disk btree path: first run builds+persists, re-run reopens cold from disk")
		cache   = flag.Int("cache", 0, "btree page-cache size in pages (0=default 5000); use a small value to force disk I/O")
	)
	flag.Parse()

	m := parseMetric(*metric)
	hr := "------------------------------------------------------------"

	fmt.Println(hr)
	fmt.Println(" any-store vectorbench")
	fmt.Println(hr)
	fmt.Printf(" go=%s  os/arch=%s/%s  cpus=%d\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU())
	fmt.Printf(" SIMD distance acceleration (asm): %v\n", vector.SIMD())
	fmt.Printf(" config: n=%d dim=%d metric=%s ef=%d k=%d queries=%d\n", *n, *dim, m, *ef, *k, *queries)
	fmt.Println(hr)

	// 1. distance kernels --------------------------------------------------
	distanceBench(*dim)

	// disk mode: build-and-persist on first run, reopen-cold on re-run.
	if *dbPath != "" {
		runDisk(*dbPath, *cache, *n, *dim, *ef, *k, *queries, *seed, m, *recall)
		return
	}

	// 2. dataset -----------------------------------------------------------
	t0 := time.Now()
	fmt.Printf("\n[1/4] generating %d markdown docs + embeddings (dim %d) ...\n", *n, *dim)
	vecs, ids, _, avgBytes := vector.GenMDDataset(*n, *dim, *seed)
	qVecs, _, _, _ := vector.GenMDDataset(*queries, *dim, *seed+1)
	rawMiB := float64(*n**dim*4) / (1 << 20)
	fmt.Printf("      done in %s — avg %.0f B/doc, ~%.0f MiB corpus, %.1f MiB raw vectors\n",
		time.Since(t0).Round(time.Millisecond), avgBytes, float64(*n)*avgBytes/(1<<20), rawMiB)

	// 3. build -------------------------------------------------------------
	fmt.Printf("\n[2/4] building in-memory HNSW ...\n")
	tb := time.Now()
	g := vector.NewFlatHNSW(*dim, m, 1)
	g.EfSearch = *ef
	for i := range vecs {
		g.Add(ids[i], vecs[i])
		if *n >= 10 && i%(*n/10) == 0 && i > 0 {
			fmt.Printf("      %3d%%  (%s)\n", 100*i/(*n), time.Since(tb).Round(time.Millisecond))
		}
	}
	buildDur := time.Since(tb)
	fmt.Printf("      built %d in %s  (%.0f docs/s)\n", *n, buildDur.Round(time.Millisecond), float64(*n)/buildDur.Seconds())

	// 4. recall ------------------------------------------------------------
	if *recall {
		fmt.Printf("\n[3/4] recall vs exact brute force (%d queries) ...\n", *queries)
		brute := vector.NewBrute(*dim, m)
		for i := range vecs {
			brute.Add(ids[i], vecs[i])
		}
		truth := make([][]vector.SearchResult, *queries)
		for i := 0; i < *queries; i++ {
			truth[i] = brute.Search(qVecs[i], *k)
		}
		for _, e := range []int{*ef, *ef * 2, *ef * 4} {
			g.EfSearch = e
			var r float64
			for i := 0; i < *queries; i++ {
				r += recallAt(g.Search(qVecs[i], *k), truth[i])
			}
			fmt.Printf("      recall@%d ef=%-4d: %.3f\n", *k, e, r/float64(*queries))
		}
		g.EfSearch = *ef
	} else {
		fmt.Printf("\n[3/4] recall skipped (-recall=false)\n")
	}

	// 5. search latency ----------------------------------------------------
	fmt.Printf("\n[4/4] query latency ...\n")
	memNs := timeSearch(func(q []float32) { g.Search(q, *k) }, qVecs)
	fmt.Printf("  A  in-memory arena : %9.1f us/q   RAM=%7.1f MiB (full)             0 reads/q\n",
		memNs/1000, mib(g.MemBytes()))

	if *paged {
		db, err := btree.Open(":memory:", btree.Options{InMemory: true})
		must(err)
		defer db.Close()
		p, err := vector.BuildPagedFromFlat(g, db, "emb", m)
		must(err)

		gets0, calls := p.Gets(), 0
		pagNs := timeSearch(func(q []float32) { p.Search(q, *k); calls++ }, qVecs)
		pagGets := float64(p.Gets()-gets0) / float64(calls)
		fmt.Printf("  B  paged vectors   : %9.1f us/q   RAM=%7.1f MiB (topology only) %5.0f reads/q  (%.1fx)\n",
			pagNs/1000, mib(p.TopologyBytes()), pagGets, pagNs/memNs)

		gets0, calls = p.Gets(), 0
		hybNs := timeSearch(func(q []float32) { p.SearchHybrid(q, *k); calls++ }, qVecs)
		hybGets := float64(p.Gets()-gets0) / float64(calls)
		fmt.Printf("  B' hybrid route+rerank: %6.1f us/q   RAM=routing-slab + topology %5.0f reads/q  (%.1fx)\n",
			hybNs/1000, hybGets, hybNs/memNs)
		fmt.Printf("     (a quantized routing slab makes B' RAM ~25%% [int8] or ~3%% [binary] of A)\n")
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Println(hr)
	fmt.Printf(" peak heap: %.0f MiB   total time: %s\n", mib(int(ms.HeapAlloc)), time.Since(t0).Round(time.Millisecond))
	fmt.Println(hr)
}

// runDisk uses a FILE-backed btree. The first invocation builds the index and
// persists it (split :topo/:vec); a re-run against the same path reopens it cold
// (loading only topology into RAM, paging vectors from disk) — so running the
// command twice measures build+warm vs reload+cold.
func runDisk(path string, cache, n, dim, ef, k, queries int, seed int64, m vector.Metric, doRecall bool) {
	hr := "------------------------------------------------------------"
	opts := btree.Options{}
	if cache > 0 {
		opts.CacheSize = cache
	}
	db, err := btree.Open(path, opts)
	must(err)
	defer db.Close()

	qVecs, _, _, _ := vector.GenMDDataset(queries, dim, seed+1)

	if vector.PagedExists(db, "emb") {
		// ---- RELOAD RUN (cold) ----
		fmt.Printf("\n=== RELOAD run: reopening on-disk index at %s (cold cache=%d pages) ===\n", path, cache)
		t := time.Now()
		p, err := vector.OpenPaged(db, "emb")
		must(err)
		fmt.Printf(" reloaded %d nodes (topology only) in %s — topology RAM=%.1f MiB (vectors stay on disk)\n",
			p.PhysLen(), time.Since(t).Round(time.Millisecond), mib(p.TopologyBytes()))
		g0, calls := p.Gets(), 0
		pagNs := timeSearch(func(q []float32) { p.Search(q, k); calls++ }, qVecs)
		fmt.Printf(" B  paged vectors (cold disk): %8.1f us/q   %5.0f reads/q\n",
			pagNs/1000, float64(p.Gets()-g0)/float64(calls))
		fmt.Println(hr)
		fmt.Println(" (cold = btree page cache started empty this process; re-run again for a warmer cache)")
		fmt.Println(hr)
		return
	}

	// ---- BUILD RUN ----
	fmt.Printf("\n=== BUILD run: building + persisting to %s ===\n", path)
	t0 := time.Now()
	vecs, ids, _, avgBytes := vector.GenMDDataset(n, dim, seed)
	fmt.Printf(" generated %d docs (avg %.0f B) in %s\n", n, avgBytes, time.Since(t0).Round(time.Millisecond))

	tb := time.Now()
	g := vector.NewFlatHNSW(dim, m, 1)
	g.EfSearch = ef
	for i := range vecs {
		g.Add(ids[i], vecs[i])
	}
	fmt.Printf(" built HNSW in %s (%.0f docs/s)\n", time.Since(tb).Round(time.Millisecond), float64(n)/time.Since(tb).Seconds())

	if doRecall {
		brute := vector.NewBrute(dim, m)
		for i := range vecs {
			brute.Add(ids[i], vecs[i])
		}
		var r float64
		for i := 0; i < queries; i++ {
			r += recallAt(g.Search(qVecs[i], k), brute.Search(qVecs[i], k))
		}
		fmt.Printf(" recall@%d ef=%d: %.3f\n", k, ef, r/float64(queries))
	}

	tp := time.Now()
	must(vector.PersistPaged(g, db, "emb", m))
	must(db.Checkpoint(btree.CheckpointFull))
	fmt.Printf(" persisted (split :topo/:vec) in %s; on-disk: %s\n", time.Since(tp).Round(time.Millisecond), fileSizes(path))

	memNs := timeSearch(func(q []float32) { g.Search(q, k) }, qVecs)
	fmt.Printf(" A  in-memory arena      : %8.1f us/q   RAM=%.1f MiB (full)\n", memNs/1000, mib(g.MemBytes()))

	p, err := vector.AttachPaged(g, db, "emb", m)
	must(err)
	g0, calls := p.Gets(), 0
	pagNs := timeSearch(func(q []float32) { p.Search(q, k); calls++ }, qVecs)
	pagGets := float64(p.Gets()-g0) / float64(calls)
	fmt.Printf(" B  paged vectors (warm) : %8.1f us/q   %5.0f reads/q  (%.1fx)\n", pagNs/1000, pagGets, pagNs/memNs)
	g0, calls = p.Gets(), 0
	hybNs := timeSearch(func(q []float32) { p.SearchHybrid(q, k); calls++ }, qVecs)
	fmt.Printf(" B' hybrid route+rerank  : %8.1f us/q   %5.0f reads/q  (%.1fx)\n", hybNs/1000, float64(p.Gets()-g0)/float64(calls), hybNs/memNs)

	fmt.Println(hr)
	fmt.Printf(" run the SAME command again to measure cold reload-from-disk (drop OS cache first for a true cold read)\n")
	fmt.Println(hr)
}

func fileSizes(path string) string {
	var total int64
	parts := ""
	for _, suf := range []string{"", "-wal"} {
		if fi, err := os.Stat(path + suf); err == nil {
			total += fi.Size()
			if suf != "" {
				parts += fmt.Sprintf(" +%s %.1fMiB", suf, float64(fi.Size())/(1<<20))
			}
		}
	}
	return fmt.Sprintf("%.1f MiB total%s", float64(total)/(1<<20), parts)
}

func distanceBench(dim int) {
	a := make([]float32, dim)
	b := make([]float32, dim)
	for i := range a {
		a[i] = float32(i%7)*0.1 - 0.3
		b[i] = float32((i*3)%5)*0.2 - 0.4
	}
	bench := func(fn func(x, y []float32) float32) float64 {
		// warm
		for i := 0; i < 1000; i++ {
			fn(a, b)
		}
		const iters = 2_000_000
		s := time.Now()
		var sink float32
		for i := 0; i < iters; i++ {
			sink += fn(a, b)
		}
		_ = sink
		return float64(time.Since(s).Nanoseconds()) / iters
	}
	sc := bench(vector.L2DistanceScalar)
	un := bench(vector.L2DistanceUnrolled)
	sd := bench(vector.L2DistanceSIMD)
	fmt.Printf("\n distance kernel (L2, dim %d):  scalar %.1f ns | unrolled %.1f ns | SIMD %.1f ns  (%.1fx vs scalar)\n",
		dim, sc, un, sd, sc/sd)
}

func timeSearch(fn func(q []float32), queries [][]float32) float64 {
	for i := 0; i < 100 && i < len(queries); i++ {
		fn(queries[i%len(queries)])
	}
	iters := 2000
	s := time.Now()
	for i := 0; i < iters; i++ {
		fn(queries[i%len(queries)])
	}
	return float64(time.Since(s).Nanoseconds()) / float64(iters)
}

func recallAt(approx, truth []vector.SearchResult) float64 {
	if len(truth) == 0 {
		return 1
	}
	want := make(map[uint64]bool, len(truth))
	for _, r := range truth {
		want[r.Key] = true
	}
	hit := 0
	for _, r := range approx {
		if want[r.Key] {
			hit++
		}
	}
	return float64(hit) / float64(len(truth))
}

func parseMetric(s string) vector.Metric {
	switch s {
	case "l2", "euclidean":
		return vector.L2
	case "dot", "ip":
		return vector.Dot
	default:
		return vector.Cosine
	}
}

func mib(bytes int) float64 { return float64(bytes) / (1 << 20) }

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
