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
// in-memory arena, on whatever box you run it on. The btree-paged index, the
// route-in-RAM + paged-rerank hybrid and the disk-backed run are the vector
// package's own, unexported: its TestVectorbench runs them with the
// -vectorbench.paged, -vectorbench.db and -vectorbench.cache flags (see
// README.md).
package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

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
		recall  = flag.Bool("recall", true, "measure recall vs exact brute force (O(n) per query)")
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

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Println(hr)
	fmt.Printf(" peak heap: %.0f MiB   total time: %s\n", mib(int(ms.HeapAlloc)), time.Since(t0).Round(time.Millisecond))
	fmt.Println(hr)
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
