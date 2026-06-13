// Command debug-fetchiter instruments the data-namespace point-lookup path used
// by the query FetchIter, to understand the per-row fetch cost of the tree-build
// query (Find({t==X, o>=""}).Sort(o)) against a real store under a realistic
// shared page-buffer slab. DEBUG ONLY.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"sort"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/internal/btree"
)

func main() {
	var (
		dbPath = flag.String("db", "/tmp/new_store.db", "path to store.db")
		coll   = flag.String("coll", "changes", "collection name")
		tree   = flag.String("tree", "bafyreiebhjg6kh2q733ariewjn3sdw64dmrcvzj7uologxfcaiuep2kuf4", "tree id (t)")
		slabMB  = flag.Int("slabmb", 64, "global page-buffer slab size in MB (0 = no slab)")
		cache   = flag.Int("cache", 0, "per-DB pcache size in pages (0 = default 5000)")
		runs    = flag.Int("runs", 2, "number of query runs (run 1 = cold pcache, run 2+ = warm)")
		cpuprof = flag.String("cpuprofile", "", "write a CPU profile of a warm fetch loop to this file")
		profN   = flag.Int("profloops", 40, "how many times to re-run the warm fetch under the CPU profile")
	)
	flag.Parse()
	ctx := context.Background()

	pageSize := 4096
	if *slabMB > 0 {
		nPages := (*slabMB * 1024 * 1024) / pageSize
		anystore.InitPageBuffer(pageSize, nPages)
		fmt.Printf("slab: %d MB = %d pages (page=%d)\n", *slabMB, nPages, pageSize)
	}

	cfg := &anystore.Config{UseGlobalPageBuffer: *slabMB > 0, CacheSize: *cache}
	db, err := anystore.Open(ctx, *dbPath, cfg)
	must(err)

	c, err := db.OpenCollection(ctx, *coll)
	must(err)

	filter := fmt.Sprintf(`{"t":%q,"o":{"$gte":""}}`, *tree)
	fmt.Printf("query: db.%s.find(%s).sort(\"o\")\n", *coll, filter)
	fmt.Printf("config: UseGlobalPageBuffer=%v CacheSize=%d (0=default 5000)\n\n", cfg.UseGlobalPageBuffer, cfg.CacheSize)

	// Confirm the plan we actually take.
	if ex, err := c.Find(filter).Sort("o").Explain(ctx); err == nil {
		fmt.Println("PLAN:", ex.Sql)
		fmt.Println()
	}

	anystore.EnablePipelinePerfCounters(true)
	btree.EnableBtreeCounters(true)

	var ids [][]byte // docIds in natural (OrderKey) iteration order, collected on run 1

	for run := 1; run <= *runs; run++ {
		btree.ResetBtreeCounters()
		anystore.ResetPipelinePerfCounters()

		start := time.Now()
		rows := 0
		iter, err := c.Find(filter).Sort("o").Iter(ctx)
		must(err)
		for iter.Next() {
			d, derr := iter.Doc()
			must(derr)
			if run == 1 {
				id := d.Value().GetStringBytes("id")
				ids = append(ids, append([]byte(nil), id...))
			}
			_ = d.Value() // force materialize
			rows++
		}
		must(iter.Err())
		iter.Close()
		elapsed := time.Since(start)

		bc := btree.SnapshotBtreeCounters()
		pp := anystore.SnapshotPipelinePerfCounters().Planner

		label := "WARM"
		if run == 1 {
			label = "COLD (fresh pcache)"
		}
		fmt.Printf("===== run %d (%s) =====\n", run, label)
		fmt.Printf("  rows fetched:          %d\n", rows)
		fmt.Printf("  wall time:             %v  (%.1f us/row)\n", elapsed, float64(elapsed.Microseconds())/float64(max1(rows)))
		fmt.Printf("  -- planner --\n")
		fmt.Printf("  fetch lookups:         %d   (%.1f us/lookup)\n", pp.FetchYields, nsPer(pp.FetchLookupNs, pp.FetchYields))
		fmt.Printf("  fetch parse:           %.1f us/row total %.1f ms\n", nsPer(pp.FetchParseNs, pp.FetchYields), float64(pp.FetchParseNs)/1e6)
		fmt.Printf("  fetch lookup total:    %.1f ms\n", float64(pp.FetchLookupNs)/1e6)
		fmt.Printf("  -- btree page reads --\n")
		fmt.Printf("  getPageReader calls:   %d   (%.2f pages/row)\n", bc.GetPageReaderCalls, perRow(bc.GetPageReaderCalls, rows))
		fmt.Printf("  pcache HITS:           %d   (%.1f%%)\n", bc.PcacheHits, pct(bc.PcacheHits, bc.GetPageReaderCalls))
		fmt.Printf("  pcache MISSES:         %d   (%.1f%%)\n", bc.PcacheMisses, pct(bc.PcacheMisses, bc.GetPageReaderCalls))
		fmt.Printf("  disk reads (pread):    %d   (%.2f preads/row)\n", bc.DiskReads, perRow(bc.DiskReads, rows))
		fmt.Printf("  descendChild:          %d   (%.2f /row)\n", bc.DescendChild, perRow(bc.DescendChild, rows))
		fmt.Printf("  overflow reads:        %d\n", bc.OverflowReads)
		fmt.Printf("  -- working set --\n")
		fmt.Printf("  distinct pages touched: %d\n", bc.DistinctReaderPages)
		fmt.Printf("  distinct pages on disk: %d\n", bc.DistinctDiskPages)
		fmt.Println()
	}

	must(db.Close())

	// ---- Comparison: fetch the SAME docs by id, in OrderKey order vs sorted
	// data-key (docId) order, each on a FRESH pcache. This isolates the effect
	// of fetch ORDER on disk reads / re-reads, holding the doc set constant.
	fmt.Printf("========== FETCH-ORDER COMPARISON (%d docs, fresh pcache each) ==========\n", len(ids))

	natural := append([][]byte(nil), ids...) // OrderKey order (as the index yields)
	sorted := append([][]byte(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i], sorted[j]) < 0 })

	// Alternate the execution order so OS-page-cache warming from one phase
	// can't be mistaken for the access-pattern effect.
	fetchByIds(ctx, *dbPath, cfg, *coll, "sorted docId order (1st)", sorted)
	fetchByIds(ctx, *dbPath, cfg, *coll, "OrderKey order   (2nd)", natural)
	fetchByIds(ctx, *dbPath, cfg, *coll, "sorted docId order (3rd)", sorted)
	fetchByIds(ctx, *dbPath, cfg, *coll, "OrderKey order   (4th)", natural)

	// ---- CPU profile of the WARM real-query path (Find(filter).Sort(o).Iter,
	// ONE read tx for the whole scan — like production GetAfterOrder), so the
	// per-row CPU is btree search vs pread-syscall vs s2 decompress vs parse,
	// without the per-FindId read-tx (fcntl) artifact.
	if *cpuprof != "" {
		cpuProfileWarmQuery(ctx, *dbPath, cfg, *coll, filter, *cpuprof, *profN)
	}
}

func cpuProfileWarmQuery(ctx context.Context, path string, cfg *anystore.Config, collName, filter, out string, loops int) {
	db, err := anystore.Open(ctx, path, cfg)
	must(err)
	defer db.Close()
	c, err := db.OpenCollection(ctx, collName)
	must(err)
	scan := func() int {
		it, err := c.Find(filter).Sort("o").Iter(ctx)
		must(err)
		n := 0
		for it.Next() {
			d, _ := it.Doc()
			_ = d.Value()
			n++
		}
		it.Close()
		return n
	}
	scan() // warm
	f, err := os.Create(out)
	must(err)
	defer f.Close()
	must(pprof.StartCPUProfile(f))
	start := time.Now()
	rows := 0
	for i := 0; i < loops; i++ {
		rows = scan()
	}
	pprof.StopCPUProfile()
	fmt.Printf("\nCPU profile (real Iter query): %d loops x %d rows in %v -> %s\n", loops, rows, time.Since(start), out)
}

// sequentiality reports the fraction of consecutive disk reads that are
// adjacent pages (|Δpgno|<=1) and the mean absolute page gap. ~1.0 / small gap
// means physically sequential access (readahead-friendly).
func sequentiality(seq []uint32) (adjPct, meanDelta float64) {
	if len(seq) < 2 {
		return 0, 0
	}
	adj, sum := 0, uint64(0)
	for i := 1; i < len(seq); i++ {
		d := int64(seq[i]) - int64(seq[i-1])
		if d < 0 {
			d = -d
		}
		if d <= 1 {
			adj++
		}
		sum += uint64(d)
	}
	n := float64(len(seq) - 1)
	return 100 * float64(adj) / n, float64(sum) / n
}

// fetchByIds opens the db fresh (cold pcache), then point-looks-up each id via
// FindId in the given order, reporting the page-read counters.
func fetchByIds(ctx context.Context, path string, cfg *anystore.Config, collName, label string, ids [][]byte) {
	db, err := anystore.Open(ctx, path, cfg)
	must(err)
	defer db.Close()
	c, err := db.OpenCollection(ctx, collName)
	must(err)

	btree.ResetBtreeCounters()
	btree.SetRecordDiskSeq(true)
	start := time.Now()
	for _, id := range ids {
		d, err := c.FindId(ctx, string(id))
		must(err)
		_ = d.Value()
	}
	elapsed := time.Since(start)
	bc := btree.SnapshotBtreeCounters()
	seq := btree.SnapshotDiskSeq()
	btree.SetRecordDiskSeq(false)
	usPerPread := 0.0
	if bc.DiskReads > 0 {
		usPerPread = float64(elapsed.Microseconds()) / float64(bc.DiskReads)
	}
	adj, meanDelta := sequentiality(seq)
	fmt.Printf("--- %s ---\n", label)
	fmt.Printf("  wall: %-12v  preads: %d (%.2f/row)  **%.1f us/pread**  pcache-miss: %.1f%%\n",
		elapsed, bc.DiskReads, perRow(bc.DiskReads, len(ids)), usPerPread,
		pct(bc.PcacheMisses, bc.GetPageReaderCalls))
	fmt.Printf("    disk-read pgno sequentiality: adjacent(|Δ|<=1)=%.1f%%  mean|Δpgno|=%.0f pages  (1.0=perfectly sequential)\n",
		adj, meanDelta)
}

func nsPer(total, n uint64) float64 {
	if n == 0 {
		return 0
	}
	return float64(total) / float64(n) / 1000.0
}
func perRow(v uint64, rows int) float64 {
	if rows == 0 {
		return 0
	}
	return float64(v) / float64(rows)
}
func pct(v, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(v) / float64(total)
}
func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}
