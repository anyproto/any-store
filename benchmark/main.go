package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
)

type Scenario struct {
	Group string
	Name  string
	Run   func() int
	Check func() error
}

func (s Scenario) BenchName() string {
	return fmt.Sprintf("Benchmark%s/%s", capitalize(s.Group), s.Name)
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	parts := strings.Split(s, "_")
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, "")
}

var (
	flagGroup    = flag.String("group", "", "filter by group name (substring match)")
	flagName     = flag.String("name", "", "filter by scenario name (substring match)")
	flagList     = flag.Bool("list", false, "list all scenarios and exit")
	flagDuration = flag.Duration("duration", 3*time.Second, "time per scenario")
	flagDocs     = flag.Int("docs", 500000, "golden collection size")
	flagCheck    = flag.Bool("check", false, "verify correctness only, no timing")
	flagSave     = flag.String("save", "", "save benchstat-compatible output to file")
)

func main() {
	flag.Parse()

	ctx := context.Background()

	db, err := anystore.Open(ctx, "benchmark.db", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		db.Close()
		os.Remove("benchmark.db")
		os.Remove("benchmark.db-shm")
		os.Remove("benchmark.db-wal")
	}()

	n := *flagDocs

	fmt.Printf("any-store benchmark (%d docs)\n", n)
	fmt.Println("=================================")
	fmt.Println()

	// Setup golden collection
	fmt.Println("--- Setup ---")
	golden := setupGolden(ctx, db, n)

	// Setup write collections for crud group
	writeColl := setupWriteCollection(ctx, db, "write_coll", 1000)
	batchWriteColl := setupWriteCollection(ctx, db, "batch_write_coll", 10000)

	// Setup no-index collection for fullscan group
	noIdxColl := setupWriteCollection(ctx, db, "noidx_coll", 10000)

	// Setup bulk collection
	bulkColl := setupWriteCollection(ctx, db, "bulk_coll", 10000)
	checkpoint(ctx, db)

	fmt.Println()

	// Register all scenarios
	scenarios := registerAll(ctx, golden, writeColl, batchWriteColl, noIdxColl, bulkColl, db, n)

	// Filter scenarios
	filtered := filterScenarios(scenarios)

	if *flagList {
		listScenarios(filtered)
		return
	}

	if *flagCheck {
		runChecks(filtered)
		return
	}

	// Detect if any write scenarios are selected
	writeGroups := map[string]bool{"crud": true, "bulk": true}
	hasWrites := false
	for _, s := range filtered {
		if writeGroups[s.Group] {
			hasWrites = true
			break
		}
	}

	// Run benchmarks
	var benchLines []string
	for _, s := range filtered {
		line := runScenario(s, *flagDuration)
		benchLines = append(benchLines, line)
	}

	// Print DB file sizes after tests if there were write ops
	if hasWrites {
		checkpoint(ctx, db)
		fmt.Println()
		fmt.Println("--- DB Size (after tests) ---")
		fmt.Println(dbSize())
	}

	// Save to file if requested
	if *flagSave != "" {
		if err := os.WriteFile(*flagSave, []byte(strings.Join(benchLines, "\n")+"\n"), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "failed to save results: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\nResults saved to %s\n", *flagSave)
	}
}

func setupGolden(ctx context.Context, db anystore.DB, n int) anystore.Collection {
	start := time.Now()
	coll, err := db.Collection(ctx, "golden")
	if err != nil {
		fatal("create golden collection: %v", err)
	}

	// Batch insert documents
	arena := &anyenc.Arena{}
	batchSize := 1000
	for i := 0; i < n; i += batchSize {
		end := i + batchSize
		if end > n {
			end = n
		}
		docs := make([]*anyenc.Value, 0, end-i)
		for j := i; j < end; j++ {
			doc := arena.NewObject()
			doc.Set("id", arena.NewNumberInt(j))
			doc.Set("a", arena.NewNumberInt(j%100))
			doc.Set("b", arena.NewNumberInt((j/100)%50))
			doc.Set("c", arena.NewNumberInt((j/5000)%10))
			doc.Set("val", arena.NewNumberInt(j*7%1000))
			doc.Set("email", arena.NewString(fmt.Sprintf("user%d@test.com", j)))
			docs = append(docs, doc)
		}
		if err := coll.Insert(ctx, docs...); err != nil {
			fatal("insert golden docs batch %d: %v", i, err)
		}
		arena.Reset()
	}
	checkpoint(ctx, db)
	fmt.Printf("%-29s%.3fs  %s\n", "Collection + data:", time.Since(start).Seconds(), dbSize())

	// Create indexes
	indexes := []struct {
		info  anystore.IndexInfo
		label string
	}{
		{anystore.IndexInfo{Fields: []string{"a"}}, "Index (a)"},
		{anystore.IndexInfo{Fields: []string{"b"}}, "Index (b)"},
		{anystore.IndexInfo{Fields: []string{"c"}}, "Index (c)"},
		{anystore.IndexInfo{Fields: []string{"a", "b"}}, "Index (a,b)"},
		{anystore.IndexInfo{Fields: []string{"a", "-b"}}, "Index (a,-b)"},
		{anystore.IndexInfo{Fields: []string{"email"}, Unique: true}, "Index (email) unique"},
	}

	for _, idx := range indexes {
		start := time.Now()
		if err := coll.EnsureIndex(ctx, idx.info); err != nil {
			fatal("create index %s: %v", idx.label, err)
		}
		checkpoint(ctx, db)
		fmt.Printf("%-29s%.3fs  %s\n", idx.label+":", time.Since(start).Seconds(), dbSize())
	}

	return coll
}

func setupWriteCollection(ctx context.Context, db anystore.DB, name string, n int) anystore.Collection {
	coll, err := db.Collection(ctx, name)
	if err != nil {
		fatal("create %s: %v", name, err)
	}

	arena := &anyenc.Arena{}
	batchSize := 1000
	for i := 0; i < n; i += batchSize {
		end := i + batchSize
		if end > n {
			end = n
		}
		docs := make([]*anyenc.Value, 0, end-i)
		for j := i; j < end; j++ {
			doc := arena.NewObject()
			doc.Set("id", arena.NewNumberInt(j))
			doc.Set("a", arena.NewNumberInt(j%100))
			doc.Set("b", arena.NewNumberInt((j/100)%50))
			doc.Set("c", arena.NewNumberInt((j/5000)%10))
			doc.Set("val", arena.NewNumberInt(j*7%1000))
			doc.Set("email", arena.NewString(fmt.Sprintf("user%d@test.com", j)))
			docs = append(docs, doc)
		}
		if err := coll.Insert(ctx, docs...); err != nil {
			fatal("insert %s docs batch %d: %v", name, i, err)
		}
		arena.Reset()
	}

	return coll
}

func filterScenarios(scenarios []Scenario) []Scenario {
	var result []Scenario
	for _, s := range scenarios {
		if *flagGroup != "" && !strings.Contains(s.Group, *flagGroup) {
			continue
		}
		if *flagName != "" && !strings.Contains(s.Name, *flagName) {
			continue
		}
		result = append(result, s)
	}
	return result
}

func listScenarios(scenarios []Scenario) {
	for _, s := range scenarios {
		fmt.Printf("  %s/%s\n", s.Group, s.Name)
	}
	fmt.Printf("\n%d scenarios\n", len(scenarios))
}

func runChecks(scenarios []Scenario) {
	passed, failed := 0, 0
	for _, s := range scenarios {
		if err := s.Check(); err != nil {
			fmt.Printf("[FAIL] %s/%s: %v\n", s.Group, s.Name, err)
			failed++
		} else {
			fmt.Printf("[OK]   %s/%s\n", s.Group, s.Name)
			passed++
		}
	}
	fmt.Printf("\n%d passed, %d failed\n", passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}

func runScenario(s Scenario, dur time.Duration) string {
	// Warmup: run a few iterations
	for i := 0; i < 3; i++ {
		s.Run()
	}

	runtime.GC()

	// Timed run with alloc tracking
	var memStart, memEnd runtime.MemStats
	runtime.ReadMemStats(&memStart)

	var iterations int
	start := time.Now()
	for time.Since(start) < dur {
		s.Run()
		iterations++
	}
	elapsed := time.Since(start)

	runtime.ReadMemStats(&memEnd)

	nsPerOp := elapsed.Nanoseconds() / int64(iterations)
	allocsPerOp := (memEnd.Mallocs - memStart.Mallocs) / uint64(iterations)
	bytesPerOp := (memEnd.TotalAlloc - memStart.TotalAlloc) / uint64(iterations)
	line := fmt.Sprintf("%-45s %8d %12d ns/op %8d B/op %6d allocs/op",
		s.BenchName(), iterations, nsPerOp, bytesPerOp, allocsPerOp)
	fmt.Println(line)
	return line
}

func checkpoint(ctx context.Context, db anystore.DB) {
	if err := db.Flush(ctx, 0, anystore.FlushModeCheckpointRestart); err != nil {
		fatal("checkpoint: %v", err)
	}
}

func dbSize() string {
	var dbBytes, walBytes int64
	if info, err := os.Stat("benchmark.db"); err == nil {
		dbBytes = info.Size()
	}
	if info, err := os.Stat("benchmark.db-wal"); err == nil {
		walBytes = info.Size()
	}
	if walBytes > 0 {
		return fmt.Sprintf("db=%s wal=%s", formatBytes(dbBytes), formatBytes(walBytes))
	}
	return fmt.Sprintf("db=%s", formatBytes(dbBytes))
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
