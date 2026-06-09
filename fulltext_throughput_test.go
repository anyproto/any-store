package anystore

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// TestFtsDatasetThroughput reports operations/second AND memory use for the FTS
// write and read paths on the real air-crash corpus. Guarded like
// TestFtsDataset: set FTS_DATASET (defaults to the sibling checkout) to run.
//
//	go test . -run TestFtsDatasetThroughput -v
//
// On-disk (default durability), single fts index over "text". Memory columns:
//   - alloc/op: bytes allocated per op (GC churn; runtime.TotalAlloc delta)
//   - heap held: live heap after the phase (runtime.GC then HeapAlloc)
func TestFtsDatasetThroughput(t *testing.T) {
	dir := os.Getenv("FTS_DATASET")
	if dir == "" {
		dir = "../any-store-vector/aircrashdataset"
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.md"))
	if err != nil || len(files) == 0 {
		t.Skipf("dataset not found at %s (set FTS_DATASET); skipping", dir)
	}

	type rec struct{ id, year, text string }
	var recs []rec
	var totalBytes int
	for _, f := range files {
		raw, rerr := os.ReadFile(f)
		if rerr != nil {
			continue
		}
		id, body := parseFrontmatter(string(raw))
		year := strings.TrimSuffix(filepath.Base(f), ".md")
		if id == "" {
			id = year
		}
		recs = append(recs, rec{id, year, body})
		totalBytes += len(body)
	}
	n := len(recs)
	t.Logf("corpus: %d docs, %d KB of text (%.2f KB/doc avg)", n, totalBytes/1024, float64(totalBytes)/1024/float64(n))
	t.Logf("%-22s %8s %12s %12s %11s", "phase", "ops", "ops/sec", "alloc/op", "heap held")

	mkDoc := func(r rec, suffix string) *anyenc.Value {
		body := r.text
		if suffix != "" {
			body += " " + suffix
		}
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%q,"year":%q,"text":%q}`, r.id, r.year, body))
	}
	newColl := func() (*fixture, Collection) {
		fx := newFixture(t)
		coll, e := fx.CreateCollection(ctx, "pages")
		require.NoError(t, e)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"text"}}))
		return fx, coll
	}
	heapAlloc := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	// measure times fn, attributing allocation churn and reporting held heap.
	measure := func(label string, count int, fn func()) {
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		t0 := time.Now()
		fn()
		d := time.Since(t0)
		runtime.ReadMemStats(&m1)
		allocPerOp := (m1.TotalAlloc - m0.TotalAlloc) / uint64(count)
		t.Logf("%-22s %8d %12.0f %9d B %8.1f MB",
			label, count, float64(count)/d.Seconds(), allocPerOp, float64(heapAlloc())/(1<<20))
	}

	// ---- batch insert: all docs in ONE tx --------------------------------
	{
		fx, coll := newColl()
		docs := make([]*anyenc.Value, n)
		for i, r := range recs {
			docs[i] = mkDoc(r, "")
		}
		measure("batch insert (1 tx)", n, func() { require.NoError(t, coll.Insert(ctx, docs...)) })
		fx.finish()
	}

	// ---- single insert: one doc per tx -----------------------------------
	{
		fx, coll := newColl()
		measure("single insert (N txs)", n, func() {
			for _, r := range recs {
				require.NoError(t, coll.Insert(ctx, mkDoc(r, "")))
			}
		})
		fx.finish()
	}

	// Populated collection reused for search + update measurements.
	fx, coll := newColl()
	defer fx.finish()
	docs := make([]*anyenc.Value, n)
	for i, r := range recs {
		docs[i] = mkDoc(r, "")
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	t.Logf("%-22s %8s %12s %12s %8.1f MB", "index loaded (held)", "-", "-", "-", float64(heapAlloc())/(1<<20))

	// ---- search ----------------------------------------------------------
	runQueries := func(label string, queries []string, repeats int) {
		measure(label, len(queries)*repeats, func() {
			for rep := 0; rep < repeats; rep++ {
				for _, q := range queries {
					iter, e := coll.Find(`{"$text":{"$search":` + quoteJSON(q) + `}}`).Limit(10).Iter(ctx)
					require.NoError(t, e)
					for iter.Next() {
						if _, e := iter.Doc(); e != nil {
							t.Fatal(e)
						}
					}
					require.NoError(t, iter.Close())
				}
			}
		})
	}
	runQueries("search (mixed)", []string{
		"Hindenburg", "Boeing", "London", "crashes into a mountain", "Zeppelin", "engine failure",
	}, 50)
	runQueries("search (single-term)", []string{"crash"}, 200)

	// ---- single update: one edit per tx ----------------------------------
	measure("single update (N txs)", n, func() {
		for i, r := range recs {
			require.NoError(t, coll.UpsertOne(ctx, mkDoc(r, fmt.Sprintf("editmark%d", i))))
		}
	})

	// ---- batch update: all edits in ONE managed tx -----------------------
	measure("batch update (1 tx)", n, func() {
		wtx, e := coll.WriteTx(ctx)
		require.NoError(t, e)
		tctx := wtx.Context()
		for i, r := range recs {
			require.NoError(t, coll.UpsertOne(tctx, mkDoc(r, fmt.Sprintf("batchedit%d", i))))
		}
		require.NoError(t, wtx.Commit())
	})

	// ---- small interactive edit: append one word to a single doc ---------
	measure("interactive edit", 500, func() {
		for i := 0; i < 500; i++ {
			require.NoError(t, coll.UpsertOne(ctx, mkDoc(recs[0], fmt.Sprintf("kw%d", i))))
		}
	})
}
