package anystore

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

// TestCBOCalibration creates a large collection and runs various query patterns,
// measuring actual wall-clock time alongside CBO cost estimates.
// Run with: go test -run TestCBOCalibration -v -count=1
func TestCBOCalibration(t *testing.T) {
	const N = 50000

	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cal")
	require.NoError(t, err)

	// Create indexes before inserting data so sketches are populated
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"c"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "-b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"c", "a"}}))

	// Insert data in batches
	// a: i%100 (100 distinct, ~500 each = 1%)
	// b: i%200 (200 distinct, ~250 each = 0.5%)
	// c: i%10  (10 distinct, ~5000 each = 10%)
	// d: i%1000 (not indexed)
	batchSize := 500
	for start := 0; start < N; start += batchSize {
		end := start + batchSize
		if end > N {
			end = N
		}
		var docs []*anyenc.Value
		for i := start; i < end; i++ {
			docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
				`{"id":%d,"a":%d,"b":%d,"c":%d,"d":%d}`,
				i, i%100, i%200, i%10, i%1000,
			)))
		}
		require.NoError(t, coll.Insert(ctx, docs...))
	}

	type queryCase struct {
		name   string
		filter string
		sort   []string
		limit  int
	}

	cases := []queryCase{
		// === Baselines ===
		{"fullscan-all", "", nil, 0},
		{"fullscan-filter-d", `{"d":42}`, nil, 0},
		{"fullscan-sort-d", "", []string{"d"}, 0},

		// === Equality filter (indexed) ===
		{"eq-a(1%)", `{"a":42}`, nil, 0},
		{"eq-c(10%)", `{"c":5}`, nil, 0},
		{"eq-b(0.5%)", `{"b":100}`, nil, 0},

		// === Range filter ===
		{"range-a(10%)", `{"a":{"$gte":40,"$lt":50}}`, nil, 0},
		{"range-a(50%)", `{"a":{"$gte":0,"$lt":50}}`, nil, 0},
		{"range-a(1%)", `{"a":{"$gte":42,"$lt":43}}`, nil, 0},

		// === Sort only (no filter) ===
		{"sort-a-asc", "", []string{"a"}, 0},
		{"sort-a-desc", "", []string{"-a"}, 0},

		// === Multi-field compound sort (no filter) ===
		{"sort-ab", "", []string{"a", "b"}, 0},
		{"sort-ab-lim10", "", []string{"a", "b"}, 10},
		{"sort-a-negb", "", []string{"a", "-b"}, 0},
		{"sort-ca", "", []string{"c", "a"}, 0},
		{"sort-ca-lim10", "", []string{"c", "a"}, 10},

		// === Sort with limit ===
		{"sort-a-lim10", "", []string{"a"}, 10},
		{"sort-a-lim100", "", []string{"a"}, 100},
		{"sort-a-lim1000", "", []string{"a"}, 1000},
		{"sort-a-desc-lim10", "", []string{"-a"}, 10},
		{"sort-a-desc-lim100", "", []string{"-a"}, 100},

		// === Filter + Sort (index covers both) ===
		{"eq-a+sort-a", `{"a":42}`, []string{"a"}, 0},
		{"eq-a+sort-b", `{"a":42}`, []string{"b"}, 0},            // compound (a,b)
		{"eq-a+sort-b-desc", `{"a":42}`, []string{"-b"}, 0},      // compound (a,-b)
		{"eq-c+sort-a", `{"c":5}`, []string{"a"}, 0},             // compound (c,a)

		// === Filter + Sort (different indexes — needs in-memory sort) ===
		{"eq-a+sort-c", `{"a":42}`, []string{"c"}, 0},
		{"eq-a+sort-d", `{"a":42}`, []string{"d"}, 0},            // d not indexed

		// === Filter + Sort + Limit ===
		{"eq-a+sort-b+lim10", `{"a":42}`, []string{"b"}, 10},
		{"eq-a+sort-b+lim100", `{"a":42}`, []string{"b"}, 100},
		{"eq-c+sort-a+lim10", `{"c":5}`, []string{"a"}, 10},
		{"eq-c+sort-a+lim100", `{"c":5}`, []string{"a"}, 100},

		// === Compound filter ===
		{"eq-ab", `{"a":42,"b":100}`, nil, 0},
		{"eq-ca", `{"c":5,"a":42}`, nil, 0},

		// === Range + sort ===
		{"range-a+sort-a", `{"a":{"$gte":40,"$lt":50}}`, []string{"a"}, 0},
		{"range-a+sort-a-desc", `{"a":{"$gte":40,"$lt":50}}`, []string{"-a"}, 0},
		{"range-a+sort-a+lim10", `{"a":{"$gte":40,"$lt":50}}`, []string{"a"}, 10},

		// === ID field (special: stored as doc key, always "indexed") ===
		{"sort-id-asc", "", []string{"id"}, 0},
		{"sort-id-desc", "", []string{"-id"}, 0},
		{"sort-id-lim10", "", []string{"id"}, 10},
		{"sort-id-desc-lim10", "", []string{"-id"}, 10},
		{"eq-a+sort-id-asc", `{"a":42}`, []string{"id"}, 0},
		{"eq-a+sort-id-desc", `{"a":42}`, []string{"-id"}, 0},
		{"eq-a+sort-id-asc-lim10", `{"a":42}`, []string{"id"}, 10},
		{"eq-a+sort-id-desc-lim10", `{"a":42}`, []string{"-id"}, 10},
		{"eq-c+sort-id-lim10", `{"c":5}`, []string{"id"}, 10},
		{"filter-d+sort-id", `{"d":42}`, []string{"id"}, 0},            // non-indexed filter + id sort
		{"filter-d+sort-id-desc", `{"d":42}`, []string{"-id"}, 0},      // non-indexed filter + reverse id sort
		{"filter-d+sort-id-lim10", `{"d":42}`, []string{"id"}, 10},     // non-indexed filter + id sort + limit
		{"range-a+sort-id", `{"a":{"$gte":40,"$lt":50}}`, []string{"id"}, 0},       // range filter + id sort
		{"range-a+sort-id-lim10", `{"a":{"$gte":40,"$lt":50}}`, []string{"id"}, 10}, // range filter + id sort + limit

		// === Compound index: scan only first field (prefix) ===
		{"eq-a-only(compound-ab)", `{"a":42}`, nil, 0},        // uses (a,b) prefix
		{"eq-a+sort-b(compound)", `{"a":42}`, []string{"b"}, 0}, // (a,b) covers both

		// === Compound index: scan only second field (non-prefix) ===
		{"eq-b-only(compound-ab)", `{"b":100}`, nil, 0},       // b is 2nd in (a,b) — can't use compound
		{"eq-a+eq-b(compound)", `{"a":42,"b":100}`, nil, 0},   // full compound match

		// === Compound filter+sort where sort is on non-first field ===
		{"eq-c+sort-a(compound-ca)", `{"c":5}`, []string{"a"}, 0},   // (c,a): c=eq, sort a
		{"eq-c+sort-a-desc(ca)", `{"c":5}`, []string{"-a"}, 0},      // (c,a): c=eq, sort -a

		// === Filter + compound sort ===
		{"eq-c+sort-ca", `{"c":5}`, []string{"c", "a"}, 0},     // filter on prefix, sort matches index
		{"eq-c+sort-ca-lim10", `{"c":5}`, []string{"c", "a"}, 10},
		{"eq-a+sort-ab", `{"a":42}`, []string{"a", "b"}, 0},     // filter on prefix, sort matches index
		{"eq-a+sort-ab-lim10", `{"a":42}`, []string{"a", "b"}, 10},

		// === Compound with limit (prefix filter + second-field sort) ===
		{"eq-a+sort-b+lim5(ab)", `{"a":42}`, []string{"b"}, 5},
		{"eq-c+sort-a+lim5(ca)", `{"c":5}`, []string{"a"}, 5},
		{"eq-c+sort-a+lim50(ca)", `{"c":5}`, []string{"a"}, 50},
	}

	const warmupRuns = 3
	const measuredRuns = 10

	type result struct {
		name     string
		plan     string
		cost     float64
		medianNs int64
		docs     int
		explain  string // full explain text
	}

	var results []result

	measureQuery := func(t *testing.T, name string, buildQuery func() Query) result {
		t.Helper()
		explain, err := buildQuery().Explain(ctx)
		require.NoError(t, err)

		var docCount int
		for range warmupRuns {
			iter, err := buildQuery().Iter(ctx)
			require.NoError(t, err)
			cnt := 0
			for iter.Next() {
				_, _ = iter.Doc()
				cnt++
			}
			require.NoError(t, iter.Err())
			iter.Close()
			docCount = cnt
		}

		times := make([]int64, measuredRuns)
		for r := range measuredRuns {
			start := time.Now()
			iter, err := buildQuery().Iter(context.Background())
			require.NoError(t, err)
			for iter.Next() {
				_, _ = iter.Doc()
			}
			require.NoError(t, iter.Err())
			iter.Close()
			times[r] = time.Since(start).Nanoseconds()
		}

		for i := 0; i < len(times); i++ {
			for j := i + 1; j < len(times); j++ {
				if times[j] < times[i] {
					times[i], times[j] = times[j], times[i]
				}
			}
		}
		medianNs := times[len(times)/2]

		var cost float64
		if idx := strings.Index(explain.Plan, "Cost: "); idx >= 0 {
			fmt.Sscanf(explain.Plan[idx:], "Cost: %f", &cost)
		}

		return result{
			name:     name,
			plan:     explain.Sql,
			cost:     cost,
			medianNs: medianNs,
			docs:     docCount,
			explain:  explain.Plan,
		}
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buildQuery := func() Query {
				var q Query
				if tc.filter == "" {
					q = coll.Find(nil)
				} else {
					q = coll.Find(tc.filter)
				}
				if len(tc.sort) > 0 {
					sortArgs := make([]any, len(tc.sort))
					for i, s := range tc.sort {
						sortArgs[i] = s
					}
					q = q.Sort(sortArgs...)
				}
				if tc.limit > 0 {
					q = q.Limit(uint(tc.limit))
				}
				return q
			}

			r := measureQuery(t, tc.name, buildQuery)
			results = append(results, r)
			t.Logf("docs=%d cost=%.1f median=%s\n%s", r.docs, r.cost, time.Duration(r.medianNs), r.explain)
		})
	}

	// Print summary table
	t.Log("")
	t.Log("=== CBO Calibration Summary (N=50000) ===")
	t.Log("Indexes: a, b, c, (a,b), (a,-b), (c,a)")
	t.Log("Data: a=i%100(1%), b=i%200(0.5%), c=i%10(10%), d=i%1000(not indexed)")
	t.Log("")
	t.Logf("%-30s %6s %10s %10s %6s  %s",
		"Query", "Docs", "Cost", "Median", "µs/doc", "Plan")
	t.Logf("%s", strings.Repeat("-", 120))
	for _, r := range results {
		usPerDoc := float64(0)
		if r.docs > 0 {
			usPerDoc = float64(r.medianNs) / float64(r.docs) / 1000.0
		}
		planShort := r.plan
		if len(planShort) > 55 {
			planShort = planShort[:52] + "..."
		}
		t.Logf("%-30s %6d %10.1f %10s %6.2f  %s",
			r.name, r.docs, r.cost, time.Duration(r.medianNs), usPerDoc, planShort)
	}

	// Print key cost vs time correlations
	t.Log("")
	t.Log("=== Key Decision Points ===")
	t.Log("CBO should prefer lower-time plan. Compare cost ranking vs time ranking:")
	for _, r := range results {
		if r.cost > 0 {
			// cost-per-microsecond ratio: how many cost units per microsecond of real time
			costPerUs := r.cost / (float64(r.medianNs) / 1000.0)
			t.Logf("  %-30s cost=%8.1f time=%10s ratio=%.2f cost/µs",
				r.name, r.cost, time.Duration(r.medianNs), costPerUs)
		}
	}
}
