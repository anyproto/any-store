package anystore

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

// Multi-key index dedup invariants.
//
// When an index is built on an array-valued field, one index entry per
// array element exists per document. A query whose bounds match multiple
// elements of the same document must still return that document exactly
// once, and Count must agree with the distinct-doc count of Iter.
//
// The pipeline achieves this via:
//  - CanonicalKeyDedupIter (O(1) memory) for single-field indexes
//  - SeenSetDedupIter (O(distinct) memory) for compound indexes
//  - Guard on the covering-index Count fast path when len(Bounds) > 1
//
// Scope: dedup is wired for both single-field and compound multi-key
// indexes with any bound shape (point, range, or empty). Scalar-valued
// fields pay a runtime TypeArray check and a pass-through cost.
func TestMultiKeyIn_Dedup(t *testing.T) {
	// ------------------------------------------------------------------
	// Count() used to over-count on multi-key because the covering-index
	// fast path counts index ENTRIES, not distinct documents. Fixed by
	// guarding the fast path with len(Bounds) <= 1.
	// ------------------------------------------------------------------
	t.Run("count_over_multikey_in_is_correct", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["ai","theory","philosophy"]}`),
		))

		filter := `{"tags":{"$in":["ai","theory"]}}`

		n, err := coll.Find(filter).Count(ctx)
		require.NoError(t, err)
		t.Logf("Count returned: %d (expected 1)", n)
		assert.Equal(t, 1, n, "one distinct doc matches; Count should dedup")
	})

	// ------------------------------------------------------------------
	// Iter() previously surfaced duplicate docs whenever IndexSeek was
	// chosen (large collection, Sort, or IndexHint). Fixed by the
	// CanonicalKeyDedupIter wrap.
	// ------------------------------------------------------------------
	t.Run("iter_over_multikey_in_is_correct_under_indexseek", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

		// One doc with both "ai" and "theory", plus filler so the CBO
		// prefers IndexSeek over FullScan.
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["ai","theory","philosophy"],"t":5}`),
		))
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(ctx,
				anyenc.MustParseJson(fmt.Sprintf(`{"id":"f%d","tags":["filler"],"t":%d}`, i, i)),
			))
		}

		filter := `{"tags":{"$in":["ai","theory"]}}`
		q := coll.Find(filter).Sort("-t") // Sort biases the planner toward the index

		exp, err := q.Explain(ctx)
		require.NoError(t, err)
		t.Logf("plan (Iter):\n%s", exp.Plan)

		ids, err := iterIds(t, coll, q)
		require.NoError(t, err)
		t.Logf("observed ids: %v", ids)
		assert.Equal(t, []string{"p1"}, ids,
			"p1 must appear exactly once — not one per matching tag element")
	})

	// ------------------------------------------------------------------
	// Deterministic version: IndexHint boost forces IndexSeek regardless
	// of the cost model, so the invariant is exercised stably.
	// ------------------------------------------------------------------
	t.Run("iter_over_multikey_in_is_correct_with_IndexHint", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["a","b","c","d"]}`),
			anyenc.MustParseJson(`{"id":"p2","tags":["a","x"]}`),
		))

		q := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).
			IndexHint(IndexHint{IndexName: "tags", Boost: 10000})

		exp, err := q.Explain(ctx)
		require.NoError(t, err)
		t.Logf("plan (Iter):\n%s", exp.Plan)

		ids, err := iterIds(t, coll, q)
		require.NoError(t, err)
		t.Logf("observed ids: %v", ids)
		assert.Equal(t, []string{"p1", "p2"}, ids,
			"p1 matches 3 ranges, p2 matches 1 — each doc must appear once")
	})

	// -------- baselines that MUST keep passing after the fix --------

	t.Run("baseline_single_value_tag_filter_is_correct", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["ai","theory","philosophy"]}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{"tags":"ai"}`))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids)

		n, err := coll.Find(`{"tags":"ai"}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("baseline_fullscan_without_index_is_correct", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		// No index on tags — planner must use FullScan.

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["ai","theory","philosophy"]}`),
			anyenc.MustParseJson(`{"id":"p2","tags":["other"]}`),
		))

		filter := `{"tags":{"$in":["ai","theory"]}}`
		ids, err := iterIds(t, coll, coll.Find(filter))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids)

		n, err := coll.Find(filter).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n, "FullScan visits each doc once; Count is correct")
	})

	t.Run("baseline_in_over_scalar_field_is_correct", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"status"}}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","status":"draft"}`),
			anyenc.MustParseJson(`{"id":"p2","status":"published"}`),
			anyenc.MustParseJson(`{"id":"p3","status":"archived"}`),
		))

		filter := `{"status":{"$in":["draft","published"]}}`
		ids, err := iterIds(t, coll, coll.Find(filter))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1", "p2"}, ids)

		n, err := coll.Find(filter).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n, "scalar field: one index entry per doc; safe to count")
	})

	// ---------------- range bounds on single-field multi-key ----------------

	t.Run("iter_range_bounds_on_multikey_no_duplicates", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["a","b","c"]}`),
			anyenc.MustParseJson(`{"id":"p2","tags":["b","z"]}`),
		))

		ids, err := iterIds(t, coll,
			coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`).
				IndexHint(IndexHint{IndexName: "tags", Boost: 10000}))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1", "p2"}, ids, "range bounds: each doc exactly once")

		n, err := coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
	})

	t.Run("iter_full_index_scan_no_filter_no_duplicates", func(t *testing.T) {
		// Pure Sort over an index with no filter ⇒ IndexScan with empty
		// bounds. Dedup must still run.
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["a","b","c"]}`),
			anyenc.MustParseJson(`{"id":"p2","tags":["d"]}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{}`).Sort("tags"))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1", "p2"}, ids,
			"Sort over multi-key index without a filter must not surface duplicates")
	})

	// ---------------- compound-index coverage (SeenSetDedupIter branch) ----------------

	t.Run("compound_scalar_array_no_duplicates", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Name: "status_tags", Fields: []string{"status", "tags"},
		}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","status":"draft","tags":["a","b"]}`),
			anyenc.MustParseJson(`{"id":"p2","status":"draft","tags":["c"]}`),
			anyenc.MustParseJson(`{"id":"p3","status":"published","tags":["a"]}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{"status":"draft","tags":{"$in":["a","b"]}}`))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids,
			"compound (scalar, array) must dedup via SeenSetDedupIter")

		n, err := coll.Find(`{"status":"draft","tags":{"$in":["a","b"]}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("compound_array_scalar_no_duplicates", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Name: "tags_status", Fields: []string{"tags", "status"},
		}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","status":"draft","tags":["a","b"]}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{"tags":{"$in":["a","b"]},"status":"draft"}`))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids,
			"compound (array, scalar) must dedup via SeenSetDedupIter")
	})

	t.Run("compound_range_on_trailing_array_no_duplicates", func(t *testing.T) {
		// Index (status, tags); range on tags produces multiple compound
		// entries per doc. SeenSet must dedup.
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Name: "status_tags", Fields: []string{"status", "tags"},
		}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","status":"draft","tags":["a","b","c"]}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{"status":"draft","tags":{"$gte":"a","$lte":"c"}}`))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids,
			"compound range on trailing array must dedup")
	})

	t.Run("compound_scalar_scalar_sanity", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Name: "author_status", Fields: []string{"authorId", "status"},
		}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","authorId":"u1","status":"draft"}`),
			anyenc.MustParseJson(`{"id":"p2","authorId":"u1","status":"published"}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{"authorId":"u1","status":"draft"}`))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids, "compound scalar-scalar: no dup expected or needed")

		n, err := coll.Find(`{"authorId":"u1","status":"draft"}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})
}

func iterIds(t *testing.T, _ Collection, q Query) ([]string, error) {
	t.Helper()
	it, err := q.Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var out []string
	for it.Next() {
		d, err := it.Doc()
		if err != nil {
			return nil, err
		}
		out = append(out, string(d.Value().GetStringBytes("id")))
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// --- Coverage tests from multikey_dedup_coverage_test.go ---

// TestMultiKeyDedup_Coverage_PureSortNoBounds exercises buildIndexScanChain
// on a multi-key index with a pure Sort() and no filter (noBounds==true).
// Covers internal/qplanner/planner.go:1017-1029 — the dedup wrap must still
// run even when bounds are empty. Each doc must appear exactly once despite
// multiple index entries per array element.
func TestMultiKeyDedup_Coverage_PureSortNoBounds(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "posts")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))

	// Each doc has multiple tags → multiple index entries per doc.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"p1","tags":["a","b","c","d","e"]}`),
		anyenc.MustParseJson(`{"id":"p2","tags":["b","c","f"]}`),
		anyenc.MustParseJson(`{"id":"p3","tags":["x"]}`),
		anyenc.MustParseJson(`{"id":"p4","tags":["a","z"]}`),
	))

	// Pure sort on the multi-key indexed field with no filter.
	q := coll.Find(`{}`).Sort("tags")

	it, err := q.Iter(ctx)
	require.NoError(t, err)
	defer it.Close()

	counts := map[string]int{}
	var order []string
	for it.Next() {
		d, err := it.Doc()
		require.NoError(t, err)
		id := string(d.Value().GetStringBytes("id"))
		counts[id]++
		order = append(order, id)
	}
	require.NoError(t, it.Err())

	// Each doc must appear exactly once (dedup must run over empty bounds).
	for _, id := range []string{"p1", "p2", "p3", "p4"} {
		assert.Equal(t, 1, counts[id],
			"doc %q must appear exactly once under Sort('tags')+no filter", id)
	}
	sorted := append([]string{}, order...)
	sort.Strings(sorted)
	assert.ElementsMatch(t, []string{"p1", "p2", "p3", "p4"}, sorted)

	// Count() must also dedup correctly (not return 11 entries).
	n, err := coll.Find(`{}`).Sort("tags").Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, n,
		"Count on sort-only multi-key must match distinct-doc count")
}
