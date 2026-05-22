package anystore

import (
	"fmt"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/stretchr/testify/require"
)

// queryIdsHinted runs Find(filter).Sort(sortSpec).Limit(limit).Offset(offset)
// forcing the named index so the IndexScan/IndexSeek path (the one the
// offset fast-skip optimizes) is exercised regardless of the cost model.
func queryIdsHinted(t *testing.T, coll Collection, idxName string, filter any, sortSpec string, limit, offset int) []int {
	t.Helper()
	q := coll.Find(filter).Sort(sortSpec)
	if idxName != "" {
		q = q.IndexHint(IndexHint{IndexName: idxName, Boost: 1_000_000})
	}
	if limit > 0 {
		q = q.Limit(uint(limit))
	}
	if offset > 0 {
		q = q.Offset(uint(offset))
	}
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var ids []int
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		ids = append(ids, int(d.Value().GetInt("id")))
	}
	require.NoError(t, iter.Err())
	return ids
}

// TestOffsetSkip_IndexOrdered_PrefixSkip is the focused correctness proof for
// the indexed-sort OFFSET streaming-skip optimization. It asserts the core
// contract: applying Offset(O)[+Limit(L)] to an index-ordered scan yields
// EXACTLY full[O:O+L], where `full` is the same query's ordered output with
// no offset (so the fast-skip is disengaged for the baseline). This validates
// that the cursor-level skip is a pure prefix-skip of the deduped logical-row
// stream — including across btree page boundaries and through the
// scalar->multi-key transition for mixed/array single-field indexes.
func TestOffsetSkip_IndexOrdered_PrefixSkip(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "golden")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"m"}}))

	// 1500 docs: large enough that the (a)/(m)/(tags) indexes span many btree
	// pages, so high offsets cross page boundaries inside the cursor skip.
	const N = 1500
	for i := 0; i < N; i++ {
		// m: half scalar, half a 2-element array. A single-field index over m
		// therefore mixes scalar (multiKey=false) and array (multiKey=true)
		// entries, exercising the fast-skip's stop-at-first-multiKey fallback.
		var mField string
		if i%2 == 0 {
			mField = fmt.Sprintf(`"m":%d`, i%50)
		} else {
			mField = fmt.Sprintf(`"m":[%d,%d]`, i%50, (i%50)+1000)
		}
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d,"tags":["tag-%d","cat-%d"],%s}`,
			i, i%100, (i/100)%50, i%20, i%10, mField))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	offsets := []int{0, 1, 2, 7, 99, 100, 101, 250, 999, 1000, 1499, 1500, 1501, 3000}
	limits := []int{0, 1, 10, 137}

	cases := []struct {
		name string
		idx  string // forced index
		sort string
	}{
		{"single_scalar_a_asc", "a", "a"},     // single-field scalar — fast-skip ENGAGED
		{"single_scalar_a_desc", "a", "-a"},   // reverse fast-skip
		{"single_scalar_b_asc", "b", "b"},     // another scalar field
		{"single_array_tags_asc", "tags", "tags"},   // every doc multi-key — fast-skip bails immediately
		{"single_mixed_m_asc", "m", "m"},      // scalar->multiKey transition mid-skip
		{"single_mixed_m_desc", "m", "-m"},    // reverse, mixed
		{"compound_ab_a_asc", "a,b", "a"},     // compound index, scalar — fast-skip ENGAGED (1 entry/doc)
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// Baseline ordered stream with NO offset (fast-skip disengaged).
			full := queryIdsHinted(t, coll, c.idx, nil, c.sort, 0, 0)
			require.Lenf(t, full, N, "baseline must return all %d docs", N)

			for _, off := range offsets {
				for _, lim := range limits {
					got := queryIdsHinted(t, coll, c.idx, nil, c.sort, lim, off)

					lo := off
					if lo > len(full) {
						lo = len(full)
					}
					hi := len(full)
					if lim > 0 && lo+lim < hi {
						hi = lo + lim
					}
					want := append([]int(nil), full[lo:hi]...)

					require.Equalf(t, want, got,
						"case=%s off=%d lim=%d: Offset must be a pure prefix-skip of the index-ordered stream",
						c.name, off, lim)
				}
			}
		})
	}

	// Prove the optimization actually FIRES for the scalar single-field case:
	// Sort(a).Limit(10).Offset(1000) must fetch+parse only ~limit docs, not
	// ~limit+offset. The skipped 1000 rows are advanced at the index cursor
	// without any data-namespace fetch. For the array case (tags), every entry
	// is multi-key so the fast-skip bails immediately and the fetch count
	// stays ~limit+offset (slow-but-correct path preserved).
	t.Run("perf_proof_scalar_skips_fetch", func(t *testing.T) {
		qplanner.EnablePerfCounters(true)
		defer qplanner.EnablePerfCounters(false)

		const limit, offset = 10, 1000

		qplanner.ResetPerfCounters()
		_ = queryIdsHinted(t, coll, "a", nil, "a", limit, offset)
		scalar := qplanner.SnapshotPerfCounters()

		qplanner.ResetPerfCounters()
		_ = queryIdsHinted(t, coll, "tags", nil, "tags", limit, offset)
		array := qplanner.SnapshotPerfCounters()

		t.Logf("scalar(a): FetchYields=%d FetchNextCalls=%d", scalar.FetchYields, scalar.FetchNextCalls)
		t.Logf("array(tags): FetchYields=%d FetchNextCalls=%d", array.FetchYields, array.FetchNextCalls)

		// Scalar path: only the limit window is fetched (a small constant slack
		// well under the offset). Decisive proof the 1000 skipped rows are not
		// fetched.
		require.Lessf(t, int(scalar.FetchYields), limit+50,
			"scalar fast-skip must fetch ~limit docs, got %d (offset=%d)", scalar.FetchYields, offset)

		// Array path: the multi-key fallback still fetches across the offset
		// window (~limit+offset, scaled by the 2 entries/doc dedup), proving we
		// did NOT wrongly fast-skip a multi-key index.
		require.Greaterf(t, int(array.FetchYields), offset,
			"array index must keep the fetch-then-skip path, got %d fetches", array.FetchYields)
	})
}
