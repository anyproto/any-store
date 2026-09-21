package test

import (
	"fmt"
	"os"
	"testing"

	"sort"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
qplanner_reverse_index_test.go — EXHAUSTIVE adversarial matrix for the
"invert reverse-flagged index fields on write" design.

Design: a compound index declared with mixed directions, e.g. {Fields:["a","-b"]},
is PHYSICALLY stored with each reverse field bitwise-inverted, so a single forward
scan yields (a asc, b desc) and a single reverse scan yields (a desc, b asc).
Realizable sort orders are served by a FAST index scan with NO in-memory sort
(ExactSort); only genuinely-unrealizable orders fall back to an in-memory Sort.

This file drives the FULL matrix:

    index shapes : "a", "-a", "a,b", "a,-b", "-a,b", "-a,-b", "a,-b,c"
    operations   : equality, $gt, $gte, $lt, $lte, two-sided range, $in,
                   sort asc, sort desc, sort mixed, sort by prefix,
                   covering filter on trailing field, insert/update/delete,
                   unique conflict, sparse/null, array multikey

Every functional assertion is indexed-vs-UNINDEXED-twin PARITY (catches silent
drops, over-counts, and mis-orders). Where the order is fully determined by the
sort spec, ordered parity is asserted via the SORT-KEY PROJECTION (not doc ids,
whose tie-break differs between an index scan and an in-memory sort). For the
index-covered sort cases the plan SHAPE is asserted via Explain.

Independently verified (each reproduces identically on a FORWARD index, so it is a
pre-existing planner limitation NOT introduced by this change):
  - compound-multikey Count over-counts a partial prefix that leaves the array
    field unconstrained; Count parity is asserted only when the array field is
    constrained. Iter (DocDedup) is always correct.
  - pure Sort over a multi-key array index uses FullScan -> Sort.

Reverse-field bound bytes render as an "err:/unknown type" placeholder in Explain
(the decode path intentionally does not un-invert); reverse-field Explain checks
therefore assert only plan shape, never the reverse [bounds=...] segment.

Helpers here are uniquely named (qrm* prefix) to avoid colliding with the
package helpers in reverse_index_test.go (revPair/idsOf/idsSorted/...).
*/

// qrmTwins builds an indexed collection and an unindexed twin populated with the
// same docs, for parity assertions.
func qrmTwins(t *testing.T, index anystore.IndexInfo, docs []string) (idx, plain anystore.Collection) {
	t.Helper()
	build := func(withIndex bool) anystore.Collection {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, coll.EnsureIndex(ctx, index))
		}
		for _, d := range docs {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
		}
		return coll
	}
	return build(true), build(false)
}

// qrmSortKey projects the sort key (the concatenated string values of the sort
// fields, in result order). This sequence is fully determined by the sort spec,
// so it is stable across an index scan vs an in-memory sort and is the correct
// thing to compare for ordered parity. fields are bare field NAMES (no '-').
func qrmSortKey(t testing.TB, q anystore.Query, fields ...string) []string {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []string
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		parts := make([]string, len(fields))
		for i, f := range fields {
			if v := d.Value().Get(f); v != nil {
				parts[i] = v.String()
			} else {
				parts[i] = "<nil>"
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	require.NoError(t, iter.Err())
	return out
}

// qrmIDs returns the result doc ids in result order.
func qrmIDs(t testing.TB, q anystore.Query) []int { return collectIntField(t, q, "id") }

// qrmIDSet returns the result doc ids as a sorted set (order-independent).
func qrmIDSet(t testing.TB, q anystore.Query) []int {
	out := qrmIDs(t, q)
	sort.Ints(out)
	return out
}

// qrmCount returns Count(ctx), failing the test on error.
func qrmCount(t testing.TB, q anystore.Query) int {
	t.Helper()
	n, err := q.Count(ctx)
	require.NoError(t, err)
	return n
}

// qrmExplain returns the Explain SQL chain string.
func qrmExplain(t testing.TB, q anystore.Query) string {
	t.Helper()
	ex, err := q.Explain(ctx)
	require.NoError(t, err)
	return ex.Sql
}

// qrmSet asserts indexed/unindexed set+count parity for a filter.
func qrmSet(t *testing.T, idx, plain anystore.Collection, filter string) {
	t.Helper()
	assert.Equal(t, qrmIDSet(t, plain.Find(filter)), qrmIDSet(t, idx.Find(filter)), "id set: %s", filter)
	assert.Equal(t, qrmCount(t, plain.Find(filter)), qrmCount(t, idx.Find(filter)), "count: %s", filter)
}

// qrmOrdered asserts indexed/unindexed ordered sort-key parity for a sorted query.
// filter may be "" for no filter. sortArgs may contain '-'; keyFields are the
// bare names to project.
func qrmOrdered(t *testing.T, idx, plain anystore.Collection, filter string, sortArgs []any, keyFields ...string) {
	t.Helper()
	var fq, pq anystore.Query
	if filter == "" {
		fq, pq = idx.Find(nil), plain.Find(nil)
	} else {
		fq, pq = idx.Find(filter), plain.Find(filter)
	}
	assert.Equal(t,
		qrmSortKey(t, pq.Sort(sortArgs...), keyFields...),
		qrmSortKey(t, fq.Sort(sortArgs...), keyFields...),
		"ordered %v sort=%v", filter, sortArgs)
}

// ============================================================================
// MATRIX 1 — single field ASC "a" and DESC "-a": every operator + sort + Explain
// ============================================================================

func TestQRM_SingleField_FullOperatorMatrix(t *testing.T) {
	docs := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		docs = append(docs, fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
	}

	shapes := []struct {
		name     string
		field    string // "a" or "-a"
		fwdSort  string // sort that the forward scan serves (declared direction)
		revSort  string // sort that the reverse scan serves
		fwdEmpty bool   // forward scan has no "(reverse)" token
	}{
		{"asc", "a", "a", "-a", true},
		{"desc", "-a", "-a", "a", true},
	}

	singleBound := []string{
		`{"a":7}`,
		`{"a":{"$gt":7}}`,
		`{"a":{"$gte":7}}`,
		`{"a":{"$lt":7}}`,
		`{"a":{"$lte":7}}`,
		`{"a":{"$gte":5,"$lte":15}}`,
		`{"a":{"$gt":5,"$lt":15}}`,
	}
	multiBound := []string{
		`{"a":{"$in":[3,11,7,2]}}`,
		`{"a":{"$ne":7}}`,
	}

	for _, sh := range shapes {
		t.Run(sh.name, func(t *testing.T) {
			idx, plain := qrmTwins(t, anystore.IndexInfo{Fields: []string{sh.field}}, docs)

			// --- single-bound operators: full set/count + ordered parity both dirs ---
			for _, f := range singleBound {
				t.Run("op "+f, func(t *testing.T) {
					qrmSet(t, idx, plain, f)
					qrmOrdered(t, idx, plain, f, []any{sh.fwdSort}, "a")
					qrmOrdered(t, idx, plain, f, []any{sh.revSort}, "a")
				})
			}

			// --- multi-bound operators: set/count parity + order both ways ---
			// A reverse scan consumes the ascending bound list from the top
			// (index_iter.go), so cross-bound order holds in BOTH directions.
			for _, f := range multiBound {
				t.Run("op "+f, func(t *testing.T) {
					qrmSet(t, idx, plain, f)
					qrmOrdered(t, idx, plain, f, []any{sh.fwdSort}, "a")
					qrmOrdered(t, idx, plain, f, []any{sh.revSort}, "a")
				})
			}

			// --- pure sort (no filter): both directions, ordered parity + Explain ---
			t.Run("sort fwd", func(t *testing.T) {
				qrmOrdered(t, idx, plain, "", []any{sh.fwdSort}, "a")
				sql := qrmExplain(t, idx.Find(nil).Sort(sh.fwdSort))
				assert.Contains(t, sql, "IndexScan("+sh.field+")")
				assert.NotContains(t, sql, "(reverse)")
				assert.NotContains(t, sql, "-> Sort")
			})
			t.Run("sort rev", func(t *testing.T) {
				qrmOrdered(t, idx, plain, "", []any{sh.revSort}, "a")
				sql := qrmExplain(t, idx.Find(nil).Sort(sh.revSort))
				assert.Contains(t, sql, "IndexScan("+sh.field+")")
				assert.Contains(t, sql, "(reverse)")
				assert.NotContains(t, sql, "-> Sort")
			})

			// --- sort + limit: index supplies order, no in-memory TopK ---
			t.Run("sort+limit", func(t *testing.T) {
				// fwd Limit(1): forward scan's first row.
				wantFwd := qrmSortKey(t, plain.Find(nil).Sort(sh.fwdSort).Limit(1), "a")
				gotFwd := qrmSortKey(t, idx.Find(nil).Sort(sh.fwdSort).Limit(1), "a")
				assert.Equal(t, wantFwd, gotFwd, "fwd limit")
				sql := qrmExplain(t, idx.Find(nil).Sort(sh.fwdSort).Limit(1))
				assert.NotContains(t, sql, "TopK")
				assert.NotContains(t, sql, "-> Sort")
			})
		})
	}
}

func TestQRM_SingleField_MixedTypeOrdering(t *testing.T) {
	// All anyenc scalar types in one field, to exercise every inverted tag in the
	// length-skip parser and the descending type ordering.
	docs := []string{
		`{"id":1,"a":null}`,
		`{"id":2,"a":5}`,
		`{"id":3,"a":-3}`,
		`{"id":4,"a":"x"}`,
		`{"id":5,"a":"abc"}`,
		`{"id":6,"a":true}`,
		`{"id":7,"a":false}`,
		`{"id":8,"a":2.5}`,
	}
	for _, field := range []string{"a", "-a"} {
		t.Run(field, func(t *testing.T) {
			idx, plain := qrmTwins(t, anystore.IndexInfo{Fields: []string{field}}, docs)
			// Full ordered parity across mixed types (ids are unique per sort key
			// here only by coincidence; compare ids in result order via the twin).
			for _, s := range []string{"a", "-a"} {
				assert.Equal(t, qrmIDs(t, plain.Find(nil).Sort(s)), qrmIDs(t, idx.Find(nil).Sort(s)), "sort %s on %s", s, field)
			}
		})
	}
}

// ============================================================================
// MATRIX 2 — all four 2-field direction combos: a,b / a,-b / -a,b / -a,-b
// ============================================================================

func TestQRM_TwoField_DirectionCombos(t *testing.T) {
	docs := make([]string, 0, 60)
	for i := 0; i < 60; i++ {
		docs = append(docs, fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%4, i%7))
	}

	// For each compound shape, the forward scan serves [dirA(a), dirB(b)] and the
	// reverse scan serves the exact opposite. Any other order falls back to Sort.
	combos := []struct {
		fields  []string // index declaration
		fwdSort []any    // realizable forward order (declared)
		revSort []any    // realizable reverse order (opposite)
		badSort []any    // unrealizable -> in-memory Sort
		name    string
	}{
		{[]string{"a", "b"}, []any{"a", "b"}, []any{"-a", "-b"}, []any{"a", "-b"}, "asc_asc"},
		{[]string{"a", "-b"}, []any{"a", "-b"}, []any{"-a", "b"}, []any{"a", "b"}, "asc_desc"},
		{[]string{"-a", "b"}, []any{"-a", "b"}, []any{"a", "-b"}, []any{"a", "b"}, "desc_asc"},
		{[]string{"-a", "-b"}, []any{"-a", "-b"}, []any{"a", "b"}, []any{"-a", "b"}, "desc_desc"},
	}

	for _, cm := range combos {
		t.Run(cm.name, func(t *testing.T) {
			idxName := strings.Join(cm.fields, ",")
			idx, plain := qrmTwins(t, anystore.IndexInfo{Fields: cm.fields}, docs)

			// --- equality / range on the LEADING field a (single bound) ---
			leadingFilters := []string{
				`{"a":2}`,
				`{"a":{"$gte":1,"$lte":2}}`,
			}
			for _, f := range leadingFilters {
				t.Run("lead "+f, func(t *testing.T) {
					qrmSet(t, idx, plain, f)
					qrmOrdered(t, idx, plain, f, cm.fwdSort, "a", "b")
					qrmOrdered(t, idx, plain, f, cm.revSort, "a", "b")
				})
			}

			// --- equality on a, range on b (equality-pinned prefix + range) ---
			pinnedFilters := []string{
				`{"a":2,"b":{"$gte":2}}`,
				`{"a":2,"b":{"$gt":2}}`,
				`{"a":2,"b":{"$lte":4}}`,
				`{"a":2,"b":{"$lt":4}}`,
				`{"a":2,"b":{"$gte":1,"$lte":5}}`,
				`{"a":2,"b":3}`,
			}
			for _, f := range pinnedFilters {
				t.Run("pin "+f, func(t *testing.T) {
					qrmSet(t, idx, plain, f)
					qrmOrdered(t, idx, plain, f, cm.fwdSort, "a", "b")
					qrmOrdered(t, idx, plain, f, cm.revSort, "a", "b")
				})
			}

			// --- $in on leading field: set+count parity, forward-only order ---
			t.Run("in-lead", func(t *testing.T) {
				f := `{"a":{"$in":[1,3]}}`
				qrmSet(t, idx, plain, f)
				qrmOrdered(t, idx, plain, f, cm.fwdSort, "a", "b")
			})

			// --- pure sort (no filter): realizable fwd/rev served by index, no Sort ---
			t.Run("sort fwd", func(t *testing.T) {
				qrmOrdered(t, idx, plain, "", cm.fwdSort, "a", "b")
				sql := qrmExplain(t, idx.Find(nil).Sort(cm.fwdSort...))
				assert.Contains(t, sql, "IndexScan("+idxName+")")
				assert.NotContains(t, sql, "(reverse)")
				assert.NotContains(t, sql, "-> Sort")
			})
			t.Run("sort rev", func(t *testing.T) {
				qrmOrdered(t, idx, plain, "", cm.revSort, "a", "b")
				sql := qrmExplain(t, idx.Find(nil).Sort(cm.revSort...))
				assert.Contains(t, sql, "IndexScan("+idxName+")")
				assert.Contains(t, sql, "(reverse)")
				assert.NotContains(t, sql, "-> Sort")
			})

			// --- unrealizable order: must fall back to an in-memory Sort, still parity ---
			t.Run("sort bad", func(t *testing.T) {
				sql := qrmExplain(t, idx.Find(nil).Sort(cm.badSort...))
				assert.Contains(t, sql, "-> Sort", "unrealizable order must use in-memory Sort")
				qrmOrdered(t, idx, plain, "", cm.badSort, "a", "b")
			})

			// --- sort by leading-field PREFIX only (partial sort spec) ---
			// Sort just by the leading field's declared direction: index prefix
			// covers it; the secondary field is irrelevant to the sort key.
			t.Run("sort prefix", func(t *testing.T) {
				leadSort := cm.fwdSort[:1]
				qrmOrdered(t, idx, plain, "", leadSort, "a")
				sql := qrmExplain(t, idx.Find(nil).Sort(leadSort...))
				assert.Contains(t, sql, "IndexScan("+idxName+")")
				assert.NotContains(t, sql, "-> Sort")
			})
		})
	}
}

// ============================================================================
// MATRIX 3 — covering filter on a trailing reverse field (a,b,-c) + 3-field sort
// ============================================================================

func TestQRM_ThreeField_CoveringFilterAndSort(t *testing.T) {
	docs := make([]string, 0, 120)
	for i := 0; i < 120; i++ {
		docs = append(docs, fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%3, i%4, i%5))
	}
	idx, plain := qrmTwins(t, anystore.IndexInfo{Fields: []string{"a", "-b", "c"}}, docs)

	// (a,-b,c): forward = (a asc, b desc, c asc); reverse = (a desc, b asc, c desc).
	t.Run("sort fwd", func(t *testing.T) {
		qrmOrdered(t, idx, plain, "", []any{"a", "-b", "c"}, "a", "b", "c")
		sql := qrmExplain(t, idx.Find(nil).Sort("a", "-b", "c"))
		assert.Contains(t, sql, "IndexScan(a,-b,c)")
		assert.NotContains(t, sql, "(reverse)")
		assert.NotContains(t, sql, "-> Sort")
	})
	t.Run("sort rev", func(t *testing.T) {
		qrmOrdered(t, idx, plain, "", []any{"-a", "b", "-c"}, "a", "b", "c")
		sql := qrmExplain(t, idx.Find(nil).Sort("-a", "b", "-c"))
		assert.Contains(t, sql, "IndexScan(a,-b,c)")
		assert.Contains(t, sql, "(reverse)")
		assert.NotContains(t, sql, "-> Sort")
	})
	t.Run("sort bad", func(t *testing.T) {
		sql := qrmExplain(t, idx.Find(nil).Sort("a", "b", "c"))
		assert.Contains(t, sql, "-> Sort")
		qrmOrdered(t, idx, plain, "", []any{"a", "b", "c"}, "a", "b", "c")
	})

	// Covering filter: {a,c} with b unbound. c lies BEYOND the contiguous bound
	// prefix (BoundFields=1), so it is enforced by an IndexFilter on the reverse
	// -b... no: -b is the MIDDLE field. Here c is the trailing field; with a pinned
	// and b unbound, c is checked by a covering IndexFilter on the (forward) c.
	// To exercise covering filter on the REVERSE field, query {a,b} with the
	// trailing c unbound -> nothing beyond prefix; instead query {a,c} so b
	// (reverse, middle) sits beyond the a-prefix and is covered.
	t.Run("covering_filter_reverse_middle", func(t *testing.T) {
		f := `{"a":2,"c":0}`
		sql := qrmExplain(t, idx.Find(f).Sort("a"))
		assert.Contains(t, sql, "IndexFilter", "b/c beyond a-prefix must be a covering IndexFilter")
		got := qrmIDSet(t, idx.Find(f))
		assert.Equal(t, qrmIDSet(t, plain.Find(f)), got)
		assert.NotEmpty(t, got)
	})

	// Fully-bound equality on all three fields: c and -b are part of the tuple
	// BOUNDS (inverted -b segment), not a covering filter.
	t.Run("fully_bound", func(t *testing.T) {
		qrmSet(t, idx, plain, `{"a":2,"b":1,"c":0}`)
	})
}

// A dedicated covering-filter test where the COVERED field is the reverse one,
// so coveringFilterFields must invert MatchValue. Index (a, -b): query {a==k}
// pins a; if we ALSO equality-filter b it becomes a 2-field bound, so to force a
// covering filter on -b we need b beyond a non-contiguous prefix. Use (a, -b, c)
// with {a==2, b==1} -> a contiguous, b is field 1 (covered by bounds), c
// unbound. That doesn't isolate -b either. The clean isolation: index (a, c, -b)
// with {a==2, b==1} -> a bound, c unbound so b (field 2, reverse) is BEYOND the
// a-prefix and checked by IndexFilter with an inverted MatchValue.
func TestQRM_CoveringFilter_OnReverseField_InvertedMatchValue(t *testing.T) {
	docs := make([]string, 0, 120)
	for i := 0; i < 120; i++ {
		docs = append(docs, fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%3, i%4, i%5))
	}
	idx, plain := qrmTwins(t, anystore.IndexInfo{Fields: []string{"a", "c", "-b"}}, docs)

	f := `{"a":2,"b":1}`
	sql := qrmExplain(t, idx.Find(f).Sort("a", "c"))
	assert.Contains(t, sql, "IndexFilter", "reverse -b beyond a-prefix must use covering IndexFilter")
	// Parity proves the INVERTED MatchValue matches the inverted stored -b bytes.
	got := qrmIDSet(t, idx.Find(f))
	assert.Equal(t, qrmIDSet(t, plain.Find(f)), got, "covering filter on reverse field")
	assert.NotEmpty(t, got, "covering filter must not drop all rows")
	// And the negative: a b value that exists but not paired -> non-empty only
	// where it actually matches; compare to twin for several b values.
	for b := 0; b < 4; b++ {
		ff := fmt.Sprintf(`{"a":2,"b":%d}`, b)
		assert.Equal(t, qrmIDSet(t, plain.Find(ff)), qrmIDSet(t, idx.Find(ff)), "b=%d", b)
	}
}

// ============================================================================
// MATRIX 4 — maintenance (insert / update / delete) on reverse indexes
// ============================================================================

func TestQRM_Maintenance_InsertUpdateDelete(t *testing.T) {
	run := func(t *testing.T, index anystore.IndexInfo, fwdSort string, mk, upd func(i int) string) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, index))

		// Unindexed twin tracked in parallel for set parity after each mutation.
		fxN := newFixture(t)
		plain, err := fxN.CreateCollection(ctx, "c")
		require.NoError(t, err)

		ins := func(j string) {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(j)))
			require.NoError(t, plain.Insert(ctx, anyenc.MustParseJson(j)))
		}
		for i := 0; i < 10; i++ {
			ins(mk(i))
		}
		assertIndexLen(t, coll.GetIndexes()[0], 10)

		// Insert a new doc.
		ins(mk(10))
		assertIndexLen(t, coll.GetIndexes()[0], 11)
		assert.Equal(t, qrmIDSet(t, plain.Find(nil)), qrmIDSet(t, coll.Find(nil)))

		// Update the reverse field of a doc: old index entry removed, new added.
		uj := upd(3)
		require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(uj)))
		require.NoError(t, plain.UpdateOne(ctx, anyenc.MustParseJson(uj)))
		assertIndexLen(t, coll.GetIndexes()[0], 11) // count stable

		// Delete a doc.
		require.NoError(t, coll.DeleteId(ctx, 5))
		require.NoError(t, plain.DeleteId(ctx, 5))
		assertIndexLen(t, coll.GetIndexes()[0], 10)

		// After all mutations: ordered parity under the index's declared direction
		// (proves both the removed old entry AND the new entry are correct).
		assert.Equal(t, qrmIDs(t, plain.Find(nil).Sort(fwdSort)), qrmIDs(t, coll.Find(nil).Sort(fwdSort)))
		// Deleted doc gone.
		for _, id := range qrmIDs(t, coll.Find(nil)) {
			assert.NotEqual(t, 5, id, "deleted doc must not appear")
		}
	}

	t.Run("single_desc", func(t *testing.T) {
		run(t, anystore.IndexInfo{Fields: []string{"-a"}}, "-a",
			func(i int) string { return fmt.Sprintf(`{"id":%d,"a":%d}`, i, i) },
			func(i int) string { return fmt.Sprintf(`{"id":%d,"a":%d}`, i, 1000+i) })
	})
	t.Run("compound_asc_desc", func(t *testing.T) {
		run(t, anystore.IndexInfo{Fields: []string{"a", "-b"}}, "-b",
			func(i int) string { return fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%2, i) },
			func(i int) string { return fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%2, 1000+i) })
	})
	t.Run("compound_desc_asc", func(t *testing.T) {
		run(t, anystore.IndexInfo{Fields: []string{"-a", "b"}}, "-a",
			func(i int) string { return fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i%2) },
			func(i int) string { return fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, 1000+i, i%2) })
	})
}

// ============================================================================
// MATRIX 5 — unique reverse indexes: conflict, point lookup, partial prefix
// ============================================================================

func TestQRM_Unique_ReverseIndexes(t *testing.T) {
	t.Run("single_desc_conflict", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"-k"}, Unique: true}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"k":5}`)))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"k":9}`)))
		// Inversion is a bijection, so the unique seek still detects the dup.
		require.ErrorIs(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"k":5}`)), anystore.ErrUniqueConstraint)
		assertIndexLen(t, coll.GetIndexes()[0], 2)
		// Updating to a free value succeeds; to a taken value conflicts.
		require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"k":7}`)))
		require.ErrorIs(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"k":9}`)), anystore.ErrUniqueConstraint)
	})

	t.Run("compound_asc_desc_point_lookup", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "-b"}, Unique: true}))
		for i := 0; i < 10; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i*2))))
		}
		// Point lookup via CoverIter + inverted equality bound on -b.
		assert.Equal(t, []int{3}, qrmIDs(t, coll.Find(`{"a":3,"b":6}`)))
		assert.Empty(t, qrmIDs(t, coll.Find(`{"a":3,"b":7}`)), "non-matching trailing value")
		// Duplicate (a,b) pair rejected.
		require.ErrorIs(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":99,"a":3,"b":6}`)), anystore.ErrUniqueConstraint)
	})

	t.Run("compound_partial_prefix_parity", func(t *testing.T) {
		docs := make([]string, 0, 12)
		for i := 0; i < 12; i++ {
			docs = append(docs, fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%3, i))
		}
		idx, plain := qrmTwins(t, anystore.IndexInfo{Fields: []string{"a", "-b"}, Unique: true}, docs)
		// Partial-prefix range on a unique index: BoundFields<len path +
		// AdjustBoundsForNonUnique with an inverted trailing field.
		qrmSet(t, idx, plain, `{"a":1}`)
		qrmOrdered(t, idx, plain, `{"a":1}`, []any{"a", "-b"}, "a", "b")
		qrmOrdered(t, idx, plain, `{"a":1}`, []any{"-a", "b"}, "a", "b")
	})
}

// ============================================================================
// MATRIX 6 — sparse / null on reverse indexes
// ============================================================================

func TestQRM_SparseAndNull_ReverseIndexes(t *testing.T) {
	t.Run("single_sparse_excludes_missing", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"-b"}, Sparse: true}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"b":1}`)))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"b":2}`)))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":9}`)))    // no b
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"b":null}`))) // null b
		assertIndexLen(t, coll.GetIndexes()[0], 3)                                      // missing excluded, null kept
	})

	t.Run("compound_nonsparse_inverted_null", func(t *testing.T) {
		// Distinct b values + exactly one missing-b doc, so every sort key is
		// unique and ordered parity is unambiguous. (A missing field and an
		// explicit null both marshal to TypeNull — i.e. an identical sort key —
		// so two such docs would be a genuine tie whose relative order legitimately
		// differs between an index scan and an in-memory sort; that is asserted by
		// SET parity in the second dataset below, not by ordered parity.)
		docs := []string{
			`{"id":1,"a":1,"b":5}`,
			`{"id":2,"a":1}`, // b missing -> inverted-null (0xFE) segment, unique sort key
			`{"id":3,"a":1,"b":2}`,
			`{"id":4,"a":1,"b":9}`,
		}
		idx, plain := qrmTwins(t, anystore.IndexInfo{Fields: []string{"a", "-b"}}, docs)
		// extractDocId must skip the inverted-null b segment (length-skip parser).
		qrmSet(t, idx, plain, `{"a":1}`)
		// Sort(a,-b): the missing-b doc sits at the descending-b (null) end. With
		// distinct b values the order is well-defined, so ordered parity proves the
		// inverted-null segment sorts at the correct position.
		qrmOrdered(t, idx, plain, `{"a":1}`, []any{"a", "-b"}, "a", "b")
		qrmOrdered(t, idx, plain, `{"a":1}`, []any{"-a", "b"}, "a", "b")

		// Explicit-null + missing parity (these two share a sort key, so only SET
		// parity is meaningful). Equality on null b matches both (inverted-null bound).
		docs2 := []string{
			`{"id":1,"a":1,"b":5}`,
			`{"id":2,"a":1}`,          // missing
			`{"id":3,"a":1,"b":null}`, // explicit null
		}
		idx2, plain2 := qrmTwins(t, anystore.IndexInfo{Fields: []string{"a", "-b"}}, docs2)
		qrmSet(t, idx2, plain2, `{"a":1}`)
		qrmSet(t, idx2, plain2, `{"a":1,"b":null}`)
	})
}

// ============================================================================
// MATRIX 7 — array multikey reverse: single-field dedup/probe + compound
// ============================================================================

func TestQRM_Array_SingleField_Reverse(t *testing.T) {
	docs := []string{
		`{"id":1,"tags":[1,2,3]}`,
		`{"id":2,"tags":[3,4]}`,
		`{"id":3,"tags":[5]}`,
		`{"id":4,"tags":[1,5]}`,
		`{"id":5,"tags":[]}`,
	}
	idx, plain := qrmTwins(t, anystore.IndexInfo{Fields: []string{"-tags"}}, docs)

	// Element equality parity.
	for _, f := range []string{`{"tags":1}`, `{"tags":3}`, `{"tags":5}`, `{"tags":9}`} {
		t.Run("eq "+f, func(t *testing.T) { qrmSet(t, idx, plain, f) })
	}

	// Count over a full scan must equal distinct docs (STEP 9 inverted-array-tag
	// probe), NOT the array-element entry count.
	t.Run("count_no_overcount", func(t *testing.T) {
		assert.Equal(t, qrmCount(t, plain.Find(nil)), qrmCount(t, idx.Find(nil)))
		assert.Equal(t, 5, qrmCount(t, idx.Find(nil)))
	})

	// $in straddling: doc 4 has both 1 and 5 -> counted once (STEP 8 dedup).
	t.Run("in_straddle_once", func(t *testing.T) {
		f := `{"tags":{"$in":[1,5]}}`
		assert.Equal(t, qrmCount(t, plain.Find(f)), qrmCount(t, idx.Find(f)))
		assert.Equal(t, qrmIDSet(t, plain.Find(f)), qrmIDSet(t, idx.Find(f)))
	})

	// Filtered Sort drives the index path -> CanonicalKeyDedupIter w/ FieldReverse.
	t.Run("filtered_sort_index_dedup", func(t *testing.T) {
		f := `{"tags":{"$gte":1}}`
		sql := qrmExplain(t, idx.Find(f).Sort("-tags"))
		assert.Contains(t, sql, "IndexScan(-tags)")
		assert.Contains(t, sql, "Dedup(canonical)")
		assert.NotContains(t, sql, "(reverse)") // declared direction
		assert.NotContains(t, sql, "-> Sort")
		// Set parity both directions (canonical-element tie-break differs from the
		// in-memory sort, so SET — not exact order — is the invariant).
		assert.Equal(t, qrmIDSet(t, plain.Find(f).Sort("-tags")), qrmIDSet(t, idx.Find(f).Sort("-tags")))
		assert.Equal(t, qrmIDSet(t, plain.Find(f).Sort("tags")), qrmIDSet(t, idx.Find(f).Sort("tags")))
	})

	// Deterministic-order check: disjoint per-doc tag ranges => unique canonical
	// element per doc => index scan and in-memory sort agree on exact order.
	t.Run("filtered_sort_disjoint_ordered", func(t *testing.T) {
		d := []string{`{"id":10,"tags":[1,2]}`, `{"id":20,"tags":[3,4]}`, `{"id":30,"tags":[5,6]}`}
		uIdx, uPlain := qrmTwins(t, anystore.IndexInfo{Fields: []string{"-tags"}}, d)
		f := `{"tags":{"$gte":1}}`
		assert.Equal(t, qrmIDs(t, uPlain.Find(f).Sort("-tags")), qrmIDs(t, uIdx.Find(f).Sort("-tags")))
		assert.Equal(t, qrmIDs(t, uPlain.Find(f).Sort("tags")), qrmIDs(t, uIdx.Find(f).Sort("tags")))
	})

	// Pure Sort over a single-field array index uses the INDEX path
	// (IndexScan -> Fetch -> Dedup(canonical)), which sorts each doc by its
	// CANONICAL in-bounds element (min for asc, max for desc). The unindexed twin
	// uses FullScan -> Sort, which sorts by the WHOLE-array encoded key. For a
	// multi-key field these two definitions legitimately disagree on doc ORDER
	// (and on where an empty-array doc lands). This divergence is PRE-EXISTING and
	// reproduces identically on a FORWARD (tags) index, so it is not introduced by
	// reverse storage. The meaningful invariants are therefore: (1) the SET of docs
	// matches, and (2) the index path emits each doc EXACTLY ONCE (no dup, no drop).
	t.Run("pure_sort_set_and_single_emission", func(t *testing.T) {
		for _, s := range []string{"-tags", "tags"} {
			idxIDs := qrmIDs(t, idx.Find(nil).Sort(s))
			assert.Equal(t, qrmIDSet(t, plain.Find(nil).Sort(s)), qrmIDSet(t, idx.Find(nil).Sort(s)), "set %s", s)
			// Exactly-once: deduped length equals raw length.
			seen := map[int]bool{}
			for _, id := range idxIDs {
				assert.False(t, seen[id], "doc %d emitted twice under Sort(%s)", id, s)
				seen[id] = true
			}
			assert.Len(t, idxIDs, 5, "all docs (incl. empty-array) emitted once under Sort(%s)", s)
		}
	})

	// Maintenance on an array reverse index: shrink an array, ensure no stale
	// entries (set parity after update).
	t.Run("array_update_maintenance", func(t *testing.T) {
		require.NoError(t, idx.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":[7]}`)))
		require.NoError(t, plain.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":[7]}`)))
		assert.Equal(t, qrmIDSet(t, plain.Find(`{"tags":7}`)), qrmIDSet(t, idx.Find(`{"tags":7}`)))
		assert.Equal(t, qrmIDSet(t, plain.Find(`{"tags":1}`)), qrmIDSet(t, idx.Find(`{"tags":1}`))) // doc1 gone from tag 1
	})
}

func TestQRM_Array_Compound_ReverseTrailing(t *testing.T) {
	docs := []string{
		`{"id":1,"a":1,"tags":[1,2]}`,
		`{"id":2,"a":1,"tags":[2,3]}`,
		`{"id":3,"a":2,"tags":[1]}`,
		`{"id":4,"a":2,"tags":[4,5]}`,
	}
	idx, plain := qrmTwins(t, anystore.IndexInfo{Fields: []string{"a", "-tags"}}, docs)

	// Iter set parity for every filter shape (consumer-side DocDedup via multiKey).
	for _, f := range []string{`{"a":1,"tags":2}`, `{"a":1}`, `{"tags":1}`, `{"a":2,"tags":5}`} {
		t.Run("iter "+f, func(t *testing.T) {
			assert.Equal(t, qrmIDSet(t, plain.Find(f)), qrmIDSet(t, idx.Find(f)), "iter set %s", f)
		})
	}

	// Count parity only where the array field is CONSTRAINED (compound-multikey
	// Count over a partial prefix that leaves the array unconstrained over-counts —
	// a pre-existing limitation reproducing on a forward (a,tags) index).
	for _, f := range []string{`{"a":1,"tags":2}`, `{"tags":1}`, `{"a":2,"tags":5}`} {
		t.Run("count "+f, func(t *testing.T) {
			assert.Equal(t, qrmCount(t, plain.Find(f)), qrmCount(t, idx.Find(f)), "count %s", f)
		})
	}
}

// ============================================================================
// MATRIX 8 — CountOnly verify chain over a reverse single-field index (STEP 10)
// ============================================================================

func TestQRM_CountOnly_VerifyChain_ReverseIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"-b"}})) // reverse verify index

	fxN := newFixture(t)
	plain, err := fxN.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 60; i++ {
		d := fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%4, i%5)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
		require.NoError(t, plain.Insert(ctx, anyenc.MustParseJson(d)))
	}
	// Count on {a==k, b==j}: a covered by index (a); b verified against (-b) with
	// an inverted prefix (buildVerifyChain). Parity proves the inverted prefix.
	for _, f := range []string{`{"a":1,"b":0}`, `{"a":2,"b":3}`, `{"a":0,"b":4}`} {
		assert.Equal(t, qrmCount(t, plain.Find(f)), qrmCount(t, coll.Find(f)), "count %s", f)
	}
}

// ============================================================================
// MATRIX 9 — migration / backfill: rebuild inverts; backfill matches twin
// ============================================================================

func TestQRM_Migration_BackfillInverts(t *testing.T) {
	t.Run("drop_recreate_rebuilds_inverted", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"-a"}}))
		for i := 0; i < 8; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
		}
		require.NoError(t, coll.DropIndex(ctx, "-a"))
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"-a"}}))
		assertIndexLen(t, coll.GetIndexes()[0], 8)
		assert.Equal(t, []int{7, 6, 5, 4, 3, 2, 1, 0}, collectIntField(t, coll.Find(nil).Sort("-a"), "a"))
		sql := qrmExplain(t, coll.Find(nil).Sort("-a"))
		assert.Contains(t, sql, "IndexScan(-a)")
		assert.NotContains(t, sql, "(reverse)")
		assert.NotContains(t, sql, "-> Sort")
	})

	t.Run("backfill_on_existing_data", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		fxN := newFixture(t)
		plain, err := fxN.CreateCollection(ctx, "c")
		require.NoError(t, err)
		// Insert BEFORE the index exists, so EnsureIndex backfills via writeValues.
		for i := 0; i < 12; i++ {
			d := fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%3, i)
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
			require.NoError(t, plain.Insert(ctx, anyenc.MustParseJson(d)))
		}
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "-b"}}))
		assert.Equal(t, qrmIDs(t, plain.Find(nil).Sort("a", "-b")), qrmIDs(t, coll.Find(nil).Sort("a", "-b")))
		sql := qrmExplain(t, coll.Find(nil).Sort("a", "-b"))
		assert.Contains(t, sql, "IndexScan(a,-b)")
		assert.NotContains(t, sql, "-> Sort")
	})
}

/*
Regression tests for correctness bugs found by the qplanner audit
(see any-store-tests:docs/any-store/qplanner/CORRECTIONS.md). Each test failed before its fix and
passes after. Fixes live in internal/qplanner/planner.go.

  bug-01  Mixed-direction compound index (a,-b) is stored with reverse-flagged
          fields bitwise-inverted, so a single forward scan yields (a asc, b desc)
          and a reverse scan (a desc, b asc). Sort(a,-b) / Sort(-a,b) are served by
          a FAST index scan with NO in-memory Sort; only genuinely-unrealizable
          orders (e.g. Sort(a,b) on an (a,-b) index) fall back to in-memory sort.
  bug-01b A reverse-declared covering filter field is stored inverted, so
          coveringFilterFields bitwise-inverts the match value to match the stored
          key bytes; IndexFilterIter then compares correctly and drops no rows.
  bug-02  Skip-middle compound Count over-counted: indexCoversFilter gated on ALL
          index fields instead of the contiguously-bounded prefix, so a trailing
          equality outside the bound prefix was ignored by the covering count.
  bug-03  Offset without Limit on the in-memory sort path returned zero rows:
          SortIter's bounded heap was sized to Offset and LimitIter then skipped
          exactly those rows.
*/

// auditABPairs collects "a,b" projection of a query result, in result order.
func auditABPairs(t testing.TB, c anystore.Collection, sort ...any) []string {
	t.Helper()
	iter, err := c.Find(nil).Sort(sort...).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []string
	for iter.Next() {
		d, err := iter.Doc()
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("%d,%d", d.Value().GetInt("a"), d.Value().GetInt("b")))
	}
	require.NoError(t, iter.Err())
	return out
}

func auditIntField(t testing.TB, q anystore.Query, field string) []int {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []int
	for iter.Next() {
		d, err := iter.Doc()
		require.NoError(t, err)
		out = append(out, d.Value().GetInt(field))
	}
	require.NoError(t, iter.Err())
	return out
}

// bug-01: a mixed-direction compound index (a,-b) is stored inverted so a single
// forward scan realizes (a asc, b desc). Sort(a,-b) and Sort(-a,b) must be served
// by a FAST index scan (no in-memory Sort), matching the unindexed twin's order.
// Sort(a,b) is genuinely unrealizable by one scan and falls back to in-memory sort.
func TestIndex_Audit_MixedDirCompound_CorrectOrder(t *testing.T) {
	mk := func(withIndex bool) anystore.Collection {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "-b"}}))
		}
		for i := 0; i < 12; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%3, i%4))))
		}
		return coll
	}
	idx, noidx := mk(true), mk(false)
	assert.Equal(t, auditABPairs(t, noidx, "a", "-b"), auditABPairs(t, idx, "a", "-b"), "Sort(a,-b)")
	assert.Equal(t, auditABPairs(t, noidx, "-a", "b"), auditABPairs(t, idx, "-a", "b"), "Sort(-a,b)")
	assert.Equal(t, auditABPairs(t, noidx, "a", "b"), auditABPairs(t, idx, "a", "b"), "Sort(a,b)")

	// Sort(a,-b) is served by a FORWARD index scan: no in-memory Sort.
	exFwd, err := idx.Find(nil).Sort("a", "-b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exFwd.Sql, "IndexScan(a,-b)")
	assert.NotContains(t, exFwd.Sql, "(reverse)")
	assert.NotContains(t, exFwd.Sql, "-> Sort")

	// Sort(-a,b) is the exact opposite: a REVERSE index scan, still no Sort.
	exRev, err := idx.Find(nil).Sort("-a", "b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exRev.Sql, "IndexScan(a,-b)")
	assert.Contains(t, exRev.Sql, "(reverse)")
	assert.NotContains(t, exRev.Sql, "-> Sort")

	// Sort(a,b) cannot be realized by a single scan direction → in-memory Sort.
	exMixed, err := idx.Find(nil).Sort("a", "b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exMixed.Sql, "-> Sort", "Sort(a,b) on (a,-b) must fall back to in-memory sort")

	// Explicit within-group check for Sort(a,-b): b strictly tracked descending.
	prevA, prevB, first := -1, -1, true
	for _, p := range auditABPairs(t, idx, "a", "-b") {
		var a, b int
		_, _ = fmt.Sscanf(p, "%d,%d", &a, &b)
		if !first && a == prevA {
			assert.LessOrEqual(t, b, prevB, "b must be descending within a=%d", a)
		}
		prevA, prevB, first = a, b, false
	}
}

// bug-01b: a reverse-declared covering filter field must not drop rows.
func TestIndex_Audit_ReverseCoveringFilter_NotDropped(t *testing.T) {
	mk := func(withIndex bool) anystore.Collection {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b", "-c"}}))
		}
		for i := 0; i < 60; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%3, i%4, i%5))))
		}
		return coll
	}
	idx, noidx := mk(true), mk(false)
	const q = `{"a":2,"c":0}`
	explain, err := idx.Find(q).Sort("a", "b").Explain(ctx)
	require.NoError(t, err)
	// Confirm the plan actually exercises the IndexFilter covering path.
	assert.Contains(t, explain.Sql, "IndexFilter")

	collect := func(c anystore.Collection) []string {
		iter, err := c.Find(q).Sort("a", "b").Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()
		var out []string
		for iter.Next() {
			d, derr := iter.Doc()
			require.NoError(t, derr)
			out = append(out, fmt.Sprintf("%d,%d,%d",
				d.Value().GetInt("a"), d.Value().GetInt("b"), d.Value().GetInt("c")))
		}
		require.NoError(t, iter.Err())
		return out
	}
	got := collect(idx)
	assert.Equal(t, collect(noidx), got, "indexed reverse-covering-filter result must match unindexed")
	assert.NotEmpty(t, got, "covering filter must not drop all rows")
}

// bug-02: skip-middle compound Count must not over-count a trailing equality.
func TestIndex_Audit_SkipMiddleCount_NoOvercount(t *testing.T) {
	mk := func(withIndex bool) anystore.Collection {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b", "c"}}))
		}
		for i := 0; i < 200; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%5, i%4, i%3))))
		}
		return coll
	}
	idx, noidx := mk(true), mk(false)

	for _, q := range []string{`{"a":1,"c":0}`, `{"a":2,"c":0}`, `{"a":1,"b":2,"c":0}`, `{"a":1,"b":2}`} {
		want, err := noidx.Find(q).Count(ctx)
		require.NoError(t, err)
		gotCount, err := idx.Find(q).Count(ctx)
		require.NoError(t, err)
		iterLen := len(auditIntField(t, idx.Find(q), "id"))
		assert.Equal(t, want, gotCount, "Count must match unindexed for %s", q)
		assert.Equal(t, iterLen, gotCount, "Count must equal Iter length for %s", q)
	}
	// Concrete sanity: a=1,c=0 over 200 docs (a=i%5,c=i%3) → i%15==6 → 13 docs.
	cnt, err := idx.Find(`{"a":1,"c":0}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 13, cnt)
}

// bug-03: offset without limit on the in-memory sort path must return the tail.
func TestIndex_Audit_OffsetWithoutLimit_InMemorySort(t *testing.T) {
	t.Run("no index, sort on field", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		for i := 0; i < 20; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
		}
		full := auditIntField(t, coll.Find(nil).Sort("a"), "a")
		require.Len(t, full, 20)
		got := auditIntField(t, coll.Find(nil).Sort("a").Offset(5), "a")
		assert.Equal(t, full[5:], got)
	})

	t.Run("index on a, sort on non-indexed field b (forces SortIter)", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
		for i := 0; i < 20; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i))))
		}
		full := auditIntField(t, coll.Find(`{"a":{"$gte":0}}`).Sort("b"), "b")
		require.Len(t, full, 20)
		got := auditIntField(t, coll.Find(`{"a":{"$gte":0}}`).Sort("b").Offset(3), "b")
		assert.Equal(t, full[3:], got)
	})
}

// baseline: a single-field reverse index (-a) stores values inverted, so a
// FORWARD scan yields descending order. Sort("-a") is therefore served by a
// forward scan (NO "(reverse)"), and Sort("a") by a reverse scan — both with no
// in-memory Sort. (Before the invert-on-write redesign, shouldReverse picked the
// wrong direction here.)
func TestIndex_Audit_SingleReverse_Baseline(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"-a"}}))
	for i := 0; i < 8; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, auditIntField(t, coll.Find(nil).Sort("a"), "a"))
	assert.Equal(t, []int{7, 6, 5, 4, 3, 2, 1, 0}, auditIntField(t, coll.Find(nil).Sort("-a"), "a"))
	assert.Equal(t, []int{3, 4, 5, 6}, auditIntField(t, coll.Find(`{"a":{"$gte":3,"$lte":6}}`).Sort("a"), "a"))

	// Sort("-a") == the index's declared direction → FORWARD scan, no Sort.
	exDesc, err := coll.Find(nil).Sort("-a").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exDesc.Sql, "IndexScan(-a)")
	assert.NotContains(t, exDesc.Sql, "(reverse)")
	assert.NotContains(t, exDesc.Sql, "-> Sort")

	// Sort("a") == opposite of declared → REVERSE scan, still no Sort.
	exAsc, err := coll.Find(nil).Sort("a").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exAsc.Sql, "IndexScan(-a)")
	assert.Contains(t, exAsc.Sql, "(reverse)")
	assert.NotContains(t, exAsc.Sql, "-> Sort")
}

// TestSparseCompoundIndex_PlannerDoesNotDropRows reproduces the alpha.13
// regression where the CBO would pick a sparse compound index for a query that
// does not constrain every indexed field. A sparse index omits documents missing
// any of its fields, so seeking it silently dropped matching rows.
//
// Layout mirrors the field report: a complete index (sp,q) is declared first and
// a sparse index (sp,as) — with `as` optional — second. The planner must not
// answer either query through the sparse index.
func TestSparseCompoundIndex_PlannerDoesNotDropRows(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx,
		anystore.IndexInfo{Name: "sp_q", Fields: []string{"sp", "q"}}))
	require.NoError(t, coll.EnsureIndex(ctx,
		anystore.IndexInfo{Name: "sp_as", Fields: []string{"sp", "as"}, Sparse: true}))

	// Two docs with sp="s": one carries `as`, one does not. The sparse (sp,as)
	// index therefore holds only one of them.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"sp":"s","q":1,"as":7}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"sp":"s","q":2}`)))

	assertSparseQueryCount(t, coll, `{"sp":"s"}`, 2)
	assertSparseQueryCount(t, coll, `{"sp":"s","as":{"$exists":false}}`, 1)
	// Sanity: the queries the sparse index CAN answer still work.
	assertSparseQueryCount(t, coll, `{"sp":"s","as":7}`, 1)
	assertSparseQueryCount(t, coll, `{"sp":"s","as":{"$exists":true}}`, 1)
}

// TestSparseSingleFieldIndex_ExistsFalse covers the single-field sparse case:
// {a:{$exists:false}} matches docs that the sparse index on `a` never stored.
func TestSparseSingleFieldIndex_ExistsFalse(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx,
		anystore.IndexInfo{Name: "a_sparse", Fields: []string{"a"}, Sparse: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3}`)))

	assertSparseQueryCount(t, coll, `{"a":{"$exists":false}}`, 2)
	assertSparseQueryCount(t, coll, `{"a":1}`, 1)
}

func assertSparseQueryCount(t *testing.T, coll anystore.Collection, cond string, want int) {
	t.Helper()
	got, err := coll.Find(cond).Count(ctx)
	require.NoError(t, err)
	require.Equalf(t, want, got, "Count for %s", cond)

	// Iter must agree with Count — the bug dropped rows from both paths.
	iter, err := coll.Find(cond).Iter(ctx)
	require.NoError(t, err)
	n := 0
	for iter.Next() {
		n++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	require.Equalf(t, want, n, "Iter for %s", cond)
}

// The v1 sparse-index row loss, as a v2 regression.
//
// In any-store v1.0.1 the query planner never read IndexInfo.Sparse, so a
// negative predicate (`!= true`) was answered by INNER JOINing the sparse index
// table — which holds only the documents that DO carry the field, the exact
// complement of what the predicate matches. Every document lacking the field
// silently disappeared. Measured there on this very fixture: 2 rows instead of
// 22, and 0 instead of 24 when no document carries the field at all.
//
// The v1 fixture: 24 docs — 20 without `isUninstalled`, 2 with `true`, 2 with
// `false`; a sparse index on `isUninstalled`; expected 22 for `$ne: true`.
// `resolvedLayout` carries a second index so the two-predicate shape from the
// issue is reproduced. Note there is no cost TIE in this fixture under v2 — the
// sparse index is gated out entirely — so these tests pin the row loss only;
// plan stability across a reopen is pinned by TestPlanner_PlanStableAcrossReopen.

type sparseNeDoc struct {
	id             int
	resolvedLayout string
	isUninstalled  *bool // nil = field absent
}

func sparseNeFixture() []sparseNeDoc {
	tru, fls := true, false
	docs := make([]sparseNeDoc, 0, 24)
	for i := range 24 {
		layout := "page"
		if i >= 12 {
			layout = "note"
		}
		d := sparseNeDoc{id: i, resolvedLayout: layout}
		switch i {
		case 20, 21:
			d.isUninstalled = &tru
		case 22, 23:
			d.isUninstalled = &fls
		}
		docs = append(docs, d)
	}
	return docs
}

func sparseNeInsert(t *testing.T, coll anystore.Collection, docs []sparseNeDoc) {
	t.Helper()
	for _, d := range docs {
		j := fmt.Sprintf(`{"id":%d,"resolvedLayout":%q`, d.id, d.resolvedLayout)
		if d.isUninstalled != nil {
			j += fmt.Sprintf(`,"isUninstalled":%t`, *d.isUninstalled)
		}
		j += "}"
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(j)))
	}
}

func sparseNeEnsureIndexes(t *testing.T, coll anystore.Collection) {
	t.Helper()
	// Both single-field, both weight-10 in v1 — a tie there, and
	// "isUninstalled" sorts before "resolvedLayout", which is why v1 broke only
	// after a reopen. Under v2 the sparse index is not a candidate at all.
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name: "isUninstalled", Fields: []string{"isUninstalled"}, Sparse: true}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name: "resolvedLayout", Fields: []string{"resolvedLayout"}}))
}

// assertSparseNotPlanned runs the query through Count and Iter — separate plan builds
// (CountOnly differs), and v1 broke both — and additionally asserts the planner
// did not answer it from a sparse index, which is the root cause as opposed to
// the row-count symptom.
func assertSparseNotPlanned(t *testing.T, coll anystore.Collection, cond string, want int) {
	t.Helper()
	assertSparseNotPlannedQuery(t, coll, cond, func() anystore.Query { return coll.Find(cond) }, want)
}

// assertSparseNotPlannedSorted is the Plan C (sort-driven) shape. It matters on its own:
// the sparse gate there is a second call site, and without it an unconstrained
// Sort over a sparse index drives the scan from an index that never stored the
// null/missing documents — Count and Iter then disagree.
func assertSparseNotPlannedSorted(t *testing.T, coll anystore.Collection, cond, sortField string, want int) {
	t.Helper()
	label := cond + " sort " + sortField
	assertSparseNotPlannedQuery(t, coll, label, func() anystore.Query { return coll.Find(cond).Sort(sortField) }, want)
}

func assertSparseNotPlannedQuery(t *testing.T, coll anystore.Collection, label string, build func() anystore.Query, want int) {
	t.Helper()

	count, err := build().Count(ctx)
	require.NoError(t, err)
	assert.Equalf(t, want, count, "Count for %s", label)

	iter, err := build().Iter(ctx)
	require.NoError(t, err)
	n := 0
	for iter.Next() {
		n++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	assert.Equalf(t, want, n, "Iter for %s", label)

	// Any sparse index, resolved from the collection — not one hard-coded name,
	// so the check keeps working for the compound and renamed fixtures.
	sparse := map[string]bool{}
	for _, idx := range coll.GetIndexes() {
		if idx.Info().Sparse {
			sparse[idx.Info().Name] = true
		}
	}
	explain, err := build().Explain(ctx)
	require.NoError(t, err)
	for _, ie := range explain.Indexes {
		if ie.Used && sparse[ie.Name] {
			t.Errorf("planner answered %s from the SPARSE index %q\n%s",
				label, ie.Name, explain.Plan)
		}
	}
}

// TestSparseIndex_NotEqualDoesNotDropRows: a negative predicate must not be
// answered from a sparse index, which lacks the documents it matches.
func TestSparseIndex_NotEqualDoesNotDropRows(t *testing.T) {
	docs := sparseNeFixture()

	t.Run("index created before insert", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		sparseNeEnsureIndexes(t, coll)
		sparseNeInsert(t, coll, docs)

		assertSparseNotPlanned(t, coll, `{"isUninstalled":{"$ne":true}}`, 22)
		assertSparseNotPlanned(t, coll, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`, 10)
		assertSparseNotPlanned(t, coll, `{"resolvedLayout":"page","isUninstalled":{"$ne":true}}`, 12)
	})

	t.Run("index created after insert", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		sparseNeInsert(t, coll, docs)
		sparseNeEnsureIndexes(t, coll)

		assertSparseNotPlanned(t, coll, `{"isUninstalled":{"$ne":true}}`, 22)
		assertSparseNotPlanned(t, coll, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`, 10)
	})

	t.Run("no index", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		sparseNeInsert(t, coll, docs)

		assertSparseNotPlanned(t, coll, `{"isUninstalled":{"$ne":true}}`, 22)
	})

	// v1: 0 rows. The heart symptom — `isUninstalled` is only ever written when
	// true, so in a real account no live object carries the field at all.
	t.Run("no document carries the field", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		sparseNeEnsureIndexes(t, coll)
		for i := range 24 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"resolvedLayout":"note"}`, i))))
		}

		assertSparseNotPlanned(t, coll, `{"isUninstalled":{"$ne":true}}`, 24)
		assertSparseNotPlanned(t, coll, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`, 24)
	})

	// Plan C: the sort-driven sparse gate is a second call site with its own
	// failure mode — an unconstrained Sort over the sparse index drives the
	// scan from an index that never stored the 20 documents missing the field,
	// and Count and Iter then disagree.
	t.Run("sort over the sparse field", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		sparseNeEnsureIndexes(t, coll)
		sparseNeInsert(t, coll, docs)

		assertSparseNotPlannedSorted(t, coll, `{}`, "isUninstalled", 24)
		assertSparseNotPlannedSorted(t, coll, `{"resolvedLayout":"note"}`, "isUninstalled", 12)
	})

	// Other predicates that can match an absent or null field, each of which
	// the sparse index would answer wrongly if the gate stopped firing.
	t.Run("other absent-matching predicates", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		sparseNeEnsureIndexes(t, coll)
		sparseNeInsert(t, coll, docs)

		assertSparseNotPlanned(t, coll, `{"isUninstalled":{"$ne":false}}`, 22) // 20 absent + 2 true
		assertSparseNotPlanned(t, coll, `{"isUninstalled":{"$ne":"x"}}`, 24)   // every doc
		assertSparseNotPlanned(t, coll, `{"isUninstalled":null}`, 20)          // absent only
		assertSparseNotPlanned(t, coll, `{"isUninstalled":{"$exists":false}}`, 20)
		// The issue measured $in:[null,false] as NOT working in v1 (In.Ok
		// returned false for an absent key). v2 answers it correctly.
		assertSparseNotPlanned(t, coll, `{"isUninstalled":{"$in":[null,false]}}`, 22)
	})

	// The other predicate shapes measured in the issue. All are "not true",
	// spelled differently; all must agree with $ne. Only $ne contributes index
	// bounds — the rest plan as a FullScan, so for them this pins row-count
	// semantics rather than the sparse gate.
	t.Run("equivalent predicate shapes", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		sparseNeEnsureIndexes(t, coll)
		sparseNeInsert(t, coll, docs)

		for _, cond := range []string{
			`{"isUninstalled":{"$ne":true}}`,
			`{"isUninstalled":{"$nin":[true]}}`,
			`{"isUninstalled":{"$not":{"$eq":true}}}`,
			`{"$nor":[{"isUninstalled":true}]}`,
			`{"$or":[{"isUninstalled":{"$exists":false}},{"isUninstalled":{"$ne":true}}]}`,
		} {
			assertSparseNotPlanned(t, coll, cond, 22)
		}
	})
}

// TestSparseIndex_NotEqualSurvivesReopen combines the row loss with plan
// instability: in v1 the query was correct in the session that created the
// index and broken in every session after, because a reopened collection
// loads its indexes in a different order and the tie-break is positional.
func TestSparseIndex_NotEqualSurvivesReopen(t *testing.T) {
	skipIfInMemory(t, "the database is closed and reopened")
	tmpDir, err := os.MkdirTemp("", "sparse-ne-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	docs := sparseNeFixture()

	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	sparseNeEnsureIndexes(t, coll)
	sparseNeInsert(t, coll, docs)
	assertSparseNotPlanned(t, coll, `{"isUninstalled":{"$ne":true}}`, 22)
	assertSparseNotPlanned(t, coll, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`, 10)
	require.NoError(t, fx1.Close())

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "objects")
	require.NoError(t, err)
	assertSparseNotPlanned(t, coll2, `{"isUninstalled":{"$ne":true}}`, 22)
	assertSparseNotPlanned(t, coll2, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`, 10)
}
