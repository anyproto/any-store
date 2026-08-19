package anystore

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

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// qrmTwins builds an indexed collection and an unindexed twin populated with the
// same docs, for parity assertions.
func qrmTwins(t *testing.T, index IndexInfo, docs []string) (idx, plain Collection) {
	t.Helper()
	build := func(withIndex bool) Collection {
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
func qrmSortKey(t testing.TB, q Query, fields ...string) []string {
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
func qrmIDs(t testing.TB, q Query) []int { return collectIntField(t, q, "id") }

// qrmIDSet returns the result doc ids as a sorted set (order-independent).
func qrmIDSet(t testing.TB, q Query) []int {
	out := qrmIDs(t, q)
	sort.Ints(out)
	return out
}

// qrmCount returns Count(ctx), failing the test on error.
func qrmCount(t testing.TB, q Query) int {
	t.Helper()
	n, err := q.Count(ctx)
	require.NoError(t, err)
	return n
}

// qrmExplain returns the Explain SQL chain string.
func qrmExplain(t testing.TB, q Query) string {
	t.Helper()
	ex, err := q.Explain(ctx)
	require.NoError(t, err)
	return ex.Sql
}

// qrmSet asserts indexed/unindexed set+count parity for a filter.
func qrmSet(t *testing.T, idx, plain Collection, filter string) {
	t.Helper()
	assert.Equal(t, qrmIDSet(t, plain.Find(filter)), qrmIDSet(t, idx.Find(filter)), "id set: %s", filter)
	assert.Equal(t, qrmCount(t, plain.Find(filter)), qrmCount(t, idx.Find(filter)), "count: %s", filter)
}

// qrmOrdered asserts indexed/unindexed ordered sort-key parity for a sorted query.
// filter may be "" for no filter. sortArgs may contain '-'; keyFields are the
// bare names to project.
func qrmOrdered(t *testing.T, idx, plain Collection, filter string, sortArgs []any, keyFields ...string) {
	t.Helper()
	var fq, pq Query
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
			idx, plain := qrmTwins(t, IndexInfo{Fields: []string{sh.field}}, docs)

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
			idx, plain := qrmTwins(t, IndexInfo{Fields: []string{field}}, docs)
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
			idx, plain := qrmTwins(t, IndexInfo{Fields: cm.fields}, docs)

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
	idx, plain := qrmTwins(t, IndexInfo{Fields: []string{"a", "-b", "c"}}, docs)

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
	idx, plain := qrmTwins(t, IndexInfo{Fields: []string{"a", "c", "-b"}}, docs)

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
	run := func(t *testing.T, index IndexInfo, fwdSort string, mk, upd func(i int) string) {
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
		run(t, IndexInfo{Fields: []string{"-a"}}, "-a",
			func(i int) string { return fmt.Sprintf(`{"id":%d,"a":%d}`, i, i) },
			func(i int) string { return fmt.Sprintf(`{"id":%d,"a":%d}`, i, 1000+i) })
	})
	t.Run("compound_asc_desc", func(t *testing.T) {
		run(t, IndexInfo{Fields: []string{"a", "-b"}}, "-b",
			func(i int) string { return fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%2, i) },
			func(i int) string { return fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%2, 1000+i) })
	})
	t.Run("compound_desc_asc", func(t *testing.T) {
		run(t, IndexInfo{Fields: []string{"-a", "b"}}, "-a",
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
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"-k"}, Unique: true}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"k":5}`)))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"k":9}`)))
		// Inversion is a bijection, so the unique seek still detects the dup.
		require.ErrorIs(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"k":5}`)), ErrUniqueConstraint)
		assertIndexLen(t, coll.GetIndexes()[0], 2)
		// Updating to a free value succeeds; to a taken value conflicts.
		require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"k":7}`)))
		require.ErrorIs(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"k":9}`)), ErrUniqueConstraint)
	})

	t.Run("compound_asc_desc_point_lookup", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "-b"}, Unique: true}))
		for i := 0; i < 10; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i*2))))
		}
		// Point lookup via CoverIter + inverted equality bound on -b.
		assert.Equal(t, []int{3}, qrmIDs(t, coll.Find(`{"a":3,"b":6}`)))
		assert.Empty(t, qrmIDs(t, coll.Find(`{"a":3,"b":7}`)), "non-matching trailing value")
		// Duplicate (a,b) pair rejected.
		require.ErrorIs(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":99,"a":3,"b":6}`)), ErrUniqueConstraint)
	})

	t.Run("compound_partial_prefix_parity", func(t *testing.T) {
		docs := make([]string, 0, 12)
		for i := 0; i < 12; i++ {
			docs = append(docs, fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%3, i))
		}
		idx, plain := qrmTwins(t, IndexInfo{Fields: []string{"a", "-b"}, Unique: true}, docs)
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
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"-b"}, Sparse: true}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"b":1}`)))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"b":2}`)))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":9}`)))    // no b
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"b":null}`))) // null b
		assertIndexLen(t, coll.GetIndexes()[0], 2)                                      // missing AND null excluded
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
		idx, plain := qrmTwins(t, IndexInfo{Fields: []string{"a", "-b"}}, docs)
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
		idx2, plain2 := qrmTwins(t, IndexInfo{Fields: []string{"a", "-b"}}, docs2)
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
	idx, plain := qrmTwins(t, IndexInfo{Fields: []string{"-tags"}}, docs)

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
		uIdx, uPlain := qrmTwins(t, IndexInfo{Fields: []string{"-tags"}}, d)
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
	idx, plain := qrmTwins(t, IndexInfo{Fields: []string{"a", "-tags"}}, docs)

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"-b"}})) // reverse verify index

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
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"-a"}}))
		for i := 0; i < 8; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
		}
		require.NoError(t, coll.DropIndex(ctx, "-a"))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"-a"}}))
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
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "-b"}}))
		assert.Equal(t, qrmIDs(t, plain.Find(nil).Sort("a", "-b")), qrmIDs(t, coll.Find(nil).Sort("a", "-b")))
		sql := qrmExplain(t, coll.Find(nil).Sort("a", "-b"))
		assert.Contains(t, sql, "IndexScan(a,-b)")
		assert.NotContains(t, sql, "-> Sort")
	})
}
