package anystore

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
)

// explainSQL returns explain.Sql for a hinted query on the "a" index.
func explainSQL(t *testing.T, coll Collection, filter string) string {
	t.Helper()
	explain, err := coll.Find(filter).
		IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}).Explain(ctx)
	require.NoError(t, err)
	return explain.Sql
}

// assertTightBounds asserts the hinted index scan carries BOTH range ends
// (no ",inf]" upper bound in its bounds string).
func assertTightBounds(t *testing.T, coll Collection, filter string) {
	t.Helper()
	sql := explainSQL(t, coll, filter)
	require.Contains(t, sql, "IndexScan(a)", "plan: %s", sql)
	assert.NotContains(t, sql, "inf]", "expected tight (two-sided) bounds, got: %s", sql)
}

// assertWideBounds asserts the hinted index scan kept the sound half-open
// over-approximation (upper bound +inf).
func assertWideBounds(t *testing.T, coll Collection, filter string) {
	t.Helper()
	sql := explainSQL(t, coll, filter)
	require.Contains(t, sql, "IndexScan(a)", "plan: %s", sql)
	assert.Contains(t, sql, "inf]", "expected wide (half-open) bounds, got: %s", sql)
}

const twoSided = `{"a":{"$gt":2,"$lt":5}}`

// TestMultikeyFlag_Lifecycle drives the persisted scalar/multikey flag through
// every transition that decides whether tight seek bounds are sound.
func TestMultikeyFlag_Lifecycle(t *testing.T) {
	newColl := func(t *testing.T, name string) Collection {
		coll, err := newFixture(t).CreateCollection(ctx, name)
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"a":1}`),
			anyenc.MustParseJson(`{"id":2,"a":3}`),
			anyenc.MustParseJson(`{"id":3,"a":7}`),
		))
		return coll
	}

	t.Run("scalar index serves tight bounds", func(t *testing.T) {
		coll := newColl(t, "c")
		assertTightBounds(t, coll, twoSided)
		assertQueryCount(t, coll.Find(twoSided), 1) // a=3
	})

	t.Run("first array write flips to wide", func(t *testing.T) {
		coll := newColl(t, "c")
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))
		assertWideBounds(t, coll, twoSided)
		// The array doc matches via different elements (6>2, 1<5) — a tight
		// seek would have dropped it.
		assertQueryCount(t, coll.Find(twoSided).IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}), 2)
	})

	t.Run("backfill over existing arrays flips", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":[6,1]}`)))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
		assertWideBounds(t, coll, twoSided)
	})

	t.Run("empty arrays do not flip", func(t *testing.T) {
		coll := newColl(t, "c")
		// A doc with an empty array produces exactly one whole-value entry —
		// single-entry docs are always sound under intersection.
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[]}`)))
		assertTightBounds(t, coll, twoSided)
		assertQueryCount(t, coll.Find(twoSided), 1)
	})

	t.Run("deleting all arrays does not clear", func(t *testing.T) {
		coll := newColl(t, "c")
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))
		require.NoError(t, coll.DeleteId(ctx, 4))
		// One-way: older snapshots may still hold the fanned-out entries.
		assertWideBounds(t, coll, twoSided)
	})

	t.Run("drop and recreate resets", func(t *testing.T) {
		coll := newColl(t, "c")
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))
		require.NoError(t, coll.DeleteId(ctx, 4))
		require.NoError(t, coll.DropIndex(ctx, "a"))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
		assertTightBounds(t, coll, twoSided)
	})

	t.Run("rename does not resurrect a stale scalar record", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "renA")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":3}`)))
		assertTightBounds(t, coll, twoSided)

		// A -> B, arrays inserted under B, then back to A: the flag record
		// must travel with the rename, or the stale scalar record written
		// under A would unsoundly re-enable tight seeks.
		require.NoError(t, coll.Rename(ctx, "renB"))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))
		assertWideBounds(t, coll, twoSided)
		require.NoError(t, coll.Rename(ctx, "renA"))
		assertWideBounds(t, coll, twoSided)
		assertQueryCount(t, coll.Find(twoSided).IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}), 2)
	})

	t.Run("absent record means wide", func(t *testing.T) {
		coll := newColl(t, "c")
		// Simulate an index built before the flag existed by deleting the
		// record out from under the collection.
		c := coll.(*collection)
		require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			return tx.Delete(c.db.systemNS, multikeyKey(c.name, "a"))
		}))
		assertWideBounds(t, coll, twoSided)
	})

	t.Run("savepoint rollback keeps record and entries consistent", func(t *testing.T) {
		coll := newColl(t, "c")
		outer, err := coll.WriteTx(ctx)
		require.NoError(t, err)
		inner, err := coll.WriteTx(outer.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(inner.Context(), anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))
		require.NoError(t, inner.Rollback())
		require.NoError(t, outer.Commit())

		// The nested rollback reverted the entries AND the flag together:
		// tight bounds stay sound.
		assertTightBounds(t, coll, twoSided)
		assertQueryCount(t, coll.Find(twoSided), 1)

		// And a subsequent real array write still flips — nothing cached a
		// stale "already flagged" state.
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":5,"a":[6,1]}`)))
		assertWideBounds(t, coll, twoSided)
	})

	t.Run("snapshot visibility", func(t *testing.T) {
		coll := newColl(t, "c")
		rt, err := coll.ReadTx(ctx)
		require.NoError(t, err)
		defer rt.Commit()

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":[6,1]}`)))

		// The old snapshot holds neither the fan-out entries nor the flag:
		// tight bounds are exactly right for it.
		explain, err := coll.Find(twoSided).
			IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}).Explain(rt.Context())
		require.NoError(t, err)
		assert.NotContains(t, explain.Sql, "inf]", "old snapshot must keep tight bounds: %s", explain.Sql)

		// A fresh snapshot sees both.
		assertWideBounds(t, coll, twoSided)
	})
}

// TestMultikeyFlag_PkTightBounds: the pk namespace needs no flag — array pks
// are rejected on write, so tight idBounds are unconditional.
func TestMultikeyFlag_PkTightBounds(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "pk")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))))
	}
	explain, err := coll.Find(`{"id":{"$gt":2,"$lt":5}}`).Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, explain.Sql, "idBounds", "plan: %s", explain.Sql)
	assert.NotContains(t, explain.Sql, "inf]", "pk range must carry both ends: %s", explain.Sql)
	assertQueryCount(t, coll.Find(`{"id":{"$gt":2,"$lt":5}}`), 2)

	// Descending pk range with limit — the seek must start at the End key,
	// not the last key of the collection.
	it, err := coll.Find(`{"id":{"$gt":2,"$lt":5}}`).Sort("-id").Limit(1).Iter(ctx)
	require.NoError(t, err)
	require.True(t, it.Next())
	d, err := it.Doc()
	require.NoError(t, err)
	assert.Equal(t, 4, d.Value().GetInt("id"))
	require.NoError(t, it.Close())
}

// TestMultikeyFlag_ChannelConsistency pins the "one bounds set per chain"
// invariant on the paths where mixing tight flags with wide execution (or
// vice versa) produces wrong rows rather than just bad plans.
func TestMultikeyFlag_ChannelConsistency(t *testing.T) {
	t.Run("unique multikey index point-collapse must not CoverIter", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "uniqmk")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}, Unique: true}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":[4,6]}`)))

		// {$gte:5,$lte:5} tight-collapses to the point [5,5]; if PointLookup
		// were computed from the tight channel while the index (multikey!)
		// executes wide — or if tight bounds leaked into the seek — the doc
		// (matching via 6>=5 and 4<=5, with no entry at 5) would vanish.
		const f = `{"a":{"$gte":5,"$lte":5}}`
		hint := IndexHint{IndexName: "a", Boost: 1_000_000}
		assertQueryCount(t, coll.Find(f).IndexHint(hint), 1)
		it, err := coll.Find(f).IndexHint(hint).Iter(ctx)
		require.NoError(t, err)
		n := 0
		for it.Next() {
			n++
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		assert.Equal(t, 1, n)
	})

	t.Run("compound multikey point-collapse must keep the SortIter", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "compmk")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "ab", Fields: []string{"a", "b"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"a":[4,6],"b":1}`),
			anyenc.MustParseJson(`{"id":2,"a":5,"b":2}`),
			anyenc.MustParseJson(`{"id":3,"a":[5,9],"b":0}`),
		))
		// All three match {$gte:5,$lte:5} under array semantics. A tight
		// equalityPrefix on a multikey index would claim ExactSort on b and
		// skip the SortIter while executing wide (a,b)-ordered bounds.
		const f = `{"a":{"$gte":5,"$lte":5}}`
		hint := IndexHint{IndexName: "ab", Boost: 1_000_000}
		it, err := coll.Find(f).IndexHint(hint).Sort("b").Iter(ctx)
		require.NoError(t, err)
		var got []int
		for it.Next() {
			d, derr := it.Doc()
			require.NoError(t, derr)
			got = append(got, d.Value().GetInt("b"))
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		assert.Equal(t, []int{0, 1, 2}, got)
	})

	t.Run("verify chain reads the wide channel of the verify index", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "verifymk")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx,
			IndexInfo{Name: "a", Fields: []string{"a"}},
			IndexInfo{Name: "b", Fields: []string{"b"}},
		))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":[2,4]}`)))

		// Candidate index a is scalar-proven; b's tight bounds collapse to
		// [3,3]. A verify probe against b's (multikey) index keyed on that
		// point would miss the doc's entries (2, 4, whole-array).
		const f = `{"a":1,"b":{"$gte":3,"$lte":3}}`
		hint := IndexHint{IndexName: "a", Boost: 1_000_000}
		assertQueryCount(t, coll.Find(f).IndexHint(hint), 1)
	})
}

// TestQuery_TightSeekDifferential is the randomized oracle: for scalar-only
// and array-bearing datasets alike, indexed queries (tight seeks where proven)
// must agree with a full in-memory evaluation of the filter.
func TestQuery_TightSeekDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	type dataset struct {
		name   string
		arrays bool
	}
	for _, ds := range []dataset{{"scalar", false}, {"arrays", true}} {
		t.Run(ds.name, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "diff")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))

			var docs []*anyenc.Value
			for i := 0; i < 300; i++ {
				var doc string
				if ds.arrays && i%3 == 0 {
					doc = fmt.Sprintf(`{"id":%d,"a":[%d,%d]}`, i, rng.Intn(50), rng.Intn(50))
				} else {
					doc = fmt.Sprintf(`{"id":%d,"a":%d}`, i, rng.Intn(50))
				}
				docs = append(docs, anyenc.MustParseJson(doc))
			}
			require.NoError(t, coll.Insert(ctx, docs...))

			var filters []string
			for i := 0; i < 40; i++ {
				lo, hi := rng.Intn(50), rng.Intn(50)
				switch i % 5 {
				case 0:
					filters = append(filters, fmt.Sprintf(`{"a":{"$gt":%d,"$lt":%d}}`, lo, hi))
				case 1:
					filters = append(filters, fmt.Sprintf(`{"a":{"$gte":%d,"$lte":%d}}`, lo, hi))
				case 2:
					filters = append(filters, fmt.Sprintf(`{"a":{"$in":[%d,%d,%d],"$gt":%d}}`, lo, hi, rng.Intn(50), rng.Intn(50)))
				case 3:
					filters = append(filters, fmt.Sprintf(`{"a":{"$gt":%d,"$lt":%d,"$ne":%d}}`, lo, hi, rng.Intn(50)))
				case 4:
					filters = append(filters, fmt.Sprintf(`{"id":{"$gt":%d,"$lt":%d}}`, lo, hi))
				}
			}

			hint := IndexHint{IndexName: "a", Boost: 1_000_000}
			for _, f := range filters {
				cond := query.MustParseCondition(f)
				var want []int
				for _, d := range docs {
					if cond.Ok(d, nil) {
						want = append(want, d.GetInt("id"))
					}
				}
				slices.Sort(want)

				for _, q := range []Query{coll.Find(f), coll.Find(f).IndexHint(hint)} {
					it, err := q.Iter(ctx)
					require.NoError(t, err)
					var got []int
					for it.Next() {
						d, derr := it.Doc()
						require.NoError(t, derr)
						got = append(got, d.Value().GetInt("id"))
					}
					require.NoError(t, it.Err())
					require.NoError(t, it.Close())
					slices.Sort(got)
					require.Equal(t, want, got, "Iter mismatch for %s", f)
				}
				assertQueryCount(t, coll.Find(f), len(want))
				assertQueryCount(t, coll.Find(f).IndexHint(hint), len(want))

				// Sorted + limited variants must return prefixes of the
				// ordered match set.
				it, err := coll.Find(f).IndexHint(hint).Sort("a").Limit(3).Iter(ctx)
				require.NoError(t, err)
				n := 0
				for it.Next() {
					n++
				}
				require.NoError(t, it.Err())
				require.NoError(t, it.Close())
				require.Equal(t, min(3, len(want)), n, "Sort+Limit count mismatch for %s", f)
			}
		})
	}
}

// TestMultikeyFlag_ExplainShowsBothEnds is the explain-level BUG-02-shaped
// acceptance: both bounds ends survive to the executed plan for the pk and a
// scalar secondary index.
func TestMultikeyFlag_ExplainShowsBothEnds(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "ends")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	for i := 0; i < 20; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}

	sql := explainSQL(t, coll, `{"a":{"$gt":5,"$lt":9}}`)
	require.Contains(t, sql, "IndexScan(a)")
	require.False(t, strings.Contains(sql, "inf"), "secondary-index bounds must be two-sided: %s", sql)

	explain, err := coll.Find(`{"id":{"$gt":5,"$lt":9}}`).Sort("-id").Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, explain.Sql, "idBounds", "plan: %s", explain.Sql)
	require.False(t, strings.Contains(explain.Sql, "inf"), "pk bounds must be two-sided: %s", explain.Sql)
}
