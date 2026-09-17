package anystore

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

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
		IndexInfo{Name: "sp_q", Fields: []string{"sp", "q"}}))
	require.NoError(t, coll.EnsureIndex(ctx,
		IndexInfo{Name: "sp_as", Fields: []string{"sp", "as"}, Sparse: true}))

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
		IndexInfo{Name: "a_sparse", Fields: []string{"a"}, Sparse: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3}`)))

	assertSparseQueryCount(t, coll, `{"a":{"$exists":false}}`, 2)
	assertSparseQueryCount(t, coll, `{"a":1}`, 1)
}

func assertSparseQueryCount(t *testing.T, coll Collection, cond string, want int) {
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

func sparseNeInsert(t *testing.T, coll Collection, docs []sparseNeDoc) {
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

func sparseNeEnsureIndexes(t *testing.T, coll Collection) {
	t.Helper()
	// Both single-field, both weight-10 in v1 — a tie there, and
	// "isUninstalled" sorts before "resolvedLayout", which is why v1 broke only
	// after a reopen. Under v2 the sparse index is not a candidate at all.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "isUninstalled", Fields: []string{"isUninstalled"}, Sparse: true}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "resolvedLayout", Fields: []string{"resolvedLayout"}}))
}

// assertSparseNotPlanned runs the query through Count and Iter — separate plan builds
// (CountOnly differs), and v1 broke both — and additionally asserts the planner
// did not answer it from a sparse index, which is the root cause as opposed to
// the row-count symptom.
func assertSparseNotPlanned(t *testing.T, coll Collection, cond string, want int) {
	t.Helper()
	assertSparseNotPlannedQuery(t, coll, cond, func() Query { return coll.Find(cond) }, want)
}

// assertSparseNotPlannedSorted is the Plan C (sort-driven) shape. It matters on its own:
// the sparse gate there is a second call site, and without it an unconstrained
// Sort over a sparse index drives the scan from an index that never stored the
// null/missing documents — Count and Iter then disagree.
func assertSparseNotPlannedSorted(t *testing.T, coll Collection, cond, sortField string, want int) {
	t.Helper()
	label := cond + " sort " + sortField
	assertSparseNotPlannedQuery(t, coll, label, func() Query { return coll.Find(cond).Sort(sortField) }, want)
}

func assertSparseNotPlannedQuery(t *testing.T, coll Collection, label string, build func() Query, want int) {
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
// instability: in v1 the query was correct in the session that created the index and broken
// in every session after, because a reopened collection loads its indexes in a
// different order and the tie-break is positional.
func TestSparseIndex_NotEqualSurvivesReopen(t *testing.T) {
	skipIfInMemory(t)
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
