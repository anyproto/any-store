package anystore

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// GO-7510 repro, ported to v2.
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
// sparse index is gated out entirely — so these tests pin defect 1 only;
// defect 2 is pinned separately by TestGO7510_PlanStableAcrossReopen.

type go7510Doc struct {
	id             int
	resolvedLayout string
	isUninstalled  *bool // nil = field absent
}

func go7510Fixture() []go7510Doc {
	tru, fls := true, false
	docs := make([]go7510Doc, 0, 24)
	for i := range 24 {
		layout := "page"
		if i >= 12 {
			layout = "note"
		}
		d := go7510Doc{id: i, resolvedLayout: layout}
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

func go7510Insert(t *testing.T, coll Collection, docs []go7510Doc) {
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

func go7510EnsureIndexes(t *testing.T, coll Collection) {
	t.Helper()
	// Both single-field, both weight-10 in v1 — a tie there, and
	// "isUninstalled" sorts before "resolvedLayout", which is why v1 broke only
	// after a reopen. Under v2 the sparse index is not a candidate at all.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "isUninstalled", Fields: []string{"isUninstalled"}, Sparse: true}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "resolvedLayout", Fields: []string{"resolvedLayout"}}))
}

// assertGo7510 runs the query through Count and Iter — separate plan builds
// (CountOnly differs), and v1 broke both — and additionally asserts the planner
// did not answer it from a sparse index, which is the root cause as opposed to
// the row-count symptom.
func assertGo7510(t *testing.T, coll Collection, cond string, want int) {
	t.Helper()
	assertGo7510Query(t, coll, cond, func() Query { return coll.Find(cond) }, want)
}

// assertGo7510Sorted is the Plan C (sort-driven) shape. It matters on its own:
// the sparse gate there is a second call site, and without it an unconstrained
// Sort over a sparse index drives the scan from an index that never stored the
// null/missing documents — Count and Iter then disagree.
func assertGo7510Sorted(t *testing.T, coll Collection, cond, sortField string, want int) {
	t.Helper()
	label := cond + " sort " + sortField
	assertGo7510Query(t, coll, label, func() Query { return coll.Find(cond).Sort(sortField) }, want)
}

func assertGo7510Query(t *testing.T, coll Collection, label string, build func() Query, want int) {
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

// TestGO7510_SparsePlannerIgnoresNotEqual is defect #1 of GO-7510.
func TestGO7510_SparsePlannerIgnoresNotEqual(t *testing.T) {
	docs := go7510Fixture()

	t.Run("index created before insert", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		go7510EnsureIndexes(t, coll)
		go7510Insert(t, coll, docs)

		assertGo7510(t, coll, `{"isUninstalled":{"$ne":true}}`, 22)
		assertGo7510(t, coll, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`, 10)
		assertGo7510(t, coll, `{"resolvedLayout":"page","isUninstalled":{"$ne":true}}`, 12)
	})

	t.Run("index created after insert", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		go7510Insert(t, coll, docs)
		go7510EnsureIndexes(t, coll)

		assertGo7510(t, coll, `{"isUninstalled":{"$ne":true}}`, 22)
		assertGo7510(t, coll, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`, 10)
	})

	t.Run("no index", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		go7510Insert(t, coll, docs)

		assertGo7510(t, coll, `{"isUninstalled":{"$ne":true}}`, 22)
	})

	// v1: 0 rows. The heart symptom — `isUninstalled` is only ever written when
	// true, so in a real account no live object carries the field at all.
	t.Run("no document carries the field", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		go7510EnsureIndexes(t, coll)
		for i := range 24 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"resolvedLayout":"note"}`, i))))
		}

		assertGo7510(t, coll, `{"isUninstalled":{"$ne":true}}`, 24)
		assertGo7510(t, coll, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`, 24)
	})

	// Plan C: the sort-driven sparse gate is a second call site with its own
	// failure mode — an unconstrained Sort over the sparse index drives the
	// scan from an index that never stored the 20 documents missing the field,
	// and Count and Iter then disagree.
	t.Run("sort over the sparse field", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		go7510EnsureIndexes(t, coll)
		go7510Insert(t, coll, docs)

		assertGo7510Sorted(t, coll, `{}`, "isUninstalled", 24)
		assertGo7510Sorted(t, coll, `{"resolvedLayout":"note"}`, "isUninstalled", 12)
	})

	// Other predicates that can match an absent or null field, each of which
	// the sparse index would answer wrongly if the gate stopped firing.
	t.Run("other absent-matching predicates", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		go7510EnsureIndexes(t, coll)
		go7510Insert(t, coll, docs)

		assertGo7510(t, coll, `{"isUninstalled":{"$ne":false}}`, 22) // 20 absent + 2 true
		assertGo7510(t, coll, `{"isUninstalled":{"$ne":"x"}}`, 24)   // every doc
		assertGo7510(t, coll, `{"isUninstalled":null}`, 20)          // absent only
		assertGo7510(t, coll, `{"isUninstalled":{"$exists":false}}`, 20)
		// The issue measured $in:[null,false] as NOT working in v1 (In.Ok
		// returned false for an absent key). v2 answers it correctly.
		assertGo7510(t, coll, `{"isUninstalled":{"$in":[null,false]}}`, 22)
	})

	// The other predicate shapes measured in the issue. All are "not true",
	// spelled differently; all must agree with $ne. Only $ne contributes index
	// bounds — the rest plan as a FullScan, so for them this pins row-count
	// semantics rather than the sparse gate.
	t.Run("equivalent predicate shapes", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "objects")
		require.NoError(t, err)
		go7510EnsureIndexes(t, coll)
		go7510Insert(t, coll, docs)

		for _, cond := range []string{
			`{"isUninstalled":{"$ne":true}}`,
			`{"isUninstalled":{"$nin":[true]}}`,
			`{"isUninstalled":{"$not":{"$eq":true}}}`,
			`{"$nor":[{"isUninstalled":true}]}`,
			`{"$or":[{"isUninstalled":{"$exists":false}},{"isUninstalled":{"$ne":true}}]}`,
		} {
			assertGo7510(t, coll, cond, 22)
		}
	})
}

// TestGO7510_SparseNotEqualSurvivesReopen is defect #1 combined with defect #2:
// in v1 the query was correct in the session that created the index and broken
// in every session after, because a reopened collection loads its indexes in a
// different order and the tie-break is positional.
func TestGO7510_SparseNotEqualSurvivesReopen(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "go7510-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	docs := go7510Fixture()

	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	go7510EnsureIndexes(t, coll)
	go7510Insert(t, coll, docs)
	assertGo7510(t, coll, `{"isUninstalled":{"$ne":true}}`, 22)
	assertGo7510(t, coll, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`, 10)
	require.NoError(t, fx1.Close())

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "objects")
	require.NoError(t, err)
	assertGo7510(t, coll2, `{"isUninstalled":{"$ne":true}}`, 22)
	assertGo7510(t, coll2, `{"resolvedLayout":"note","isUninstalled":{"$ne":true}}`, 10)
}

// TestGO7510_PlanStableAcrossReopen is defect #2 on its own: v1 kept creation
// order for a freshly created collection and got alphabetical order from the
// catalog on reopen, so a cost tie resolved differently per session and the
// query was correct only in the session that created the index.
//
// v2 loads indexes the same two ways (EnsureIndex appends; a reopen replays the
// catalog in key order), and that is fine — what must not depend on it is the
// plan. The planner breaks an exact cost tie on the index name, so every
// session picks the same one.
func TestGO7510_PlanStableAcrossReopen(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "go7510-order-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	for _, name := range []string{"zzz", "mmm", "aaa"} {
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: name, Fields: []string{name}}))
	}
	// Identical cardinality on every indexed field => a genuine cost tie.
	for i := range 100 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"zzz":%d,"mmm":%d,"aaa":%d}`, i, i%10, i%10, i%10))))
	}

	const tiedCond = `{"zzz":1,"mmm":1,"aaa":1}`
	freshNames := go7510IndexNames(coll)
	freshExplain, err := coll.Find(tiedCond).Explain(ctx)
	require.NoError(t, err)
	freshCosts := map[string]float64{}
	for _, ie := range freshExplain.Indexes {
		freshCosts[ie.Name] = ie.Cost
	}
	freshCount, err := coll.Find(tiedCond).Count(ctx)
	require.NoError(t, err)
	require.NoError(t, fx1.Close())

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "objects")
	require.NoError(t, err)
	reopenedNames := go7510IndexNames(coll2)
	reopenedExplain, err := coll2.Find(tiedCond).Explain(ctx)
	require.NoError(t, err)
	reopenedCount, err := coll2.Find(tiedCond).Count(ctx)
	require.NoError(t, err)

	// This test only means something if the three seeks genuinely tie: the
	// name rung fires on an exact cost tie and nothing else. Assert it, so a
	// cost-model change that separates the costs fails loudly instead of
	// turning the test into a silent no-op.
	require.Len(t, freshCosts, 3)
	for name, cost := range freshCosts {
		assert.Equalf(t, freshCosts["aaa"], cost,
			"index %s must tie with the others for this test to exercise the tie-break", name)
	}

	// The two sessions see the indexes in different orders — creation order
	// live, catalog (name) order after a reopen. That is the input the planner
	// must not be sensitive to. Logged rather than asserted: normalizing the
	// load order is a legitimate future fix, and it must not fail this test.
	assert.ElementsMatch(t, freshNames, reopenedNames)
	t.Logf("index order: live=%v reopened=%v", freshNames, reopenedNames)

	assert.Equal(t, freshCount, reopenedCount, "row count must not depend on the session")
	assert.Equal(t, go7510UsedIndex(t, freshExplain), go7510UsedIndex(t, reopenedExplain),
		"a tied plan must resolve identically on a fresh and a reopened collection")
	assert.Equal(t, "aaa", go7510UsedIndex(t, freshExplain),
		"an exact cost tie resolves on the lowest index name")
	// The whole explain output — chosen plan, costs and the candidate listing —
	// is reproducible across sessions.
	assert.Equal(t, freshExplain.Plan, reopenedExplain.Plan)
}

func go7510IndexNames(coll Collection) []string {
	var names []string
	for _, idx := range coll.GetIndexes() {
		names = append(names, idx.Info().Name)
	}
	return names
}

func go7510UsedIndex(t *testing.T, explain Explain) string {
	t.Helper()
	var used []string
	for _, ie := range explain.Indexes {
		if ie.Used {
			used = append(used, ie.Name)
		}
	}
	return strings.Join(used, ",")
}
