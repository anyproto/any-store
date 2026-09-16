package anystore

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// Count and Iter must answer the same question. They do not when a COUNT plan
// verifies an uncovered equality predicate through a SPARSE index.
//
// Count plans with CountOnly=true, which can build a verify chain: seek one
// indexed field, then confirm the remaining ("uncovered") equality predicates
// by probing an index on each of those fields instead of fetching the document.
// buildVerifyChain (internal/qplanner/planner.go) picks that probe index with:
//
//	if !info.Unique && len(info.FieldNames) == 1 && info.FieldNames[0] == field {
//
// — no Sparse check. A sparse index stores no entry for a document whose field
// is missing or null, so probing it for `field == null` reports "not there" for
// exactly the documents the predicate is supposed to match, and Count drops
// them. Iter fetches the document and evaluates the filter, so it is correct.
//
// Plan B and Plan C both gate sparse indexes through sparseIndexComplete; this
// third call site does not.
//
// Shape of the fixture: an anytype-heart "type" object, where `isUninstalled`
// is written only when true — so live objects lack the field entirely — and
// carries a sparse index.

func newSparseVerifyColl(t *testing.T, fx *fixture, name string, alsoPlainIndex bool) Collection {
	t.Helper()
	coll, err := fx.CreateCollection(ctx, name)
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "spaceId", Fields: []string{"spaceId"}}))
	if alsoPlainIndex {
		// A NON-sparse index on the same field, created BEFORE the sparse one
		// and named so that it sorts AFTER it. Creation order then offers the
		// correct (non-sparse) index first and catalog order offers the sparse
		// one first — which is what makes the answer flip across a reopen.
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Name: "zzIsUninstalledPlain", Fields: []string{"isUninstalled"}}))
	}
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "isUninstalled", Fields: []string{"isUninstalled"}, Sparse: true}))

	for i := range 200 {
		j := fmt.Sprintf(`{"id":%d,"spaceId":"space%d"}`, i, i)
		if i%50 == 0 { // a few really are uninstalled
			j = fmt.Sprintf(`{"id":%d,"spaceId":"space%d","isUninstalled":true}`, i, i)
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(j)))
	}
	return coll
}

// assertCountMatchesIter is the whole contract: whatever the planner does,
// Count must return the number of documents Iter yields.
func assertCountMatchesIter(t *testing.T, coll Collection, cond string, want int) {
	t.Helper()

	iter, err := coll.Find(cond).Iter(ctx)
	require.NoError(t, err)
	n := 0
	for iter.Next() {
		n++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	assert.Equalf(t, want, n, "Iter for %s", cond)

	count, err := coll.Find(cond).Count(ctx)
	require.NoError(t, err)
	assert.Equalf(t, want, count, "Count for %s", cond)
}

// TestCountVerify_SparseIndexDropsMatchingDocs is the minimal case, and it does
// not need a reopen: when the sparse index is the only single-field index on
// the field, every session gets the wrong Count.
func TestCountVerify_SparseIndexDropsMatchingDocs(t *testing.T) {
	fx := newFixture(t)
	coll := newSparseVerifyColl(t, fx, "types", false)

	// Doc 7 exists, is in space7, and carries no isUninstalled field — so it
	// matches `isUninstalled == null`. Iter yields it; Count says 0.
	assertCountMatchesIter(t, coll, `{"spaceId":"space7","isUninstalled":null}`, 1)
}

// TestCountVerify_SparseProbeFlipsOnReopen shows the second-order effect. With
// BOTH a sparse and a non-sparse index on the field, which one the verify chain
// probes is decided by slice position — creation order live, catalog (name)
// order after a reopen — so a collection can get the right answer in the
// session that created the indexes and the wrong one afterwards.
func TestCountVerify_SparseProbeFlipsOnReopen(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "sparse-verify-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	const cond = `{"spaceId":"space7","isUninstalled":null}`

	fx1 := newFixturePath(t, tmpDir)
	coll := newSparseVerifyColl(t, fx1, "types", true)
	assertCountMatchesIter(t, coll, cond, 1)
	require.NoError(t, fx1.Close())

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "types")
	require.NoError(t, err)
	assertCountMatchesIter(t, coll2, cond, 1)
}

// TestCountVerify_UnaffectedShapes pins the boundary of the defect, so a fix
// can be checked for over-reach. None of these reach the verify chain: $ne and
// $exists emit range bounds rather than a fixed equality, and a single
// predicate leaves no uncovered field to verify.
func TestCountVerify_UnaffectedShapes(t *testing.T) {
	fx := newFixture(t)
	coll := newSparseVerifyColl(t, fx, "types", false)

	assertCountMatchesIter(t, coll, `{"spaceId":"space7","isUninstalled":{"$ne":true}}`, 1)
	assertCountMatchesIter(t, coll, `{"spaceId":"space7","isUninstalled":{"$exists":false}}`, 1)
	assertCountMatchesIter(t, coll, `{"spaceId":"space7","isUninstalled":{"$in":[null,false]}}`, 1)
	assertCountMatchesIter(t, coll, `{"isUninstalled":null}`, 196)
	assertCountMatchesIter(t, coll, `{"spaceId":{"$ne":"none"},"isUninstalled":null}`, 196)
	assertCountMatchesIter(t, coll, `{"spaceId":"space0","isUninstalled":true}`, 1)
}
