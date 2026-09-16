package anystore

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// A $text clause names no index. So if a collection carried two full-text
// indexes, detectFtsQuery's `fxs[0]` would search whichever one came first in
// the collection's slice — creation order in the session that created them,
// catalog (name) order after a reopen. The same query then returned different
// rows before and after a restart, and a term that WAS indexed could come back
// empty with no error:
//
//	LIVE      order=[zbody atitle]   $text "alpha" -> 0    $text "beta" -> 50
//	REOPENED  order=[atitle zbody]   $text "alpha" -> 50   $text "beta" -> 0
//
// The ambiguity is now unrepresentable: a second full-text index is rejected at
// creation. Multiple fields belong in ONE full-text index, with per-field
// Weights — which is what BM25F is for.

func TestFulltext_SecondIndexRejected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "zbody", Fields: []string{"body"}, Kind: IndexKindFulltext}))

	err = coll.EnsureIndex(ctx, IndexInfo{
		Name: "atitle", Fields: []string{"title"}, Kind: IndexKindFulltext})
	require.ErrorIs(t, err, ErrMultipleFulltextIndexes)

	// Rejected, not half-created: the collection still has exactly one.
	var fts []string
	for _, idx := range coll.GetIndexes() {
		if idx.Info().Kind == IndexKindFulltext {
			fts = append(fts, idx.Info().Name)
		}
	}
	assert.Equal(t, []string{"zbody"}, fts)
}

// Two full-text indexes in a single EnsureIndex call are rejected too — the
// second is not yet published to the collection when the first is created.
func TestFulltext_SecondIndexRejectedInSameBatch(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	err = coll.EnsureIndex(ctx,
		IndexInfo{Name: "zbody", Fields: []string{"body"}, Kind: IndexKindFulltext},
		IndexInfo{Name: "atitle", Fields: []string{"title"}, Kind: IndexKindFulltext})
	require.ErrorIs(t, err, ErrMultipleFulltextIndexes)
}

// The guard must not break the idempotent and conflicting redefinition paths,
// which are about ONE index, not two.
func TestFulltext_SameNameRedefinitionUnaffected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	info := IndexInfo{Name: "ft", Fields: []string{"title", "body"}, Kind: IndexKindFulltext}
	require.NoError(t, coll.EnsureIndex(ctx, info))
	// Same name, same definition: idempotent.
	require.NoError(t, coll.EnsureIndex(ctx, info))
	// Same name, different definition: still a mismatch, not an ambiguity.
	err = coll.EnsureIndex(ctx, IndexInfo{
		Name: "ft", Fields: []string{"title"}, Kind: IndexKindFulltext})
	require.ErrorIs(t, err, ErrIndexMismatch)

	// Dropping frees the slot.
	require.NoError(t, coll.DropIndex(ctx, "ft"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "other", Fields: []string{"body"}, Kind: IndexKindFulltext}))
}

// One full-text index over several fields is the supported way to search more
// than one field, and it finds terms from every field it covers.
func TestFulltext_OneIndexCoversSeveralFields(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ft", Fields: []string{"title", "body"}, Kind: IndexKindFulltext}))
	for i := range 50 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"title":"alpha doc %d","body":"beta text %d"}`, i, i, i))))
	}

	for _, search := range []string{"alpha", "beta"} {
		count, err := coll.Find(fmt.Sprintf(`{"$text":{"$search":%q}}`, search)).Count(ctx)
		require.NoError(t, err)
		assert.Equalf(t, 50, count, "$text %q", search)
	}
}

// A database written before the guard can still carry two full-text indexes.
// Opening it must keep working, but a $text query must refuse rather than
// silently search whichever index load order puts first.
func TestFulltext_LegacyTwoIndexesRefuseQuery(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "fts-legacy-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "zbody", Fields: []string{"body"}, Kind: IndexKindFulltext}))
	// Bypass the DDL guard to forge the pre-guard on-disk state.
	require.NoError(t, forceSecondFtsIndex(t, coll))
	require.NoError(t, fx1.Close())

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "docs")
	require.NoError(t, err)

	_, err = coll2.Find(`{"$text":{"$search":"alpha"}}`).Count(ctx)
	require.ErrorIs(t, err, ErrMultipleFulltextIndexes)

	// Non-$text queries on such a collection keep working.
	count, err := coll2.Find(`{}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
