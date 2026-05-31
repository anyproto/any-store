package anystore

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// act-02: $ne over a multikey (array) index performs a two-bound negation
// seek that visits a straddling element via BOTH bounds; CanonicalKeyDedup
// emits it exactly once. The residual FilterIter applies all-elements $ne
// semantics. $nin desugars to Nor and yields NO index bounds, so it must
// FullScan even when given a max-Boost IndexHint. In every case the indexed
// result equals the fullscan result.
func TestIndex_ArrayNested_NeOverMultiKey_DedupAndAgreement(t *testing.T) {
	sortedIds := func(ids []string) []string {
		out := append([]string(nil), ids...)
		sort.Strings(out)
		return out
	}

	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))

	// id3 straddles the negated value "a": "A" < "a" < "z", so both the
	// lower bound [-inf,"a") and the upper bound ("a",inf] visit it.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"3","tags":["A","z"]}`),
		anyenc.MustParseJson(`{"id":"4","tags":"a"}`),
		anyenc.MustParseJson(`{"id":"5","tags":"d"}`),
	))

	// (a) Explain: index IS used with the two-bound split, and the chain
	// ends in Dedup(canonical). Identical with and without an IndexHint.
	neExplain, err := coll.Find(`{"tags":{"$ne":"a"}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, neExplain.Sql, "IndexScan(tags)")
	assert.Contains(t, neExplain.Sql, "Dedup(canonical)")
	assert.Contains(t, neExplain.Sql, `[-inf,'"a"'),('"a"',inf]`)

	neHintExplain, err := coll.Find(`{"tags":{"$ne":"a"}}`).
		IndexHint(IndexHint{IndexName: "tags", Boost: 1000000}).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, neHintExplain.Sql, "IndexScan(tags)")
	assert.Contains(t, neHintExplain.Sql, "Dedup(canonical)")
	assert.Contains(t, neHintExplain.Sql, `[-inf,'"a"'),('"a"',inf]`)

	// (b) Count == 3 and == fullscan count (no-index twin).
	neCount, err := coll.Find(`{"tags":{"$ne":"a"}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, neCount)

	fxNo := newFixture(t)
	collNo, err := fxNo.CreateCollection(ctx, "test_noidx")
	require.NoError(t, err)
	require.NoError(t, collNo.Insert(ctx,
		anyenc.MustParseJson(`{"id":"1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"3","tags":["A","z"]}`),
		anyenc.MustParseJson(`{"id":"4","tags":"a"}`),
		anyenc.MustParseJson(`{"id":"5","tags":"d"}`),
	))
	neCountNo, err := collNo.Find(`{"tags":{"$ne":"a"}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, neCount, neCountNo)

	// (c) Iter ids: raw scan order is 3,2,5; compare as a sorted set.
	// id3 the straddler appears exactly once; id1 and id4 (contain "a") excluded.
	neIds := collectIdsString(t, coll.Find(`{"tags":{"$ne":"a"}}`))
	assert.Len(t, neIds, 3) // exactly once each — no duplicate straddler
	assert.Equal(t, []string{"2", "3", "5"}, sortedIds(neIds))
	neIdsNo := collectIdsString(t, collNo.Find(`{"tags":{"$ne":"a"}}`))
	assert.Equal(t, sortedIds(neIds), sortedIds(neIdsNo))

	// $nin desugars to Nor: NO index bounds, must FullScan even when hinted.
	ninExplain, err := coll.Find(`{"tags":{"$nin":["a"]}}`).
		IndexHint(IndexHint{IndexName: "tags", Boost: 1000000}).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, ninExplain.Sql, "FullScan")
	assert.NotContains(t, ninExplain.Sql, "IndexScan")

	ninCount, err := coll.Find(`{"tags":{"$nin":["a"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, ninCount)
	ninIds := collectIdsString(t, coll.Find(`{"tags":{"$nin":["a"]}}`))
	assert.Len(t, ninIds, 3)
	assert.Equal(t, []string{"2", "3", "5"}, sortedIds(ninIds))
}

// act-29: A nested index path that crosses an array intermediate is NOT
// implicitly traversed. Value.Get does strconv.Atoi on the segment after the
// array; the non-numeric "name" fails -> nil -> one 'null' entry (non-sparse).
// Positional access (items.0.name) resolves via the numeric index but is a
// different, unindexed path.
func TestIndex_ArrayNested_NestedField_IntermediateArray_NotTraversed(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "items.name", Fields: []string{"items.name"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"items":[{"name":"a"},{"name":"b"}]}`),
		anyenc.MustParseJson(`{"id":2,"items":[{"name":"c"}]}`),
	))

	// Each doc contributes exactly one 'null' entry: the array intermediate
	// stops traversal so the indexed value is null for both docs.
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 2)

	// No implicit array traversal: items.name="a" finds nothing. This query
	// still IndexScans the items.name index (which holds only null entries).
	cnt, err := coll.Find(`{"items.name":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, cnt)

	// Positional access works (resolves via the numeric index). This is a
	// different, unindexed path -> FullScan; do not assert IndexScan.
	posCnt, err := coll.Find(`{"items.0.name":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, posCnt)

	// Both docs are findable via the shared 'null' entry.
	nullCnt, err := coll.Find(`{"items.name":null}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, nullCnt)
}

// act-30: Querying by a whole NON-empty array value uses the post-loop
// whole-array index entry as a single, order-sensitive point bound.
func TestIndex_ArrayNested_WholeArrayEquality_UsesIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"2","tags":["a"]}`),
	))

	// Whole-array equality uses the index with a single point bound on the
	// order-sensitive whole-array encoding.
	wholeExplain, err := coll.Find(`{"tags":["a","b"]}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, wholeExplain.Sql, "IndexScan(tags)")
	assert.Contains(t, wholeExplain.Sql, `['["a","b"]','["a","b"]']`)

	cnt, err := coll.Find(`{"tags":["a","b"]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	assert.Equal(t, []string{"1"}, collectIdsString(t, coll.Find(`{"tags":["a","b"]}`)))

	// Order-sensitive: ["b","a"] is a different encoding -> no match.
	revCnt, err := coll.Find(`{"tags":["b","a"]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, revCnt)

	// Single-element whole array matches only its own doc.
	oneCnt, err := coll.Find(`{"tags":["a"]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, oneCnt)
	assert.Equal(t, []string{"2"}, collectIdsString(t, coll.Find(`{"tags":["a"]}`)))
}

// act-31: A null ARRAY ELEMENT is indexed as a real element (the sparse
// guard only short-circuits whole-field null), so it survives even on a
// sparse index. On a non-sparse index a null element makes a doc
// indistinguishable from a missing-field doc by a {tags:null} query.
// Duplicate nulls dedup within a doc.
func TestIndex_ArrayNested_NullElementInArray_IndexedAndQueryable(t *testing.T) {
	// Sub A: non-sparse.
	fxA := newFixture(t)
	collA, err := fxA.CreateCollection(ctx, "ns")
	require.NoError(t, err)
	require.NoError(t, collA.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))
	require.NoError(t, collA.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["a",null,"b"]}`), // null,a,b,whole-array = 4
		anyenc.MustParseJson(`{"id":2}`),                       // missing -> 1 null
	))
	assertIndexLen(t, collA.GetIndexes()[0], 5)

	// {tags:null} matches BOTH the null-element doc and the missing-field doc.
	nullA, err := collA.Find(`{"tags":null}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, nullA)
	aA, err := collA.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, aA)

	// Sub B: sparse contrast. The null element is still indexed (v is an
	// array, so the sparse guard does not fire); only the missing-field doc
	// is skipped.
	fxB := newFixture(t)
	collB, err := fxB.CreateCollection(ctx, "sp")
	require.NoError(t, err)
	require.NoError(t, collB.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}, Sparse: true}))
	require.NoError(t, collB.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["a",null,"b"]}`), // null,a,b,whole-array = 4
		anyenc.MustParseJson(`{"id":2}`),                       // missing -> skipped
	))
	assertIndexLen(t, collB.GetIndexes()[0], 4)
	nullB, err := collB.Find(`{"tags":null}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, nullB)

	// Sub C: duplicate nulls collapse within a doc.
	fxC := newFixture(t)
	collC, err := fxC.CreateCollection(ctx, "dup")
	require.NoError(t, err)
	require.NoError(t, collC.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))
	require.NoError(t, collC.Insert(ctx,
		anyenc.MustParseJson(`{"id":3,"tags":[null,null,"a"]}`), // null + a + whole-array = 3
	))
	assertIndexLen(t, collC.GetIndexes()[0], 3)
}

// act-32: A deep nested path whose LEAF is an array fans out multikey-style
// (K elements + whole-array) path-agnostically. $in over nested-leaf
// elements dedups overlapping docs to one result.
func TestIndex_ArrayNested_NestedLeafArray_MultiKeyFanout(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "abc", Fields: []string{"a.b.c"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":{"b":{"c":["x","y"]}}}`),
		anyenc.MustParseJson(`{"id":2,"a":{"b":{"c":["y","z"]}}}`),
	))

	// 3 entries per doc (2 elements + whole-array) regardless of nesting depth.
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 6)

	xCount, err := coll.Find(`{"a.b.c":"x"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, xCount)

	yCount, err := coll.Find(`{"a.b.c":"y"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, yCount)

	// The single-field equality on the nested-leaf array uses the index.
	eqExplain, err := coll.Find(`{"a.b.c":"x"}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, eqExplain.Sql, "IndexScan(abc)")

	// $in dedup: id1 has both x and y but collapses to one doc; id2 has y.
	inCount, err := coll.Find(`{"a.b.c":{"$in":["x","y"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, inCount)

	// Iter over the $in filter yields 2 results, each id exactly once.
	inIds := collectIntField(t, coll.Find(`{"a.b.c":{"$in":["x","y"]}}`), "id")
	sort.Ints(inIds)
	assert.Equal(t, []int{1, 2}, inIds)
}
