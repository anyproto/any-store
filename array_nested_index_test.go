/*
Index/Planner tests inspired by SQLite: index.test

Test scenario:
Tests array field indexing (multi-key entries per document), nested field
indexing with dot-notation, and compound indexes mixing arrays and nested
fields. Verifies that index entries are correctly created, updated, and
removed for these special field types, and that queries against them
return the expected results.

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestIndex_ArrayNested_ArrayField_MultipleEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	doc := anyenc.MustParseJson(`{"id":1,"tags":["go","rust","python"]}`)
	require.NoError(t, coll.Insert(ctx, doc))

	// From fillKeysBuf: array ["go","rust","python"] produces keys:
	// "go", "rust", "python" (3 unique elements) + ["go","rust","python"] (the array itself) = 4 entries
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 4)

	// Query for an element should find the document
	count, err := coll.Find(`{"tags":"go"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_ArrayNested_ArrayField_QueryElement(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	// Insert multiple docs, some containing "go" in their tags
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["go","rust"]}`),
		anyenc.MustParseJson(`{"id":2,"tags":["python","java"]}`),
		anyenc.MustParseJson(`{"id":3,"tags":["go","python","c"]}`),
		anyenc.MustParseJson(`{"id":4,"tags":["haskell"]}`),
		anyenc.MustParseJson(`{"id":5,"tags":["go"]}`),
	))

	// Query for "go" should find docs 1, 3, 5
	count, err := coll.Find(`{"tags":"go"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Query for "python" should find docs 2, 3
	count, err = coll.Find(`{"tags":"python"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Query for "haskell" should find doc 4 only
	count, err = coll.Find(`{"tags":"haskell"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Query for non-existent tag
	count, err = coll.Find(`{"tags":"cobol"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIndex_ArrayNested_ArrayField_UpdateArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	// Insert doc with tags ["a","b"]
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b"]}`)))
	idx := coll.GetIndexes()[0]
	// "a", "b", ["a","b"] = 3 entries
	assertIndexLen(t, idx, 3)

	// Update to tags ["c","d"]
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":["c","d"]}`)))

	// Old entries removed, new entries added: "c", "d", ["c","d"] = 3
	assertIndexLen(t, idx, 3)

	// Query old values — should find nothing
	count, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"tags":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Query new values — should find the doc
	count, err = coll.Find(`{"tags":"c"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"tags":"d"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_ArrayNested_ArrayField_DeleteDoc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":2,"tags":["c","d"]}`),
	))

	idx := coll.GetIndexes()[0]
	// Doc1: "a","b",["a","b"] = 3; Doc2: "c","d",["c","d"] = 3; total = 6
	assertIndexLen(t, idx, 6)

	// Delete doc 1
	require.NoError(t, coll.DeleteId(ctx, 1))

	// Only doc2 entries remain: "c","d",["c","d"] = 3
	assertIndexLen(t, idx, 3)

	// Query for deleted doc's tags — nothing
	count, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Query for remaining doc's tags — found
	count, err = coll.Find(`{"tags":"c"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Delete doc 2
	require.NoError(t, coll.DeleteId(ctx, 2))
	assertIndexLen(t, idx, 0)
}

func TestIndex_ArrayNested_ArrayField_EmptyArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	// Insert doc with empty array
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":[]}`)))

	idx := coll.GetIndexes()[0]
	// Empty array: no elements to expand, but the array value itself [] is indexed
	// From writeValues: when arr is empty, the if-len(arr)!=0 branch is skipped,
	// then v.MarshalTo is called on the full value (which is []), producing one key.
	assertIndexLen(t, idx, 1)
}

func TestIndex_ArrayNested_ArrayField_DuplicateElements(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	// Insert doc with duplicate elements
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","a","b"]}`)))

	idx := coll.GetIndexes()[0]
	// From fillKeysBuf test cases: {"id":1,"a":["a", "a", "b", "c", "b"]} → ["a","b","c",full-array]
	// So ["a","a","b"] → deduplicated elements "a","b" + full array ["a","a","b"] = 3 entries
	assertIndexLen(t, idx, 3)

	// Query should still find the doc
	count, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"tags":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_ArrayNested_NestedField_DotNotation(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"meta.score"}}))

	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"meta":{"score":%d}}`, i, i*10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 20)

	// Range query on nested field
	count, err := coll.Find(`{"meta.score":{"$gte":50}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 15, count) // scores 50,60,...,190

	count, err = coll.Find(`{"meta.score":{"$gte":50,"$lt":100}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count) // scores 50,60,70,80,90

	// Equality
	count, err = coll.Find(`{"meta.score":100}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify index is used
	explain, err := coll.Find(`{"meta.score":{"$gte":50}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_ArrayNested_NestedField_MissingParent(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"meta.score"}}))

	// Insert doc without "meta" field at all
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"name":"no-meta"}`)))
	// Insert doc with meta but no score
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"meta":{"name":"has-meta"}}`)))
	// Insert doc with meta.score
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"meta":{"score":42}}`)))

	idx := coll.GetIndexes()[0]
	// Doc1: meta missing → null key; Doc2: meta.score missing → null key; Doc3: meta.score=42
	// All 3 docs get indexed (non-sparse index indexes nulls)
	assertIndexLen(t, idx, 3)

	// Query for score=42 should find only doc3
	count, err := coll.Find(`{"meta.score":42}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Sparse index should skip docs without meta.score
	fx2 := newFixture(t)
	coll2, err := fx2.CreateCollection(ctx, "test2")
	require.NoError(t, err)
	require.NoError(t, coll2.EnsureIndex(ctx, IndexInfo{Fields: []string{"meta.score"}, Sparse: true}))

	require.NoError(t, coll2.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"name":"no-meta"}`),
		anyenc.MustParseJson(`{"id":2,"meta":{"name":"has-meta"}}`),
		anyenc.MustParseJson(`{"id":3,"meta":{"score":42}}`),
	))

	idx2 := coll2.GetIndexes()[0]
	// Only doc3 has meta.score — sparse index should have 1 entry
	assertIndexLen(t, idx2, 1)
}

func TestIndex_ArrayNested_NestedField_DeepNesting(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a.b.c"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":{"b":{"c":10}}}`),
		anyenc.MustParseJson(`{"id":2,"a":{"b":{"c":20}}}`),
		anyenc.MustParseJson(`{"id":3,"a":{"b":{"c":30}}}`),
		anyenc.MustParseJson(`{"id":4,"a":{"b":{}}}`),    // c missing
		anyenc.MustParseJson(`{"id":5,"a":{}}`),           // b missing
		anyenc.MustParseJson(`{"id":6}`),                  // a missing
	))

	idx := coll.GetIndexes()[0]
	// All 6 docs indexed (non-sparse: missing = null key)
	assertIndexLen(t, idx, 6)

	// Range query
	count, err := coll.Find(`{"a.b.c":{"$gte":15}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count) // c=20, c=30

	// Equality
	count, err = coll.Find(`{"a.b.c":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify index scan
	explain, err := coll.Find(`{"a.b.c":10}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_ArrayNested_CompoundArrayNested(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags", "meta.score"}}))

	// Doc with array tags and nested score
	// tags: ["go","rust"] → elements "go","rust" + array ["go","rust"]
	// meta.score: 80 → single value
	// Cartesian product: "go"/80, "rust"/80, ["go","rust"]/80 = 3 entries
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["go","rust"],"meta":{"score":80}}`),
	))

	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 3)

	// Insert doc with single-element array
	// tags: ["python"] → "python" + ["python"]
	// meta.score: 90
	// Product: "python"/90, ["python"]/90 = 2 entries
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":2,"tags":["python"],"meta":{"score":90}}`),
	))
	assertIndexLen(t, idx, 5)

	// Query by tag element + nested score
	count, err := coll.Find(`{"tags":"go","meta.score":80}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Query by tag only
	count, err = coll.Find(`{"tags":"go"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Query by nested field only
	count, err = coll.Find(`{"meta.score":{"$gte":85}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count) // only doc2 with score=90

	// Insert doc with missing nested field
	// tags: ["go"] → "go" + ["go"]
	// meta.score: missing → null
	// Product: "go"/null, ["go"]/null = 2 entries
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":3,"tags":["go"]}`),
	))
	assertIndexLen(t, idx, 7)

	// Query "go" should now find doc1 and doc3
	count, err = coll.Find(`{"tags":"go"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}
