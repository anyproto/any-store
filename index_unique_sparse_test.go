/*
Index/Planner tests inspired by SQLite: index3.test, index4.test

Test scenario:
Tests unique index constraints (compound, self-update, upsert, delete+reinsert,
bulk partial failure), sparse index behavior (missing/null fields, field
appearance via update, compound sparse), sparse+unique combinations,
index length tracking through mixed mutations, nested field unique indexes,
and drop-index followed by duplicate insert.

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

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"sort"
)

func TestIndex_UniqueSparse_CompoundUnique(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}, Unique: true}))

	// Same a, different b — should succeed
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":1,"b":2}`)))
	// Different a, same b — should succeed
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":2,"b":1}`)))
	// Same a+b combo — should fail
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":1,"b":1}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	assertCollCount(t, coll, 3)
	assertIndexLen(t, coll.GetIndexes()[0], 3)
}

func TestIndex_UniqueSparse_UpdateToNewValue(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":2}`)))

	// Update to a completely new unique value — should succeed
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":99}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 2)

	// Verify the old value is gone and new value is queryable
	count, err := coll.Find(`{"a":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"a":99}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_UniqueSparse_UpdateSameValue(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":10}`)))

	// Update same doc keeping same unique value but changing other field — no self-conflict
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":20}`)))

	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, `{"id":1,"a":1,"b":20}`, doc.Value().String())
	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

func TestIndex_UniqueSparse_UpsertDuplicate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":20}`)))

	// Upsert existing doc with same id — should update (not conflict)
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":30}`)))
	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, `{"id":1,"a":30}`, doc.Value().String())
	assertIndexLen(t, coll.GetIndexes()[0], 2)

	// Upsert new doc with value that conflicts with existing doc — should fail
	err = coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":3,"a":20}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
	assertCollCount(t, coll, 2)
}

func TestIndex_UniqueSparse_SparseQuery(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	// Mix of docs with and without the field
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"b":20}`),
		anyenc.MustParseJson(`{"id":3,"a":30}`),
		anyenc.MustParseJson(`{"id":4,"c":40}`),
		anyenc.MustParseJson(`{"id":5,"a":50}`),
	))

	// Only 3 docs have field "a"
	assertIndexLen(t, coll.GetIndexes()[0], 3)

	// Query for a specific value — should use index
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Total collection count is 5
	assertCollCount(t, coll, 5)
}

func TestIndex_UniqueSparse_SparseUpdateFieldAppears(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	// Insert doc without field "a"
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"b":5}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// Update to add field "a" — index should grow
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":10,"b":5}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 1)

	// Update to remove field "a" — index should shrink
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"b":5}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

func TestIndex_UniqueSparse_SparseUniqueNoConflict(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true, Unique: true}))

	// Multiple docs without field "a" — all succeed (sparse skips them)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3}`)))

	// Doc with field "a" — success
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":10}`)))
	// Another doc with same "a" value — should fail unique constraint
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":5,"a":10}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// Different "a" value — success
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":6,"a":20}`)))

	assertCollCount(t, coll, 5) // 3 without field + 2 with field
	assertIndexLen(t, coll.GetIndexes()[0], 2)
}

func TestIndex_UniqueSparse_UniqueNestedField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"meta.email"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"meta":{"email":"a@b.com"}}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"meta":{"email":"c@d.com"}}`)))

	// Duplicate nested value — should fail
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"meta":{"email":"a@b.com"}}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	assertCollCount(t, coll, 2)
	assertIndexLen(t, coll.GetIndexes()[0], 2)
}

func TestIndex_UniqueSparse_BulkInsertPartialDuplicate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))

	// Bulk insert where one doc duplicates an existing value
	err = coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":2,"a":2}`),
		anyenc.MustParseJson(`{"id":3,"a":1}`), // duplicate of id:1
		anyenc.MustParseJson(`{"id":4,"a":4}`),
	)
	require.Error(t, err)

	// The whole batch should be rejected (transactional)
	assertCollCount(t, coll, 1)
	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

func TestIndex_UniqueSparse_IndexLenMixedMutations(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	idx := coll.GetIndexes()[0]

	// Insert 5 docs
	for i := 1; i <= 5; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}
	assertIndexLen(t, idx, 5)

	// Delete 2 docs
	require.NoError(t, coll.DeleteId(ctx, 1))
	require.NoError(t, coll.DeleteId(ctx, 3))
	assertIndexLen(t, idx, 3)

	// Update 1 doc (change a value)
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":2,"a":99}`)))
	assertIndexLen(t, idx, 3)

	// Insert 3 more
	for i := 6; i <= 8; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}
	assertIndexLen(t, idx, 6)

	// Delete all remaining
	for _, id := range []int{2, 4, 5, 6, 7, 8} {
		require.NoError(t, coll.DeleteId(ctx, id))
	}
	assertIndexLen(t, idx, 0)
}

func TestIndex_UniqueSparse_DropUniqueThenInsertDuplicate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))

	// Duplicate fails while index exists
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":1}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// Drop the unique index
	require.NoError(t, coll.DropIndex(ctx, "a"))
	assert.Len(t, coll.GetIndexes(), 0)

	// Now the duplicate insert should succeed
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":1}`)))
	assertCollCount(t, coll, 2)
}

func TestIndex_UniqueSparse_DeleteAndReinsert(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":42}`)))

	// Delete the doc
	require.NoError(t, coll.DeleteId(ctx, 1))
	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// Reinsert with same value — should succeed (slot is free)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":42}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

func TestIndex_UniqueSparse_CreateUniqueOnExistingDuplicates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	// Insert docs with duplicate values first
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":5}`),
		anyenc.MustParseJson(`{"id":2,"a":5}`),
	))

	// Attempting to create unique index should fail
	err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true})
	require.Error(t, err)

	// No index should have been created
	assert.Len(t, coll.GetIndexes(), 0)

	// Data is intact
	assertCollCount(t, coll, 2)
}

func TestIndex_UniqueSparse_SparseCompoundBothMissing(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}, Sparse: true}))

	// Only "a" present — not indexed (b missing)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	// Only "b" present — not indexed (a missing)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"b":2}`)))
	// Both present — indexed
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":1,"b":2}`)))
	// a is null — not indexed
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":null,"b":2}`)))

	assertIndexLen(t, coll.GetIndexes()[0], 1)
	assertCollCount(t, coll, 4)
}

func TestIndex_UniqueSparse_NonSparseNullIndexed(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Non-sparse index indexes everything, including null/missing
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":null}`),
		anyenc.MustParseJson(`{"id":2}`),
		anyenc.MustParseJson(`{"id":3,"a":5}`),
	))

	assertIndexLen(t, coll.GetIndexes()[0], 3)
}

func TestIndex_UniqueSparse_SparseNullField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":null}`),
		anyenc.MustParseJson(`{"id":2,"a":10}`),
		anyenc.MustParseJson(`{"id":3}`),
	))

	// Only doc with a=10 is indexed
	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

// --- Coverage tests from unique_array_coverage_test.go ---

// TestIndex_UniqueArray_Coverage_WithinDocDuplicates verifies that a single
// document containing duplicate values inside an array (e.g. x:["a","b","a"])
// does not produce a self-collision under a unique index. The within-doc
// dedup path in isUnique() should collapse duplicates before writing, leaving
// one index entry per distinct value.
func TestIndex_UniqueArray_Coverage_WithinDocDuplicates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Fields: []string{"x"},
		Unique: true,
	}))

	// Single doc with duplicate "a" inside the array — must not self-conflict.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"x":["a","b","a"]}`)))

	assertCollCount(t, coll, 1)
	idx := coll.GetIndexes()[0]

	// Expect exactly 3 entries on a 1-field index for an array of 3 elements
	// after within-doc dedup: "a", "b", and the array itself (serialized).
	// The array-as-a-whole entry is emitted as a canonical key by fillKeysBuf.
	// We only care that there's no unique violation AND that distinct scalar
	// values show up exactly once each.
	count, err := coll.Find(`{"x":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "x=a should match exactly one doc")

	count, err = coll.Find(`{"x":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "x=b should match exactly one doc")

	// And the index length should not blow past what a dedup'd array produces.
	n, err := idx.Len(ctx)
	require.NoError(t, err)
	// Two distinct scalar keys ("a","b") plus the array-as-a-whole canonical key.
	assert.Equal(t, 3, n, "expected 3 unique index entries after within-doc dedup")
}

// TestIndex_UniqueArray_Coverage_ScalarVsArrayCollision verifies MongoDB
// semantics: a unique index applies to every indexed key, so a scalar doc
// with x:"a" and an array doc with x:["a","b"] both produce a key "a"
// and must collide on the second insert.
func TestIndex_UniqueArray_Coverage_ScalarVsArrayCollision(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Fields: []string{"x"},
		Unique: true,
	}))

	// Scalar insert: key "a" is written.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"d1","x":"a"}`)))

	// Array insert whose elements include "a" — must fail on key "a".
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"d2","x":["a","b"]}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// Only d1 is in the collection.
	assertCollCount(t, coll, 1)

	// And symmetric: array first, scalar second.
	coll2, err := fx.CreateCollection(ctx, "test2")
	require.NoError(t, err)
	require.NoError(t, coll2.EnsureIndex(ctx, IndexInfo{
		Fields: []string{"x"},
		Unique: true,
	}))
	require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":"d1","x":["a","b"]}`)))
	err = coll2.Insert(ctx, anyenc.MustParseJson(`{"id":"d2","x":"b"}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
	assertCollCount(t, coll2, 1)
}

// TestIndex_UniqueArray_Coverage_CompoundTrailingField verifies that a
// compound unique index on (a,b) respects the full tuple. Docs that share
// the first field but differ in the second must both be accepted; a
// re-insertion of an exact tuple must then fail.
func TestIndex_UniqueArray_Coverage_CompoundTrailingField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Fields: []string{"a", "b"},
		Unique: true,
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":2}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":1,"b":3}`)))

	// Different id, but exact same (a,b) tuple as id:1 — must fail.
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":1,"b":2}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	assertCollCount(t, coll, 2)
	assertIndexLen(t, coll.GetIndexes()[0], 2)
}

// TestIndex_UniqueArray_Coverage_BooleanField verifies that a unique index
// on a boolean field distinguishes TypeTrue from TypeFalse. Two docs with
// active:true must collide; one true + one false are both accepted.
func TestIndex_UniqueArray_Coverage_BooleanField(t *testing.T) {
	fx := newFixture(t)

	t.Run("two trues collide", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_bool_collision")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Fields: []string{"active"},
			Unique: true,
		}))

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"active":true}`)))
		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"active":true}`))
		require.ErrorIs(t, err, ErrUniqueConstraint)

		assertCollCount(t, coll, 1)
	})

	t.Run("true and false both accepted", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_bool_mix")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Fields: []string{"active"},
			Unique: true,
		}))

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"active":true}`)))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"active":false}`)))

		assertCollCount(t, coll, 2)
		assertIndexLen(t, coll.GetIndexes()[0], 2)
	})
}

// --- Coverage tests from sparse_nested_coverage_test.go ---

// TestIndex_SparseNested_Coverage_MissingIntermediate verifies that when the
// intermediate path (meta) is null, a sparse index on meta.tags.name produces
// no index entries.
func TestIndex_SparseNested_Coverage_MissingIntermediate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Fields: []string{"meta.tags.name"},
		Sparse: true,
	}))

	// Intermediate path "meta" is null — entire leaf "meta.tags.name" is absent.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"p1","meta":null}`)))

	// Also try a document where the whole "meta" key is missing.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"p2"}`)))

	// And one where meta exists but tags is missing.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"p3","meta":{"x":1}}`)))

	// And one where tags is an array of objects but none have a "name" field.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"p4","meta":{"tags":[{"other":1}]}}`)))

	// A real hit so we also confirm the index otherwise works.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"p5","meta":{"tags":{"name":"go"}}}`)))

	assertCollCount(t, coll, 5)

	idx := coll.GetIndexes()[0]
	// Only p5's "go" entry should be in the index; p1..p4 all miss the leaf.
	// Note: p4's tags is an array of objects with only {"other":1} — the leaf
	// path "meta.tags.name" still resolves to nil, which sparse skips.
	n, err := idx.Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only p5 should have an index entry under sparse meta.tags.name")
}

// TestAudit12_SparseEmptyArray_* — focused edge-case audit for the
// interaction between a SPARSE index and an EMPTY ARRAY (e.g. {tags: []}).
//
// Background — index.go::writeValues (around line 213/227):
//
//	v := d.Get(idx.fieldPaths[i]...)
//	if idx.info.Sparse && (v == nil || v.Type() == anyenc.TypeNull) {
//	    return false
//	}
//
//	k := idx.keyBuf
//	if v != nil && v.Type() == anyenc.TypeArray {
//	    arr, _ := v.Array()
//	    if len(arr) != 0 {
//	        ... per-element loop ...
//	    }
//	}
//	idx.keyBuf = v.MarshalTo(k)
//	return idx.writeValues(d, i+1)
//
// Sparse-index semantics (matches MongoDB): "skip docs where the
// indexed field is MISSING or NULL". An empty array is neither — it
// is a present, queryable value. So the guard correctly only skips
// nil and TypeNull, and an empty array slips through to be indexed.
// The whole-array marshalled key is written, which makes
// Find({tags:[]}) work via the index.
//
// These tests pin the (intentional) behaviour: 0 entries for missing
// or null, exactly 1 entry for an empty array. If anybody changes the
// sparse guard to also skip empty arrays, they'd silently break
// Find({tags:[]}) queries against sparse indexes.

// TestAudit12_SparseEmptyArray_NoFieldZeroEntries: doc with no `tags`
// field at all. Sparse guard short-circuits on v == nil → 0 entries.
// Baseline for what "sparse" is supposed to do.
func TestAudit12_SparseEmptyArray_NoFieldZeroEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit12_no_field")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
		Sparse: true,
	}))

	// Doc with NO `tags` field — sparse guard hits v == nil branch.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	entries := readRawIndexEntries(t, fx.DB, "audit12_no_field", "ix_tags")
	require.Empty(t, entries,
		"sparse index over missing field must produce zero entries (v == nil branch)")

	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

// TestAudit12_SparseEmptyArray_NullFieldZeroEntries: doc with explicit
// `tags: null`. Sparse guard short-circuits on TypeNull → 0 entries.
// Baseline #2 — sparse semantics as documented.
func TestAudit12_SparseEmptyArray_NullFieldZeroEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit12_null_field")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
		Sparse: true,
	}))

	// Doc with explicit `tags: null` — sparse guard hits TypeNull branch.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":null}`)))

	entries := readRawIndexEntries(t, fx.DB, "audit12_null_field", "ix_tags")
	require.Empty(t, entries,
		"sparse index over null field must produce zero entries (TypeNull branch)")

	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

// TestAudit12_SparseEmptyArray_EmptyArrayBehaviour: doc with `tags: []`
// — empty array. Pins that an empty array IS indexed (sparse only
// skips missing/null, not empty values), and confirms the entry uses
// the whole-empty-array marshalled key with IndexValueScalar (because
// len(keysBuf) == 1 at insertKeys time — no per-element entries since
// the array has no elements).
//
// This makes Find({tags:[]}) work via the index — see
// TestAudit12_SparseEmptyArray_EmptyArrayQueryable below.
func TestAudit12_SparseEmptyArray_EmptyArrayBehaviour(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit12_empty_arr_sparse")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
		Sparse: true,
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":[]}`)))

	entries := readRawIndexEntries(t, fx.DB, "audit12_empty_arr_sparse", "ix_tags")

	// Empty array IS indexed — exactly 1 entry, the whole-empty-array
	// marshal. Matches MongoDB's sparse-index semantics: sparse skips
	// missing/null only, empty arrays are present queryable values.
	require.Len(t, entries, 1,
		"empty array on sparse index produces exactly 1 entry — the "+
			"whole-empty-array marshal. Sparse only skips missing/null per "+
			"the guard at index.go:227, which is intended.")

	// keysBuf had exactly one entry → IndexValueScalar (0x00).
	require.NotEmpty(t, entries[0].Value)
	assert.Equal(t, qplanner.IndexValueScalar, entries[0].Value,
		"len(keysBuf)==1 at insertKeys time → empty-array entry tagged IndexValueScalar (0x00)")
	assert.Zero(t, entries[0].Value[0]&qplanner.IndexEntryFlagMultiKey,
		"multi-key flag bit must be cleared (single key in keysBuf)")

	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

// TestAudit12_SparseEmptyArray_NonSparse: same empty-array doc, but on
// a NON-sparse index. Behaviour matches the sparse case for this
// input: 1 entry (the whole-array marshal). Confirms the empty-array
// handling is a property of writeValues, not of the sparse flag.
func TestAudit12_SparseEmptyArray_NonSparse(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit12_empty_arr_nonsparse")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
		// Sparse: false (default).
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":[]}`)))

	entries := readRawIndexEntries(t, fx.DB, "audit12_empty_arr_nonsparse", "ix_tags")

	require.Len(t, entries, 1,
		"non-sparse index + empty array also produces exactly 1 entry "+
			"(whole-array marshal). Confirms empty-array indexing is "+
			"independent of the sparse flag.")

	require.NotEmpty(t, entries[0].Value)
	assert.Equal(t, qplanner.IndexValueScalar, entries[0].Value,
		"len(keysBuf)==1 → empty-array entry on non-sparse index also tagged IndexValueScalar")
	assert.Zero(t, entries[0].Value[0]&qplanner.IndexEntryFlagMultiKey,
		"multi-key flag bit must be cleared (single key in keysBuf)")

	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

// TestAudit12_SparseEmptyArray_QueryReturnsZero: per-element queries
// against an empty-array doc return 0 — the stored entry is the
// whole-empty-array key, not any element key. Find({tags:"anything"})
// cannot match. To FIND the empty-array doc, query with the empty
// array directly (Find({tags:[]})) — see the next test.
func TestAudit12_SparseEmptyArray_QueryReturnsZero(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit12_empty_arr_query")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
		Sparse: true,
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":[]}`)))

	assertIndexLen(t, coll.GetIndexes()[0], 1)

	for _, probe := range []string{`"anything"`, `"foo"`, `""`, `null`} {
		count, qerr := coll.Find(`{"tags":` + probe + `}`).Count(ctx)
		require.NoError(t, qerr, "probe %s", probe)
		assert.Equalf(t, 0, count,
			"Find({tags: %s}).Count must be 0 — empty-array doc holds whole-array key, "+
				"not element keys; no element lookup can match it.",
			probe)
	}

	assertCollCount(t, coll, 1)
}

// TestAudit12_SparseEmptyArray_EmptyArrayQueryable verifies the
// positive direction of TestAudit12_SparseEmptyArray_QueryReturnsZero:
// querying with the empty array literally (`Find({tags:[]})`) DOES
// match the empty-array doc via the index entry. This is the
// motivation for indexing empty arrays in the first place — without
// it, this query would never use the index.
func TestAudit12_SparseEmptyArray_EmptyArrayQueryable(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit12_empty_arr_findable")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
		Sparse: true,
	}))

	// Mix: one empty-array doc, one nonempty for contrast.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"empty","tags":[]}`),
		anyenc.MustParseJson(`{"id":"nonempty","tags":["a","b"]}`),
	))

	// Querying for the empty array literally hits the whole-empty-array
	// index entry and returns the empty-array doc.
	n, err := coll.Find(`{"tags":[]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"Find({tags:[]}) must find the empty-array doc via the index entry")

	ids := collectField(t, coll.Find(`{"tags":[]}`), "id")
	assert.Equal(t, []string{`"empty"`}, ids,
		"Iter must yield the empty-array doc exactly once")

	// Element queries still match the nonempty doc (sanity check).
	na, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, na, "element query for 'a' matches the nonempty doc")
}

// TestAudit13_UniqueCompoundArray_* — focused edge-case audit for the
// most complex insert path: UNIQUE constraint + COMPOUND index + ARRAY
// dimension on the trailing field.
//
// Background — index.go::insertKeys around line 160:
//
//	for _, key := range idx.keysBuf {
//	    idx.fullKeyBuf = append(idx.fullKeyBuf[:0], key...)
//	    idx.fullKeyBuf = append(idx.fullKeyBuf, idKey...)
//
//	    if idx.info.Unique {
//	        var err error
//	        idx.seekBuf, err = tx.AppendSeekKey(idx.ns, key, idx.seekBuf[:0])
//	        if err == nil && bytes.HasPrefix(idx.seekBuf, key) {
//	            if !bytes.Equal(idx.seekBuf, idx.fullKeyBuf) {
//	                return ErrUniqueConstraint
//	            }
//	            continue // same doc, idempotent
//	        }
//	    }
//
//	    if err := tx.Put(idx.ns, idx.fullKeyBuf, entryValue); err != nil {
//	        return err
//	    }
//	    ...
//	}
//
// With a compound index on `Fields:["category","tags"]` where `tags` is an
// array, fillKeysBuf emits one key per array element PLUS a fall-through
// "whole array" key. The unique check above fires per-emitted-key, so a
// collision on ANY of the per-element compound tuples (e.g. (x,b)) rejects
// the whole insert — and because txErr propagates up to db.doWriteTx, the
// surrounding transaction rolls back, erasing any partial Put() from
// earlier in the loop.
//
// These tests pin:
//
//  1. NoCollision: docs whose compound tuples don't share any (cat,tag)
//     pair both insert. Every entry written must be IndexValueMultiKey
//     because the array dimension expands keysBuf > 1.
//  2. CollisionRejected: d2's per-element seek for (x,b) finds d1's
//     existing (x,b,d1) row; the prefix check fires → ErrUniqueConstraint.
//  3. CollisionRollsBack: d2's earlier successful Put for (x,b) — wait,
//     d2 hits the conflict on (x,b) immediately, so its FIRST emitted key
//     (x,b) is the one that errors. But d2 emits keys in array-iteration
//     order: tags=["b","c"] → first key (x,b) collides BEFORE (x,c) is
//     written. Either way, after rollback the namespace must contain ONLY
//     d1's entries — no orphaned d2 partial writes.
//  4. DifferentCategoriesNoCollide: same array element "b" but different
//     leading category — the compound prefix differs so no conflict.
//  5. IdempotentSelfReinsert: re-inserting the SAME (key, docId) tuple
//     hits the `continue` branch (seekBuf == fullKeyBuf). The user-facing
//     UpdateOne(d1) -> d1 short-circuits inside collection.update() via
//     anyencutil.Equal and never reaches insertKeys at all, so we ALSO
//     directly call insertKeys twice in a write-tx to truly exercise the
//     `continue` branch.
//  6. QueryFindByElement: confirms the unique compound + array index is
//     queryable per-element via the standard Find pipeline.

// TestAudit13_UniqueCompoundArray_NoCollision: d1 (x,[a,b]) + d2 (x,[c,d]).
// No shared (category, tag) tuple. Both inserts succeed; verify entry counts
// and that ALL entries carry IndexValueMultiKey (because each doc's keysBuf
// has > 1 entries from the array expansion).
func TestAudit13_UniqueCompoundArray_NoCollision(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit13_no_collision")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_cat_tags",
		Fields: []string{"category", "tags"},
		Unique: true,
	}))

	// d1: tags=["a","b"] → keysBuf entries (x,a), (x,b), (x,["a","b"]) = 3
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","category":"x","tags":["a","b"]}`)))
	// d2: tags=["c","d"] → keysBuf entries (x,c), (x,d), (x,["c","d"]) = 3
	// No shared (x,*) tuple with d1 → no unique conflict.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d2","category":"x","tags":["c","d"]}`)))

	assertCollCount(t, coll, 2)
	idx := coll.GetIndexes()[0]
	// 3 entries per doc * 2 docs = 6 entries.
	assertIndexLen(t, idx, 6)

	entries := readRawIndexEntries(t, fx.DB, "audit13_no_collision", "ix_cat_tags")
	require.Len(t, entries, 6,
		"compound + array: 2 docs * 3 keys/doc = 6 entries")

	// Every entry must carry IndexValueMultiKey because each doc's keysBuf
	// has 3 entries (>1) — the array dimension forces the multi-key tag on
	// EVERY emitted key for that doc, including the whole-array fall-through.
	for i, e := range entries {
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"entry %d: compound + array insert must write IndexValueMultiKey "+
				"because keysBuf > 1 from array expansion", i)
		require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"entry %d: multi-key flag bit must be set", i)
	}
}

// TestAudit13_UniqueCompoundArray_CollisionRejected: d1 (x,[a,b]) succeeds;
// d2 (x,[b,c]) must fail with ErrUniqueConstraint because d2's per-element
// emit of (x,b,d2) collides with d1's existing (x,b,d1) row on the unique
// (category,tags) tuple.
func TestAudit13_UniqueCompoundArray_CollisionRejected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit13_collision_rejected")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_cat_tags",
		Fields: []string{"category", "tags"},
		Unique: true,
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","category":"x","tags":["a","b"]}`)))

	// d2 emits (x,b,d2) — seek finds (x,b,d1), prefix matches, docIds
	// differ → ErrUniqueConstraint.
	err = coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d2","category":"x","tags":["b","c"]}`))
	require.ErrorIs(t, err, ErrUniqueConstraint,
		"compound (x,b) tuple must collide with d1 across docIds")
}

// TestAudit13_UniqueCompoundArray_CollisionRollsBack: after the failed d2
// insert, the collection must contain only d1; assert via Count and via
// raw-entry inspection (d2's partial entries — including any per-element
// Put() that succeeded BEFORE the colliding one — must NOT linger).
//
// This is the critical rollback-safety assertion: insertKeys returns the
// error to insertItem, which returns it to db.doWriteTx, which calls
// tx.Rollback(). If the namespace ever shows a stray d2 entry post-rollback,
// that's a bug in the transaction layer, not in insertKeys itself.
func TestAudit13_UniqueCompoundArray_CollisionRollsBack(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit13_rollback")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_cat_tags",
		Fields: []string{"category", "tags"},
		Unique: true,
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","category":"x","tags":["a","b"]}`)))

	// Snapshot pre-collision: 3 entries for d1.
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 3)
	preEntries := readRawIndexEntries(t, fx.DB, "audit13_rollback", "ix_cat_tags")
	require.Len(t, preEntries, 3,
		"pre-collision baseline: d1 must have exactly 3 index entries (a, b, [a,b])")

	// Trigger the collision. d2 emits (x,a) [no conflict] then (x,b)
	// [conflict with d1] OR (x,b) [conflict immediately] depending on
	// array iteration order — either way the whole tx must roll back.
	err = coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d2","category":"x","tags":["b","c"]}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// Doc-level assertion: only d1 in the collection.
	assertCollCount(t, coll, 1)

	// Index-level assertion: still exactly 3 entries (d1's), no orphans.
	assertIndexLen(t, idx, 3)
	postEntries := readRawIndexEntries(t, fx.DB, "audit13_rollback", "ix_cat_tags")
	require.Len(t, postEntries, 3,
		"post-rollback: namespace must hold ONLY d1's 3 entries — "+
			"any additional row would prove a partial d2 write leaked through rollback")

	// Stronger check: the entry bytes must be identical to the pre-collision
	// snapshot — same keys, same values. If d2 partially wrote anything, the
	// post-set wouldn't equal the pre-set.
	require.Equal(t, len(preEntries), len(postEntries),
		"entry count must be unchanged across the rolled-back tx")
	for i := range preEntries {
		assert.Equalf(t, preEntries[i].Key, postEntries[i].Key,
			"entry %d key changed after rolled-back collision", i)
		assert.Equalf(t, preEntries[i].Value, postEntries[i].Value,
			"entry %d value changed after rolled-back collision", i)
	}

	// Sanity: queries still find d1 by every original element of its array.
	count, err := coll.Find(`{"category":"x","tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "d1 must still match (category=x, tags=a)")
	count, err = coll.Find(`{"category":"x","tags":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "d1 must still match (category=x, tags=b)")

	// And the colliding tag value "c" (which only d2 had) must NOT match
	// anything — confirming d2 left no trace.
	count, err = coll.Find(`{"category":"x","tags":"c"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count,
		"tag 'c' belonged only to the rolled-back d2 — must match nothing")
}

// TestAudit13_UniqueCompoundArray_DifferentCategoriesNoCollide: d1 (x,[b])
// + d2 (y,[b]). Different category → the compound prefix differs ((x,b) vs
// (y,b)) so the unique seek for d2's (y,b) finds nothing matching the
// (y,b) prefix → no conflict. Both succeed.
func TestAudit13_UniqueCompoundArray_DifferentCategoriesNoCollide(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit13_diff_categories")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_cat_tags",
		Fields: []string{"category", "tags"},
		Unique: true,
	}))

	// d1: (x, ["b"]) → entries (x,b,d1), (x,["b"],d1) = 2
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","category":"x","tags":["b"]}`)))
	// d2: (y, ["b"]) → entries (y,b,d2), (y,["b"],d2) = 2.
	// Same tag value "b" but different leading "y" — unique tuple is the
	// FULL compound (category, tag), so no conflict.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d2","category":"y","tags":["b"]}`)))

	assertCollCount(t, coll, 2)
	idx := coll.GetIndexes()[0]
	// 2 entries per doc * 2 docs = 4 entries.
	assertIndexLen(t, idx, 4)

	// Verify both docs are queryable by the shared tag value, distinguished
	// by category.
	count, err := coll.Find(`{"category":"x","tags":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "(category=x, tags=b) must match only d1")
	count, err = coll.Find(`{"category":"y","tags":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "(category=y, tags=b) must match only d2")
}

// TestAudit13_UniqueCompoundArray_IdempotentSelfReinsert exercises the
// `continue` branch in insertKeys (seekBuf == fullKeyBuf path).
//
// Two-pronged coverage:
//
//  1. User-facing UpdateOne(d1) -> d1 with same data. The collection.update
//     fast-path short-circuits via anyencutil.Equal BEFORE insertKeys runs,
//     so the operation is a no-op. We assert it succeeds and changes
//     nothing — entry count unchanged, every entry still IndexValueMultiKey.
//
//  2. Direct double insertKeys() in a single write-tx. This is the only way
//     to actually reach the `continue` branch — second invocation finds the
//     existing entry whose (key, docId) matches fullKeyBuf bytewise and
//     skips the Put without erroring.
func TestAudit13_UniqueCompoundArray_IdempotentSelfReinsert(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit13_idempotent")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_cat_tags",
		Fields: []string{"category", "tags"},
		Unique: true,
	}))

	docJSON := `{"id":"d1","category":"x","tags":["a","b"]}`
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(docJSON)))

	idx := coll.GetIndexes()[0]
	// 3 entries: (x,a), (x,b), (x,["a","b"]).
	assertIndexLen(t, idx, 3)

	preEntries := readRawIndexEntries(t, fx.DB, "audit13_idempotent", "ix_cat_tags")
	require.Len(t, preEntries, 3)
	for i, e := range preEntries {
		require.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"pre-update entry %d must already be IndexValueMultiKey "+
				"(array dimension >1 keysBuf entries)", i)
	}

	// === Prong 1: UpdateOne to itself ===
	// collection.update short-circuits on anyencutil.Equal BEFORE insertKeys,
	// so this is effectively a no-op — but the public contract is that it
	// MUST succeed without raising ErrUniqueConstraint.
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(docJSON)),
		"updating d1 to identical data must succeed (short-circuit, no error)")

	// Entry count unchanged.
	assertIndexLen(t, idx, 3)

	postEntries := readRawIndexEntries(t, fx.DB, "audit13_idempotent", "ix_cat_tags")
	require.Len(t, postEntries, 3,
		"self-update must not alter the index entry count")

	// Every post-update entry must still carry IndexValueMultiKey, and the
	// raw bytes must match the pre-update snapshot exactly (no churn).
	for i, e := range postEntries {
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"post-update entry %d must still be IndexValueMultiKey", i)
		assert.Equalf(t, preEntries[i].Key, e.Key,
			"entry %d key changed across self-update", i)
		assert.Equalf(t, preEntries[i].Value, e.Value,
			"entry %d value changed across self-update", i)
	}

	// === Prong 2: Directly invoke insertKeys twice in one tx ===
	// This actually exercises the `continue` branch. We re-fetch the index
	// pointer because GetIndexes() returns the public Index interface; we
	// need the concrete *index for insertKeys.
	idxImpl := idx.(*index)
	it, itErr := newItem(anyenc.MustParseJson(docJSON))
	require.NoError(t, itErr)

	wrTx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	btWtx := wrTx.btreeWriteTx()

	// First call: would normally fail because the entries already exist
	// — but the unique check sees seekBuf == fullKeyBuf for the SAME docId
	// and takes `continue` instead of returning ErrUniqueConstraint.
	require.NoError(t, idxImpl.insertKeys(btWtx, it),
		"re-inserting the same (key, docId) tuples must hit `continue`, "+
			"not raise ErrUniqueConstraint")

	// Second consecutive call inside the same tx — still idempotent.
	require.NoError(t, idxImpl.insertKeys(btWtx, it),
		"second back-to-back insertKeys must also be a no-op via `continue`")

	// Namespace count must still be exactly 3 — no duplicates were added.
	count, err := btWtx.Count(idxImpl.ns)
	require.NoError(t, err)
	assert.Equal(t, 3, count,
		"idempotent re-insert(s) must not add any duplicate index rows")

	require.NoError(t, wrTx.Rollback())
}

// TestAudit13_UniqueCompoundArray_QueryFindByElement: insert d1, query
// Find({category:"x", tags:"a"}) → 1 doc; Find({category:"x", tags:"b"}) →
// 1 doc (same d1). Confirms the unique compound + array index is queryable
// per-element via the standard Find pipeline.
func TestAudit13_UniqueCompoundArray_QueryFindByElement(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit13_query")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_cat_tags",
		Fields: []string{"category", "tags"},
		Unique: true,
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","category":"x","tags":["a","b"]}`)))

	// Query by first array element.
	count, err := coll.Find(`{"category":"x","tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count,
		"Find({category:x, tags:a}) must match d1 via the (x,a,d1) index entry")

	// Query by second array element — must match the SAME d1.
	count, err = coll.Find(`{"category":"x","tags":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count,
		"Find({category:x, tags:b}) must match d1 via the (x,b,d1) index entry")

	// Query by tag NOT present on d1 — must match nothing.
	count, err = coll.Find(`{"category":"x","tags":"z"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "no doc has tags='z'")

	// Query by present tag but wrong category — compound prefix mismatch.
	count, err = coll.Find(`{"category":"y","tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count,
		"compound (category=y, tags=a) does not match d1's (x,a) tuple")
}

// ── Unique/sparse audit (act-05..12) ──
// act-05: Find().Update() enforces the unique constraint and rolls back.
// A matched doc whose indexed value is driven onto another doc's existing
// unique value funnels through collection.update -> insertBuf, whose unique
// check fires, returning ErrUniqueConstraint and rolling back the whole tx.
// ModifyResult is {Matched:1, Modified:0}; both old values stay queryable.
//
// REVEALS BUG: ErrUniqueConstraint IS returned (Matched==1, Modified==0), but
// the doc's DATA ROW write is partially committed instead of rolled back. Root
// cause: query.Update's deferred iterator.Close() calls tx.Commit() on the
// shared write tx before the error-path tx.Rollback() can run, committing the
// half-applied update (data row changed to a:1, index insert failed) and
// leaving data+index inconsistent. The asserted behavior below is the correct
// (rollback) behavior; it fails against the current code.
func TestIndex_Maintenance_FindUpdateUniqueConstraintEnforced(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1}`),
		anyenc.MustParseJson(`{"id":2,"a":2}`),
	))

	res, err := coll.Find(`{"a":2}`).Update(ctx, `{"$set":{"a":1}}`)
	require.ErrorIs(t, err, ErrUniqueConstraint)
	// matched before the constraint check fired; nothing committed.
	assert.Equal(t, 1, res.Matched)
	assert.Equal(t, 0, res.Modified)

	// doc 2 byte-for-byte unchanged.
	doc2, err := coll.FindId(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, `{"id":2,"a":2}`, doc2.Value().String())

	// both old values still queryable through the index.
	assertQueryCount(t, coll.Find(`{"a":1}`), 1)
	assertQueryCount(t, coll.Find(`{"a":2}`), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 2)
}

// act-06: Non-sparse UNIQUE index: a second missing/null-field doc collides on
// the shared 'null' key; the sparse+unique variant allows multiple missing docs.
func TestIndex_UniqueSparse_NonSparseUniqueNullCollision(t *testing.T) {
	fx := newFixture(t)

	t.Run("non-sparse collides on null", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "nonsparse")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":5}`)))
		// first missing-field doc -> first 'null' key, ok.
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"b":1}`)))
		// second missing-field doc -> collides on the shared 'null' key.
		require.ErrorIs(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"c":1}`)), ErrUniqueConstraint)
		// explicit null also marshals to the same 'null' key -> collides.
		require.ErrorIs(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":null}`)), ErrUniqueConstraint)

		assertCollCount(t, coll, 2)
		assertIndexLen(t, coll.GetIndexes()[0], 2)
	})

	t.Run("sparse-unique allows multiple missing/null", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "sparse")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true, Unique: true}))

		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"b":1}`)))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"c":1}`)))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":null}`)))

		assertCollCount(t, coll, 3)
		// all three emit zero entries on a sparse index.
		assertIndexLen(t, coll.GetIndexes()[0], 0)
	})
}

// act-07: EnsureIndex unique backfill fails on pre-existing missing-field
// duplicates (non-sparse), but the sparse variant succeeds with an empty index.
func TestIndex_UniqueSparse_CreateUniqueOnNullDuplicatesFailsButSparseSucceeds(t *testing.T) {
	fx := newFixture(t)

	t.Run("non-sparse backfill fails and registers no index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "nonsparse")
		require.NoError(t, err)
		// neither doc has field "a".
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"b":1}`),
			anyenc.MustParseJson(`{"id":2,"c":2}`),
		))

		err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true})
		require.ErrorIs(t, err, ErrUniqueConstraint)
		// backfill rolled back fully: no index registered, data intact.
		assert.Len(t, coll.GetIndexes(), 0)
		assertCollCount(t, coll, 2)
	})

	t.Run("sparse-unique backfill succeeds empty", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "sparse")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"b":1}`),
			anyenc.MustParseJson(`{"id":2,"c":2}`),
		))

		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true, Unique: true}))
		require.Len(t, coll.GetIndexes(), 1)
		// missing fields write zero 'null' keys -> empty index.
		assertIndexLen(t, coll.GetIndexes()[0], 0)
	})
}

// act-08: Compound UNIQUE update-to-duplicate fails and rolls back the doc and
// both index entries, restoring the (1,2,id2) entry with no orphan (1,1,id2).
func TestIndex_UniqueSparse_CompoundUpdateToDuplicateRollback(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1,"b":1}`),
		anyenc.MustParseJson(`{"id":2,"a":1,"b":2}`),
	))

	// drive (1,2) onto the existing (1,1) -> compound collision.
	err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":2,"a":1,"b":1}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// doc 2 byte-for-byte unchanged.
	doc2, err := coll.FindId(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, `{"id":2,"a":1,"b":2}`, doc2.Value().String())

	assertIndexLen(t, coll.GetIndexes()[0], 2)
	// only id:1 has (1,1); old (1,2) entry restored (proving no orphan + insert undone).
	assertQueryCount(t, coll.Find(`{"a":1,"b":1}`), 1)
	assertQueryCount(t, coll.Find(`{"a":1,"b":2}`), 1)
}

// act-09: Sparse+unique: removing the field via update frees the unique slot.
func TestIndex_UniqueSparse_SparseUniqueUpdateFreesSlot(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true, Unique: true}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"b":1}`),
	))
	// only doc1 (a=10) is indexed; doc2 (missing a) is not.
	assertIndexLen(t, coll.GetIndexes()[0], 1)

	// (1) doc2 cannot claim 10 while doc1 holds it.
	err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":2,"a":10}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
	assertIndexLen(t, coll.GetIndexes()[0], 1) // rollback-safe.

	// (2) drop the sparse field from doc1, freeing the unique value 10.
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"b":9}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// (3) doc2 can now claim 10.
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":2,"a":10}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 1)

	collectIds := func(q Query) []string {
		iter, iterErr := q.Iter(ctx)
		require.NoError(t, iterErr)
		defer func() { _ = iter.Close() }()
		var ids []string
		for iter.Next() {
			doc, docErr := iter.Doc()
			require.NoError(t, docErr)
			ids = append(ids, doc.Value().Get("id").String())
		}
		require.NoError(t, iter.Err())
		return ids
	}
	assertQueryCount(t, coll.Find(`{"a":10}`), 1)
	assert.Equal(t, []string{"2"}, collectIds(coll.Find(`{"a":10}`)))
}

// act-10: Find().Update() over multiple docs rolls back atomically when a later
// doc collides; no doc updated earlier in the loop is committed.
//
// REVEALS BUG: same partial-commit defect as act-05. ErrUniqueConstraint is
// returned, but the earlier-in-loop doc (id:1, u 1->100) is committed instead
// of rolled back, and the index is left with 2 entries instead of 3. The
// asserted atomic-rollback behavior is correct; it fails against current code.
func TestIndex_UniqueSparse_FindUpdateMultiDocCollisionRollsBack(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"u"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"grp":1,"u":1}`),
		anyenc.MustParseJson(`{"id":2,"grp":1,"u":2}`),
		anyenc.MustParseJson(`{"id":3,"grp":2,"u":100}`),
	))

	// id:1 increments u 1->100, colliding with id:3.u==100.
	_, err = coll.Find(`{"grp":1}`).Update(ctx, `{"$inc":{"u":99}}`)
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// no partial commit: every doc reverted / untouched.
	doc1, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, `{"id":1,"grp":1,"u":1}`, doc1.Value().String())

	doc2, err := coll.FindId(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, `{"id":2,"grp":1,"u":2}`, doc2.Value().String())

	doc3, err := coll.FindId(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, float64(100), doc3.Value().GetFloat64("u"))

	assertIndexLen(t, coll.GetIndexes()[0], 3)
}

// act-11: Unique ARRAY index: cross-doc element collision, then delete + reinsert
// frees the blocked value.
func TestIndex_UniqueSparse_UniqueArrayCollisionThenDeleteReinsert(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x"}, Unique: true}))

	// (1) id:1 -> 3 entries: "a","b",["a","b"].
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"x":["a","b"]}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 3)

	// (2) id:2 shares element "b" -> ErrUniqueConstraint, nothing committed.
	require.ErrorIs(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"x":["b","c"]}`)), ErrUniqueConstraint)
	assertIndexLen(t, coll.GetIndexes()[0], 3)
	assertCollCount(t, coll, 1)

	// (3) deleting id:1 removes ALL its per-element entries.
	require.NoError(t, coll.DeleteId(ctx, 1))
	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// (4) the previously-blocked array now inserts cleanly.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"x":["b","c"]}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 3)

	assertQueryCount(t, coll.Find(`{"x":"a"}`), 0)
	assertQueryCount(t, coll.Find(`{"x":"c"}`), 1)
	assertQueryCount(t, coll.Find(`{"x":"b"}`), 1)
}

// act-12: Sparse COMPOUND index, array leading field + missing trailing field
// => ZERO entries (no partial multikey leak from the started array branch).
func TestIndex_UniqueSparse_SparseCompoundArrayLeadMissingTrail(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}, Sparse: true}))

	// array lead, trailing field b missing -> every branch hits the sparse guard.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":[1,2],"c":9}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// both present -> 3 entries: (1,7),(2,7),([1,2],7).
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":[1,2],"b":7}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 3)

	// all matches are id:2; id:1 is not indexed.
	collectIds := func(q Query) []string {
		iter, iterErr := q.Iter(ctx)
		require.NoError(t, iterErr)
		defer func() { _ = iter.Close() }()
		var ids []string
		for iter.Next() {
			doc, docErr := iter.Doc()
			require.NoError(t, docErr)
			ids = append(ids, doc.Value().Get("id").String())
		}
		require.NoError(t, iter.Err())
		sort.Strings(ids)
		return ids
	}
	assertQueryCount(t, coll.Find(`{"a":1,"b":7}`), 1)
	assert.Equal(t, []string{"2"}, collectIds(coll.Find(`{"a":1,"b":7}`)))
	assertQueryCount(t, coll.Find(`{"a":2,"b":7}`), 1)
	assertQueryCount(t, coll.Find(`{"a":1}`), 1)
	assert.Equal(t, []string{"2"}, collectIds(coll.Find(`{"a":1}`)))
}
