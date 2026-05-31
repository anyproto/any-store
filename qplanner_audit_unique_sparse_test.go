package anystore

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

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
