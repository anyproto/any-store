package test

import (
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollection_PrimaryKey_ModifierUnsetsKey(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":"a","n":1}`)))

	// $unset of the primary-key field must return ErrDocWithoutId, not panic.
	_, err = coll.UpdateId(ctx, "a", query.MustParseModifier(`{"$unset":{"uuid":""}}`))
	assert.ErrorIs(t, err, anystore.ErrDocWithoutId)

	// The UpsertId insert path that $unsets the pk likewise errors, not panics.
	_, err = coll.UpsertId(ctx, "z", query.MustParseModifier(`{"$unset":{"uuid":""}}`))
	assert.ErrorIs(t, err, anystore.ErrDocWithoutId)
}

func TestCollection_PrimaryKey_DefaultId(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, "id", coll.PrimaryKey())
}

func TestCollection_PrimaryKey_Custom(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	assert.Equal(t, "uuid", coll.PrimaryKey())
}

func TestCollection_PrimaryKey_PersistedAcrossReopen(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	db, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	_, err = db.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db2, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer db2.Close()
	coll, err := db2.OpenCollection(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, "uuid", coll.PrimaryKey())
}

func TestCollection_PrimaryKey_Validation(t *testing.T) {
	fx := newFixture(t)
	_, err := fx.CreateCollection(ctx, "c1", anystore.CollectionOptions{PrimaryKey: "$bad"})
	require.Error(t, err)
	_, err = fx.CreateCollection(ctx, "c2", anystore.CollectionOptions{PrimaryKey: "a.b"})
	require.Error(t, err)
	_, err = fx.CreateCollection(ctx, "c3", anystore.CollectionOptions{PrimaryKey: "-foo"})
	require.Error(t, err)
}

func TestCollection_PrimaryKey_ImmutableMismatch(t *testing.T) {
	fx := newFixture(t)
	_, err := fx.Collection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	_, err = fx.Collection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "other"})
	assert.ErrorIs(t, err, anystore.ErrPrimaryKeyMismatch)
	// Re-opening with the same key (or none) is fine.
	_, err = fx.Collection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
}

func TestCollection_PrimaryKey_RoundTrip(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","n":1}`),
		anyenc.MustParseJson(`{"uuid":"b","n":2}`),
	))
	assertCollCount(t, coll, 2)

	doc, err := coll.FindId(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, 1, doc.Value().GetInt("n"))

	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"uuid":"a","n":11}`)))
	doc, err = coll.FindId(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, 11, doc.Value().GetInt("n"))

	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"uuid":"b","n":22}`)))
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"uuid":"c","n":3}`)))
	assertCollCount(t, coll, 3)

	require.NoError(t, coll.DeleteId(ctx, "c"))
	assertCollCount(t, coll, 2)

	// A document missing the primary-key field is rejected.
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"x","n":9}`))
	assert.ErrorIs(t, err, anystore.ErrDocWithoutId)
}

func TestCollection_PrimaryKey_IntValue(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "key"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"key":2,"n":20}`),
		anyenc.MustParseJson(`{"key":1,"n":10}`),
	))
	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, 10, doc.Value().GetInt("n"))
}

func TestCollection_PrimaryKey_Index(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","color":"red"}`),
		anyenc.MustParseJson(`{"uuid":"b","color":"red"}`),
		anyenc.MustParseJson(`{"uuid":"c","color":"blue"}`),
	))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"color"}}))

	n, err := coll.Find(`{"color":"red"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	// Deleting a doc must remove its index entry (entry suffix is the uuid key).
	require.NoError(t, coll.DeleteId(ctx, "a"))
	n, err = coll.Find(`{"color":"red"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestCollection_PrimaryKey_FilterByNonPkId(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","id":5}`),
		anyenc.MustParseJson(`{"uuid":"b","id":6}`),
	))

	// "id" is an ordinary field here (NOT the primary key). It must match by
	// value, not be treated as a data-namespace key seek.
	n, err := coll.Find(`{"id":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	iter, err := coll.Find(`{"id":5}`).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var got []string
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		got = append(got, d.Value().GetString("uuid"))
	}
	require.NoError(t, iter.Err())
	assert.Equal(t, []string{"a"}, got)

	// Filtering by the real primary key still works.
	n, err = coll.Find(`{"uuid":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

// sortedInts runs Find(nil).Sort(sortField) and collects intField from each doc.
func sortedInts(t *testing.T, coll anystore.Collection, sortField, intField string) []int {
	t.Helper()
	iter, err := coll.Find(nil).Sort(sortField).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []int
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		out = append(out, d.Value().GetInt(intField))
	}
	require.NoError(t, iter.Err())
	return out
}

// sortedStrings runs Find(nil).Sort(sortField) and collects strField from each doc.
func sortedStrings(t *testing.T, coll anystore.Collection, sortField, strField string) []string {
	t.Helper()
	iter, err := coll.Find(nil).Sort(sortField).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []string
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		out = append(out, d.Value().GetString(strField))
	}
	require.NoError(t, iter.Err())
	return out
}

func TestCollection_PrimaryKey_SortNonPkId(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	// Storage order is by uuid (a,b,c); their ids 3,1,2 are deliberately NOT in
	// storage order, so a wrong "natural order" optimization is observable.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","id":3}`),
		anyenc.MustParseJson(`{"uuid":"b","id":1}`),
		anyenc.MustParseJson(`{"uuid":"c","id":2}`),
	))

	// Sorting by the non-pk field "id" must sort by value, not storage order.
	assert.Equal(t, []int{1, 2, 3}, sortedInts(t, coll, "id", "id"))

	// Sorting by the real primary key yields natural (storage) order.
	assert.Equal(t, []string{"a", "b", "c"}, sortedStrings(t, coll, "uuid", "uuid"))
}

func TestCollection_PrimaryKey_ReopenWithData(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	db, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","color":"red","n":1}`),
		anyenc.MustParseJson(`{"uuid":"b","color":"red","n":2}`),
		anyenc.MustParseJson(`{"uuid":"c","color":"blue","n":3}`),
	))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"color"}}))
	require.NoError(t, db.Close())

	// Reopen: data keys + secondary index must round-trip through a fresh init.
	db2, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer db2.Close()
	coll2, err := db2.OpenCollection(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, "uuid", coll2.PrimaryKey())

	doc, err := coll2.FindId(ctx, "b")
	require.NoError(t, err)
	assert.Equal(t, 2, doc.Value().GetInt("n"))

	n, err := coll2.Find(`{"color":"red"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	require.NoError(t, coll2.UpdateOne(ctx, anyenc.MustParseJson(`{"uuid":"a","color":"red","n":11}`)))
	doc, err = coll2.FindId(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, 11, doc.Value().GetInt("n"))

	require.NoError(t, coll2.DeleteId(ctx, "c"))
	assertCollCount(t, coll2, 2)
}

func TestCollection_PrimaryKey_UniqueIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"email"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":"a","email":"x@y.z"}`)))

	// Same email under a different primary key violates the unique constraint.
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":"b","email":"x@y.z"}`))
	assert.ErrorIs(t, err, anystore.ErrUniqueConstraint)

	// Re-inserting the same primary key is ErrDocExists, not a unique violation.
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":"a","email":"new@y.z"}`))
	assert.ErrorIs(t, err, anystore.ErrDocExists)

	// Deleting the holder frees the unique value for a new pk.
	require.NoError(t, coll.DeleteId(ctx, "a"))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":"b","email":"x@y.z"}`)))
}

func TestCollection_PrimaryKey_InOnPk(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","n":1}`),
		anyenc.MustParseJson(`{"uuid":"b","n":2}`),
		anyenc.MustParseJson(`{"uuid":"c","n":3}`),
	))

	// $in over the primary key (id-bounds fast path) returns the right rows.
	n, err := coll.Find(`{"uuid":{"$in":["a","c"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	iter, err := coll.Find(`{"uuid":{"$in":["a","c"]}}`).Sort("uuid").Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var got []string
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		got = append(got, d.Value().GetString("uuid"))
	}
	require.NoError(t, iter.Err())
	assert.Equal(t, []string{"a", "c"}, got)
}

func TestCollection_PrimaryKey_QueryUpdateDelete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","grp":1,"n":1}`),
		anyenc.MustParseJson(`{"uuid":"b","grp":1,"n":2}`),
		anyenc.MustParseJson(`{"uuid":"c","grp":2,"n":3}`),
	))

	// UpdateMany via query exercises q.c.newItem on each modified value.
	res, err := coll.Find(`{"grp":1}`).Update(ctx, `{"$inc":{"n":10}}`)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Modified)
	doc, err := coll.FindId(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, 11, doc.Value().GetInt("n"))

	// DeleteMany via query removes index/data entries keyed by the pk.
	dres, err := coll.Find(`{"grp":1}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, dres.Modified)
	assertCollCount(t, coll, 1)
}

func TestCollection_PrimaryKey_ArrayRejected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)

	// Insert with an array pk fails on both default and custom pk fields.
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":[1,2],"n":1}`))
	assert.ErrorIs(t, err, anystore.ErrArrayPrimaryKey)

	// A batch containing one array-pk doc fails as a whole (tx rollback).
	err = coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"ok","n":1}`),
		anyenc.MustParseJson(`{"id":["bad"],"n":2}`),
	)
	assert.ErrorIs(t, err, anystore.ErrArrayPrimaryKey)
	assertCollCount(t, coll, 0)

	// UpsertOne and UpdateOne validate through the same item construction.
	err = coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":[3],"n":1}`))
	assert.ErrorIs(t, err, anystore.ErrArrayPrimaryKey)
	err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":[3],"n":1}`))
	assert.ErrorIs(t, err, anystore.ErrArrayPrimaryKey)

	// A modifier that rewrites the pk to an array is rejected on the update
	// and upsert-insert paths.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"a","n":1}`)))
	_, err = coll.UpdateId(ctx, "a", query.MustParseModifier(`{"$set":{"id":[1]}}`))
	assert.ErrorIs(t, err, anystore.ErrArrayPrimaryKey)
	_, err = coll.UpsertId(ctx, "z", query.MustParseModifier(`{"$set":{"id":[1]}}`))
	assert.ErrorIs(t, err, anystore.ErrArrayPrimaryKey)

	// Non-array pks stay legal: strings, numbers, and objects (whole-value
	// comparison semantics, no element-wise decoupling).
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1.5,"n":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":{"k":"v"},"n":1}`)))
}

func TestCollection_PrimaryKey_ArrayRejected_CustomPk(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", anystore.CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":[1,2],"n":1}`))
	assert.ErrorIs(t, err, anystore.ErrArrayPrimaryKey)
	// The default "id" field may still hold arrays when it is NOT the pk.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":"a","id":[1,2]}`)))
}

// These tests guard the Rename contract: it must re-key the btree namespaces and
// the handle registry, not just the catalog metadata. The reopen tests use a
// raw Open on a temp path (not newFixture) because they must survive a real
// close/open cycle.

func TestRename_ReopenAfterEviction(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1","a":10}`)))

	require.NoError(t, coll.Rename(ctx, "after"))
	require.NoError(t, coll.Close())

	reopened, err := fx.OpenCollection(ctx, "after")
	require.NoError(t, err)
	cnt, err := reopened.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	cnt, err = reopened.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// The bricked-state repro: pre-fix, a renamed collection was permanently
// unopenable after a DB reopen (the data namespace still carried the old name).
func TestRename_ReopenAfterDBReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := store.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1","a":10}`)))
	require.NoError(t, coll.Rename(ctx, "after"))
	require.NoError(t, store.Close())

	store, err = anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer store.Close()
	reopened, err := store.OpenCollection(ctx, "after")
	require.NoError(t, err)
	cnt, err := reopened.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	require.NoError(t, store.IntegrityCheck(ctx))
}

func TestRename_OldNameNotFound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := store.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.Rename(ctx, "after"))

	// With the renamed handle still cached.
	_, err = store.OpenCollection(ctx, "before")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)

	require.NoError(t, store.Close())
	store, err = anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer store.Close()
	_, err = store.OpenCollection(ctx, "before")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
}

func TestRename_CreateOldNameSucceeds(t *testing.T) {
	fx := newFixture(t)
	collA, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, collA.Rename(ctx, "b"))

	// The vacated name must be freely creatable (pre-fix the stale map key
	// blocked it) and isolated from the renamed collection.
	collA2, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, collA.Insert(ctx, anyenc.MustParseJson(`{"id":"in-b"}`)))
	require.NoError(t, collA2.Insert(ctx, anyenc.MustParseJson(`{"id":"in-a"}`)))
	_, err = collA2.FindId(ctx, "in-b")
	assert.ErrorIs(t, err, anystore.ErrDocNotFound)
	_, err = collA.FindId(ctx, "in-a")
	assert.ErrorIs(t, err, anystore.ErrDocNotFound)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

func TestRename_OntoExistingRejected(t *testing.T) {
	fx := newFixture(t)
	collA, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	collB, err := fx.CreateCollection(ctx, "b")
	require.NoError(t, err)
	require.NoError(t, collB.Insert(ctx, anyenc.MustParseJson(`{"id":"b-doc"}`)))

	// Pre-fix this silently overwrote b's catalog entry.
	assert.ErrorIs(t, collA.Rename(ctx, "b"), anystore.ErrCollectionExists)

	// Both collections stay fully usable.
	cnt, err := collB.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	require.NoError(t, collA.Insert(ctx, anyenc.MustParseJson(`{"id":"a-doc"}`)))
	assert.Equal(t, "a", collA.Name())
	require.NoError(t, fx.IntegrityCheck(ctx))
}

func TestRename_SameNameNoop(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "same")
	require.NoError(t, err)
	require.NoError(t, coll.Rename(ctx, "same"))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)))
	sameColl, err := fx.OpenCollection(ctx, "same")
	require.NoError(t, err)
	assert.Same(t, coll, sameColl)
}

// The handle registry re-keys only when the rename COMMITS: while the rename
// tx is open, opening the old name returns the same (still-committed) handle
// and the new name does not exist; after commit, the mapping flips. This is
// what prevents a concurrent OpenCollection(oldName) from registering a
// duplicate live handle from the still-committed old catalog.
func TestRename_MapRekeyAtCommit(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "old")
	require.NoError(t, err)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Rename(tx.Context(), "new"))

	// Pre-commit: the committed world still has "old".
	sameColl, err := fx.OpenCollection(ctx, "old")
	require.NoError(t, err)
	assert.Same(t, coll, sameColl, "pre-commit, the old name must resolve to the renaming handle")
	_, err = fx.OpenCollection(ctx, "new")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound, "pre-commit, the new name is not committed yet")

	require.NoError(t, tx.Commit())

	// Post-commit: the mapping flipped.
	renamed, err := fx.OpenCollection(ctx, "new")
	require.NoError(t, err)
	assert.Same(t, coll, renamed)
	_, err = fx.OpenCollection(ctx, "old")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

func TestRename_InvalidNamesRejected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	for _, bad := range []string{"", "_system", "ix:a:b", "ftx:a:b:map", "vix:a:b:meta"} {
		assert.ErrorIs(t, coll.Rename(ctx, bad), anystore.ErrInvalidCollectionName, "rename to %q", bad)
		_, err = fx.CreateCollection(ctx, bad)
		assert.ErrorIs(t, err, anystore.ErrInvalidCollectionName, "create %q", bad)
	}
	assert.Equal(t, "a", coll.Name())
}

// One test kills the metadata/namespace world-split for every index kind:
// range, unique, multikey (array field), full-text and vector entries must all
// be reachable through their indexes after a rename and a full DB reopen.
func TestRename_AllIndexKindsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := store.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		anystore.IndexInfo{Name: "a", Fields: []string{"a"}},
		anystore.IndexInfo{Name: "u", Fields: []string{"u"}, Unique: true},
		anystore.IndexInfo{Name: "tags", Fields: []string{"tags"}},
		anystore.IndexInfo{Name: "txt", Kind: anystore.IndexKindFulltext, Fields: []string{"body"}},
		anystore.IndexInfo{Name: "emb", Kind: anystore.IndexKindVector, Vector: &anystore.VectorParams{Field: "v", Dim: 4, Metric: anystore.VectorL2}},
	))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"1","a":10,"u":"x","tags":["red","blue"],"body":"quick brown fox","v":[1,0,0,0]}`),
		anyenc.MustParseJson(`{"id":"2","a":20,"u":"y","tags":["green"],"body":"lazy dog","v":[0,1,0,0]}`),
	))
	require.NoError(t, coll.Rename(ctx, "after"))
	require.NoError(t, store.Close())

	store, err = anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer store.Close()
	reopened, err := store.OpenCollection(ctx, "after")
	require.NoError(t, err)

	// Range.
	cnt, err := reopened.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "range index")
	// Unique constraint still enforced (metadata followed the rename).
	err = reopened.Insert(ctx, anyenc.MustParseJson(`{"id":"3","u":"x"}`))
	assert.ErrorIs(t, err, anystore.ErrUniqueConstraint, "unique index")
	// Multikey (array fan-out).
	cnt, err = reopened.Find(`{"tags":"blue"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "multikey index")
	// Full-text.
	cnt, err = reopened.Find(map[string]any{"$text": map[string]any{"$search": "fox"}}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "fts index")
	// Vector: nearest neighbour of [1,0,0,0] is doc 1.
	iter, err := reopened.Find(`{"v":{"$knn":{"$query":[1,0,0,0],"$k":1}}}`).Iter(ctx)
	require.NoError(t, err)
	require.True(t, iter.Next(), "vector index returned no hits")
	doc, err := iter.Doc()
	require.NoError(t, err)
	assert.Equal(t, "1", doc.Value().GetString("id"))
	require.NoError(t, iter.Close())

	require.NoError(t, store.IntegrityCheck(ctx))
}

// The closed flag is finally checked by operations: every op
// on a closed/dropped handle fails with ErrCollectionClosed, which also
// matches ErrCollectionNotFound.
func TestClosedHandleOpsFail(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1","a":1}`)))
	require.NoError(t, coll.Close())

	assertClosed := func(err error) {
		t.Helper()
		assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
		assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	}
	assertClosed(coll.Insert(ctx, anyenc.MustParseJson(`{"id":"2"}`)))
	_, err = coll.UpdateId(ctx, "1", nil)
	assertClosed(err)
	assertClosed(coll.DeleteId(ctx, "1"))
	_, err = coll.FindId(ctx, "1")
	assertClosed(err)
	_, err = coll.Find(`{"a":1}`).Count(ctx)
	assertClosed(err)
	_, err = coll.Find(`{"a":1}`).Iter(ctx)
	assertClosed(err)
	assertClosed(coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	assertClosed(coll.DropIndex(ctx, "a"))
	assertClosed(coll.Rename(ctx, "d"))
	assertClosed(coll.Drop(ctx))
	_, err = coll.Stats(ctx)
	assertClosed(err)

	// A fresh handle works.
	reopened, err := fx.OpenCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, reopened.Insert(ctx, anyenc.MustParseJson(`{"id":"2"}`)))

	// The same ops on a DROPPED handle behave identically.
	require.NoError(t, reopened.Drop(ctx))
	assertClosed(reopened.Insert(ctx, anyenc.MustParseJson(`{"id":"3"}`)))

	// After db.Close the established error takes precedence.
	fx2 := newFixture(t)
	coll2, err := fx2.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, fx2.Close())
	assert.ErrorIs(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)), anystore.ErrDBIsClosed)
}

// These tests guard the per-tx DDL undo log (commonTx.undo): DDL publishes
// in-memory schema state (openedCollections, index sets, names) at execution
// time, and a rollback of the enclosing scope must unwind those publications
// so no handle survives over a reverted (freed) catalog entry.

// The corruption sequence: create a collection inside an ambient tx, roll
// the outer tx back, let a later collection reuse the freed root page, then
// use the original name again. Pre-fix, the stale handle stayed registered and
// a write through it landed inside the OTHER collection while IntegrityCheck
// stayed green (any-store-tests:docs/any-store/repro/i11-stale-handle-rollback).
func TestCreateCollectionRollback_EvictsHandle(t *testing.T) {
	fx := newFixture(t)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	collX, err := fx.Collection(tx.Context(), "x")
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	// Create "y" first so it reuses the freed root page, then re-acquire "x":
	// it must be a FRESH collection (get-or-create against the reverted disk
	// state), not the stale handle.
	collY, err := fx.Collection(ctx, "y")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, collY.Insert(ctx, anyenc.MustParseJson(`{"id":"y-`+string(rune('a'+i%26))+string(rune('a'+i/26))+`"}`)))
	}

	collX2, err := fx.Collection(ctx, "x")
	require.NoError(t, err)
	assert.NotSame(t, collX, collX2, "rolled-back create must not return the stale handle")

	require.NoError(t, collX2.Insert(ctx, anyenc.MustParseJson(`{"id":"doc-in-x"}`)))

	// The insert must be visible in x and invisible in y.
	cntX, err := collX2.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cntX)
	cntY, err := collY.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 50, cntY)
	_, err = collY.FindId(ctx, "doc-in-x")
	assert.ErrorIs(t, err, anystore.ErrDocNotFound)

	require.NoError(t, fx.IntegrityCheck(ctx))
}

// Savepoint scope: only DDL inside the rolled-back nested tx unwinds; the
// outer tx (and DDL published before the savepoint) is untouched.
func TestCreateCollectionSavepointRollback_EvictsOnlyNestedDDL(t *testing.T) {
	fx := newFixture(t)

	outer, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	collA, err := fx.Collection(outer.Context(), "a")
	require.NoError(t, err)

	nested, err := fx.WriteTx(outer.Context())
	require.NoError(t, err)
	_, err = fx.Collection(nested.Context(), "b")
	require.NoError(t, err)
	require.NoError(t, nested.Rollback())

	// "a" (outer scope) stays registered and usable; "b" is gone.
	require.NoError(t, collA.Insert(outer.Context(), anyenc.MustParseJson(`{"id":"1"}`)))
	require.NoError(t, outer.Commit())

	names, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Contains(t, names, "a")
	assert.NotContains(t, names, "b")

	// "b" must be freshly creatable — a stale registration would return
	// ErrCollectionExists from CreateCollection.
	_, err = fx.CreateCollection(ctx, "b")
	require.NoError(t, err)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// EnsureIndex inside a rolled-back tx must unpublish the index from the
// in-memory set — otherwise later writes maintain an index over freed pages.
func TestEnsureIndexRollback_UnpublishesIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1","a":1}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(tx.Context(), anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, tx.Rollback())

	assert.Empty(t, coll.GetIndexes(), "rolled-back index must not stay published")

	// Writes must not touch the phantom index, and the index must be
	// creatable again from scratch.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"2","a":2}`)))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.Len(t, coll.GetIndexes(), 1)

	// The rebuilt index must cover all docs, including the one inserted while
	// the phantom was (correctly) absent.
	cnt, err := coll.Find(`{"a":{"$gte":0}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// DropIndex inside a rolled-back tx must restore the in-memory set — the
// on-disk index survives the rollback and would otherwise silently stop being
// maintained by later writes.
func TestDropIndexRollback_RestoresIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1","a":1}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.DropIndex(tx.Context(), "a"))
	require.NoError(t, tx.Rollback())

	require.Len(t, coll.GetIndexes(), 1, "rolled-back drop must restore the in-memory index set")

	// The restored index must keep being maintained: a doc inserted after the
	// rollback must be reachable through it.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"2","a":2}`)))
	explain, err := coll.Find(`{"a":2}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "a", "query should be able to use the restored index")
	cnt, err := coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// Savepoint scope: a rename inside a rolled-back nested tx unwinds while the
// outer tx stays alive and writable through the same handle.
func TestRenameSavepointRollback_UnwindsOnlyNested(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)

	outer, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	nested, err := fx.WriteTx(outer.Context())
	require.NoError(t, err)
	require.NoError(t, coll.Rename(nested.Context(), "b"))
	require.NoError(t, nested.Rollback())

	assert.Equal(t, "a", coll.Name())
	require.NoError(t, coll.Insert(outer.Context(), anyenc.MustParseJson(`{"id":"1"}`)))
	require.NoError(t, outer.Commit())

	names, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Contains(t, names, "a")
	assert.NotContains(t, names, "b")
	cnt, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// After a rolled-back rename the target name must be freely creatable and
// isolated from the original collection.
func TestRenameRollback_NewNameReusable(t *testing.T) {
	fx := newFixture(t)
	collA, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, collA.Rename(tx.Context(), "b"))
	require.NoError(t, tx.Rollback())

	collB, err := fx.CreateCollection(ctx, "b")
	require.NoError(t, err)
	require.NoError(t, collA.Insert(ctx, anyenc.MustParseJson(`{"id":"in-a"}`)))
	require.NoError(t, collB.Insert(ctx, anyenc.MustParseJson(`{"id":"in-b"}`)))
	_, err = collB.FindId(ctx, "in-a")
	assert.ErrorIs(t, err, anystore.ErrDocNotFound)
	_, err = collA.FindId(ctx, "in-b")
	assert.ErrorIs(t, err, anystore.ErrDocNotFound)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// Rename then Drop in one rolled-back tx: the undo log runs in reverse (Drop's
// eviction first, then the rename undo), and the original handle must come
// back alive under the old name — the "zombie heals itself" property of the
// identity-guarded undo.
func TestRenameThenDropRollback_HandleHeals(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Rename(tx.Context(), "b"))
	require.NoError(t, coll.Drop(tx.Context()))
	require.NoError(t, tx.Rollback())

	// The dropped-then-rolled-back handle is closed; a fresh open under the
	// old name must find the collection with its data intact.
	reopened, err := fx.OpenCollection(ctx, "a")
	require.NoError(t, err)
	cnt, err := reopened.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	require.NoError(t, reopened.Insert(ctx, anyenc.MustParseJson(`{"id":"2"}`)))

	names, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Contains(t, names, "a")
	assert.NotContains(t, names, "b")
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// The dual of the undo tests above: Drop's handle eviction is a COMMIT
// publication (commonTx.pubs). Evicting at execution time opened the same
// corruption through the concurrency side door: a concurrent
// OpenCollection during the uncommitted-drop window re-registered a fresh
// live handle against the still-committed catalog, the drop's commit freed
// its root pages, and a later insert through it landed inside whichever
// collection reused them.
func TestDropConcurrentOpen_NoDanglingHandle(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "x")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Drop(tx.Context()))

	// Window: the drop is uncommitted. A concurrent open must return the
	// registered (closed) handle — fail-safe — not register a fresh live one.
	// Probe it through the read path: a write would block on the write lock
	// the open drop tx holds.
	during, err := fx.OpenCollection(ctx, "x")
	require.NoError(t, err)
	_, err = during.FindId(ctx, "1")
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed)

	require.NoError(t, tx.Commit())

	// Committed: the handle is evicted and the catalog entry is gone.
	_, err = fx.OpenCollection(ctx, "x")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)

	// Let a new collection reuse the freed pages, then write through the
	// window handle: it must fail, and nothing may leak into "y".
	collY, err := fx.CreateCollection(ctx, "y")
	require.NoError(t, err)
	require.NoError(t, collY.Insert(ctx, anyenc.MustParseJson(`{"id":"y1"}`)))
	err = during.Insert(ctx, anyenc.MustParseJson(`{"id":"stray"}`))
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
	cnt, err := collY.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	_, err = collY.FindId(ctx, "stray")
	assert.ErrorIs(t, err, anystore.ErrDocNotFound)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// A rolled-back Drop must leave the handle exactly as it was: registered,
// open, and usable. Pre-fix, the error/rollback path left it closed and
// evicted (callers had to re-open by name to heal).
func TestDropRollback_HandleHeals(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "x")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Drop(tx.Context()))

	// Same-tx semantics: the handle is closed for the rest of the tx.
	err = coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"2"}`))
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed)

	require.NoError(t, tx.Rollback())

	// Healed: same handle, still registered, fully usable.
	again, err := fx.OpenCollection(ctx, "x")
	require.NoError(t, err)
	assert.Same(t, coll, again)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"2"}`)))
	cnt, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// Create→Drop in one tx, both outcomes: the reverse-order undos must net to
// closed+evicted on rollback, and the drop publication to evicted on commit.
func TestCreateThenDropSameTx(t *testing.T) {
	fx := newFixture(t)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	coll, err := fx.CreateCollection(tx.Context(), "x")
	require.NoError(t, err)
	require.NoError(t, coll.Drop(tx.Context()))
	require.NoError(t, tx.Rollback())
	_, err = fx.OpenCollection(ctx, "x")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`))
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed)

	tx2, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	coll2, err := fx.CreateCollection(tx2.Context(), "y")
	require.NoError(t, err)
	require.NoError(t, coll2.Drop(tx2.Context()))
	require.NoError(t, tx2.Commit())
	_, err = fx.OpenCollection(ctx, "y")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// Atomic drop-then-recreate in one tx: the deferred eviction leaves the
// closed handle registered, so the registry checks in CreateCollection /
// OpenCollection must look through it and let the catalog (which sees the
// tx's own delete) decide.
func TestDropThenRecreateSameTx(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "x")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"old"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Drop(tx.Context()))

	// Same tx: the name reads as gone...
	_, err = fx.OpenCollection(tx.Context(), "x")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	// ...and is creatable again.
	coll2, err := fx.CreateCollection(tx.Context(), "x")
	require.NoError(t, err)
	require.NoError(t, coll2.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"new"}`)))
	require.NoError(t, tx.Commit())

	reopened, err := fx.OpenCollection(ctx, "x")
	require.NoError(t, err)
	assert.Same(t, coll2, reopened)
	cnt, err := reopened.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	_, err = reopened.FindId(ctx, "new")
	require.NoError(t, err)
	_, err = reopened.FindId(ctx, "old")
	assert.ErrorIs(t, err, anystore.ErrDocNotFound)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// Drop-then-recreate whose tx rolls back: reverse-order undos evict the
// replacement and leave the original handle closed and unregistered (a revived
// unregistered handle would escape the staleness pass); a fresh open finds the
// original data.
func TestDropThenRecreateRollback(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "x")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"old"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Drop(tx.Context()))
	coll2, err := fx.CreateCollection(tx.Context(), "x")
	require.NoError(t, err)
	require.NoError(t, coll2.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"new"}`)))
	require.NoError(t, tx.Rollback())

	// Both tx-scoped handles are dead; a fresh open serves the old data.
	_, err = coll2.FindId(ctx, "new")
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
	reopened, err := fx.OpenCollection(ctx, "x")
	require.NoError(t, err)
	assert.NotSame(t, coll, reopened)
	assert.NotSame(t, coll2, reopened)
	_, err = reopened.FindId(ctx, "old")
	require.NoError(t, err)
	cnt, err := reopened.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// A user Close() racing in AFTER Drop's closed-flip is swallowed by the CAS;
// the rollback undo must honor it — evict, stay closed — not resurrect the
// handle its owner released.
func TestDropRollback_UserCloseDuringWindowSticks(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "x")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Drop(tx.Context()))
	require.NoError(t, coll.Close())
	require.NoError(t, tx.Rollback())

	// Closed sticks; a fresh open works against the restored collection.
	_, err = coll.FindId(ctx, "1")
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
	reopened, err := fx.OpenCollection(ctx, "x")
	require.NoError(t, err)
	assert.NotSame(t, coll, reopened)
	_, err = reopened.FindId(ctx, "1")
	require.NoError(t, err)
}
