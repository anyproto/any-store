package anystore

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

func TestCollection_PrimaryKey_ModifierUnsetsKey(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":"a","n":1}`)))

	// $unset of the primary-key field must return ErrDocWithoutId, not panic.
	_, err = coll.UpdateId(ctx, "a", query.MustParseModifier(`{"$unset":{"uuid":""}}`))
	assert.ErrorIs(t, err, ErrDocWithoutId)

	// The UpsertId insert path that $unsets the pk likewise errors, not panics.
	_, err = coll.UpsertId(ctx, "z", query.MustParseModifier(`{"$unset":{"uuid":""}}`))
	assert.ErrorIs(t, err, ErrDocWithoutId)
}

func TestCollection_PrimaryKey_DefaultId(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, "id", coll.PrimaryKey())
}

func TestCollection_PrimaryKey_Custom(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	assert.Equal(t, "uuid", coll.PrimaryKey())
}

func TestCollection_PrimaryKey_PersistedAcrossReopen(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	_, err = db.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db2, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer db2.Close()
	coll, err := db2.OpenCollection(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, "uuid", coll.PrimaryKey())
}

func TestCollection_PrimaryKey_Validation(t *testing.T) {
	fx := newFixture(t)
	_, err := fx.CreateCollection(ctx, "c1", CollectionOptions{PrimaryKey: "$bad"})
	require.Error(t, err)
	_, err = fx.CreateCollection(ctx, "c2", CollectionOptions{PrimaryKey: "a.b"})
	require.Error(t, err)
	_, err = fx.CreateCollection(ctx, "c3", CollectionOptions{PrimaryKey: "-foo"})
	require.Error(t, err)
}

func TestCollection_PrimaryKey_ImmutableMismatch(t *testing.T) {
	fx := newFixture(t)
	_, err := fx.Collection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	_, err = fx.Collection(ctx, "c", CollectionOptions{PrimaryKey: "other"})
	assert.ErrorIs(t, err, ErrPrimaryKeyMismatch)
	// Re-opening with the same key (or none) is fine.
	_, err = fx.Collection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
}

func TestCollection_PrimaryKey_RoundTrip(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
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
	assert.ErrorIs(t, err, ErrDocWithoutId)
}

func TestCollection_PrimaryKey_IntValue(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "key"})
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
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","color":"red"}`),
		anyenc.MustParseJson(`{"uuid":"b","color":"red"}`),
		anyenc.MustParseJson(`{"uuid":"c","color":"blue"}`),
	))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"color"}}))

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
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
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
func sortedInts(t *testing.T, coll Collection, sortField, intField string) []int {
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
func sortedStrings(t *testing.T, coll Collection, sortField, strField string) []string {
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
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
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

	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","color":"red","n":1}`),
		anyenc.MustParseJson(`{"uuid":"b","color":"red","n":2}`),
		anyenc.MustParseJson(`{"uuid":"c","color":"blue","n":3}`),
	))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"color"}}))
	require.NoError(t, db.Close())

	// Reopen: data keys + secondary index must round-trip through a fresh init.
	db2, err := Open(ctx, path, nil)
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
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":"a","email":"x@y.z"}`)))

	// Same email under a different primary key violates the unique constraint.
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":"b","email":"x@y.z"}`))
	assert.ErrorIs(t, err, ErrUniqueConstraint)

	// Re-inserting the same primary key is ErrDocExists, not a unique violation.
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":"a","email":"new@y.z"}`))
	assert.ErrorIs(t, err, ErrDocExists)

	// Deleting the holder frees the unique value for a new pk.
	require.NoError(t, coll.DeleteId(ctx, "a"))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"uuid":"b","email":"x@y.z"}`)))
}

func TestCollection_PrimaryKey_InOnPk(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
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
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
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
