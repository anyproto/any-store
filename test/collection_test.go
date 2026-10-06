package test

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
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

// A handle opened through a read transaction is not built from that
// transaction's snapshot for everyone else: a schema change this process
// committed after the snapshot is in force for every later transaction.

// An index created after the snapshot is maintained by writes through the
// handle.
func TestOpenThroughOlderReadTx_IndexCreatedSince(t *testing.T) {
	for _, kind := range []string{"range", "fulltext"} {
		t.Run(kind, func(t *testing.T) {
			fx := newFixture(t)
			a, err := fx.CreateCollection(ctx, "a")
			require.NoError(t, err)
			require.NoError(t, a.Close())

			rtx, err := fx.ReadTx(ctx)
			require.NoError(t, err)
			h0, err := fx.OpenCollection(ctx, "a")
			require.NoError(t, err)
			info := anystore.IndexInfo{Name: "ix", Fields: []string{"body"}}
			find := `{"body":"alpha"}`
			if kind == "fulltext" {
				info.Kind = anystore.IndexKindFulltext
				find = `{"$text":{"$search":"alpha"}}`
			}
			require.NoError(t, h0.EnsureIndex(ctx, info))
			require.NoError(t, h0.Close())

			h, err := fx.OpenCollection(rtx.Context(), "a")
			require.NoError(t, err)
			// The reader's own view has neither the index nor a document.
			n, err := h.Count(rtx.Context())
			require.NoError(t, err)
			assert.Zero(t, n)
			require.NoError(t, rtx.Commit())

			require.NoError(t, h.Insert(ctx, anyenc.MustParseJson(`{"id":1,"body":"alpha"}`)))
			require.Len(t, h.GetIndexes(), 1)
			n, err = h.Find(find).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 1, n, "the document was committed without its index entry")

			require.NoError(t, h.Close())
			fresh, err := fx.OpenCollection(ctx, "a")
			require.NoError(t, err)
			n, err = fresh.Find(find).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			require.NoError(t, fx.IntegrityCheck(ctx))
		})
	}
}

// An index dropped after the snapshot is not written to: its root pages are
// free, or another tree's by now.
func TestOpenThroughOlderReadTx_IndexDroppedSince(t *testing.T) {
	fx := newFixture(t)
	a, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, a.EnsureIndex(ctx, anystore.IndexInfo{Name: "k", Fields: []string{"k"}}))
	for i := range 5 {
		require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d}`, i, i))))
	}
	require.NoError(t, a.Close())

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	h0, err := fx.OpenCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, h0.DropIndex(ctx, "k"))
	require.NoError(t, h0.Close())

	h, err := fx.OpenCollection(rtx.Context(), "a")
	require.NoError(t, err)
	// The reader's snapshot still has the index, and plans with it.
	n, err := h.Find(`{"k":{"$gte":3}}`).IndexHint(anystore.IndexHint{IndexName: "k", Boost: 1_000_000}).Count(rtx.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	require.NoError(t, rtx.Commit())
	assert.Empty(t, h.GetIndexes())

	// Whatever reuses the freed pages must stay intact.
	other, err := fx.CreateCollection(ctx, "other")
	require.NoError(t, err)
	require.NoError(t, other.EnsureIndex(ctx, anystore.IndexInfo{Name: "k", Fields: []string{"k"}}))
	for i := range 5 {
		require.NoError(t, other.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d}`, i, i))))
	}
	for i := 5; i < 10; i++ {
		require.NoError(t, h.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d}`, i, i))))
	}
	n, err = other.Find(`{"k":{"$gte":0}}`).IndexHint(anystore.IndexHint{IndexName: "k", Boost: 1_000_000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	n, err = h.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, n)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// The collection was dropped and another one created under its name after
// the snapshot: the handle the older transaction opens stands for the
// collection of its snapshot and for nothing else. It never carries that
// collection's primary key and indexes over the new collection's data.
func TestOpenThroughOlderReadTx_RecreatedSince(t *testing.T) {
	fx := newFixture(t)
	a, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, a.EnsureIndex(ctx, anystore.IndexInfo{Name: "k", Fields: []string{"k"}}))
	require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(`{"id":1,"k":5}`)))
	require.NoError(t, a.Close())

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	h0, err := fx.OpenCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, h0.Drop(ctx))
	n, err := fx.CreateCollection(ctx, "a", anystore.CollectionOptions{PrimaryKey: "uid"})
	require.NoError(t, err)
	require.NoError(t, n.Insert(ctx, anyenc.MustParseJson(`{"uid":"x","k":7}`)))
	require.NoError(t, n.Close())

	h, err := fx.OpenCollection(rtx.Context(), "a")
	require.NoError(t, err)
	doc, err := h.FindId(rtx.Context(), 1)
	require.NoError(t, err, "the reader's snapshot has the dropped collection")
	assert.Equal(t, 5, doc.Value().GetInt("k"))
	require.NoError(t, rtx.Commit())

	// Outside that snapshot the collection it stands for is gone.
	err = h.Insert(ctx, anyenc.MustParseJson(`{"id":2,"uid":"y","k":9}`))
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)

	fresh, err := fx.OpenCollection(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, "uid", fresh.PrimaryKey())
	assert.Empty(t, fresh.GetIndexes())
	cnt, err := fresh.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// One collection has one handle, whatever name a transaction knows it by. An
// older read transaction knows the collection under the name the open write
// transaction has just renamed it back to: a second handle registered under
// that name would be used by the writer beside the first, and each would
// miss the other's schema changes.
func TestOpenThroughOlderReadTx_NameTheOpenWriteTxGaveIt(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	for i := range 5 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d}`, i, i))))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { _ = rtx.Commit() }()
	require.NoError(t, coll.Rename(ctx, "b"))

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Rename(wtx.Context(), "a"))
	require.NoError(t, coll.EnsureIndex(wtx.Context(), anystore.IndexInfo{Name: "k", Fields: []string{"k"}}))

	h, err := fx.OpenCollection(rtx.Context(), "a")
	require.NoError(t, err)
	require.True(t, h == coll, "a second handle for the collection")
	n, err := h.Count(rtx.Context())
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// The writer changes the schema through one and writes through the other.
	require.NoError(t, h.EnsureIndex(wtx.Context(), anystore.IndexInfo{Name: "j", Fields: []string{"j"}}))
	require.NoError(t, coll.DropIndex(wtx.Context(), "k"))
	require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":5,"k":5,"j":5}`)))
	require.NoError(t, h.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":6,"k":6,"j":6}`)))
	require.NoError(t, wtx.Commit())

	n, err = coll.Find(`{"j":{"$gte":5}}`).IndexHint(anystore.IndexHint{IndexName: "j", Boost: 1_000_000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// A transaction older than a collection does not find it, whoever holds it
// open, and reads nothing through a handle to it.
func TestOlderReadTx_CollectionCreatedSince(t *testing.T) {
	fx := newFixture(t)
	pad, err := fx.CreateCollection(ctx, "pad")
	require.NoError(t, err)
	require.NoError(t, pad.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { _ = rtx.Commit() }()

	n, err := fx.CreateCollection(ctx, "n")
	require.NoError(t, err)
	require.NoError(t, n.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	// Held open by someone, or not: the answer is the snapshot's.
	_, err = fx.OpenCollection(rtx.Context(), "n")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	_, err = n.FindId(rtx.Context(), 1)
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	_, err = n.Count(rtx.Context())
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	_, err = n.Find(nil).Iter(rtx.Context())
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	require.NoError(t, n.Close())
	_, err = fx.OpenCollection(rtx.Context(), "n")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)

	// The handle is fine for everyone at the present state.
	n, err = fx.OpenCollection(ctx, "n")
	require.NoError(t, err)
	cnt, err := n.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
}

// The handle of a collection recreated under the same name does not serve the
// documents of the one an older transaction still sees there.
func TestOlderReadTx_CollectionRecreatedSince(t *testing.T) {
	fx := newFixture(t)
	a, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	for i := range 3 {
		require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { _ = rtx.Commit() }()

	require.NoError(t, a.Drop(ctx))
	n, err := fx.CreateCollection(ctx, "a", anystore.CollectionOptions{PrimaryKey: "uid"})
	require.NoError(t, err)
	require.NoError(t, n.Insert(ctx, anyenc.MustParseJson(`{"uid":1}`)))

	_, err = n.FindId(rtx.Context(), 1)
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	_, err = n.Count(rtx.Context())
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	_, err = fx.OpenCollection(rtx.Context(), "a")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)

	cnt, err := n.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
}

// A collection created in an open write transaction is that transaction's
// alone until it commits: a caller around the transaction finds nothing
// under a handle to it, and no page of an uncommitted tree.
func TestReadAroundWriteTx_CollectionCreatedInIt(t *testing.T) {
	fx := newFixture(t)
	pad, err := fx.CreateCollection(ctx, "pad")
	require.NoError(t, err)
	require.NoError(t, pad.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	n, err := fx.CreateCollection(wtx.Context(), "n")
	require.NoError(t, err)
	require.NoError(t, n.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":1}`)))

	_, err = n.FindId(ctx, 1)
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	_, err = n.Count(ctx)
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	_, err = n.Find(`{"id":{"$gte":0}}`).Count(ctx)
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	// The transaction itself reads what it wrote.
	cnt, err := n.Count(wtx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)

	require.NoError(t, wtx.Commit())
	cnt, err = n.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
}

// Name and GetIndexes take no context, so they cannot answer for a
// transaction: they report the committed schema. A rename or an index made
// in an open transaction shows once it commits, and never if it rolls back.
func TestAccessorsReportCommittedSchema(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":1}`)))
	indexNames := func() []string {
		var names []string
		for _, idx := range coll.GetIndexes() {
			names = append(names, idx.Info().Name)
		}
		return names
	}

	for _, commit := range []bool{false, true} {
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(tx.Context(), anystore.IndexInfo{Name: "b", Fields: []string{"b"}}))
		require.NoError(t, coll.DropIndex(tx.Context(), "a"))
		require.NoError(t, coll.Rename(tx.Context(), "after"))
		assert.Equal(t, "before", coll.Name())
		assert.Equal(t, []string{"a"}, indexNames())
		// The transaction works with what it made.
		n, err := coll.Find(`{"b":1}`).IndexHint(anystore.IndexHint{IndexName: "b", Boost: 1_000_000}).Count(tx.Context())
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		reopened, err := fx.OpenCollection(tx.Context(), "after")
		require.NoError(t, err)
		assert.True(t, reopened == coll)

		if !commit {
			require.NoError(t, tx.Rollback())
			assert.Equal(t, "before", coll.Name())
			assert.Equal(t, []string{"a"}, indexNames())
			continue
		}
		require.NoError(t, tx.Commit())
		assert.Equal(t, "after", coll.Name())
		assert.Equal(t, []string{"b"}, indexNames())
	}
}

// An Index stands for a name: its length is that of the index the caller's
// transaction has under it — not of the trees it was listed with, which a
// drop frees.
func TestIndexLen_OfTheCallersTransaction(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))
	for i := range 3 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}
	listed := coll.GetIndexes()
	require.Len(t, listed, 1)

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { _ = rtx.Commit() }()

	require.NoError(t, coll.DropIndex(ctx, "a"))
	// Whatever takes the freed pages is not this index.
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "b", Fields: []string{"b"}}))
	for i := 3; i < 10; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"b":%d}`, i, i))))
	}

	_, err = listed[0].Len(ctx)
	assert.ErrorIs(t, err, anystore.ErrIndexNotFound)
	n, err := listed[0].Len(rtx.Context())
	require.NoError(t, err)
	assert.Equal(t, 3, n, "the older transaction still has the index")

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))
	n, err = listed[0].Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, n, "the index of that name as it is now: an entry per document")
}

// A read transaction that began before a rename keeps reading through the
// handle it holds: the data tree and the documents are the ones its snapshot
// has, whatever the collection is called by now.
func TestRename_OlderReaderKeepsReadingHeldHandle(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))
	for i := range 3 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { _ = rtx.Commit() }()

	require.NoError(t, coll.Rename(ctx, "after"))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":30}`)))

	cnt, err := coll.Count(rtx.Context())
	require.NoError(t, err)
	assert.Equal(t, 3, cnt, "the reader's snapshot predates the fourth document")
	doc, err := coll.FindId(rtx.Context(), 1)
	require.NoError(t, err)
	assert.Equal(t, 10, doc.Value().GetInt("a"))
	cnt, err = coll.Find(`{"a":{"$gte":10}}`).Count(rtx.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
	_, err = coll.FindId(rtx.Context(), 3)
	assert.ErrorIs(t, err, anystore.ErrDocNotFound)
}

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
	assertClosed(coll.CompactVectorIndex(ctx, "emb"))
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

// These tests guard the rollback of DDL: a rollback of the enclosing scope
// discards the transaction's schema changes, and no handle survives over a
// reverted (freed) catalog entry.

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

// Rename then Drop in one rolled-back tx: the handle is alive under the old
// name, as it was throughout for everyone outside the transaction.
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

// An uncommitted Drop is the dropping transaction's own: a concurrent
// OpenCollection during its window gets the registered handle, which keeps
// serving the committed collection, and the commit closes it. A second
// handle registered in the window would be the old corruption: the drop's
// commit frees the root pages, and a later insert through it lands inside
// whichever collection reuses them.
func TestDropConcurrentOpen_NoDanglingHandle(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "x")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Drop(tx.Context()))

	// Window: the drop is uncommitted. A concurrent open must return the
	// registered handle, which still reads the committed collection — not
	// register a fresh one. Probe it through the read path: a write would
	// block on the write lock the open drop tx holds.
	during, err := fx.OpenCollection(ctx, "x")
	require.NoError(t, err)
	assert.True(t, during == coll, "the registered handle")
	_, err = during.FindId(ctx, "1")
	require.NoError(t, err)
	_, err = coll.FindId(tx.Context(), "1")
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed, "dropped for the dropping transaction")

	require.NoError(t, tx.Commit())
	_, err = during.FindId(ctx, "1")
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed)

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

// Create→Drop in one tx, both outcomes: the handle ends closed and out of
// the registry either way.
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

// Drop-then-recreate whose tx rolls back: the replacement is closed and the
// original handle is whole again, as after any rolled-back Drop.
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

	_, err = coll2.FindId(ctx, "new")
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
	_, err = coll.FindId(ctx, "old")
	require.NoError(t, err)
	reopened, err := fx.OpenCollection(ctx, "x")
	require.NoError(t, err)
	assert.True(t, reopened == coll, "the original handle must be registered again")
	assertCollCount(t, reopened, 1)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// A collection created in a write tx is dropped and recreated inside a
// savepoint that rolls back. The creating handle is the collection's handle
// again, so the rollback of the tx closes whatever was opened meanwhile.
func TestDropThenRecreateInSavepointRollback(t *testing.T) {
	fx := newFixture(t)
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	r, err := fx.CreateCollection(tx.Context(), "r")
	require.NoError(t, err)

	sp, err := fx.WriteTx(tx.Context())
	require.NoError(t, err)
	require.NoError(t, r.Drop(sp.Context()))
	_, err = fx.CreateCollection(sp.Context(), "r")
	require.NoError(t, err)
	require.NoError(t, sp.Rollback())

	h, err := fx.OpenCollection(tx.Context(), "r")
	require.NoError(t, err)
	assert.True(t, h == r, "the creating handle must be the one opened")
	require.NoError(t, h.Insert(tx.Context(), ddlDoc(1)))
	require.NoError(t, tx.Rollback())

	other, err := fx.CreateCollection(ctx, "other")
	require.NoError(t, err)
	assert.ErrorIs(t, h.Insert(ctx, ddlDoc(2)), anystore.ErrCollectionClosed)
	_, err = fx.OpenCollection(ctx, "r")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	assertCollCount(t, other, 0)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// A Close() of a dropped handle while the Drop's tx is open waits for the tx
// to end; after the rollback the handle is closed, not resurrected.
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
	assert.False(t, reopened == coll, "the closed handle must have left the registry")
	_, err = reopened.FindId(ctx, "1")
	require.NoError(t, err)
}

// An open of the collection after that Close() gets the dropped handle back
// and cancels the close: the handle reads the committed collection
// throughout, and the rollback leaves it whole.
func TestDropRollback_OpenAfterCloseKeepsHandle(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "x")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Drop(tx.Context()))
	require.NoError(t, coll.Close())
	h, err := fx.OpenCollection(ctx, "x")
	require.NoError(t, err)
	assert.True(t, h == coll, "an open around the dropping tx must return the dropped handle")
	_, err = h.FindId(ctx, "1")
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	_, err = h.FindId(ctx, "1")
	require.NoError(t, err)
	again, err := fx.OpenCollection(ctx, "x")
	require.NoError(t, err)
	assert.True(t, again == coll, "the cancelled Close() left the handle registered")
}

var (
	ddlFtIdx = anystore.IndexInfo{Name: "ft", Kind: anystore.IndexKindFulltext, Fields: []string{"body"}}
	ddlKIdx  = anystore.IndexInfo{Name: "k", Fields: []string{"k"}}
)

func ddlDoc(id int) *anyenc.Value {
	return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d,"body":"alpha"}`, id, id))
}

func indexNames(c anystore.Collection) []string {
	names := []string{}
	for _, idx := range c.GetIndexes() {
		names = append(names, idx.Info().Name)
	}
	slices.Sort(names)
	return names
}

// assertIndexesHold closes h and checks, through a handle built from the
// committed state, that collection name holds docs documents, has exactly the
// indexes named, and that each of them covers every document.
func assertIndexesHold(t *testing.T, fx *fixture, h anystore.Collection, name string, docs int, indexes ...string) {
	t.Helper()
	require.NoError(t, h.Close())
	fresh, err := fx.OpenCollection(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, append([]string{}, indexes...), indexNames(fresh))
	assertCollCount(t, fresh, docs)
	for _, idx := range fresh.GetIndexes() {
		if idx.Info().Kind == anystore.IndexKindFulltext {
			assertQueryCount(t, fresh.Find(`{"$text":{"$search":"alpha"}}`), docs)
			continue
		}
		assertIndexLen(t, idx, docs)
	}
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// A write tx changes a collection's schema, the handle that made the change
// is closed and the collection is opened again inside the tx. The handle that
// open returns follows the outcome of the tx.
func TestReopenAfterDDLInTx(t *testing.T) {
	t.Run("rolled-back DropIndex", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(ctx, ddlFtIdx))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.DropIndex(tx.Context(), "ft"))
		require.NoError(t, a.Close())
		h, err := fx.OpenCollection(tx.Context(), "a")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		assert.Equal(t, []string{"ft"}, indexNames(h))
		require.NoError(t, h.Insert(ctx, ddlDoc(1)))
		assertIndexesHold(t, fx, h, "a", 1, "ft")
	})

	t.Run("rolled-back CreateCollection", func(t *testing.T) {
		fx := newFixture(t)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		r, err := fx.CreateCollection(tx.Context(), "r")
		require.NoError(t, err)
		require.NoError(t, r.Close())
		h, err := fx.OpenCollection(tx.Context(), "r")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		// The collection is gone and so is its handle: nothing it writes can
		// land in the pages the next collection takes.
		other, err := fx.CreateCollection(ctx, "other")
		require.NoError(t, err)
		assert.ErrorIs(t, h.Insert(ctx, ddlDoc(1)), anystore.ErrCollectionClosed)
		_, err = fx.OpenCollection(ctx, "r")
		assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
		assertCollCount(t, other, 0)
		require.NoError(t, fx.IntegrityCheck(ctx))
	})

	t.Run("rolled-back EnsureIndex", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)
		for i := range 5 {
			require.NoError(t, a.Insert(ctx, ddlDoc(i)))
		}

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx))
		require.NoError(t, a.Close())
		h, err := fx.OpenCollection(tx.Context(), "a")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		// The index is gone from the handle too: its pages are free for the
		// next collection, and a write through the handle must not reach them.
		assert.Empty(t, indexNames(h))
		other, err := fx.CreateCollection(ctx, "other")
		require.NoError(t, err)
		for i := range 5 {
			require.NoError(t, other.Insert(ctx, ddlDoc(100+i)))
		}
		require.NoError(t, h.Insert(ctx, ddlDoc(50)))
		assertCollCount(t, other, 5)
		assertCollCountInTx(ctx, t, other, 5)
		assertIndexesHold(t, fx, h, "a", 6)
	})

	t.Run("rolled-back savepoint", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(ctx, ddlFtIdx))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, a.DropIndex(sp.Context(), "ft"))
		require.NoError(t, a.Close())
		h, err := fx.OpenCollection(sp.Context(), "a")
		require.NoError(t, err)
		require.NoError(t, sp.Rollback())
		require.NoError(t, h.Insert(tx.Context(), ddlDoc(1)))
		require.NoError(t, tx.Commit())

		assertIndexesHold(t, fx, h, "a", 1, "ft")
	})

	t.Run("rolled-back Rename", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(ctx, ddlKIdx))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.Rename(tx.Context(), "b"))
		require.NoError(t, a.Close())
		h, err := fx.OpenCollection(tx.Context(), "b")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		assert.Equal(t, "a", h.Name())
		_, err = fx.OpenCollection(ctx, "b")
		assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
		_, err = fx.CreateCollection(ctx, "b")
		require.NoError(t, err)
		require.NoError(t, h.Insert(ctx, ddlDoc(1)))
		assertIndexesHold(t, fx, h, "a", 1, "k")
	})

	t.Run("rolled-back Drop of the reopened handle", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)
		for i := range 5 {
			require.NoError(t, a.Insert(ctx, ddlDoc(i)))
		}

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx))
		require.NoError(t, a.Close())
		h, err := fx.OpenCollection(tx.Context(), "a")
		require.NoError(t, err)
		require.NoError(t, h.Drop(tx.Context()))
		require.NoError(t, tx.Rollback())

		assert.Empty(t, indexNames(h))
		require.NoError(t, h.Insert(ctx, ddlDoc(50)))
		assertIndexesHold(t, fx, h, "a", 6)
	})

	t.Run("rolled-back CompactVectorIndex", func(t *testing.T) {
		const n, dim = 60, 8
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)
		require.NoError(t, a.CreateIndex(ctx, anystore.IndexInfo{
			Name: "emb", Kind: anystore.IndexKindVector,
			Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64},
		}))
		vecs := vrand(n, dim, 3)
		for i, v := range vecs {
			require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, v))))
		}
		for i := range n / 2 {
			require.NoError(t, a.DeleteId(ctx, i))
		}

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.CompactVectorIndex(tx.Context(), "emb"))
		require.NoError(t, a.Close())
		h, err := fx.OpenCollection(tx.Context(), "a")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		// The handle writes into the graph the rollback restored, not into
		// the compacted one, whose pages are free for the next collection.
		other, err := fx.CreateCollection(ctx, "other")
		require.NoError(t, err)
		for i := range 5 {
			require.NoError(t, other.Insert(ctx, ddlDoc(i)))
		}
		added := vrand(10, dim, 9)
		for i, v := range added {
			require.NoError(t, h.Insert(ctx, anyenc.MustParseJson(vecDocJSON(n+i, v))))
		}
		for i, v := range added {
			hits, err := vsearch(h, "v", v, 1, 64)
			require.NoError(t, err)
			require.Len(t, hits, 1)
			require.Equal(t, idBytesOf(n+i), hits[0].DocId)
		}
		for i := n / 2; i < n; i++ {
			hits, err := vsearch(h, "v", vecs[i], 1, 64)
			require.NoError(t, err)
			require.Len(t, hits, 1)
			require.Equal(t, idBytesOf(i), hits[0].DocId)
		}
		assertCollCount(t, other, 5)
		assertCollCountInTx(ctx, t, other, 5)
		require.NoError(t, fx.IntegrityCheck(ctx))
	})

	t.Run("commit", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx, ddlFtIdx))
		require.NoError(t, a.Close())
		h, err := fx.OpenCollection(tx.Context(), "a")
		require.NoError(t, err)
		require.NoError(t, h.Insert(tx.Context(), ddlDoc(1)))
		require.NoError(t, tx.Commit())

		assert.Equal(t, []string{"ft", "k"}, indexNames(h))
		require.NoError(t, h.Insert(ctx, ddlDoc(2)))
		assertIndexesHold(t, fx, h, "a", 2, "ft", "k")
	})

	// Only the reopen sits in the savepoint that rolls back: the schema
	// change outside it stands, and the handle keeps it.
	t.Run("savepoint rolled back after the reopen", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx))
		require.NoError(t, a.Close())
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		h, err := fx.OpenCollection(sp.Context(), "a")
		require.NoError(t, err)
		require.NoError(t, sp.Rollback())
		require.NoError(t, h.Insert(tx.Context(), ddlDoc(1)))
		require.NoError(t, tx.Commit())

		assertIndexesHold(t, fx, h, "a", 1, "k")
	})

	// The tx changed another collection only: the handle opened in it is
	// right as it is and survives the rollback.
	t.Run("collection the tx did not change", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(ctx, ddlKIdx))
		require.NoError(t, a.Close())

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		_, err = fx.CreateCollection(tx.Context(), "z")
		require.NoError(t, err)
		h, err := fx.OpenCollection(tx.Context(), "a")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		require.NoError(t, h.Insert(ctx, ddlDoc(1)))
		assertIndexesHold(t, fx, h, "a", 1, "k")
	})
}

// A write tx holds an uncommitted schema change of a collection, the handle
// that made it is closed, and the collection is opened with a context that
// does not carry the tx. The open returns a handle that follows the outcome
// of the tx, not one frozen at the state committed before it.
func TestOpenDuringUncommittedDDL(t *testing.T) {
	t.Run("committed EnsureIndex", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx, ddlFtIdx))
		require.NoError(t, a.Close())
		h, err := fx.OpenCollection(ctx, "a")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		assert.Equal(t, []string{"ft", "k"}, indexNames(h))
		require.NoError(t, h.Insert(ctx, ddlDoc(1)))
		assertIndexesHold(t, fx, h, "a", 1, "ft", "k")
	})

	t.Run("committed DropIndex", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(ctx, ddlKIdx))
		for i := range 5 {
			require.NoError(t, a.Insert(ctx, ddlDoc(i)))
		}

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.DropIndex(tx.Context(), "k"))
		require.NoError(t, a.Close())
		h, err := fx.OpenCollection(ctx, "a")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		assert.Empty(t, indexNames(h))
		other, err := fx.CreateCollection(ctx, "other")
		require.NoError(t, err)
		for i := range 5 {
			require.NoError(t, other.Insert(ctx, ddlDoc(100+i)))
		}
		require.NoError(t, h.Insert(ctx, ddlDoc(50)))
		assertCollCount(t, other, 5)
		assertCollCountInTx(ctx, t, other, 5)
		assertIndexesHold(t, fx, h, "a", 6)
	})

	t.Run("rolled-back DropIndex", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(ctx, ddlFtIdx))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.DropIndex(tx.Context(), "ft"))
		require.NoError(t, a.Close())
		h, err := fx.OpenCollection(ctx, "a")
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())

		assert.Equal(t, []string{"ft"}, indexNames(h))
		require.NoError(t, h.Insert(ctx, ddlDoc(1)))
		assertIndexesHold(t, fx, h, "a", 1, "ft")
	})
}

// A Close() of a handle that changed the schema in an open write tx takes
// effect when the tx ends: until then the handle stays the collection's
// handle, and nothing opens the collection a second time.
func TestCloseDuringDDLTx(t *testing.T) {
	for _, commit := range []bool{true, false} {
		name := "rollback"
		if commit {
			name = "commit"
		}
		t.Run(name, func(t *testing.T) {
			fx := newFixture(t)
			a, err := fx.CreateCollection(ctx, "a")
			require.NoError(t, err)

			tx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx))
			require.NoError(t, a.Close())
			require.NoError(t, a.Insert(tx.Context(), ddlDoc(1)))
			if commit {
				require.NoError(t, tx.Commit())
			} else {
				require.NoError(t, tx.Rollback())
			}

			assert.ErrorIs(t, a.Insert(ctx, ddlDoc(2)), anystore.ErrCollectionClosed)
			fresh, err := fx.OpenCollection(ctx, "a")
			require.NoError(t, err)
			assert.False(t, fresh == a, "the closed handle must have left the registry")
			if commit {
				assertIndexesHold(t, fx, fresh, "a", 1, "k")
			} else {
				assertIndexesHold(t, fx, fresh, "a", 0)
			}
		})
	}

	// The savepoint's change is rolled back, the one made outside it is still
	// uncommitted: the close keeps waiting.
	t.Run("savepoint rollback with a change outside it", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx))
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(sp.Context(), ddlFtIdx))
		require.NoError(t, a.Close())
		require.NoError(t, sp.Rollback())
		require.NoError(t, a.Insert(tx.Context(), ddlDoc(1)))
		require.NoError(t, tx.Commit())

		assert.ErrorIs(t, a.Insert(ctx, ddlDoc(2)), anystore.ErrCollectionClosed)
		fresh, err := fx.OpenCollection(ctx, "a")
		require.NoError(t, err)
		assertIndexesHold(t, fx, fresh, "a", 1, "k")
	})

	// Only an open that hands the handle to a caller keeps it open: one that
	// ends in an error does not, nor does the open an aggregation makes of
	// its $out target.
	t.Run("opens that hand nothing out", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)
		src, err := fx.CreateCollection(ctx, "src")
		require.NoError(t, err)
		require.NoError(t, src.Insert(ctx, ddlDoc(1)))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx))
		require.NoError(t, a.Close())
		_, err = fx.Collection(tx.Context(), "a", anystore.CollectionOptions{PrimaryKey: "other"})
		require.ErrorIs(t, err, anystore.ErrPrimaryKeyMismatch)
		_, err = src.Aggregate(`[{"$out":"a"}]`).Count(tx.Context())
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		assert.ErrorIs(t, a.Insert(ctx, ddlDoc(2)), anystore.ErrCollectionClosed)
		fresh, err := fx.OpenCollection(ctx, "a")
		require.NoError(t, err)
		assertIndexesHold(t, fx, fresh, "a", 1, "k")
	})

	// The savepoint's change was the handle's only one: its rollback leaves
	// nothing uncommitted and the close takes effect inside the tx.
	t.Run("savepoint rollback of the only change", func(t *testing.T) {
		fx := newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, a.EnsureIndex(sp.Context(), ddlKIdx))
		require.NoError(t, a.Close())
		require.NoError(t, sp.Rollback())
		assert.ErrorIs(t, a.Insert(tx.Context(), ddlDoc(1)), anystore.ErrCollectionClosed)
		h, err := fx.OpenCollection(tx.Context(), "a")
		require.NoError(t, err)
		assert.False(t, h == a, "the closed handle must have left the registry")
		require.NoError(t, h.Insert(tx.Context(), ddlDoc(1)))
		require.NoError(t, tx.Commit())

		assertIndexesHold(t, fx, h, "a", 1)
	})
}

// One goroutine holds a write tx with an uncommitted EnsureIndex; another
// closes the collection's handle, opens it again and inserts — an insert that
// waits for the tx to commit. The handle it opened maintains the new indexes.
func TestCloseAndReopenFromAnotherGoroutineDuringDDL(t *testing.T) {
	fx := newFixture(t)
	a, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx, ddlFtIdx))

	opened := make(chan struct{})
	inserted := make(chan error, 1)
	go func() {
		err := a.Close()
		var h anystore.Collection
		if err == nil {
			h, err = fx.OpenCollection(ctx, "a")
		}
		close(opened)
		if err == nil {
			err = h.Insert(ctx, ddlDoc(1))
		}
		inserted <- err
	}()
	<-opened
	require.NoError(t, tx.Commit())
	require.NoError(t, <-inserted)

	h, err := fx.OpenCollection(ctx, "a")
	require.NoError(t, err)
	assertIndexesHold(t, fx, h, "a", 1, "ft", "k")
}

// db.Close() closes every handle at once, also one whose schema change sits
// in a write tx nobody has finished: an operation through that tx fails, it
// does not read a closed database.
func TestDBCloseDuringDDLTx(t *testing.T) {
	fx := newFixture(t)
	a, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	for i := range 5 {
		require.NoError(t, a.Insert(ctx, ddlDoc(i)))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx))
	require.NoError(t, a.Insert(tx.Context(), ddlDoc(100)))
	require.NoError(t, fx.Close())

	_, err = a.FindId(tx.Context(), 100)
	assert.ErrorIs(t, err, anystore.ErrDBIsClosed)
	_, err = a.Count(tx.Context())
	assert.ErrorIs(t, err, anystore.ErrDBIsClosed)
	_, err = a.Find(nil).Iter(tx.Context())
	assert.ErrorIs(t, err, anystore.ErrDBIsClosed)
	assert.ErrorIs(t, a.Insert(tx.Context(), ddlDoc(101)), anystore.ErrDBIsClosed)
}

// The handle of a collection the open transaction created is closed with
// the database like every other, though it entered no registry yet.
func TestDBCloseDuringCreateTx(t *testing.T) {
	fx := newFixture(t)
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	n, err := fx.CreateCollection(tx.Context(), "n")
	require.NoError(t, err)
	require.NoError(t, n.EnsureIndex(tx.Context(), ddlKIdx))
	require.NoError(t, n.Insert(tx.Context(), ddlDoc(1)))
	require.NoError(t, fx.Close())

	_, err = n.FindId(tx.Context(), 1)
	assert.ErrorIs(t, err, anystore.ErrDBIsClosed)
	_, err = n.Count(tx.Context())
	assert.ErrorIs(t, err, anystore.ErrDBIsClosed)
	_, err = n.Find(`{"k":{"$gte":0}}`).Count(tx.Context())
	assert.ErrorIs(t, err, anystore.ErrDBIsClosed)
	assert.ErrorIs(t, n.Insert(tx.Context(), ddlDoc(2)), anystore.ErrDBIsClosed)
	_, err = fx.OpenCollection(tx.Context(), "n")
	assert.ErrorIs(t, err, anystore.ErrDBIsClosed)
}

// Once the transaction of a schema change has ended — whatever the change and
// its outcome — the handle carries nothing uncommitted and Close() closes it
// at once.
func TestCloseAfterDDL(t *testing.T) {
	vecIdx := anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: 4, Metric: anystore.VectorL2},
	}
	inTx := func(t *testing.T, fx *fixture, commit bool, do func(txCtx context.Context)) {
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		do(tx.Context())
		if commit {
			require.NoError(t, tx.Commit())
		} else {
			require.NoError(t, tx.Rollback())
		}
	}
	// Each case leaves the collection under the name it returns.
	cases := []struct {
		name string
		ddl  func(t *testing.T, fx *fixture, c anystore.Collection) string
	}{
		{"create", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			return "a"
		}},
		{"ensure index", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			require.NoError(t, c.EnsureIndex(ctx, ddlKIdx, ddlFtIdx))
			return "a"
		}},
		{"ensure an existing index", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			require.NoError(t, c.EnsureIndex(ctx, ddlKIdx))
			require.NoError(t, c.EnsureIndex(ctx, ddlKIdx))
			return "a"
		}},
		{"create an existing index", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			require.NoError(t, c.CreateIndex(ctx, ddlKIdx))
			require.ErrorIs(t, c.CreateIndex(ctx, ddlKIdx), anystore.ErrIndexExists)
			return "a"
		}},
		{"drop index", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			require.NoError(t, c.EnsureIndex(ctx, ddlKIdx))
			require.NoError(t, c.DropIndex(ctx, "k"))
			return "a"
		}},
		{"drop a missing index", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			require.ErrorIs(t, c.DropIndex(ctx, "k"), anystore.ErrIndexNotFound)
			return "a"
		}},
		{"rename", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			require.NoError(t, c.Rename(ctx, "b"))
			return "b"
		}},
		{"rename to the same name", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			require.NoError(t, c.Rename(ctx, "a"))
			return "a"
		}},
		{"rename to a taken name", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			_, err := fx.CreateCollection(ctx, "b")
			require.NoError(t, err)
			require.ErrorIs(t, c.Rename(ctx, "b"), anystore.ErrCollectionExists)
			return "a"
		}},
		{"compact vector index", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			require.NoError(t, c.CreateIndex(ctx, vecIdx))
			for i, v := range vrand(8, 4, 1) {
				require.NoError(t, c.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, v))))
			}
			require.NoError(t, c.DeleteId(ctx, 0))
			require.NoError(t, c.CompactVectorIndex(ctx, "emb"))
			return "a"
		}},
		{"compact a missing vector index", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			require.ErrorIs(t, c.CompactVectorIndex(ctx, "emb"), anystore.ErrIndexNotFound)
			return "a"
		}},
		{"committed tx", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			inTx(t, fx, true, func(txCtx context.Context) {
				require.NoError(t, c.EnsureIndex(txCtx, ddlKIdx, ddlFtIdx))
				require.NoError(t, c.DropIndex(txCtx, "k"))
				require.NoError(t, c.Rename(txCtx, "b"))
			})
			return "b"
		}},
		{"rolled-back tx", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			inTx(t, fx, false, func(txCtx context.Context) {
				require.NoError(t, c.EnsureIndex(txCtx, ddlKIdx, ddlFtIdx))
				require.NoError(t, c.DropIndex(txCtx, "k"))
				require.NoError(t, c.Rename(txCtx, "b"))
			})
			return "a"
		}},
		{"rolled-back Drop", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			inTx(t, fx, false, func(txCtx context.Context) {
				require.NoError(t, c.Drop(txCtx))
			})
			return "a"
		}},
		{"rolled-back savepoint", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			inTx(t, fx, true, func(txCtx context.Context) {
				sp, err := fx.WriteTx(txCtx)
				require.NoError(t, err)
				require.NoError(t, c.EnsureIndex(sp.Context(), ddlKIdx))
				require.NoError(t, sp.Rollback())
			})
			return "a"
		}},
		{"released savepoint", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			inTx(t, fx, true, func(txCtx context.Context) {
				sp, err := fx.WriteTx(txCtx)
				require.NoError(t, err)
				require.NoError(t, c.EnsureIndex(sp.Context(), ddlKIdx))
				require.NoError(t, sp.Commit())
			})
			return "a"
		}},
		{"created in a committed tx", func(t *testing.T, fx *fixture, c anystore.Collection) string {
			var created anystore.Collection
			inTx(t, fx, true, func(txCtx context.Context) {
				var err error
				created, err = fx.CreateCollection(txCtx, "n")
				require.NoError(t, err)
			})
			require.NoError(t, created.Close())
			_, err := created.Count(ctx)
			assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
			return "a"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			c, err := fx.CreateCollection(ctx, "a")
			require.NoError(t, err)
			name := tc.ddl(t, fx, c)

			require.NoError(t, c.Close())
			_, err = c.Count(ctx)
			assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
			fresh, err := fx.OpenCollection(ctx, name)
			require.NoError(t, err)
			assert.False(t, fresh == c, "the closed handle must have left the registry")
			_, err = fresh.Count(ctx)
			require.NoError(t, err)
		})
	}
}

// A collection is renamed twice in one write tx and a new collection takes
// the name in between. Each handle is its collection's registered handle
// after the commit, and after a rollback the tx left no trace.
func TestRenameTwiceThenCreateIntermediateName(t *testing.T) {
	setup := func(t *testing.T) (fx *fixture, a, b anystore.Collection, tx anystore.WriteTx) {
		fx = newFixture(t)
		a, err := fx.CreateCollection(ctx, "a")
		require.NoError(t, err)
		tx, err = fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.Rename(tx.Context(), "b"))
		require.NoError(t, a.Rename(tx.Context(), "c"))
		b, err = fx.CreateCollection(tx.Context(), "b")
		require.NoError(t, err)
		return fx, a, b, tx
	}

	t.Run("commit", func(t *testing.T) {
		fx, a, b, tx := setup(t)
		require.NoError(t, tx.Commit())

		assert.Equal(t, "c", a.Name())
		reg, err := fx.OpenCollection(ctx, "c")
		require.NoError(t, err)
		assert.True(t, reg == a, "the renamed handle must be registered under its last name")
		reg, err = fx.OpenCollection(ctx, "b")
		require.NoError(t, err)
		assert.True(t, reg == b, "the created handle must keep the intermediate name")
		_, err = fx.OpenCollection(ctx, "a")
		assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)

		require.NoError(t, reg.EnsureIndex(ctx, ddlFtIdx))
		require.NoError(t, b.Insert(ctx, ddlDoc(1)))
		assertIndexesHold(t, fx, b, "b", 1, "ft")
	})

	t.Run("rollback", func(t *testing.T) {
		fx, a, b, tx := setup(t)
		require.NoError(t, tx.Rollback())

		assert.Equal(t, "a", a.Name())
		reg, err := fx.OpenCollection(ctx, "a")
		require.NoError(t, err)
		assert.True(t, reg == a, "the renamed handle must be registered under its old name")
		_, err = b.Count(ctx)
		assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
		for _, name := range []string{"b", "c"} {
			_, err = fx.OpenCollection(ctx, name)
			assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
		}
		require.NoError(t, fx.IntegrityCheck(ctx))
	})
}

// An OpenCollection of the new name races the commit of a Rename: the btree
// commit shows the name to readers before the registry holds it. Whatever the
// open returns, the renamed handle is the collection's one live handle.
func TestRenameCommit_ConcurrentOpenOfNewName(t *testing.T) {
	// A checkpoint after every commit widens the window.
	fx := newFixture(t, &anystore.Config{AutoCheckpointAfter: 1})
	for i := range 200 {
		from, to := fmt.Sprintf("a%d", i), fmt.Sprintf("b%d", i)
		c, err := fx.CreateCollection(ctx, from)
		require.NoError(t, err)

		renamed := make(chan struct{})
		opened := make(chan anystore.Collection, 1)
		go func() {
			for {
				if h, err := fx.OpenCollection(ctx, to); err == nil {
					opened <- h
					return
				}
				select {
				case <-renamed:
					opened <- nil
					return
				default:
				}
			}
		}()
		require.NoError(t, c.Rename(ctx, to))
		close(renamed)

		reg, err := fx.OpenCollection(ctx, to)
		require.NoError(t, err)
		require.True(t, reg == c, "the renamed handle must be registered under the new name")
		if h := <-opened; h != nil && h != c {
			_, err = h.Count(ctx)
			require.ErrorIs(t, err, anystore.ErrCollectionClosed, "a second handle of the renamed collection must not stay live")
		}
	}
}

// Inside the tx that renamed a collection its old name is gone — free for a
// collection created in the same tx — whether or not the handle was closed
// since; other callers keep the committed name until the commit.
func TestRename_OldNameInSameTx(t *testing.T) {
	for _, closed := range []bool{false, true} {
		name := "handle open"
		if closed {
			name = "handle closed"
		}
		t.Run(name, func(t *testing.T) {
			fx := newFixture(t)
			a, err := fx.CreateCollection(ctx, "a")
			require.NoError(t, err)

			tx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			require.NoError(t, a.Rename(tx.Context(), "b"))
			if closed {
				require.NoError(t, a.Close())
			}

			_, err = fx.OpenCollection(tx.Context(), "a")
			assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
			// Open-or-create must not hand out the renamed collection for
			// the old name: the name is free, and a new collection takes it.
			a2, err := fx.Collection(tx.Context(), "a")
			require.NoError(t, err)
			require.False(t, a2 == a)
			require.NoError(t, a2.Insert(tx.Context(), ddlDoc(2)))

			around, err := fx.OpenCollection(ctx, "a")
			require.NoError(t, err)
			assert.True(t, around == a, "outside the tx the committed name resolves to the handle")
			b, err := fx.OpenCollection(tx.Context(), "b")
			require.NoError(t, err)
			assert.True(t, b == a, "inside the tx the new name resolves to the handle")
			require.NoError(t, b.Insert(tx.Context(), ddlDoc(1)))
			require.NoError(t, tx.Commit())

			reg, err := fx.OpenCollection(ctx, "a")
			require.NoError(t, err)
			assert.True(t, reg == a2, "the created collection holds the old name")
			assertCollCount(t, a2, 1)
			assertIndexesHold(t, fx, b, "b", 1)
		})
	}
}

// Uncommitted DDL is the write transaction's own. Inside it a created
// collection, a new index, a new name and a drop are in effect; every other
// caller — through a handle, by name, or in the catalog listing — has the
// committed schema until the commit, and has it still after a rollback.
func TestUncommittedDDL_IsTheWritersAlone(t *testing.T) {
	for _, outcome := range []string{"commit", "rollback"} {
		t.Run(outcome, func(t *testing.T) {
			fx := newFixture(t)
			a, err := fx.CreateCollection(ctx, "a")
			require.NoError(t, err)
			require.NoError(t, a.Insert(ctx, ddlDoc(1)))
			b, err := fx.CreateCollection(ctx, "b")
			require.NoError(t, err)
			require.NoError(t, b.Insert(ctx, ddlDoc(1)))
			d, err := fx.CreateCollection(ctx, "d")
			require.NoError(t, err)
			require.NoError(t, d.Insert(ctx, ddlDoc(1)))

			tx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			n, err := fx.CreateCollection(tx.Context(), "n")
			require.NoError(t, err)
			require.NoError(t, n.EnsureIndex(tx.Context(), ddlKIdx))
			require.NoError(t, n.Insert(tx.Context(), ddlDoc(1)))
			require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx))
			require.NoError(t, a.Insert(tx.Context(), ddlDoc(2)))
			require.NoError(t, b.Rename(tx.Context(), "b2"))
			require.NoError(t, b.Insert(tx.Context(), ddlDoc(2)))
			require.NoError(t, d.Drop(tx.Context()))

			// The transaction's view.
			inTx, err := fx.OpenCollection(tx.Context(), "n")
			require.NoError(t, err)
			assert.True(t, inTx == n)
			assertCollCountInTx(tx.Context(), t, n, 1)
			assertCollCountInTx(tx.Context(), t, a, 2)
			cnt, err := a.Find(`{"k":2}`).Count(tx.Context())
			require.NoError(t, err)
			assert.Equal(t, 1, cnt)
			inTx, err = fx.OpenCollection(tx.Context(), "b2")
			require.NoError(t, err)
			assert.True(t, inTx == b)
			_, err = fx.OpenCollection(tx.Context(), "b")
			assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
			_, err = fx.OpenCollection(tx.Context(), "d")
			assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
			_, err = d.Count(tx.Context())
			assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
			names, err := fx.GetCollectionNames(tx.Context())
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"a", "b2", "n"}, names)

			// Everyone else's.
			_, err = fx.OpenCollection(ctx, "n")
			assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
			_, err = n.Count(ctx)
			assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
			assertCollCount(t, a, 1)
			assert.Empty(t, a.GetIndexes())
			assert.Equal(t, "b", b.Name())
			assertCollCount(t, b, 1)
			around, err := fx.OpenCollection(ctx, "b")
			require.NoError(t, err)
			assert.True(t, around == b)
			_, err = fx.OpenCollection(ctx, "b2")
			assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
			assertCollCount(t, d, 1)
			around, err = fx.OpenCollection(ctx, "d")
			require.NoError(t, err)
			assert.True(t, around == d)
			names, err = fx.GetCollectionNames(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"a", "b", "d"}, names)

			if outcome == "rollback" {
				require.NoError(t, tx.Rollback())
				_, err = n.Count(ctx)
				assert.ErrorIs(t, err, anystore.ErrCollectionClosed, "the handle of a collection never created")
				_, err = fx.OpenCollection(ctx, "n")
				assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
				assertIndexesHold(t, fx, a, "a", 1)
				assert.Equal(t, "b", b.Name())
				assertIndexesHold(t, fx, b, "b", 1)
				assertIndexesHold(t, fx, d, "d", 1)
				return
			}
			require.NoError(t, tx.Commit())
			reg, err := fx.OpenCollection(ctx, "n")
			require.NoError(t, err)
			assert.True(t, reg == n, "the created handle is the registered one")
			assertIndexesHold(t, fx, n, "n", 1, "k")
			assertIndexesHold(t, fx, a, "a", 2, "k")
			assert.Equal(t, "b2", b.Name())
			reg, err = fx.OpenCollection(ctx, "b2")
			require.NoError(t, err)
			assert.True(t, reg == b)
			_, err = fx.OpenCollection(ctx, "b")
			assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
			assertIndexesHold(t, fx, b, "b2", 2)
			_, err = d.Count(ctx)
			assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
			_, err = fx.OpenCollection(ctx, "d")
			assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
			names, err = fx.GetCollectionNames(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"a", "b2", "n"}, names)
		})
	}
}

// A savepoint's rollback discards the DDL made inside it and nothing else:
// the collection created there is gone and its handle dead, the index
// created there is gone while the one created before stays, and the
// transaction commits the rest.
func TestSavepointRollback_DiscardsItsDDLOnly(t *testing.T) {
	fx := newFixture(t)
	a, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, a.Insert(ctx, ddlDoc(1)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, a.EnsureIndex(tx.Context(), ddlKIdx))
	sp, err := fx.WriteTx(tx.Context())
	require.NoError(t, err)
	inner, err := fx.CreateCollection(sp.Context(), "inner")
	require.NoError(t, err)
	require.NoError(t, inner.Insert(sp.Context(), ddlDoc(1)))
	require.NoError(t, a.EnsureIndex(sp.Context(), ddlFtIdx))
	require.NoError(t, a.Insert(sp.Context(), ddlDoc(2)))
	require.NoError(t, sp.Rollback())

	_, err = inner.Count(tx.Context())
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
	_, err = fx.OpenCollection(tx.Context(), "inner")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	assertCollCountInTx(tx.Context(), t, a, 1)
	_, err = a.Find(`{"$text":{"$search":"alpha"}}`).Count(tx.Context())
	assert.ErrorIs(t, err, anystore.ErrNoFulltextIndex)
	cnt, err := a.Find(`{"k":1}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	require.NoError(t, a.Insert(tx.Context(), ddlDoc(3)))
	require.NoError(t, tx.Commit())

	_, err = fx.OpenCollection(ctx, "inner")
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound)
	assertIndexesHold(t, fx, a, "a", 2, "k")
}

// A Close() of a handle the write transaction created waits, like any
// close of a handle the transaction's DDL references: the handle works until
// the transaction ends, an open in between keeps it, and otherwise it is
// closed once the commit has registered it — so the next open builds a
// fresh handle for the committed collection.
func TestCreatedHandle_CloseDuringTx(t *testing.T) {
	for _, reopened := range []bool{false, true} {
		t.Run(fmt.Sprintf("reopened=%v", reopened), func(t *testing.T) {
			fx := newFixture(t)
			tx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			n, err := fx.CreateCollection(tx.Context(), "n")
			require.NoError(t, err)
			require.NoError(t, n.Insert(tx.Context(), ddlDoc(1)))
			require.NoError(t, n.Close())
			assertCollCountInTx(tx.Context(), t, n, 1)
			if reopened {
				again, err := fx.OpenCollection(tx.Context(), "n")
				require.NoError(t, err)
				assert.True(t, again == n)
			}
			require.NoError(t, tx.Commit())

			fresh, err := fx.OpenCollection(ctx, "n")
			require.NoError(t, err)
			assert.Equal(t, reopened, fresh == n)
			assertCollCount(t, fresh, 1)
			if !reopened {
				_, err = n.Count(ctx)
				assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
			}
		})
	}
}
