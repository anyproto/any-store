package anystore

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// These tests guard the Rename contract: it must re-key the btree namespaces and
// the handle registry, not just the catalog metadata. The reopen tests use a
// raw Open on a temp path (not newFixture) because they must survive a real
// close/open cycle.

func TestRename_ReopenAfterEviction(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
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
	store, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := store.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1","a":10}`)))
	require.NoError(t, coll.Rename(ctx, "after"))
	require.NoError(t, store.Close())

	store, err = Open(ctx, path, nil)
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
	store, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := store.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.Rename(ctx, "after"))

	// With the renamed handle still cached.
	_, err = store.OpenCollection(ctx, "before")
	assert.ErrorIs(t, err, ErrCollectionNotFound)

	require.NoError(t, store.Close())
	store, err = Open(ctx, path, nil)
	require.NoError(t, err)
	defer store.Close()
	_, err = store.OpenCollection(ctx, "before")
	assert.ErrorIs(t, err, ErrCollectionNotFound)
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
	assert.ErrorIs(t, err, ErrDocNotFound)
	_, err = collA.FindId(ctx, "in-a")
	assert.ErrorIs(t, err, ErrDocNotFound)
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
	assert.ErrorIs(t, collA.Rename(ctx, "b"), ErrCollectionExists)

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
	assert.ErrorIs(t, err, ErrCollectionNotFound, "pre-commit, the new name is not committed yet")

	require.NoError(t, tx.Commit())

	// Post-commit: the mapping flipped.
	renamed, err := fx.OpenCollection(ctx, "new")
	require.NoError(t, err)
	assert.Same(t, coll, renamed)
	_, err = fx.OpenCollection(ctx, "old")
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

func TestRename_InvalidNamesRejected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	for _, bad := range []string{"", "_system", "ix:a:b", "ftx:a:b:map", "vix:a:b:meta"} {
		assert.ErrorIs(t, coll.Rename(ctx, bad), ErrInvalidCollectionName, "rename to %q", bad)
		_, err = fx.CreateCollection(ctx, bad)
		assert.ErrorIs(t, err, ErrInvalidCollectionName, "create %q", bad)
	}
	assert.Equal(t, "a", coll.Name())
}

// One test kills the metadata/namespace world-split for every index kind:
// range, unique, multikey (array field), full-text and vector entries must all
// be reachable through their indexes after a rename and a full DB reopen.
func TestRename_AllIndexKindsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := store.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		IndexInfo{Name: "a", Fields: []string{"a"}},
		IndexInfo{Name: "u", Fields: []string{"u"}, Unique: true},
		IndexInfo{Name: "tags", Fields: []string{"tags"}},
		IndexInfo{Name: "txt", Kind: IndexKindFulltext, Fields: []string{"body"}},
		IndexInfo{Name: "emb", Kind: IndexKindVector, Vector: &VectorParams{Field: "v", Dim: 4, Metric: VectorL2}},
	))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"1","a":10,"u":"x","tags":["red","blue"],"body":"quick brown fox","v":[1,0,0,0]}`),
		anyenc.MustParseJson(`{"id":"2","a":20,"u":"y","tags":["green"],"body":"lazy dog","v":[0,1,0,0]}`),
	))
	require.NoError(t, coll.Rename(ctx, "after"))
	require.NoError(t, store.Close())

	store, err = Open(ctx, path, nil)
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
	assert.ErrorIs(t, err, ErrUniqueConstraint, "unique index")
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

// Index DDL after a rename must register/deregister metadata under the SAME
// world as the renamed namespaces — no split between idx:new:* and ix:old:*.
func TestRename_PostRenameIndexDDL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := store.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "old_idx", Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1","a":10,"b":20}`)))

	require.NoError(t, coll.Rename(ctx, "after"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "new_idx", Fields: []string{"b"}}))
	require.NoError(t, coll.DropIndex(ctx, "old_idx"))
	require.NoError(t, store.Close())

	store, err = Open(ctx, path, nil)
	require.NoError(t, err)
	defer store.Close()
	reopened, err := store.OpenCollection(ctx, "after")
	require.NoError(t, err)
	idxs := reopened.GetIndexes()
	require.Len(t, idxs, 1)
	assert.Equal(t, "new_idx", idxs[0].Info().Name)
	cnt, err := reopened.Find(`{"b":20}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)

	// No orphan namespaces from either world.
	nsNames, err := store.(*db).btreeDB.ListNamespaces()
	require.NoError(t, err)
	for _, ns := range nsNames {
		assert.NotContains(t, ns, "before", "orphan namespace %q", ns)
		assert.NotContains(t, ns, "old_idx", "orphan namespace %q", ns)
	}
	require.NoError(t, store.IntegrityCheck(ctx))
}

// Drop after rename must delete the REAL namespaces (pre-fix it derived the
// names from the new name and orphaned the old-name data + index trees).
func TestRename_ThenDrop_NoOrphans(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "before")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		IndexInfo{Name: "a", Fields: []string{"a"}},
		IndexInfo{Name: "txt", Kind: IndexKindFulltext, Fields: []string{"body"}},
		IndexInfo{Name: "emb", Kind: IndexKindVector, Vector: &VectorParams{Field: "v", Dim: 4, Metric: VectorL2}},
	))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1","a":10,"body":"text","v":[1,0,0,0]}`)))

	require.NoError(t, coll.Rename(ctx, "after"))
	require.NoError(t, coll.Drop(ctx))

	nsNames, err := fx.DB.(*db).btreeDB.ListNamespaces()
	require.NoError(t, err)
	assert.Equal(t, []string{systemNamespace}, nsNames,
		"drop after rename must leave only the system namespace, got %v", nsNames)
	require.NoError(t, fx.IntegrityCheck(ctx))
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
		assert.ErrorIs(t, err, ErrCollectionClosed)
		assert.ErrorIs(t, err, ErrCollectionNotFound)
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
	assertClosed(coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
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
	assert.ErrorIs(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)), ErrDBIsClosed)
}
