package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// These tests guard the per-tx DDL undo log (commonTx.undo): DDL publishes
// in-memory schema state (openedCollections, index sets, names) at execution
// time, and a rollback of the enclosing scope must unwind those publications
// so no handle survives over a reverted (freed) catalog entry.

// The I-11 corruption sequence: create a collection inside an ambient tx, roll
// the outer tx back, let a later collection reuse the freed root page, then
// use the original name again. Pre-fix, the stale handle stayed registered and
// a write through it landed inside the OTHER collection while IntegrityCheck
// stayed green (docs/repro/i11-stale-handle-rollback).
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
	assert.ErrorIs(t, err, ErrDocNotFound)

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
	require.NoError(t, coll.EnsureIndex(tx.Context(), IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, tx.Rollback())

	assert.Empty(t, coll.GetIndexes(), "rolled-back index must not stay published")

	// Writes must not touch the phantom index, and the index must be
	// creatable again from scratch.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"2","a":2}`)))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
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

// Rename inside a rolled-back tx must restore the in-memory name, which keys
// sketch persistence and index metadata.
func TestRenameRollback_RestoresName(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "old")
	require.NoError(t, err)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Rename(tx.Context(), "new"))
	require.NoError(t, tx.Rollback())

	assert.Equal(t, "old", coll.Name())
	names, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Contains(t, names, "old")
	assert.NotContains(t, names, "new")
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)))
	require.NoError(t, fx.IntegrityCheck(ctx))
}
