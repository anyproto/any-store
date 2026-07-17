package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
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

// Rename inside a rolled-back tx must restore the in-memory name, the
// openedCollections key, the index generation and (via the btree rollback)
// the namespaces themselves.
func TestRenameRollback_RestoresName(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "old")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Rename(tx.Context(), "new"))
	require.NoError(t, tx.Rollback())

	assert.Equal(t, "old", coll.Name())
	names, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Contains(t, names, "old")
	assert.NotContains(t, names, "new")

	// Map key restored: the cached handle answers for the old name, nothing
	// answers for the new one.
	sameColl, err := fx.OpenCollection(ctx, "old")
	require.NoError(t, err)
	assert.Same(t, coll, sameColl, "rolled-back rename must keep the handle keyed by the old name")
	_, err = fx.OpenCollection(ctx, "new")
	assert.ErrorIs(t, err, ErrCollectionNotFound)

	// Namespace restore comes from the btree rollback — assert it, don't
	// assume it.
	nsNames, err := fx.DB.(*db).btreeDB.ListNamespaces()
	require.NoError(t, err)
	assert.Contains(t, nsNames, "old")
	assert.Contains(t, nsNames, "ix:old:a")
	assert.NotContains(t, nsNames, "new")
	assert.NotContains(t, nsNames, "ix:new:a")

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)))
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
	assert.ErrorIs(t, err, ErrDocNotFound)
	_, err = collA.FindId(ctx, "in-b")
	assert.ErrorIs(t, err, ErrDocNotFound)
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
// I-11 corruption through the concurrency side door: a concurrent
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
	assert.ErrorIs(t, err, ErrCollectionClosed)

	require.NoError(t, tx.Commit())

	// Committed: the handle is evicted and the catalog entry is gone.
	_, err = fx.OpenCollection(ctx, "x")
	assert.ErrorIs(t, err, ErrCollectionNotFound)

	// Let a new collection reuse the freed pages, then write through the
	// window handle: it must fail, and nothing may leak into "y".
	collY, err := fx.CreateCollection(ctx, "y")
	require.NoError(t, err)
	require.NoError(t, collY.Insert(ctx, anyenc.MustParseJson(`{"id":"y1"}`)))
	err = during.Insert(ctx, anyenc.MustParseJson(`{"id":"stray"}`))
	assert.ErrorIs(t, err, ErrCollectionClosed)
	cnt, err := collY.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	_, err = collY.FindId(ctx, "stray")
	assert.ErrorIs(t, err, ErrDocNotFound)
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
	assert.ErrorIs(t, err, ErrCollectionClosed)

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
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`))
	assert.ErrorIs(t, err, ErrCollectionClosed)

	tx2, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	coll2, err := fx.CreateCollection(tx2.Context(), "y")
	require.NoError(t, err)
	require.NoError(t, coll2.Drop(tx2.Context()))
	require.NoError(t, tx2.Commit())
	_, err = fx.OpenCollection(ctx, "y")
	assert.ErrorIs(t, err, ErrCollectionNotFound)
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
	assert.ErrorIs(t, err, ErrCollectionNotFound)
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
	assert.ErrorIs(t, err, ErrDocNotFound)
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
	assert.ErrorIs(t, err, ErrCollectionClosed)
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
	assert.ErrorIs(t, err, ErrCollectionClosed)
	reopened, err := fx.OpenCollection(ctx, "x")
	require.NoError(t, err)
	assert.NotSame(t, coll, reopened)
	_, err = reopened.FindId(ctx, "1")
	require.NoError(t, err)
}

// Same-tx write + Drop + commit: the commit-time sketch sweep must skip the
// dropped (closed, still-registered) handle — persisting its dirty sketches
// would durably resurrect the stat_data rows removeCollection deleted in this
// very tx, and a later same-named index would adopt the stale sketch.
func TestDropInTxDoesNotResurrectSketches(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "x")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := 0; i < 10; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	// Dirty the sketch inside the tx, then drop.
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":100,"a":100}`)))
	require.NoError(t, coll.Drop(tx.Context()))
	require.NoError(t, tx.Commit())

	// No stat_data leftovers for the dropped collection.
	dbi := fx.DB.(*db)
	require.NoError(t, dbi.doReadTx(ctx, func(btx *btree.ReadTx) error {
		_, gErr := btx.Get(dbi.systemNS, sketchKey("x", "a"))
		assert.ErrorIs(t, gErr, btree.ErrKeyNotFound)
		return nil
	}))
	require.NoError(t, fx.IntegrityCheck(ctx))
}
