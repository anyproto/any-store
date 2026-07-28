package anystore

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// These tests guard the index-set publication ratchet: a staleness pass
// whose snapshot is older than the published sets must not republish from
// its older catalog. Rebuilt from that snapshot it would resurrect a
// dropped index (readers at current snapshots then plan against freed
// namespaces — silently wrong results) or evict a fresher one (a concurrent
// DropIndex fails ErrIndexNotFound; writes stop maintaining it).

func ratchetFixture(t *testing.T) (*fixture, *db, Collection) {
	t.Helper()
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("staleness counters model cross-process commits; not applicable in-memory")
	}
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	docs := make([]*anyenc.Value, 0, 10)
	for i := 0; i < 10; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	return fx, fx.DB.(*db), coll
}

// A stale pass with a snapshot from when the index still existed must not
// resurrect it after DropIndex committed.
func TestReconcileRatchet_NoResurrectionAfterDrop(t *testing.T) {
	_, dbi, coll := ratchetFixture(t)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Reader begun while the index exists, with a stale-baked verdict.
	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)

	require.NoError(t, coll.DropIndex(ctx, "a"))

	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())

	require.Empty(t, coll.(*collection).loadIndexes(),
		"stale pass resurrected a dropped index into the live set")
	n, err := coll.Find(`{"a":{"$gte":0}}`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, n)
}

// A stale pass with a snapshot predating CreateIndex must not evict the
// freshly created index from the live set.
func TestReconcileRatchet_NoEvictionAfterCreate(t *testing.T) {
	_, dbi, coll := ratchetFixture(t)

	// Reader begun before the index exists, with a stale-baked verdict.
	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())

	require.Len(t, coll.(*collection).loadIndexes(), 1,
		"stale pass evicted a freshly created index from the live set")
	// The observed production symptom of the eviction: DropIndex not found.
	require.NoError(t, coll.DropIndex(ctx, "a"))
}

// A rolled-back DDL tx must leave the ratchet where it was: the bump happens
// only in the commit publication, which a rollback drops. A moved ratchet
// would make later same-cookie staleness passes skip reconciles for state
// that never committed.
func TestReconcileRatchet_RollbackLeavesRatchetUntouched(t *testing.T) {
	fx, dbi, coll := ratchetFixture(t)
	before := coll.(*collection).indexSetCookie

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(wtx.Context(), IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, wtx.Rollback())

	cc := coll.(*collection)
	require.Empty(t, cc.loadIndexes(), "rollback must restore the published set")
	require.Equal(t, before, cc.indexSetCookie, "rollback must not move the ratchet")

	// The unmoved ratchet must not block a genuine staleness reconcile at the
	// unchanged cookie (peer-bump emulation), and real DDL still works.
	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	n, err := coll.Find(`{"a":{"$gte":0}}`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.DropIndex(ctx, "a"))
}
