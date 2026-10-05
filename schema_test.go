package anystore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// countSchemaLoads counts the loads of a schema from the catalog until the
// test ends.
func countSchemaLoads(t *testing.T) *int {
	n := new(int)
	testHookLoadSchema = func() { *n++ }
	t.Cleanup(func() { testHookLoadSchema = nil })
	return n
}

func hasIndex(c Collection, name string) bool {
	for _, idx := range c.GetIndexes() {
		if idx.Info().Name == name {
			return true
		}
	}
	return false
}

// The window between loading a handle and registering it: a schema change
// committed through an earlier handle, closed since, lands in it. The loaded
// version predates the change and must not become the head.
func TestOpenCollection_SchemaChangeBetweenLoadAndRegistration(t *testing.T) {
	fx := newFixture(t)
	a, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, a.Close())

	hooked := false
	testHookBeforeRegister = func(name string) {
		if name != "a" || hooked {
			return
		}
		hooked = true
		h0, err := fx.OpenCollection(ctx, "a")
		require.NoError(t, err)
		require.NoError(t, h0.EnsureIndex(ctx, IndexInfo{Name: "k", Fields: []string{"k"}}))
		require.NoError(t, h0.Close())
	}
	t.Cleanup(func() { testHookBeforeRegister = nil })

	h, err := fx.OpenCollection(ctx, "a")
	require.NoError(t, err)
	require.True(t, hooked)
	require.NoError(t, h.Insert(ctx, anyenc.MustParseJson(`{"id":1,"k":5}`)))
	assert.True(t, hasIndex(h, "k"))
	n, err := h.Find(`{"k":5}`).IndexHint(IndexHint{IndexName: "k", Boost: 1_000_000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the document was committed without its index entry")
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// A schema commit of this process costs the handles it did not go through
// nothing: no catalog read on their next use, however many there are.
func TestResolve_LocalSchemaCommitLeavesOtherHandlesAlone(t *testing.T) {
	fx := newFixture(t)
	colls := make([]Collection, 50)
	for i := range colls {
		var err error
		colls[i], err = fx.CreateCollection(ctx, fmt.Sprintf("c%02d", i))
		require.NoError(t, err)
		require.NoError(t, colls[i].EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, colls[i].Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	}
	loads := countSchemaLoads(t)

	extra, err := fx.CreateCollection(ctx, "extra")
	require.NoError(t, err)
	require.NoError(t, colls[0].EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
	require.NoError(t, extra.Drop(ctx))
	*loads = 0

	for _, c := range colls {
		_, err := c.FindId(ctx, 1)
		require.NoError(t, err)
		n, err := c.Find(`{"a":1}`).Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.NoError(t, c.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":2}`)))
	}
	assert.Zero(t, *loads)
}

// Another process's schema change is checked per handle, by the first
// transaction that uses it: once for the handle used, never for the others.
func TestResolve_PeerSchemaChangeVerifiesOnFirstUse(t *testing.T) {
	skipIfInMemory(t, "staleness counters model cross-process commits; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	colls := make([]Collection, 50)
	for i := range colls {
		var err error
		colls[i], err = fx.CreateCollection(ctx, fmt.Sprintf("c%02d", i))
		require.NoError(t, err)
		require.NoError(t, colls[i].Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	}
	loads := countSchemaLoads(t)

	peerSchemaChangeNow(t, dbi)
	for range 3 {
		n, err := colls[7].Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		_, err = colls[7].FindId(ctx, 1)
		require.NoError(t, err)
	}
	assert.Equal(t, 1, *loads, "one check of the one handle used")

	// The same through the fast point lookup alone, and through a write.
	peerSchemaChangeNow(t, dbi)
	*loads = 0
	for range 3 {
		_, err := colls[8].FindId(ctx, 1)
		require.NoError(t, err)
	}
	assert.Equal(t, 1, *loads)
	peerSchemaChangeNow(t, dbi)
	*loads = 0
	require.NoError(t, colls[9].Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":2}`)))
	require.NoError(t, colls[9].Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":3}`)))
	assert.Equal(t, 1, *loads)
}

// A reader that begins between a commit and the moment the epoch has caught
// up with it reads the committed schema through its own view — the index
// the commit dropped is not in its plan — and leaves the head to the
// committer.
func TestResolve_ReaderBetweenCommitAndEpoch(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	other, err := fx.CreateCollection(ctx, "other")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	for i := range 10 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}
	require.NoError(t, other.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
	c := coll.(*collection)
	dbi := fx.DB.(*db)

	ran := false
	testHookAfterBtreeCommit = func() {
		if ran {
			return
		}
		ran = true
		head, gen := c.cur(), dbi.epoch.Load().gen
		n, err := coll.Find(`{"a":{"$gte":5}}`).IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		exp, err := coll.Find(`{"a":{"$gte":5}}`).Explain(ctx)
		require.NoError(t, err)
		for _, ie := range exp.Indexes {
			assert.NotEqual(t, "a", ie.Name, "the dropped index is a candidate for a reader past the commit")
		}
		n, err = other.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		assert.True(t, head == c.cur(), "a reader in the gap replaced the head")
		assert.Equal(t, gen, dbi.epoch.Load().gen, "an own commit was taken for another process's")
	}
	t.Cleanup(func() { testHookAfterBtreeCommit = nil })

	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.True(t, ran)
	n, err := coll.Find(`{"a":{"$gte":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
}

// A version is not trusted between two cookies at which it merely checks out
// the same. Another process drops an index and recreates it, and the new
// tree lands on the freed root page: the head looks identical before and
// after. A reader whose snapshot lies between the two commits has no such
// index.
func TestResolve_NoRangeFromTwoChecks(t *testing.T) {
	skipIfInMemory(t, "staleness counters model cross-process commits; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	info := IndexInfo{Name: "k", Fields: []string{"k"}}
	require.NoError(t, coll.EnsureIndex(ctx, info))
	for i := range 20 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d}`, i, i))))
	}
	c := coll.(*collection)
	unaware := c.cur()
	rootBefore := unaware.indexes[0].ns.RootPage()
	// forget puts the handle back where a process that saw none of the
	// schema changes since has it.
	forget := func() {
		c.mu.Lock()
		c.head.Store(unaware)
		c.mu.Unlock()
		peerSchemaChangeNow(t, dbi)
	}
	hint := IndexHint{IndexName: "k", Boost: 1_000_000}
	usesIndex := func(tctx context.Context) bool {
		exp, err := coll.Find(`{"k":{"$gte":10}}`).IndexHint(hint).Explain(tctx)
		require.NoError(t, err)
		for _, ie := range exp.Indexes {
			if ie.Name == "k" && ie.Used {
				return true
			}
		}
		return false
	}

	require.NoError(t, coll.DropIndex(ctx, "k"))
	forget()
	between, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { _ = between.Commit() }()

	// The freelist hands the first root page back at the second recreate.
	require.NoError(t, coll.EnsureIndex(ctx, info))
	require.NoError(t, coll.DropIndex(ctx, "k"))
	require.NoError(t, coll.EnsureIndex(ctx, info))
	if c.cur().indexes[0].ns.RootPage() != rootBefore {
		t.Skip("the recreated index did not land on the freed root page")
	}
	forget()

	// A transaction past the recreate checks the head and finds it as it was.
	require.True(t, usesIndex(ctx))
	require.True(t, unaware == c.cur())

	// The one in between must not take that for its own snapshot.
	assert.False(t, usesIndex(between.Context()), "a reader between the drop and the recreate planned with the index")
	n, err := coll.Find(`{"k":{"$gte":10}}`).IndexHint(hint).Count(between.Context())
	require.NoError(t, err)
	assert.Equal(t, 10, n)
}

// A fast read begin learns of another process's schema change like any
// other: the point lookup does not go through a handle that change retired.
func TestFindId_NoticesPeerSchemaChange(t *testing.T) {
	skipIfInMemory(t, "staleness counters model cross-process commits; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
	c := coll.(*collection)

	// Another process dropped the collection and created one of that name:
	// the catalog now has another token under it.
	require.NoError(t, dbi.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		tx.MarkSchemaChanged()
		tx.MarkDataChanged()
		return tx.Put(dbi.systemNS, collKey("c"), newCatalogID())
	}))
	peerSchemaChangeNow(t, dbi)

	_, err = coll.FindId(ctx, 1)
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	assert.True(t, c.closed.Load(), "the handle of a replaced collection stays live")
	_, err = coll.FindId(ctx, 1)
	assert.ErrorIs(t, err, ErrCollectionClosed)
}
