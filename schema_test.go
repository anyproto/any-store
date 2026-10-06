package anystore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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

// A reader that begins between a commit becoming visible and the epoch
// catching up with it — instructions apart, inside the btree commit — waits
// for the epoch: the heads and the registry of that moment are the
// pre-commit ones. It then works with the committed head, replaces nothing
// and takes the commit for no other process's. When the announcement is
// withdrawn instead — the commit of this process published nothing, the
// cookie is another process's — the reader starts a generation and works
// through its own view.
func TestResolve_ReaderBetweenCommitAndEpoch(t *testing.T) {
	setup := func(t *testing.T) (*fixture, *db, Collection, *schemaEpoch, *schemaEpoch) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
		for i := range 10 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
		}
		dbi := fx.DB.(*db)
		// The commit, and the epoch put back where that reader finds it:
		// the cookie announced, known one behind.
		before := dbi.epoch.Load()
		require.NoError(t, coll.DropIndex(ctx, "a"))
		after := dbi.epoch.Load()
		require.Equal(t, before.known+1, after.known)
		dbi.epoch.Store(before)
		dbi.announceCookie(after.known)
		return fx, dbi, coll, before, after
	}
	noDroppedIndex := func(t *testing.T, coll Collection) {
		n, err := coll.Find(`{"a":{"$gte":5}}`).IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		exp, err := coll.Find(`{"a":{"$gte":5}}`).Explain(ctx)
		require.NoError(t, err)
		for _, ie := range exp.Indexes {
			assert.NotEqual(t, "a", ie.Name, "the dropped index is a candidate for a reader past the commit")
		}
		_, err = coll.FindId(ctx, 1)
		require.NoError(t, err)
	}

	t.Run("epoch catches up", func(t *testing.T) {
		_, dbi, coll, before, after := setup(t)
		c := coll.(*collection)
		head := c.cur()
		loads := countSchemaLoads(t)
		// The commit hook, as the btree commit runs it, while the reader
		// waits for it.
		done := make(chan struct{})
		go func() {
			defer close(done)
			time.Sleep(2 * time.Millisecond)
			dbi.schemaCommitted(0, after.known)
		}()
		noDroppedIndex(t, coll)
		<-done
		assert.Zero(t, *loads, "the reader did not wait for the epoch")
		assert.True(t, head == c.cur())
		assert.Equal(t, before.gen, dbi.epoch.Load().gen, "an own commit was taken for another process's")
		assert.Equal(t, after.known, dbi.epoch.Load().known)
	})

	t.Run("announcement withdrawn", func(t *testing.T) {
		_, dbi, coll, before, after := setup(t)
		c := coll.(*collection)
		head := c.cur()
		loads := countSchemaLoads(t)
		done := make(chan struct{})
		go func() {
			defer close(done)
			time.Sleep(2 * time.Millisecond)
			dbi.endAnnouncement(after.known)
		}()
		noDroppedIndex(t, coll)
		<-done
		assert.NotZero(t, *loads, "the reader trusted a head of a generation it is not in")
		assert.Equal(t, before.gen+1, dbi.epoch.Load().gen, "the cookie is another process's")
		assert.Equal(t, after.known, dbi.epoch.Load().known)
		assert.True(t, head == c.cur(), "the first reader of the generation confirmed the committed head")
		assert.Equal(t, before.gen+1, c.cur().gen.Load())
	})
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

// While a write tx has a schema change in flight, the readers of that time
// are served the committed version it stands on — the index the tx is
// dropping included, the one it is creating not — without a catalog read.
// The tx itself works with what it made.
func TestResolve_ReadersDuringUncommittedDDLUseCommittedVersion(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	for i := range 10 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i))))
	}
	c := coll.(*collection)
	committed := c.cur()
	candidates := func(tctx context.Context) []string {
		exp, err := coll.Find(`{"a":{"$gte":5},"b":{"$gte":5}}`).Explain(tctx)
		require.NoError(t, err)
		var names []string
		for _, ie := range exp.Indexes {
			names = append(names, ie.Name)
		}
		return names
	}
	loads := countSchemaLoads(t)

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(wtx.Context(), IndexInfo{Name: "b", Fields: []string{"b"}}))
	require.NoError(t, coll.DropIndex(wtx.Context(), "a"))
	require.True(t, committed == c.cur(), "the uncommitted changes are the transaction's own")
	*loads = 0
	for range 3 {
		n, err := coll.Find(`{"a":{"$gte":5}}`).IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, n)
		_, err = coll.FindId(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, []string{"a"}, candidates(ctx))
	}
	assert.Zero(t, *loads, "a reader beside an open DDL tx read the catalog")
	assert.Equal(t, []string{"b"}, candidates(wtx.Context()))
	require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":10,"a":10,"b":10}`)))

	require.NoError(t, wtx.Rollback())
	require.True(t, committed == c.cur(), "the rollback leaves the committed head")
	assert.Equal(t, []string{"a"}, candidates(ctx))

	wtx, err = fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(wtx.Context(), IndexInfo{Name: "b", Fields: []string{"b"}}))
	require.NoError(t, wtx.Commit())
	*loads = 0
	assert.ElementsMatch(t, []string{"a", "b"}, candidates(ctx))
	assert.Zero(t, *loads, "a reader past the commit works with the head")
	assert.False(t, committed == c.cur(), "the commit installed the version")
}

// holdCommitBeforeInstall arranges for the next schema-changing commit to
// pause as it becomes visible, before its heads are installed: visible is
// closed then, and the commit goes on once release is closed.
func holdCommitBeforeInstall(t *testing.T) (visible, release chan struct{}) {
	t.Helper()
	visible, release = make(chan struct{}), make(chan struct{})
	var once sync.Once
	testHookBeforeInstall = func() {
		once.Do(func() {
			close(visible)
			<-release
		})
	}
	// A test that fails while the commit is held must still let it go, or
	// the fixture cannot close the database.
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		testHookBeforeInstall = nil
	})
	return visible, release
}

// A reader that pins a schema-changing commit as it becomes visible, before
// its heads are installed, waits for the installation (observeCookie): it
// plans with the committed schema, not with the index the commit dropped,
// and takes the commit for no other process's.
func TestCommit_ReaderInThePublicationWaits(t *testing.T) {
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	for i := range 10 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}
	gen := dbi.epoch.Load().gen
	loads := countSchemaLoads(t)

	visible, release := holdCommitBeforeInstall(t)
	committed := make(chan error, 1)
	go func() { committed <- coll.DropIndex(ctx, "a") }()
	<-visible

	type result struct {
		n   int
		err error
	}
	read := make(chan result, 1)
	go func() {
		n, err := coll.Find(`{"a":{"$gte":5}}`).IndexHint(IndexHint{IndexName: "a", Boost: 1_000_000}).Count(ctx)
		read <- result{n, err}
	}()
	time.Sleep(5 * time.Millisecond)
	select {
	case r := <-read:
		t.Fatalf("the reader did not wait for the heads: %+v", r)
	default:
	}
	close(release)
	require.NoError(t, <-committed)
	r := <-read
	require.NoError(t, r.err)
	assert.Equal(t, 5, r.n)
	assert.Zero(t, *loads, "the reader read the catalog instead of the installed head")
	assert.Equal(t, gen, dbi.epoch.Load().gen, "an own commit was taken for another process's")
	assert.False(t, hasIndex(coll, "a"))
}

// The registry follows a commit as it becomes visible: an open of a
// collection the commit created, or of the name it gave one, that begins in
// the publication gets the committed handle itself, never a second one.
func TestCommit_RegistrySettlesWithThePublication(t *testing.T) {
	fx := newFixture(t)
	a, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	visible, release := holdCommitBeforeInstall(t)
	var created Collection
	committed := make(chan error, 1)
	go func() {
		tx, err := fx.WriteTx(ctx)
		if err != nil {
			committed <- err
			return
		}
		if created, err = fx.CreateCollection(tx.Context(), "n"); err != nil {
			committed <- err
			return
		}
		if err = a.Rename(tx.Context(), "b"); err != nil {
			committed <- err
			return
		}
		committed <- tx.Commit()
	}()
	<-visible

	type opened struct {
		c   Collection
		err error
	}
	byOpen, byCollection, byNewName, byOldName := make(chan opened, 1), make(chan opened, 1), make(chan opened, 1), make(chan opened, 1)
	go func() { c, err := fx.OpenCollection(ctx, "n"); byOpen <- opened{c, err} }()
	go func() { c, err := fx.Collection(ctx, "n"); byCollection <- opened{c, err} }()
	go func() { c, err := fx.OpenCollection(ctx, "b"); byNewName <- opened{c, err} }()
	go func() { c, err := fx.OpenCollection(ctx, "a"); byOldName <- opened{c, err} }()
	time.Sleep(2 * time.Millisecond)
	close(release)
	require.NoError(t, <-committed)

	for name, ch := range map[string]chan opened{"OpenCollection": byOpen, "Collection": byCollection} {
		o := <-ch
		require.NoError(t, o.err, name)
		assert.True(t, o.c == created, "%s: the created handle", name)
		_, err = o.c.Count(ctx)
		require.NoError(t, err, name)
	}
	o := <-byNewName
	require.NoError(t, o.err)
	assert.True(t, o.c == a, "the renamed handle under its new name")
	o = <-byOldName
	if o.err == nil {
		// Opened before the commit became visible: the committed name of
		// that moment.
		assert.True(t, o.c == a)
	} else {
		assert.ErrorIs(t, o.err, ErrCollectionNotFound)
	}
	_, err = fx.OpenCollection(ctx, "a")
	assert.ErrorIs(t, err, ErrCollectionNotFound)
}

// A commit flagged as schema-changing that publishes nothing — its one DDL
// verb failed — withdraws its announcement before any writer of this process
// can begin: a commit of another process at the announced cookie, made as
// the write lock was released, is seen for what it is, and the writer
// maintains the index it created.
func TestCommit_EmptySchemaCommitAnnouncesNothingToTheNextWriter(t *testing.T) {
	skipIfInMemory(t, "models another process's commit; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"k":1}`)))
	_, err = coll.Count(ctx)
	require.NoError(t, err)
	gen := dbi.epoch.Load().gen

	inserted := make(chan error, 1)
	var fired bool
	testHookAfterBtreeCommit = func() {
		if fired {
			return
		}
		fired = true
		// The other process: index k on c, at the cookie this commit
		// announced and did not produce.
		fcc, sc := dbi.btreeDB.LocalCounters()
		ptx, err := dbi.btreeDB.BeginWrite()
		require.NoError(t, err)
		info := IndexInfo{Name: "k", Fields: []string{"k"}}
		require.NoError(t, dbi.registerIndex(ptx, "c", info))
		ns, err := ptx.GetNamespace("c")
		require.NoError(t, err)
		pc := &collection{db: dbi, primaryKey: "id"}
		_, err = pc.buildRangeIndex(ptx, &collSchema{name: "c", ns: ns}, info)
		require.NoError(t, err)
		ptx.MarkSchemaChanged()
		ptx.MarkDataChanged()
		require.NoError(t, ptx.Commit())
		dbi.btreeDB.UpdateLocalCounters(fcc, sc)
		// A writer of this process, beginning now: held at the gate until
		// the announcement is withdrawn.
		go func() { inserted <- coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"k":2}`)) }()
		time.Sleep(2 * time.Millisecond)
	}
	defer func() { testHookAfterBtreeCommit = nil }()

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.ErrorIs(t, coll.DropIndex(wtx.Context(), "nope"), ErrIndexNotFound)
	require.NoError(t, wtx.Commit())
	require.NoError(t, <-inserted)

	assert.Equal(t, gen+1, dbi.epoch.Load().gen, "the other process's commit started no generation")
	n, err := coll.Find(`{"k":2}`).IndexHint(IndexHint{IndexName: "k", Boost: 1_000_000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	for _, idx := range coll.GetIndexes() {
		if idx.Info().Name == "k" {
			l, err := idx.Len(ctx)
			require.NoError(t, err)
			assert.Equal(t, 2, l, "the writer did not maintain the other process's index")
		}
	}
}

// A commit whose log holds pins only — a verb began on the handle and
// changed nothing — discards the log after the btree released the write
// lock, under the schema gate: a writer beginning then would find the
// handle's mark cleared under its own entry, and resolve the head instead
// of the index it is creating.
func TestCommit_PinOnlyCommitIsGated(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"k":1}`)))

	next := make(chan error, 1)
	began := make(chan struct{})
	var fired bool
	testHookAfterBtreeCommit = func() {
		if fired {
			return
		}
		fired = true
		// The next writer: its begin waits at the gate until this commit
		// has discarded its log.
		go func() {
			wtx, err := fx.WriteTx(ctx)
			if err != nil {
				next <- err
				return
			}
			close(began)
			if err = coll.CreateIndex(wtx.Context(), IndexInfo{Name: "k", Fields: []string{"k"}}); err != nil {
				_ = wtx.Rollback()
				next <- err
				return
			}
			if err = coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":2,"k":2}`)); err != nil {
				_ = wtx.Rollback()
				next <- err
				return
			}
			next <- wtx.Commit()
		}()
		time.Sleep(3 * time.Millisecond)
		select {
		case <-began:
			t.Error("a writer began while a pin-only commit was still to discard its log")
		default:
		}
	}
	defer func() { testHookAfterBtreeCommit = nil }()

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Rename(wtx.Context(), "c"))
	require.NoError(t, wtx.Commit())
	require.NoError(t, <-next)

	for _, idx := range coll.GetIndexes() {
		if idx.Info().Name == "k" {
			l, err := idx.Len(ctx)
			require.NoError(t, err)
			assert.Equal(t, 2, l, "the next writer's index misses the document it inserted")
		}
	}
	n, err := coll.Find(`{"k":2}`).IndexHint(IndexHint{IndexName: "k", Boost: 1_000_000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

// A transaction at the snapshot before a drop that opens the dropped
// collection while the commit is settling its registry — the handle gone
// from it, the epoch not yet past the commit — gets a handle of its own for
// its snapshot and nothing more: the handle is proven from the commit's
// cookie on (since), so the transaction does not install the dropped
// collection's version for the transactions after the commit.
func TestCommit_OpenOfDroppedInThePublication(t *testing.T) {
	fx := newFixture(t)
	a, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
	b, err := fx.CreateCollection(ctx, "b")
	require.NoError(t, err)

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { _ = rtx.Commit() }()

	settled, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	testHookAfterSettle = func() {
		once.Do(func() {
			close(settled)
			<-release
		})
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		testHookAfterSettle = nil
	})
	committed := make(chan error, 1)
	go func() { committed <- a.Drop(ctx) }()
	<-settled

	// The older transaction: its snapshot has the collection.
	old, err := fx.OpenCollection(rtx.Context(), "a")
	require.NoError(t, err)
	require.False(t, old == a)
	n, err := old.Count(rtx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	close(release)
	require.NoError(t, <-committed)

	// The present: the collection is gone, the handle with it, and the
	// pages it had are another collection's to take.
	_, err = fx.OpenCollection(ctx, "a")
	assert.ErrorIs(t, err, ErrCollectionNotFound)
	err = old.Insert(ctx, anyenc.MustParseJson(`{"id":2}`))
	assert.ErrorIs(t, err, ErrCollectionNotFound, "a write through the dropped collection's handle")
	for i := range 50 {
		require.NoError(t, b.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%0200d"}`, i, i))))
	}
	require.NoError(t, fx.IntegrityCheck(ctx))
	a2, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	assertCollCount(t, a2, 0)
}

// A handle registered while a schema-flagged commit that publishes nothing
// still has its announcement up — EnsureIndex of an existing index, the
// startup idiom — is proven from the epoch's known like any other: the
// announced cookie is produced by nothing, and a handle proven from it
// would have no head until the next schema change, every transaction
// loading its own version.
func TestCommit_RegistrationDuringEmptySchemaCommit(t *testing.T) {
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "k", Fields: []string{"k"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"k":1}`)))
	require.NoError(t, coll.Close())
	d, err := fx.CreateCollection(ctx, "d")
	require.NoError(t, err)
	require.NoError(t, d.EnsureIndex(ctx, IndexInfo{Name: "k", Fields: []string{"k"}}))
	known := dbi.epoch.Load().known

	var opened Collection
	var fired bool
	testHookAfterBtreeCommit = func() {
		if fired {
			return
		}
		fired = true
		// The announcement is up, the write lock released, nothing
		// published: a registration now.
		var err error
		opened, err = fx.OpenCollection(ctx, "c")
		require.NoError(t, err)
	}
	defer func() { testHookAfterBtreeCommit = nil }()
	require.NoError(t, d.EnsureIndex(ctx, IndexInfo{Name: "k", Fields: []string{"k"}}))
	require.NotNil(t, opened)

	c := opened.(*collection)
	assert.Equal(t, known, c.since)
	loads := countSchemaLoads(t)
	for range 3 {
		n, err := opened.Find(`{"k":1}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	}
	assert.NotNil(t, c.cur(), "no head installed: the handle is proven from a cookie nothing produces")
	assert.LessOrEqual(t, *loads, 1, "every transaction loaded its own version")
}

// The sketch deltas of a transaction's writes to a collection whose schema
// the same transaction changed are persisted with the commit: the version
// the transaction logged is its own, whatever its generation stamp says
// before the install.
func TestCommit_PersistsSketchesOfLoggedVersions(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	other, err := fx.CreateCollection(ctx, "other")
	require.NoError(t, err)

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(wtx.Context(), IndexInfo{Name: "k", Fields: []string{"k"}}))
	for i := range 50 {
		require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d}`, i, i%5))))
	}
	require.NoError(t, wtx.Commit())
	idx := coll.(*collection).cur().indexes[0]
	assert.False(t, idx.sketchModified, "the commit did not persist the sketch")
	// The next write's begin rebases whatever was not persisted.
	require.NoError(t, other.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
	assert.Equal(t, uint64(50), idx.loadPubSketch().GetDocCount(), "the deltas were rebased away")
}

// Name() of a handle whose collection is gone — closed, dropped, renamed
// by another process — reports the name it had, not the one it was opened
// under.
func TestName_OfAGoneHandle(t *testing.T) {
	skipIfInMemory(t, "staleness counters model cross-process commits; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	a, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, a.Rename(ctx, "b"))
	peerSchemaChangeNow(t, dbi)
	require.NoError(t, a.Close())
	assert.Equal(t, "b", a.Name())
}
