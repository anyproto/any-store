package anystore

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// User code (a query.Modifier, a query.Filter) runs inside the write tx.
// A panic there used to escape without a Rollback, leaking the btree write lock
// — BeginWrite has no ctx-cancel escape, so every later write, and Close(),
// blocked forever. The bulk paths were worse: their finalizer committed when
// `err == nil`, which is exactly the state during a panic, durably persisting
// the subset modified before it. A panic must roll back and re-panic.

const panicMsg = "user bug in modifier"

// panicModifier panics on the nth call to Modify (1-indexed), marking every
// document it touches before that so a partial commit is observable.
type panicModifier struct {
	n     int
	calls int
}

func (m *panicModifier) Modify(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
	m.calls++
	if m.calls >= m.n {
		panic(panicMsg)
	}
	v.Set("touched", a.NewNumberInt(1))
	return v, true, nil
}

// panicFilter panics on the nth call to Ok — the user-code panic surface on the
// read/scan side (plan.Root.Next evaluates it).
type panicFilter struct {
	n     int
	calls int
}

func (f *panicFilter) Ok(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	f.calls++
	if f.calls >= f.n {
		panic(panicMsg)
	}
	return true
}

func (f *panicFilter) IndexBounds(_ string, bs query.Bounds) query.Bounds { return bs }
func (f *panicFilter) String() string                                     { return "panicFilter" }

func openPanicDB(t *testing.T, dir string) DB {
	db, err := Open(context.Background(), filepath.Join(dir, "store.db"), nil)
	require.NoError(t, err)
	return db
}

func fillDocs(t *testing.T, coll Collection, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		require.NoError(t, coll.Insert(context.Background(),
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"grp":"x"}`, i))))
	}
}

// requireNotWedged asserts the write lock was released: a write, and then
// Close(), must both complete instead of blocking forever.
func requireNotWedged(t *testing.T, db DB, coll Collection) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- coll.Insert(context.Background(), anyenc.MustParseJson(`{"id":9999}`))
	}()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("DB is wedged: a write after the panic blocked — the write lock leaked")
	}

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("db.Close() hangs after the panic — the write lock leaked")
	}
}

// (a) single-doc modifier panic must not leak the write lock (doWriteTxModifiedW).
func TestTxPanic_UpdateIdDoesNotWedgeDB(t *testing.T) {
	ctx := context.Background()
	db := openPanicDB(t, t.TempDir())

	coll, err := db.Collection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"v":1}`)))

	assert.PanicsWithValue(t, panicMsg, func() {
		_, _ = coll.UpdateId(ctx, 1, &panicModifier{n: 1})
	})

	// The rolled-back tx must have left the document untouched.
	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, `{"id":1,"v":1}`, doc.Value().String())

	requireNotWedged(t, db, coll)
}

func TestTxPanic_UpsertIdDoesNotWedgeDB(t *testing.T) {
	ctx := context.Background()
	db := openPanicDB(t, t.TempDir())

	coll, err := db.Collection(ctx, "c")
	require.NoError(t, err)

	assert.PanicsWithValue(t, panicMsg, func() {
		_, _ = coll.UpsertId(ctx, 1, &panicModifier{n: 1})
	})

	requireNotWedged(t, db, coll)
}

// (b) a modifier that panics mid-batch must not commit the already-modified
// subset — the bulk finalizer used to take the commit branch because err == nil.
func TestTxPanic_BulkUpdateIsAtomic(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db := openPanicDB(t, dir)

	coll, err := db.Collection(ctx, "c")
	require.NoError(t, err)
	fillDocs(t, coll, 10)

	// Panics on the 5th matched doc, after modifying 4.
	assert.PanicsWithValue(t, panicMsg, func() {
		_, _ = coll.Find(query.MustParseCondition(`{"grp":"x"}`)).Update(ctx, &panicModifier{n: 5})
	})

	touched, err := coll.Find(query.MustParseCondition(`{"touched":1}`)).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, touched, "partial bulk update was committed")

	require.NoError(t, db.Close())

	// It must not have survived a reopen either.
	db2 := openPanicDB(t, dir)
	defer func() { _ = db2.Close() }()
	coll2, err := db2.Collection(ctx, "c")
	require.NoError(t, err)
	touched, err = coll2.Find(query.MustParseCondition(`{"touched":1}`)).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, touched, "partial bulk update survived restart")
}

// A filter that panics mid-scan must not commit the documents already deleted.
func TestTxPanic_BulkDeleteIsAtomic(t *testing.T) {
	ctx := context.Background()
	db := openPanicDB(t, t.TempDir())

	coll, err := db.Collection(ctx, "c")
	require.NoError(t, err)
	fillDocs(t, coll, 10)

	assert.PanicsWithValue(t, panicMsg, func() {
		_, _ = coll.Find(&panicFilter{n: 5}).Delete(ctx)
	})

	count, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count, "partial bulk delete was committed")

	requireNotWedged(t, db, coll)
}

// The read path: a panicking filter inside Count runs under doReadTx, which must
// release the read tx. Left stranded, MaxReaders of these block every read and
// hang Close().
func TestTxPanic_ReadTxIsReleased(t *testing.T) {
	ctx := context.Background()
	db := openPanicDB(t, t.TempDir())

	coll, err := db.Collection(ctx, "c")
	require.NoError(t, err)
	fillDocs(t, coll, 5)

	for i := 0; i < 24; i++ {
		assert.PanicsWithValue(t, panicMsg, func() {
			_, _ = coll.Find(&panicFilter{n: 1}).Count(ctx)
		})
	}

	// Reads must still work — the reader slots were not leaked.
	done := make(chan error, 1)
	go func() {
		_, cErr := coll.Count(ctx)
		done <- cErr
	}()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("reads are blocked: the read tx leaked on the panic path")
	}

	requireNotWedged(t, db, coll)
}

// Inside a caller-managed tx the wrappers operate on a savepoint, so a panic must
// roll back only that savepoint — discarding the documents the modifier touched
// before it panicked — and leave the consumer's tx usable. The consumer still
// owns the outer rollback.
func TestTxPanic_NestedTxRollsBackOnlySavepoint(t *testing.T) {
	ctx := context.Background()
	db := openPanicDB(t, t.TempDir())

	coll, err := db.Collection(ctx, "c")
	require.NoError(t, err)
	fillDocs(t, coll, 10)

	tx, err := db.WriteTx(ctx)
	require.NoError(t, err)

	// A write made inside the consumer's tx, before the panicking one.
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":99,"grp":"y"}`)))

	// Panics on the 5th matched doc, after the modifier already touched 4 inside
	// the savepoint's scope.
	assert.PanicsWithValue(t, panicMsg, func() {
		_, _ = coll.Find(query.MustParseCondition(`{"grp":"x"}`)).
			Update(tx.Context(), &panicModifier{n: 5})
	})

	// Rolled back to the savepoint: none of the 4 touched docs survive...
	touched, err := coll.Find(query.MustParseCondition(`{"touched":1}`)).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, touched, "savepoint rollback did not discard the partial update")

	// ...but the consumer's own earlier write is intact, and its tx still commits.
	count, err := coll.Find(query.MustParseCondition(`{"id":99}`)).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count, "the savepoint rollback aborted the consumer's tx")
	require.NoError(t, tx.Commit())

	touched, err = coll.Find(query.MustParseCondition(`{"touched":1}`)).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, touched, "partial update committed with the consumer's tx")
	count, err = coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 11, count)

	requireNotWedged(t, db, coll)
}
