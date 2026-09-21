package test

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Each benchmark opens a fresh DB, then times b.N transactions of
// 100 InsertOne operations each — representative of a small-write
// workload. NoCommitSync = !CommitSync (default false) means we
// skip fsync and measure pure CPU + buffered I/O cost.

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

func openPanicDB(t *testing.T, dir string) anystore.DB {
	db, err := anystore.Open(context.Background(), filepath.Join(dir, "store.db"), nil)
	require.NoError(t, err)
	return db
}

func fillDocs(t *testing.T, coll anystore.Collection, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		require.NoError(t, coll.Insert(context.Background(),
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"grp":"x"}`, i))))
	}
}

// requireNotWedged asserts the write lock was released: a write, and then
// Close(), must both complete instead of blocking forever.
func requireNotWedged(t *testing.T, db anystore.DB, coll anystore.Collection) {
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

// Collection and index names are capped so the derived namespace names
// (widest: "ftx:"+coll+":"+index+":vocab") can never overflow a master-table
// cell at the default page size.

func TestNameLengthCap_Collection(t *testing.T) {
	fx := newFixture(t)

	okName := strings.Repeat("c", 255)
	coll, err := fx.CreateCollection(ctx, okName)
	require.NoError(t, err)
	require.NoError(t, coll.Close())

	tooLong := strings.Repeat("c", 256)
	_, err = fx.CreateCollection(ctx, tooLong)
	assert.ErrorIs(t, err, anystore.ErrInvalidCollectionName)

	short, err := fx.CreateCollection(ctx, "short")
	require.NoError(t, err)
	assert.ErrorIs(t, short.Rename(ctx, tooLong), anystore.ErrInvalidCollectionName)
	assert.Equal(t, "short", short.Name())
}

func TestNameLengthCap_Index(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	okName := strings.Repeat("i", 255)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: okName, Fields: []string{"a"}}))

	tooLong := strings.Repeat("i", 256)
	err = coll.EnsureIndex(ctx, anystore.IndexInfo{Name: tooLong, Fields: []string{"b"}})
	assert.ErrorIs(t, err, anystore.ErrInvalidIndexName)

	// A name synthesized from the field list is capped the same way.
	longField := strings.Repeat("f", 300)
	err = coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{longField}})
	assert.ErrorIs(t, err, anystore.ErrInvalidIndexName)

	// Full-text index creation goes through the same validation.
	err = coll.EnsureIndex(ctx, anystore.IndexInfo{Name: tooLong, Fields: []string{"body"}, Kind: anystore.IndexKindFulltext})
	assert.ErrorIs(t, err, anystore.ErrInvalidIndexName)

	require.NoError(t, fx.IntegrityCheck(ctx))
}

// End-to-end coverage for strings containing NUL bytes (newly representable —
// the old encoder silently stripped them): document round-trip, primary-key
// lookups, and exact index bounds on ascending and descending indexes where
// the encoding of "a" is a byte-prefix of the encoding of "a\x00b".
func TestNulStrings(t *testing.T) {
	t.Run("doc round trip and FindId", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		a := &anyenc.Arena{}
		doc := a.NewObject()
		doc.Set("id", a.NewString("id\x00x"))
		doc.Set("va\x00l", a.NewString("data\x00data"))
		require.NoError(t, coll.Insert(ctx, doc))

		got, err := coll.FindId(ctx, "id\x00x")
		require.NoError(t, err)
		assert.Equal(t, "data\x00data", got.Value().GetString("va\x00l"))

		_, err = coll.FindId(ctx, "idx")
		assert.ErrorIs(t, err, anystore.ErrDocNotFound)
	})

	for _, field := range []string{"name", "-name"} {
		t.Run("comparison ops "+field, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "test")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{field}}))
			names := []string{"a", "a\x00b", "a\x00", "ab", "b", "\x00"}
			for i, name := range names {
				a := &anyenc.Arena{}
				doc := a.NewObject()
				doc.Set("id", a.NewNumberInt(i))
				doc.Set("name", a.NewString(name))
				require.NoError(t, coll.Insert(ctx, doc))
			}
			hint := anystore.IndexHint{IndexName: strings.TrimPrefix(field, "-"), Boost: 100}
			expected := map[query.CompOp][]string{
				query.CompOpEq:  {"a"},
				query.CompOpGt:  {"a\x00", "a\x00b", "ab", "b"},
				query.CompOpGte: {"a", "a\x00", "a\x00b", "ab", "b"},
				query.CompOpLt:  {"\x00"},
				query.CompOpLte: {"\x00", "a"},
			}
			for op, want := range expected {
				q := query.Key{Path: []string{"name"}, Filter: query.NewComp(op, "a")}
				it, err := coll.Find(q).IndexHint(hint).Iter(ctx)
				require.NoError(t, err)
				var got []string
				for it.Next() {
					d, derr := it.Doc()
					require.NoError(t, derr)
					got = append(got, d.Value().GetString("name"))
				}
				require.NoError(t, it.Close())
				slices.Sort(got)
				assert.Equal(t, want, got, "find op=%d (%s)", op, field)

				cnt, err := coll.Find(q).IndexHint(hint).Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, len(want), cnt, "count op=%d (%s)", op, field)
			}
		})

		t.Run("unique index "+field, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "test")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{field}, Unique: true}))

			a := &anyenc.Arena{}
			doc := a.NewObject()
			doc.Set("id", a.NewNumberInt(1))
			doc.Set("name", a.NewString("a\x00b"))
			require.NoError(t, coll.Insert(ctx, doc))

			// unique point lookup for the absent prefix value must not match
			// the "a\x00b" entry (CoverIter exact-prefix check)
			hint := anystore.IndexHint{IndexName: strings.TrimPrefix(field, "-"), Boost: 100}
			q := query.Key{Path: []string{"name"}, Filter: query.NewComp(query.CompOpEq, "a")}
			cnt, err := coll.Find(q).IndexHint(hint).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, cnt)

			// inserting "a" must NOT trip the unique constraint against "a\x00b"
			doc2 := a.NewObject()
			doc2.Set("id", a.NewNumberInt(2))
			doc2.Set("name", a.NewString("a"))
			require.NoError(t, coll.Insert(ctx, doc2))

			// but a real duplicate must — both the NUL value and the plain
			// value whose duplicate probe could be shadowed by the NUL entry
			doc3 := a.NewObject()
			doc3.Set("id", a.NewNumberInt(3))
			doc3.Set("name", a.NewString("a\x00b"))
			assert.ErrorIs(t, coll.Insert(ctx, doc3), anystore.ErrUniqueConstraint)
			doc4 := a.NewObject()
			doc4.Set("id", a.NewNumberInt(4))
			doc4.Set("name", a.NewString("a"))
			assert.ErrorIs(t, coll.Insert(ctx, doc4), anystore.ErrUniqueConstraint)

			cnt, err = coll.Find(q).IndexHint(hint).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 1, cnt)
		})

		t.Run("index bounds "+field, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "test")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{field}}))

			names := []string{"a", "a\x00b", "a\x00", "ab", "b", "\x00"}
			for i, name := range names {
				a := &anyenc.Arena{}
				doc := a.NewObject()
				doc.Set("id", a.NewNumberInt(i))
				doc.Set("name", a.NewString(name))
				require.NoError(t, coll.Insert(ctx, doc))
			}

			for _, name := range names {
				q := query.Key{Path: []string{"name"}, Filter: query.NewComp(query.CompOpEq, name)}

				count, err := coll.Find(q).Count(ctx)
				require.NoError(t, err, "count eq %q", name)
				assert.Equal(t, 1, count, "count eq %q (%s)", name, field)

				iter, err := coll.Find(q).Iter(ctx)
				require.NoError(t, err)
				var got []string
				for iter.Next() {
					d, derr := iter.Doc()
					require.NoError(t, derr)
					got = append(got, d.Value().GetString("name"))
				}
				require.NoError(t, iter.Close())
				assert.Equal(t, []string{name}, got, "find eq %q (%s)", name, field)
			}

			// range scan must see every doc exactly once, in field order
			it, err := coll.Find(nil).Sort(field).Iter(ctx)
			require.NoError(t, err)
			var got []string
			for it.Next() {
				d, derr := it.Doc()
				require.NoError(t, derr)
				got = append(got, d.Value().GetString("name"))
			}
			require.NoError(t, it.Close())
			assert.Len(t, got, len(names), field)
			sorted := slices.Clone(names)
			slices.Sort(sorted)
			if field == "-name" {
				slices.Reverse(sorted)
			}
			assert.Equal(t, sorted, got, field)
		})
	}
}

// Mutating the iterated collection through the same write tx is UNDEFINED
// (see Iterator): which rows the traversal visits is not asserted here. This
// pins the SAFE half of the contract — no panic, no iterator error, and the
// committed store stays fully consistent (count matches what was actually
// deleted, structural integrity clean).
func TestIteratorDeleteDuringIterationStaysConsistent(t *testing.T) {
	const n = 500
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":%d}`, i, i%7))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	iter, err := coll.Find(nil).Iter(tx.Context())
	require.NoError(t, err)
	deleted := 0
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		id := doc.Value().GetInt("id")
		require.NoError(t, coll.DeleteId(tx.Context(), id))
		deleted++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	require.NoError(t, tx.Commit())

	// The committed state is exactly consistent with the deletes performed.
	cnt, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, n-deleted, cnt)
	visited := 0
	fresh, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	for fresh.Next() {
		visited++
	}
	require.NoError(t, fresh.Err())
	require.NoError(t, fresh.Close())
	assert.Equal(t, cnt, visited)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// TestSketchNoAccumulatingDriftAcrossRolledBackTxs is the regression test for the
// rolled-back-sketch-delta fix. insertKeys/deleteKeys mutate the live index sketch
// in place and set sketchModified; a committed tx clears that flag via
// persistSketches, but a ROLLED-BACK tx does not. Before the fix, those phantom
// deltas accumulated unbounded across rolled-back txs (a non-stale write tx never
// reloaded the sketch) and were persisted to disk on the next clean commit —
// drifting the planner's cardinality estimate (advisory only; never query results).
//
// The fix (db.resetUncommittedSketches, called at write-tx begin): if an index's
// sketchModified is still set when a new write tx begins, its live sketch is
// rebased to the last committed on-disk state before the new tx applies its own
// deltas — so phantom deltas from a prior rolled-back tx are discarded instead of
// accumulating, and are never persisted. This is the in-methodology analog of
// resetting to the committed snapshot at tx begin, with zero cost on the
// all-commit happy path.
//
// Guarantees asserted here:
//   - NO ACCUMULATION: after M rolled-back txs the sketch docCount is bounded to
//     N + K (the most recent rolled-back tx's deltas, cleaned up at the next write
//     tx begin), NOT N + (1+M)*K.
//   - NO DISK POLLUTION: a clean commit after the rollbacks persists the true
//     count (N+1), not the accumulated drift.
//   - The real Count() is exact (== N) throughout (the sketch is advisory).
func TestSketchNoAccumulatingDriftAcrossRolledBackTxs(t *testing.T) {
	const (
		N = 10 // committed baseline docs
		K = 3  // docs inserted (then rolled back) per iteration
		M = 50 // number of rolled-back txs
	)

	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "coll")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"k"}}))
	insp := fx.DB.(anystore.IndexSketchInspector)

	// --- 1) Commit a clean baseline of N distinct docs. ---
	for i := 0; i < N; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"k":%d}`, i, i),
		)))
	}
	cnt, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, N, cnt, "baseline real Count")
	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.Indexes, 1)
	require.Equal(t, uint64(N), st.Indexes[0].SketchDocCount, "baseline sketch docCount == N")

	// --- 2) Loop a rolled-back insert of K docs M times. Each new write tx begin
	// rebases the live sketch to the committed state (N) before adding its own K and
	// rolling back, so the phantom deltas DO NOT accumulate. ---
	for it := 0; it < M; it++ {
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		base := 2000 + it*K
		for j := 0; j < K; j++ {
			require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"k":%d}`, base+j, base+j),
			)))
		}
		require.NoError(t, tx.Rollback())
	}

	// Real count never moved.
	cnt, err = coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, N, cnt, "real Count after M rolled-back txs is STILL N")

	// NO ACCUMULATION: sketch docCount is bounded to N+K (the last rolled-back tx's
	// deltas, not yet cleaned up because no further write tx has begun), NOT the
	// pre-fix N+(1+M)*K. The decisive check: it does not grow with M.
	st, err = coll.Stats(ctx)
	require.NoError(t, err)
	gotDrift := st.Indexes[0].SketchDocCount
	t.Logf("after %d rolled-back txs (K=%d each): real Count=%d, sketch docCount=%d (bounded residual=N+K=%d, pre-fix would be %d)",
		M, K, cnt, gotDrift, N+K, N+(1+M)*K)
	assert.LessOrEqual(t, gotDrift, uint64(N+K),
		"sketch docCount must be bounded to a single tx's deltas, not accumulate with M")

	// --- 3) On-disk sketch is still N: every tx above rolled back, persist never ran. ---
	on, err := insp.InspectIndexSketch(ctx, "coll", "k")
	require.NoError(t, err)
	assert.Equal(t, uint64(N), on.DocCount, "on-disk sketch still N after only-rolled-back txs")

	// --- 4) A clean commit now persists the TRUE count (N+1), not accumulated drift:
	// its write-tx begin rebased live to the committed N, then it added 1. ---
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":9999,"k":9999}`)))
	cnt, err = coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, N+1, cnt, "real Count now N+1 after one clean commit")
	on, err = insp.InspectIndexSketch(ctx, "coll", "k")
	require.NoError(t, err)
	assert.Equal(t, uint64(N+1), on.DocCount,
		"on-disk sketch == true N+1 (phantom deltas discarded, not persisted)")

	// In-memory sketch now also reflects the true committed count.
	st, err = coll.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(N+1), st.Indexes[0].SketchDocCount, "in-memory sketch == true N+1")
}

// TestSketchNoDriftOnUniqueViolationRollback covers the most common real trigger
// of the rolled-back-sketch-delta class (case C1): a batch Insert whose later doc
// violates a unique constraint auto-rolls-back the whole tx (doWriteTx), leaving
// the earlier docs' sketch increments phantom. The fix's write-tx-begin rebase
// must keep those phantoms from accumulating or reaching disk; the real Count and
// unique enforcement are unaffected (the sketch is advisory).
func TestSketchNoDriftOnUniqueViolationRollback(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "coll")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"u"}, Unique: true}))
	insp := fx.DB.(anystore.IndexSketchInspector)

	// Commit one doc with u=99.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":0,"u":99}`)))

	// Repeatedly attempt a batch whose 3rd doc collides on u=99 → ErrUniqueConstraint
	// → the whole Insert tx auto-rolls-back, leaking the first two docs' increments.
	for it := 0; it < 25; it++ {
		base := 100 + it*10
		err = coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"u":%d}`, base, base)),
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"u":%d}`, base+1, base+1)),
			anyenc.MustParseJson(`{"id":777,"u":99}`), // collides → rollback
		)
		require.ErrorIs(t, err, anystore.ErrUniqueConstraint, "unique constraint must still be enforced (real seek, not sketch)")
	}

	// Real count is exactly 1 throughout (every batch rolled back).
	cnt, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "real Count unaffected by rolled-back unique-violation batches")

	// Sketch docCount did NOT accumulate to 1 + 25*2: it is bounded to the last
	// rolled-back batch's residual (≤ 1 + 2), cleaned up at each subsequent write-tx
	// begin. Pre-fix this would be 51.
	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	t.Logf("after 25 unique-violation rollbacks: real Count=%d, sketch docCount=%d (pre-fix would be %d)",
		cnt, st.Indexes[0].SketchDocCount, 1+25*2)
	assert.LessOrEqual(t, st.Indexes[0].SketchDocCount, uint64(1+2),
		"sketch docCount bounded to one batch's deltas, not accumulating across unique-violation rollbacks")

	// A clean commit persists the true count (2), not accumulated drift.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"u":1}`)))
	cnt, err = coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
	on, err := insp.InspectIndexSketch(ctx, "coll", "u")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), on.DocCount, "on-disk sketch == true 2 (phantoms discarded, not persisted)")
}
