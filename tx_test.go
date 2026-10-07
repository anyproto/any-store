package anystore

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// blockingFilter keeps the call evaluating it in progress: Ok blocks until
// release is closed. entered is closed as Ok first runs: the collection
// must hold a document for Ok to be reached.
type blockingFilter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingFilter() *blockingFilter {
	return &blockingFilter{entered: make(chan struct{}), release: make(chan struct{})}
}

func (f *blockingFilter) Ok(*anyenc.Value, *syncpool.DocBuffer) bool {
	f.once.Do(func() { close(f.entered) })
	<-f.release
	return true
}

func (f *blockingFilter) IndexBounds(_ string, bs query.Bounds) query.Bounds { return bs }

func (f *blockingFilter) String() string { return "blocking" }

func TestDb_WriteTx(t *testing.T) {
	t.Run("err other instance", func(t *testing.T) {
		fx := newFixture(t)
		fx2 := newFixture(t)

		tx, err := fx2.WriteTx(ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = fx.CreateCollection(tx.Context(), "test")
		assert.ErrorIs(t, err, ErrTxOtherInstance)
	})
	t.Run("err tx been used", func(t *testing.T) {
		fx := newFixture(t)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		_, err = fx.CreateCollection(tx.Context(), "test")
		assert.ErrorIs(t, err, ErrTxIsUsed)
	})
	t.Run("rollback + savepoint rollback", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)

		require.NoError(t, coll.Insert(tx.Context(),
			anyenc.MustParseJson(`{"id":1,"a":1}`),
			anyenc.MustParseJson(`{"id":2,"a":2}`),
			anyenc.MustParseJson(`{"id":3,"a":3}`),
		))
		assertCollCountInTx(tx.Context(), t, coll, 3)

		// this insert will fail because id:1 already exists, and should rollback to savepoint
		require.Error(t, coll.Insert(tx.Context(),
			anyenc.MustParseJson(`{"id":4,"a":4}`),
			anyenc.MustParseJson(`{"id":5,"a":5}`),
			anyenc.MustParseJson(`{"id":1,"a":6}`),
		))
		assertCollCountInTx(tx.Context(), t, coll, 3)

		require.NoError(t, tx.Rollback())
		assertCollCount(t, coll, 0)
	})
	// A savepoint ends with its transaction: finishing it afterwards fails
	// and leaves the state of the next writer alone.
	t.Run("savepoint outlives its tx", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))

		for i, finish := range []func(WriteTx) error{WriteTx.Commit, WriteTx.Rollback} {
			tx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			sp, err := fx.WriteTx(tx.Context())
			require.NoError(t, err)
			assert.False(t, sp.Done())
			require.NoError(t, tx.Commit())
			assert.True(t, sp.Done())

			// The next writer has postings buffered when the stale handle is used.
			next, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			require.NoError(t, coll.Insert(next.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"body":"word%d"}`, i, i))))
			assert.ErrorIs(t, finish(sp), ErrTxIsUsed)
			require.NoError(t, next.Commit())

			n, err := coll.Find(fmt.Sprintf(`{"$text":{"$search":"word%d"}}`, i)).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
		}
	})
	// A savepoint ends with the savepoint that encloses it: finishing it
	// afterwards fails and undoes nothing the transaction has done since.
	t.Run("savepoint outlives its enclosing savepoint", func(t *testing.T) {
		ends := map[string]func(WriteTx) error{"commit": WriteTx.Commit, "rollback": WriteTx.Rollback}
		for outerName, endOuter := range ends {
			for innerName, endInner := range ends {
				t.Run(outerName+" then "+innerName, func(t *testing.T) {
					fx := newFixture(t)
					dropped, err := fx.CreateCollection(ctx, "dropped")
					require.NoError(t, err)

					tx, err := fx.WriteTx(ctx)
					require.NoError(t, err)
					outer, err := fx.WriteTx(tx.Context())
					require.NoError(t, err)
					// A schema change leaves the undo log and the publication
					// list at different lengths for the inner savepoint to mark.
					_, err = fx.CreateCollection(tx.Context(), "inOuter")
					require.NoError(t, err)
					inner, err := fx.WriteTx(tx.Context())
					require.NoError(t, err)
					assert.False(t, inner.Done())
					require.NoError(t, endOuter(outer))
					assert.True(t, inner.Done())

					// A savepoint opened now takes the btree savepoint id the
					// inner one had.
					fresh, err := fx.WriteTx(tx.Context())
					require.NoError(t, err)
					created, err := fx.CreateCollection(tx.Context(), "created")
					require.NoError(t, err)
					require.NoError(t, created.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1}`)))
					require.NoError(t, dropped.Drop(tx.Context()))

					assert.ErrorIs(t, endInner(inner), ErrTxIsUsed)
					assert.False(t, fresh.Done())
					require.NoError(t, fresh.Commit())
					require.NoError(t, tx.Commit())

					assertCollCount(t, created, 1)
					_, err = dropped.Count(ctx)
					assert.ErrorIs(t, err, ErrCollectionClosed)
					_, err = fx.OpenCollection(ctx, "dropped")
					assert.ErrorIs(t, err, ErrCollectionNotFound)
					_, err = fx.OpenCollection(ctx, "inOuter")
					if outerName == "commit" {
						require.NoError(t, err)
					} else {
						assert.ErrorIs(t, err, ErrCollectionNotFound)
					}
					require.NoError(t, fx.IntegrityCheck(ctx))
				})
			}
		}
	})
	t.Run("rollback - commit race", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		var wg sync.WaitGroup

		var insertFunc = func() {
			tx, txErr := coll.WriteTx(ctx)
			require.NoError(t, txErr)
			defer func() {
				assert.NoError(t, tx.Rollback())
			}()
			assert.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":"%s", "data": %d}`, anyenc.NewObjectID().Hex(), rand.Int()))))
			assert.NoError(t, tx.Commit())
		}

		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 100 {
					insertFunc()
				}
			}()
		}
		wg.Wait()
	})
}

// A call on a transaction while another call on it is in progress — from
// a second goroutine — panics with ErrTxConcurrentCalls (txHandle.enter),
// and the transaction stays usable by the call that was in progress.
func TestTx_ConcurrentCalls(t *testing.T) {
	// during runs body while a count with a blocking filter is in progress
	// on tx from another goroutine, and lets that count finish after.
	during := func(t *testing.T, coll Collection, tx ReadTx, body func()) {
		t.Helper()
		f := newBlockingFilter()
		held := make(chan error, 1)
		go func() {
			_, err := coll.Find(f).Count(tx.Context())
			held <- err
		}()
		<-f.entered
		body()
		close(f.release)
		require.NoError(t, <-held)
	}

	t.Run("write tx", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		// Sorted in memory: Doc reads the document through the cursor (a
		// call).
		iter, err := coll.Find(nil).Sort("-a").Iter(tx.Context())
		require.NoError(t, err)
		require.True(t, iter.Next())
		during(t, coll, tx, func() {
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { _, _ = coll.Count(tx.Context()) })
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { _ = coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2}`)) })
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { _, _ = fx.WriteTx(tx.Context()) })
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { _, _ = fx.OpenCollection(tx.Context(), "test") })
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { _, _ = fx.Collection(tx.Context(), "test") })
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { _, _ = coll.Aggregate(`[{"$count":"n"}]`).Count(tx.Context()) })
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { _, _ = iter.Doc() })
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { iter.Next() })
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { _ = iter.Close() })
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { tx.SetModified() })
		})
		// The call that was in progress has ended: the transaction goes on,
		// and the refused calls changed nothing — the iterator is open.
		_, err = iter.Doc()
		require.NoError(t, err)
		require.False(t, iter.Next())
		require.NoError(t, iter.Close())
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2}`)))
		require.NoError(t, tx.Commit())
		assertCollCount(t, coll, 2)
	})

	t.Run("read tx", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
		tx, err := fx.ReadTx(ctx)
		require.NoError(t, err)
		during(t, coll, tx, func() {
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { _, _ = coll.FindId(tx.Context(), 1) })
		})
		_, err = coll.FindId(tx.Context(), 1)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	})

	t.Run("savepoint", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":0}`)))
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		// A savepoint's methods, and the calls made through its context,
		// are calls on the transaction.
		during(t, coll, tx, func() {
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { _ = coll.Insert(sp.Context(), anyenc.MustParseJson(`{"id":1}`)) })
			require.PanicsWithValue(t, ErrTxConcurrentCalls, func() { sp.SetModified() })
		})
		require.NoError(t, coll.Insert(sp.Context(), anyenc.MustParseJson(`{"id":1}`)))
		require.NoError(t, sp.Commit())
		require.NoError(t, tx.Commit())
		assertCollCount(t, coll, 2)
	})

	// Several goroutines counting on one write transaction: a call the
	// check refuses never reaches the btree page cache the transaction
	// owns, built for one caller, so with -race this subtest reports
	// nothing. Each goroutine retries a refused call, and the transaction
	// commits. The refusals are not asserted: the overlap is near certain,
	// not guaranteed.
	t.Run("counts from several goroutines", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		for i := 0; i < 2000; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%100))))
		}
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		count := func() (n int, err error) {
			defer func() {
				if r := recover(); r != nil {
					if r != ErrTxConcurrentCalls {
						panic(r)
					}
					err = ErrTxConcurrentCalls
				}
			}()
			return coll.Find(`{"a":{"$gt":5}}`).Count(tx.Context())
		}
		var refused atomic.Int32
		var wg sync.WaitGroup
		errs := make(chan error, 4)
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 20; j++ {
					n, err := count()
					if errors.Is(err, ErrTxConcurrentCalls) {
						refused.Add(1)
						runtime.Gosched()
						j--
						continue
					}
					if err != nil {
						errs <- err
						return
					}
					if n != 2000-6*20 {
						errs <- fmt.Errorf("count %d", n)
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		t.Logf("refused calls: %d", refused.Load())
		require.NoError(t, tx.Commit())
	})

	// Commit and Rollback wait a call in progress out: a deferred Rollback
	// ends the transaction after a refused call, and the writer lock goes
	// with it.
	t.Run("an end waits for the call in progress", func(t *testing.T) {
		for name, end := range map[string]func(WriteTx) error{"rollback": WriteTx.Rollback, "commit": WriteTx.Commit} {
			t.Run(name, func(t *testing.T) {
				fx := newFixture(t)
				coll, err := fx.CreateCollection(ctx, "test")
				require.NoError(t, err)
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
				tx, err := fx.WriteTx(ctx)
				require.NoError(t, err)
				require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2}`)))
				f := newBlockingFilter()
				held := make(chan error, 1)
				go func() {
					_, err := coll.Find(f).Count(tx.Context())
					held <- err
				}()
				<-f.entered
				ended := make(chan error, 1)
				go func() { ended <- end(tx) }()
				select {
				case err := <-ended:
					t.Fatalf("the end did not wait: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
				close(f.release)
				require.NoError(t, <-held)
				require.NoError(t, <-ended)
				require.True(t, tx.Done())
				_, err = coll.Count(tx.Context())
				assert.ErrorIs(t, err, ErrTxIsUsed)
				// The writer lock is free.
				next, err := fx.WriteTx(ctx)
				require.NoError(t, err)
				require.NoError(t, next.Rollback())
				if name == "commit" {
					assertCollCount(t, coll, 2)
				} else {
					assertCollCount(t, coll, 1)
				}
			})
		}
	})

	t.Run("a read end waits for the call in progress", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
		tx, err := fx.ReadTx(ctx)
		require.NoError(t, err)
		f := newBlockingFilter()
		held := make(chan error, 1)
		go func() {
			_, err := coll.Find(f).Count(tx.Context())
			held <- err
		}()
		<-f.entered
		ended := make(chan error, 1)
		go func() { ended <- tx.Commit() }()
		select {
		case err := <-ended:
			t.Fatalf("the end did not wait: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
		close(f.release)
		require.NoError(t, <-held)
		require.NoError(t, <-ended)
		require.True(t, tx.Done())
	})
}

// A call made from inside a user callback nests in the call that runs it:
// a modifier reads through the transaction's context, the collection it
// modifies and another, inside the operation that runs it.
func TestTx_NestedCalls(t *testing.T) {
	type refMod struct {
		db         DB
		coll, refs Collection
		tx         WriteTx
	}
	// modifier derives refName from the refs collection and seen from a
	// count of coll, both read through the transaction's context.
	modifier := func(m refMod) query.Modifier {
		return query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			ref, err := m.refs.FindId(m.tx.Context(), "r")
			if err != nil {
				return nil, false, err
			}
			n, err := m.coll.Count(m.tx.Context())
			if err != nil {
				return nil, false, err
			}
			if _, err = m.db.OpenCollection(m.tx.Context(), "refs"); err != nil {
				return nil, false, err
			}
			v.Set("refName", a.NewString(string(ref.Value().GetStringBytes("name"))))
			v.Set("seen", a.NewNumberInt(n))
			return v, true, nil
		})
	}
	setup := func(t *testing.T) refMod {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		refs, err := fx.CreateCollection(ctx, "refs")
		require.NoError(t, err)
		require.NoError(t, refs.Insert(ctx, anyenc.MustParseJson(`{"id":"r","name":"ref"}`)))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		return refMod{db: fx, coll: coll, refs: refs, tx: tx}
	}
	t.Run("UpdateId and UpsertId", func(t *testing.T) {
		m := setup(t)
		_, err := m.coll.UpdateId(m.tx.Context(), 1, modifier(m))
		require.NoError(t, err)
		_, err = m.coll.UpsertId(m.tx.Context(), 2, modifier(m))
		require.NoError(t, err)
		require.NoError(t, m.tx.Commit())
		for id, seen := range map[int]int{1: 1, 2: 1} {
			doc, err := m.coll.FindId(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, "ref", string(doc.Value().GetStringBytes("refName")))
			assert.Equal(t, seen, doc.Value().GetInt("seen"))
		}
	})

	t.Run("bulk update", func(t *testing.T) {
		m := setup(t)
		require.NoError(t, m.coll.Insert(m.tx.Context(), anyenc.MustParseJson(`{"id":2}`)))
		res, err := m.coll.Find(nil).Update(m.tx.Context(), modifier(m))
		require.NoError(t, err)
		assert.Equal(t, 2, res.Modified)
		require.NoError(t, m.tx.Commit())
		for _, id := range []int{1, 2} {
			doc, err := m.coll.FindId(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, "ref", string(doc.Value().GetStringBytes("refName")))
			assert.Equal(t, 2, doc.Value().GetInt("seen"))
		}
	})

	t.Run("iterator and aggregation inside a modifier", func(t *testing.T) {
		m := setup(t)
		require.NoError(t, m.coll.Insert(m.tx.Context(), anyenc.MustParseJson(`{"id":2,"ref":"r"}`)))
		_, err := m.coll.UpdateId(m.tx.Context(), 1, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			iter, err := m.coll.Find(nil).Sort("-id").Iter(m.tx.Context())
			if err != nil {
				return nil, false, err
			}
			var seen int
			for iter.Next() {
				if _, err = iter.Doc(); err != nil {
					_ = iter.Close()
					return nil, false, err
				}
				seen++
			}
			if err = errors.Join(iter.Err(), iter.Close()); err != nil {
				return nil, false, err
			}
			n, err := m.coll.Aggregate(`[{"$match":{"id":2}},{"$lookup":{"localField":"ref","foreignField":"id","as":"r"}},{"$count":"n"}]`).Count(m.tx.Context())
			if err != nil {
				return nil, false, err
			}
			v.Set("seen", a.NewNumberInt(seen))
			v.Set("looked", a.NewNumberInt(n))
			return v, true, nil
		}))
		require.NoError(t, err)
		require.NoError(t, m.tx.Commit())
		doc, err := m.coll.FindId(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, 2, doc.Value().GetInt("seen"))
		assert.Equal(t, 1, doc.Value().GetInt("looked"))
	})

	// What the operation running the modifier holds, the modifier must not
	// pull away: an end of the transaction, or of a savepoint enclosing
	// the modifier, and a write to the collection are refused. A savepoint
	// opened inside the modifier ends inside it, and other collections —
	// one opened inside the modifier included — take writes and schema
	// changes.
	t.Run("ends refused inside a modifier", func(t *testing.T) {
		m := setup(t)
		sp, err := m.db.WriteTx(m.tx.Context())
		require.NoError(t, err)
		_, err = m.coll.UpdateId(sp.Context(), 1, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			assert.ErrorIs(t, m.tx.Commit(), ErrTxEndInModifier)
			assert.ErrorIs(t, m.tx.Rollback(), ErrTxEndInModifier)
			assert.ErrorIs(t, sp.Commit(), ErrTxEndInModifier)
			assert.ErrorIs(t, sp.Rollback(), ErrTxEndInModifier)
			inner, err := m.db.WriteTx(m.tx.Context())
			if err != nil {
				return nil, false, err
			}
			if err = m.refs.Insert(inner.Context(), anyenc.MustParseJson(`{"id":"inner"}`)); err != nil {
				return nil, false, err
			}
			if err = inner.Commit(); err != nil {
				return nil, false, err
			}
			v.Set("v", a.NewNumberInt(1))
			return v, true, nil
		}))
		require.NoError(t, err)
		assert.False(t, sp.Done())
		require.NoError(t, sp.Commit())
		require.NoError(t, m.tx.Commit())
		doc, err := m.coll.FindId(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, 1, doc.Value().GetInt("v"))
		assertCollCount(t, m.refs, 2)
	})

	t.Run("writes to the collection refused inside its modifier", func(t *testing.T) {
		m := setup(t)
		require.NoError(t, m.coll.EnsureIndex(m.tx.Context(), IndexInfo{Fields: []string{"a"}}))
		fresh, err := m.db.CreateCollection(m.tx.Context(), "fresh")
		require.NoError(t, err)
		require.NoError(t, fresh.Insert(m.tx.Context(), anyenc.MustParseJson(`{"id":1,"b":1}`)))
		_, err = m.coll.UpdateId(m.tx.Context(), 1, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			tctx := m.tx.Context()
			assert.ErrorIs(t, m.coll.Insert(tctx, anyenc.MustParseJson(`{"id":9}`)), ErrWriteInModifier)
			assert.ErrorIs(t, m.coll.UpsertOne(tctx, anyenc.MustParseJson(`{"id":9}`)), ErrWriteInModifier)
			assert.ErrorIs(t, m.coll.DeleteId(tctx, 1), ErrWriteInModifier)
			_, err := m.coll.UpsertId(tctx, 9, query.MustParseModifier(`{"$set":{"a":1}}`))
			assert.ErrorIs(t, err, ErrWriteInModifier)
			_, err = m.coll.Find(nil).Update(tctx, query.MustParseModifier(`{"$set":{"a":1}}`))
			assert.ErrorIs(t, err, ErrWriteInModifier)
			_, err = m.coll.Find(nil).Delete(tctx)
			assert.ErrorIs(t, err, ErrWriteInModifier)
			assert.ErrorIs(t, m.coll.EnsureIndex(tctx, IndexInfo{Fields: []string{"b"}}), ErrWriteInModifier)
			assert.ErrorIs(t, m.coll.DropIndex(tctx, "a"), ErrWriteInModifier)
			assert.ErrorIs(t, m.coll.Rename(tctx, "renamed"), ErrWriteInModifier)
			assert.ErrorIs(t, m.coll.Drop(tctx), ErrWriteInModifier)
			// Reads of the collection, and other collections, nest.
			if _, err = m.coll.Count(tctx); err != nil {
				return nil, false, err
			}
			if err = m.refs.Insert(tctx, anyenc.MustParseJson(`{"id":"other"}`)); err != nil {
				return nil, false, err
			}
			opened, err := m.db.OpenCollection(tctx, "fresh")
			if err != nil {
				return nil, false, err
			}
			if err = opened.EnsureIndex(tctx, IndexInfo{Fields: []string{"b"}}); err != nil {
				return nil, false, err
			}
			if err = opened.DropIndex(tctx, "b"); err != nil {
				return nil, false, err
			}
			if err = opened.EnsureIndex(tctx, IndexInfo{Fields: []string{"b"}}); err != nil {
				return nil, false, err
			}
			v.Set("a", a.NewNumberInt(7))
			return v, true, nil
		}))
		require.NoError(t, err)
		// A bulk modifier the same way.
		_, err = m.coll.Find(nil).Update(m.tx.Context(), query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			assert.ErrorIs(t, m.coll.Insert(m.tx.Context(), anyenc.MustParseJson(`{"id":9}`)), ErrWriteInModifier)
			return v, false, nil
		}))
		require.NoError(t, err)
		require.NoError(t, m.tx.Commit())
		assertCollCount(t, m.coll, 1)
		assertQueryCount(t, m.coll.Find(`{"a":7}`), 1)
		require.Len(t, m.coll.GetIndexes(), 1)
		assertCollCount(t, m.refs, 2)
		assertQueryCount(t, fresh.Find(`{"b":1}`), 1)
		require.Len(t, fresh.GetIndexes(), 1)
	})
}

// A call on a transaction another goroutine has ended finds it ended, and
// does not touch the state the transaction gave back.
func TestWriteTx_CallAfterAnotherGoroutineEnds(t *testing.T) {
	t.Run("iterator", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))))
		}
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":10}`)))
		iter, err := coll.Find(nil).Iter(tx.Context())
		require.NoError(t, err)
		require.True(t, iter.Next())
		_, err = iter.Doc()
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() { done <- tx.Commit() }()
		require.NoError(t, <-done)

		// The document the last Next parsed is the iterator's own memory;
		// ended, the transaction has nothing more to give.
		_, err = iter.Doc()
		assert.ErrorIs(t, err, ErrTxIsUsed)
		assert.False(t, iter.Next())
		assert.ErrorIs(t, iter.Err(), ErrTxIsUsed)
		_, err = iter.Doc()
		assert.ErrorIs(t, err, ErrTxIsUsed)
		assert.NoError(t, iter.Close())
		_, err = coll.Count(tx.Context())
		assert.ErrorIs(t, err, ErrTxIsUsed)
		assertCollCount(t, coll, 11)
	})

	t.Run("aggregation with a blocking stage", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		for i := 0; i < 20; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"g":%d}`, i, i%4))))
		}
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		// $group drains the collection before the first row comes back;
		// the rows are in memory, and end with the transaction all the same.
		iter, err := coll.Aggregate(`[{"$group":{"_id":"$g","n":{"$sum":1}}}]`).Iter(tx.Context())
		require.NoError(t, err)
		require.True(t, iter.Next())
		_, err = iter.Doc()
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- tx.Commit() }()
		require.NoError(t, <-done)
		_, err = iter.Doc()
		assert.ErrorIs(t, err, ErrTxIsUsed)
		assert.False(t, iter.Next())
		assert.ErrorIs(t, iter.Err(), ErrTxIsUsed)
		assert.NoError(t, iter.Close())

		// An empty prefix, with a row a stage synthesizes.
		tx, err = fx.WriteTx(ctx)
		require.NoError(t, err)
		iter, err = coll.Aggregate(`[{"$match":{"id":{"$in":[]}}},{"$count":"n"}]`).Iter(tx.Context())
		require.NoError(t, err)
		require.True(t, iter.Next())
		require.NoError(t, tx.Commit())
		assert.False(t, iter.Next())
		assert.ErrorIs(t, iter.Err(), ErrTxIsUsed)
		assert.NoError(t, iter.Close())
	})
}

// A handle of a transaction that ended fails with ErrTxIsUsed and touches
// nothing of the transaction that reuses the pooled state: its call counters
// and its context are the handle's own.
func TestWriteTx_HandleOfEndedTx(t *testing.T) {
	t.Run("context", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		// The next transaction takes the pooled state.
		tx2, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		defer func() { _ = tx2.Rollback() }()
		assert.ErrorIs(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1}`)), ErrTxIsUsed)
		_, err = coll.Count(tx.Context())
		assert.ErrorIs(t, err, ErrTxIsUsed)
		assert.NoError(t, tx.Rollback())
		assertCollCountInTx(tx2.Context(), t, coll, 0)
	})

	t.Run("call counts on its own handle", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
		tx1, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, tx1.Commit())
		tx2, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		f := newBlockingFilter()
		held := make(chan error, 1)
		go func() {
			_, err := coll.Find(f).Count(tx2.Context())
			held <- err
		}()
		<-f.entered
		// tx2 is inside a call: a call on tx1 counts on tx1's handle, and
		// finds the transaction ended.
		_, err = coll.Count(tx1.Context())
		assert.ErrorIs(t, err, ErrTxIsUsed)
		close(f.release)
		require.NoError(t, <-held)
		require.NoError(t, tx2.Commit())
	})

	t.Run("savepoint context after its state is reused", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(sp.Context(), anyenc.MustParseJson(`{"id":1}`)))
		require.NoError(t, sp.Commit())
		// Every write in the transaction opens a savepoint of its own, on
		// the pooled state sp used. sp's context stays the transaction's:
		// a call through it is a call on the transaction like any other.
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2}`)))
		_, err = coll.FindId(sp.Context(), 1)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(sp.Context(), anyenc.MustParseJson(`{"id":3}`)))
		require.NoError(t, tx.Commit())
		assertCollCount(t, coll, 3)
	})

	t.Run("SetModified", func(t *testing.T) {
		fx := newFixture(t)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		tx2, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		tx.SetModified()
		assert.False(t, tx2.(writeTx).modified.Load())
		require.NoError(t, tx2.Rollback())
	})

	t.Run("savepoint Done inside a modifier", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"v":0}`)))
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		var seen bool
		_, err = coll.UpdateId(sp.Context(), 1, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			seen = sp.Done()
			return v, false, nil
		}))
		require.NoError(t, err)
		assert.False(t, seen)
		require.NoError(t, sp.Commit())
		assert.True(t, sp.Done())
		require.NoError(t, tx.Rollback())
	})
}

// The end of a transaction trips the iterators open on it: their cursors
// are closed with the transaction — the pages go with it — and an iterator
// closed afterwards holds none.
func TestWriteTx_EndTripsOpenIterators(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	for i := 0; i < 300; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%7))))
	}
	for i := 0; i < 50; i++ {
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		// Partly drained: a leaf stays pinned under the cursor, and the
		// Doc fallback cursor sits on another.
		iter, err := coll.Find(`{"a":3}`).Sort("-id").Iter(tx.Context())
		require.NoError(t, err)
		require.True(t, iter.Next())
		_, err = iter.Doc()
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		pi := iter.(*planIterator)
		require.True(t, pi.tripped, "the commit did not trip the iterator")
		require.Nil(t, pi.dataCursor, "the fallback cursor outlived the transaction")
		assert.False(t, iter.Next())
		assert.ErrorIs(t, iter.Err(), ErrTxIsUsed)
		require.NoError(t, iter.Close())
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	iter, err := coll.Find(nil).Iter(rtx.Context())
	require.NoError(t, err)
	require.True(t, iter.Next())
	require.NoError(t, rtx.Commit())
	require.True(t, iter.(*planIterator).tripped)
	assert.False(t, iter.Next())
	assert.ErrorIs(t, iter.Err(), ErrTxIsUsed)
	require.NoError(t, iter.Close())
}

// The calls an operation makes on the transaction its context carries,
// against the same operations with no transaction: what a call on the
// transaction (txHandle.enter) costs on the in-transaction paths.
func BenchmarkWriteTx_Calls(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(b, err)
	require.NoError(b, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := 0; i < 1000; i++ {
		require.NoError(b, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))))
	}
	mod := query.MustParseModifier(`{"$inc":{"v":1}}`)
	p := &anyenc.Parser{}
	for _, inTx := range []bool{false, true} {
		name := "own tx"
		c := ctx
		if inTx {
			name = "in tx"
			tx, err := fx.WriteTx(ctx)
			require.NoError(b, err)
			c = tx.Context()
			defer func() { require.NoError(b, tx.Commit()) }()
		}
		b.Run(name+"/FindId", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_, err := coll.FindIdWithParser(c, p, 1)
				require.NoError(b, err)
			}
		})
		b.Run(name+"/Count", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				n, err := coll.Find(`{"a":3}`).Count(c)
				require.NoError(b, err)
				require.Equal(b, 100, n)
			}
		})
		b.Run(name+"/Iter100", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				iter, err := coll.Find(`{"a":3}`).Iter(c)
				require.NoError(b, err)
				var n int
				for iter.Next() {
					if _, err = iter.Doc(); err != nil {
						b.Fatal(err)
					}
					n++
				}
				require.NoError(b, iter.Close())
				require.Equal(b, 100, n)
			}
		})
		b.Run(name+"/UpdateId", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_, err := coll.UpdateId(c, 1, mod)
				require.NoError(b, err)
			}
		})
	}
}

// sketchCounts is a persisted sketch as a comparable, printable value: the
// document count and the non-empty buckets.
func sketchCounts(t *testing.T, dbi *db, coll, idx string) (counts map[int]uint64) {
	info, err := dbi.InspectIndexSketch(ctx, coll, idx)
	require.NoError(t, err)
	counts = map[int]uint64{-1: info.DocCount}
	for i, n := range info.Buckets {
		if n != 0 {
			counts[i] = n
		}
	}
	return counts
}

// The sketch deltas of the verbs a savepoint reverts go with it: the commit
// persists the counts the savepoint found.
func TestWriteTx_SavepointRollbackRevertsSketch(t *testing.T) {
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	before := sketchCounts(t, dbi, "c", "a")
	require.EqualValues(t, 1, before[-1])

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	sp, err := fx.WriteTx(tx.Context())
	require.NoError(t, err)
	require.NoError(t, coll.DeleteId(sp.Context(), 1))
	require.NoError(t, sp.Rollback())
	require.NoError(t, tx.Commit())

	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assert.Equal(t, before, sketchCounts(t, dbi, "c", "a"), "the persisted sketch counts the document the rollback kept")
	idx := coll.(*collection).loadIndexes()[0]
	assert.EqualValues(t, 1, idx.sketch.GetDocCount(), "the live sketch too")
}

// A verb that fails inside a transaction runs under a savepoint of its
// own: the sketch deltas it made before failing go with it.
func TestWriteTx_FailedVerbRevertsSketch(t *testing.T) {
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		IndexInfo{Fields: []string{"a"}},
		IndexInfo{Fields: []string{"u"}, Unique: true},
	))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1,"u":1}`),
		anyenc.MustParseJson(`{"id":2,"a":2,"u":2}`),
	))
	before := sketchCounts(t, dbi, "c", "a")

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	_, err = coll.UpdateId(tx.Context(), 2, query.MustParseModifier(`{"$set":{"v":1}}`))
	require.NoError(t, err)
	_, err = coll.UpdateId(tx.Context(), 1, query.MustParseModifier(`{"$set":{"a":5,"u":2}}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
	require.NoError(t, tx.Commit())

	assert.Equal(t, before, sketchCounts(t, dbi, "c", "a"), "no entry of a moved")
}

// A commit the btree refuses leaves no sketch delta behind: the next write
// transaction rebases the live sketches to the committed bytes, and no
// later commit persists what the failed one made.
func TestWriteTx_FailedCommitDiscardsSketchDeltas(t *testing.T) {
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2,"a":2}`)))
	// Rolling the btree tx back under the commit makes the commit fail
	// after the sketches were persisted into it, as an I/O error would.
	testHookBeforeBtreeCommit = func(btx *btree.WriteTx) { require.NoError(t, btx.Rollback()) }
	err = tx.Commit()
	testHookBeforeBtreeCommit = nil
	require.ErrorIs(t, err, btree.ErrTxClosed)

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":3}`)))
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	assert.EqualValues(t, 2, sketchCounts(t, dbi, "c", "a")[-1], "the document the failed commit inserted is not counted")
	idx := coll.(*collection).loadIndexes()[0]
	assert.EqualValues(t, 2, idx.sketch.GetDocCount())
}

// sketchRebuilt is the sketch a fresh build of the index persists: the
// ground truth for an index whose maintenance was exact.
func sketchRebuilt(t *testing.T, dbi *db, coll Collection, idx string) map[int]uint64 {
	var info IndexInfo
	for _, i := range coll.GetIndexes() {
		if i.Info().Name == idx {
			info = i.Info()
		}
	}
	require.NoError(t, coll.DropIndex(ctx, idx))
	require.NoError(t, coll.EnsureIndex(ctx, info))
	return sketchCounts(t, dbi, coll.Name(), idx)
}

// Nested savepoints: an image taken inside an inner savepoint serves the
// outer one once the inner is released; an older image of the outer wins
// over the inner's; an inner rollback leaves the outer's writes in place.
func TestWriteTx_NestedSavepointsRevertSketch(t *testing.T) {
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"u":%d}`, id, id, id))
	}
	setup := func(t *testing.T) (*db, Collection) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx,
			IndexInfo{Fields: []string{"a"}},
			IndexInfo{Fields: []string{"u"}, Unique: true},
		))
		require.NoError(t, coll.Insert(ctx, doc(1)))
		return fx.DB.(*db), coll
	}
	check := func(t *testing.T, dbi *db, coll Collection, docs int) {
		n, err := coll.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, docs, n)
		for _, idx := range []string{"a", "u"} {
			persisted := sketchCounts(t, dbi, "c", idx)
			assert.EqualValues(t, docs, persisted[-1], idx)
			assert.Equal(t, sketchRebuilt(t, dbi, coll, idx), persisted, idx)
		}
	}

	t.Run("released into the outer, the outer rolled back", func(t *testing.T) {
		dbi, coll := setup(t)
		tx, err := dbi.WriteTx(ctx)
		require.NoError(t, err)
		outer, err := dbi.WriteTx(tx.Context())
		require.NoError(t, err)
		inner, err := dbi.WriteTx(outer.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(inner.Context(), doc(2)))
		require.NoError(t, inner.Commit())
		require.NoError(t, coll.Insert(outer.Context(), doc(3)))
		require.NoError(t, outer.Rollback())
		require.NoError(t, tx.Commit())
		check(t, dbi, coll, 1)
	})

	t.Run("the outer's older image wins", func(t *testing.T) {
		dbi, coll := setup(t)
		tx, err := dbi.WriteTx(ctx)
		require.NoError(t, err)
		outer, err := dbi.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(outer.Context(), doc(2)))
		inner, err := dbi.WriteTx(outer.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(inner.Context(), doc(3)))
		require.NoError(t, inner.Commit())
		require.NoError(t, coll.Insert(outer.Context(), doc(4)))
		require.NoError(t, outer.Rollback())
		require.NoError(t, tx.Commit())
		check(t, dbi, coll, 1)
	})

	t.Run("the inner rolled back, the outer released", func(t *testing.T) {
		dbi, coll := setup(t)
		tx, err := dbi.WriteTx(ctx)
		require.NoError(t, err)
		outer, err := dbi.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, coll.DeleteId(outer.Context(), 1))
		inner, err := dbi.WriteTx(outer.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(inner.Context(), doc(2), doc(3)))
		require.NoError(t, inner.Rollback())
		require.NoError(t, coll.Insert(outer.Context(), doc(4)))
		require.NoError(t, outer.Commit())
		require.NoError(t, tx.Commit())
		check(t, dbi, coll, 1)
	})

	t.Run("the outer ends the inner it encloses", func(t *testing.T) {
		dbi, coll := setup(t)
		tx, err := dbi.WriteTx(ctx)
		require.NoError(t, err)
		outer, err := dbi.WriteTx(tx.Context())
		require.NoError(t, err)
		inner, err := dbi.WriteTx(outer.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(inner.Context(), doc(2)))
		require.NoError(t, outer.Rollback())
		require.ErrorIs(t, inner.Rollback(), ErrTxIsUsed)
		require.NoError(t, coll.Insert(tx.Context(), doc(3)))
		require.NoError(t, tx.Commit())
		check(t, dbi, coll, 2)
	})

	t.Run("a verb failing inside a savepoint", func(t *testing.T) {
		dbi, coll := setup(t)
		tx, err := dbi.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := dbi.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(sp.Context(), doc(2)))
		_, err = coll.UpdateId(sp.Context(), 1, query.MustParseModifier(`{"$set":{"a":7,"u":2}}`))
		require.ErrorIs(t, err, ErrUniqueConstraint)
		require.NoError(t, sp.Commit())
		require.NoError(t, tx.Commit())
		check(t, dbi, coll, 2)
	})

	t.Run("a full rollback after savepoints", func(t *testing.T) {
		dbi, coll := setup(t)
		tx, err := dbi.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := dbi.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(sp.Context(), doc(2)))
		require.NoError(t, sp.Commit())
		require.NoError(t, coll.Insert(tx.Context(), doc(3)))
		require.NoError(t, tx.Rollback())
		require.NoError(t, coll.Insert(ctx, doc(4)))
		check(t, dbi, coll, 2)
	})
}

// A schema-changing commit settles the sketches it persisted as it
// installs the schema; a failed one leaves them flagged.
func TestWriteTx_SchemaCommitSettlesSketches(t *testing.T) {
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	idx := coll.(*collection).loadIndexes()[0]

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2,"a":2}`)))
	_, err = fx.CreateCollection(tx.Context(), "d")
	require.NoError(t, err)
	assert.True(t, idx.sketchModified)
	testHookBeforeBtreeCommit = func(btx *btree.WriteTx) { require.NoError(t, btx.Rollback()) }
	err = tx.Commit()
	testHookBeforeBtreeCommit = nil
	require.ErrorIs(t, err, btree.ErrTxClosed)
	assert.True(t, idx.sketchModified, "a failed commit settles nothing")
	assert.EqualValues(t, 2, idx.sketch.GetDocCount())

	tx, err = fx.WriteTx(ctx)
	require.NoError(t, err)
	assert.False(t, idx.sketchModified, "rebased at begin")
	assert.EqualValues(t, 1, idx.sketch.GetDocCount())
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":3,"a":3}`)))
	_, err = fx.CreateCollection(tx.Context(), "d")
	require.NoError(t, err)
	idx.storePubSketch(qplanner.NewIndexSketch(qplanner.DefaultSketchSize, 1)) // as a peer's reload leaves one
	require.NoError(t, tx.Commit())
	assert.False(t, idx.sketchModified)
	assert.Same(t, idx.sketch, idx.loadPubSketch(), "republished by the schema commit")
	assert.EqualValues(t, 2, sketchCounts(t, dbi, "c", "a")[-1])
}

// A savepoint left open when its transaction ends keeps nothing: its
// pooled state serves the next savepoint — of any database — with no image
// of a sketch the earlier one wrote.
func TestWriteTx_SavepointLeftOpenAtTxEnd(t *testing.T) {
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, id, id))
	}
	setup := func(t *testing.T) (*db, Collection, *index) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx, doc(1)))
		return fx.DB.(*db), coll, coll.(*collection).loadIndexes()[0]
	}
	// leaveOpen writes doc 2 inside a savepoint and commits the transaction
	// over it; the savepoint's late end finds it orphaned.
	leaveOpen := func(t *testing.T, dbi *db, coll Collection) {
		tx, err := dbi.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := dbi.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(sp.Context(), doc(2)))
		require.NoError(t, tx.Commit())
		state := sp.(savepointWrapper).sp
		require.ErrorIs(t, sp.Rollback(), ErrTxIsUsed)
		assert.Empty(t, state.images, "the state goes to the pool clean")
	}

	t.Run("same database", func(t *testing.T) {
		dbi, coll, idx := setup(t)
		leaveOpen(t, dbi, coll)
		tx, err := dbi.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := dbi.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(sp.Context(), doc(3)))
		require.NoError(t, sp.Rollback())
		assert.EqualValues(t, 2, idx.sketch.GetDocCount(), "the rollback restores its own image only")
		require.NoError(t, coll.Insert(tx.Context(), doc(4)))
		require.NoError(t, tx.Commit())
		assert.Equal(t, sketchRebuilt(t, dbi, coll, "a"), sketchCounts(t, dbi, "c", "a"))
	})

	t.Run("another database", func(t *testing.T) {
		db1, coll1, idx1 := setup(t)
		db2, coll2, _ := setup(t)
		leaveOpen(t, db1, coll1)
		require.NoError(t, coll1.Insert(ctx, doc(3)))
		require.EqualValues(t, 3, idx1.sketch.GetDocCount())

		tx, err := db2.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := db2.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, coll2.Insert(sp.Context(), doc(2)))
		require.NoError(t, sp.Rollback())
		require.NoError(t, tx.Commit())
		assert.EqualValues(t, 3, idx1.sketch.GetDocCount(), "untouched by the other database's rollback")
	})
}

// A savepoint holds one image per index its scope wrote, however many
// verbs wrote it: the verbs' own savepoints hand their images up once and
// drop them from then on. A rename's clone is another index object of the
// same sketch: its newer image restores first, the older wins.
func TestWriteTx_SavepointImagesOncePerIndex(t *testing.T) {
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, id, id, id))
	}
	t.Run("one per index", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}, IndexInfo{Fields: []string{"b"}}))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(sp.Context(), doc(i)))
		}
		assert.Len(t, sp.(savepointWrapper).sp.images, 2)
		require.NoError(t, sp.Rollback())
		require.NoError(t, tx.Commit())
		n, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
	})

	t.Run("a clone's newer image", func(t *testing.T) {
		fx := newFixture(t)
		dbi := fx.DB.(*db)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx, doc(1)))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		outer, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		inner, err := fx.WriteTx(outer.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(inner.Context(), doc(2)))
		require.NoError(t, coll.Rename(inner.Context(), "d"))
		require.NoError(t, inner.Commit())
		require.NoError(t, coll.Insert(outer.Context(), doc(3)))
		require.Len(t, outer.(savepointWrapper).sp.images, 2, "the original's and the clone's")
		require.NoError(t, outer.Rollback())
		require.NoError(t, tx.Commit())
		require.NoError(t, coll.Insert(ctx, doc(4)))
		assert.EqualValues(t, 2, sketchCounts(t, dbi, "c", "a")[-1])
		assert.Equal(t, sketchRebuilt(t, dbi, coll, "a"), sketchCounts(t, dbi, "c", "a"))
	})
}

// A rollback restores every counter, the level totals included, and a
// commit of sketches alone republishes the live sketch and clears the flag.
func TestWriteTx_SketchRestoreAndSettle(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, id, id, id))
	}
	require.NoError(t, coll.Insert(ctx, doc(1)))
	idx := coll.(*collection).loadIndexes()[0]
	before := idx.sketch.MarshalBinary(nil)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	sp, err := fx.WriteTx(tx.Context())
	require.NoError(t, err)
	require.NoError(t, coll.Insert(sp.Context(), doc(2), doc(3)))
	require.NoError(t, sp.Rollback())
	assert.Equal(t, before, idx.sketch.MarshalBinary(nil))
	require.NoError(t, tx.Commit())

	// A reader-owned snapshot, as a peer's reload leaves one.
	idx.storePubSketch(qplanner.NewIndexSketch(qplanner.DefaultSketchSize, 2))
	require.NoError(t, coll.Insert(ctx, doc(4)))
	assert.False(t, idx.sketchModified)
	assert.Same(t, idx.sketch, idx.loadPubSketch(), "republished")
}
