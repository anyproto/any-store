package anystore

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
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

// blockingModifier parks the modifier it runs until released, like
// blockingFilter.
type blockingModifier struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingModifier() *blockingModifier {
	return &blockingModifier{entered: make(chan struct{}), release: make(chan struct{})}
}

func (m *blockingModifier) Modify(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
	m.once.Do(func() { close(m.entered) })
	<-m.release
	v.Set("m", a.NewNumberInt(1))
	return v, true, nil
}

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
	// pull away or layer over: an end of the transaction or of a savepoint,
	// a savepoint of its own, and a write to the collection are refused.
	// Other collections — one opened inside the modifier included — take
	// writes and schema changes.
	t.Run("ends and savepoints refused inside a modifier", func(t *testing.T) {
		m := setup(t)
		sp, err := m.db.WriteTx(m.tx.Context())
		require.NoError(t, err)
		_, err = m.coll.UpdateId(sp.Context(), 1, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			assert.ErrorIs(t, m.tx.Commit(), ErrTxEndInModifier)
			assert.ErrorIs(t, m.tx.Rollback(), ErrTxEndInModifier)
			assert.ErrorIs(t, sp.Commit(), ErrTxEndInModifier)
			assert.ErrorIs(t, sp.Rollback(), ErrTxEndInModifier)
			inner, err := m.db.WriteTx(m.tx.Context())
			assert.ErrorIs(t, err, ErrSavepointInModifier)
			assert.Nil(t, inner)
			inner, err = m.coll.WriteTx(m.tx.Context())
			assert.ErrorIs(t, err, ErrSavepointInModifier)
			assert.Nil(t, inner)
			if err := m.refs.Insert(m.tx.Context(), anyenc.MustParseJson(`{"id":"inner"}`)); err != nil {
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
			_, err = m.refs.Aggregate(`[{"$out":"test"}]`).Count(tctx)
			assert.ErrorIs(t, err, ErrWriteInModifier)
			_, err = m.refs.Aggregate(`[{"$merge":{"into":"test"}}]`).Count(tctx)
			assert.ErrorIs(t, err, ErrWriteInModifier)
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

// A savepoint a modifier opens would sit above the scope of the operation
// running it, and the operation's later writes would land inside it: a
// rollback from a later call of a bulk verb's modifier undid the verb's
// writes between the calls. The open is refused instead.
func TestWriteTx_SavepointRefusedInBulkModifier(t *testing.T) {
	setup := func(t *testing.T) (*fixture, Collection, Collection, WriteTx) {
		fx := newFixture(t)
		c, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		d, err := fx.CreateCollection(ctx, "d")
		require.NoError(t, err)
		for i := range 3 {
			require.NoError(t, c.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":0}`, i))))
		}
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		return fx, c, d, tx
	}

	// The shape of the bug: opened on the first call, rolled back on the
	// third. Unrefused, the rollback undid the first two documents' writes.
	t.Run("refusal ignored: every document written", func(t *testing.T) {
		fx, c, d, tx := setup(t)
		var sp WriteTx
		var opens, ends []error
		n := 0
		res, err := c.Find(nil).Update(tx.Context(), query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			n++
			switch {
			case n == 1:
				var err error
				sp, err = fx.WriteTx(tx.Context())
				opens = append(opens, err)
			case n == 3 && sp != nil:
				ends = append(ends, sp.Rollback())
			}
			if err := d.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, n))); err != nil {
				return nil, false, err
			}
			v.Set("v", a.NewNumberInt(1))
			return v, true, nil
		}))
		require.NoError(t, err)
		assert.Equal(t, ModifyResult{Matched: 3, Modified: 3}, res)
		require.Len(t, opens, 1)
		assert.ErrorIs(t, opens[0], ErrSavepointInModifier)
		assert.Nil(t, sp)
		assert.Empty(t, ends)
		n, err = c.Find(`{"v":1}`).Count(tx.Context())
		require.NoError(t, err)
		assert.Equal(t, 3, n)
		require.NoError(t, tx.Commit())
		assertQueryCount(t, c.Find(`{"v":1}`), 3)
		assertCollCount(t, d, 3)
		require.NoError(t, fx.IntegrityCheck(ctx))
	})

	t.Run("refusal returned: the verb fails, the transaction goes on", func(t *testing.T) {
		fx, c, d, tx := setup(t)
		_, err := c.Find(nil).Update(tx.Context(), query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			if err := d.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1}`)); err != nil {
				return nil, false, err
			}
			_, err := fx.WriteTx(tx.Context())
			return nil, false, err
		}))
		assert.ErrorIs(t, err, ErrSavepointInModifier)
		// The verb's scope is rolled back, the modifier's insert with it.
		n, err := d.Find(nil).Count(tx.Context())
		require.NoError(t, err)
		assert.Equal(t, 0, n)
		require.NoError(t, c.Insert(tx.Context(), anyenc.MustParseJson(`{"id":3,"v":0}`)))
		require.NoError(t, tx.Commit())
		assertQueryCount(t, c.Find(`{"v":1}`), 0)
		assertCollCount(t, c, 4)
		assertCollCount(t, d, 0)
	})
}

// A modifier refuses the writes to its collection made from inside it,
// in its own transaction: a write from another transaction waits for the
// writer lock, as it does behind any transaction, and lands once the
// modifier's transaction has ended.
func TestWriteTx_ModifierRefusesOnlyItsOwnTransaction(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))

	m := newBlockingModifier()
	held := make(chan error, 1)
	go func() {
		_, err := coll.UpdateId(ctx, 1, m)
		held <- err
	}()
	<-m.entered

	writes := map[string]func() error{
		"insert": func() error {
			return coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":2}`))
		},
		"bulk update": func() error {
			_, err := coll.Find(`{"id":1}`).Update(ctx, `{"$set":{"b":1}}`)
			return err
		},
		"ensure index": func() error {
			return coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}})
		},
		"explicit tx": func() error {
			tx, err := fx.WriteTx(ctx)
			if err != nil {
				return err
			}
			if err = coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":3,"a":3}`)); err != nil {
				return errors.Join(err, tx.Rollback())
			}
			return tx.Commit()
		},
	}
	type result struct {
		name string
		err  error
	}
	results := make(chan result, len(writes))
	for name, write := range writes {
		go func() { results <- result{name, write()} }()
	}
	// Each write began while the modifier runs: none ends before it.
	select {
	case r := <-results:
		t.Fatalf("%s returned while the modifier runs: %v", r.name, r.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(m.release)
	require.NoError(t, <-held)
	for range writes {
		r := <-results
		assert.NoError(t, r.err, r.name)
	}

	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, doc.Value().GetInt("m"))
	assert.Equal(t, 1, doc.Value().GetInt("b"))
	assertCollCount(t, coll, 3)
	require.Len(t, coll.GetIndexes(), 1)
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
		require.True(t, pi.tripped.Load(), "the commit did not trip the iterator")
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
	require.True(t, iter.(*planIterator).tripped.Load())
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

// A savepoint rollback ends the savepoint: the btree level it rolled back
// to goes with it, as SQLite's statement journal rolls back to and then
// releases its savepoint (vdbeCloseStatement). A level left on the pager's
// stack takes a copy of every page the transaction writes from then on and
// holds it until the transaction ends.
func TestWriteTx_RollbackReleasesBtreeSavepoint(t *testing.T) {
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"u":%d}`, id, id))
	}
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"u"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, doc(1), doc(2)))

	// depth is the number of btree savepoints open on tx: the id the next
	// one gets, released again at once.
	depth := func(t *testing.T, tx WriteTx) int {
		btx := tx.btreeWriteTx()
		id, err := btx.Savepoint()
		require.NoError(t, err)
		require.NoError(t, btx.ReleaseSavepoint(id))
		return id
	}

	t.Run("failed verb", func(t *testing.T) {
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		_, err = coll.UpdateId(tx.Context(), 1, query.MustParseModifier(`{"$set":{"u":2}}`))
		require.ErrorIs(t, err, ErrUniqueConstraint)
		assert.Equal(t, 0, depth(t, tx))
	})

	t.Run("savepoint rollback", func(t *testing.T) {
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(sp.Context(), doc(3)))
		require.NoError(t, sp.Rollback())
		assert.Equal(t, 0, depth(t, tx))
	})

	t.Run("nested savepoint rollback", func(t *testing.T) {
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		outer, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		inner, err := fx.WriteTx(outer.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(inner.Context(), doc(3)))
		require.NoError(t, inner.Rollback())
		assert.Equal(t, 1, depth(t, tx), "the outer savepoint stays")
		require.NoError(t, outer.Rollback())
		assert.Equal(t, 0, depth(t, tx))
	})
}

// An inner savepoint's rollback hands the pre-images of the pages its
// scope wrote to the enclosing savepoint, which plays them back with its
// own when it is rolled back in turn.
func TestWriteTx_OuterRollbackAfterInnerRollback(t *testing.T) {
	pad := strings.Repeat("x", 200)
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"u":%d,"p":"%s"}`, id, id, pad))
	}
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"u"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, doc(1), doc(2)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	outer, err := fx.WriteTx(tx.Context())
	require.NoError(t, err)
	inner, err := fx.WriteTx(outer.Context())
	require.NoError(t, err)
	for i := 3; i < 203; i++ {
		require.NoError(t, coll.Insert(inner.Context(), doc(i)))
	}
	require.NoError(t, inner.Rollback())
	for i := 203; i < 403; i++ {
		require.NoError(t, coll.Insert(outer.Context(), doc(i)))
	}
	_, err = coll.UpdateId(outer.Context(), 1, query.MustParseModifier(`{"$set":{"u":2}}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
	require.NoError(t, outer.Rollback())
	require.NoError(t, coll.Insert(tx.Context(), doc(3)), "the unique index forgot the inner scope's entry")
	require.NoError(t, tx.Commit())

	n, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	_, err = coll.FindId(ctx, 203)
	assert.ErrorIs(t, err, ErrDocNotFound)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// A savepoint's rollback trips the iterators opened or moved inside its
// scope: the pages they stand on are the rollback's to discard. Next and Doc then
// fail with ErrIterRolledBack, Close returns nil, and the transaction goes
// on. SQLite trips the cursors of a ROLLBACK TO before the pager plays the
// savepoint back (sqlite3BtreeTripAllCursors, saveAllCursors).
func TestSavepoint_RollbackTripsIteratorsPositionedInside(t *testing.T) {
	pad := strings.Repeat("x", 400)
	for _, mode := range []string{"next", "close", "nextAfterWrites"} {
		t.Run(mode, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "c")
			require.NoError(t, err)
			for i := 1; i <= 10; i++ {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":"base"}`, i))))
			}
			tx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			sp, err := fx.WriteTx(tx.Context())
			require.NoError(t, err)
			for i := 1000; i < 2000; i++ {
				require.NoError(t, coll.Insert(sp.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":"sp","pad":"%s"}`, i, pad))))
			}
			// Parked on a page the savepoint allocated.
			iter, err := coll.Find(nil).Iter(sp.Context())
			require.NoError(t, err)
			for read := 0; read < 600; read++ {
				require.True(t, iter.Next())
			}
			_, err = iter.Doc()
			require.NoError(t, err)
			require.NoError(t, sp.Rollback())

			require.True(t, iter.(*planIterator).tripped.Load(), "the rollback did not trip the iterator")
			if mode == "nextAfterWrites" {
				// The page structs the rollback freed are reused.
				for i := 5000; i < 5600; i++ {
					require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":"after","pad":"%s"}`, i, pad))))
				}
			}
			if mode != "close" {
				_, err = iter.Doc()
				assert.ErrorIs(t, err, ErrIterRolledBack)
				assert.False(t, iter.Next())
				assert.ErrorIs(t, iter.Err(), ErrIterRolledBack)
				_, err = iter.Doc()
				assert.ErrorIs(t, err, ErrIterRolledBack)
			}
			require.NoError(t, iter.Close())

			require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":7777,"v":"tail"}`)))
			require.NoError(t, tx.Commit())
			n, err := coll.Count(ctx)
			require.NoError(t, err)
			want := 11
			if mode == "nextAfterWrites" {
				want += 600
			}
			assert.Equal(t, want, n)
			require.NoError(t, fx.IntegrityCheck(ctx))
		})
	}
}

// An iterator that last moved before the savepoint opened is not tripped by
// its rollback: the pages it stands on are restored to what it saw, and it
// yields exactly the rest of its scan — SQLite saves such cursors and lets
// them continue. The savepoint splits the leaf under the cursor, or writes
// another collection.
func TestSavepoint_RollbackKeepsIteratorsPositionedBefore(t *testing.T) {
	pad := strings.Repeat("x", 400)
	for _, other := range []bool{false, true} {
		t.Run(fmt.Sprintf("otherCollection=%v", other), func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "c")
			require.NoError(t, err)
			target := coll
			if other {
				target, err = fx.CreateCollection(ctx, "d")
				require.NoError(t, err)
			}
			for i := 0; i < 300; i++ {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, 2*i))))
			}
			tx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			iter, err := coll.Find(nil).Iter(tx.Context())
			require.NoError(t, err)
			for read := 0; read < 100; read++ {
				require.True(t, iter.Next())
			}
			doc, err := iter.Doc()
			require.NoError(t, err)
			require.Equal(t, 198, doc.Value().GetInt("id"))

			sp, err := fx.WriteTx(tx.Context())
			require.NoError(t, err)
			for i := 0; i < 300; i++ {
				require.NoError(t, target.Insert(sp.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%s"}`, 2*i+1, pad))))
			}
			require.NoError(t, sp.Rollback())

			require.False(t, iter.(*planIterator).tripped.Load(), "an iterator positioned before the savepoint was tripped")
			var got []int
			for iter.Next() {
				doc, err := iter.Doc()
				require.NoError(t, err)
				got = append(got, doc.Value().GetInt("id"))
			}
			require.NoError(t, iter.Err())
			var want []int
			for i := 100; i < 300; i++ {
				want = append(want, 2*i)
			}
			assert.Equal(t, want, got)
			require.NoError(t, iter.Close())
			require.NoError(t, tx.Commit())
			require.NoError(t, fx.IntegrityCheck(ctx))
		})
	}
}

// A failed verb rolls its own savepoint back; an iterator open in the
// enclosing scope stood on nothing of it and continues.
func TestSavepoint_FailedVerbKeepsOuterIterator(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"u"}, Unique: true}))
	for i := 0; i < 100; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"u":%d}`, i, i))))
	}
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	iter, err := coll.Find(nil).Iter(tx.Context())
	require.NoError(t, err)
	for read := 0; read < 50; read++ {
		require.True(t, iter.Next())
	}
	err = coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1000,"u":7}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
	_, err = coll.UpdateId(tx.Context(), 3, query.MustParseModifier(`{"$set":{"u":8}}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	require.False(t, iter.(*planIterator).tripped.Load())
	n := 50
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		require.Equal(t, n, doc.Value().GetInt("id"))
		n++
	}
	require.NoError(t, iter.Err())
	assert.Equal(t, 100, n)
	require.NoError(t, iter.Close())
	require.NoError(t, tx.Commit())
}

// Nested savepoints: an iterator that moved inside an inner savepoint is
// tripped when the enclosing one is rolled back after the inner's release,
// not when a later sibling is; one positioned before an inner savepoint
// survives the inner's rollback.
func TestSavepoint_RollbackTripsNestedPositions(t *testing.T) {
	pad := strings.Repeat("x", 400)
	fill := func(t *testing.T, coll Collection, sp WriteTx, from, to int) {
		for i := from; i < to; i++ {
			require.NoError(t, coll.Insert(sp.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%s"}`, i, pad))))
		}
	}
	parked := func(t *testing.T, coll Collection, tx WriteTx, n int) Iterator {
		iter, err := coll.Find(nil).Iter(tx.Context())
		require.NoError(t, err)
		for read := 0; read < n; read++ {
			require.True(t, iter.Next())
		}
		_, err = iter.Doc()
		require.NoError(t, err)
		return iter
	}

	t.Run("innerReleasedOuterRolledBack", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		outer, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		inner, err := fx.WriteTx(outer.Context())
		require.NoError(t, err)
		fill(t, coll, inner, 0, 1000)
		iter := parked(t, coll, inner, 600)
		require.NoError(t, inner.Commit())
		require.NoError(t, outer.Rollback())
		require.True(t, iter.(*planIterator).tripped.Load())
		assert.False(t, iter.Next())
		assert.ErrorIs(t, iter.Err(), ErrIterRolledBack)
		require.NoError(t, iter.Close())
		require.NoError(t, tx.Commit())
		n, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
		require.NoError(t, fx.IntegrityCheck(ctx))
	})

	t.Run("siblingRolledBack", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		first, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		fill(t, coll, first, 0, 1000)
		iter := parked(t, coll, first, 600)
		require.NoError(t, first.Commit())
		second, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		fill(t, coll, second, 1000, 1300)
		require.NoError(t, second.Rollback())
		require.False(t, iter.(*planIterator).tripped.Load())
		n := 600
		for iter.Next() {
			doc, err := iter.Doc()
			require.NoError(t, err)
			require.Equal(t, n, doc.Value().GetInt("id"))
			n++
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, 1000, n)
		require.NoError(t, iter.Close())
		require.NoError(t, tx.Commit())
		n, err = coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1000, n)
		require.NoError(t, fx.IntegrityCheck(ctx))
	})

	t.Run("positionedBeforeInner", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		outer, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		fill(t, coll, outer, 0, 1000)
		iter := parked(t, coll, outer, 600)
		inner, err := fx.WriteTx(outer.Context())
		require.NoError(t, err)
		fill(t, coll, inner, 1000, 1300)
		require.NoError(t, inner.Rollback())
		require.False(t, iter.(*planIterator).tripped.Load())
		n := 600
		for iter.Next() {
			doc, err := iter.Doc()
			require.NoError(t, err)
			require.Equal(t, n, doc.Value().GetInt("id"))
			n++
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, 1000, n)
		require.NoError(t, iter.Close())
		require.NoError(t, outer.Rollback())
		require.NoError(t, tx.Commit())
		n, err = coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
		require.NoError(t, fx.IntegrityCheck(ctx))
	})
}

// A failed write rolls back its own savepoint; an iterator its modifier
// moved while it ran stands on pages of that scope and is tripped with it.
// Without the trip, the write's growth of the documents — incompressible
// padding, so that it takes pages — moves the iterator onto pages the
// rollback discards, and it yields rolled-back documents.
func TestSavepoint_FailedVerbTripsIteratorMovedByModifier(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"u"}, Unique: true}))
	for i := 0; i < 300; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"u":%d}`, i, i))))
	}
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	outer, err := coll.Find(nil).Iter(tx.Context())
	require.NoError(t, err)
	require.True(t, outer.Next())

	rng := rand.New(rand.NewSource(1))
	pad := make([]byte, 200)
	moved := 0
	grow := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		if outer.Next() {
			moved++
		}
		rng.Read(pad)
		v.Set("pad", a.NewString(fmt.Sprintf("%x", pad)))
		if v.GetInt("id") == 150 {
			v.Set("u", a.NewNumberInt(299))
		}
		return v, true, nil
	})
	_, err = coll.Find(`{"id":{"$lt":200}}`).Update(tx.Context(), grow)
	require.ErrorIs(t, err, ErrUniqueConstraint)
	require.Greater(t, moved, 100, "the modifier did not move the outer iterator")

	require.True(t, outer.(*planIterator).tripped.Load(), "the failed write did not trip the iterator its modifier moved")
	assert.False(t, outer.Next())
	assert.ErrorIs(t, outer.Err(), ErrIterRolledBack)
	_, err = outer.Doc()
	assert.ErrorIs(t, err, ErrIterRolledBack)
	require.NoError(t, outer.Close())

	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1000,"u":1000}`)))
	require.NoError(t, tx.Commit())
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 301, n)
	n, err = coll.Find(`{"pad":{"$exists":true}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n, "the failed write's changes survived")
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// An aggregation with a blocking stage drains its inner iterator at the
// first Next and serves rows from memory: tripped with the inner iterator
// all the same, as what its rows and lookups came from is gone. The sort is
// on a computed field, which the planner cannot take over: the stage is the
// pipeline's own.
func TestSavepoint_RollbackTripsAggregationIterator(t *testing.T) {
	pad := strings.Repeat("x", 400)
	for _, openInside := range []bool{false, true} {
		t.Run(fmt.Sprintf("openedInside=%v", openInside), func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "c")
			require.NoError(t, err)
			for i := 0; i < 10; i++ {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":"base"}`, i))))
			}
			tx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			var iter Iterator
			if !openInside {
				iter, err = coll.Aggregate(`[{"$addFields":{"k":"$id"}},{"$sort":{"k":-1}}]`).Iter(tx.Context())
				require.NoError(t, err)
			}
			sp, err := fx.WriteTx(tx.Context())
			require.NoError(t, err)
			for i := 1000; i < 1600; i++ {
				require.NoError(t, coll.Insert(sp.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":"sp","pad":"%s"}`, i, pad))))
			}
			if openInside {
				iter, err = coll.Aggregate(`[{"$addFields":{"k":"$id"}},{"$sort":{"k":-1}}]`).Iter(sp.Context())
				require.NoError(t, err)
			}
			// The first Next drains the inner iterator, inside the savepoint.
			require.True(t, iter.Next())
			doc, err := iter.Doc()
			require.NoError(t, err)
			require.Equal(t, 1599, doc.Value().GetInt("id"))
			require.NoError(t, sp.Rollback())

			assert.False(t, iter.Next())
			assert.ErrorIs(t, iter.Err(), ErrIterRolledBack)
			_, err = iter.Doc()
			assert.ErrorIs(t, err, ErrIterRolledBack)
			require.NoError(t, iter.Close())
			require.NoError(t, tx.Commit())
			n, err := coll.Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 10, n)
		})
	}
}

// What counts as a move inside the savepoint: a Next of an iterator opened
// before it, the opening itself with nothing done since, and a Doc that
// reads the store — the fallback cursor of a sorted query seeks inside the
// savepoint — each on its own. After the trip, Doc on the fallback path
// reports the rollback and the fallback cursor is gone.
func TestSavepoint_RollbackTripsEachKindOfMove(t *testing.T) {
	pad := strings.Repeat("x", 400)
	setup := func(t *testing.T) (*fixture, Collection, WriteTx) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		for i := 0; i < 300; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"pad":"%s"}`, i, i, pad))))
		}
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		return fx, coll, tx
	}
	tripped := func(t *testing.T, iter Iterator) {
		require.True(t, iter.(*planIterator).tripped.Load(), "the rollback did not trip the iterator")
		// Doc first: the trip is reported before Next records it.
		_, err := iter.Doc()
		assert.ErrorIs(t, err, ErrIterRolledBack)
		assert.False(t, iter.Next())
		assert.ErrorIs(t, iter.Err(), ErrIterRolledBack)
		_, err = iter.Doc()
		assert.ErrorIs(t, err, ErrIterRolledBack)
		require.NoError(t, iter.Close())
	}

	t.Run("nextInside", func(t *testing.T) {
		fx, coll, tx := setup(t)
		iter, err := coll.Find(nil).Iter(tx.Context())
		require.NoError(t, err)
		require.True(t, iter.Next())
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		for i := 1000; i < 1300; i++ {
			require.NoError(t, coll.Insert(sp.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%s"}`, i, pad))))
		}
		require.True(t, iter.Next())
		require.NoError(t, sp.Rollback())
		tripped(t, iter)
		require.NoError(t, tx.Commit())
	})

	t.Run("openedInsideNothingWritten", func(t *testing.T) {
		fx, coll, tx := setup(t)
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		// Opened, never moved: the opening is the move.
		iter, err := coll.Find(nil).Iter(sp.Context())
		require.NoError(t, err)
		require.NoError(t, sp.Rollback())
		tripped(t, iter)
		require.NoError(t, tx.Commit())
	})

	t.Run("docInside", func(t *testing.T) {
		fx, coll, tx := setup(t)
		// Sorted on a field without an index: the sort buffers the ids
		// and Doc re-fetches the document through the fallback cursor.
		iter, err := coll.Find(nil).Sort("-a").Iter(tx.Context())
		require.NoError(t, err)
		require.True(t, iter.Next())
		sp, err := fx.WriteTx(tx.Context())
		require.NoError(t, err)
		for i := 1000; i < 1300; i++ {
			require.NoError(t, coll.Insert(sp.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%s"}`, i, pad))))
		}
		doc, err := iter.Doc()
		require.NoError(t, err)
		require.Equal(t, 299, doc.Value().GetInt("id"))
		pi := iter.(*planIterator)
		require.NotNil(t, pi.dataCursor, "the sorted query did not use the fallback cursor")
		require.NoError(t, sp.Rollback())
		require.Nil(t, pi.dataCursor, "the fallback cursor outlived the savepoint")
		tripped(t, iter)
		require.NoError(t, tx.Commit())
	})
}

// A write to the scanned collection through the same transaction saves the
// scan's cursors and their next move restores them (btree.Cursor): the scan
// goes on with every document, the ones inserted ahead of it included. The
// data tree here is one root leaf that the inserts split in place.
func TestIterator_WriteWhileIterating_RootLeafSplit(t *testing.T) {
	pad := strings.Repeat("x", 316)
	for _, extra := range []int{30, 108} {
		t.Run(fmt.Sprintf("extra=%d", extra), func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "c")
			require.NoError(t, err)
			doc := func(id int) *anyenc.Value {
				return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%s"}`, id, pad))
			}
			for i := 0; i < 46; i++ {
				require.NoError(t, coll.Insert(ctx, doc(i)))
			}
			tx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			next := 1000
			ins := func(n int) {
				for i := 0; i < n; i++ {
					require.NoError(t, coll.Insert(tx.Context(), doc(next)))
					next++
				}
			}
			ins(3)
			iter, err := coll.Find(nil).Iter(tx.Context())
			require.NoError(t, err)
			ins(18)
			require.True(t, iter.Next())
			d, err := iter.Doc()
			require.NoError(t, err)
			require.Equal(t, 0, d.Value().GetInt("id"))
			ins(extra)
			got := []int{0}
			for iter.Next() {
				d, err := iter.Doc()
				require.NoError(t, err)
				got = append(got, d.Value().GetInt("id"))
			}
			require.NoError(t, iter.Err())
			require.NoError(t, iter.Close())
			var want []int
			for i := 0; i < 46; i++ {
				want = append(want, i)
			}
			for i := 1000; i < next; i++ {
				want = append(want, i)
			}
			assert.Equal(t, want, got)
			require.NoError(t, tx.Commit())
			require.NoError(t, fx.IntegrityCheck(ctx))
		})
	}
}

// Deletes under a scan of the same transaction — a full scan, an index
// range, a reverse pk scan, under a small page cache and incompressible
// documents of varied size: the scan ends without an error, yields no
// deleted document and no document twice, and the store stays sound.
func TestIterator_WriteWhileIterating_DeletesUnderScan(t *testing.T) {
	seeds := 60
	if testing.Short() {
		seeds = 10
	}
	for seed := 0; seed < seeds; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(seed)))
			fx := newFixture(t, &Config{DisableCompression: true, CacheSize: 8 + 6*rng.Intn(3)})
			coll, err := fx.CreateCollection(ctx, "c")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"k"}}))
			n := 100 + rng.Intn(300)
			padLen := 10 + rng.Intn(9000)
			pad := make([]byte, padLen)
			kOf := make([]int, n)
			for i := 0; i < n; i++ {
				rng.Read(pad)
				kOf[i] = rng.Intn(50)
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d,"pad":"%x"}`, i, kOf[i], pad))))
			}
			tx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			var q Query
			kind := rng.Intn(3)
			switch kind {
			case 0:
				q = coll.Find(nil)
			case 1:
				q = coll.Find(`{"k":{"$gte":10}}`).Sort("k")
			default:
				q = coll.Find(nil).Sort("-id")
			}
			iter, err := q.Iter(tx.Context())
			require.NoError(t, err)
			seen := map[int]int{}
			last := -1
			for i := 0; i < n/4 && iter.Next(); i++ {
				d, err := iter.Doc()
				require.NoError(t, err)
				last = d.Value().GetInt("id")
				seen[last]++
			}
			require.NoError(t, iter.Err())
			deleted := map[int]bool{}
			for id := 0; id < n; id++ {
				if rng.Intn(3) != 0 {
					require.NoError(t, coll.DeleteId(tx.Context(), id))
					deleted[id] = true
				}
			}
			// The rest of the scan: every surviving document after the position
			// in the scan's order, each once.
			after := func(id int) bool {
				switch kind {
				case 0:
					return id > last
				case 1:
					if kOf[id] < 10 {
						return false
					}
					return kOf[id] > kOf[last] || (kOf[id] == kOf[last] && id > last)
				default:
					return id < last
				}
			}
			want := map[int]int{}
			for id := 0; id < n; id++ {
				if !deleted[id] && after(id) {
					want[id] = 1
				}
			}
			got := map[int]int{}
			for iter.Next() {
				d, err := iter.Doc()
				require.NoError(t, err)
				id := d.Value().GetInt("id")
				require.False(t, deleted[id], "deleted document %d yielded", id)
				seen[id]++
				got[id]++
			}
			require.NoError(t, iter.Err())
			require.NoError(t, iter.Close())
			require.Equal(t, want, got)
			for id, c := range seen {
				require.Equalf(t, 1, c, "document %d yielded %d times", id, c)
			}
			require.NoError(t, tx.Commit())
			require.NoError(t, fx.IntegrityCheck(ctx))
			cnt, err := coll.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, n-len(deleted), cnt)
		})
	}
}

// Doc of a sorted scan on an unindexed field reads the document through
// the iterator's own data cursor; inserts that split the data tree under
// that cursor leave every later Doc correct.
func TestIterator_WriteWhileIterating_DocFallbackCursor(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 300; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"u":%d}`, i, (i*7)%300))))
	}
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	iter, err := coll.Find(nil).Sort("u").Iter(tx.Context())
	require.NoError(t, err)
	var got []int
	read := func() {
		d, err := iter.Doc()
		require.NoError(t, err)
		require.Equal(t, len(got), d.Value().GetInt("u"))
		got = append(got, d.Value().GetInt("id"))
	}
	for i := 0; i < 100; i++ {
		require.True(t, iter.Next())
		read()
	}
	pad := strings.Repeat("y", 500)
	for i := 1000; i < 1600; i++ {
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"u":%d,"pad":"%s"}`, i, i, pad))))
	}
	// The current document again, through the saved data cursor.
	d, err := iter.Doc()
	require.NoError(t, err)
	require.Equal(t, got[99], d.Value().GetInt("id"))
	for iter.Next() {
		read()
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	require.Len(t, got, 300)
	require.NoError(t, tx.Commit())
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// The document under the scan deleted — at the first position, mid-scan,
// and as the last one — under a full scan, a reverse scan and an index
// scan in both directions: the scan goes on with the neighbour, every
// document is yielded once, and nothing is yielded after the last.
func TestIterator_WriteWhileIterating_DeleteCurrent(t *testing.T) {
	const n = 300
	kinds := map[string]func(coll Collection) Query{
		"full":         func(coll Collection) Query { return coll.Find(nil) },
		"reverse":      func(coll Collection) Query { return coll.Find(nil).Sort("-id") },
		"index":        func(coll Collection) Query { return coll.Find(nil).Sort("k") },
		"indexReverse": func(coll Collection) Query { return coll.Find(nil).Sort("-k") },
	}
	for name, q := range kinds {
		for _, at := range []int{1, 100, n} {
			t.Run(fmt.Sprintf("%s/at=%d", name, at), func(t *testing.T) {
				fx := newFixture(t)
				coll, err := fx.CreateCollection(ctx, "c")
				require.NoError(t, err)
				require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"k"}}))
				for i := 0; i < n; i++ {
					require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d}`, i, i))))
				}
				tx, err := fx.WriteTx(ctx)
				require.NoError(t, err)
				iter, err := q(coll).Iter(tx.Context())
				require.NoError(t, err)
				var got []int
				for i := 0; i < at; i++ {
					require.True(t, iter.Next())
					d, err := iter.Doc()
					require.NoError(t, err)
					got = append(got, d.Value().GetInt("id"))
				}
				require.NoError(t, coll.DeleteId(tx.Context(), got[len(got)-1]))
				for iter.Next() {
					d, err := iter.Doc()
					require.NoError(t, err)
					got = append(got, d.Value().GetInt("id"))
				}
				require.NoError(t, iter.Err())
				require.NoError(t, iter.Close())
				want := make([]int, n)
				for i := range want {
					want[i] = i
					if strings.Contains(name, "everse") {
						want[i] = n - 1 - i
					}
				}
				require.Equal(t, want, got)
				require.NoError(t, tx.Commit())
				require.NoError(t, fx.IntegrityCheck(ctx))
			})
		}
	}
}

// A plan that collects its result before yielding it — a sort the planner
// runs in memory — yields what it collected: a document deleted since
// makes its Doc fail with ErrDocNotFound and the scan goes on, one
// inserted since is not visited.
func TestIterator_WriteWhileIterating_CollectedPlan(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"u":%d}`, i, (i*7)%100))))
	}
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	iter, err := coll.Find(nil).Sort("u").Iter(tx.Context())
	require.NoError(t, err)
	require.True(t, iter.Next())
	d, err := iter.Doc()
	require.NoError(t, err)
	require.Equal(t, 0, d.Value().GetInt("u"))
	require.NoError(t, coll.DeleteId(tx.Context(), 43)) // u = 1
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1000,"u":2}`)))
	require.True(t, iter.Next())
	_, err = iter.Doc()
	require.ErrorIs(t, err, ErrDocNotFound)
	var us []int
	for iter.Next() {
		d, err := iter.Doc()
		require.NoError(t, err)
		require.NotEqual(t, 1000, d.Value().GetInt("id"))
		us = append(us, d.Value().GetInt("u"))
	}
	require.NoError(t, iter.Err())
	require.Len(t, us, 98)
	require.Equal(t, 2, us[0])
	require.NoError(t, iter.Close())
	require.NoError(t, tx.Commit())
}

// A Close() of the handle while its modifier runs waits for the transaction
// to end, as one made while the handle changes the schema does. Evicted at
// once, the handle would be replaced by an open by name with a second one
// that counts no modifier, and a write through it — a verb, a sink — would
// pass the gate and leave the index out of step with the document.
func TestWriteTx_ModifierPinsItsHandle(t *testing.T) {
	setup := func(t *testing.T) (*fixture, Collection, Collection, WriteTx) {
		fx := newFixture(t)
		x, err := fx.CreateCollection(ctx, "x")
		require.NoError(t, err)
		require.NoError(t, x.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, x.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
		y, err := fx.CreateCollection(ctx, "y")
		require.NoError(t, err)
		require.NoError(t, y.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":5}`)))
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		return fx, x, y, tx
	}
	// The document and its index agree after the commit.
	agree := func(t *testing.T, x Collection) {
		assert.Equal(t, expectJson(t, `{"id":1,"a":1,"m":1}`), collRows(t, x))
		byIndex := []IndexHint{{IndexName: "a", Boost: 1 << 30}}
		assertQueryCount(t, x.Find(`{"a":1}`).IndexHint(byIndex...), 1)
		assertQueryCount(t, x.Find(`{"a":5}`).IndexHint(byIndex...), 0)
	}

	t.Run("closed inside the modifier", func(t *testing.T) {
		fx, x, y, tx := setup(t)
		_, err := x.UpdateId(tx.Context(), 1, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			if err := x.Close(); err != nil {
				return nil, false, err
			}
			_, err := y.Aggregate(`[{"$out":"x"}]`).Count(tx.Context())
			assert.ErrorIs(t, err, ErrWriteInModifier)
			_, err = y.Aggregate(`[{"$merge":{"into":"x","whenMatched":"replace"}}]`).Count(tx.Context())
			assert.ErrorIs(t, err, ErrWriteInModifier)
			v.Set("m", a.NewNumberInt(1))
			return v, true, nil
		}))
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		// The close took effect with the commit.
		_, err = x.FindId(ctx, 1)
		assert.ErrorIs(t, err, ErrCollectionClosed)
		x, err = fx.OpenCollection(ctx, "x")
		require.NoError(t, err)
		agree(t, x)
	})

	t.Run("closed and opened again inside the modifier", func(t *testing.T) {
		fx, x, y, tx := setup(t)
		_, err := x.UpdateId(tx.Context(), 1, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			if err := x.Close(); err != nil {
				return nil, false, err
			}
			// The registry still holds the handle: the open hands it out
			// again, and the writes through it are refused.
			again, err := fx.OpenCollection(tx.Context(), "x")
			if err != nil {
				return nil, false, err
			}
			assert.ErrorIs(t, again.UpsertOne(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":5}`)), ErrWriteInModifier)
			assert.ErrorIs(t, again.DeleteId(tx.Context(), 1), ErrWriteInModifier)
			assert.ErrorIs(t, again.DropIndex(tx.Context(), "a"), ErrWriteInModifier)
			assert.ErrorIs(t, again.Drop(tx.Context()), ErrWriteInModifier)
			_, err = y.Aggregate(`[{"$out":"x"}]`).Count(tx.Context())
			assert.ErrorIs(t, err, ErrWriteInModifier)
			v.Set("m", a.NewNumberInt(1))
			return v, true, nil
		}))
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		// Opened again before the transaction ended: the handle stays open.
		agree(t, x)
	})

	// A bulk verb pins once; the pin outlives the rollback of a nested
	// write's savepoint between two of its calls.
	t.Run("bulk modifier", func(t *testing.T) {
		fx, x, y, tx := setup(t)
		require.NoError(t, x.Insert(tx.Context(),
			anyenc.MustParseJson(`{"id":2,"a":2}`),
			anyenc.MustParseJson(`{"id":3,"a":3}`),
		))
		call := 0
		res, err := x.Find(nil).Sort("id").Update(tx.Context(), query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			call++
			switch call {
			case 1:
				if err := x.Close(); err != nil {
					return nil, false, err
				}
			case 2:
				// A nested write that fails rolls back its own savepoint.
				assert.ErrorIs(t, y.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1}`)), ErrDocExists)
			case 3:
				again, err := fx.OpenCollection(tx.Context(), "x")
				if err != nil {
					return nil, false, err
				}
				assert.ErrorIs(t, again.UpsertOne(tx.Context(), anyenc.MustParseJson(`{"id":3,"a":5}`)), ErrWriteInModifier)
				_, err = y.Aggregate(`[{"$out":"x"}]`).Count(tx.Context())
				assert.ErrorIs(t, err, ErrWriteInModifier)
			}
			v.Set("m", a.NewNumberInt(1))
			return v, true, nil
		}))
		require.NoError(t, err)
		assert.Equal(t, ModifyResult{Matched: 3, Modified: 3}, res)
		require.NoError(t, tx.Commit())
		assert.Equal(t, expectJson(t,
			`{"id":1,"a":1,"m":1}`,
			`{"id":2,"a":2,"m":1}`,
			`{"id":3,"a":3,"m":1}`,
		), collRows(t, x))
		byIndex := []IndexHint{{IndexName: "a", Boost: 1 << 30}}
		assertQueryCount(t, x.Find(`{"a":3}`).IndexHint(byIndex...), 1)
		assertQueryCount(t, x.Find(`{"a":5}`).IndexHint(byIndex...), 0)
	})
}
