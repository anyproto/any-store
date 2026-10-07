package anystore

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// blockingFilter holds the turn of the call evaluating it: Ok blocks until
// release is closed. entered is closed as Ok first runs.
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

// The calls several goroutines make on one transaction run one at a time
// (commonTx.opMu). Without the lock every subtest races in the btree page
// cache the write tx owns — built for one caller — and ends in a nil
// dereference or in allocation without bound. Run with -race.
func TestWriteTx_SharedAcrossGoroutines(t *testing.T) {
	const goroutines = 4

	// Collects one error per goroutine and fails on the first.
	run := func(t *testing.T, body func(g int) error) {
		t.Helper()
		var wg sync.WaitGroup
		errs := make(chan error, goroutines)
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				errs <- body(g)
			}(g)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
	}

	t.Run("reads", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		for i := 0; i < 2000; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%100))))
		}
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		run(t, func(int) error {
			for j := 0; j < 20; j++ {
				n, err := coll.Find(`{"a":{"$gt":5}}`).Count(tx.Context())
				if err != nil {
					return err
				}
				if n != 2000-6*20 {
					return fmt.Errorf("count %d", n)
				}
			}
			return nil
		})
		require.NoError(t, tx.Commit())
	})

	t.Run("reads and writes", func(t *testing.T) {
		fx := newFixture(t)
		w, err := fx.CreateCollection(ctx, "w")
		require.NoError(t, err)
		require.NoError(t, w.EnsureIndex(ctx, IndexInfo{Fields: []string{"g"}}))
		// Read only: an iterator stays open across the others' writes, which
		// the contract allows on a collection nobody writes.
		r, err := fx.CreateCollection(ctx, "r")
		require.NoError(t, err)
		for i := 0; i < 100; i++ {
			require.NoError(t, r.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))))
		}
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		const perGoroutine = 200
		run(t, func(g int) error {
			tctx := tx.Context()
			for j := 0; j < perGoroutine; j++ {
				id := g*perGoroutine + j
				if err := w.Insert(tctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"g":%d,"j":%d}`, id, g, j))); err != nil {
					return err
				}
				if _, err := w.UpdateId(tctx, id, query.MustParseModifier(`{"$inc":{"j":1}}`)); err != nil {
					return err
				}
				if j%5 == 0 {
					if err := w.DeleteId(tctx, id); err != nil {
						return err
					}
					if _, err := w.FindId(tctx, id); !errors.Is(err, ErrDocNotFound) {
						return fmt.Errorf("deleted %d: %v", id, err)
					}
				} else if doc, err := w.FindId(tctx, id); err != nil || doc.Value().GetInt("j") != j+1 {
					return fmt.Errorf("doc %d: %v", id, err)
				}
				if j%20 != 0 {
					continue
				}
				// The goroutine's own documents through the index.
				n, err := w.Find(fmt.Sprintf(`{"g":%d}`, g)).Count(tctx)
				if err != nil {
					return err
				}
				if want := j - j/5; n != want {
					return fmt.Errorf("g %d: count %d, want %d", g, n, want)
				}
				iter, err := r.Find(`{"id":{"$gte":50}}`).Iter(tctx)
				if err != nil {
					return err
				}
				var seen int
				for iter.Next() {
					if _, err := iter.Doc(); err != nil {
						_ = iter.Close()
						return err
					}
					seen++
				}
				if err := errors.Join(iter.Err(), iter.Close()); err != nil {
					return err
				}
				if seen != 50 {
					return fmt.Errorf("r: %d docs", seen)
				}
			}
			return nil
		})
		want := goroutines * (perGoroutine - perGoroutine/5)
		assertCollCountInTx(tx.Context(), t, w, want)
		require.NoError(t, tx.Commit())
		assertCollCount(t, w, want)
	})

	t.Run("full-text reads flush the buffered postings", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"text"}, Kind: IndexKindFulltext}))
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		for i := 0; i < 20; i++ {
			require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"text":"alpha beta %d"}`, i, i))))
		}
		run(t, func(int) error {
			for j := 0; j < 50; j++ {
				n, err := coll.Find(`{"$text":{"$search":"alpha"}}`).Count(tx.Context())
				if err != nil {
					return err
				}
				if n != 20 {
					return fmt.Errorf("count %d", n)
				}
			}
			return nil
		})
		require.NoError(t, tx.Commit())
		assertQueryCount(t, coll.Find(`{"$text":{"$search":"beta"}}`), 20)
	})

	t.Run("savepoints of their own", func(t *testing.T) {
		// Each goroutine opens, writes in and ends savepoints of its own.
		// They nest in the order they are opened, so one goroutine's end
		// may orphan another's — ErrTxIsUsed, as the same interleaving
		// from one goroutine — and what the transaction counts is what
		// it commits.
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		run(t, func(g int) error {
			for j := 0; j < 30; j++ {
				sp, err := fx.WriteTx(tx.Context())
				if err != nil {
					return err
				}
				if err = coll.Insert(sp.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, g*1000+j))); err != nil {
					return err
				}
				if j%2 == 0 {
					err = sp.Commit()
				} else {
					err = sp.Rollback()
				}
				if err != nil && !errors.Is(err, ErrTxIsUsed) {
					return err
				}
				if !sp.Done() {
					return errors.New("savepoint not done")
				}
			}
			return nil
		})
		n, err := coll.Count(tx.Context())
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		assertCollCount(t, coll, n)
		require.NoError(t, fx.IntegrityCheck(ctx))
	})

	t.Run("savepoints beside reads", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		// Savepoints nest: two goroutines interleaving theirs would orphan
		// one, as the same interleaving from one goroutine does. Each
		// savepoint runs whole under a lock of the test's own, beside the
		// other goroutines' reads.
		var spMu sync.Mutex
		var kept int
		run(t, func(g int) error {
			for j := 0; j < 20; j++ {
				if g%2 == 1 {
					if _, err := coll.Count(tx.Context()); err != nil {
						return err
					}
					continue
				}
				spMu.Lock()
				err := func() error {
					sp, err := fx.WriteTx(tx.Context())
					if err != nil {
						return err
					}
					if sp.Done() {
						return errors.New("savepoint done at once")
					}
					if err = coll.Insert(sp.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, g*100+j))); err != nil {
						return err
					}
					if j%2 == 0 {
						kept++
						err = sp.Commit()
					} else {
						err = sp.Rollback()
					}
					if err != nil {
						return err
					}
					if !sp.Done() {
						return errors.New("savepoint not done")
					}
					return nil
				}()
				spMu.Unlock()
				if err != nil {
					return err
				}
			}
			return nil
		})
		assertCollCountInTx(tx.Context(), t, coll, kept)
		require.NoError(t, tx.Commit())
		assertCollCount(t, coll, kept)
	})
}

func TestReadTx_SharedAcrossGoroutines(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := 0; i < 500; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))))
	}
	tx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			errs <- func() error {
				for j := 0; j < 30; j++ {
					n, err := coll.Find(`{"a":{"$lt":5}}`).Count(tx.Context())
					if err != nil {
						return err
					}
					if n != 250 {
						return fmt.Errorf("count %d", n)
					}
					if _, err = coll.FindId(tx.Context(), g*100+j); err != nil {
						return err
					}
					iter, err := coll.Find(`{"a":1}`).Iter(tx.Context())
					if err != nil {
						return err
					}
					var seen int
					for iter.Next() {
						if _, err := iter.Doc(); err != nil {
							_ = iter.Close()
							return err
						}
						seen++
					}
					if err := errors.Join(iter.Err(), iter.Close()); err != nil {
						return err
					}
					if seen != 50 {
						return fmt.Errorf("iter: %d docs", seen)
					}
				}
				return nil
			}()
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

// A call on a transaction another goroutine has ended finds it ended —
// under the lock, so after the call it may have waited for — and does not
// touch the state the transaction gave back.
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

	t.Run("waiting at the commit", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2}`)))

		// A call holds the turn inside a filter. The commit queues for it
		// first and the count after: the count passes every check made
		// before the lock while the transaction is live, and finds it
		// ended under the lock (the mutex hands over in order once the
		// waits pass a millisecond).
		f := newBlockingFilter()
		held := make(chan error, 1)
		go func() {
			_, err := coll.Find(f).Count(tx.Context())
			held <- err
		}()
		<-f.entered
		committed := make(chan error, 1)
		go func() { committed <- tx.Commit() }()
		time.Sleep(20 * time.Millisecond)
		counted := make(chan error, 1)
		go func() {
			_, err := coll.Count(tx.Context())
			counted <- err
		}()
		time.Sleep(20 * time.Millisecond)
		close(f.release)
		require.NoError(t, <-held)
		require.NoError(t, <-committed)
		assert.ErrorIs(t, <-counted, ErrTxIsUsed)
		assertCollCount(t, coll, 2)
	})

	t.Run("write waiting at the commit", func(t *testing.T) {
		// The write scope's re-check (enterWriteTx), the same way.
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
		committed := make(chan error, 1)
		go func() { committed <- tx.Commit() }()
		time.Sleep(20 * time.Millisecond)
		inserted := make(chan error, 1)
		go func() { inserted <- coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":3}`)) }()
		time.Sleep(20 * time.Millisecond)
		close(f.release)
		require.NoError(t, <-held)
		require.NoError(t, <-committed)
		assert.ErrorIs(t, <-inserted, ErrTxIsUsed)
		assertCollCount(t, coll, 2)
	})
}

// A handle of a transaction that ended fails with ErrTxIsUsed and touches
// nothing of the transaction that reuses the pooled state: its lock and its
// context are the handle's own.
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

	t.Run("call does not wait for the reusing transaction", func(t *testing.T) {
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
		// tx2's turn is held: a call on tx1 waits for nothing.
		counted := make(chan error, 1)
		go func() {
			_, err := coll.Count(tx1.Context())
			counted <- err
		}()
		select {
		case err := <-counted:
			assert.ErrorIs(t, err, ErrTxIsUsed)
		case <-time.After(2 * time.Second):
			t.Fatal("a call on the ended transaction waited for the transaction reusing its state")
		}
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
		// a call through it takes its turn like any other.
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2}`)))
		var wg sync.WaitGroup
		errs := make(chan error, 4)
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for j := 0; j < 200; j++ {
					var err error
					if g%2 == 0 {
						_, err = coll.FindId(sp.Context(), 1)
					} else {
						err = coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, 100+g*1000+j)))
					}
					if err != nil {
						errs <- err
						return
					}
				}
				errs <- nil
			}(g)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())
		assertCollCount(t, coll, 2+400)
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
		updated := make(chan error, 1)
		go func() {
			_, err := coll.UpdateId(sp.Context(), 1, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
				seen = sp.Done()
				return v, false, nil
			}))
			updated <- err
		}()
		select {
		case err := <-updated:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("sp.Done inside a modifier waited for the modifier's own call")
		}
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

// Two goroutines that ensure the same collection in one transaction get
// one handle: the open — the log, the registry, the catalog — and the
// create run in one turn.
func TestWriteTx_OpenAndCreateInOneTurn(t *testing.T) {
	fx := newFixture(t)
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	// Between the first open's log lookup and its catalog read, a second
	// goroutine ensures the same collection: it waits for the turn, and
	// creates the collection — or finds it — once the first open is done.
	// Were the open two turns, the create would land between them and the
	// catalog read would register a second handle.
	var fired atomic.Bool
	created := make(chan error, 1)
	bDone := make(chan struct{})
	var hB Collection
	testHookAfterLogLookup = func(name string) {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		go func() {
			defer close(bDone)
			var err error
			hB, err = fx.Collection(tx.Context(), name)
			created <- err
		}()
		// Done at once were the open two turns; waiting for the turn the
		// open holds otherwise.
		select {
		case <-bDone:
		case <-time.After(300 * time.Millisecond):
		}
	}
	defer func() { testHookAfterLogLookup = nil }()
	hA, err := fx.Collection(tx.Context(), "c")
	require.NoError(t, err)
	require.NoError(t, <-created)
	// One handle: the index made through one is maintained by the other.
	require.NoError(t, hA.EnsureIndex(tx.Context(), IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, hB.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":1}`)))
	require.NoError(t, tx.Commit())
	c, err := fx.OpenCollection(ctx, "c")
	require.NoError(t, err)
	assertQueryCount(t, c.Find(`{"a":1}`), 1)
	require.Len(t, c.GetIndexes(), 1)
	assertIndexLen(t, c.GetIndexes()[0], 1)
}

// The calls an operation makes on the transaction its context carries,
// against the same operations with no transaction: what the per-call turn
// (commonTx.opMu) costs on the in-transaction paths.
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
	require.NoError(t, tx.Commit())
	assert.False(t, idx.sketchModified)
	assert.Same(t, idx.sketch, idx.loadPubSketch())
	assert.EqualValues(t, 2, sketchCounts(t, dbi, "c", "a")[-1])
}
