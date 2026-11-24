package anystore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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
)

// ANYSTORE_TEST_PPROF=<addr> serves net/http/pprof for the run, e.g.
// ANYSTORE_TEST_PPROF=localhost:6066. Off by default: an unconditional
// listener collides between concurrent runs and hides the bind error.
func init() {
	addr := os.Getenv("ANYSTORE_TEST_PPROF")
	if addr == "" {
		return
	}
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			fmt.Fprintf(os.Stderr, "pprof listener on %s: %v\n", addr, err)
		}
	}()
}

var ctx = context.Background()

func TestDb_CreateCollection(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	assert.NotNil(t, coll)

	cNames, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"test"}, cNames)

	_, err = fx.CreateCollection(ctx, "test")
	assert.ErrorIs(t, err, ErrCollectionExists)

	require.NoError(t, coll.Close())

	_, err = fx.CreateCollection(ctx, "test")
	assert.ErrorIs(t, err, ErrCollectionExists)
}

func TestDb_OpenCollection(t *testing.T) {
	t.Run("err not found", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.OpenCollection(ctx, "test")
		require.Nil(t, coll)
		require.ErrorIs(t, err, ErrCollectionNotFound)
	})
	t.Run("success", func(t *testing.T) {
		fx := newFixture(t)
		_, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		coll, err := fx.OpenCollection(ctx, "test")
		require.NoError(t, err)
		assert.NotNil(t, coll)
	})
	t.Run("with indexes", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		indexInfo := IndexInfo{Fields: []string{"a", "-b"}, Sparse: true, Unique: true}
		require.NoError(t, coll.EnsureIndex(ctx, indexInfo))
		require.NoError(t, coll.Close())

		coll, err = fx.OpenCollection(ctx, "test")
		require.NoError(t, err)
		assert.NotNil(t, coll)
		indexes := coll.GetIndexes()
		assert.Len(t, indexes, 1)
	})
}

// A collection whose name, followed by ":", begins another's does not take
// the other's indexes: the catalog keys of collection "A:b"'s indexes,
// "idx:A:b:<index>", begin with "idx:A:", the prefix of collection "A"'s.
func TestGetIndexInfos_NameBeginsAnotherCollection(t *testing.T) {
	fx := newFixture(t)
	a, err := fx.CreateCollection(ctx, "A")
	require.NoError(t, err)
	ab, err := fx.CreateCollection(ctx, "A:b")
	require.NoError(t, err)
	require.NoError(t, ab.EnsureIndex(ctx, IndexInfo{Name: "c", Fields: []string{"x"}}))
	require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(`{"id":1,"x":1}`)))
	require.NoError(t, ab.Insert(ctx, anyenc.MustParseJson(`{"id":1,"x":1}`)))

	d := fx.DB.(*db)
	indexNames := func(coll string) (names []string) {
		t.Helper()
		require.NoError(t, d.doReadTx(ctx, func(tx *btree.ReadTx) error {
			infos, err := d.getIndexInfos(tx, coll)
			for _, info := range infos {
				names = append(names, info.Name)
			}
			return err
		}))
		return names
	}
	assert.Empty(t, indexNames("A"))
	assert.Equal(t, []string{"c"}, indexNames("A:b"))

	// A new handle loads its schema from the catalog.
	require.NoError(t, a.Close())
	require.NoError(t, ab.Close())
	a, err = fx.OpenCollection(ctx, "A")
	require.NoError(t, err)
	assert.Empty(t, a.GetIndexes())
	assertCollCount(t, a, 1)
	ab, err = fx.OpenCollection(ctx, "A:b")
	require.NoError(t, err)
	require.Len(t, ab.GetIndexes(), 1)
	assert.Equal(t, "c", ab.GetIndexes()[0].Info().Name)
	n, err := ab.Find(`{"x":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

// Collection "A" asking for index "b:c" derives the catalog key of
// collection "A:b"'s index "c". The record there is not "A"'s, so neither
// EnsureIndex nor CreateIndex takes it for the index asked for, the same
// definition or not: both fail with ErrNamespaceExists, "A" gets no index,
// and "A:b" keeps its own.
func TestRegisterIndex_KeyOfAnotherCollectionsIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	fx, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, fx.Close()) }()
	a, err := fx.CreateCollection(ctx, "A")
	require.NoError(t, err)
	ab, err := fx.CreateCollection(ctx, "A:b")
	require.NoError(t, err)
	require.NoError(t, ab.EnsureIndex(ctx, IndexInfo{Name: "c", Fields: []string{"x"}, Unique: true}))
	require.NoError(t, ab.Insert(ctx, anyenc.MustParseJson(`{"id":1,"x":1}`)))

	same := IndexInfo{Name: "b:c", Fields: []string{"x"}, Unique: true}
	other := IndexInfo{Name: "b:c", Fields: []string{"y"}}
	check := func(t *testing.T, a, ab Collection) {
		t.Helper()
		for _, info := range []IndexInfo{same, other} {
			err := a.EnsureIndex(ctx, info)
			require.ErrorIs(t, err, ErrNamespaceExists)
			assert.NotErrorIs(t, err, ErrIndexExists)
			assert.Contains(t, err.Error(), `"idx:A:b:c" holds index "c" of another collection`)
			require.ErrorIs(t, a.CreateIndex(ctx, info), ErrNamespaceExists)
		}
		assert.Empty(t, a.GetIndexes(), "A has no index")
		// No unique index on A: equal values of x go in.
		for _, id := range []int{1, 2} {
			require.NoError(t, a.UpsertOne(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"x":1}`, id))))
		}
		assertCollCount(t, a, 2)
		// A:b's index is as it was, and still unique.
		require.Len(t, ab.GetIndexes(), 1)
		assert.Equal(t, "c", ab.GetIndexes()[0].Info().Name)
		require.NoError(t, ab.EnsureIndex(ctx, IndexInfo{Name: "c", Fields: []string{"x"}, Unique: true}))
		require.ErrorIs(t, ab.Insert(ctx, anyenc.MustParseJson(`{"id":2,"x":1}`)), ErrUniqueConstraint)
		n, err := ab.Find(`{"x":1}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		assertCollCount(t, ab, 1)
	}
	check(t, a, ab)

	// The same from the file.
	require.NoError(t, fx.Close())
	fx, err = Open(ctx, path, nil)
	require.NoError(t, err)
	a, err = fx.OpenCollection(ctx, "A")
	require.NoError(t, err)
	ab, err = fx.OpenCollection(ctx, "A:b")
	require.NoError(t, err)
	check(t, a, ab)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

func TestDb_GetCollectionNames(t *testing.T) {
	fx := newFixture(t)
	var collNames = []string{"c1", "c2", "c3"}
	for _, collName := range collNames {
		coll, err := fx.CreateCollection(ctx, collName)
		require.NoError(t, err)
		require.NoError(t, coll.Close())
	}
	names, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, collNames, names)
}

func TestDb_Stats(t *testing.T) {
	skipIfInMemory(t, "Stats checks file sizes on disk")
	fx := newFixture(t)
	stats, err := fx.Stats(ctx)
	require.NoError(t, err)
	assert.Empty(t, 0, stats.IndexesCount)
	assert.Empty(t, 0, stats.CollectionsCount)
	assert.NotEmpty(t, stats.TotalSizeBytes)
	assert.NotEmpty(t, stats.DataSizeBytes)
}

func TestDb_QuickCheck(t *testing.T) {
	fx := newFixture(t)
	assert.NoError(t, fx.QuickCheck(ctx))
}

func TestDb_Flush(t *testing.T) {
	fx := newFixture(t)
	assert.NoError(t, fx.Flush(ctx, 0, FlushModeCheckpointPassive))
	assert.NoError(t, fx.Flush(ctx, 0, FlushModeCheckpointFull))
}

func TestDb_Backup(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.Collection(ctx, "coll")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"doc"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))

	tmpDir, err := os.MkdirTemp("", "any-store-backup-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	require.NoError(t, fx.Backup(ctx, filepath.Join(tmpDir, "any-store-test.db")))

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.Collection(ctx, "coll")
	require.NoError(t, err)
	assert.Len(t, coll2.GetIndexes(), 1)
	assertCollCount(t, coll2, 2)
}

func TestDb_Backup_OnlineDuringWrites(t *testing.T) {
	skipIfInMemory(t, "concurrent-writer test; file backend only")
	fx := newFixture(t)
	coll, err := fx.Collection(ctx, "coll")
	require.NoError(t, err)

	// Seed 500 docs.
	for i := 0; i < 500; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "val":"seed"}`, i))))
	}

	tmpDir, err := os.MkdirTemp("", "any-store-backup-online-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	backupPath := filepath.Join(tmpDir, "any-store-test.db")

	// Start backup and a concurrent writer.
	done := make(chan error, 1)
	go func() { done <- fx.Backup(ctx, backupPath) }()

	for i := 0; i < 50; i++ {
		_ = coll.UpsertOne(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "val":"updated"}`, i)))
	}

	require.NoError(t, <-done)

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.Collection(ctx, "coll")
	require.NoError(t, err)
	assertCollCount(t, coll2, 500)
}

func TestDb_Close(t *testing.T) {
	t.Run("race", func(t *testing.T) {
		fx := newFixture(t, &Config{})

		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)

		var docs []*anyenc.Value
		for i := range 100 {
			docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id": %d, "value": %d}`, i, rand.Int())))
		}
		require.NoError(t, coll.Insert(ctx, docs...))
		var results = make(chan error, 2)
		go func() {
			// writing
			for {
				if pErr := coll.UpsertOne(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id": %d, "value": %d}`, rand.Int(), rand.Int()))); pErr != nil {
					results <- errors.Join(pErr, errors.New("upsertOne"))
					return
				}
			}
		}()

		go func() {
			// tx insert
			tx, tErr := coll.WriteTx(ctx)
			if tErr != nil {
				results <- errors.Join(tErr, errors.New("writeTx"))
				return
			}
			tErr = coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id": "%s", "value": %d}`, anyenc.NewObjectID().Hex(), rand.Int())))
			if tErr != nil {
				results <- errors.Join(tErr, errors.New("insert tx"))
				return
			}
			if tErr = tx.Commit(); tErr != nil {
				results <- errors.Join(tErr, errors.New("insert tx commit"))
				return
			}
		}()

		time.Sleep(time.Second / 2)

		require.NoError(t, fx.Close())

		for range len(results) {
			rErr := <-results
			assert.True(t, errors.Is(rErr, btree.ErrClosed), rErr.Error())
		}
	})

	// closeAsync runs Close on its own goroutine; waitClosed reports whether
	// it returned within d.
	closeAsync := func(fx *fixture) chan error {
		done := make(chan error, 1)
		go func() { done <- fx.DB.Close() }()
		return done
	}
	waitClosed := func(done chan error, d time.Duration) (error, bool) {
		select {
		case err := <-done:
			return err, true
		case <-time.After(d):
			return nil, false
		}
	}
	// closeToReaderWait runs Close on its own goroutine and returns once it
	// stands at its wait for the read transactions: the database is closing
	// and the write side is dealt with.
	closeToReaderWait := func(t *testing.T, fx *fixture) chan error {
		atWait := make(chan struct{})
		btree.SetTestCloseWaitingReadersHook(func() { close(atWait) })
		defer btree.SetTestCloseWaitingReadersHook(nil)
		done := closeAsync(fx)
		select {
		case <-atWait:
		case <-time.After(10 * time.Second):
			t.Fatal("Close did not reach its wait for the readers")
		}
		return done
	}
	// closeToRollback runs Close on its own goroutine and holds it once it
	// has closed the writer gate, before it rolls the write transaction
	// back; release lets it go on.
	closeToRollback := func(t *testing.T, fx *fixture) (done chan error, release func()) {
		atGate, hold := make(chan struct{}), make(chan struct{})
		btree.SetTestWriterGateDrainedHook(func() {
			close(atGate)
			<-hold
		})
		var once sync.Once
		release = func() { once.Do(func() { close(hold) }) }
		t.Cleanup(func() {
			release()
			btree.SetTestWriterGateDrainedHook(nil)
		})
		done = closeAsync(fx)
		select {
		case <-atGate:
		case <-time.After(10 * time.Second):
			t.Fatal("Close did not reach the writer gate")
		}
		return done, release
	}
	reopen := func(t *testing.T, fx *fixture) Collection {
		db, err := Open(ctx, filepath.Join(fx.tmpDir, "any-store-test.db"), nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		c, err := db.Collection(ctx, "test")
		require.NoError(t, err)
		return c
	}
	newColl := func(t *testing.T) (*fixture, Collection) {
		fx := newFixture(t, &Config{})
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1}`), anyenc.MustParseJson(`{"id":2}`)))
		return fx, coll
	}

	// The documented contract: Close waits for every read transaction and
	// iterator to end, with no timeout, and returns once the last has.
	t.Run("waits for an open iterator", func(t *testing.T) {
		for _, advanced := range []bool{true, false} {
			fx, coll := newColl(t)
			it, err := coll.Find(nil).Iter(ctx)
			require.NoError(t, err)
			if advanced {
				require.True(t, it.Next())
			}

			// Close stands at the wait, which the iterator's read
			// transaction holds; the pause only shows it stays there.
			done := closeToReaderWait(t, fx)
			_, closed := waitClosed(done, 50*time.Millisecond)
			require.False(t, closed, "Close returned with an iterator open (advanced=%v)", advanced)

			require.NoError(t, it.Close())
			err, closed = waitClosed(done, 10*time.Second)
			require.True(t, closed, "Close did not return after the iterator closed (advanced=%v)", advanced)
			require.NoError(t, err)
		}
	})

	t.Run("waits for an open read transaction", func(t *testing.T) {
		fx, coll := newColl(t)
		rtx, err := fx.ReadTx(ctx)
		require.NoError(t, err)
		_, err = coll.FindId(rtx.Context(), 1)
		require.NoError(t, err)

		done := closeToReaderWait(t, fx)
		_, closed := waitClosed(done, 50*time.Millisecond)
		require.False(t, closed, "Close returned with a read transaction open")

		// A closing database begins no operation on a collection, on an
		// open transaction either...
		_, err = coll.FindId(rtx.Context(), 2)
		require.ErrorIs(t, err, ErrDBIsClosed)
		// ...while a call on the database itself still reads through the
		// transaction, which is open until its Commit.
		names, err := fx.GetCollectionNames(rtx.Context())
		require.NoError(t, err)
		require.Equal(t, []string{"test"}, names)

		require.NoError(t, rtx.Commit())
		err, closed = waitClosed(done, 10*time.Second)
		require.True(t, closed, "Close did not return after the read transaction ended")
		require.NoError(t, err)
	})

	// An open write transaction does not hold Close back: Close rolls it
	// back. Its writes are gone, and what is done with it afterwards fails
	// with ErrDBIsClosed instead of blocking.
	t.Run("rolls back an open write transaction", func(t *testing.T) {
		// Commit and Rollback each as the first call that ends the
		// transaction after Close: the handle of an ended transaction
		// returns nil from both without reaching the storage.
		for _, end := range []string{"Commit", "Rollback"} {
			fx, coll := newColl(t)
			wtx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":3}`)))

			err, closed := waitClosed(closeAsync(fx), 10*time.Second)
			require.True(t, closed, "Close did not return with a write transaction open")
			require.NoError(t, err)

			require.False(t, wtx.Done())
			require.ErrorIs(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":4}`)), ErrDBIsClosed)
			if end == "Commit" {
				require.ErrorIs(t, wtx.Commit(), ErrDBIsClosed)
			} else {
				require.NoError(t, wtx.Rollback())
			}
			require.True(t, wtx.Done())

			if fx.tmpDir == "" {
				continue // InMemory: nothing to reopen
			}
			n, err := reopen(t, fx).Count(ctx)
			require.NoError(t, err)
			require.Equal(t, 2, n)
		}
	})

	// A savepoint of the write transaction is a WriteTx too. Its Rollback
	// after Close is a storage call like any other, and reports the close;
	// the transaction itself still ends with a nil Rollback.
	t.Run("a savepoint of a rolled-back write transaction", func(t *testing.T) {
		fx, coll := newColl(t)
		wtx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		sp, err := fx.WriteTx(wtx.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(sp.Context(), anyenc.MustParseJson(`{"id":3}`)))
		require.NoError(t, fx.DB.Close())
		require.ErrorIs(t, sp.Rollback(), ErrDBIsClosed)
		require.NoError(t, wtx.Rollback())
	})

	// Once Close has refused one call of the write transaction it has
	// refused its Commit: a statement that failed half-way, whose own
	// rollback Close refused, is never committed by its transaction. The
	// update writes the first document, moving its unique index entry, and
	// Close comes from the modifier as the update turns to the second: the
	// update's next write is refused, and so is the rollback of its
	// savepoint. Close is held between its two steps, the refusal and the
	// rollback, while the caller commits.
	t.Run("a refused write transaction cannot commit", func(t *testing.T) {
		fx := newFixture(t, &Config{})
		if fx.tmpDir == "" {
			t.Skip("needs a reopen")
		}
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"u"}, Unique: true}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"u":"a"}`), anyenc.MustParseJson(`{"id":2,"u":"b"}`)))

		wtx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":3,"u":"c"}`)))

		var (
			calls   int
			done    chan error
			release func()
		)
		mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
			calls++
			if calls == 2 {
				done, release = closeToRollback(t, fx)
			}
			v.Set("u", a.NewString(fmt.Sprintf("x%d", calls)))
			return v, true, nil
		})
		_, err = coll.Find(`{"id":{"$in":[1,2]}}`).Update(wtx.Context(), mod)
		require.ErrorIs(t, err, ErrDBIsClosed)
		require.Equal(t, 2, calls)
		// The caller keeps its earlier work and commits.
		require.ErrorIs(t, wtx.Commit(), ErrDBIsClosed)
		release()
		err, closed := waitClosed(done, 10*time.Second)
		require.True(t, closed)
		require.NoError(t, err)

		c := reopen(t, fx)
		n, err := c.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 2, n, "the refused transaction was committed")
		for id, u := range map[int]string{1: "a", 2: "b"} {
			it, err := c.Find(fmt.Sprintf(`{"u":%q}`, u)).Iter(ctx)
			require.NoError(t, err)
			require.True(t, it.Next(), "document %d is missing from its unique index", id)
			doc, err := it.Doc()
			require.NoError(t, err)
			require.Equal(t, float64(id), doc.Value().GetFloat64("id"))
			require.Equal(t, u, doc.Value().GetString("u"))
			require.NoError(t, it.Close())
		}
		it, err := c.Find(`{"u":"x1"}`).Iter(ctx)
		require.NoError(t, err)
		require.False(t, it.Next(), "the first half of the refused update was committed")
		require.NoError(t, it.Close())
	})

	// What an iterator of the write transaction holds in memory is of the
	// transaction Close rolled back: it is not handed out afterwards. The
	// filter's holds the document its Next parsed, the sort's the rows it
	// sorted, the aggregation's the groups it built.
	t.Run("ends the iterators of a rolled-back write transaction", func(t *testing.T) {
		for _, q := range []string{"filter", "sort", "aggregate"} {
			fx, coll := newColl(t)
			wtx, err := fx.WriteTx(ctx)
			require.NoError(t, err)
			require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":3,"a":1,"b":2}`),
				anyenc.MustParseJson(`{"id":4,"a":1,"b":1}`), anyenc.MustParseJson(`{"id":5,"a":2,"b":3}`)))
			var it Iterator
			switch q {
			case "filter":
				it, err = coll.Find(`{"a":1}`).Iter(wtx.Context())
			case "sort":
				it, err = coll.Find(`{"a":1}`).Sort("b").Iter(wtx.Context())
				require.NoError(t, err)
				// A field with no index: the plan sorts the rows in memory.
				require.Contains(t, fmt.Sprint(it.(*planIterator).plan.Root), "-> Sort")
			case "aggregate":
				it, err = coll.Aggregate(`[{"$group":{"_id":"$a","n":{"$sum":1}}}]`).Iter(wtx.Context())
			}
			require.NoError(t, err)
			require.True(t, it.Next())
			_, err = it.Doc()
			require.NoError(t, err)

			require.NoError(t, fx.DB.Close())

			_, err = it.Doc()
			require.ErrorIs(t, err, ErrDBIsClosed, q)
			require.False(t, it.Next(), q)
			require.ErrorIs(t, it.Err(), ErrDBIsClosed, q)
			_ = it.Close()
			require.NoError(t, wtx.Rollback())
		}
	})
}

func newFixture(t testing.TB, c ...*Config) *fixture {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		var conf *Config
		if len(c) != 0 {
			conf = c[0]
		}
		if conf == nil {
			conf = &Config{}
		}
		conf.InMemory = true
		db, err := Open(ctx, ":memory:", conf)
		require.NoError(t, err)
		fx := &fixture{DB: db, t: t}
		t.Cleanup(fx.finish)
		return fx
	}
	tmpDir, err := os.MkdirTemp("", "any-store-*")
	require.NoError(t, err)
	return newFixturePath(t, tmpDir, c...)
}

func newFixturePath(t testing.TB, tmpDir string, c ...*Config) *fixture {
	var conf *Config
	if len(c) != 0 {
		conf = c[0]
	}

	db, err := Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), conf)
	require.NoError(t, err)

	fx := &fixture{
		DB:     db,
		tmpDir: tmpDir,
		t:      t,
	}
	t.Cleanup(fx.finish)
	return fx
}

type fixture struct {
	DB
	tmpDir string
	t      testing.TB
}

func (fx *fixture) finish() {
	closeErr := fx.Close()
	if !errors.Is(closeErr, ErrDBIsClosed) {
		require.NoError(fx.t, closeErr)
	}
	if fx.tmpDir != "" {
		_ = os.RemoveAll(fx.tmpDir)
	}
}

func TestInspectIndexSketch_BasicReturnsCount(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 50 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}

	insp, ok := fx.DB.(IndexSketchInspector)
	require.True(t, ok, "db must implement IndexSketchInspector")

	info, err := insp.InspectIndexSketch(ctx, "test", "a")
	require.NoError(t, err)
	assert.Equal(t, 1024, info.Size)
	assert.Equal(t, uint64(50), info.DocCount)
	require.Len(t, info.Buckets, 1024)

	var sum uint64
	for _, b := range info.Buckets {
		sum += b
	}
	assert.Equal(t, uint64(50), sum)
}

func TestInspectIndexSketch_UnknownIndexReturnsErrNotFound(t *testing.T) {
	fx := newFixture(t)
	_, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	insp := fx.DB.(IndexSketchInspector)
	_, err = insp.InspectIndexSketch(ctx, "test", "nonexistent")
	assert.ErrorIs(t, err, ErrIndexNotFound)
}

func TestInspectIndexSketch_AccumulatesOnInsertAndDelete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 30 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%3),
		)))
	}

	insp := fx.DB.(IndexSketchInspector)
	info, err := insp.InspectIndexSketch(ctx, "test", "a")
	require.NoError(t, err)
	assert.Equal(t, uint64(30), info.DocCount)

	for i := range 10 {
		require.NoError(t, coll.DeleteId(ctx, i))
	}

	info, err = insp.InspectIndexSketch(ctx, "test", "a")
	require.NoError(t, err)
	assert.Equal(t, uint64(20), info.DocCount)
}

// In a working directory whose full name is longer than the OS takes, the
// relative name "test.db" opens but the full name cannot be built: getcwd
// fails, as SQLite's open fails when osGetcwd does (os_unix.c:7023-7024).
// The database file is opened by the same full name as its sentinel file,
// so the open fails, and leaves no file, rather than going on with a
// sentinel file it cannot reach.
func TestOpen_FullNameTooLongFailsTheOpen(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("needs a PATH_MAX that a deep working directory exceeds")
	}
	t.Chdir(t.TempDir())
	name := strings.Repeat("d", 200)
	for range 25 { // 5000 bytes, past PATH_MAX on Linux (4096) and Darwin (1024)
		require.NoError(t, os.Mkdir(name, 0o755))
		require.NoError(t, os.Chdir(name))
	}
	f, err := os.Create("test.db.probe")
	require.NoError(t, err, "setup: a relative name opens")
	require.NoError(t, f.Close())
	require.NoError(t, os.Remove("test.db.probe"))
	_, err = btree.FullPathname("test.db")
	require.Error(t, err, "setup: getcwd fails in this working directory")

	db, err := Open(context.Background(), "test.db", &Config{Durability: DurabilityConfig{Sentinel: true}})
	if err == nil {
		_ = db.Close()
	}
	require.Error(t, err)
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	require.Empty(t, entries, "the failed open left a file")
}

// On js/wasm the name a database is opened by is the name as given: a
// relative name reaches the VFS unchanged, not joined to a working directory,
// so the key of a database in a browser store stays the caller's.
func TestOpen_RelativeNameReachesTheVFSUnchangedOnJS(t *testing.T) {
	if runtime.GOOS != "js" {
		t.Skip("js/wasm only: elsewhere a VFS needs -tags vfs, and Unix and Windows resolve a full name")
	}
	t.Chdir(t.TempDir())
	var mu sync.Mutex
	var names []string
	SetVFS(VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			mu.Lock()
			names = append(names, name)
			mu.Unlock()
			f, err := os.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			return f, nil
		},
		Remove: func(name string) error {
			mu.Lock()
			names = append(names, name)
			mu.Unlock()
			return os.Remove(name)
		},
		Fdatasync: func(f File) error { return f.Sync() },
	})
	t.Cleanup(ResetVFS)

	ctx := context.Background()
	db, err := Open(ctx, "notes.db", nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"doc1"}`)))
	require.NoError(t, db.Close())

	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, names, "notes.db")
	require.Contains(t, names, "notes.db-wal")
	for _, name := range names {
		require.True(t, strings.HasPrefix(name, "notes.db"), "the VFS was given %q", name)
	}
}

func TestOpen_AnyStoreV1File(t *testing.T) {
	dir := t.TempDir()

	t.Run("v1 file is reported explicitly", func(t *testing.T) {
		path := filepath.Join(dir, "v1.db")
		// The SQLite header every any-store v1 database starts with. Literal on
		// purpose: a rename or edit of v1Magic must fail here rather than pass
		// by comparing the constant against itself.
		page := make([]byte, 4096)
		copy(page, "SQLite format 3\x00")
		require.NoError(t, os.WriteFile(path, page, 0o600))

		db, err := Open(ctx, path, nil)
		require.ErrorIs(t, err, ErrV1Database)
		require.Nil(t, db)
	})

	t.Run("v1 file the caller may only read", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("needs a file that opens for reading and not for writing")
		}
		path := filepath.Join(dir, "v1-readonly.db")
		page := make([]byte, 4096)
		copy(page, "SQLite format 3\x00")
		require.NoError(t, os.WriteFile(path, page, 0o400))
		if f, err := os.OpenFile(path, os.O_RDWR, 0); err == nil {
			_ = f.Close()
			t.Skip("needs a file that opens for reading and not for writing")
		}

		db, err := Open(ctx, path, nil)
		require.ErrorIs(t, err, ErrV1Database)
		require.Nil(t, db)
	})

	t.Run("v2 file still opens", func(t *testing.T) {
		path := filepath.Join(dir, "v2.db")
		db, err := Open(ctx, path, nil)
		require.NoError(t, err)
		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Close())
		require.NoError(t, db.Close())

		db, err = Open(ctx, path, nil)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	})

	t.Run("other garbage keeps the generic error", func(t *testing.T) {
		path := filepath.Join(dir, "garbage.db")
		require.NoError(t, os.WriteFile(path, []byte("this is not a database at all"), 0o600))

		_, err := Open(ctx, path, nil)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrV1Database)
	})
}

// TestSavepointOverflowCorruption replicates the crash test's savepoint-stress
// pattern at the anystore level with documents large enough to trigger overflow
// pages in the btree. This tests for zero-content overflow pages after
// a clean commit with savepoint rollback.
func TestSavepointOverflowCorruption(t *testing.T) {
	btree.SetDebugOverflowReadErrors(true)
	defer btree.SetDebugOverflowReadErrors(false)

	seeds := []int64{42, 12345, 55443322, 88776655, 99887766, 31337}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testSavepointOverflowWithSeed(t, seed)
		})
	}
}

// TestSavepointOverflowConcurrent adds concurrent readers and background
// checkpoints to the overflow savepoint test, matching the crash test pattern.
func TestSavepointOverflowConcurrent(t *testing.T) {
	btree.SetDebugOverflowReadErrors(true)
	defer btree.SetDebugOverflowReadErrors(false)

	seeds := []int64{42, 12345, 55443322, 88776655}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testSavepointOverflowConcurrent(t, seed)
		})
	}
}

// TestSavepointOverflowCloseReopen exercises the full crash-test pattern:
// savepoint stress → close → reopen → verify. This catches corruption that
// only manifests after checkpoint + WAL truncation during close.
func TestSavepointOverflowCloseReopen(t *testing.T) {
	btree.SetDebugOverflowReadErrors(true)
	defer btree.SetDebugOverflowReadErrors(false)

	seeds := []int64{42, 12345, 55443322, 88776655, 99887766, 31337, 11223344, 77665544}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testSavepointOverflowCloseReopen(t, seed)
		})
	}
}

// TestSavepointOverflowStress runs many iterations with random seeds,
// close/reopen cycles, and large document counts to reproduce zero-content
// overflow pages.
func TestSavepointOverflowStress(t *testing.T) {
	btree.SetDebugOverflowReadErrors(true)
	defer btree.SetDebugOverflowReadErrors(false)

	masterRng := rand.New(rand.NewSource(55443322)) // crash test's problematic seed
	numIterations := 100

	for i := 0; i < numIterations; i++ {
		seed := masterRng.Int63()
		t.Run(fmt.Sprintf("iter_%04d_seed_%d", i, seed), func(t *testing.T) {
			stressSavepointOverflow(t, seed)
		})
	}
}

// TestSavepointOverflowMultiIteration simulates the crash test pattern:
// multiple iterations on the same DB, where some iterations "crash" (close
// mid-transaction) and the DB is reopened for recovery. This tests whether
// accumulated DB state from crash/recovery cycles can trigger zero-content
// overflow pages.
func TestSavepointOverflowMultiIteration(t *testing.T) {
	btree.SetDebugOverflowReadErrors(true)
	defer btree.SetDebugOverflowReadErrors(false)

	seeds := []int64{55443322, 42, 12345, 88776655, 99887766, 31337}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			testMultiIterationOverflow(t, seed)
		})
	}
}

func testSavepointOverflowWithSeed(t *testing.T, seed int64) {
	t.Helper()
	fx := newFixture(t)
	ctx := context.Background()

	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(seed))
	expected := make(map[string]string)
	maxDocs := 30
	numTx := 80

	for txIdx := 0; txIdx < numTx; txIdx++ {
		outerTx, err := fx.WriteTx(ctx)
		require.NoError(t, err, "tx %d: WriteTx", txIdx)
		outerCtx := outerTx.Context()

		prevState := snapshotState(expected)

		outerOps := 1 + rng.Intn(5)
		for j := 0; j < outerOps; j++ {
			doCollOp(t, outerCtx, coll, rng, expected, maxDocs)
		}

		innerTx, err := fx.WriteTx(outerCtx)
		require.NoError(t, err, "tx %d: inner WriteTx", txIdx)
		innerCtx := innerTx.Context()

		innerPrev := snapshotState(expected)

		innerOps := 1 + rng.Intn(8)
		for j := 0; j < innerOps; j++ {
			doCollOp(t, innerCtx, coll, rng, expected, maxDocs)
		}

		if rng.Float64() < 0.3 {
			require.NoError(t, innerTx.Rollback(), "tx %d: inner rollback", txIdx)
			expected = innerPrev
		} else {
			require.NoError(t, innerTx.Commit(), "tx %d: inner commit", txIdx)
		}

		if rng.Float64() < 0.2 {
			for j := 0; j < rng.Intn(3); j++ {
				doCollOp(t, outerCtx, coll, rng, expected, maxDocs)
			}
		}

		if rng.Float64() < 0.1 {
			require.NoError(t, outerTx.Rollback(), "tx %d: outer rollback", txIdx)
			expected = prevState
		} else {
			require.NoError(t, outerTx.Commit(), "tx %d: outer commit", txIdx)
		}

		verifySavepointData(t, ctx, coll, expected, txIdx)
	}
}

func testSavepointOverflowConcurrent(t *testing.T, seed int64) {
	t.Helper()
	fx := newFixture(t)
	ctx := context.Background()

	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(seed))
	expected := make(map[string]string)
	maxDocs := 30

	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for !stop.Load() {
				rtx, err := fx.ReadTx(ctx)
				if err != nil {
					continue
				}
				iter, err := coll.Find(nil).Iter(rtx.Context())
				if err == nil {
					for iter.Next() {
						_, _ = iter.Doc()
					}
					iter.Close()
				}
				_ = rtx.Commit()
			}
		}(r)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = fx.Flush(ctx, 0, FlushModeCheckpointPassive)
			time.Sleep(time.Millisecond)
		}
	}()

	for txIdx := 0; txIdx < 80; txIdx++ {
		outerTx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		outerCtx := outerTx.Context()

		prevState := snapshotState(expected)

		outerOps := 1 + rng.Intn(5)
		for j := 0; j < outerOps; j++ {
			doCollOp(t, outerCtx, coll, rng, expected, maxDocs)
		}

		innerTx, err := fx.WriteTx(outerCtx)
		require.NoError(t, err)
		innerCtx := innerTx.Context()

		innerPrev := snapshotState(expected)

		innerOps := 1 + rng.Intn(8)
		for j := 0; j < innerOps; j++ {
			doCollOp(t, innerCtx, coll, rng, expected, maxDocs)
		}

		if rng.Float64() < 0.3 {
			require.NoError(t, innerTx.Rollback())
			expected = innerPrev
		} else {
			require.NoError(t, innerTx.Commit())
		}

		if rng.Float64() < 0.1 {
			require.NoError(t, outerTx.Rollback())
			expected = prevState
		} else {
			require.NoError(t, outerTx.Commit())
		}

		verifySavepointData(t, ctx, coll, expected, txIdx)
	}

	stop.Store(true)
	wg.Wait()
}

// testSavepointOverflowCloseReopen does savepoint stress, then closes and
// reopens the DB to verify data survives the checkpoint + WAL truncation cycle.
func testSavepointOverflowCloseReopen(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "savepoint-overflow-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	dbPath := filepath.Join(tmpDir, "test.db")

	rng := rand.New(rand.NewSource(seed))
	expected := make(map[string]string)
	maxDocs := 100
	numTx := 60

	// Phase 1: open DB, do savepoint stress
	db1, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)

	coll, err := db1.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	// Initial batch insert
	initialBatch := 20 + rng.Intn(30)
	for i := 0; i < initialBatch; i++ {
		doCollOp(t, ctx, coll, rng, expected, maxDocs)
	}

	// Background readers + flusher during savepoint stress
	var stop atomic.Bool
	var wg sync.WaitGroup

	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				rtx, err := db1.ReadTx(ctx)
				if err != nil {
					continue
				}
				iter, err := coll.Find(nil).Iter(rtx.Context())
				if err == nil {
					for iter.Next() {
						_, _ = iter.Doc()
					}
					iter.Close()
				}
				_ = rtx.Commit()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = db1.Flush(ctx, 0, FlushModeCheckpointPassive)
			time.Sleep(time.Millisecond)
		}
	}()

	// Savepoint stress
	for txIdx := 0; txIdx < numTx; txIdx++ {
		outerTx, err := db1.WriteTx(ctx)
		require.NoError(t, err, "tx %d: WriteTx", txIdx)
		outerCtx := outerTx.Context()

		prevState := snapshotState(expected)

		// Outer operations
		outerOps := 1 + rng.Intn(10)
		for j := 0; j < outerOps; j++ {
			doCollOp(t, outerCtx, coll, rng, expected, maxDocs)
		}

		// Inner WriteTx (savepoint)
		innerTx, err := db1.WriteTx(outerCtx)
		require.NoError(t, err, "tx %d: inner WriteTx", txIdx)
		innerCtx := innerTx.Context()

		innerPrev := snapshotState(expected)

		innerOps := 1 + rng.Intn(10)
		for j := 0; j < innerOps; j++ {
			doCollOp(t, innerCtx, coll, rng, expected, maxDocs)
		}

		// 30% rollback inner
		if rng.Float64() < 0.3 {
			require.NoError(t, innerTx.Rollback(), "tx %d: inner rollback", txIdx)
			expected = innerPrev
		} else {
			require.NoError(t, innerTx.Commit(), "tx %d: inner commit", txIdx)
		}

		// Post-savepoint ops (20%)
		if rng.Float64() < 0.2 {
			postOps := rng.Intn(5)
			for j := 0; j < postOps; j++ {
				doCollOp(t, outerCtx, coll, rng, expected, maxDocs)
			}
		}

		// 10% rollback outer
		if rng.Float64() < 0.1 {
			require.NoError(t, outerTx.Rollback(), "tx %d: outer rollback", txIdx)
			expected = prevState
		} else {
			require.NoError(t, outerTx.Commit(), "tx %d: outer commit", txIdx)
		}
	}

	// Stop background goroutines
	stop.Store(true)
	wg.Wait()

	// Verify in-session before close
	verifySavepointData(t, ctx, coll, expected, -1)

	// Phase 2: close DB
	require.NoError(t, db1.Close())

	// Phase 3: reopen and verify
	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.Collection(ctx, "docs")
	require.NoError(t, err)

	verifySavepointData(t, ctx, coll2, expected, -2)
}

func stressSavepointOverflow(t *testing.T, seed int64) {
	t.Helper()
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "sp-stress-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	dbPath := filepath.Join(tmpDir, "test.db")

	rng := rand.New(rand.NewSource(seed))
	expected := make(map[string]string)
	maxDocs := 500
	numTx := 10 + rng.Intn(50) // 10-59 tx, matching crash test

	// Open DB
	db, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)

	coll, err := db.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	// Initial batch (like crash test)
	initialBatch := 50 + rng.Intn(100)
	for i := 0; i < initialBatch; i++ {
		stressDoOp(t, ctx, coll, rng, expected, maxDocs)
	}

	// Background readers
	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				rtx, err := db.ReadTx(ctx)
				if err != nil {
					continue
				}
				iter, err := coll.Find(nil).Iter(rtx.Context())
				if err == nil {
					for iter.Next() {
						_, _ = iter.Doc()
					}
					iter.Close()
				}
				_ = rtx.Commit()
			}
		}()
	}

	// Background flusher
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = db.Flush(ctx, 0, FlushModeCheckpointPassive)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Savepoint stress
	for txIdx := 0; txIdx < numTx; txIdx++ {
		outerTx, err := db.WriteTx(ctx)
		if err != nil {
			continue
		}
		outerCtx := outerTx.Context()

		prevState := snapshotState(expected)

		outerOps := 1 + rng.Intn(10)
		for j := 0; j < outerOps; j++ {
			stressDoOp(t, outerCtx, coll, rng, expected, maxDocs)
		}

		innerTx, err := db.WriteTx(outerCtx)
		if err != nil {
			outerTx.Rollback()
			expected = prevState
			continue
		}
		innerCtx := innerTx.Context()

		innerPrev := snapshotState(expected)

		innerOps := 1 + rng.Intn(10)
		for j := 0; j < innerOps; j++ {
			stressDoOp(t, innerCtx, coll, rng, expected, maxDocs)
		}

		// 30% rollback inner
		if rng.Float64() < 0.3 {
			if err := innerTx.Rollback(); err != nil {
				t.Fatalf("tx %d: inner rollback: %v", txIdx, err)
			}
			expected = innerPrev
		} else {
			if err := innerTx.Commit(); err != nil {
				t.Fatalf("tx %d: inner commit: %v", txIdx, err)
			}
		}

		// 10% rollback outer
		if rng.Float64() < 0.1 {
			if err := outerTx.Rollback(); err != nil {
				t.Fatalf("tx %d: outer rollback: %v", txIdx, err)
			}
			expected = prevState
		} else {
			if err := outerTx.Commit(); err != nil {
				t.Fatalf("tx %d: outer commit: %v", txIdx, err)
			}
		}
	}

	stop.Store(true)
	wg.Wait()

	// Verify in-session
	stressVerify(t, ctx, coll, expected, "pre-close")

	// Close and reopen
	require.NoError(t, db.Close())

	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.Collection(ctx, "docs")
	require.NoError(t, err)

	stressVerify(t, ctx, coll2, expected, "post-reopen")
}

func testMultiIterationOverflow(t *testing.T, masterSeed int64) {
	t.Helper()
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "multi-iter-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })
	dbPath := filepath.Join(tmpDir, "test.db")

	rng := rand.New(rand.NewSource(masterSeed))
	maxDocs := 500
	numIterations := 50

	// expected tracks the "verified ground truth" state.
	// After a crash, we rebuild expected from the DB.
	var expected map[string]string

	for iter := 0; iter < numIterations; iter++ {
		iterSeed := rng.Int63()
		crashProb := 0.1 // 10% chance of crash (close without commit)
		isCrash := rng.Float64() < crashProb && iter > 0

		db, err := Open(ctx, dbPath, nil)
		require.NoError(t, err, "iter %d: Open", iter)

		coll, err := db.Collection(ctx, "docs")
		require.NoError(t, err, "iter %d: Collection", iter)

		// After open, rebuild expected from DB if we lost state (first iter or after crash)
		if expected == nil {
			expected = readAllDocs(t, ctx, coll)
		}

		iterRng := rand.New(rand.NewSource(iterSeed))

		if isCrash {
			// Simulate a "crash": start a write tx, do some work, then
			// close the DB WITHOUT committing. This simulates SIGKILL.
			crashIteration(t, ctx, db, coll, iterRng, expected, maxDocs)
			// After crash close, expected is unknown — rebuild on next open
			expected = nil
			continue
		}

		// Normal iteration: savepoint stress with background readers/flusher
		var stop atomic.Bool
		var wg sync.WaitGroup

		for r := 0; r < 4; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for !stop.Load() {
					rtx, err := db.ReadTx(ctx)
					if err != nil {
						return
					}
					iter, err := coll.Find(nil).Iter(rtx.Context())
					if err == nil {
						for iter.Next() {
							_, _ = iter.Doc()
						}
						iter.Close()
					}
					_ = rtx.Commit()
				}
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_ = db.Flush(ctx, 0, FlushModeCheckpointPassive)
				time.Sleep(5 * time.Millisecond)
			}
		}()

		numTx := 5 + iterRng.Intn(20)
		for txIdx := 0; txIdx < numTx; txIdx++ {
			outerTx, err := db.WriteTx(ctx)
			if err != nil {
				continue
			}
			outerCtx := outerTx.Context()
			prevState := snapshotState(expected)

			outerOps := 1 + iterRng.Intn(10)
			for j := 0; j < outerOps; j++ {
				multiIterDoOp(t, outerCtx, coll, iterRng, expected, maxDocs)
			}

			innerTx, err := db.WriteTx(outerCtx)
			if err != nil {
				outerTx.Rollback()
				expected = prevState
				continue
			}
			innerCtx := innerTx.Context()
			innerPrev := snapshotState(expected)

			innerOps := 1 + iterRng.Intn(10)
			for j := 0; j < innerOps; j++ {
				multiIterDoOp(t, innerCtx, coll, iterRng, expected, maxDocs)
			}

			if iterRng.Float64() < 0.3 {
				require.NoError(t, innerTx.Rollback(), "iter %d tx %d: inner rollback", iter, txIdx)
				expected = innerPrev
			} else {
				require.NoError(t, innerTx.Commit(), "iter %d tx %d: inner commit", iter, txIdx)
			}

			if iterRng.Float64() < 0.1 {
				require.NoError(t, outerTx.Rollback(), "iter %d tx %d: outer rollback", iter, txIdx)
				expected = prevState
			} else {
				require.NoError(t, outerTx.Commit(), "iter %d tx %d: outer commit", iter, txIdx)
			}
		}

		stop.Store(true)
		wg.Wait()

		// Verify before close
		multiIterVerify(t, ctx, coll, expected, fmt.Sprintf("iter_%d_pre_close", iter))

		require.NoError(t, db.Close(), "iter %d: Close", iter)

		// Reopen and verify
		db2, err := Open(ctx, dbPath, nil)
		require.NoError(t, err, "iter %d: Reopen", iter)

		coll2, err := db2.Collection(ctx, "docs")
		require.NoError(t, err, "iter %d: Collection after reopen", iter)

		multiIterVerify(t, ctx, coll2, expected, fmt.Sprintf("iter_%d_post_reopen", iter))

		require.NoError(t, db2.Close(), "iter %d: Close2", iter)
	}
}

func snapshotState(m map[string]string) map[string]string {
	snap := make(map[string]string, len(m))
	for k, v := range m {
		snap[k] = v
	}
	return snap
}

// doCollOp performs a random collection operation with an overflow-sized document.
func doCollOp(t *testing.T, ctx context.Context, coll Collection, rng *rand.Rand, expected map[string]string, maxDocs int) {
	t.Helper()
	id := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))

	if rng.Float64() < 0.25 {
		err := coll.DeleteId(ctx, id)
		if err == nil {
			delete(expected, id)
		}
		return
	}

	// Insert/update with large document (triggers overflow)
	dataSize := 1200 + rng.Intn(4000)
	data := make([]byte, dataSize)
	rand.Read(data)
	hexData := hex.EncodeToString(data[:16])

	arena := &anyenc.Arena{}
	obj := arena.NewObject()
	obj.Set("id", arena.NewString(id))
	obj.Set("data", arena.NewString(hex.EncodeToString(data)))
	obj.Set("hash", arena.NewString(hexData))

	err := coll.UpsertOne(ctx, obj)
	if err == nil {
		expected[id] = hexData
	}
}

func verifySavepointData(t *testing.T, ctx context.Context, coll Collection, expected map[string]string, txIdx int) {
	t.Helper()

	found := make(map[string]bool)
	iter, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err, "tx %d verify: Find", txIdx)
	defer iter.Close()

	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err, "tx %d verify: Doc", txIdx)

		id := doc.Value().GetStringBytes("id")
		hash := doc.Value().GetStringBytes("hash")

		idStr := string(id)
		found[idStr] = true

		expectedHash, exists := expected[idStr]
		if !exists {
			t.Fatalf("tx %d verify: unexpected doc %s in collection", txIdx, idStr)
		}

		if string(hash) != expectedHash {
			t.Fatalf("tx %d verify: doc %s hash mismatch: got=%s want=%s",
				txIdx, idStr, string(hash), expectedHash)
		}
	}

	for id := range expected {
		if !found[id] {
			t.Fatalf("tx %d verify: expected doc %s not found in collection", txIdx, id)
		}
	}
}

func stressDoOp(t *testing.T, ctx context.Context, coll Collection, rng *rand.Rand, expected map[string]string, maxDocs int) {
	t.Helper()
	id := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))

	// 10% delete, 20% insert (smaller doc), 40% upsert (large overflow), 30% upsert (medium)
	r := rng.Float64()
	if r < 0.10 {
		err := coll.DeleteId(ctx, id)
		if err == nil {
			delete(expected, id)
		}
		return
	}

	var dataSize int
	if r < 0.30 {
		// Small document (no overflow)
		dataSize = 50 + rng.Intn(200)
	} else if r < 0.70 {
		// Large document (triggers overflow)
		dataSize = 2000 + rng.Intn(6000)
	} else {
		// Medium document (may or may not overflow depending on key size)
		dataSize = 500 + rng.Intn(1500)
	}

	data := make([]byte, dataSize)
	rand.Read(data)
	hexData := hex.EncodeToString(data[:16])

	arena := &anyenc.Arena{}
	obj := arena.NewObject()
	obj.Set("id", arena.NewString(id))
	obj.Set("data", arena.NewString(hex.EncodeToString(data)))
	obj.Set("hash", arena.NewString(hexData))

	err := coll.UpsertOne(ctx, obj)
	if err == nil {
		expected[id] = hexData
	}
}

func stressVerify(t *testing.T, ctx context.Context, coll Collection, expected map[string]string, label string) {
	t.Helper()

	found := make(map[string]bool)
	iter, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err, "%s verify: Find", label)
	defer iter.Close()

	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err, "%s verify: Doc", label)

		id := doc.Value().GetStringBytes("id")
		hash := doc.Value().GetStringBytes("hash")

		idStr := string(id)
		found[idStr] = true

		expectedHash, exists := expected[idStr]
		if !exists {
			t.Fatalf("%s verify: unexpected doc %s in collection", label, idStr)
		}

		if string(hash) != expectedHash {
			t.Fatalf("%s verify: doc %s hash mismatch: got=%s want=%s",
				label, idStr, string(hash), expectedHash)
		}
	}

	for id := range expected {
		if !found[id] {
			t.Fatalf("%s verify: expected doc %s not found in collection", label, id)
		}
	}
}

func readAllDocs(t *testing.T, ctx context.Context, coll Collection) map[string]string {
	t.Helper()
	result := make(map[string]string)
	iter, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err, "readAllDocs: Find")
	defer iter.Close()

	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err, "readAllDocs: Doc")
		id := string(doc.Value().GetStringBytes("id"))
		hash := string(doc.Value().GetStringBytes("hash"))
		result[id] = hash
	}
	return result
}

func crashIteration(t *testing.T, ctx context.Context, db DB, coll Collection, rng *rand.Rand, expected map[string]string, maxDocs int) {
	t.Helper()

	// Start a tx, do some work, then rollback and close.
	// This exercises the WAL with dirty data that gets rolled back,
	// followed by close (checkpoint + WAL truncation).
	outerTx, err := db.WriteTx(ctx)
	if err != nil {
		db.Close()
		return
	}
	outerCtx := outerTx.Context()

	ops := 5 + rng.Intn(20)
	for j := 0; j < ops; j++ {
		id := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))
		if rng.Float64() < 0.2 {
			coll.DeleteId(outerCtx, id)
			continue
		}
		dataSize := 1000 + rng.Intn(5000)
		data := make([]byte, dataSize)
		rand.Read(data)
		arena := &anyenc.Arena{}
		obj := arena.NewObject()
		obj.Set("id", arena.NewString(id))
		obj.Set("data", arena.NewString(hex.EncodeToString(data)))
		obj.Set("hash", arena.NewString(hex.EncodeToString(data[:16])))
		coll.UpsertOne(outerCtx, obj)
	}

	// Maybe create a nested savepoint too
	if rng.Float64() < 0.5 {
		innerTx, err := db.WriteTx(outerCtx)
		if err == nil {
			innerCtx := innerTx.Context()
			for j := 0; j < rng.Intn(10); j++ {
				id := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))
				dataSize := 1000 + rng.Intn(5000)
				data := make([]byte, dataSize)
				rand.Read(data)
				arena := &anyenc.Arena{}
				obj := arena.NewObject()
				obj.Set("id", arena.NewString(id))
				obj.Set("data", arena.NewString(hex.EncodeToString(data)))
				obj.Set("hash", arena.NewString(hex.EncodeToString(data[:16])))
				coll.UpsertOne(innerCtx, obj)
			}
			// Rollback inner savepoint
			innerTx.Rollback()
		}
	}

	// Rollback the outer tx (simulating uncommitted work before crash)
	outerTx.Rollback()
	// Close normally — this exercises checkpoint + WAL truncation
	db.Close()
}

func multiIterDoOp(t *testing.T, ctx context.Context, coll Collection, rng *rand.Rand, expected map[string]string, maxDocs int) {
	t.Helper()
	id := fmt.Sprintf("doc-%06d", rng.Intn(maxDocs))

	if rng.Float64() < 0.15 {
		err := coll.DeleteId(ctx, id)
		if err == nil {
			delete(expected, id)
		}
		return
	}

	dataSize := 1000 + rng.Intn(5000)
	data := make([]byte, dataSize)
	rand.Read(data)
	hexData := hex.EncodeToString(data[:16])

	arena := &anyenc.Arena{}
	obj := arena.NewObject()
	obj.Set("id", arena.NewString(id))
	obj.Set("data", arena.NewString(hex.EncodeToString(data)))
	obj.Set("hash", arena.NewString(hexData))

	err := coll.UpsertOne(ctx, obj)
	if err == nil {
		expected[id] = hexData
	}
}

func multiIterVerify(t *testing.T, ctx context.Context, coll Collection, expected map[string]string, label string) {
	t.Helper()

	found := make(map[string]bool)
	iter, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err, "%s verify: Find", label)
	defer iter.Close()

	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err, "%s verify: Doc", label)

		id := doc.Value().GetStringBytes("id")
		hash := doc.Value().GetStringBytes("hash")

		idStr := string(id)
		found[idStr] = true

		expectedHash, exists := expected[idStr]
		if !exists {
			t.Fatalf("%s verify: unexpected doc %s in collection", label, idStr)
		}
		if string(hash) != expectedHash {
			t.Fatalf("%s verify: doc %s hash mismatch: got=%s want=%s",
				label, idStr, string(hash), expectedHash)
		}
	}

	for id := range expected {
		if !found[id] {
			t.Fatalf("%s verify: expected doc %s not found in collection", label, id)
		}
	}
}

// These tests guard the create-in-flight gap in reconcileIndexSet: a read tx
// whose staleness verdict was baked at begin (an unconsumed schema-cookie
// delta — a peer commit, or a local commit racing beginRead between snapshot
// capture and the local-counter load) reconciles ALL registered handles
// against its own, older snapshot. A collection created after that snapshot
// has no catalog key there yet, so collectionVanished reports true via the
// keyNotFound branch and the staleness pass invalidates a live handle. The
// create commits fine; disk is correct; the cached handle is dead — every
// later op fails ErrCollectionClosed until the caller reopens.
//
// reconcileIndexSet guards rename-in-flight explicitly but not
// create-in-flight, though the same safety argument transfers: the creating
// writer holds the cross-process write lock, so any cookie bump a concurrent
// read pass consumes predates the creating tx's begin.

// The in-flight variant: the create sits inside an open write tx (handle
// registered mid-tx, catalog write uncommitted) while a plain read op begins
// with a stale local-counter cache and runs the full production
// ReadTx→checkStale path.
func TestReconcile_ReadTxSparesCreateInFlightHandle(t *testing.T) {
	skipIfInMemory(t, "models another process's commit; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	seed, err := fx.CreateCollection(ctx, "seed")
	require.NoError(t, err)

	// Committed counters, to rewind against below.
	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	sc := rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Create in-flight: handle registered in openedCollections, catalog write
	// pending until wtx.Commit. The write tx's own begin ran checkStale with
	// counters in sync, so nothing is consumed yet.
	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	fresh, err := fx.CreateCollection(wtx.Context(), "fresh")
	require.NoError(t, err)

	// Emulate an unconsumed cookie bump: the state a reader captures when its
	// beginRead races a commit (snapshot fields baked before the writer's
	// counter store lands) or when a peer process committed DDL.
	modelPeerSchemaChange(dbi, sc)

	// Any read op now begins a read tx that sees the unknown cookie and runs
	// reconcileIndexSet against a snapshot that predates the create.
	_, err = seed.Count(ctx)
	require.NoError(t, err)

	require.NoError(t, wtx.Commit())

	// The create committed; the handle must still be live.
	_, err = fresh.Count(ctx)
	require.NoError(t, err, "read-tx staleness pass invalidated a create-in-flight handle")
}

// The post-commit variant: the create commits normally, but the reader's
// snapshot predates it (begun before the create, checkStale preempted until
// after — the gap between BeginRead and checkStale inside db.ReadTx is a real
// preemption point). The committed handle must survive the pass.
func TestReconcile_ReadTxSparesFreshlyCommittedHandle(t *testing.T) {
	skipIfInMemory(t, "models another process's commit; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	_, err := fx.CreateCollection(ctx, "seed")
	require.NoError(t, err)

	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	sc := rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Stale verdict baked at begin; snapshot predates the create below.
	modelPeerSchemaChange(dbi, sc)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	require.NotEqual(t, dbi.epoch.Load().known, reader.SnapshotSchemaCookie(), "setup: the reader's cookie is one the schema state has not seen")

	fresh, err := fx.CreateCollection(ctx, "fresh")
	require.NoError(t, err)
	// The create stands in for a peer's commit, which leaves the local
	// counters behind (TestCheckStale_ConsumesOnlySnapshotCounters).
	modelPeerSchemaChange(dbi, sc)

	// The reader resumes exactly where db.ReadTx would: checkStale on its own
	// pre-create snapshot.
	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())

	_, err = fresh.Count(ctx)
	require.NoError(t, err, "read-tx staleness pass invalidated a freshly committed handle")
}

// The rename analog: after Rename commits, the handle is registered under its
// NEW name, which a pre-rename snapshot cannot resolve (keyNotFound on the
// new key). Rename's publication raises the handle's visibility bound to the
// rename commit cookie so older-snapshot passes skip it.
func TestReconcile_ReadTxSparesFreshlyRenamedHandle(t *testing.T) {
	skipIfInMemory(t, "models another process's commit; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	coll, err := fx.CreateCollection(ctx, "before")
	require.NoError(t, err)

	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	sc := rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Stale verdict baked at begin; snapshot predates the rename below.
	modelPeerSchemaChange(dbi, sc)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	require.NotEqual(t, dbi.epoch.Load().known, reader.SnapshotSchemaCookie(), "setup: the reader's cookie is one the schema state has not seen")

	require.NoError(t, coll.Rename(ctx, "after"))
	// The rename stands in for a peer's commit (see above).
	modelPeerSchemaChange(dbi, sc)

	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())

	_, err = coll.Count(ctx)
	require.NoError(t, err, "read-tx staleness pass invalidated a freshly renamed handle")
}

// The consumption pin: a transaction whose snapshot predates a later change
// consumes nothing of it. Its snapshot is not exact for the change epoch, so
// nothing it loads is stamped current; the change it never saw is counted by
// the next begin, and a begin after that finds nothing new.
func TestCheckStale_ConsumesOnlySnapshotCounters(t *testing.T) {
	skipIfInMemory(t, "the change epoch counts other processes' commits; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	_, err := fx.CreateCollection(ctx, "seed")
	require.NoError(t, err)

	// A peer's commit, noticed by this reader's begin. Assertions run after
	// each tx is released so a failure cannot leave a reader open (Close
	// would block in the fixture cleanup).
	dbi.btreeDB.ForgetSeenHeader()
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	e1, exact1 := reader.ChangeEpoch()

	later, err := fx.CreateCollection(ctx, "later")
	require.NoError(t, err)
	// The create stands in for a peer's commit: one the change state has
	// not seen (an own commit is none, TestCheckStale_OwnCommitBeforePassSkipsReload).
	dbi.btreeDB.ForgetSeenHeader()

	// The pass runs with a snapshot that lacks the create: it must neither
	// invalidate the new handle nor count as having seen the create.
	dbi.checkStale(reader)
	e1b, exact1b := reader.ChangeEpoch()
	require.NoError(t, reader.Rollback())
	require.True(t, exact1)
	require.Equal(t, e1, e1b, "a transaction counts nothing after its begin")
	require.False(t, exact1b, "a snapshot older than the newest change is not exact")

	next, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	e2, exact2 := next.ChangeEpoch()
	dbi.checkStale(next)
	require.NoError(t, next.Rollback())
	require.Equal(t, e1+1, e2, "the change the older snapshot lacked is counted by the next begin")
	require.True(t, exact2)

	settled, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	e3, exact3 := settled.ChangeEpoch()
	require.NoError(t, settled.Rollback())
	require.Equal(t, e2, e3, "and counted once")
	require.True(t, exact3)

	_, err = later.Count(ctx)
	require.NoError(t, err)
}

// A stale verdict a commit of this process overtakes before the pass runs
// reloads nothing: that commit's own begin reconciled the peer's state, its
// commit republished the live sketch and recorded the counters. A peer's
// commit (the local counters left behind) still reloads, into a reader-owned
// sketch that is allocated once and reused by later reloads of the same
// shape; a reader never touches the live sketch. The live sketch is the next
// write transaction's to load: it takes the peer's row before its own
// deltas, however many readers loaded that row before it.
func TestCheckStale_OwnCommitBeforePassSkipsReload(t *testing.T) {
	skipIfInMemory(t, "models another process's commit; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	live := idx.sketch
	require.Same(t, live, idx.loadPubSketch())

	// A peer's commit: the change state has not seen the committed header.
	rewindLocalCounters := dbi.btreeDB.ForgetSeenHeader
	epochOf := func(tx *btree.ReadTx) uint64 {
		e, _ := tx.ChangeEpoch()
		return e
	}
	settledBefore, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	e0 := epochOf(settledBefore)
	require.NoError(t, settledBefore.Rollback())
	// stalePass is a reader that notices the peer's commit at its begin and
	// then plans with the collection.
	stalePass := func() {
		reader, err := dbi.btreeDB.BeginRead()
		require.NoError(t, err)
		e := epochOf(reader)
		dbi.checkStale(reader)
		c.refreshSketches(reader, c.cur())
		require.NoError(t, reader.Rollback())
		require.Greater(t, e, e0, "the begin counted the peer's commit")
		e0 = e
	}

	rewindLocalCounters()
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	e1 := epochOf(reader)
	insertErr := coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":2}`))
	dbi.checkStale(reader)
	c.refreshSketches(reader, c.cur())
	require.NoError(t, reader.Rollback())
	require.NoError(t, insertErr)
	require.Equal(t, e0+1, e1, "the begin counted the peer's commit")
	assert.Same(t, live, idx.loadPubSketch(), "an own commit's verdict reloaded")
	settled, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	e2 := epochOf(settled)
	require.NoError(t, settled.Rollback())
	assert.Equal(t, e1, e2, "the own commit is no change")
	e0 = e2

	rewindLocalCounters()
	stalePass()
	pub := idx.loadPubSketch()
	require.NotSame(t, live, pub, "a peer's commit must reload into a reader-owned sketch")
	assert.Same(t, live, idx.sketch)
	assert.Equal(t, uint64(2), pub.GetDocCount())

	// The bytes on disk change under a peer, in either stored encoding: the
	// reload decodes them into the same reader-owned object, and the live
	// sketch keeps its count while only readers pass.
	docs, liveDocs := uint64(40), uint64(2)
	for _, enc := range []struct {
		name    string
		marshal func(*qplanner.IndexSketch) []byte
	}{
		{"v2", func(s *qplanner.IndexSketch) []byte { return s.MarshalBinary(nil) }},
		{"compressed", func(s *qplanner.IndexSketch) []byte { return s.MarshalCompressed(nil) }},
	} {
		peerWrites := func(levels int) []byte {
			docs++
			onDisk := qplanner.NewIndexSketch(qplanner.DefaultSketchSize, levels)
			for i := uint64(0); i < docs; i++ {
				for l := 0; l < levels; l++ {
					onDisk.Increment(l, []byte{byte(i), byte(l)})
				}
				onDisk.IncrementDocCount()
			}
			row := enc.marshal(onDisk)
			rawPut(t, dbi, sketchKey("c", idx.info.Name), row)
			rewindLocalCounters()
			stalePass()
			return row
		}
		// own is the shape of the live sketch (the index has one field; the
		// first pass's writer leaves the live sketch with two levels, see
		// below), other is the shape it does not have.
		own := live.Levels
		other := 3 - own
		peerWrites(own)
		pub = idx.loadPubSketch()
		require.NotSame(t, live, pub, enc.name)
		require.Equal(t, docs, pub.GetDocCount(), enc.name)

		peerWrites(own)
		assert.Same(t, pub, idx.loadPubSketch(), "%s: the reader-owned sketch is reused", enc.name)
		assert.Equal(t, docs, pub.GetDocCount(), enc.name)
		assert.Equal(t, liveDocs, live.GetDocCount(), enc.name)

		// Bytes that do not decode leave the published counters alone, every
		// one of them: a row of the index's shape, and one whose header names
		// another shape.
		counters := pub.Snapshot(nil)
		for levels := 1; levels <= 2; levels++ {
			row := enc.marshal(qplanner.NewIndexSketch(qplanner.DefaultSketchSize, levels))
			rawPut(t, dbi, sketchKey("c", idx.info.Name), row[:len(row)-1])
			rewindLocalCounters()
			stalePass()
			assert.Same(t, pub, idx.loadPubSketch(), "%s: a truncated row of %d levels", enc.name, levels)
			assert.True(t, slices.Equal(counters, pub.Snapshot(nil)), "%s: a truncated row of %d levels changed the counters", enc.name, levels)
		}

		// Bytes of another shape never reshape the published object: they are
		// decoded into a fresh one.
		peerWrites(other)
		assert.NotSame(t, pub, idx.loadPubSketch(), enc.name)
		assert.Equal(t, own, pub.Levels, enc.name)
		assert.Equal(t, other, idx.loadPubSketch().Levels, enc.name)
		assert.Equal(t, docs, idx.loadPubSketch().GetDocCount(), enc.name)
		assert.Same(t, live, idx.sketch, enc.name)
		assert.Equal(t, liveDocs, live.GetDocCount(), "%s: readers alone leave the live sketch", enc.name)

		// A write of this process: the readers before it loaded the peer's
		// row into the published sketch only, so the writer loads it into
		// the live one and its delta goes on top. The row has another
		// shape than the live object, which was published and may still be
		// in a planner's hands: it is left as it is, and a fresh object
		// becomes the live one. The reader below holds the old object
		// across the load (a data race if the load reshaped it). The row
		// the writer stores holds the peer's counts and its own, and the
		// published sketch is the live one again.
		held, stop, stopped := live, make(chan struct{}), make(chan struct{})
		heldCounters := held.Snapshot(nil)
		go func() {
			defer close(stopped)
			for {
				select {
				case <-stop:
					return
				default:
					_ = held.Estimate(held.NumLevels()-1, []byte{1})
					_ = held.EntryCount(0)
					_ = held.Distribution(0)
				}
			}
		}()
		insertErr := coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":"own-%s","a":7}`, enc.name)))
		close(stop)
		<-stopped
		require.NoError(t, insertErr)
		live = idx.sketch
		// Compared as a bool: a failed NotSame prints both sketches.
		require.True(t, held != live, "%s: a row of another shape is not decoded into the published object", enc.name)
		assert.Equal(t, own, held.Levels, enc.name)
		assert.True(t, slices.Equal(heldCounters, held.Snapshot(nil)), "%s: the object a reader may hold is as it was", enc.name)
		liveDocs = docs + 1
		assert.Equal(t, other, live.Levels, enc.name)
		assert.Equal(t, liveDocs, live.GetDocCount(), "%s: the peer's row and the own delta", enc.name)
		stored, err := dbi.InspectIndexSketch(ctx, "c", idx.info.Name)
		require.NoError(t, err)
		assert.Equal(t, liveDocs, stored.DocCount, "%s: the stored row", enc.name)
		require.Same(t, live, idx.loadPubSketch(), enc.name)

		// After a commit of this process the published sketch is the live
		// one, and a reload decodes into a fresh object. A row that does not
		// decode must not publish that object, empty, over the live counts.
		settled, err := dbi.btreeDB.BeginRead()
		require.NoError(t, err)
		e0 = epochOf(settled)
		require.NoError(t, settled.Rollback())
		row := enc.marshal(qplanner.NewIndexSketch(qplanner.DefaultSketchSize, 1))
		rawPut(t, dbi, sketchKey("c", idx.info.Name), row[:len(row)-1])
		rewindLocalCounters()
		stalePass()
		assert.Same(t, live, idx.loadPubSketch(), "%s: a truncated row while the live sketch is published", enc.name)
	}
}

// rawPut writes a system-namespace record through a raw btree tx: a write
// the in-memory state knows nothing about, like a peer's.
func rawPut(t *testing.T, dbi *db, key, val []byte) {
	t.Helper()
	wtx, err := dbi.btreeDB.BeginWrite()
	require.NoError(t, err)
	if err = wtx.Put(dbi.systemNS, key, val); err != nil {
		_ = wtx.Rollback()
		t.Fatal(err)
	}
	wtx.MarkDataChanged()
	require.NoError(t, wtx.Commit())
}

// A commit of another process is noticed at a begin without a read per open
// collection: a collection's sketches are reloaded when it is next planned
// with, or written to.
func TestCheckStale_SketchesReloadOnUse(t *testing.T) {
	skipIfInMemory(t, "models another process's commit; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	open := func(name string) (Collection, *index) {
		coll, err := fx.CreateCollection(ctx, name)
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
		return coll, coll.(*collection).loadIndexes()[0]
	}
	used, usedIdx := open("used")
	written, writtenIdx := open("written")
	_, idleIdx := open("idle")

	// The other process: new sketch bytes for all three, and a commit this
	// process has not counted.
	peer := qplanner.NewIndexSketch(qplanner.DefaultSketchSize, 1)
	for range 42 {
		peer.IncrementDocCount()
	}
	for _, name := range []string{"used", "written", "idle"} {
		rawPut(t, dbi, sketchKey(name, usedIdx.info.Name), peer.MarshalBinary(nil))
	}
	dbi.btreeDB.ForgetSeenHeader()

	n, err := used.Find(`{"a":1}`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assert.Equal(t, uint64(42), usedIdx.loadPubSketch().GetDocCount(), "planned with: reloaded")
	assert.Equal(t, uint64(1), writtenIdx.loadPubSketch().GetDocCount(), "not used yet")
	assert.Equal(t, uint64(1), idleIdx.loadPubSketch().GetDocCount(), "not used yet")

	// A write catches up before it adds its own.
	require.NoError(t, written.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":2}`)))
	assert.Equal(t, uint64(43), writtenIdx.loadPubSketch().GetDocCount())
	assert.Equal(t, uint64(1), idleIdx.loadPubSketch().GetDocCount())
}

// Another process's schema change noticed while a local tx has uncommitted
// DDL on a collection — index DDL, or a rename — is verified against the
// committed head, which is what the readers of that moment work with; the
// writer's version is in its log until the commit installs it, proven for
// the generation the writer is in.
func TestPeerSchemaChange_VerifiedAfterLocalDDL(t *testing.T) {
	skipIfInMemory(t, "models another process's commit; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	other, err := fx.CreateCollection(ctx, "other")
	require.NoError(t, err)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	c := coll.(*collection)
	verified := func() bool { return c.cur().gen.Load() == dbi.epoch.Load().gen }
	require.True(t, verified())

	for name, ddl := range map[string]func(wtx WriteTx) error{
		"index":  func(wtx WriteTx) error { return coll.EnsureIndex(wtx.Context(), IndexInfo{Fields: []string{"a"}}) },
		"rename": func(wtx WriteTx) error { return coll.Rename(wtx.Context(), "d") },
	} {
		committed := c.cur()
		wtx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, ddl(wtx), name)
		inFlight, err := c.resolve(wtx.btreeReadTx())
		require.NoError(t, err, name)
		require.True(t, committed == c.cur(), "%s: the writer's version is its own until the commit", name)

		peerSchemaChangeNow(t, dbi)
		_, err = other.Count(ctx)
		require.NoError(t, err, name)
		n, err := coll.Count(ctx)
		require.NoError(t, err, name)
		assert.Equal(t, 1, n, name)
		assert.True(t, committed == c.cur(), "%s: a reader replaced the committed head", name)
		assert.True(t, verified(), "%s: the reader verified the committed head", name)

		require.NoError(t, wtx.Commit(), name)
		assert.True(t, inFlight == c.cur(), "%s: the commit installed the writer's version", name)
		assert.True(t, verified(), "%s: installed for the writer's generation", name)
		n, err = coll.Find(`{"a":1}`).Count(ctx)
		require.NoError(t, err, name)
		assert.Equal(t, 1, n, name)
	}
}

// Reloads decode into the published sketch while planners read it: run
// with -race.
func TestCheckStale_ReloadUnderConcurrentReaders(t *testing.T) {
	skipIfInMemory(t, "models another process's commit; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := 0; i < 50; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var readerErrs atomic.Int32
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := coll.Find(`{"a":{"$gte":3}}`).Count(ctx); err != nil {
					readerErrs.Add(1)
					return
				}
				if _, err := coll.Find(`{"a":3}`).Explain(ctx); err != nil {
					readerErrs.Add(1)
					return
				}
				if _, err := dbi.InspectIndexSketch(ctx, "c", "a"); err != nil {
					readerErrs.Add(1)
					return
				}
			}
		}()
	}
	for i := 0; i < 300; i++ {
		dbi.btreeDB.ForgetSeenHeader()
		reader, err := dbi.btreeDB.BeginRead()
		require.NoError(t, err)
		dbi.checkStale(reader)
		require.NoError(t, reader.Rollback())
	}
	close(stop)
	wg.Wait()
	assert.Zero(t, readerErrs.Load())
}

// These tests guard the index-set publication ratchet: a staleness pass
// whose snapshot is older than the published sets must not republish from
// its older catalog. Rebuilt from that snapshot it would resurrect a
// dropped index (readers at current snapshots then plan against freed
// namespaces — silently wrong results) or evict a fresher one (a concurrent
// DropIndex fails ErrIndexNotFound; writes stop maintaining it).

func ratchetFixture(t *testing.T) (*fixture, *db, Collection) {
	t.Helper()
	skipIfInMemory(t, "models another process's commit; not applicable in-memory")
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
	sc := rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())
	modelPeerSchemaChange(dbi, sc)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)

	require.NoError(t, coll.DropIndex(ctx, "a"))
	// The drop stands in for a peer's commit (TestCheckStale_ConsumesOnlySnapshotCounters).
	modelPeerSchemaChange(dbi, sc)

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
	sc := rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())
	modelPeerSchemaChange(dbi, sc)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	// The create stands in for a peer's commit (see above).
	modelPeerSchemaChange(dbi, sc)

	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())

	require.Len(t, coll.(*collection).loadIndexes(), 1,
		"stale pass evicted a freshly created index from the live set")
	// The observed production symptom of the eviction: DropIndex not found.
	require.NoError(t, coll.DropIndex(ctx, "a"))
}

// A rolled-back DDL tx must leave the head exactly as it was: the same
// version, proven for the same range. One stamped for a cookie that never
// came would be trusted once another commit reaches that cookie, for state
// that never committed.
func TestReconcileRatchet_RollbackLeavesRatchetUntouched(t *testing.T) {
	fx, dbi, coll := ratchetFixture(t)
	before := coll.(*collection).cur()

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(wtx.Context(), IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, wtx.Rollback())

	cc := coll.(*collection)
	require.Empty(t, cc.loadIndexes(), "rollback must restore the published set")
	require.True(t, before == cc.cur(), "rollback must restore the head it replaced")

	// The unmoved ratchet must not block a genuine staleness reconcile at the
	// unchanged cookie (peer-bump emulation), and real DDL still works.
	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	sc := rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())
	modelPeerSchemaChange(dbi, sc)
	n, err := coll.Find(`{"a":{"$gte":0}}`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.DropIndex(ctx, "a"))
}

// The writer lists only the collections it wrote (db.sketchDirty,
// db.ftsDirty); begin, commit and savepoints visit those and nothing else.
func TestWriteTx_DirtyLists(t *testing.T) {
	create := func(t *testing.T, fx *fixture, name string, infos ...IndexInfo) *collection {
		coll, err := fx.CreateCollection(ctx, name)
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, infos...))
		return coll.(*collection)
	}
	rangeIdx := IndexInfo{Fields: []string{"a"}}
	ftsIdx := IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"body":"word%d here"}`, id, id, id))
	}
	// Names, not handles: a failed assertion on handles prints the whole db.
	names := func(cs []*collection) []string {
		ns := make([]string, len(cs))
		for i, c := range cs {
			ns[i] = c.cur().name
		}
		return ns
	}

	t.Run("sketches: joined on write, left at the next begin", func(t *testing.T) {
		fx := newFixture(t)
		dbi := fx.DB.(*db)
		a, b := create(t, fx, "a", rangeIdx), create(t, fx, "b", rangeIdx)

		assert.Empty(t, names(dbi.sketchDirty), "creating a collection and an index over no documents lists nothing")
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.Insert(tx.Context(), doc(1)))
		assert.Equal(t, []string{"a"}, names(dbi.sketchDirty))
		require.NoError(t, tx.Commit())
		assert.Equal(t, []string{"a"}, names(dbi.sketchDirty), "commit persists and removes nothing")
		assert.False(t, a.loadIndexes()[0].sketchModified)

		tx, err = fx.WriteTx(ctx)
		require.NoError(t, err)
		assert.Empty(t, names(dbi.sketchDirty), "begin drains what the commit left")
		assert.False(t, a.sketchDirty)
		require.NoError(t, b.Insert(tx.Context(), doc(1)))
		assert.Equal(t, []string{"b"}, names(dbi.sketchDirty))
		require.NoError(t, tx.Commit())
	})

	t.Run("sketches: a rollback is rebased by a tx writing another collection", func(t *testing.T) {
		fx := newFixture(t)
		dbi := fx.DB.(*db)
		a, b := create(t, fx, "a", rangeIdx), create(t, fx, "b", rangeIdx)
		require.NoError(t, a.Insert(ctx, doc(1)))
		idx := a.loadIndexes()[0]

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.Insert(tx.Context(), doc(2), doc(3)))
		require.NoError(t, tx.Rollback())
		assert.Equal(t, []string{"a"}, names(dbi.sketchDirty))
		assert.True(t, idx.sketchModified)
		assert.EqualValues(t, 3, idx.sketch.GetDocCount())

		require.NoError(t, b.Insert(ctx, doc(1)))
		assert.False(t, idx.sketchModified)
		assert.EqualValues(t, 1, idx.sketch.GetDocCount())
		assert.Equal(t, []string{"b"}, names(dbi.sketchDirty))
		on, err := dbi.InspectIndexSketch(ctx, "a", "a")
		require.NoError(t, err)
		assert.EqualValues(t, 1, on.DocCount)
	})

	// No stored row is no statistics: the rebase empties the live sketch,
	// which is then as the file has it, and the collection leaves the list.
	// Nothing of the rolled-back transaction is stored by a later commit.
	t.Run("sketches: no committed row is rebased to an empty sketch", func(t *testing.T) {
		fx := newFixture(t)
		dbi := fx.DB.(*db)
		a, b := create(t, fx, "a", rangeIdx), create(t, fx, "b", rangeIdx)
		require.NoError(t, a.Insert(ctx, doc(1)))
		require.NoError(t, dbi.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			return tx.Delete(dbi.systemNS, sketchKey("a", "a"))
		}))
		idx := a.loadIndexes()[0]

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.Insert(tx.Context(), doc(2)))
		require.NoError(t, tx.Rollback())
		assert.Equal(t, []string{"a"}, names(dbi.sketchDirty))
		assert.EqualValues(t, 2, idx.sketch.GetDocCount())

		tx, err = fx.WriteTx(ctx)
		require.NoError(t, err)
		assert.Empty(t, names(dbi.sketchDirty))
		assert.False(t, idx.sketchModified)
		assert.Zero(t, idx.sketch.GetDocCount())
		require.NoError(t, b.Insert(tx.Context(), doc(1)))
		require.NoError(t, tx.Commit())
		_, err = dbi.InspectIndexSketch(ctx, "a", "a")
		require.ErrorIs(t, err, ErrIndexNotFound, "a commit that did not use the collection stores no row for it")

		require.NoError(t, a.Insert(ctx, doc(2)))
		assert.False(t, idx.sketchModified)
		on, err := dbi.InspectIndexSketch(ctx, "a", "a")
		require.NoError(t, err, "the commit of a transaction that wrote to the collection stored the row")
		assert.EqualValues(t, 1, on.DocCount, "the document counted since the index had no statistics")

		require.NoError(t, b.Insert(ctx, doc(2)))
		assert.Equal(t, []string{"b"}, names(dbi.sketchDirty))
	})

	// The rebase finds no row for a dropped collection either; being closed
	// is what takes it off the list.
	t.Run("sketches: a collection dropped after a write leaves at the next begin", func(t *testing.T) {
		fx := newFixture(t)
		dbi := fx.DB.(*db)
		a := create(t, fx, "a", rangeIdx)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.Insert(tx.Context(), doc(1)))
		require.NoError(t, a.Drop(tx.Context()))
		require.NoError(t, tx.Commit())
		assert.Equal(t, []string{"a"}, names(dbi.sketchDirty))

		tx, err = fx.WriteTx(ctx)
		require.NoError(t, err)
		assert.Empty(t, names(dbi.sketchDirty))
		assert.False(t, a.sketchDirty)
		require.NoError(t, tx.Rollback())
	})

	t.Run("sketches: an update leaving every key as it was joins nothing", func(t *testing.T) {
		fx := newFixture(t)
		dbi := fx.DB.(*db)
		a := create(t, fx, "a", rangeIdx, IndexInfo{Fields: []string{"n"}})
		b := create(t, fx, "b", rangeIdx)
		require.NoError(t, a.Insert(ctx, doc(1)))
		require.NoError(t, b.Insert(ctx, doc(1)))

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		set := func(c *collection, mod string) {
			_, err := c.UpdateId(tx.Context(), 1, query.MustParseModifier(mod))
			require.NoError(t, err)
		}
		assert.Empty(t, names(dbi.sketchDirty))
		set(a, `{"$set":{"body":"other words"}}`)
		assert.Empty(t, names(dbi.sketchDirty), "no key of a changed")
		set(b, `{"$set":{"a":2}}`)
		assert.Equal(t, []string{"b"}, names(dbi.sketchDirty))
		set(a, `{"$set":{"n":1}}`)
		assert.Equal(t, []string{"b", "a"}, names(dbi.sketchDirty))
		var modified []string
		for _, idx := range a.loadIndexes() {
			if idx.sketchModified {
				modified = append(modified, idx.info.Name)
			}
		}
		assert.Equal(t, []string{"n"}, modified, "only the index whose key changed")
		set(a, `{"$set":{"body":"more words"}}`)
		assert.Equal(t, []string{"b", "a"}, names(dbi.sketchDirty))
		require.NoError(t, tx.Commit())
	})

	t.Run("fulltext: listed while postings are buffered", func(t *testing.T) {
		fx := newFixture(t)
		dbi := fx.DB.(*db)
		a, b := create(t, fx, "a", ftsIdx), create(t, fx, "b", rangeIdx)
		pending := &a.loadFtsIndexes()[0].pending

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.Insert(tx.Context(), doc(1)))
		assert.Equal(t, []string{"a"}, names(dbi.ftsDirty))
		assert.False(t, pending.empty())

		// The next verb's savepoint flushes first.
		require.NoError(t, b.Insert(tx.Context(), doc(1)))
		assert.Empty(t, names(dbi.ftsDirty))
		assert.False(t, a.ftsDirty)
		assert.True(t, pending.empty())

		require.NoError(t, a.Insert(tx.Context(), doc(2)))
		assert.Equal(t, []string{"a"}, names(dbi.ftsDirty))
		require.NoError(t, tx.Rollback())
		assert.Empty(t, names(dbi.ftsDirty), "the rollback discards the buffer")
		assert.False(t, a.ftsDirty)
		assert.True(t, pending.empty())

		require.NoError(t, a.Insert(ctx, doc(3)))
		assert.Empty(t, names(dbi.ftsDirty))

		n, err := a.Find(`{"$text":{"$search":"word3"}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		n, err = a.Find(`{"$text":{"$search":"word1 word2"}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n, "rolled-back postings must not surface")
	})

	t.Run("fulltext: a long tx keeps one entry", func(t *testing.T) {
		fx := newFixture(t)
		dbi := fx.DB.(*db)
		colls := make([]*collection, 8)
		for i := range colls {
			colls[i] = create(t, fx, fmt.Sprintf("c%d", i), ftsIdx, rangeIdx)
		}

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		for i, c := range colls {
			require.NoError(t, c.Insert(tx.Context(), doc(i)))
			assert.Equal(t, []string{c.cur().name}, names(dbi.ftsDirty))
		}
		assert.Equal(t, names(colls), names(dbi.sketchDirty))
		require.NoError(t, tx.Commit())
		assert.Empty(t, names(dbi.ftsDirty))

		for i, c := range colls {
			n, err := c.Find(fmt.Sprintf(`{"$text":{"$search":"word%d"}}`, i)).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
		}
	})

	// Reordering words rewrites postings without touching a vocab entry:
	// the postings alone must list the collection for the commit flush.
	t.Run("fulltext: an update that only reorders words is flushed", func(t *testing.T) {
		fx := newFixture(t)
		a := create(t, fx, "a", ftsIdx)
		require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(`{"id":1,"body":"alpha beta"}`)))
		require.NoError(t, a.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"body":"beta alpha"}`)))

		n, err := a.Find(`{"$text":{"$search":"\"beta alpha\""}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		n, err = a.Find(`{"$text":{"$search":"\"alpha beta\""}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
	})

	// A flush that fails leaves the collection listed, so the rollback still
	// finds and discards the postings the flush did not write.
	t.Run("fulltext: a failed flush keeps the collection listed", func(t *testing.T) {
		fx := newFixture(t)
		dbi := fx.DB.(*db)
		a := create(t, fx, "a", ftsIdx)
		require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(`{"id":1,"body":"term"}`)))
		nsPost := a.loadFtsIndexes()[0].nsPost
		// rewritePostings replaces every stored chunk with an undecodable
		// one, or deletes them all.
		rewritePostings := func(del bool) {
			require.NoError(t, dbi.doWriteTx(ctx, func(tx *btree.WriteTx) error {
				var keys [][]byte
				cur := tx.NewCursor(nsPost)
				defer cur.Close()
				if err := cur.First(); err != nil {
					return err
				}
				for cur.Valid() {
					k, err := cur.Key()
					if err != nil {
						return err
					}
					keys = append(keys, bytes.Clone(k))
					if err = cur.Next(); err != nil {
						return err
					}
				}
				for _, k := range keys {
					var err error
					if del {
						err = tx.Delete(nsPost, k)
					} else {
						err = tx.Put(nsPost, k, []byte{0xEE})
					}
					if err != nil {
						return err
					}
				}
				return nil
			}))
		}
		rewritePostings(false)

		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, a.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2,"body":"term ghostword"}`)))
		_, err = fx.CreateCollection(tx.Context(), "b")
		require.Error(t, err, "the verb's savepoint flush reads the undecodable chunk")
		assert.Equal(t, []string{"a"}, names(dbi.ftsDirty))
		require.NoError(t, tx.Rollback())
		assert.Empty(t, names(dbi.ftsDirty))

		rewritePostings(true)
		require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(`{"id":3,"body":"fresh"}`)))
		n, err := a.Find(`{"$text":{"$search":"ghostword"}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n, "postings of the rolled-back tx must not surface")
	})
}

// A handle a write transaction opens has its version installed before any
// other transaction can find the handle. The handle's sketch stamps speak
// for the sketches the writer loaded; a reader at an older snapshot that
// found the handle first would install the ones it loads, and the writer
// would put its count on them and store a row that misses a commit.
func TestOpenCollection_VersionIsInstalledBeforeTheHandleIsFound(t *testing.T) {
	d, _ := openSketchDB(t)
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":3}`, id))
	}
	old, err := d.ReadTx(ctx) // at 50 documents
	require.NoError(t, err)
	t.Cleanup(func() { _ = old.Commit() })
	h, err := d.OpenCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, h.Insert(ctx, doc(100)))
	require.NoError(t, h.Close()) // 51 documents, no handle

	wtx, err := d.WriteTx(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wtx.Rollback() })
	hooked := false
	testHookRegistered = func(name string) {
		if name != "c" || hooked {
			return
		}
		hooked = true
		o, err := d.OpenCollection(old.Context(), "c")
		require.NoError(t, err)
		n, err := o.Find(`{"a":3}`).Count(old.Context())
		require.NoError(t, err)
		require.Equal(t, 10, n, "setup: the older reader used the handle at its snapshot")
	}
	defer func() { testHookRegistered = nil }()
	coll, err := d.OpenCollection(wtx.Context(), "c")
	require.NoError(t, err)
	require.True(t, hooked, "setup: the write transaction made the handle")
	require.NoError(t, coll.Insert(wtx.Context(), doc(101)))
	require.NoError(t, wtx.Commit())
	require.NoError(t, old.Commit())

	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 52, n, "setup: the documents")
	require.Equal(t, uint64(52), storedSketch(t, d, coll).DocCount)
	require.Equal(t, uint64(52), sketchDocCount(t, coll))
}

// A handle a write transaction used stays the collection's one handle until
// the transaction ends: a Close() waits for that, and an open in between gets
// the same handle. So the documents written before the Close() commit with
// their counts, a drop through the reopened handle is the handle's own, and
// no second set of sketches comes to exist for the commit to store over the
// first. A handle that is closed because its collection is gone for the
// transaction stores nothing.
func TestWriteTx_HandleClosedBeforeCommitStoresItsSketches(t *testing.T) {
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":3}`, id))
	}
	reopened := func(t *testing.T, d DB, want int) {
		t.Helper()
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		require.Equal(t, uint64(want), storedSketch(t, d, coll).DocCount, "the stored row")
		docs, sketch := statsDocCounts(t, ctx, coll)
		require.Equal(t, want, docs, "setup: the documents")
		require.Equal(t, uint64(want), sketch)
		require.Equal(t, uint64(want), sketchDocCount(t, coll), "the sketch the next handle loads")
		require.NoError(t, coll.Insert(ctx, doc(900)))
		require.Equal(t, uint64(want+1), storedSketch(t, d, coll).DocCount, "the next write counts on")
	}

	t.Run("commit", func(t *testing.T) {
		d, _ := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		require.NoError(t, coll.Insert(wtx.Context(), doc(100), doc(101)))
		require.NoError(t, coll.Close())
		require.NoError(t, wtx.Commit())
		require.ErrorIs(t, coll.Insert(ctx, doc(102)), ErrCollectionClosed, "the Close() took effect at the commit")
		reopened(t, d, 52)
	})

	// written returns the handle of "c" a write transaction wrote one
	// document through and then closed.
	written := func(t *testing.T, d DB) (WriteTx, Collection) {
		t.Helper()
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		require.NoError(t, coll.Insert(wtx.Context(), doc(100)))
		require.NoError(t, coll.Close())
		return wtx, coll
	}

	t.Run("dropped through the next open", func(t *testing.T) {
		d, _ := openSketchDB(t)
		wtx, _ := written(t, d)
		again, err := d.OpenCollection(wtx.Context(), "c")
		require.NoError(t, err)
		require.NoError(t, again.Drop(wtx.Context()))
		require.NoError(t, wtx.Commit())
		_, err = d.(*db).InspectIndexSketch(ctx, "c", "a")
		require.ErrorIs(t, err, ErrIndexNotFound, "no row for a dropped collection")
	})

	t.Run("dropped and created again", func(t *testing.T) {
		d, _ := openSketchDB(t)
		wtx, _ := written(t, d)
		again, err := d.OpenCollection(wtx.Context(), "c")
		require.NoError(t, err)
		require.NoError(t, again.Drop(wtx.Context()))
		made, err := d.CreateCollection(wtx.Context(), "c")
		require.NoError(t, err)
		require.NoError(t, made.EnsureIndex(wtx.Context(), IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, wtx.Commit())
		n, err := made.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 0, n, "setup: the new collection is empty")
		require.Equal(t, uint64(0), storedSketch(t, d, made).DocCount)
	})

	t.Run("opened again, written after the commit", func(t *testing.T) {
		d, _ := openSketchDB(t)
		wtx, coll := written(t, d)
		again, err := d.OpenCollection(wtx.Context(), "c")
		require.NoError(t, err)
		if coll != again {
			t.Error("the transaction has a second handle of the collection")
		}
		require.NoError(t, wtx.Commit())
		require.Equal(t, uint64(51), storedSketch(t, d, again).DocCount)
		require.NoError(t, again.Insert(ctx, doc(101)))
		require.Equal(t, uint64(52), storedSketch(t, d, again).DocCount, "the next write counts on")
	})

	t.Run("opened again and written in the transaction", func(t *testing.T) {
		d, _ := openSketchDB(t)
		wtx, _ := written(t, d)
		again, err := d.OpenCollection(wtx.Context(), "c")
		require.NoError(t, err)
		require.NoError(t, again.Insert(wtx.Context(), doc(101)))
		require.NoError(t, wtx.Commit())
		require.Equal(t, uint64(52), storedSketch(t, d, again).DocCount)
		require.NoError(t, again.Insert(ctx, doc(102)))
		require.Equal(t, uint64(53), storedSketch(t, d, again).DocCount, "the next write counts on")
	})

	// The transaction's open made the handle (nothing had the collection
	// open) and loaded its sketches: it is the transaction's one handle from
	// the registration on, with no write through it yet.
	t.Run("opened by the transaction and closed before any write", func(t *testing.T) {
		d, _ := openSketchDB(t)
		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		coll, err := d.OpenCollection(wtx.Context(), "c")
		require.NoError(t, err)
		require.NoError(t, coll.Close())
		again, err := d.OpenCollection(wtx.Context(), "c")
		require.NoError(t, err)
		if coll != again {
			t.Error("the transaction has a second handle of the collection")
		}
		require.NoError(t, coll.Insert(wtx.Context(), doc(100)), "the handle the transaction opened first")
		require.NoError(t, wtx.Commit())
		require.Equal(t, uint64(51), storedSketch(t, d, again).DocCount)
		require.NoError(t, again.Insert(ctx, doc(101)), "the second open cancelled the Close()")
		require.Equal(t, uint64(52), storedSketch(t, d, again).DocCount, "the next write counts on")
	})

	// The same Close() with no open after it takes effect when the
	// transaction ends.
	t.Run("opened by the transaction, closed, written and committed", func(t *testing.T) {
		d, _ := openSketchDB(t)
		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		coll, err := d.OpenCollection(wtx.Context(), "c")
		require.NoError(t, err)
		require.NoError(t, coll.Close())
		require.NoError(t, coll.Insert(wtx.Context(), doc(100)), "the Close() waits for the transaction")
		require.NoError(t, wtx.Commit())
		require.ErrorIs(t, coll.Insert(ctx, doc(101)), ErrCollectionClosed, "the Close() took effect at the commit")
		reopened(t, d, 51)
	})

	// The transaction first used the handle inside a savepoint that rolled
	// back: the write after it holds the handle all the same.
	t.Run("first used in a savepoint that rolled back", func(t *testing.T) {
		d, _ := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		sp, err := d.WriteTx(wtx.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(sp.Context(), doc(99)))
		require.NoError(t, sp.Rollback())
		require.NoError(t, coll.Insert(wtx.Context(), doc(100)))
		require.NoError(t, coll.Close())
		again, err := d.OpenCollection(wtx.Context(), "c")
		require.NoError(t, err)
		if coll != again {
			t.Error("the transaction has a second handle of the collection")
		}
		require.NoError(t, again.Insert(wtx.Context(), doc(101)))
		require.NoError(t, wtx.Commit())
		require.Equal(t, uint64(52), storedSketch(t, d, again).DocCount)
	})

	// The transaction first used the handle inside a savepoint and closed
	// it there. The savepoint's commit hands the pin to the transaction:
	// the handle works until the transaction ends.
	t.Run("first used and closed in a savepoint that committed", func(t *testing.T) {
		d, _ := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		sp, err := d.WriteTx(wtx.Context())
		require.NoError(t, err)
		require.NoError(t, coll.Insert(sp.Context(), doc(100)))
		require.NoError(t, coll.Close())
		require.NoError(t, sp.Commit())
		require.NoError(t, coll.Insert(wtx.Context(), doc(101)), "the Close() waits for the transaction")
		require.NoError(t, wtx.Commit())
		require.ErrorIs(t, coll.Insert(ctx, doc(102)), ErrCollectionClosed, "the Close() took effect at the commit")
		reopened(t, d, 52)
	})

	// The transaction opened the collection inside a savepoint, wrote and
	// closed the handle there, and the savepoint rolled back: the use is
	// undone, and the Close() takes effect with it. The next open makes a
	// new handle, which loads the stored row.
	t.Run("opened, written and closed in a savepoint that rolled back", func(t *testing.T) {
		d, _ := openSketchDB(t)
		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		sp, err := d.WriteTx(wtx.Context())
		require.NoError(t, err)
		coll, err := d.OpenCollection(sp.Context(), "c")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(sp.Context(), doc(100)))
		require.NoError(t, coll.Close())
		require.NoError(t, sp.Rollback())
		require.ErrorIs(t, coll.Insert(wtx.Context(), doc(101)), ErrCollectionClosed, "the Close() took effect at the rollback")
		again, err := d.OpenCollection(wtx.Context(), "c")
		require.NoError(t, err)
		if again == coll {
			t.Error("the open returned the closed handle")
		}
		require.NoError(t, again.Insert(wtx.Context(), doc(101)))
		require.NoError(t, wtx.Commit())
		reopened(t, d, 51)
	})

	t.Run("rollback", func(t *testing.T) {
		d, _ := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		require.NoError(t, coll.Insert(wtx.Context(), doc(100), doc(101)))
		require.NoError(t, coll.Close())
		require.NoError(t, wtx.Rollback())
		reopened(t, d, 50)
	})

	// The collection was made inside a savepoint that rolled back: its
	// handle is closed, and no row is stored for a collection that does not
	// exist, for one of that name to load later.
	t.Run("a collection whose creation rolled back", func(t *testing.T) {
		d, _ := openSketchDB(t)
		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wtx.Rollback() })
		sp, err := d.WriteTx(wtx.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = sp.Rollback() })
		made, err := d.CreateCollection(sp.Context(), "made")
		require.NoError(t, err)
		require.NoError(t, made.EnsureIndex(sp.Context(), IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, made.Insert(sp.Context(), doc(1)))
		require.NoError(t, sp.Rollback())
		c, err := d.OpenCollection(wtx.Context(), "c")
		require.NoError(t, err)
		require.NoError(t, c.Insert(wtx.Context(), doc(100)))
		require.NoError(t, wtx.Commit())
		_, err = d.(*db).InspectIndexSketch(ctx, "made", "a")
		require.ErrorIs(t, err, ErrIndexNotFound)
		require.Equal(t, uint64(51), storedSketch(t, d, c).DocCount)
	})
}

// An open that found a handle whose Close() waits for the write transaction
// never returns that handle closed: when the transaction ends between the
// open and the hand-out, and the close takes effect, the open is made again.
func TestOpenCollection_HandOutAfterTheDeferredCloseTookEffect(t *testing.T) {
	for _, open := range []struct {
		name string
		fn   func(d DB) (Collection, error)
	}{
		{"OpenCollection", func(d DB) (Collection, error) { return d.OpenCollection(ctx, "c") }},
		{"Collection", func(d DB) (Collection, error) { return d.Collection(ctx, "c") }},
	} {
		t.Run(open.name, func(t *testing.T) {
			d, _ := openSketchDB(t)
			coll, err := d.OpenCollection(ctx, "c")
			require.NoError(t, err)
			wtx, err := d.WriteTx(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wtx.Rollback() })
			require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":100,"a":3}`)))
			require.NoError(t, coll.Close())

			// The opener found the handle and stops before the hand-out;
			// the writer commits there.
			found, committed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			testHookBeforeHandOut = func(*collection) {
				once.Do(func() {
					close(found)
					<-committed
				})
			}
			t.Cleanup(func() { testHookBeforeHandOut = nil })
			type result struct {
				coll Collection
				err  error
			}
			res := make(chan result, 1)
			go func() {
				c, err := open.fn(d)
				res <- result{c, err}
			}()
			<-found
			require.NoError(t, wtx.Commit())
			require.ErrorIs(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":101,"a":3}`)), ErrCollectionClosed,
				"setup: the Close() took effect at the commit")
			close(committed)
			r := <-res
			require.NoError(t, r.err)
			if r.coll == coll {
				t.Error("the open returned the handle the commit closed")
			}
			n, err := r.coll.Count(ctx)
			require.NoError(t, err, "the handle an open returns is open")
			require.Equal(t, 51, n)
		})
	}
}

// An open whose every attempt finds its handle closed before it can hand it
// out returns ErrCollectionClosedDuringOpen, the error of that race, not a
// catalog miss: the collection is there throughout, the error does not match
// ErrCollectionNotFound, and Collection creates nothing. Each attempt's
// handle is closed at the hand-out, or, for an open that finds a registered
// handle, while it resolves the handle. Both public opens, without a
// transaction and in a read transaction.
func TestOpenCollection_HandleClosedAtEveryAttempt(t *testing.T) {
	opens := []struct {
		name string
		fn   func(d DB, ctx context.Context) (Collection, error)
	}{
		{"OpenCollection", func(d DB, ctx context.Context) (Collection, error) { return d.OpenCollection(ctx, "c") }},
		{"Collection", func(d DB, ctx context.Context) (Collection, error) { return d.Collection(ctx, "c") }},
	}
	txs := []struct {
		name string
		ctx  func(t *testing.T, d DB) context.Context
	}{
		{"no transaction", func(*testing.T, DB) context.Context { return ctx }},
		{"read transaction", func(t *testing.T, d DB) context.Context {
			rtx, err := d.ReadTx(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = rtx.Commit() })
			return rtx.Context()
		}},
	}
	for _, open := range opens {
		for _, tx := range txs {
			t.Run(open.name+" at the hand-out, "+tx.name, func(t *testing.T) {
				d, _ := openSketchDB(t)
				octx := tx.ctx(t, d)
				attempts := 0
				testHookBeforeHandOut = func(c *collection) {
					attempts++
					require.NoError(t, c.Close())
				}
				t.Cleanup(func() { testHookBeforeHandOut = nil })
				_, err := open.fn(d, octx)
				require.ErrorIs(t, err, ErrCollectionClosedDuringOpen)
				require.False(t, errors.Is(err, ErrCollectionNotFound), "the collection is there: not a catalog miss")
				require.Equal(t, handOutAttempts, attempts, "setup: every attempt found a handle")
				testHookBeforeHandOut = nil
				coll, err := open.fn(d, octx)
				require.NoError(t, err, "setup: the collection is there")
				n, err := coll.Count(octx)
				require.NoError(t, err)
				require.Equal(t, 50, n)
			})

			t.Run(open.name+" while it resolves a registered handle, "+tx.name, func(t *testing.T) {
				d, _ := openSketchDB(t)
				_, err := d.OpenCollection(ctx, "c")
				require.NoError(t, err)
				octx := tx.ctx(t, d)
				// Each attempt finds a registered handle, which is closed
				// before the attempt resolves it; another open registers the
				// next one.
				attempts := 0
				testHookFoundRegistered = func(c *collection) {
					attempts++
					require.NoError(t, c.Close())
					_, err := d.OpenCollection(ctx, "c")
					require.NoError(t, err)
				}
				t.Cleanup(func() { testHookFoundRegistered = nil })
				_, err = open.fn(d, octx)
				require.ErrorIs(t, err, ErrCollectionClosedDuringOpen)
				require.False(t, errors.Is(err, ErrCollectionNotFound), "the collection is there: not a catalog miss")
				require.Equal(t, 3, attempts, "every attempt found a registered handle, and no more were made")
				testHookFoundRegistered = nil
				coll, err := open.fn(d, octx)
				require.NoError(t, err, "setup: the collection is there")
				n, err := coll.Count(octx)
				require.NoError(t, err)
				require.Equal(t, 50, n)
			})
		}
	}
}

// A lookup by name in the write transaction's schema log goes by its schema
// changes, whatever handles the transaction uses besides: it looks at no pin.
// It follows a savepoint that rolls a change back.
func TestWriteTx_NameLookupAmongPinnedHandles(t *testing.T) {
	fx := newFixture(t)
	for i := range 8 {
		_, err := fx.CreateCollection(ctx, fmt.Sprintf("p%d", i))
		require.NoError(t, err)
	}
	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wtx.Rollback() })
	log := wtx.schemaLog()
	use := func(from, to int) {
		for i := from; i < to; i++ {
			c, err := fx.OpenCollection(wtx.Context(), fmt.Sprintf("p%d", i))
			require.NoError(t, err)
			require.NoError(t, c.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":1}`)))
		}
	}
	// visits returns the log entries byName looks at for name.
	visits := func(name string) int {
		v := &visitCounts{}
		testVisits = v
		defer func() { testVisits = nil }()
		log.byName(name)
		return v.byName
	}
	use(0, 3)
	require.Len(t, log.log, 3, "setup: one pin per handle used")
	require.Empty(t, log.changes, "a pin is not a schema change")
	require.Nil(t, log.byName("p0"), "a used handle is found in the registry, not in the log")
	require.Zero(t, visits("p0"), "a log of pins")

	kept, err := fx.CreateCollection(wtx.Context(), "kept")
	require.NoError(t, err)
	use(3, 6)
	sp, err := fx.WriteTx(wtx.Context())
	require.NoError(t, err)
	_, err = fx.CreateCollection(sp.Context(), "undone")
	require.NoError(t, err)
	p6, err := fx.OpenCollection(sp.Context(), "p6")
	require.NoError(t, err)
	require.NoError(t, p6.Rename(sp.Context(), "p6r"))
	require.Equal(t, []int{3, 7, 9}, log.changes,
		"the created collection, the one made in the savepoint, and the rename of p6 after its pin")
	require.NotNil(t, log.byName("undone"))
	require.NotNil(t, log.byName("p6r"))
	require.NoError(t, sp.Rollback())

	require.Equal(t, []int{3}, log.changes, "the savepoint's changes left the index with its entries")
	require.Nil(t, log.byName("undone"))
	require.Nil(t, log.byName("p6r"))
	_, err = fx.OpenCollection(wtx.Context(), "undone")
	require.ErrorIs(t, err, ErrCollectionNotFound)
	use(6, 8)
	require.Len(t, log.log, 9, "setup: eight pins and the creation")
	require.Equal(t, 1, visits("kept"), "the one schema change among the pins")
	again, err := fx.OpenCollection(wtx.Context(), "kept")
	require.NoError(t, err)
	if again != kept {
		t.Error("the created collection is not found by name among the pins")
	}
	require.NoError(t, wtx.Commit())
	require.Empty(t, log.changes)
	require.Empty(t, log.log)
}

// A savepoint rollback walks the log entries its scope made, not the log: a
// verb that fails on a collection the transaction has used costs the same
// however many collections the transaction has used, and the handles keep
// the marks they had before the scope.
func TestWriteTx_SavepointRollbackWalksItsOwnEntries(t *testing.T) {
	fx := newFixture(t)
	const n = 64
	for i := range n + 1 {
		c, err := fx.CreateCollection(ctx, fmt.Sprintf("p%d", i))
		require.NoError(t, err)
		require.NoError(t, c.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
	}
	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wtx.Rollback() })
	log := wtx.schemaLog()
	colls := make([]Collection, n+1)
	for i := range colls {
		colls[i], err = fx.OpenCollection(ctx, fmt.Sprintf("p%d", i))
		require.NoError(t, err)
	}
	use := func(from, to int) {
		for i := from; i < to; i++ {
			_, err := colls[i].FindId(wtx.Context(), 1)
			require.NoError(t, err)
		}
	}
	// failOn makes an insert into c fail and returns the entries the
	// rollback of its savepoint walked.
	failOn := func(c Collection) int {
		v := &visitCounts{}
		testVisits = v
		defer func() { testVisits = nil }()
		require.ErrorIs(t, c.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":1}`)), ErrDocExists)
		return v.discard
	}

	use(0, 1)
	require.Len(t, log.log, 1, "setup: one pin per collection used")
	require.Equal(t, 0, failOn(colls[0]), "one collection used")
	use(1, n)
	require.Len(t, log.log, n, "setup: one pin per collection used")
	require.Equal(t, 0, failOn(colls[0]), "%d collections used", n)
	require.Len(t, log.log, n)
	require.Equal(t, 1, colls[0].(*collection).logLast, "the used handle keeps its pin")

	// The verb is the first use of the collection: the rollback walks its pin.
	require.Equal(t, 1, failOn(colls[n]), "a first use in the failing verb")
	require.Len(t, log.log, n)
	require.Equal(t, 0, colls[n].(*collection).logLast, "the pin went with the savepoint")

	// A handle with entries before the scope and in it gets back the mark
	// of its last entry before the scope.
	sp, err := fx.WriteTx(wtx.Context())
	require.NoError(t, err)
	require.NoError(t, colls[1].EnsureIndex(sp.Context(), IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, colls[1].EnsureIndex(sp.Context(), IndexInfo{Fields: []string{"b"}}))
	require.Equal(t, n+2, colls[1].(*collection).logLast, "setup: the scope's changes are the handle's last")
	require.NoError(t, sp.Rollback())
	require.Len(t, log.log, n)
	require.Equal(t, 2, colls[1].(*collection).logLast, "the handle's pin before the scope")
	require.NoError(t, wtx.Commit())
}

// A savepoint rollback that discards no log entry leaves every handle's mark
// as it is: it looks at no entry before the savepoint. One handle is given
// a mark no walk of the log writes, and a failing verb on another handle
// must leave it.
func TestWriteTx_SavepointRollbackLeavesTheMarksBeforeIt(t *testing.T) {
	fx := newFixture(t)
	colls := make([]*collection, 2)
	for i := range colls {
		c, err := fx.CreateCollection(ctx, fmt.Sprintf("p%d", i))
		require.NoError(t, err)
		require.NoError(t, c.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
		colls[i] = c.(*collection)
	}
	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wtx.Rollback() })
	log := wtx.schemaLog()
	for _, c := range colls {
		_, err := c.FindId(wtx.Context(), 1)
		require.NoError(t, err)
	}
	require.Len(t, log.log, 2, "setup: one pin per collection used")

	kept := colls[1].logLast
	const mark = 1 << 20
	colls[1].logLast = mark
	require.ErrorIs(t, colls[0].Insert(wtx.Context(), anyenc.MustParseJson(`{"id":1}`)), ErrDocExists)
	require.Len(t, log.log, 2, "setup: the rollback discarded no entry")
	require.Equal(t, mark, colls[1].logLast, "the rollback rewrote the mark of a handle it did not discard")
	colls[1].logLast = kept
	require.NoError(t, wtx.Commit())
}

// Handles a write transaction opened and then closed leave the registry when
// the transaction ends, with one eviction each. That an eviction looks only
// under the handle's name is TestDB_EvictionLooksOnlyUnderTheRegisteredName.
func TestWriteTx_ClosedHandlesLeaveTheRegistryByName(t *testing.T) {
	const n = 64
	for _, end := range []string{"commit", "rollback"} {
		t.Run(end, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "r.db")
			d, err := Open(ctx, path, nil)
			require.NoError(t, err)
			for i := range n {
				_, err = d.CreateCollection(ctx, fmt.Sprintf("p%d", i))
				require.NoError(t, err)
			}
			require.NoError(t, d.Close())
			d, err = Open(ctx, path, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = d.Close() })
			dbi := d.(*db)
			registered := func() int {
				dbi.mu.Lock()
				defer dbi.mu.Unlock()
				return len(dbi.openedCollections)
			}

			wtx, err := d.WriteTx(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wtx.Rollback() })
			colls := make([]Collection, n)
			for i := range colls {
				colls[i], err = d.OpenCollection(wtx.Context(), fmt.Sprintf("p%d", i))
				require.NoError(t, err)
				require.NoError(t, colls[i].Close())
			}
			require.Equal(t, n, registered(), "setup: the closes wait for the transaction")

			v := &visitCounts{}
			testVisits = v
			t.Cleanup(func() { testVisits = nil })
			if end == "commit" {
				require.NoError(t, wtx.Commit())
			} else {
				require.NoError(t, wtx.Rollback())
			}
			testVisits = nil
			require.Equal(t, n, v.evict, "one eviction per handle")
			require.Zero(t, registered())
			for _, c := range colls {
				_, err = c.Count(ctx)
				require.ErrorIs(t, err, ErrCollectionClosed)
			}
		})
	}
}

// An eviction deletes the registry slot of the name the handle was put in
// the registry under (collection.regName) and looks nowhere else, so it
// costs the same however many handles are registered. The registry is set
// up by hand: the closed handle is under another name only, where an
// eviction must leave it.
func TestDB_EvictionLooksOnlyUnderTheRegisteredName(t *testing.T) {
	fx := newFixture(t)
	c, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, c.Close())
	h := c.(*collection)
	dbi := fx.DB.(*db)
	dbi.mu.Lock()
	defer dbi.mu.Unlock()
	require.Equal(t, "c", h.regName, "setup: registered under c")
	require.NotContains(t, dbi.openedCollections, "c", "setup: Close() evicted the handle")
	dbi.openedCollections["decoy"] = h
	dbi.evictLocked(h)
	got, kept := dbi.openedCollections["decoy"]
	delete(dbi.openedCollections, "decoy")
	require.True(t, kept && got == Collection(h), "the eviction searched the registry for the handle")
}

// BenchmarkSketch_FindThenWriteAfterAPeersCommit is the cycle of a process
// that shares its file: another process commits, this one plans a query and
// then writes. The reader loads the stored sketch rows into the published
// sketches and the writer into the live ones, every time.
func BenchmarkSketch_FindThenWriteAfterAPeersCommit(b *testing.B) {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		b.Skip("models another process's commit; not applicable in-memory")
	}
	fx := newFixture(b)
	dbi := fx.DB.(*db)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(b, err)
	require.NoError(b, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := 0; i < 200; i++ {
		require.NoError(b, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))))
	}
	docs := [2]*anyenc.Value{anyenc.MustParseJson(`{"id":1,"a":1}`), anyenc.MustParseJson(`{"id":1,"a":2}`)}
	q := coll.Find(`{"a":3}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dbi.btreeDB.ForgetSeenHeader() // the newest commit is another process's
		if _, err := q.Count(ctx); err != nil {
			b.Fatal(err)
		}
		if err := coll.UpdateOne(ctx, docs[i&1]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSketch_RollbackThenFind is a write transaction that rolls back,
// then a query: the counts the transaction left on the live sketches of two
// indexes are there when the query plans, and the next transaction's begin
// rebases them.
func BenchmarkSketch_RollbackThenFind(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(b, err)
	require.NoError(b, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}, IndexInfo{Fields: []string{"b"}}))
	for i := 0; i < 200; i++ {
		require.NoError(b, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%5, i%7))))
	}
	doc := anyenc.MustParseJson(`{"id":1000,"a":3,"b":3}`)
	q := coll.Find(`{"a":3}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wtx, err := fx.WriteTx(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err = coll.Insert(wtx.Context(), doc); err != nil {
			b.Fatal(err)
		}
		if err = wtx.Rollback(); err != nil {
			b.Fatal(err)
		}
		if _, err = q.Count(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteTx_OpenCollections writes to one collection of a db holding n
// open ones: the cost of a write must not depend on n. TxAll is one
// transaction that opens every one of the n by name and writes to it, and
// TxAllFind one that reads from each instead: the cost of an open by name
// must not depend on the number of collections the transaction uses. Nor
// must the cost of a verb that fails after them (FailAfterAll).
func BenchmarkWriteTx_OpenCollections(b *testing.B) {
	for _, n := range []int{1, 1000, 20000} {
		b.Run(fmt.Sprintf("open=%d", n), func(b *testing.B) {
			fx := newFixture(b)
			tx, err := fx.WriteTx(ctx)
			require.NoError(b, err)
			colls := make([]Collection, n)
			for i := range colls {
				colls[i], err = fx.CreateCollection(tx.Context(), fmt.Sprintf("c%05d", i))
				require.NoError(b, err)
				require.NoError(b, colls[i].EnsureIndex(tx.Context(), IndexInfo{Fields: []string{"a"}}))
			}
			names := make([]string, n)
			for i := range colls {
				names[i] = fmt.Sprintf("c%05d", i)
				require.NoError(b, colls[i].Insert(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":0,"v":0}`)))
			}
			coll := colls[0]
			require.NoError(b, tx.Commit())

			inc := query.MustParseModifier(`{"$inc":{"v":1}}`)
			same := query.MustParseModifier(`{"$set":{"a":0}}`)

			b.Run("UpdateId", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					_, err := coll.UpdateId(ctx, 1, inc)
					require.NoError(b, err)
				}
			})
			// Nothing modified: the tx begins and commits without writing.
			b.Run("UpdateIdNoChange", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					_, err := coll.UpdateId(ctx, 1, same)
					require.NoError(b, err)
				}
			})
			// Ten verbs in one tx: each opens a savepoint.
			b.Run("Tx10", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					tx, err := fx.WriteTx(ctx)
					require.NoError(b, err)
					for range 10 {
						_, err = coll.UpdateId(tx.Context(), 1, inc)
						require.NoError(b, err)
					}
					require.NoError(b, tx.Commit())
				}
			})
			// One tx uses every collection: an open by name, then a write.
			// Reported per collection.
			b.Run("TxAll", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					tx, err := fx.WriteTx(ctx)
					require.NoError(b, err)
					for _, name := range names {
						c, err := fx.OpenCollection(tx.Context(), name)
						require.NoError(b, err)
						_, err = c.UpdateId(tx.Context(), 1, inc)
						require.NoError(b, err)
					}
					require.NoError(b, tx.Commit())
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/coll")
			})
			// The same with a lookup by id in place of the write: the tx
			// uses every collection and its commit writes nothing, so the
			// time is the opens and the lookups. Reported per collection.
			b.Run("TxAllFind", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					tx, err := fx.WriteTx(ctx)
					require.NoError(b, err)
					for _, name := range names {
						c, err := fx.OpenCollection(tx.Context(), name)
						require.NoError(b, err)
						_, err = c.FindId(tx.Context(), 1)
						require.NoError(b, err)
					}
					require.Len(b, tx.schemaLog().log, n, "every collection is pinned")
					require.NoError(b, tx.Commit())
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/coll")
			})
			// A verb that fails, in a tx that has used every collection: the
			// rollback of its savepoint must not cost by the collections
			// the tx has used.
			b.Run("FailAfterAll", func(b *testing.B) {
				tx, err := fx.WriteTx(ctx)
				require.NoError(b, err)
				defer func() { _ = tx.Rollback() }()
				for _, name := range names {
					c, err := fx.OpenCollection(tx.Context(), name)
					require.NoError(b, err)
					_, err = c.FindId(tx.Context(), 1)
					require.NoError(b, err)
				}
				dup := anyenc.MustParseJson(`{"id":1}`)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := coll.Insert(tx.Context(), dup); !errors.Is(err, ErrDocExists) {
						b.Fatal(err)
					}
				}
				// The deferred rollback releases every pin: not the verb.
				b.StopTimer()
			})
		})
	}
}

// BenchmarkRead_DuringSchemaCommits reads one collection while another
// goroutine keeps creating and dropping others: a schema commit that does not
// touch a collection must not slow the reads of it.
func BenchmarkRead_DuringSchemaCommits(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "hot")
	require.NoError(b, err)
	require.NoError(b, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(b, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":0}`)))

	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			c, cErr := fx.CreateCollection(ctx, fmt.Sprintf("n%07d", i))
			if cErr == nil {
				cErr = c.Drop(ctx)
			}
			if cErr != nil {
				done <- cErr
				return
			}
		}
	}()

	b.Run("FindId", func(b *testing.B) {
		p := &anyenc.Parser{}
		b.ReportAllocs()
		for range b.N {
			_, err := coll.FindIdWithParser(ctx, p, 1)
			require.NoError(b, err)
		}
	})
	b.Run("Count", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, err := coll.Count(ctx)
			require.NoError(b, err)
		}
	})
	b.Run("FindByIndex", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, err := coll.Find(`{"a":0}`).Count(ctx)
			require.NoError(b, err)
		}
	})
	close(stop)
	require.NoError(b, <-done)
}

// BenchmarkPeerSchemaChange_OpenCollections reads one collection right after
// another process changed the schema, with n collections open: the cost of
// noticing the change must not depend on n.
func BenchmarkPeerSchemaChange_OpenCollections(b *testing.B) {
	skipIfInMemory(b, "staleness counters model cross-process commits; not applicable in-memory")
	for _, n := range []int{1, 1000, 20000} {
		b.Run(fmt.Sprintf("open=%d", n), func(b *testing.B) {
			fx := newFixture(b)
			dbi := fx.DB.(*db)
			tx, err := fx.WriteTx(ctx)
			require.NoError(b, err)
			colls := make([]Collection, n)
			for i := range colls {
				colls[i], err = fx.CreateCollection(tx.Context(), fmt.Sprintf("c%05d", i))
				require.NoError(b, err)
				require.NoError(b, colls[i].EnsureIndex(tx.Context(), IndexInfo{Fields: []string{"a"}}))
			}
			coll := colls[0]
			require.NoError(b, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":0}`)))
			require.NoError(b, tx.Commit())

			rtx, err := dbi.btreeDB.BeginRead()
			require.NoError(b, err)
			sc := rtx.DiskSchemaCookie()
			require.NoError(b, rtx.Rollback())

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				modelPeerSchemaChange(dbi, sc)
				_, err := coll.Count(ctx)
				require.NoError(b, err)
			}
		})
	}
}

// modelPeerSchemaChange leaves the process as it is after another process
// committed the schema change that produced the cookie sc, unnoticed so far:
// the epoch is one schema commit behind.
func modelPeerSchemaChange(d *db, sc uint32) {
	e := d.epoch.Load()
	d.epoch.Store(&schemaEpoch{gen: e.gen, start: min(e.start, sc-1), known: sc - 1})
}

// peerSchemaChangeNow models that the newest committed state came from
// another process (see modelPeerSchemaChange).
func peerSchemaChangeNow(t testing.TB, d *db) {
	t.Helper()
	rtx, err := d.btreeDB.BeginRead()
	require.NoError(t, err)
	sc := rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())
	modelPeerSchemaChange(d, sc)
}

// setHead replaces the head of c with a copy changed by edit, as proven as
// the one it replaces: for tests that put a handle in a state no code path
// leaves it in.
func setHead(c *collection, edit func(s *collSchema)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev := c.cur()
	next := prev.clone()
	edit(next)
	next.gen.Store(prev.gen.Load())
	c.head.Store(next)
}

// A collection created in a transaction has no registry entry until the
// commit: a closed handle left in the slot — one being retired, closed
// already, evicted next — is neither replaced by the create nor put back by
// its rollback.
func TestCreateCollectionRollback_RestoresOnlyADroppedHandle(t *testing.T) {
	fx := newFixture(t)
	d := fx.DB.(*db)
	registered := func(name string) bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		_, ok := d.openedCollections[name]
		return ok
	}

	retiring, err := fx.CreateCollection(ctx, "x")
	require.NoError(t, err)
	require.NoError(t, retiring.Drop(ctx))
	require.False(t, registered("x"))
	d.mu.Lock()
	d.openedCollections["x"] = retiring
	d.mu.Unlock()

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	created, err := fx.CreateCollection(tx.Context(), "x")
	require.NoError(t, err)
	d.mu.Lock()
	assert.True(t, d.openedCollections["x"] == retiring, "the create registers nothing before the commit")
	d.mu.Unlock()
	require.NoError(t, tx.Rollback())
	d.mu.Lock()
	assert.True(t, d.openedCollections["x"] == retiring)
	d.mu.Unlock()
	_, err = created.Count(ctx)
	assert.ErrorIs(t, err, ErrCollectionClosed, "the rolled-back collection's handle is closed")
	d.mu.Lock()
	delete(d.openedCollections, "x")
	d.mu.Unlock()
	assert.False(t, registered("x"))
}

// peerRenameCollection renames the collection with a raw btree transaction,
// which the schema state knows nothing about: what another process's Rename
// leaves for this one to find.
func peerRenameCollection(t *testing.T, d *db, from, to string) {
	t.Helper()
	tx, err := d.btreeDB.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, d.renameCollection(tx, from, to))
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())
}

// The sketch passes of a write transaction — the rebase at its begin, the
// persist at its commit — act only on heads verified for the present: a
// handle whose collection another process renamed since the deltas were
// made keeps its flags until a transaction verifies it, and no sketch row
// is written under a name the catalog no longer has.
func TestSketchPasses_SkipUnverifiedHead(t *testing.T) {
	skipIfInMemory(t, "models another process's commit; not applicable in-memory")
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	a, err := fx.CreateCollection(ctx, "a")
	require.NoError(t, err)
	require.NoError(t, a.EnsureIndex(ctx, IndexInfo{Name: "k", Fields: []string{"k"}}))
	x, err := fx.CreateCollection(ctx, "x")
	require.NoError(t, err)
	require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(`{"id":1,"k":1}`)))

	// Deltas left by a rollback: the head's index stays flagged and a stays
	// listed for the next begin.
	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, a.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":2,"k":2}`)))
	require.NoError(t, wtx.Rollback())
	idx := a.(*collection).cur().indexes[0]
	require.True(t, idx.sketchModified)

	peerRenameCollection(t, dbi, "a", "b")
	require.NoError(t, x.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	_, err = rtx.Get(dbi.systemNS, sketchKey("a", "k"))
	assert.ErrorIs(t, err, btree.ErrKeyNotFound, "a sketch row written under the name the other process took away")
	_, err = rtx.Get(dbi.systemNS, sketchKey("b", "k"))
	assert.NoError(t, err)
	require.NoError(t, rtx.Rollback())
	assert.True(t, idx.sketchModified, "the deltas were dropped without a rebase")

	// The handle retires on use; the collection goes on under its new name.
	_, err = a.Count(ctx)
	assert.ErrorIs(t, err, ErrCollectionClosed)
	b, err := fx.OpenCollection(ctx, "b")
	require.NoError(t, err)
	n, err := b.Find(`{"k":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

// A dirty open whose catalog's root page number is damaged to 0 is a quick
// check that met damage: ErrQuickCheckFailed and ErrCorrupt, as before, and
// not an incomplete check.
func TestOpen_DirtyQuickCheckDamagedCatalogRoot(t *testing.T) {
	ctx := context.Background()
	cfg := func() *Config {
		// The page-1 edit below breaks its checksum; the open goes past it.
		return &Config{Durability: DurabilityConfig{Sentinel: true}, ContinueOnIntegrityError: true}
	}
	path := filepath.Join(t.TempDir(), "a.db")
	db, err := Open(ctx, path, cfg())
	require.NoError(t, err)
	_, err = db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// The catalog's master cell: key length, value length, key, then the
	// 4-byte root page number (parseLeafCellWithSize), on page 1.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	cell := append([]byte{byte(len(systemNamespace)), 4}, systemNamespace...)
	page1 := data[:4096]
	require.Equal(t, 1, bytes.Count(page1, cell))
	at := bytes.Index(page1, cell) + len(cell)
	require.NotZero(t, binary.BigEndian.Uint32(data[at:]))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0, 0, 0, 0}, int64(at))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	marker := path + ".lock"
	require.NoError(t, os.WriteFile(marker, nil, 0o644))

	_, err = Open(ctx, path, cfg())
	require.ErrorIs(t, err, ErrQuickCheckFailed)
	require.ErrorIs(t, err, ErrCorrupt)
	_, err = os.Stat(marker)
	require.NoError(t, err)
}

// The quick check of a dirty open fails the open as ErrQuickCheckFailed,
// and so as ErrCorrupt, only when it met damage. A check that did not
// finish returns what stopped it, and that is not corruption: the caller
// must not discard a file over a cancelled context or an I/O error. In
// every case the sentinel stays and the next open checks again.
func TestOpen_DirtyQuickCheckErrors(t *testing.T) {
	ctx := context.Background()
	cfg := func() *Config { return &Config{Durability: DurabilityConfig{Sentinel: true}} }
	path := filepath.Join(t.TempDir(), "a.db")
	db, err := Open(ctx, path, cfg())
	require.NoError(t, err)
	_, err = db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	// The marker a process that died after a write leaves behind.
	marker := path + ".lock"
	require.NoError(t, os.WriteFile(marker, nil, 0o644))
	t.Cleanup(func() { testQuickCheckReadHook = nil })

	open := func(ctx context.Context, injected error) error {
		testQuickCheckReadHook = nil
		if injected != nil {
			testQuickCheckReadHook = func() error { return injected }
		}
		db, err := Open(ctx, path, cfg())
		testQuickCheckReadHook = nil
		if err == nil {
			require.NoError(t, db.Close())
		}
		return err
	}
	stillDirty := func() {
		t.Helper()
		_, err := os.Stat(marker)
		require.NoError(t, err, "a failed quick check cleared the sentinel")
	}

	t.Run("damage", func(t *testing.T) {
		damage := fmt.Errorf("page 7: %w", ErrPageIntegrity)
		err := open(ctx, damage)
		require.ErrorIs(t, err, ErrQuickCheckFailed)
		require.ErrorIs(t, err, ErrCorrupt)
		require.ErrorIs(t, err, ErrPageIntegrity)
		stillDirty()
	})
	t.Run("io error", func(t *testing.T) {
		eio := errors.New("input/output error")
		err := open(ctx, &os.PathError{Op: "read", Path: path, Err: eio})
		require.ErrorIs(t, err, eio)
		require.NotErrorIs(t, err, ErrQuickCheckFailed)
		require.NotErrorIs(t, err, ErrCorrupt)
		stillDirty()
	})
	t.Run("cancelled in the check", func(t *testing.T) {
		err := open(ctx, context.Canceled)
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, ErrCorrupt)
		stillDirty()
	})
	t.Run("deadline", func(t *testing.T) {
		err := open(ctx, context.DeadlineExceeded)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, ErrCorrupt)
		stillDirty()
	})
	t.Run("busy", func(t *testing.T) {
		err := open(ctx, btree.ErrBusySnapshot)
		require.ErrorIs(t, err, ErrBusy)
		require.NotErrorIs(t, err, ErrCorrupt)
		stillDirty()
	})
	// The read does not consult the ctx once it has begun: a read that
	// succeeds after the ctx ended is a check that did not finish in time.
	t.Run("cancelled during a read that succeeds", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		testQuickCheckReadHook = func() error { cancel(); return nil }
		db, err := Open(cctx, path, cfg())
		testQuickCheckReadHook = nil
		if err == nil {
			require.NoError(t, db.Close())
		}
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, ErrCorrupt)
		stillDirty()
	})
	t.Run("deadline passed during a read that succeeds", func(t *testing.T) {
		dctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		testQuickCheckReadHook = func() error { <-dctx.Done(); return nil }
		db, err := Open(dctx, path, cfg())
		testQuickCheckReadHook = nil
		if err == nil {
			require.NoError(t, db.Close())
		}
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, ErrCorrupt)
		stillDirty()
	})
	t.Run("clean", func(t *testing.T) {
		db, err := Open(ctx, path, cfg())
		require.NoError(t, err)
		stats, err := db.Stats(ctx)
		require.NoError(t, err)
		require.True(t, stats.DirtyOnOpen)
		require.NoError(t, db.Close())
	})
}

// Backup refuses a destination that holds a database of another schema
// format with an exported error, and leaves it as it was. Another page
// layout: test/backup_roundtrip_test.go.
func TestBackup_DestinationOfAnotherKind(t *testing.T) {
	ctx := context.Background()
	newDB := func(t *testing.T, path string, conf *Config) {
		t.Helper()
		db, err := Open(ctx, path, conf)
		require.NoError(t, err)
		_, err = db.CreateCollection(ctx, "dst")
		require.NoError(t, err)
		require.NoError(t, db.Close())
	}
	backupRefused := func(t *testing.T, src DB, path string, want error) {
		t.Helper()
		before, err := os.ReadFile(path)
		require.NoError(t, err)
		err = src.Backup(ctx, path)
		require.ErrorIs(t, err, want)
		require.NotErrorIs(t, err, ErrCorrupt)
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, before, after)
	}

	t.Run("another schema format", func(t *testing.T) {
		dir := t.TempDir()
		src, err := Open(ctx, filepath.Join(dir, "src.db"), nil)
		require.NoError(t, err)
		defer src.Close()
		path := filepath.Join(dir, "dst.db")
		createFormat(t, os.Getenv("ANYSTORE_TEST_LEGACY_FORMAT") != "1")
		newDB(t, path, nil)
		backupRefused(t, src, path, ErrBackupFormatMismatch)
	})
}

// IntegrityCheck's catalog check: a collection in the catalog with no data
// namespace is damage, and the error matches ErrCorrupt, as the b-tree
// check's findings do.
func TestIntegrityCheck_MissingDataNamespaceMatchesErrCorrupt(t *testing.T) {
	ctx := context.Background()
	d, err := Open(ctx, filepath.Join(t.TempDir(), "a.db"), nil)
	require.NoError(t, err)
	defer d.Close()
	coll, err := d.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
	require.NoError(t, d.IntegrityCheck(ctx))

	// The collection's data goes; its catalog entry stays.
	wtx, err := d.(*db).btreeDB.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, wtx.DeleteNamespace("c"))
	require.NoError(t, wtx.Commit())

	err = d.IntegrityCheck(ctx)
	require.ErrorIs(t, err, ErrCorrupt)
	require.Contains(t, err.Error(), "data namespace missing")
}

// mmapShm reports whether the wal-index is shared memory other processes
// map (linux, android, darwin and ios on amd64 and arm64): only there can a
// test tear its header, as a writer between its two copies leaves it.
func mmapShm() bool {
	switch runtime.GOOS {
	case "linux", "android", "darwin", "ios":
		return runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64"
	}
	return false
}

// A busy IntegrityCheck matches ErrBusy, as every busy error of the package
// does, and keeps the btree's busy error: here a rebuild of the wal-index
// that another connection holds past the busy timeout.
func TestIntegrityCheck_BusyMatchesErrBusy(t *testing.T) {
	if !mmapShm() {
		t.Skip("the wal-index is this process's memory: no other connection rebuilds it")
	}
	ctx := context.Background()
	d, err := Open(ctx, filepath.Join(t.TempDir(), "a.db"), nil)
	require.NoError(t, err)
	defer d.Close()
	_, err = d.CreateCollection(ctx, "c")
	require.NoError(t, err)

	seam := d.(*db).btreeDB.TestSeam()
	seam.SetBusyTimeout(100 * time.Millisecond)
	restore := seam.TearHeader()
	unlockWrite, err := seam.Lock("write")
	require.NoError(t, err)
	unlockRecover, err := seam.Lock("recover")
	require.NoError(t, err)
	err = d.IntegrityCheck(ctx)
	restore()
	unlockRecover()
	unlockWrite()
	require.ErrorIs(t, err, ErrBusy)
	require.ErrorIs(t, err, btree.ErrBusyRecovery)
	require.NotErrorIs(t, err, ErrCorrupt)
	require.NoError(t, d.IntegrityCheck(ctx))
}

// A busy error of the initialization Open runs once the btree has opened
// (here the write begin of init, its snapshot overtaken by another
// process's commit at every attempt) matches ErrBusy and keeps the btree's
// error.
func TestOpen_BusyInitializationMatchesErrBusy(t *testing.T) {
	if !mmapShm() {
		t.Skip("the wal-index is this process's memory: no other process commits")
	}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "a.db")
	d, err := Open(ctx, path, nil)
	require.NoError(t, err)
	require.NoError(t, d.Close())

	testHookAfterBtreeOpen = func(bdb *btree.DB) {
		bdb.TestSeam().SetBusyTimeout(100 * time.Millisecond)
		btree.SetTestBeforeBeginWriteHook(func(s btree.TestSeam) {
			if s.Path() == bdb.TestSeam().Path() {
				s.PeerPublish()
			}
		})
	}
	defer func() {
		testHookAfterBtreeOpen = nil
		btree.SetTestBeforeBeginWriteHook(nil)
	}()
	_, err = Open(ctx, path, nil)
	require.ErrorIs(t, err, ErrBusy)
	require.ErrorIs(t, err, btree.ErrBusySnapshot)
	require.NotErrorIs(t, err, ErrCorrupt)

	testHookAfterBtreeOpen = nil
	btree.SetTestBeforeBeginWriteHook(nil)
	d, err = Open(ctx, path, nil)
	require.NoError(t, err)
	require.NoError(t, d.Close())
}
