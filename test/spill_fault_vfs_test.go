//go:build vfs && (linux || darwin) && (amd64 || arm64)

package test

// A write of the log that fails while a write transaction's cache spills:
// the operation reports the I/O error, the transaction cannot commit, and
// once it is over the database holds exactly what was committed before it.

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

var errInjectedSpillIO = errors.New("injected WAL write failure")

// spillFaults counts the writes of the log and fails them from the
// failAt-th one on: that one alone, or all of them while sticky is set. A
// torn failure writes the first half of its bytes before it fails.
type spillFaults struct {
	writes atomic.Int64
	failAt atomic.Int64 // 0: never
	sticky atomic.Bool
	torn   atomic.Bool
	fired  atomic.Int64
}

type spillFaultFile struct {
	anystore.File
	f *spillFaults
}

func (w *spillFaultFile) WriteAt(b []byte, off int64) (int, error) {
	n := w.f.writes.Add(1)
	if at := w.f.failAt.Load(); at != 0 && (n == at || (n > at && w.f.sticky.Load())) {
		w.f.fired.Add(1)
		if w.f.torn.Load() {
			k, _ := w.File.WriteAt(b[:len(b)/2], off)
			return k, errInjectedSpillIO
		}
		return 0, errInjectedSpillIO
	}
	return w.File.WriteAt(b, off)
}

func (f *spillFaults) arm(nthFromNow int64, sticky, torn bool) {
	f.fired.Store(0)
	f.sticky.Store(sticky)
	f.torn.Store(torn)
	f.failAt.Store(f.writes.Load() + nthFromNow)
}

func (f *spillFaults) disarm() { f.failAt.Store(0) }

// spillDoc is a document of n payload bytes.
func spillDoc(rng *rand.Rand, id, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	var sb strings.Builder
	sb.Grow(n)
	for i := 0; i < n; i++ {
		sb.WriteByte(letters[rng.Intn(len(letters))])
	}
	return fmt.Sprintf(`{"id":%d,"n":%d,"p":"%s"}`, id, rng.Intn(1000), sb.String())
}

type spillDocOp struct {
	del bool
	id  int
	doc string
}

// guardedSpill runs f and turns a panic into a failure, so that what a
// wrong outcome leaves behind is still checked. A hang is go test's
// -timeout, which prints every goroutine.
func guardedSpill(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s: panic: %v\n%s", what, r, debug.Stack())
		}
	}()
	f()
}

// copySpillDB copies the database and its log, without the log's index,
// as a crash at this instant would leave them, and returns the copy's path.
func copySpillDB(t *testing.T, path, name string) string {
	t.Helper()
	dir := filepath.Join(filepath.Dir(path), name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	dst := filepath.Join(dir, filepath.Base(path))
	for _, suffix := range []string{"", "-wal"} {
		data, err := os.ReadFile(path + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(dst+suffix, data, 0o644))
	}
	return dst
}

// A read whose spill fails ends the transaction's scans as well: an
// iterator open on another collection stops at its next step with the
// read's error, as SQLite's statement halt ends the connection's cursors,
// and does not go on through the documents of the page it stands on.
func TestSpillFault_FailedReadEndsIterators(t *testing.T) {
	faults := &spillFaults{}
	anystore.SetVFS(anystore.VFS{OpenFile: func(name string, flag int, perm os.FileMode) (anystore.File, error) {
		f, err := os.OpenFile(name, flag, perm)
		if err != nil || !strings.HasSuffix(name, "-wal") {
			return f, err
		}
		return &spillFaultFile{File: f, f: faults}, nil
	}})
	t.Cleanup(anystore.ResetVFS)

	path := filepath.Join(t.TempDir(), "iter.db")
	conf := &anystore.Config{CacheSize: 30, DisableAutoCheckpoint: true, DisableCompression: true}
	db, err := anystore.Open(ctx, path, conf)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	a, err := db.CreateCollection(ctx, "a")
	require.NoError(t, err)
	b, err := db.CreateCollection(ctx, "b")
	require.NoError(t, err)
	rng := rand.New(rand.NewSource(1))
	const nA = 300
	for i := 0; i < nA; i++ {
		require.NoError(t, a.Insert(ctx, anyenc.MustParseJson(spillDoc(rng, i, 1000))))
	}
	for i := 0; i < 20; i++ {
		require.NoError(t, b.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))))
	}

	tx, err := db.WriteTx(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	iter, err := b.Find(nil).Iter(tx.Context())
	require.NoError(t, err)
	defer func() { _ = iter.Close() }()
	require.True(t, iter.Next(), "the scan of b stands on its first document")
	// Each insert of a large document fills the transaction's cache with
	// dirty pages, and the read after it of a document the cache does not
	// hold has to spill one once no clean page is left: the log's write
	// fails under that read.
	var readErr error
	for i := 0; i < nA && faults.fired.Load() == 0; i++ {
		require.NoError(t, a.Insert(tx.Context(), anyenc.MustParseJson(spillDoc(rng, nA+i, 12000))))
		faults.arm(1, false, false)
		_, readErr = a.FindId(tx.Context(), i)
		if faults.fired.Load() == 0 {
			require.NoError(t, readErr)
			faults.disarm()
		}
	}
	require.Equal(t, int64(1), faults.fired.Load(), "a read met the fault")
	require.ErrorIs(t, readErr, errInjectedSpillIO)

	assert.False(t, iter.Next(), "the scan of b goes on after the failed read")
	assert.ErrorIs(t, iter.Err(), errInjectedSpillIO)
	faults.disarm()
}

func TestSpillFault_WriteTx(t *testing.T) {
	const baseDocs = 150
	for _, cacheSize := range []int{30, 0} {
		t.Run(fmt.Sprintf("cache=%d", cacheSize), func(t *testing.T) {
			if cacheSize == 0 && testing.Short() {
				t.Skip("writes about 100 MiB")
			}
			nOps, points := 300, int64(12)
			if cacheSize == 0 {
				// defaultCacheSize pages: the transaction has to dirty
				// more than that before it spills.
				nOps, points = 30000, 3
			}
			faults := &spillFaults{}
			anystore.SetVFS(anystore.VFS{OpenFile: func(name string, flag int, perm os.FileMode) (anystore.File, error) {
				f, err := os.OpenFile(name, flag, perm)
				if err != nil || !strings.HasSuffix(name, "-wal") {
					return f, err
				}
				return &spillFaultFile{File: f, f: faults}, nil
			}})
			t.Cleanup(anystore.ResetVFS)

			path := filepath.Join(t.TempDir(), "spill.db")
			conf := &anystore.Config{CacheSize: cacheSize, DisableAutoCheckpoint: true, DisableCompression: true}
			db, err := anystore.Open(ctx, path, conf)
			require.NoError(t, err)
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			coll, err := db.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{Fields: []string{"n"}}))

			model := map[int]string{}
			var big []int
			rng := rand.New(rand.NewSource(1))
			tx, err := db.WriteTx(ctx)
			require.NoError(t, err)
			for i := 0; i < baseDocs; i++ {
				n := 100 + rng.Intn(800)
				if i%3 == 0 {
					n = 5000 + rng.Intn(15000)
					big = append(big, i)
				}
				doc := spillDoc(rng, i, n)
				require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(doc)))
				model[i] = doc
			}
			require.NoError(t, tx.Commit())

			ops := func(seed int64) []spillDocOp {
				rng := rand.New(rand.NewSource(seed))
				left := append([]int(nil), big...)
				out := make([]spillDocOp, 0, nOps)
				next := baseDocs
				for i := 0; i < nOps; i++ {
					switch r := rng.Intn(10); {
					case r == 0 && len(left) > 0:
						j := rng.Intn(len(left))
						out = append(out, spillDocOp{del: true, id: left[j]})
						left = append(left[:j], left[j+1:]...)
					case r < 4:
						id := rng.Intn(baseDocs)
						out = append(out, spillDocOp{id: id, doc: spillDoc(rng, id, 50+rng.Intn(9000))})
					default:
						out = append(out, spillDocOp{id: next, doc: spillDoc(rng, next, 400+rng.Intn(1200))})
						next++
					}
				}
				return out
			}
			apply := func(tx anystore.WriteTx, op spillDocOp) error {
				if op.del {
					err := coll.DeleteId(tx.Context(), op.id)
					if errors.Is(err, anystore.ErrDocNotFound) {
						return nil
					}
					return err
				}
				return coll.UpsertOne(tx.Context(), anyenc.MustParseJson(op.doc))
			}
			// verifyDB checks that db's collection holds exactly the model.
			// It reports failures without ending the test.
			verifyDB := func(db anystore.DB, what string) bool {
				t.Helper()
				if !assert.NoError(t, db.IntegrityCheck(ctx), "%s: integrity check", what) {
					return false
				}
				coll, err := db.Collection(ctx, "docs")
				if !assert.NoError(t, err, what) {
					return false
				}
				n, err := coll.Count(ctx)
				ok := assert.NoError(t, err, what) && assert.Equal(t, len(model), n, "%s: document count", what)
				for id, want := range model {
					doc, err := coll.FindId(ctx, id)
					if !assert.NoError(t, err, "%s: document %d", what, id) ||
						!assert.JSONEq(t, want, doc.Value().String(), "%s: document %d", what, id) {
						return false
					}
				}
				return ok
			}
			verify := func(what string) bool {
				t.Helper()
				return verifyDB(db, what)
			}
			// verifyCrashCopy opens a copy of the files as a crash at this
			// instant leaves them, so that the log is recovered from its
			// bytes: an orderly Close checkpoints and truncates it first.
			copies := 0
			verifyCrashCopy := func(what string) {
				t.Helper()
				copies++
				cdb, err := anystore.Open(ctx, copySpillDB(t, path, fmt.Sprintf("crash-%d", copies)), conf)
				require.NoError(t, err, what)
				defer func() { require.NoError(t, cdb.Close(), what) }()
				verifyDB(cdb, what)
			}

			// The writes of the log the operations make before the commit:
			// the spills.
			tx, err = db.WriteTx(ctx)
			require.NoError(t, err)
			before := faults.writes.Load()
			for _, op := range ops(7) {
				require.NoError(t, apply(tx, op))
			}
			total := faults.writes.Load() - before
			require.NoError(t, tx.Rollback())
			require.Greater(t, total, int64(50), "the transaction spills")
			require.True(t, verify("after the counting transaction"))

			// Every check of a fault point is made, a failed one included:
			// what a wrong outcome leaves behind is the point.
			for i := int64(0); i < points && !t.Failed(); i++ {
				nth := 1 + i*(total-1)/points
				guardedSpill(t, "failed transaction", func() {
					tx, err := db.WriteTx(ctx)
					require.NoError(t, err)
					ended := false
					defer func() {
						if !ended {
							_ = tx.Rollback()
						}
					}()
					faults.arm(nth, i%3 == 1, i%4 == 3)
					failed := -1
					for j, op := range ops(7) {
						fired := faults.fired.Load()
						opErr := apply(tx, op)
						if faults.fired.Load() == fired {
							require.NoError(t, opErr, "operation %d, before the fault", j)
							continue
						}
						// The operation whose spill met the fault fails with
						// its error.
						assert.ErrorIs(t, opErr, errInjectedSpillIO, "operation %d meets the fault", j)
						assert.NotErrorIs(t, opErr, anystore.ErrCorrupt, "operation %d", j)
						failed = j
						break
					}
					require.GreaterOrEqual(t, failed, 0, "the fault fired inside an operation")

					// The transaction is over but for its rollback: a later
					// write or read in it reports the error, a committed
					// document is never "not found", and nothing more is
					// written to the log, not even at the end.
					writes := faults.writes.Load()
					err = coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"after the error"}`))
					assert.ErrorIs(t, err, errInjectedSpillIO)
					for id := range model {
						_, err := coll.FindId(tx.Context(), id)
						if !assert.ErrorIs(t, err, errInjectedSpillIO, "read of committed document %d in the failed transaction", id) {
							break
						}
					}
					// Other transactions read what is committed meanwhile.
					if doc, err := coll.FindId(ctx, 1); assert.NoError(t, err, "a read beside the failed transaction") {
						assert.JSONEq(t, model[1], doc.Value().String())
					}
					ended = true
					if i%2 == 0 {
						assert.NoError(t, tx.Rollback())
					} else {
						assert.ErrorIs(t, tx.Commit(), errInjectedSpillIO, "the transaction cannot commit")
					}
					assert.Equal(t, writes, faults.writes.Load(), "a write of the log in the error state or at its end")
				})
				faults.disarm()
				guardedSpill(t, "after the failed transaction", func() {
					verify("after the failed transaction")
					verifyCrashCopy("crash copy after the failed transaction")
					// An empty transaction, then a normal one.
					tx, err := db.WriteTx(ctx)
					require.NoError(t, err)
					assert.NoError(t, tx.Commit())
					tx, err = db.WriteTx(ctx)
					require.NoError(t, err)
					rng := rand.New(rand.NewSource(nth))
					for k := 0; k < 30; k++ {
						id := 1_000_000 + int(i)*100 + k
						doc := spillDoc(rng, id, 100+rng.Intn(6000))
						if !assert.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(doc))) {
							_ = tx.Rollback()
							return
						}
						model[id] = doc
					}
					assert.NoError(t, tx.Commit())
					verify("after the next transaction")
					verifyCrashCopy("crash copy after the next transaction")
				})
			}
			require.False(t, t.Failed())

			require.NoError(t, db.Close())
			db = nil
			anystore.ResetVFS()
			db, err = anystore.Open(ctx, path, conf)
			require.NoError(t, err)
			require.True(t, verify("after reopen"))
		})
	}
}
