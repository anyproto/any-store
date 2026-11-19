//go:build vfs

package anystore

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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

var errInjectedSpillIO = errors.New("injected WAL write failure")

// spillFaults counts the writes of the log and fails them from the
// failAt-th one on: that one alone, or all of them while sticky is set.
type spillFaults struct {
	writes atomic.Int64
	failAt atomic.Int64 // 0: never
	sticky atomic.Bool
	fired  atomic.Int64
}

type spillFaultFile struct {
	File
	f *spillFaults
}

func (w *spillFaultFile) WriteAt(b []byte, off int64) (int, error) {
	n := w.f.writes.Add(1)
	if at := w.f.failAt.Load(); at != 0 && (n == at || (n > at && w.f.sticky.Load())) {
		w.f.fired.Add(1)
		return 0, errInjectedSpillIO
	}
	return w.File.WriteAt(b, off)
}

func (f *spillFaults) arm(nthFromNow int64, sticky bool) {
	f.fired.Store(0)
	f.sticky.Store(sticky)
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

// guardedSpill runs f with a deadline and turns a panic into a failure. It
// reports whether f returned: after a timeout f's goroutine may still hold
// the database.
func guardedSpill(t *testing.T, what string, f func()) bool {
	t.Helper()
	done := make(chan string, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Sprintf("panic: %v\n%s", r, debug.Stack())
				return
			}
			done <- ""
		}()
		f()
	}()
	select {
	case msg := <-done:
		if msg != "" {
			t.Errorf("%s: %s", what, msg)
		}
		return true
	case <-time.After(60 * time.Second):
		t.Errorf("%s: no progress in 60s (livelock)", what)
		return false
	}
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
			SetVFS(VFS{OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
				f, err := os.OpenFile(name, flag, perm)
				if err != nil || !strings.HasSuffix(name, "-wal") {
					return f, err
				}
				return &spillFaultFile{File: f, f: faults}, nil
			}})
			t.Cleanup(ResetVFS)

			path := filepath.Join(t.TempDir(), "spill.db")
			conf := &Config{CacheSize: cacheSize, DisableAutoCheckpoint: true, DisableCompression: true}
			db, err := Open(ctx, path, conf)
			require.NoError(t, err)
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			coll, err := db.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"n"}}))

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
			apply := func(tx WriteTx, op spillDocOp) error {
				if op.del {
					err := coll.DeleteId(tx.Context(), op.id)
					if errors.Is(err, ErrDocNotFound) {
						return nil
					}
					return err
				}
				return coll.UpsertOne(tx.Context(), anyenc.MustParseJson(op.doc))
			}
			// verify reports failures without ending the test: it also runs
			// on guardedSpill's goroutine.
			verify := func(what string) bool {
				t.Helper()
				if !assert.NoError(t, db.IntegrityCheck(ctx), "%s: integrity check", what) {
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
			var inOp int
			for i := int64(0); i < points && !t.Failed(); i++ {
				nth := 1 + i*(total-1)/points
				returned := guardedSpill(t, "failed transaction", func() {
					tx, err := db.WriteTx(ctx)
					if !assert.NoError(t, err) {
						return
					}
					ended := false
					defer func() {
						if !ended {
							_ = tx.Rollback()
						}
					}()
					faults.arm(nth, i%3 == 1)
					var opErr error
					for j, op := range ops(7) {
						if opErr = apply(tx, op); opErr != nil {
							assert.ErrorIs(t, opErr, errInjectedSpillIO, "operation %d fails with the I/O error", j)
							assert.NotErrorIs(t, opErr, ErrCorrupt, "operation %d", j)
							break
						}
					}
					if !assert.Error(t, opErr, "the fault fires inside an operation") {
						return
					}
					inOp++
					// The transaction is over but for its rollback: a later
					// write reports the error and nothing is written.
					fired := faults.fired.Load()
					err = coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":"after the error"}`))
					assert.ErrorIs(t, err, errInjectedSpillIO)
					assert.Equal(t, fired, faults.fired.Load(), "no write of the log in the error state")
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
				})
				faults.disarm()
				if !returned {
					db = nil // its goroutine is stuck in it
					return
				}
				returned = guardedSpill(t, "after the failed transaction", func() {
					verify("after the failed transaction")
					// An empty transaction, then a normal one.
					tx, err := db.WriteTx(ctx)
					if !assert.NoError(t, err) {
						return
					}
					assert.NoError(t, tx.Commit())
					if tx, err = db.WriteTx(ctx); !assert.NoError(t, err) {
						return
					}
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
				})
				if !returned {
					db = nil
					return
				}
			}
			require.False(t, t.Failed())
			require.Equal(t, int(points), inOp)

			require.NoError(t, db.Close())
			db = nil
			ResetVFS()
			db, err = Open(ctx, path, conf)
			require.NoError(t, err)
			coll, err = db.Collection(ctx, "docs")
			require.NoError(t, err)
			require.True(t, verify("after reopen"))
		})
	}
}
