//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

// A write of the log that fails while the writer cache spills. The spill's
// error is the error of the page fetch that needed room
// (sqlite3PcacheFetchStress, pcache.c:481-484; getPageNormal,
// pager.c:5555-5556): the b-tree operation fails with it, the cache is left
// as it was under the operation, the transaction can only be rolled back, and
// the rollback leaves nothing of it for the next one.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errInjectedSpillIO = errors.New("injected WAL write failure")

// spillFaultFile is the log of a database whose writes fail from the
// failAt-th one on: that one alone, or all of them while sticky is set (a
// full disk). A torn failure writes the first half of its bytes before it
// fails.
type spillFaultFile struct {
	File
	writes atomic.Int64
	failAt atomic.Int64 // 0: never
	sticky atomic.Bool
	torn   atomic.Bool
	fired  atomic.Int64
}

func (f *spillFaultFile) WriteAt(b []byte, off int64) (int, error) {
	n := f.writes.Add(1)
	if at := f.failAt.Load(); at != 0 && (n == at || (n > at && f.sticky.Load())) {
		f.fired.Add(1)
		if f.torn.Load() {
			w, _ := f.File.WriteAt(b[:len(b)/2], off)
			return w, errInjectedSpillIO
		}
		return 0, errInjectedSpillIO
	}
	return f.File.WriteAt(b, off)
}

func (f *spillFaultFile) arm(nthFromNow int64, sticky, torn bool) {
	f.fired.Store(0)
	f.sticky.Store(sticky)
	f.torn.Store(torn)
	f.failAt.Store(f.writes.Load() + nthFromNow)
}

func (f *spillFaultFile) disarm() { f.failAt.Store(0) }

func spillKey(i int) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], uint64(i))
	return k[:]
}

func spillValue(rng *rand.Rand, n int) []byte {
	v := make([]byte, n)
	rng.Read(v)
	return v
}

// spillFaultDB is a database with a committed baseline in namespace "a" and
// the model of what is committed.
type spillFaultDB struct {
	db     *DB
	ns     *Namespace
	path   string
	file   *spillFaultFile
	model  map[int][]byte
	big    []int // baseline keys whose values are on overflow chains
	copies int   // crash copies taken
}

func openSpillFaultDB(t *testing.T, cacheSize, baseKeys int) *spillFaultDB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spill.db")
	db, err := testOpen(t, path, Options{PageSize: 4096, CacheSize: cacheSize, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	s := &spillFaultDB{db: db, path: path, model: map[int][]byte{}}
	t.Cleanup(func() {
		if s.db != nil {
			_ = s.db.Close()
		}
	})
	rng := rand.New(rand.NewSource(1))
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	s.ns, err = tx.CreateNamespace("a")
	require.NoError(t, err)
	for i := 0; i < baseKeys; i++ {
		n := 100 + rng.Intn(800)
		if i%3 == 0 {
			n = 5000 + rng.Intn(15000)
			s.big = append(s.big, i)
		}
		v := spillValue(rng, n)
		require.NoError(t, tx.Put(s.ns, spillKey(i), v))
		s.model[i] = v
	}
	require.NoError(t, tx.Commit())
	s.file = &spillFaultFile{File: db.pager.wal.file}
	db.pager.wal.file = s.file
	return s
}

// spillOp is one operation of the transaction under the fault.
type spillOp struct {
	del bool
	key int
	val []byte
}

// spillOps is a transaction that dirties far more pages than a small cache
// holds: deletes of overflowing values (pages go to the freelist and come
// back), updates and inserts.
func (s *spillFaultDB) spillOps(seed int64, n, baseKeys int) []spillOp {
	rng := rand.New(rand.NewSource(seed))
	big := append([]int(nil), s.big...)
	ops := make([]spillOp, 0, n)
	next := baseKeys
	for i := 0; i < n; i++ {
		switch r := rng.Intn(10); {
		case r == 0 && len(big) > 0:
			j := rng.Intn(len(big))
			ops = append(ops, spillOp{del: true, key: big[j]})
			big = append(big[:j], big[j+1:]...)
		case r < 4:
			ops = append(ops, spillOp{key: rng.Intn(baseKeys), val: spillValue(rng, 50+rng.Intn(9000))})
		default:
			ops = append(ops, spillOp{key: next, val: spillValue(rng, 100+rng.Intn(800))})
			next++
		}
	}
	return ops
}

// apply runs op. Every delete of spillOps is of a key the transaction holds:
// a not-found is an error.
func (s *spillFaultDB) apply(tx *WriteTx, op spillOp) error {
	if op.del {
		return tx.Delete(s.ns, spillKey(op.key))
	}
	return tx.Put(s.ns, spillKey(op.key), op.val)
}

// verify is the integrity check and a read of every committed key.
func (s *spillFaultDB) verify(t *testing.T, what string) bool {
	t.Helper()
	return verifySpillModel(t, s.db, s.model, what)
}

// verifySpillModel checks that db's namespace "a" holds exactly model. It
// reports failures without ending the test.
func verifySpillModel(t *testing.T, db *DB, model map[int][]byte, what string) bool {
	t.Helper()
	if !assert.NoError(t, db.IntegrityCheck(), "%s: integrity check", what) {
		return false
	}
	rtx, err := db.BeginRead()
	if !assert.NoError(t, err, what) {
		return false
	}
	defer rtx.Rollback()
	ns, err := rtx.GetNamespace("a")
	if !assert.NoError(t, err, what) {
		return false
	}
	n, err := rtx.Count(ns)
	ok := assert.NoError(t, err, what) && assert.Equal(t, len(model), n, "%s: key count", what)
	for k, want := range model {
		got, err := rtx.Get(ns, spillKey(k))
		if !assert.NoError(t, err, "%s: key %d", what, k) {
			return false
		}
		if !bytes.Equal(want, got) {
			t.Errorf("%s: key %d: value differs (len %d, want %d)", what, k, len(got), len(want))
			return false
		}
	}
	return ok
}

// verifyCrashCopy opens a copy of the database and its log as they are now,
// as a crash would leave them, and checks that it holds what is committed.
// The copy's log is recovered from its bytes: an orderly Close checkpoints
// and truncates it first, and a log whose frames chain from the wrong
// checksums, or that ends in a torn frame the recovery takes for one, would
// go unnoticed.
func (s *spillFaultDB) verifyCrashCopy(t *testing.T, what string) {
	t.Helper()
	s.copies++
	path := restartCopyFiles(t, s.path, fmt.Sprintf("crash-%d", s.copies))
	db, err := testOpen(t, path, Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err, what)
	defer func() { require.NoError(t, db.Close(), what) }()
	verifySpillModel(t, db, s.model, what)
}

// guarded runs f and turns a panic into a failure, so that what a wrong
// outcome leaves behind is still checked. A hang is go test's -timeout,
// which prints every goroutine.
func guarded(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s: panic: %v\n%s", what, r, debug.Stack())
		}
	}()
	f()
}

type spillFaultCase struct {
	nth        int64 // the write of the log that fails, counted from the transaction's begin
	sticky     bool
	torn       bool // the failing write writes half its bytes
	savepoints bool // each operation in a savepoint, rolled back when it fails
	commit     bool // the failed transaction ends with Commit instead of Rollback
}

// runSpillFault runs one transaction with the fault at c.nth, which falls
// on a spill of one of its operations, and everything that must hold after
// it. Every check is made, a failed one included: what a wrong outcome
// leaves behind is the point.
func (s *spillFaultDB) runSpillFault(t *testing.T, c spillFaultCase, ops []spillOp) {
	t.Helper()
	p := s.db.pager
	guarded(t, "failed transaction", func() {
		tx, err := s.db.BeginWrite()
		require.NoError(t, err)
		ended := false
		defer func() {
			if !ended {
				_ = tx.Rollback()
			}
		}()
		s.file.arm(c.nth, c.sticky, c.torn)
		failed := -1
		for i, op := range ops {
			sp := -1
			if c.savepoints {
				sp, err = tx.Savepoint()
				require.NoError(t, err)
			}
			fired := s.file.fired.Load()
			opErr := s.apply(tx, op)
			if s.file.fired.Load() == fired {
				require.NoError(t, opErr, "operation %d, before the fault", i)
				if c.savepoints {
					require.NoError(t, tx.ReleaseSavepoint(sp))
				}
				continue
			}
			// The operation whose spill met the fault fails with its error.
			assert.ErrorIs(t, opErr, errInjectedSpillIO, "operation %d meets the fault", i)
			assert.NotErrorIs(t, opErr, ErrCorrupt, "operation %d", i)
			if c.savepoints {
				// The transaction is in the error state: its savepoint
				// cannot be played back, only the whole of it.
				assert.ErrorIs(t, tx.RollbackToSavepoint(sp), errInjectedSpillIO)
			}
			failed = i
			break
		}
		require.GreaterOrEqual(t, failed, 0, "the fault fired inside an operation")
		assert.Equal(t, int32(pagerError), p.state.Load(), "the transaction is in the error state")

		// Every later operation reports the error, and nothing more is
		// written to the log: not by the savepoint rollback above, not by
		// them, not by the end of the transaction. The failed write is the
		// log's last.
		assert.ErrorIs(t, tx.Put(s.ns, spillKey(1<<30), []byte("after the error")), errInjectedSpillIO)
		assert.ErrorIs(t, tx.Delete(s.ns, spillKey(0)), errInjectedSpillIO)
		_, err = tx.Savepoint()
		assert.ErrorIs(t, err, errInjectedSpillIO)
		// A read of the failed transaction reports the error too, a
		// committed key never "not found": a balance the spill stopped
		// half-way left the cached pages of the tree incomplete.
		for k := range s.model {
			_, err := tx.Get(s.ns, spillKey(k))
			if !assert.ErrorIs(t, err, errInjectedSpillIO, "read of committed key %d in the failed transaction", k) {
				break
			}
		}
		// A read transaction begins and reads what is committed meanwhile.
		if rtx, err := s.db.BeginRead(); assert.NoError(t, err, "a read beside the failed transaction") {
			got, err := rtx.Get(s.ns, spillKey(1))
			assert.NoError(t, err)
			assert.True(t, bytes.Equal(s.model[1], got))
			_ = rtx.Rollback()
		}
		ended = true
		if c.commit {
			assert.ErrorIs(t, tx.Commit(), errInjectedSpillIO, "the transaction cannot commit")
		} else {
			assert.NoError(t, tx.Rollback())
		}
		assert.Equal(t, s.file.failAt.Load(), s.file.writes.Load(), "a write of the log after the failed one")
	})
	s.file.disarm()
	assert.Equal(t, int32(pagerOpen), p.state.Load())
	assert.Zero(t, p.writerCache.nDirty, "no dirty page outlives the transaction")
	assert.Nil(t, p.errCode)

	guarded(t, "after the failed transaction", func() {
		s.verify(t, "after the failed transaction")
		s.verifyCrashCopy(t, "crash copy after the failed transaction")
		// An empty transaction commits nothing.
		frames := p.wal.nFrame.Load()
		tx, err := s.db.BeginWrite()
		require.NoError(t, err)
		assert.NoError(t, tx.Commit())
		assert.Equal(t, frames, p.wal.nFrame.Load(), "an empty commit writes no frame")
		// A normal one commits.
		tx, err = s.db.BeginWrite()
		require.NoError(t, err)
		rng := rand.New(rand.NewSource(c.nth))
		for i := 0; i < 40; i++ {
			k := 1_000_000 + i
			v := spillValue(rng, 100+rng.Intn(6000))
			if !assert.NoError(t, tx.Put(s.ns, spillKey(k), v)) {
				_ = tx.Rollback()
				return
			}
			s.model[k] = v
		}
		assert.NoError(t, tx.Commit())
		s.verify(t, "after the next transaction")
		s.verifyCrashCopy(t, "crash copy after the next transaction")
		s.reopen(t)
	})
}

// reopen closes the database and opens it again without the fault file.
func (s *spillFaultDB) reopen(t *testing.T) {
	t.Helper()
	db := s.db
	s.db = nil
	if !assert.NoError(t, db.Close()) {
		return
	}
	db, err := testOpen(t, s.path, Options{PageSize: 4096, DisableAutoCheckpoint: true})
	if !assert.NoError(t, err) {
		return
	}
	s.db = db
	rtx, err := db.BeginRead()
	if !assert.NoError(t, err) {
		return
	}
	s.ns, err = rtx.GetNamespace("a")
	assert.NoError(t, err)
	assert.NoError(t, rtx.Rollback())
	s.verify(t, "after reopen")
}

// spillWrites is the number of writes of the log the operations make before
// the commit, each in a savepoint of its own or not: the spills. It is
// counted on a database of its own, as fresh as the one each fault point
// gets.
func spillWrites(t *testing.T, cacheSize, baseKeys int, seed int64, nOps int, savepoints bool) int64 {
	t.Helper()
	s := openSpillFaultDB(t, cacheSize, baseKeys)
	tx, err := s.db.BeginWrite()
	require.NoError(t, err)
	before := s.file.writes.Load()
	for _, op := range s.spillOps(seed, nOps, baseKeys) {
		sp := -1
		if savepoints {
			sp, err = tx.Savepoint()
			require.NoError(t, err)
		}
		require.NoError(t, s.apply(tx, op))
		if savepoints {
			require.NoError(t, tx.ReleaseSavepoint(sp))
		}
	}
	n := s.file.writes.Load() - before
	require.NoError(t, tx.Rollback())
	require.True(t, s.verify(t, "after the counting transaction"))
	return n
}

// The fault point swept across a transaction that spills from a 32-page
// cache, a database for each point. The writes are counted for each mode,
// so that every point falls on a spill.
func TestSpillFault_Sweep(t *testing.T) {
	const (
		baseKeys = 150
		nOps     = 400
		seed     = 7
	)
	modes := []struct {
		name string
		c    spillFaultCase
	}{
		{"rollback", spillFaultCase{}},
		{"commit", spillFaultCase{commit: true}},
		{"sticky", spillFaultCase{sticky: true}},
		{"torn", spillFaultCase{torn: true}},
		{"savepoints", spillFaultCase{savepoints: true}},
		{"savepoints-commit-sticky-torn", spillFaultCase{savepoints: true, commit: true, sticky: true, torn: true}},
	}
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			total := spillWrites(t, 32, baseKeys, seed, nOps, m.c.savepoints)
			require.Greater(t, total, int64(100), "the transaction spills")
			for nth := int64(1); nth <= total; nth += total / 12 {
				t.Run(fmt.Sprintf("write=%d", nth), func(t *testing.T) {
					s := openSpillFaultDB(t, 32, baseKeys)
					c := m.c
					c.nth = nth
					s.runSpillFault(t, c, s.spillOps(seed, nOps, baseKeys))
				})
			}
		})
	}
}

// The same with the default cache (defaultCacheSize pages): the transaction
// has to dirty more than that before it spills.
func TestSpillFault_DefaultCache(t *testing.T) {
	if testing.Short() {
		t.Skip("writes about 100 MiB")
	}
	const (
		baseKeys = 300
		nOps     = 80000
		seed     = 3
	)
	totals := map[bool]int64{}
	for i, c := range []spillFaultCase{{commit: true}, {sticky: true, torn: true}, {savepoints: true}} {
		total, ok := totals[c.savepoints]
		if !ok {
			total = spillWrites(t, 0, baseKeys, seed, nOps, c.savepoints)
			require.Greater(t, total, int64(100), "the transaction spills")
			totals[c.savepoints] = total
		}
		c.nth = []int64{1, total / 3, total - 1}[i]
		s := openSpillFaultDB(t, 0, baseKeys)
		require.Equal(t, defaultCacheSize, s.db.pager.writerCache.maxPages)
		s.runSpillFault(t, c, s.spillOps(seed, nOps, baseKeys))
	}
}

// A spill inside a savepoint that is then rolled back, with no fault: the
// savepoint's pages come back from their copies and the transaction commits
// what preceded it.
func TestSpillFault_SavepointRollbackAfterSpill(t *testing.T) {
	const baseKeys = 150
	s := openSpillFaultDB(t, 32, baseKeys)
	ops := s.spillOps(11, 600, baseKeys)
	tx, err := s.db.BeginWrite()
	require.NoError(t, err)
	apply := func(ops []spillOp, keep bool) {
		for _, op := range ops {
			require.NoError(t, s.apply(tx, op))
			if !keep {
				continue
			}
			if op.del {
				delete(s.model, op.key)
			} else {
				s.model[op.key] = op.val
			}
		}
	}
	apply(ops[:200], true)
	sp, err := tx.Savepoint()
	require.NoError(t, err)
	before := s.file.writes.Load()
	apply(ops[200:400], false)
	require.Greater(t, s.file.writes.Load()-before, int64(50), "the savepoint spills")
	require.NoError(t, tx.RollbackToSavepoint(sp))
	require.NoError(t, tx.ReleaseSavepoint(sp))
	// The keys of ops[400:] that ops[200:400] touched are written again from
	// the state the rollback restored.
	apply(ops[400:], true)
	require.NoError(t, tx.Commit())
	require.True(t, s.verify(t, "after the commit"))
	s.reopen(t)
}

// A failed spill can stop the split of a tree's root half-way:
// balanceDeeper has moved the root's cells to a new child and left the
// root an interior page with no cell, and the fetch that fails is the one
// for the sibling the balance allocates next. That root stays in the
// writer cache. The next read or write of the tree meets the spill's
// error, not the zero-cell root, which only page 1 may be and which a
// descent reports as corruption.
func TestSpillFault_RootSplitStoppedHalfWay(t *testing.T) {
	const valSize = 400
	open := func(t *testing.T, keys int) (*DB, *spillFaultFile) {
		path := filepath.Join(t.TempDir(), "split.db")
		db, err := testOpen(t, path, Options{PageSize: 4096, CacheSize: closeRescueCache, DisableAutoCheckpoint: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		createNS(t, db, "a")
		putN(t, db, "a", closeRescueRows, closeRescueValSize)
		createNS(t, db, "b")
		if keys > 0 {
			putN(t, db, "b", keys, valSize)
		}
		f := &spillFaultFile{File: db.pager.wal.file}
		db.pager.wal.file = f
		return db, f
	}
	rootIsInterior := func(t *testing.T, tx *WriteTx, ns *Namespace) bool {
		pg, err := tx.pager.getPage(ns.rootPage)
		require.NoError(t, err)
		defer tx.pager.releasePage(pg)
		return pg.header.isInterior()
	}

	// The number of keys "b"'s leaf root holds: the next one splits it.
	db, _ := open(t, 0)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("b")
	require.NoError(t, err)
	fit := 0
	for !rootIsInterior(t, tx, ns) {
		fit++
		require.NoError(t, tx.Put(ns, closeRescueKey(fit), make([]byte, valSize)))
	}
	fit--
	require.NoError(t, tx.Rollback())
	require.Greater(t, fit, 2)

	// The fault swept across the writes of the split, until one is past
	// its last.
	halfWay := 0
	for nth, fired := int64(1), true; fired; nth++ {
		fired = false
		t.Run(fmt.Sprintf("write=%d", nth), func(t *testing.T) {
			db, f := open(t, fit)
			p := db.pager
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			ns, err := tx.GetNamespace("b")
			require.NoError(t, err)
			require.False(t, rootIsInterior(t, tx, ns))
			// Every page the split allocates now spills one.
			fillWriterCacheDirty(t, p)

			f.arm(nth, false, false)
			err = tx.Put(ns, closeRescueKey(fit+1), make([]byte, valSize))
			if f.fired.Load() == 0 {
				require.NoError(t, err)
				return
			}
			fired = true
			require.ErrorIs(t, err, errInjectedSpillIO)
			if root := p.writerCache.hashFind(ns.rootPage); root != nil &&
				root.header.isInterior() && root.header.cellCount == 0 {
				halfWay++
			}

			for i, op := range []func() error{
				func() error { return tx.Put(ns, closeRescueKey(fit+2), []byte("x")) },
				func() error { return tx.Put(ns, closeRescueKey(1), []byte("x")) },
				func() error { return tx.Delete(ns, closeRescueKey(2)) },
				func() error { _, err := tx.Get(ns, closeRescueKey(3)); return err },
				func() error {
					c := tx.NewCursor(ns)
					defer c.Close()
					return c.First()
				},
			} {
				err := op()
				assert.ErrorIs(t, err, errInjectedSpillIO, "operation %d after the failed split", i)
				assert.NotErrorIs(t, err, ErrCorrupt, "operation %d after the failed split", i)
			}

			require.NoError(t, tx.Rollback())
			f.disarm()
			require.Equal(t, fit, countKeys(t, db, "b"))
			require.NoError(t, db.IntegrityCheck())
		})
	}
	require.Greater(t, halfWay, 0, "no fault stopped the split with a zero-cell root")
}

// The cursors of a transaction in the error state are ended when the call
// that met the error returns, and again by its rollback, before the cache
// they pin is discarded: a cursor left open on another tree would otherwise
// step on through its leaf, keep pointers into the discarded cache and
// unpin, at its Close, pages the next transaction holds. The commit of such
// a transaction rolls it back the same way.
func TestSpillFault_RollbackTripsCursors(t *testing.T) {
	const baseKeys = 150
	for _, end := range []string{"rollback", "commit"} {
		t.Run(end, func(t *testing.T) {
			s := openSpillFaultDB(t, 32, baseKeys)
			tx, err := s.db.BeginWrite()
			require.NoError(t, err)
			nsB, err := tx.CreateNamespace("b")
			require.NoError(t, err)
			for i := 0; i < 20; i++ {
				require.NoError(t, tx.Put(nsB, spillKey(i), []byte("b")))
			}
			require.NoError(t, tx.Commit())

			tx, err = s.db.BeginWrite()
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			c := tx.NewCursor(nsB)
			defer c.Close()
			require.NoError(t, c.First())
			require.True(t, c.Valid())
			require.NotEmpty(t, c.stack)

			s.file.arm(1, false, false)
			var opErr error
			for _, op := range s.spillOps(7, 400, baseKeys) {
				if opErr = s.apply(tx, op); opErr != nil {
					break
				}
			}
			require.ErrorIs(t, opErr, errInjectedSpillIO)
			// The failed call ended the cursor as it returned.
			require.Empty(t, c.stack, "the cursor still pins its leaf after the failed call")
			require.ErrorIs(t, c.Next(), errInjectedSpillIO, "the cursor steps on after the failed call")
			if end == "rollback" {
				require.NoError(t, tx.Rollback())
			} else {
				require.ErrorIs(t, tx.Commit(), errInjectedSpillIO)
			}
			s.file.disarm()

			require.Empty(t, c.stack, "the cursor holds pages of the discarded cache")
			require.ErrorIs(t, c.Next(), errInjectedSpillIO, "the cursor was not ended with the transaction's error")
			c.Close()
			require.True(t, s.verify(t, "after the failed transaction"))
		})
	}
}

// The call during which a spill fails ends the transaction's cursors as it
// returns, as SQLite's statement halt does (sqlite3VdbeHalt: closeAllCursors,
// then sqlite3RollbackAll's trip, vdbeaux.c:3335, 3378): a cursor positioned
// on another tree does not go on stepping through the leaf it pins. Each
// call of a write transaction that can fetch a page is the failing one in
// turn, a read and a move of another cursor included.
func TestSpillFault_FailedCallEndsCursors(t *testing.T) {
	bigKey := bytes.Repeat([]byte("k"), 3*4096)
	longName := strings.Repeat("n", 3*4096)
	open := func(t *testing.T) (*DB, *spillFaultFile) {
		path := filepath.Join(t.TempDir(), "halt.db")
		db, err := testOpen(t, path, Options{PageSize: 4096, CacheSize: closeRescueCache, DisableAutoCheckpoint: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		createNS(t, db, "a")
		putN(t, db, "a", closeRescueRows, closeRescueValSize)
		createNS(t, db, "b")
		putN(t, db, "b", 20, 10)
		createNS(t, db, "c")
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		nsC, err := tx.GetNamespace("c")
		require.NoError(t, err)
		require.NoError(t, tx.Put(nsC, closeRescueKey(1), make([]byte, 10*4096)))
		require.NoError(t, tx.Put(nsC, bigKey, []byte("v")))
		require.NoError(t, tx.Commit())
		f := &spillFaultFile{File: db.pager.wal.file}
		db.pager.wal.file = f
		return db, f
	}
	far := closeRescueKey(closeRescueRows - 1)
	// Each case positions what it needs before the cache is filled, and
	// returns the call that must meet the fault.
	cases := []struct {
		name string
		call func(t *testing.T, tx *WriteTx) func() error
	}{
		{"Put", func(t *testing.T, tx *WriteTx) func() error {
			ns := mustNS(t, tx, "a")
			return func() error { return tx.Put(ns, far, []byte("x")) }
		}},
		{"Delete", func(t *testing.T, tx *WriteTx) func() error {
			ns := mustNS(t, tx, "a")
			return func() error { return tx.Delete(ns, far) }
		}},
		{"Get", func(t *testing.T, tx *WriteTx) func() error {
			ns := mustNS(t, tx, "a")
			return func() error { _, err := tx.Get(ns, far); return err }
		}},
		{"Has", func(t *testing.T, tx *WriteTx) func() error {
			ns := mustNS(t, tx, "a")
			return func() error { _, err := tx.Has(ns, far); return err }
		}},
		{"SeekKey", func(t *testing.T, tx *WriteTx) func() error {
			ns := mustNS(t, tx, "a")
			return func() error { _, err := tx.SeekKey(ns, far); return err }
		}},
		{"Count", func(t *testing.T, tx *WriteTx) func() error {
			ns := mustNS(t, tx, "a")
			return func() error { _, err := tx.Count(ns); return err }
		}},
		{"NamespaceSize", func(t *testing.T, tx *WriteTx) func() error {
			ns := mustNS(t, tx, "a")
			return func() error { _, err := tx.NamespaceSize(ns); return err }
		}},
		{"CreateNamespace", func(t *testing.T, tx *WriteTx) func() error {
			return func() error { _, err := tx.CreateNamespace(longName); return err }
		}},
		{"DeleteNamespace", func(t *testing.T, tx *WriteTx) func() error {
			return func() error { return tx.DeleteNamespace("a") }
		}},
		{"RenameNamespace", func(t *testing.T, tx *WriteTx) func() error {
			return func() error { return tx.RenameNamespace("a", longName) }
		}},
		{"Cursor.Last", func(t *testing.T, tx *WriteTx) func() error {
			c := tx.NewCursor(mustNS(t, tx, "a"))
			t.Cleanup(c.Close)
			return c.Last
		}},
		{"Cursor.Seek", func(t *testing.T, tx *WriteTx) func() error {
			c := tx.NewCursor(mustNS(t, tx, "a"))
			t.Cleanup(c.Close)
			return func() error { return c.Seek(far) }
		}},
		{"Cursor.Next", func(t *testing.T, tx *WriteTx) func() error {
			// On the last entry of a dirty leaf: the next step crosses to
			// a leaf the full cache does not hold, and the one it leaves
			// cannot make room.
			c := tx.NewCursor(mustNS(t, tx, "a"))
			t.Cleanup(c.Close)
			require.NoError(t, c.Seek(closeRescueKey(closeRescueRows/2)))
			leaf := &c.stack[len(c.stack)-1]
			leaf.cellIdx = int(leaf.pg.header.cellCount) - 1
			pg, err := tx.pager.getWritablePage(leaf.pgno)
			require.NoError(t, err)
			tx.pager.releasePage(pg)
			return c.Next
		}},
		{"Cursor.RangeFraction", func(t *testing.T, tx *WriteTx) func() error {
			c := tx.NewCursor(mustNS(t, tx, "a"))
			t.Cleanup(c.Close)
			return func() error { _, err := c.RangeFraction(closeRescueKey(1), far); return err }
		}},
		{"Cursor.Value", func(t *testing.T, tx *WriteTx) func() error {
			c := tx.NewCursor(mustNS(t, tx, "c"))
			t.Cleanup(c.Close)
			require.NoError(t, c.SeekExact(closeRescueKey(1)))
			return func() error { _, err := c.Value(); return err }
		}},
		{"Cursor.AppendValue", func(t *testing.T, tx *WriteTx) func() error {
			c := tx.NewCursor(mustNS(t, tx, "c"))
			t.Cleanup(c.Close)
			require.NoError(t, c.SeekExact(closeRescueKey(1)))
			return func() error { _, err := c.AppendValue(nil); return err }
		}},
		{"Cursor.Key", func(t *testing.T, tx *WriteTx) func() error {
			c := tx.NewCursor(mustNS(t, tx, "c"))
			t.Cleanup(c.Close)
			require.NoError(t, c.SeekExact(bigKey))
			return func() error { _, err := c.Key(); return err }
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, f := open(t)
			p := db.pager
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			// The cursor that must not go on: on the first of the 20 keys of
			// "b", whose one leaf it pins.
			cur := tx.NewCursor(mustNS(t, tx, "b"))
			defer cur.Close()
			require.NoError(t, cur.First())
			call := tc.call(t, tx)
			fillWriterCacheDirty(t, p)

			f.arm(1, false, false)
			err = call()
			require.Equal(t, int64(1), f.fired.Load(), "the call met the fault")
			require.ErrorIs(t, err, errInjectedSpillIO)
			require.Equal(t, int32(pagerError), p.state.Load())

			assert.Empty(t, cur.stack, "the cursor still pins its leaf after the failed call")
			assert.False(t, cur.Valid())
			assert.ErrorIs(t, cur.Next(), errInjectedSpillIO, "the cursor steps on after the failed call")
			_, err = cur.Key()
			assert.ErrorIs(t, err, errInjectedSpillIO)

			require.NoError(t, tx.Rollback())
			f.disarm()
			require.NoError(t, db.IntegrityCheck())
			require.Equal(t, 20, countKeys(t, db, "b"))
		})
	}
}

func mustNS(t *testing.T, tx *WriteTx, name string) *Namespace {
	t.Helper()
	ns, err := tx.GetNamespace(name)
	require.NoError(t, err)
	return ns
}

// A rename reads its new catalog entry back. With a name whose overflow
// chain is longer than the writer cache, that read needs pages the cache
// has no room for, and the spill that makes room can fail: the rename
// reports the spill's error, as every other step of it does, and not
// corruption.
func TestSpillFault_RenameReadBack(t *testing.T) {
	newName := strings.Repeat("n", 3*closeRescueCache*4096)
	open := func(t *testing.T) (*DB, *spillFaultFile) {
		path := filepath.Join(t.TempDir(), "rename.db")
		db, err := testOpen(t, path, Options{PageSize: 4096, CacheSize: closeRescueCache, DisableAutoCheckpoint: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		createNS(t, db, "old")
		putN(t, db, "old", 10, 100)
		f := &spillFaultFile{File: db.pager.wal.file}
		db.pager.wal.file = f
		return db, f
	}

	db, f := open(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	before := f.writes.Load()
	require.NoError(t, tx.RenameNamespace("old", newName))
	total := f.writes.Load() - before
	require.NoError(t, tx.Rollback())
	require.Greater(t, total, int64(closeRescueCache), "the rename spills")

	for nth := int64(1); nth <= total; nth++ {
		t.Run(fmt.Sprintf("write=%d", nth), func(t *testing.T) {
			db, f := open(t)
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			f.arm(nth, false, false)
			err = tx.RenameNamespace("old", newName)
			require.Equal(t, int64(1), f.fired.Load(), "the fault fired")
			require.ErrorIs(t, err, errInjectedSpillIO)
			require.NotErrorIs(t, err, ErrCorrupt)
			require.NoError(t, tx.Rollback())
			f.disarm()
			require.Equal(t, 10, countKeys(t, db, "old"))
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// Close with a transaction that earlier spills and then a failed one left
// in the error state: Close ends it at once, its spilled frames go with
// it, and the abandoned writer's next write reports ErrClosed.
func TestSpillFault_CloseInErrorState(t *testing.T) {
	const (
		baseKeys = 150
		nOps     = 400
		seed     = 7
	)
	total := spillWrites(t, 32, baseKeys, seed, nOps, false)
	s := openSpillFaultDB(t, 32, baseKeys)
	p := s.db.pager
	tx, err := s.db.BeginWrite()
	require.NoError(t, err)
	base := p.wal.nFrame.Load()
	s.file.arm(total/2, false, false)
	var opErr error
	for _, op := range s.spillOps(seed, nOps, baseKeys) {
		if opErr = s.apply(tx, op); opErr != nil {
			break
		}
	}
	require.ErrorIs(t, opErr, errInjectedSpillIO)
	require.Greater(t, p.wal.nFrame.Load(), base, "no spill succeeded before the failed one")
	require.Equal(t, int32(pagerError), p.state.Load())

	db := s.db
	s.db = nil
	start := time.Now()
	require.NoError(t, db.Close())
	require.Less(t, time.Since(start), time.Second, "Close waited on the write lock of the transaction it ends")
	require.ErrorIs(t, tx.Put(s.ns, spillKey(1<<30), []byte("x")), ErrClosed)
	_ = tx.Rollback()

	s.file.disarm()
	db, err = testOpen(t, s.path, Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.True(t, verifySpillModel(t, db, s.model, "after Close and reopen"))
}
