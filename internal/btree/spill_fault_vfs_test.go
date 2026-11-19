//go:build vfs

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errInjectedSpillIO = errors.New("injected WAL write failure")

// spillFaultFile is the log of a database whose writes fail from the
// failAt-th one on: that one alone, or all of them while sticky is set (a
// full disk).
type spillFaultFile struct {
	File
	writes atomic.Int64
	failAt atomic.Int64 // 0: never
	sticky atomic.Bool
	fired  atomic.Int64
}

func (f *spillFaultFile) WriteAt(b []byte, off int64) (int, error) {
	n := f.writes.Add(1)
	if at := f.failAt.Load(); at != 0 && (n == at || (n > at && f.sticky.Load())) {
		f.fired.Add(1)
		return 0, errInjectedSpillIO
	}
	return f.File.WriteAt(b, off)
}

func (f *spillFaultFile) arm(nthFromNow int64, sticky bool) {
	f.fired.Store(0)
	f.sticky.Store(sticky)
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
	db    *DB
	ns    *Namespace
	path  string
	file  *spillFaultFile
	model map[int][]byte
	big   []int // baseline keys whose values are on overflow chains
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

func (s *spillFaultDB) apply(tx *WriteTx, op spillOp) error {
	if op.del {
		err := tx.Delete(s.ns, spillKey(op.key))
		if errors.Is(err, ErrKeyNotFound) {
			return nil
		}
		return err
	}
	return tx.Put(s.ns, spillKey(op.key), op.val)
}

// verify is the integrity check and a read of every committed key. It
// reports failures without ending the test: it runs on guarded's goroutine.
func (s *spillFaultDB) verify(t *testing.T, what string) bool {
	t.Helper()
	if !assert.NoError(t, s.db.IntegrityCheck(), "%s: integrity check", what) {
		return false
	}
	rtx, err := s.db.BeginRead()
	if !assert.NoError(t, err, what) {
		return false
	}
	defer rtx.Rollback()
	n, err := rtx.Count(s.ns)
	ok := assert.NoError(t, err, what) && assert.Equal(t, len(s.model), n, "%s: key count", what)
	for k, want := range s.model {
		got, err := rtx.Get(s.ns, spillKey(k))
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

// guarded runs f with a deadline and turns a panic into a failure. It
// reports whether f returned: after a timeout f's goroutine may still hold
// the database.
func guarded(t *testing.T, what string, f func()) bool {
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

type spillFaultCase struct {
	nth        int64 // the write of the log that fails, counted from the transaction's begin
	sticky     bool
	savepoints bool // each operation in a savepoint, rolled back when it fails
	commit     bool // the failed transaction ends with Commit instead of Rollback
}

// runSpillFault runs one transaction under the fault and everything that
// must hold after it. It reports whether the fault fired inside an
// operation (a spill) rather than at the commit. Every check is made, a
// failed one included: what a wrong outcome leaves behind is the point.
func (s *spillFaultDB) runSpillFault(t *testing.T, c spillFaultCase, ops []spillOp) (inOp bool) {
	t.Helper()
	p := s.db.pager
	returned := guarded(t, "failed transaction", func() {
		tx, err := s.db.BeginWrite()
		if !assert.NoError(t, err) {
			return
		}
		ended := false
		defer func() {
			if !ended {
				_ = tx.Rollback()
			}
		}()
		s.file.arm(c.nth, c.sticky)
		pending := map[int][]byte{}
		deleted := map[int]bool{}
		var opErr error
		for i, op := range ops {
			sp := -1
			if c.savepoints {
				if sp, err = tx.Savepoint(); !assert.NoError(t, err) {
					return
				}
			}
			if opErr = s.apply(tx, op); opErr != nil {
				assert.ErrorIs(t, opErr, errInjectedSpillIO, "operation %d fails with the I/O error", i)
				assert.NotErrorIs(t, opErr, ErrCorrupt, "operation %d", i)
				if c.savepoints {
					// The transaction is in the error state: its savepoint
					// cannot be played back, only the whole of it.
					assert.ErrorIs(t, tx.RollbackToSavepoint(sp), errInjectedSpillIO)
				}
				break
			}
			if c.savepoints {
				if !assert.NoError(t, tx.ReleaseSavepoint(sp)) {
					return
				}
			}
			if op.del {
				deleted[op.key] = true
				delete(pending, op.key)
			} else {
				pending[op.key] = op.val
				delete(deleted, op.key)
			}
		}
		if opErr == nil {
			// The fault point is past the last spill: the commit meets it.
			ended = true
			err := tx.Commit()
			if s.file.fired.Load() != 0 {
				assert.ErrorIs(t, err, errInjectedSpillIO)
				return
			}
			if assert.NoError(t, err) {
				for k := range deleted {
					delete(s.model, k)
				}
				for k, v := range pending {
					s.model[k] = v
				}
			}
			return
		}
		inOp = true
		assert.Equal(t, int32(pagerError), p.state.Load(), "the transaction is in the error state")
		// Every later write reports the error, and nothing more is written.
		fired := s.file.fired.Load()
		assert.ErrorIs(t, tx.Put(s.ns, spillKey(1<<40), []byte("after the error")), errInjectedSpillIO)
		assert.ErrorIs(t, tx.Delete(s.ns, spillKey(0)), errInjectedSpillIO)
		_, err = tx.Savepoint()
		assert.ErrorIs(t, err, errInjectedSpillIO)
		assert.Equal(t, fired, s.file.fired.Load(), "no write of the log in the error state")
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
	})
	s.file.disarm()
	if !returned {
		s.db = nil // its goroutine is stuck in it
		return inOp
	}
	assert.Equal(t, int32(pagerOpen), p.state.Load())
	assert.Zero(t, p.writerCache.nDirty, "no dirty page outlives the transaction")
	assert.Nil(t, p.errCode)

	returned = guarded(t, "after the failed transaction", func() {
		s.verify(t, "after the failed transaction")
		// An empty transaction commits nothing.
		frames := p.wal.nFrame.Load()
		tx, err := s.db.BeginWrite()
		if !assert.NoError(t, err) {
			return
		}
		assert.NoError(t, tx.Commit())
		assert.Equal(t, frames, p.wal.nFrame.Load(), "an empty commit writes no frame")
		// A normal one commits.
		if tx, err = s.db.BeginWrite(); !assert.NoError(t, err) {
			return
		}
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
		s.reopen(t)
	})
	if !returned {
		s.db = nil
	}
	return inOp
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
// the commit: the spills.
func (s *spillFaultDB) spillWrites(t *testing.T, ops []spillOp) int64 {
	t.Helper()
	tx, err := s.db.BeginWrite()
	require.NoError(t, err)
	before := s.file.writes.Load()
	for _, op := range ops {
		require.NoError(t, s.apply(tx, op))
	}
	n := s.file.writes.Load() - before
	require.NoError(t, tx.Rollback())
	require.True(t, s.verify(t, "after the counting transaction"))
	return n
}

// The fault point swept across a transaction that spills from a 32-page
// cache, a database for each point.
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
		{"savepoints", spillFaultCase{savepoints: true}},
		{"savepoints-commit-sticky", spillFaultCase{savepoints: true, commit: true, sticky: true}},
	}
	s := openSpillFaultDB(t, 32, baseKeys)
	total := s.spillWrites(t, s.spillOps(seed, nOps, baseKeys))
	require.Greater(t, total, int64(100), "the transaction spills")
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			var inOp int
			for nth := int64(1); nth <= total; nth += total / 12 {
				t.Run(fmt.Sprintf("write=%d", nth), func(t *testing.T) {
					s := openSpillFaultDB(t, 32, baseKeys)
					c := m.c
					c.nth = nth
					if s.runSpillFault(t, c, s.spillOps(seed, nOps, baseKeys)) {
						inOp++
					}
				})
			}
			require.Greater(t, inOp, 8, "the fault fired inside an operation")
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
	)
	s := openSpillFaultDB(t, 0, baseKeys)
	require.Equal(t, defaultCacheSize, s.db.pager.writerCache.maxPages)
	total := s.spillWrites(t, s.spillOps(3, nOps, baseKeys))
	require.Greater(t, total, int64(100), "the transaction spills")
	for i, nth := range []int64{1, total / 3, total - 1} {
		s := openSpillFaultDB(t, 0, baseKeys)
		c := spillFaultCase{nth: nth, sticky: i == 1, savepoints: i == 2, commit: i == 0}
		require.True(t, s.runSpillFault(t, c, s.spillOps(3, nOps, baseKeys)), "the fault fired inside an operation")
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
