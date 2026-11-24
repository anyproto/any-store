//go:build vfs

package anystore

// A read fault harness for the callers above the b-tree. eachReadFault runs
// one operation once per page read the operation makes, with that one read
// failing, and the tests hold every run to the same rule: the operation
// returns the read error and commits nothing, or the failed read was none
// of its business and its result is complete.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

var errInjectedRead = errors.New("injected read failure")

// readFaults fails one read of the database file at path: the failAt-th
// (from 0) of those made inside during. The reads after it succeed, so an
// operation that drops the error and goes on finds the file healthy.
type readFaults struct {
	path   string
	failAt int64 // -1: count only
	err    error // what the read that fails returns
	armed  atomic.Bool
	reads  atomic.Int64
	fired  atomic.Bool
	stack  atomic.Pointer[string] // of the read that failed
}

type readFaultFile struct {
	File
	rf *readFaults
}

func (f *readFaultFile) ReadAt(b []byte, off int64) (int, error) {
	if rf := f.rf; rf.armed.Load() {
		if rf.reads.Add(1)-1 == rf.failAt {
			rf.fired.Store(true)
			stack := string(debug.Stack())
			rf.stack.Store(&stack)
			return 0, rf.err
		}
	}
	return f.File.ReadAt(b, off)
}

// firedIn reports that the read that failed was made by fn, a function
// name as a stack trace prints it ("(*index).markMultiKey").
func (rf *readFaults) firedIn(fn string) bool {
	stack := rf.stack.Load()
	return stack != nil && strings.Contains(*stack, fn)
}

// during runs op with the fault armed.
func (rf *readFaults) during(op func() error) error {
	rf.armed.Store(true)
	defer rf.armed.Store(false)
	return op()
}

// buildImage builds a database with build and returns its file, checkpointed
// and closed: a copy of it opens with nothing of it cached.
func buildImage(t *testing.T, build func(t *testing.T, db DB)) []byte {
	t.Helper()
	pristine := filepath.Join(t.TempDir(), "pristine.db")
	db, err := Open(ctx, pristine, nil)
	require.NoError(t, err)
	build(t, db)
	require.NoError(t, db.Close())
	image, err := os.ReadFile(pristine)
	require.NoError(t, err)
	return image
}

// eachReadFault builds a database with build, then runs body on a fresh
// copy of it once without a fault, to count the reads body makes inside
// rf.during, and once per such read with that read failing. body gets the
// copy just opened, so nothing of it is cached, and may close and reopen it
// at rf.path.
func eachReadFault(t *testing.T, build func(t *testing.T, db DB), body func(t *testing.T, db DB, rf *readFaults)) {
	eachReadFaultOf(t, errInjectedRead, build, body)
}

// eachReadFaultOf is eachReadFault with fault as what the failing read
// returns.
func eachReadFaultOf(t *testing.T, fault error, build func(t *testing.T, db DB), body func(t *testing.T, db DB, rf *readFaults)) {
	image := buildImage(t, build)
	run := func(t *testing.T, failAt int64) int64 {
		rf := &readFaults{path: filepath.Join(t.TempDir(), "copy.db"), failAt: failAt, err: fault}
		require.NoError(t, os.WriteFile(rf.path, image, 0o600))
		SetVFS(VFS{OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil || name != rf.path {
				return f, err
			}
			return &readFaultFile{File: f, rf: rf}, nil
		}})
		defer ResetVFS()
		db, err := Open(ctx, rf.path, nil)
		require.NoError(t, err)
		defer func() {
			if err := db.Close(); err != nil && !errors.Is(err, ErrDBIsClosed) {
				t.Error(err)
			}
		}()
		body(t, db, rf)
		return rf.reads.Load()
	}
	total := run(t, -1)
	require.NotZero(t, total, "the operation read no page: there is no read to fail")
	for n := int64(0); n < total; n++ {
		t.Run(fmt.Sprintf("read %d of %d", n, total), func(t *testing.T) { run(t, n) })
	}
}

// reopenAt closes db and opens the file at path again, with no fault armed.
func reopenAt(t *testing.T, db DB, path string) DB {
	t.Helper()
	require.NoError(t, db.Close())
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := db.Close(); err != nil && !errors.Is(err, ErrDBIsClosed) {
			t.Error(err)
		}
	})
	return db
}

// reopened closes db and opens the file of rf again, with no fault armed.
func reopened(t *testing.T, db DB, rf *readFaults) DB {
	t.Helper()
	return reopenAt(t, db, rf.path)
}

func userEmail(i int) string { return fmt.Sprintf("user%05d@test.com", i) }

// usersBuild fills a collection "users" with n documents {id, email, n, m}
// under a unique index on email and a plain one on n.
func usersBuild(n int) func(t *testing.T, db DB) {
	return func(t *testing.T, db DB) {
		coll, err := db.CreateCollection(ctx, "users")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}, IndexInfo{Fields: []string{"n"}}))
		for i := 0; i < n; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"email":%q,"n":%d,"m":%d}`, i, userEmail(i), i, n-i))))
		}
	}
}

// countOf is the number of documents cond matches.
func countOf(t *testing.T, coll Collection, cond string) int {
	t.Helper()
	n, err := coll.Find(cond).Count(ctx)
	require.NoError(t, err)
	return n
}
