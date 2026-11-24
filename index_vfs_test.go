//go:build vfs

package anystore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// A unique-index write whose duplicate probe fails is not written. Each
// read the insert of a duplicate makes fails once in turn; a probe that
// takes its failure for "no duplicate" lets the write that follows read
// the page again and commit the second document.
func TestReadFault_UniqueProbe(t *testing.T) {
	const docs = 3000
	eachReadFault(t, usersBuild(docs), func(t *testing.T, db DB, rf *readFaults) {
		coll, err := db.OpenCollection(ctx, "users")
		require.NoError(t, err)
		dup := fmt.Sprintf(`{"id":900000,"email":%q,"n":-1,"m":-1}`, userEmail(1500))
		err = rf.during(func() error { return coll.Insert(ctx, anyenc.MustParseJson(dup)) })
		require.Error(t, err, "a second document with the unique value was inserted (fault fired: %v)", rf.fired.Load())
		if !errors.Is(err, ErrUniqueConstraint) {
			require.ErrorIs(t, err, errInjectedRead)
		}
		check := func(db DB) {
			coll, err := db.OpenCollection(ctx, "users")
			require.NoError(t, err)
			_, err = coll.FindId(ctx, 900000)
			assert.ErrorIs(t, err, ErrDocNotFound)
			assertCollCount(t, coll, docs)
			assert.Equal(t, 1, countOf(t, coll, `{"email":"`+userEmail(1500)+`"}`))
			assert.Zero(t, countOf(t, coll, `{"n":-1}`))
			for _, idx := range coll.GetIndexes() {
				assertIndexLen(t, idx, docs)
			}
		}
		check(db)
		check(reopened(t, db, rf))
	})
}

// pageFaults fails every read of one page of a database file while armed,
// and records the pages read while recording.
type pageFaults struct {
	pageSize  int64
	failPage  uint32
	armed     atomic.Bool
	fired     atomic.Bool
	recording atomic.Bool
	mu        sync.Mutex
	read      []uint32
}

type pageFaultFile struct {
	File
	pf *pageFaults
}

func (f *pageFaultFile) ReadAt(b []byte, off int64) (int, error) {
	pf := f.pf
	pgno := uint32(off/pf.pageSize) + 1
	if pf.recording.Load() {
		pf.mu.Lock()
		pf.read = append(pf.read, pgno)
		pf.mu.Unlock()
	}
	if pf.armed.Load() && pgno == pf.failPage {
		pf.fired.Store(true)
		return 0, errInjectedRead
	}
	return f.File.ReadAt(b, off)
}

// open opens a copy of image at a new path through pf.
func (pf *pageFaults) open(t *testing.T, image []byte) (DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "copy.db")
	require.NoError(t, os.WriteFile(path, image, 0o600))
	SetVFS(VFS{OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
		f, err := os.OpenFile(name, flag, perm)
		if err != nil || name != path {
			return f, err
		}
		return &pageFaultFile{File: f, pf: pf}, nil
	}})
	t.Cleanup(ResetVFS)
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := db.Close(); err != nil && !errors.Is(err, ErrDBIsClosed) {
			t.Error(err)
		}
	})
	return db, path
}

// take returns the pages read since the last take.
func (pf *pageFaults) take() []uint32 {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	read := pf.read
	pf.read = nil
	return read
}

func indexNamed(t *testing.T, coll Collection, name string) *index {
	t.Helper()
	for _, idx := range coll.(*collection).loadIndexes() {
		if idx.info.Name == name {
			return idx
		}
	}
	t.Fatalf("no index %q", name)
	return nil
}

// The probe seeks the bare field value, the write the full key with the
// document id, and at a divider the two take different paths: the divider
// is the first key of its right child, so for a value whose entry is that
// first key the probe goes left, overshoots the left leaf and reads the
// entry from the right one, while the write of the same value under a
// larger id goes right at once. With the left leaf unreadable the probe
// fails and the write does not; a probe that takes the failure for "no
// duplicate" commits a second document with the unique value. SQLite's
// OP_NoConflict aborts on a failed seek before it looks at the seek's
// result (vdbe.c:5414).
func TestReadFault_UniqueProbeDivider(t *testing.T) {
	const docs = 3000
	var pageSize int64
	image := buildImage(t, func(t *testing.T, d DB) {
		usersBuild(docs)(t, d)
		pageSize = int64(d.(*db).btreeDB.PageSize())
	})

	// The leaves of the email index in key order, from the pages a cursor
	// reads on a copy nothing of which is cached: an entry whose step
	// reads a page is the first of a new leaf. The entries are in id order.
	pf := &pageFaults{pageSize: pageSize}
	d, path := pf.open(t, image)
	coll, err := d.OpenCollection(ctx, "users")
	require.NoError(t, err)
	email := indexNamed(t, coll, "email")
	rtx, err := d.(*db).btreeDB.BeginRead()
	require.NoError(t, err)
	type leafStart struct {
		entry    int    // the first entry of the right leaf
		leftLeaf uint32 // the page of the leaf before it
	}
	var starts []leafStart
	var leaf uint32
	entries := 0
	cur := rtx.NewCursor(email.ns)
	pf.recording.Store(true)
	for err = cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		if pages := pf.take(); len(pages) > 0 {
			if leaf != 0 {
				starts = append(starts, leafStart{entries, leaf})
			}
			leaf = pages[len(pages)-1]
		}
		entries++
	}
	pf.recording.Store(false)
	require.NoError(t, err)
	cur.Close()
	require.NoError(t, rtx.Rollback())
	require.Equal(t, docs, entries)
	require.NotEmpty(t, starts, "the email index has one leaf")
	start := starts[0]
	for _, s := range starts {
		if abs(s.entry-docs/2) < abs(start.entry-docs/2) {
			start = s
		}
	}
	// Room in the right leaf for the duplicate's entry: a write into a full
	// leaf balances it with its siblings and reads the left one too.
	for i := start.entry + 1; i <= start.entry+3; i++ {
		require.NoError(t, coll.DeleteId(ctx, i))
	}
	require.NoError(t, d.Close())
	image, err = os.ReadFile(path)
	require.NoError(t, err)

	value := userEmail(start.entry)
	check := func(d DB) {
		coll, err := d.OpenCollection(ctx, "users")
		require.NoError(t, err)
		_, err = coll.FindId(ctx, 900000)
		assert.ErrorIs(t, err, ErrDocNotFound)
		assert.Equal(t, 1, countOf(t, coll, `{"email":"`+value+`"}`))
		assertCollCount(t, coll, docs-3)
		for _, idx := range coll.GetIndexes() {
			assertIndexLen(t, idx, docs-3)
		}
	}
	pf = &pageFaults{pageSize: pageSize, failPage: start.leftLeaf}
	d, path = pf.open(t, image)
	coll, err = d.OpenCollection(ctx, "users")
	require.NoError(t, err)
	pf.armed.Store(true)
	err = coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":900000,"email":%q,"n":-1,"m":-1}`, value)))
	pf.armed.Store(false)
	require.True(t, pf.fired.Load(), "the insert did not read the leaf left of the divider")
	require.ErrorIs(t, err, errInjectedRead, "the insert of a second document with the unique value %q", value)
	check(d)
	check(reopenAt(t, d, path))
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// A fan-out write reads the index's multikey flag before it sets it. A
// read of the flag that fails is the write's error: the flag record is the
// planner's licence for tight seeks, and an I/O error under it is not a
// record to overwrite and go on from. Every run whose fault hits the read
// of the flag (markMultiKey) must return that error rather than take it for
// an absent flag and commit, and at least one run must hit it. A failed
// insert leaves the collection as it was, in this process and after a
// reopen.
func TestReadFault_MultiKeyFlag(t *testing.T) {
	build := func(t *testing.T, db DB) {
		// Other collections' sketch rows sit between the flag record and the
		// sketch row of "docs", so the insert's first-use sketch load does
		// not bring the flag's page into the cache.
		for c := 0; c < 12; c++ {
			coll, err := db.CreateCollection(ctx, fmt.Sprintf("a%02d", c))
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}, IndexInfo{Fields: []string{"b"}}))
			for i := 0; i < 200; i++ {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":"%d"}`, i, i, i*7))))
			}
		}
		coll, err := db.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))
		for i := 0; i < 20; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"tags":["a%d","b%d"]}`, i, i, i))))
		}
	}
	flagReads := 0
	eachReadFault(t, build, func(t *testing.T, db DB, rf *readFaults) {
		coll, err := db.OpenCollection(ctx, "docs")
		require.NoError(t, err)
		err = rf.during(func() error { return coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"tags":["p","q"]}`)) })
		if rf.firedIn("(*index).markMultiKey") {
			flagReads++
			require.ErrorIs(t, err, errInjectedRead, "the read of the multikey flag failed and the insert went on")
		}
		if err == nil {
			assert.Equal(t, 1, countOf(t, coll, `{"tags":"p"}`))
			return
		}
		require.ErrorIs(t, err, errInjectedRead)
		check := func(db DB) {
			coll, err := db.OpenCollection(ctx, "docs")
			require.NoError(t, err)
			assertCollCount(t, coll, 20)
			assert.Zero(t, countOf(t, coll, `{"tags":"p"}`))
		}
		check(db)
		check(reopened(t, db, rf))
	})
	require.NotZero(t, flagReads, "no run failed the read of the multikey flag")
}
