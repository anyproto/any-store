package anystore

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// writeDB makes a closed database with enough documents to span many pages and returns its path.
func writeDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "corrupt.db")
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	for i := 0; i < 2000; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":"%05d","text":"%0200d"}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	require.NoError(t, db.Close())
	return path
}

func overwrite(t *testing.T, path string, offset int64, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(b, offset)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestErrCorrupt_OpenOfDamagedFile(t *testing.T) {
	garbage := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = 0xAB
		}
		return b
	}
	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, path string)
	}{
		{"header overwritten", func(t *testing.T, path string) { overwrite(t, path, 0, make([]byte, 100)) }},
		{"file cut in half", func(t *testing.T, path string) {
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.NoError(t, os.Truncate(path, info.Size()/2))
		}},
		{"file cut inside the header", func(t *testing.T, path string) { require.NoError(t, os.Truncate(path, 50)) }},
		{"text instead of a database", func(t *testing.T, path string) {
			require.NoError(t, os.WriteFile(path, []byte("this is not a database"), 0o600))
		}},
		{"garbage for the whole file", func(t *testing.T, path string) {
			info, err := os.Stat(path)
			require.NoError(t, err)
			overwrite(t, path, 0, garbage(int(info.Size())))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			path := writeDB(t)
			tc.damage(t, path)

			// when
			db, err := Open(ctx, path, nil)

			// then
			if err == nil {
				_ = db.Close()
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrHeaderCorrupt)
			assert.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func TestErrCorrupt_DamagedPageFailsTheReadThatMeetsIt(t *testing.T) {
	// given: pages in the middle of the file are overwritten, which Open does not read
	path := writeDB(t)
	info, err := os.Stat(path)
	require.NoError(t, err)
	b := make([]byte, 20000)
	for i := range b {
		b[i] = 0xAB
	}
	overwrite(t, path, info.Size()/2, b)

	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	coll, err := db.Collection(ctx, "docs")
	require.NoError(t, err)

	// when
	_, err = coll.Count(ctx)

	// then
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPageIntegrity)
	assert.ErrorIs(t, err, ErrCorrupt)
}

func TestErrCorrupt_NotCorruption(t *testing.T) {
	for _, err := range []error{
		nil, ErrDocNotFound, ErrDBIsClosed, ErrV1Database, ErrIncompatibleVersion,
		fmt.Errorf("open: %w", os.ErrPermission),
	} {
		assert.NotErrorIs(t, err, ErrCorrupt, "%v", err)
	}

	t.Run("an empty file is a new database", func(t *testing.T) {
		// given
		path := filepath.Join(t.TempDir(), "empty.db")
		require.NoError(t, os.WriteFile(path, nil, 0o600))

		// when
		db, err := Open(ctx, path, nil)

		// then
		require.NoError(t, err)
		require.NoError(t, db.Close())
	})
}

func TestErrCorrupt_Nesting(t *testing.T) {
	// every kind of damage is also the umbrella, with its own text kept
	for _, err := range []error{ErrHeaderCorrupt, ErrWALCorrupt, ErrPageIntegrity, ErrQuickCheckFailed} {
		assert.ErrorIs(t, fmt.Errorf("open: %w", err), ErrCorrupt, "%v", err)
	}
	assert.Equal(t, "any-store: quick check failed on db open", ErrQuickCheckFailed.Error())
	assert.Equal(t, "btree: WAL is corrupt", ErrWALCorrupt.Error())
	assert.Equal(t, "btree: page integrity check failed", ErrPageIntegrity.Error())

	// the kinds stay apart
	assert.NotErrorIs(t, ErrWALCorrupt, ErrPageIntegrity)
	assert.NotErrorIs(t, ErrPageIntegrity, ErrHeaderCorrupt)
	assert.NotErrorIs(t, ErrCorrupt, ErrHeaderCorrupt)
}

func TestOpen_NewerFormat(t *testing.T) {
	// given
	path := writeDB(t)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	// a schema format one above anything this release writes, under the magic that carries it
	copy(data[:16], "any-store v2 fmt")
	data[47] = 99
	require.NoError(t, os.WriteFile(path, data, 0o644))

	// when
	_, err = Open(ctx, path, nil)

	// then
	require.ErrorIs(t, err, ErrIncompatibleVersion)
	assert.NotErrorIs(t, err, ErrCorrupt)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, data, after, "a refused open must leave the file as it was")
}

// cyclicTreeDB makes a database with a collection "c" and an index on "k",
// both trees three levels deep, closes it, and rewrites one child pointer of
// the named tree in the file: the first child of the root's first child now
// leads back to the root. Every page stays well formed and carries a valid
// checksum; only the shape is wrong. It returns the reopened database.
func cyclicTreeDB(t *testing.T, nsName string) (DB, Collection) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cycle.db")
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "k", Fields: []string{"k"}}))
	docs := make([]*anyenc.Value, 0, 6000)
	for i := 1; i <= 6000; i++ {
		docs = append(docs, cyclicTreeDoc(i))
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	require.NoError(t, db.Flush(ctx, 0, FlushModeCheckpointFull))
	bdb := BtreeOf(db)
	pageSize := int64(bdb.PageSize())
	ns, err := bdb.GetNamespace(nsName)
	require.NoError(t, err)
	root := ns.RootPage()
	require.NoError(t, db.Close())

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	readPage := func(pgno uint32) []byte {
		b := make([]byte, pageSize)
		_, err := f.ReadAt(b, int64(pgno-1)*pageSize)
		require.NoError(t, err)
		return b
	}
	// An interior page (type 2) keeps its cell pointers from byte 12; a
	// cell starts with its left child.
	firstChildOff := func(pg []byte) uint16 {
		require.Equal(t, byte(2), pg[0], "not an interior page: the tree is too shallow")
		return binary.BigEndian.Uint16(pg[12:14])
	}
	rootPg := readPage(root)
	off := firstChildOff(rootPg)
	second := binary.BigEndian.Uint32(rootPg[off : off+4])
	secondPg := readPage(second)
	off = firstChildOff(secondPg)
	binary.BigEndian.PutUint32(secondPg[off:off+4], root)
	StampPageChecksumForTest(secondPg)
	// The page must pass its checksum: a page that fails it is refused when
	// it is read, before its child pointer is followed, and that error
	// matches ErrCorrupt too.
	require.True(t, VerifyPageChecksum(secondPg), "the patched page does not pass its checksum")
	_, err = f.WriteAt(secondPg, int64(second-1)*pageSize)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	db, err = Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err = db.Collection(ctx, "c")
	require.NoError(t, err)
	return db, coll
}

func cyclicTreeID(i int) string { return fmt.Sprintf("id-%06d-%s", i, strings.Repeat("x", 90)) }

func cyclicTreeK(i int) string { return fmt.Sprintf("k-%06d-%s", i, strings.Repeat("y", 90)) }

func cyclicTreeDoc(i int) *anyenc.Value {
	return anyenc.MustParseJson(fmt.Sprintf(`{"id":%q,"k":%q,"b":%q}`,
		cyclicTreeID(i), cyclicTreeK(i), strings.Repeat("b", 300)))
}

// endsWithin runs fn on its own goroutine and returns its error. When fn has
// not returned in the given time it panics, which ends the test binary: a
// descent that follows the cycle cannot be stopped, keeps its transaction's
// locks, and may grow memory with every turn, so nothing can run beside it.
func endsWithin(t *testing.T, d time.Duration, what string, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		panic(fmt.Sprintf("%s: %s did not return within %v", t.Name(), what, d))
	}
}

// A tree whose interior pages form a cycle is deeper than any tree can be:
// the operation that descends it reports ErrCorrupt, and neither spins nor
// keeps a lock.
func TestErrCorrupt_TreeWithACycleOfInteriorPages(t *testing.T) {
	// Document 1 has the smallest id and the smallest k: its descent takes
	// the first child at each level, into the cycle. Document 0 would sort
	// before it.
	type op struct {
		name string
		run  func(ctx context.Context, coll Collection) error
		// rolledBack checks that the failed write left nothing behind.
		rolledBack func(t *testing.T, coll Collection)
	}
	// Where the data tree is sound, document 1 is still what was inserted.
	firstDocUnchanged := func(t *testing.T, coll Collection) {
		t.Helper()
		doc, err := coll.FindId(ctx, cyclicTreeID(1))
		require.NoError(t, err)
		require.Equal(t, cyclicTreeK(1), string(doc.Value().GetStringBytes("k")))
	}
	findId := op{name: "FindId", run: func(ctx context.Context, coll Collection) error {
		_, err := coll.FindId(ctx, cyclicTreeID(1))
		return err
	}}
	insert := op{name: "Insert", run: func(ctx context.Context, coll Collection) error {
		return coll.Insert(ctx, cyclicTreeDoc(0))
	}}
	updateId := op{name: "UpdateId", run: func(ctx context.Context, coll Collection) error {
		_, err := coll.UpdateId(ctx, cyclicTreeID(1), query.MustParseModifier(`{"$set":{"k":"zzz"}}`))
		return err
	}}
	deleteId := op{name: "DeleteId", run: func(ctx context.Context, coll Collection) error {
		return coll.DeleteId(ctx, cyclicTreeID(1))
	}}
	// With the cycle in the index tree the data tree takes the write first
	// and the index write then fails: the document write must not stay.
	insertIdx, updateIdx, deleteIdx := insert, updateId, deleteId
	insertIdx.rolledBack = func(t *testing.T, coll Collection) {
		t.Helper()
		_, err := coll.FindId(ctx, cyclicTreeID(0))
		require.ErrorIs(t, err, ErrDocNotFound)
	}
	updateIdx.rolledBack = firstDocUnchanged
	deleteIdx.rolledBack = firstDocUnchanged
	for _, tc := range []struct {
		name   string
		nsName string
		ops    []op
	}{
		{"data tree", "c", []op{findId, insert, updateId, deleteId}},
		// The data tree is sound here, so FindId answers; the writes reach
		// the index tree with the entry they add or remove.
		{"index tree", indexNsName("c", "k"), []op{insertIdx, updateIdx, deleteIdx}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, coll := cyclicTreeDB(t, tc.nsName)
			for _, o := range tc.ops {
				opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				err := endsWithin(t, 3*time.Second, o.name, func() error { return o.run(opCtx, coll) })
				cancel()
				require.ErrorIs(t, err, ErrCorrupt, o.name)
				// The depth bound, not a page that fails its checksum.
				require.NotErrorIs(t, err, ErrPageIntegrity, o.name)
				if o.rolledBack != nil {
					o.rolledBack(t, coll)
				}
			}
			doc, err := coll.FindId(ctx, cyclicTreeID(6000))
			require.NoError(t, err)
			require.Equal(t, cyclicTreeID(6000), string(doc.Value().GetStringBytes("id")))
			// Close checkpoints the WAL: disk I/O, with a bound of its own.
			require.NoError(t, endsWithin(t, 2*time.Minute, "Close", db.Close))
		})
	}
}
