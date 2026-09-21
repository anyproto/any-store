package anystore

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

func TestIndexFormat_TableMatchesVersion(t *testing.T) {
	assert.Equal(t, indexFormatVersion, len(indexFormatChanges))
}

func TestIndexFormat_Outdated(t *testing.T) {
	plain := IndexInfo{Fields: []string{"a", "-b"}}
	sparse := IndexInfo{Fields: []string{"a"}, Sparse: true}
	dotted := IndexInfo{Fields: []string{"a", "b.c"}}
	for _, tc := range []struct {
		name   string
		stored int
		info   IndexInfo
		want   bool
	}{
		{"unversioned plain", 0, plain, false},
		{"unversioned sparse", 0, sparse, true},
		{"unversioned dotted", 0, dotted, true},
		{"current sparse", indexFormatVersion, sparse, false},
		{"current dotted", indexFormatVersion, dotted, false},
		{"newer build", indexFormatVersion + 1, sparse, false},
	} {
		assert.Equal(t, tc.want, indexFormatOutdated(tc.stored, tc.info), tc.name)
	}
}

// legacyIndex makes an index look built by a pre-versioning release: the
// catalog record loses its stamp, the entries are cleared (so a rebuild is
// observable through Len), and the multikey marker is set (a rebuild resets
// it from the data).
func legacyIndex(t *testing.T, c *collection, name string) {
	t.Helper()
	var idx *index
	for _, i := range c.loadIndexes() {
		if i.info.Name == name {
			idx = i
		}
	}
	require.NotNil(t, idx)
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		key := indexKey(c.name, name)
		raw, err := tx.AppendValue(c.db.systemNS, key, nil)
		if err != nil {
			return err
		}
		var p anyenc.Parser
		v, err := p.Parse(raw)
		if err != nil {
			return err
		}
		v.Del("v")
		if err = tx.Put(c.db.systemNS, key, v.MarshalTo(nil)); err != nil {
			return err
		}
		if err = tx.Put(c.db.systemNS, multikeyKey(idx.ns.Name()), mkValMultiKey); err != nil {
			return err
		}
		cursor := tx.NewCursor(idx.ns)
		defer cursor.Close()
		var keys [][]byte
		for err = cursor.First(); err == nil && cursor.Valid(); err = cursor.Next() {
			k, kErr := cursor.Key()
			if kErr != nil {
				return kErr
			}
			keys = append(keys, append([]byte(nil), k...))
		}
		if err != nil {
			return err
		}
		for _, k := range keys {
			if err = tx.Delete(idx.ns, k); err != nil {
				return err
			}
		}
		return nil
	}))
}

func readIndexFormat(t *testing.T, c *collection, name string) int {
	t.Helper()
	var format int
	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		format, err = c.db.readIndexFormat(tx, c.name, name)
		return err
	}))
	return format
}

func readMultikey(t *testing.T, c *collection, name string) []byte {
	t.Helper()
	var mk []byte
	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		mk, err = tx.AppendValue(c.db.systemNS, multikeyKey(indexNsName(c.name, name)), nil)
		return err
	}))
	return mk
}

const legacyDocs = 20

// legacyFixture builds a collection with a sparse, a dotted-path and a plain
// index, all made to look pre-versioning, and closes the db. The caller
// reopens it.
func legacyFixture(t *testing.T) (dir string) {
	dir = t.TempDir()
	fx := newFixturePath(t, dir)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		IndexInfo{Name: "sparse", Fields: []string{"a"}, Sparse: true},
		IndexInfo{Name: "dotted", Fields: []string{"b.c"}},
		IndexInfo{Name: "plain", Fields: []string{"a"}},
	))
	for i := 0; i < legacyDocs; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":{"c":%d}}`, i, i, i))))
	}
	c := coll.(*collection)
	for _, name := range []string{"sparse", "dotted", "plain"} {
		legacyIndex(t, c, name)
		assert.Equal(t, 0, readIndexFormat(t, c, name))
	}
	require.NoError(t, fx.Close())
	return dir
}

func assertRebuilt(t *testing.T, coll Collection, name string) {
	t.Helper()
	c := coll.(*collection)
	for _, idx := range c.loadIndexes() {
		if idx.info.Name == name {
			assertIndexLen(t, idx, legacyDocs)
		}
	}
	assert.Equal(t, indexFormatVersion, readIndexFormat(t, c, name), name)
	assert.Equal(t, mkValScalar, readMultikey(t, c, name), name)
}

func assertUntouched(t *testing.T, coll Collection, name string) {
	t.Helper()
	c := coll.(*collection)
	for _, idx := range c.loadIndexes() {
		if idx.info.Name == name {
			assertIndexLen(t, idx, 0)
		}
	}
	assert.Equal(t, 0, readIndexFormat(t, c, name), name)
	assert.Equal(t, mkValMultiKey, readMultikey(t, c, name), name)
}

func TestIndexFormat_RebuildOnOpen(t *testing.T) {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("reopens a file-backed db")
	}
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)

	assertRebuilt(t, coll, "sparse")
	assertRebuilt(t, coll, "dotted")
	assertUntouched(t, coll, "plain")

	// The rebuilt indexes answer queries again.
	n, err := coll.Find(`{"a":{"$exists":true}}`).IndexHint(IndexHint{IndexName: "sparse"}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, legacyDocs, n)
	n, err = coll.Find(`{"b.c":{"$gte":10}}`).IndexHint(IndexHint{IndexName: "dotted"}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, legacyDocs-10, n)

	// A second open in the same process is a cache hit: nothing to redo.
	again, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assert.Same(t, coll, again)

	// The stamp persists: a fresh process finds nothing outdated.
	require.NoError(t, fx.Close())
	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assert.Empty(t, coll.(*collection).outdatedIndexes)
	assertRebuilt(t, coll, "sparse")
	assertUntouched(t, coll, "plain")
}

func TestIndexFormat_RebuildInsideWriteTx(t *testing.T) {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("reopens a file-backed db")
	}
	t.Run("rollback re-arms, next open rebuilds", func(t *testing.T) {
		dir := legacyFixture(t)
		fx := newFixturePath(t, dir)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		coll, err := fx.OpenCollection(tx.Context(), "test")
		require.NoError(t, err)
		c := coll.(*collection)
		for _, idx := range c.loadIndexes() {
			if idx.info.Name == "sparse" {
				n, lErr := idx.Len(tx.Context())
				require.NoError(t, lErr)
				assert.Equal(t, legacyDocs, n)
			}
		}
		require.NoError(t, tx.Rollback())

		assert.True(t, c.rebuildPending.Load())
		assert.Equal(t, 0, readIndexFormat(t, c, "sparse"))
		again, err := fx.OpenCollection(ctx, "test")
		require.NoError(t, err)
		assert.Same(t, coll, again)
		assert.False(t, c.rebuildPending.Load())
		assertRebuilt(t, coll, "sparse")
		assertRebuilt(t, coll, "dotted")
		assertUntouched(t, coll, "plain")
	})
	t.Run("commit", func(t *testing.T) {
		dir := legacyFixture(t)
		fx := newFixturePath(t, dir)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		coll, err := fx.OpenCollection(tx.Context(), "test")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		assertRebuilt(t, coll, "sparse")
		assertRebuilt(t, coll, "dotted")
		assertUntouched(t, coll, "plain")
	})
}

func TestIndexFormat_RebuildInsideReadTx(t *testing.T) {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("reopens a file-backed db")
	}
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	coll, err := fx.OpenCollection(rtx.Context(), "test")
	require.NoError(t, err)
	// The reader's snapshot predates the rebuild: it still answers, without
	// the rebuilt index.
	n, err := coll.Find(`{"a":{"$exists":true}}`).Count(rtx.Context())
	require.NoError(t, err)
	assert.Equal(t, legacyDocs, n)
	exp, err := coll.Find(`{"a":{"$exists":true}}`).Explain(rtx.Context())
	require.NoError(t, err)
	assert.NotContains(t, exp.Plan, "sparse")
	require.NoError(t, rtx.Commit())

	assertRebuilt(t, coll, "sparse")
	assertRebuilt(t, coll, "dotted")
	assertUntouched(t, coll, "plain")
	exp, err = coll.Find(`{"a":{"$exists":true}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exp.Plan, "sparse")
}

func TestIndexFormat_RebuildUniqueViolationFailsOpen(t *testing.T) {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("reopens a file-backed db")
	}
	dir := t.TempDir()
	fx := newFixturePath(t, dir)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "u", Fields: []string{"a"}, Sparse: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":null}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":null}`)))
	c := coll.(*collection)
	legacyIndex(t, c, "u")
	// A pre-versioning unique sparse index skipped the two explicit nulls; the
	// current format holds them and finds them duplicate.
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		key := indexKey(c.name, "u")
		raw, err := tx.AppendValue(c.db.systemNS, key, nil)
		if err != nil {
			return err
		}
		var p anyenc.Parser
		v, err := p.Parse(raw)
		if err != nil {
			return err
		}
		var a anyenc.Arena
		v.Set("unique", a.NewTrue())
		return tx.Put(c.db.systemNS, key, v.MarshalTo(nil))
	}))
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	_, err = fx.OpenCollection(ctx, "test")
	require.ErrorIs(t, err, ErrIndexRebuild)
	require.ErrorIs(t, err, ErrUniqueConstraint)
	assert.Contains(t, err.Error(), "test.u")
}
