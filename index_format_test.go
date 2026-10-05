package anystore

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

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
	shared := IndexInfo{Fields: []string{"items.sku", "items.qty"}}
	for _, tc := range []struct {
		name   string
		stored int
		info   IndexInfo
		want   bool
	}{
		{"unversioned plain", 0, plain, false},
		{"unversioned sparse", 0, sparse, true},
		{"unversioned dotted", 0, dotted, true},
		{"unversioned shared array", 0, shared, true},
		{"current sparse", indexFormatVersion, sparse, false},
		{"current dotted", indexFormatVersion, dotted, false},
		{"newer build", indexFormatVersion + 1, sparse, true},
		{"newer build plain", indexFormatVersion + 1, plain, true},
	} {
		assert.Equal(t, tc.want, indexFormatOutdated(tc.stored, tc.info), tc.name)
	}
}

// A ddlTx commits catalog edits as schema-changing DDL of some build.
type ddlTx func(t *testing.T, d *db, do func(tx *btree.WriteTx) error)

// thisBuildTx commits through this build's commit path: the index format
// stamp follows the schema cookie, so the next Open trusts it.
func thisBuildTx(t *testing.T, d *db, do func(tx *btree.WriteTx) error) {
	t.Helper()
	require.NoError(t, d.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		tx.MarkSchemaChanged()
		return do(tx)
	}))
}

// otherBuildTx commits as a build that does not maintain the index format
// stamp: a raw btree transaction bumps the schema cookie without the commit
// hook, leaving the stamp behind, so the next Open walks the catalog.
func otherBuildTx(t *testing.T, d *db, do func(tx *btree.WriteTx) error) {
	t.Helper()
	tx, err := d.btreeDB.BeginWrite()
	require.NoError(t, err)
	tx.MarkSchemaChanged()
	if err = do(tx); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	require.NoError(t, tx.Commit())
}

// readSettled returns the format settled record.
func readSettled(t *testing.T, d *db) (format int, cookie uint32, ok bool) {
	t.Helper()
	require.NoError(t, d.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		format, cookie, ok, err = d.readFormatSettled(tx)
		return err
	}))
	return format, cookie, ok
}

// assertSettled checks that the settled record holds the current format at
// the database's current schema cookie, so the next Open skips the walk.
func assertSettled(t *testing.T, fx *fixture) {
	t.Helper()
	format, cookie, ok := readSettled(t, fx.DB.(*db))
	require.True(t, ok, "no settled record")
	assert.Equal(t, indexFormatVersion, format)
	assert.Equal(t, schemaCookie(t, fx), cookie)
}

// legacyIndex makes an index look built by a pre-versioning release, as DDL
// of a build without the stamp (otherBuildTx): the catalog record loses its
// stamp, the entries are cleared (so a rebuild is observable through Len),
// and the multikey marker is set (a rebuild resets it from the data).
func legacyIndex(t *testing.T, c *collection, name string) {
	t.Helper()
	legacyIndexBy(t, c, name, otherBuildTx)
}

// legacyIndexBy is legacyIndex committing through the given build.
func legacyIndexBy(t *testing.T, c *collection, name string, commit ddlTx) {
	t.Helper()
	var idx *index
	for _, i := range c.loadIndexes() {
		if i.info.Name == name {
			idx = i
		}
	}
	require.NotNil(t, idx)
	commit(t, c.db, func(tx *btree.WriteTx) error {
		key := indexKey(c.cur().name, name)
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
	})
}

func readIndexFormat(t *testing.T, c *collection, name string) int {
	t.Helper()
	var format int
	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		format, err = c.db.readIndexFormat(tx, c.cur().name, name)
		return err
	}))
	return format
}

func readMultikey(t *testing.T, c *collection, name string) []byte {
	t.Helper()
	var mk []byte
	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		mk, err = tx.AppendValue(c.db.systemNS, multikeyKey(indexNsName(c.cur().name, name)), nil)
		return err
	}))
	return mk
}

const legacyDocs = 20

// legacyFixture builds a collection with a sparse, a dotted-path and a plain
// index, all made to look pre-versioning, and closes the db. The caller
// reopens it, which rebuilds the sparse and dotted-path ones.
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

// formatIndex returns the collection's live handle for the named range index.
func formatIndex(t *testing.T, coll Collection, name string) *index {
	t.Helper()
	for _, idx := range coll.(*collection).loadIndexes() {
		if idx.info.Name == name {
			return idx
		}
	}
	t.Fatalf("index %q not loaded", name)
	return nil
}

func assertRebuilt(t *testing.T, coll Collection, name string) {
	t.Helper()
	c := coll.(*collection)
	idx := formatIndex(t, coll, name)
	assertIndexLen(t, idx, legacyDocs)
	assert.False(t, idx.outdated, name)
	assert.Equal(t, indexFormatVersion, readIndexFormat(t, c, name), name)
	assert.Equal(t, mkValScalar, readMultikey(t, c, name), name)
}

func assertUntouched(t *testing.T, coll Collection, name string) {
	t.Helper()
	c := coll.(*collection)
	idx := formatIndex(t, coll, name)
	assertIndexLen(t, idx, 0)
	assert.Equal(t, 0, readIndexFormat(t, c, name), name)
	assert.Equal(t, mkValMultiKey, readMultikey(t, c, name), name)
}

// schemaCookie returns the db's current schema cookie.
func schemaCookie(t *testing.T, fx *fixture) uint32 {
	t.Helper()
	var cookie uint32
	require.NoError(t, fx.DB.(*db).doReadTx(ctx, func(tx *btree.ReadTx) error {
		cookie = tx.SnapshotSchemaCookie()
		return nil
	}))
	return cookie
}

// setIndexRecord rewrites one field of an index's catalog record as DDL of
// a build without the stamp.
func setIndexRecord(t *testing.T, c *collection, name, field string, val func(a *anyenc.Arena) *anyenc.Value) {
	t.Helper()
	otherBuildTx(t, c.db, func(tx *btree.WriteTx) error {
		key := indexKey(c.cur().name, name)
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
		v.Set(field, val(&a))
		return tx.Put(c.db.systemNS, key, v.MarshalTo(nil))
	})
}

func TestIndexFormat_RebuildOnOpen(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)

	assertRebuilt(t, coll, "sparse")
	assertRebuilt(t, coll, "dotted")
	assertUntouched(t, coll, "plain")

	// The rebuilt indexes answer queries again.
	n, err := coll.Find(`{"a":{"$exists":true}}`).IndexHint(IndexHint{IndexName: "sparse", Boost: 1000000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, legacyDocs, n)
	n, err = coll.Find(`{"b.c":{"$gte":10}}`).IndexHint(IndexHint{IndexName: "dotted", Boost: 1000000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, legacyDocs-10, n)

	// The stamps persist: the next Open finds the database stamped at its
	// cookie and the plain index is still as the old release left it.
	assertSettled(t, fx)
	require.NoError(t, fx.Close())
	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	assertUntouched(t, coll, "plain")
	assertSettled(t, fx)
}

// The database stamp follows this build's own DDL, so the next Open skips
// the catalog walk: a record made to look pre-versioning through this
// build's commit path is left alone (and, as a backstop, never planned).
func TestIndexFormat_StampFollowsOwnDDL(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	assertSettled(t, fx)

	other, err := fx.CreateCollection(ctx, "other")
	require.NoError(t, err)
	require.NoError(t, other.EnsureIndex(ctx, IndexInfo{Name: "sparse", Fields: []string{"a"}, Sparse: true}))
	require.NoError(t, other.DropIndex(ctx, "sparse"))
	require.NoError(t, other.Rename(ctx, "renamed"))
	assertSettled(t, fx)

	legacyIndexBy(t, coll.(*collection), "sparse", thisBuildTx)
	assertSettled(t, fx)
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertUntouched(t, coll, "sparse")
	assert.True(t, formatIndex(t, coll, "sparse").outdated)
	assertSettled(t, fx)
}

// DDL of a build without the stamp moves the cookie past it: the next Open
// walks the catalog (here finding nothing) and stamps again.
func TestIndexFormat_StampGapWalks(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	c := coll.(*collection)
	assertSettled(t, fx)
	_, stampedAt, _ := readSettled(t, c.db)

	otherBuildTx(t, c.db, func(tx *btree.WriteTx) error {
		return tx.Put(c.db.systemNS, multikeyKey(indexNsName("test", "plain")), mkValMultiKey)
	})
	_, at, ok := readSettled(t, c.db)
	require.True(t, ok)
	assert.Equal(t, stampedAt, at)
	assert.NotEqual(t, schemaCookie(t, fx), at)
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	assertUntouched(t, coll, "plain")
	assertSettled(t, fx)
	_, at, _ = readSettled(t, coll.(*collection).db)
	assert.NotEqual(t, stampedAt, at)
}

// A stamp at another format version (a newer build's) is not trusted even
// at the current cookie: the walk runs and restamps at this version.
func TestIndexFormat_StampOtherVersionWalks(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	c := coll.(*collection)
	legacyIndexBy(t, c, "sparse", thisBuildTx)
	assertSettled(t, fx)
	cookie := schemaCookie(t, fx)
	var a anyenc.Arena
	obj := a.NewObject()
	obj.Set("v", a.NewNumberInt(indexFormatVersion+1))
	obj.Set("sc", a.NewNumberInt(int(cookie)))
	rawPut(t, c.db, formatSettledKey, obj.MarshalTo(nil))
	format, at, ok := readSettled(t, c.db)
	require.True(t, ok)
	require.Equal(t, [2]uint32{uint32(indexFormatVersion + 1), cookie}, [2]uint32{uint32(format), at})
	// This build's DDL leaves the other version's record alone.
	_, err = fx.CreateCollection(ctx, "other")
	require.NoError(t, err)
	format, at, ok = readSettled(t, c.db)
	require.True(t, ok)
	assert.Equal(t, indexFormatVersion+1, format)
	assert.Equal(t, cookie, at)
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	assertSettled(t, fx)
}

// A schema-changing tx that writes nothing (EnsureIndex of an existing
// index) commits empty: no cookie bump, no stamp rewrite.
func TestIndexFormat_EmptyDDLCommitLeavesStamp(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	assertSettled(t, fx)
	cookie := schemaCookie(t, fx)
	_, at, _ := readSettled(t, fx.DB.(*db))
	for i := 0; i < 3; i++ {
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	}
	assert.Equal(t, cookie, schemaCookie(t, fx))
	_, after, _ := readSettled(t, fx.DB.(*db))
	assert.Equal(t, at, after)
	assertSettled(t, fx)
}

// A rebuild that fails for a reason the next Open may not meet again (here
// a collection config that does not parse) leaves the file unstamped, so
// the next Open retries.
func TestIndexFormat_RetriableFailureNotStamped(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	c := coll.(*collection)
	assertRebuilt(t, coll, "sparse")
	legacyIndex(t, c, "sparse")
	otherBuildTx(t, c.db, func(tx *btree.WriteTx) error {
		return tx.Put(c.db.systemNS, collConfigKey("test"), []byte{0xfe, 0xfe, 0xfe})
	})
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	dbi := fx.DB.(*db)
	_, at, ok := readSettled(t, dbi)
	require.True(t, ok)
	assert.NotEqual(t, schemaCookie(t, fx), at, "settled over a failed rebuild")
	var format int
	require.NoError(t, dbi.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		format, err = dbi.readIndexFormat(tx, "test", "sparse")
		return err
	}))
	assert.Equal(t, 0, format)
}

// A gap a build without the record left survives this build's own DDL:
// the record moves along only while it holds the tx's begin cookie.
func TestIndexFormat_GapSurvivesOwnDDL(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	c := coll.(*collection)
	assertRebuilt(t, coll, "sparse")
	assertSettled(t, fx)

	legacyIndex(t, c, "sparse")
	_, err = fx.CreateCollection(ctx, "own")
	require.NoError(t, err)
	_, at, ok := readSettled(t, c.db)
	require.True(t, ok)
	assert.NotEqual(t, schemaCookie(t, fx), at, "own DDL closed the gap")
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	assertSettled(t, fx)
}

// Several DDL operations in one tx, a rolled-back DDL tx and a dropped
// collection all leave the record current.
func TestIndexFormat_SettledFollowsDDLTx(t *testing.T) {
	fx := newFixture(t)
	dbi := fx.DB.(*db)
	assertSettled(t, fx)

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	coll, err := fx.CreateCollection(wtx.Context(), "a")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(wtx.Context(),
		IndexInfo{Name: "x", Fields: []string{"x"}}, IndexInfo{Name: "y", Fields: []string{"y"}}))
	require.NoError(t, coll.DropIndex(wtx.Context(), "x"))
	require.NoError(t, wtx.Commit())
	assertSettled(t, fx)

	_, at, _ := readSettled(t, dbi)
	wtx, err = fx.WriteTx(ctx)
	require.NoError(t, err)
	_, err = fx.CreateCollection(wtx.Context(), "b")
	require.NoError(t, err)
	require.NoError(t, wtx.Rollback())
	_, after, _ := readSettled(t, dbi)
	assert.Equal(t, at, after)
	assertSettled(t, fx)

	require.NoError(t, coll.Drop(ctx))
	assertSettled(t, fx)
}

// A backup is a database of its own: the copy's first Open walks the
// catalog, while the source keeps trusting its record.
func TestIndexFormat_BackupNotStamped(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	legacyIndexBy(t, coll.(*collection), "sparse", thisBuildTx)
	assertSettled(t, fx)

	backupDir := t.TempDir()
	require.NoError(t, fx.Backup(ctx, filepath.Join(backupDir, "any-store-test.db")))
	bfx := newFixturePath(t, backupDir)
	bcoll, err := bfx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, bcoll, "sparse")
	assertSettled(t, bfx)
	assertUntouched(t, coll, "sparse")
}

// The rebuild happens in Open, so opening a collection never starts a write
// transaction: a caller holding one and opening with a plain ctx returns.
func TestIndexFormat_OpenCollectionUnderWriteTxReturns(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		_, oErr := fx.OpenCollection(context.Background(), "test")
		done <- oErr
	}()
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("OpenCollection blocked behind the caller's own write tx")
	}
	require.NoError(t, tx.Rollback())
}

// A stamp above the current version (a newer release wrote it) is rebuilt
// into this release's format, whatever the index shape.
func TestIndexFormat_NewerStampRebuilt(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	setIndexRecord(t, coll.(*collection), "plain", "v", func(a *anyenc.Arena) *anyenc.Value {
		return a.NewNumberInt(indexFormatVersion + 1)
	})
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "plain")
}

// An outdated index a running handle adopts after Open (a peer's
// pre-versioning DDL, reconciled at the next tx) is never planned; the query
// falls back to a scan, writes keep maintaining it, and the next Open
// rebuilds it. The peer is simulated on the handle's own db, followed by a
// forced reconcile pass.
func TestIndexFormat_OutdatedIndexNotPlanned(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	c := coll.(*collection)
	assertRebuilt(t, coll, "sparse")
	exp, err := coll.Find(`{"a":{"$exists":true}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exp.Plan, "sparse")

	legacyIndex(t, c, "sparse")
	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		c.reconcileIndexes(tx)
		return nil
	}))
	assert.True(t, formatIndex(t, coll, "sparse").outdated)

	exp, err = coll.Find(`{"a":{"$exists":true}}`).Explain(ctx)
	require.NoError(t, err)
	assert.NotContains(t, exp.Plan, "sparse")
	n, err := coll.Find(`{"a":{"$exists":true}}`).IndexHint(IndexHint{IndexName: "sparse", Boost: 1000000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, legacyDocs, n)
	// Inside a write tx as well.
	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	exp, err = coll.Find(`{"a":{"$exists":true}}`).Explain(wtx.Context())
	require.NoError(t, err)
	assert.NotContains(t, exp.Plan, "sparse")
	require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":100,"a":100}`)))
	require.NoError(t, wtx.Commit())
	assertIndexLen(t, formatIndex(t, coll, "sparse"), 1)
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	idx := formatIndex(t, coll, "sparse")
	assertIndexLen(t, idx, legacyDocs+1)
	assert.False(t, idx.outdated)
	assert.Equal(t, indexFormatVersion, readIndexFormat(t, coll.(*collection), "sparse"))
}

// A unique sparse index over documents with duplicate explicit nulls cannot
// be rebuilt: Open still succeeds, the index stays as the old release left it
// and is never planned, Stats and Explain report it, its sibling is rebuilt,
// no later Open retries, EnsureIndex with the same definition leaves it
// alone, and dropping and recreating it reports the duplicate.
func TestIndexFormat_RebuildUniqueViolationQuarantines(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := t.TempDir()
	fx := newFixturePath(t, dir)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		IndexInfo{Name: "u", Fields: []string{"a"}, Sparse: true},
		IndexInfo{Name: "s", Fields: []string{"b"}, Sparse: true},
	))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":null,"b":2}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":null,"b":3}`)))
	c := coll.(*collection)
	legacyIndex(t, c, "u")
	legacyIndex(t, c, "s")
	// A pre-versioning unique sparse index skipped the two explicit nulls; the
	// current format holds them and finds them duplicate.
	setIndexRecord(t, c, "u", "unique", func(a *anyenc.Arena) *anyenc.Value { return a.NewTrue() })
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	c = coll.(*collection)
	assert.Equal(t, 0, readIndexFormat(t, c, "u"))
	assert.Equal(t, mkValMultiKey, readMultikey(t, c, "u"))
	u := formatIndex(t, coll, "u")
	assert.True(t, u.outdated)
	assert.True(t, u.info.Unique)
	s := formatIndex(t, coll, "s")
	assert.False(t, s.outdated)
	assertIndexLen(t, s, 3)
	assert.Equal(t, indexFormatVersion, readIndexFormat(t, c, "s"))
	// A quarantined index does not keep the file unsettled.
	assertSettled(t, fx)

	exp, err := coll.Find(`{"a":{"$exists":true}}`).Explain(ctx)
	require.NoError(t, err)
	assert.NotContains(t, exp.Plan, "IndexSeek(u)")
	assert.Contains(t, exp.Indexes, IndexExplain{Name: "u"})
	n, err := coll.Find(`{"a":{"$exists":true}}`).IndexHint(IndexHint{IndexName: "u", Boost: 1000000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	exp, err = coll.Find(`{"b":{"$exists":true}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exp.Plan, "IndexSeek(s)")
	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	for _, is := range st.Indexes {
		assert.Equal(t, is.Name == "u", is.Outdated, is.Name)
	}

	// The failed attempt is recorded: the next Open neither retries nor
	// touches the schema.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "u", Fields: []string{"a"}, Unique: true, Sparse: true}))
	assert.True(t, formatIndex(t, coll, "u").outdated)
	require.NoError(t, fx.Close())
	fx = newFixturePath(t, dir)
	before := schemaCookie(t, fx)
	require.NoError(t, fx.Close())
	fx = newFixturePath(t, dir)
	assert.Equal(t, before, schemaCookie(t, fx))
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	c = coll.(*collection)
	assert.True(t, formatIndex(t, coll, "u").outdated)

	// The duplicate is reported where the index is recreated.
	require.NoError(t, coll.DropIndex(ctx, "u"))
	err = coll.EnsureIndex(ctx, IndexInfo{Name: "u", Fields: []string{"a"}, Unique: true, Sparse: true})
	require.ErrorIs(t, err, ErrUniqueConstraint)
	require.NoError(t, coll.DeleteId(ctx, 3))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "u", Fields: []string{"a"}, Unique: true, Sparse: true}))
	exp, err = coll.Find(`{"a":{"$exists":true}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exp.Plan, "IndexSeek(u)")
	assert.Equal(t, indexFormatVersion, readIndexFormat(t, c, "u"))
}

// A document this release rejects (here one without a primary key) defeats
// the rebuild the same way: quarantine, not a failed Open; the sibling and
// the other collection are rebuilt in their own transactions.
func TestIndexFormat_RebuildBadDocumentQuarantines(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	other, err := fx.CreateCollection(ctx, "other")
	require.NoError(t, err)
	require.NoError(t, other.EnsureIndex(ctx, IndexInfo{Name: "sparse", Fields: []string{"a"}, Sparse: true}))
	for i := 0; i < legacyDocs; i++ {
		require.NoError(t, other.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}
	legacyIndex(t, other.(*collection), "sparse")
	c := other.(*collection)
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Put(c.cur().ns, []byte("zzz"), anyenc.MustParseJson(`{"a":1}`).MarshalTo(nil))
	}))
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	assertRebuilt(t, coll, "dotted")
	other, err = fx.OpenCollection(ctx, "other")
	require.NoError(t, err)
	assert.True(t, formatIndex(t, other, "sparse").outdated)
	assert.Equal(t, 0, readIndexFormat(t, other.(*collection), "sparse"))
}

// A collection whose catalog record cannot be parsed does not fail the Open:
// the other collections are upgraded and opening that one reports it.
func TestIndexFormat_CorruptRecordDoesNotFailOpen(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	bad, err := fx.CreateCollection(ctx, "bad")
	require.NoError(t, err)
	require.NoError(t, bad.EnsureIndex(ctx, IndexInfo{Name: "plain", Fields: []string{"a"}}))
	c := bad.(*collection)
	otherBuildTx(t, c.db, func(tx *btree.WriteTx) error {
		return tx.Put(c.db.systemNS, indexKey("bad", "plain"), []byte{0xfe, 0xfe, 0xfe})
	})
	// The same pass has a rebuild to do.
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	legacyIndex(t, coll.(*collection), "sparse")
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	_, err = fx.OpenCollection(ctx, "bad")
	require.Error(t, err)
	// A catalog the walk could not read in full is not settled: the next
	// Open walks again.
	_, at, ok := readSettled(t, coll.(*collection).db)
	require.True(t, ok)
	assert.NotEqual(t, schemaCookie(t, fx), at, "settled over an unread collection")
}

// Full-text indexes are outside the format: no stamp, no rebuild, still answering.
func TestIndexFormat_FulltextUntouched(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "ft", Fields: []string{"txt"}, Kind: IndexKindFulltext}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"txt":"alpha beta"}`)))
	c := coll.(*collection)
	var raw []byte
	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		raw, err = tx.AppendValue(c.db.systemNS, indexKey("test", "ft"), nil)
		return err
	}))
	format, quarantined := indexFormatRecord(raw)
	assert.Equal(t, 0, format)
	assert.Equal(t, 0, quarantined)
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	n, err := coll.Find(`{"$text":{"$search":"alpha"}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

// The private rebuild handle honours a custom primary key.
func TestIndexFormat_RebuildCustomPrimaryKey(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := t.TempDir()
	fx := newFixturePath(t, dir)
	coll, err := fx.CreateCollection(ctx, "pk", CollectionOptions{PrimaryKey: "uid"})
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "sparse", Fields: []string{"a"}, Sparse: true}))
	for i := 0; i < legacyDocs; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"uid":"u%d","a":%d}`, i, i))))
	}
	legacyIndex(t, coll.(*collection), "sparse")
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "pk")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	n, err := coll.Find(`{"a":{"$gte":10}}`).IndexHint(IndexHint{IndexName: "sparse", Boost: 1000000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, legacyDocs-10, n)
}

// A failed attempt recorded by another format does not hold back a rebuild
// at this one, and a successful rebuild clears the mark.
func TestIndexFormat_QuarantineFromOtherFormatRetried(t *testing.T) {
	skipIfInMemory(t, "the index format stamp is written and the database reopened")
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	c := coll.(*collection)
	legacyIndex(t, c, "sparse")
	setIndexRecord(t, c, "sparse", "vq", func(a *anyenc.Arena) *anyenc.Value {
		return a.NewNumberInt(indexFormatVersion + 1)
	})
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	c = coll.(*collection)
	var raw []byte
	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		raw, err = tx.AppendValue(c.db.systemNS, indexKey("test", "sparse"), nil)
		return err
	}))
	_, quarantined := indexFormatRecord(raw)
	assert.Equal(t, 0, quarantined)
}
