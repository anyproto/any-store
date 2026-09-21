package anystore

import (
	"context"
	"fmt"
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

// legacyIndex makes an index look built by a pre-versioning release: the
// catalog record loses its stamp, the entries are cleared (so a rebuild is
// observable through Len), and the multikey marker is set (a rebuild resets
// it from the data). The tx marks the schema changed, so another handle on
// the file reconciles like after a peer's DDL.
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
		tx.MarkSchemaChanged()
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

// setIndexRecord rewrites one field of an index's catalog record.
func setIndexRecord(t *testing.T, c *collection, name, field string, val func(a *anyenc.Arena) *anyenc.Value) {
	t.Helper()
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
		var a anyenc.Arena
		v.Set(field, val(&a))
		return tx.Put(c.db.systemNS, key, v.MarshalTo(nil))
	}))
}

func TestIndexFormat_RebuildOnOpen(t *testing.T) {
	skipIfInMemory(t)
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

	// The stamp persists: the next Open finds nothing to do and the plain
	// index is still as the old release left it.
	require.NoError(t, fx.Close())
	fx = newFixturePath(t, dir)
	coll, err = fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	assertUntouched(t, coll, "plain")
}

// The rebuild happens in Open, so opening a collection never starts a write
// transaction: a caller holding one and opening with a plain ctx returns.
func TestIndexFormat_OpenCollectionUnderWriteTxReturns(t *testing.T) {
	skipIfInMemory(t)
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
	skipIfInMemory(t)
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
	skipIfInMemory(t)
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
	skipIfInMemory(t)
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
	skipIfInMemory(t)
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
		return tx.Put(c.ns, []byte("zzz"), anyenc.MustParseJson(`{"a":1}`).MarshalTo(nil))
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
	skipIfInMemory(t)
	dir := legacyFixture(t)
	fx := newFixturePath(t, dir)
	bad, err := fx.CreateCollection(ctx, "bad")
	require.NoError(t, err)
	require.NoError(t, bad.EnsureIndex(ctx, IndexInfo{Name: "plain", Fields: []string{"a"}}))
	c := bad.(*collection)
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Put(c.db.systemNS, indexKey("bad", "plain"), []byte{0xfe, 0xfe, 0xfe})
	}))
	require.NoError(t, fx.Close())

	fx = newFixturePath(t, dir)
	coll, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)
	assertRebuilt(t, coll, "sparse")
	_, err = fx.OpenCollection(ctx, "bad")
	require.Error(t, err)
}

// Full-text indexes are outside the format: no stamp, no rebuild, still answering.
func TestIndexFormat_FulltextUntouched(t *testing.T) {
	skipIfInMemory(t)
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
	skipIfInMemory(t)
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
	skipIfInMemory(t)
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
