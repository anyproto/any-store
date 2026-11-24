//go:build vfs

package anystore

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// An insert whose check for a document under the id fails does not write:
// the document under the id may exist, and the write would replace it and
// leave the index entries of its old values behind.
func TestReadFault_InsertExistenceProbe(t *testing.T) {
	const docs = 3000
	unchanged := func(t *testing.T, db DB) {
		coll, err := db.OpenCollection(ctx, "users")
		require.NoError(t, err)
		doc, err := coll.FindId(ctx, 1500)
		require.NoError(t, err)
		assert.Equal(t, userEmail(1500), doc.Value().GetString("email"))
		assertCollCount(t, coll, docs)
		assert.Zero(t, countOf(t, coll, `{"n":-5}`))
		assert.Zero(t, countOf(t, coll, `{"email":"other@test.com"}`))
		for _, idx := range coll.GetIndexes() {
			assertIndexLen(t, idx, docs)
		}
	}
	const replacement = `{"id":1500,"email":"other@test.com","n":-5,"m":-5}`
	t.Run("insert", func(t *testing.T) {
		eachReadFault(t, usersBuild(docs), func(t *testing.T, db DB, rf *readFaults) {
			coll, err := db.OpenCollection(ctx, "users")
			require.NoError(t, err)
			err = rf.during(func() error { return coll.Insert(ctx, anyenc.MustParseJson(replacement)) })
			require.Error(t, err, "an insert under an existing id succeeded (fault fired: %v)", rf.fired.Load())
			if !errors.Is(err, ErrDocExists) {
				require.ErrorIs(t, err, errInjectedRead)
			}
			unchanged(t, db)
			unchanged(t, reopened(t, db, rf))
		})
	})
	// An upsert that takes the failed check for "no such document" inserts
	// over the document: the entries of its old values stay in the indexes.
	t.Run("upsert", func(t *testing.T) {
		eachReadFault(t, usersBuild(docs), func(t *testing.T, db DB, rf *readFaults) {
			coll, err := db.OpenCollection(ctx, "users")
			require.NoError(t, err)
			err = rf.during(func() error { return coll.UpsertOne(ctx, anyenc.MustParseJson(replacement)) })
			if err != nil {
				require.ErrorIs(t, err, errInjectedRead)
				unchanged(t, db)
				unchanged(t, reopened(t, db, rf))
				return
			}
			check := func(db DB) {
				coll, err := db.OpenCollection(ctx, "users")
				require.NoError(t, err)
				assertCollCount(t, coll, docs)
				assert.Equal(t, 1, countOf(t, coll, `{"n":-5}`))
				assert.Zero(t, countOf(t, coll, `{"email":"`+userEmail(1500)+`"}`))
				for _, idx := range coll.GetIndexes() {
					assertIndexLen(t, idx, docs)
				}
			}
			check(db)
			check(reopened(t, db, rf))
		})
	})
}

// catalogKeys lists the keys of the system namespace, in order.
func catalogKeys(t *testing.T, d DB) (keys []string) {
	t.Helper()
	sdb := d.(*db)
	require.NoError(t, sdb.doReadTx(ctx, func(tx *btree.ReadTx) error {
		cursor := tx.NewCursor(sdb.systemNS)
		defer cursor.Close()
		err := cursor.First()
		for ; err == nil && cursor.Valid(); err = cursor.Next() {
			key, kerr := cursor.Key()
			if kerr != nil {
				return kerr
			}
			keys = append(keys, string(key))
		}
		return err
	}))
	return keys
}

// catalogState is what the catalog holds: the keys of the system namespace
// and the names of the namespaces, sorted.
func catalogState(t *testing.T, d DB) []string {
	t.Helper()
	state := catalogKeys(t, d)
	names, err := d.(*db).btreeDB.ListNamespaces()
	require.NoError(t, err)
	for _, name := range names {
		state = append(state, "namespace "+name)
	}
	slices.Sort(state)
	return state
}

// ddlShape is a catalog for the DDL tests: a collection "docs" under the
// primary key "key" with a unique index, an index over an array field (a
// multikey record) and, with fts, a full-text index, among other
// collections, so the catalog spans pages. The two shapes read the catalog
// in different orders, so each fails other reads.
type ddlShape struct{ fts bool }

var ddlShapes = []ddlShape{{fts: false}, {fts: true}}

func (sh ddlShape) String() string {
	if sh.fts {
		return "with full-text index"
	}
	return "range indexes"
}

func (sh ddlShape) build(t *testing.T, db DB) {
	for i := 0; i < 150; i++ {
		other, err := db.CreateCollection(ctx, fmt.Sprintf("other-collection-with-a-long-name-%03d", i), CollectionOptions{PrimaryKey: "key"})
		require.NoError(t, err)
		require.NoError(t, other.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}}, IndexInfo{Fields: []string{"tags"}}))
		require.NoError(t, other.Insert(ctx, anyenc.MustParseJson(`{"key":1,"email":"a","tags":["x","y"]}`)))
	}
	coll, err := db.CreateCollection(ctx, "docs", CollectionOptions{PrimaryKey: "key"})
	require.NoError(t, err)
	indexes := []IndexInfo{{Fields: []string{"email"}, Unique: true}, {Fields: []string{"tags"}}}
	if sh.fts {
		indexes = append(indexes, IndexInfo{Name: "body", Kind: IndexKindFulltext, Fields: []string{"body"}})
	}
	require.NoError(t, coll.EnsureIndex(ctx, indexes...))
	for i := 0; i < 50; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"key":"k%03d","email":%q,"tags":["a%d","b%d"],"body":"word%d text"}`, i, userEmail(i), i, i, i))))
	}
}

// indexes is the number of indexes of "docs".
func (sh ddlShape) indexes() int {
	if sh.fts {
		return 3
	}
	return 2
}

// intact asserts the collection "docs" is whole under name, its primary key
// included.
func (sh ddlShape) intact(t *testing.T, db DB, name string) {
	t.Helper()
	coll, err := db.OpenCollection(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, "key", coll.PrimaryKey(), "the collection kept its primary key")
	assertCollCount(t, coll, 50)
	doc, err := coll.FindId(ctx, "k007")
	require.NoError(t, err)
	assert.Equal(t, userEmail(7), doc.Value().GetString("email"))
	assert.Equal(t, 1, countOf(t, coll, `{"email":"`+userEmail(7)+`"}`))
	assert.Equal(t, 1, countOf(t, coll, `{"tags":"b7"}`))
	if sh.fts {
		assert.Equal(t, 1, countOf(t, coll, `{"$text":{"$search":"word7"}}`))
	}
	assert.Len(t, coll.GetIndexes(), sh.indexes())
}

// assertState fails with the entries of want and got, both sorted, that the
// other does not hold.
func assertState(t *testing.T, want, got []string, msg string, args ...any) {
	t.Helper()
	in := func(set []string, s string) bool {
		_, found := slices.BinarySearch(set, s)
		return found
	}
	var missing, extra []string
	for _, s := range want {
		if !in(got, s) {
			missing = append(missing, s)
		}
	}
	for _, s := range got {
		if !in(want, s) {
			extra = append(extra, s)
		}
	}
	if len(missing)+len(extra) > 0 {
		t.Errorf("%s: missing %q, unexpected %q", fmt.Sprintf(msg, args...), missing, extra)
	}
}

func replaceAll(state []string, old, new string) []string {
	out := make([]string, 0, len(state))
	for _, s := range state {
		out = append(out, strings.ReplaceAll(s, old, new))
	}
	slices.Sort(out)
	return out
}

func without(state []string, drop func(string) bool) []string {
	out := make([]string, 0, len(state))
	for _, s := range state {
		if !drop(s) {
			out = append(out, s)
		}
	}
	return out
}

// A rename moves every catalog record of the collection or none. A read
// that fails while it moves them fails the rename, and the catalog is as it
// was: the collection is under its old name, with the primary key its config
// record holds, in this process and after a reopen. A rename that succeeds
// has moved every record, the config record included, so the collection
// opens under the new name with that primary key, not the default one.
func TestReadFault_Rename(t *testing.T) {
	for _, sh := range ddlShapes {
		t.Run(sh.String(), func(t *testing.T) { testReadFaultRename(t, sh) })
	}
}

func testReadFaultRename(t *testing.T, sh ddlShape) {
	var before []string
	build := func(t *testing.T, db DB) {
		sh.build(t, db)
		before = catalogState(t, db)
	}
	eachReadFault(t, build, func(t *testing.T, db DB, rf *readFaults) {
		coll, err := db.OpenCollection(ctx, "docs")
		require.NoError(t, err)
		err = rf.during(func() error { return coll.Rename(ctx, "moved") })
		if err != nil {
			require.ErrorIs(t, err, errInjectedRead)
			check := func(db DB) {
				assertState(t, before, catalogState(t, db), "a failed rename changed the catalog")
				sh.intact(t, db, "docs")
				_, err := db.OpenCollection(ctx, "moved")
				assert.ErrorIs(t, err, ErrCollectionNotFound)
			}
			check(db)
			check(reopened(t, db, rf))
			return
		}
		check := func(db DB) {
			assertState(t, replaceAll(replaceAll(before, ":docs", ":moved"), "namespace docs", "namespace moved"), catalogState(t, db),
				"the rename moved every record (fault fired: %v)", rf.fired.Load())
			sh.intact(t, db, "moved")
			_, err := db.OpenCollection(ctx, "docs")
			assert.ErrorIs(t, err, ErrCollectionNotFound)
		}
		check(db)
		check(reopened(t, db, rf))
	})
}

// A drop removes every catalog record of the collection or none.
func TestReadFault_Drop(t *testing.T) {
	for _, sh := range ddlShapes {
		t.Run(sh.String(), func(t *testing.T) { testReadFaultDrop(t, sh) })
	}
}

func testReadFaultDrop(t *testing.T, sh ddlShape) {
	var before []string
	build := func(t *testing.T, db DB) {
		sh.build(t, db)
		before = catalogState(t, db)
	}
	eachReadFault(t, build, func(t *testing.T, db DB, rf *readFaults) {
		coll, err := db.OpenCollection(ctx, "docs")
		require.NoError(t, err)
		err = rf.during(func() error { return coll.Drop(ctx) })
		if err != nil {
			require.ErrorIs(t, err, errInjectedRead)
			check := func(db DB) {
				assertState(t, before, catalogState(t, db), "a failed drop changed the catalog")
				sh.intact(t, db, "docs")
			}
			check(db)
			check(reopened(t, db, rf))
			return
		}
		ofDocs := func(s string) bool {
			return strings.HasSuffix(s, ":docs") || strings.Contains(s, ":docs:") || s == "namespace docs"
		}
		check := func(db DB) {
			assertState(t, without(before, ofDocs), catalogState(t, db), "the drop removed every record (fault fired: %v)", rf.fired.Load())
			_, err := db.OpenCollection(ctx, "docs")
			assert.ErrorIs(t, err, ErrCollectionNotFound)
		}
		check(db)
		check(reopened(t, db, rf))
	})
}

// A dropped index leaves no catalog record behind, or the drop fails and
// the index is whole.
func TestReadFault_DropIndex(t *testing.T) {
	for _, sh := range ddlShapes {
		t.Run(sh.String(), func(t *testing.T) { testReadFaultDropIndex(t, sh) })
	}
}

func testReadFaultDropIndex(t *testing.T, sh ddlShape) {
	var before []string
	build := func(t *testing.T, db DB) {
		sh.build(t, db)
		before = catalogState(t, db)
	}
	eachReadFault(t, build, func(t *testing.T, db DB, rf *readFaults) {
		coll, err := db.OpenCollection(ctx, "docs")
		require.NoError(t, err)
		err = rf.during(func() error { return coll.DropIndex(ctx, "tags") })
		if err != nil {
			require.ErrorIs(t, err, errInjectedRead)
			check := func(db DB) {
				assertState(t, before, catalogState(t, db), "a failed index drop changed the catalog")
				sh.intact(t, db, "docs")
			}
			check(db)
			check(reopened(t, db, rf))
			return
		}
		ofIndex := func(s string) bool { return strings.HasSuffix(s, ":docs:tags") }
		check := func(db DB) {
			assertState(t, without(before, ofIndex), catalogState(t, db), "the drop removed every record of the index (fault fired: %v)", rf.fired.Load())
			coll, err := db.OpenCollection(ctx, "docs")
			require.NoError(t, err)
			assert.Len(t, coll.GetIndexes(), sh.indexes()-1)
		}
		check(db)
		check(reopened(t, db, rf))
	})
}
