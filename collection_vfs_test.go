//go:build vfs

package anystore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
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
