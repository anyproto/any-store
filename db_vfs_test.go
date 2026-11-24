//go:build vfs

package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Stats counts the catalog or fails: a seek that fails is not a catalog
// without collections.
func TestReadFault_Stats(t *testing.T) {
	sh := ddlShape{fts: true}
	eachReadFault(t, sh.build, func(t *testing.T, db DB, rf *readFaults) {
		var stats DBStats
		err := rf.during(func() (err error) {
			stats, err = db.Stats(ctx)
			return err
		})
		if err != nil {
			require.ErrorIs(t, err, errInjectedRead)
			return
		}
		assert.Equal(t, 151, stats.CollectionsCount, "fault fired: %v", rf.fired.Load())
		assert.Equal(t, 303, stats.IndexesCount, "fault fired: %v", rf.fired.Load())
	})
}

// EnsureIndex of an index the collection has finds its catalog record or
// fails: a read of the record that fails is not an index to create. Before
// the fix it went on to register the index again over its record and
// failed on the namespace that exists, with an error that names neither.
func TestReadFault_EnsureIndexExisting(t *testing.T) {
	sh := ddlShape{fts: true}
	var before []string
	build := func(t *testing.T, db DB) {
		sh.build(t, db)
		before = catalogState(t, db)
	}
	eachReadFault(t, build, func(t *testing.T, db DB, rf *readFaults) {
		coll, err := db.OpenCollection(ctx, "docs")
		require.NoError(t, err)
		err = rf.during(func() error {
			return coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true})
		})
		if err != nil {
			require.ErrorIs(t, err, errInjectedRead)
		}
		check := func(db DB) {
			assertState(t, before, catalogState(t, db), "EnsureIndex of an existing index changed the catalog")
			sh.intact(t, db, "docs")
		}
		check(db)
		check(reopened(t, db, rf))
	})
}

// IntegrityCheck reports a catalog it cannot list: a seek that fails is not
// a catalog without collections, and the check is not clean.
func TestReadFault_IntegrityCheck(t *testing.T) {
	listReads := 0
	eachReadFault(t, ddlShape{fts: true}.build, func(t *testing.T, db DB, rf *readFaults) {
		err := rf.during(func() error { return db.IntegrityCheck(ctx) })
		if rf.firedIn("(*db).listCollectionNames") {
			listReads++
			require.ErrorIs(t, err, errInjectedRead, "the catalog read failed and the check is clean")
		}
	})
	require.NotZero(t, listReads, "no run failed a read of the catalog listing")
}
