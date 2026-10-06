package vindex

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// Every build of an index has an identity, persisted in its meta record
// and carried by every object opened for it; a compaction is a new build.
func TestBuildIdentity(t *testing.T) {
	const dim = 8
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	defer db.Close()
	params := Params{Dim: dim, Metric: L2, EfSearch: 64}
	vecs := randVecs(20, dim, 7)
	ids := make([][]byte, len(vecs))
	for i := range ids {
		ids[i] = docID(i)
	}

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	built, err := BulkBuild(wtx, "vix", params, 1, ids, vecs)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	require.NotZero(t, built.Build())

	opened, err := Open(db, "vix", 1)
	require.NoError(t, err)
	require.Equal(t, built.Build(), opened.Build())
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	inTx, err := OpenTx(rtx, "vix", 1)
	require.NoError(t, err)
	require.Equal(t, built.Build(), inTx.Build())
	inMeta, err := BuildOf(rtx, inTx.vmeta)
	require.NoError(t, err)
	require.Equal(t, built.Build(), inMeta)
	require.NoError(t, rtx.Rollback())

	// A write leaves the build; a compaction is a new one.
	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	_, err = opened.Delete(wtx, ids[3])
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	rtx, err = db.BeginRead()
	require.NoError(t, err)
	afterWrite, err := BuildOf(rtx, opened.vmeta)
	require.NoError(t, err)
	require.NoError(t, rtx.Rollback())
	require.Equal(t, built.Build(), afterWrite)

	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	compacted, err := Compact(wtx, "vix", 1)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	require.NotZero(t, compacted.Build())
	require.NotEqual(t, built.Build(), compacted.Build())
	reopened, err := Open(db, "vix", 1)
	require.NoError(t, err)
	require.Equal(t, compacted.Build(), reopened.Build())
}
