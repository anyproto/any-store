package vivf

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Rebuild of an index whose documents were all deleted must not panic (kmeans
// on an empty set): it keeps the trained codebooks, clears the data, and the
// store stays usable for searches and inserts.
func TestRebuildEmptiedIndex(t *testing.T) {
	const dim = 8
	vecs := clusteredVecs(50, dim, 4, 1)
	db := openMem(t)
	p := StoreParams{Dim: dim, NList: 4, M: 4, Assign: 1, NProbe: 4, Seed: 1}
	buildStore(t, db, p, vecs)

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ix, err := OpenTx(&wtx.ReadTx, "ivf")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		_, err = ix.Delete(wtx, bid(i))
		require.NoError(t, err)
	}
	require.NoError(t, wtx.Commit())

	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	ix, err = Rebuild(wtx, "ivf")
	require.NoError(t, err, "Rebuild of an emptied index must not fail")
	// empty search works
	cands, err := ix.SearchCandidates(&wtx.ReadTx, vecs[0], 10)
	require.NoError(t, err)
	require.Empty(t, cands)
	require.NoError(t, wtx.Commit())

	// inserts into the emptied store still work (old centroids retained)
	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	ix, err = OpenTx(&wtx.ReadTx, "ivf")
	require.NoError(t, err)
	require.NoError(t, ix.Insert(wtx, bid(777), vecs[3]))
	require.NoError(t, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ix, err = OpenTx(rtx, "ivf")
	require.NoError(t, err)
	cands, err = ix.SearchCandidates(rtx, vecs[3], 10)
	require.NoError(t, err)
	require.NotEmpty(t, cands)
	require.Equal(t, bid(777), cands[0].DocID)
}

// BulkBuild with no vectors must error, not panic.
func TestBulkBuildEmptyErrors(t *testing.T) {
	db := openMem(t)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	defer wtx.Rollback()
	_, err = BulkBuild(wtx, "ivf", StoreParams{Dim: 8, NList: 4, M: 4}, nil, nil)
	require.Error(t, err)
}
