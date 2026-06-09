package vector

import (
	"path/filepath"
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDB(t testing.TB, inMemory bool) *btree.DB {
	t.Helper()
	opts := btree.Options{InMemory: inMemory}
	path := ":memory:"
	if !inMemory {
		path = filepath.Join(t.TempDir(), "vec.db")
	}
	db, err := btree.Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestBtreeHNSWPersistAndReload(t *testing.T) {
	const (
		n   = 1500
		dim = 48
		k   = 10
	)
	vecs, keys := randVectors(n, dim, 11)
	queries, _ := randVectors(60, dim, 77)

	dir := t.TempDir()
	path := filepath.Join(dir, "vec.db")

	// ground truth
	brute := NewBrute(dim, L2)
	for i := range vecs {
		brute.Add(keys[i], vecs[i])
	}

	// Build, flush, close.
	var preReload [][]SearchResult
	{
		db, err := btree.Open(path, btree.Options{})
		require.NoError(t, err)
		idx, err := OpenBtreeHNSW(db, "emb", dim, L2)
		require.NoError(t, err)
		idx.SetEf(64)
		for i := range vecs {
			idx.Add(keys[i], vecs[i])
		}
		require.NoError(t, idx.Flush())
		require.Equal(t, n, idx.Len())
		for _, q := range queries {
			preReload = append(preReload, idx.Search(q, k))
		}
		require.NoError(t, db.Close())
	}

	// Reopen from disk — must reconstruct an identical graph.
	db, err := btree.Open(path, btree.Options{})
	require.NoError(t, err)
	defer db.Close()
	idx, err := OpenBtreeHNSW(db, "emb", dim, L2)
	require.NoError(t, err)
	idx.SetEf(64)
	require.Equal(t, n, idx.Len())

	var recall float64
	for i, q := range queries {
		post := idx.Search(q, k)
		// identical results before/after reload (same graph, deterministic search)
		require.Equal(t, len(preReload[i]), len(post), "query %d result count", i)
		for j := range post {
			assert.Equal(t, preReload[i][j].Key, post[j].Key, "query %d hit %d key after reload", i, j)
		}
		recall += recallAt(post, brute.Search(q, k))
	}
	recall /= float64(len(queries))
	t.Logf("btree-HNSW recall@%d after reload = %.3f", k, recall)
	assert.Greater(t, recall, 0.85)
}

func TestBtreeHNSWIncrementalFlush(t *testing.T) {
	const dim = 16
	db := openTestDB(t, true)
	idx, err := OpenBtreeHNSW(db, "emb", dim, L2)
	require.NoError(t, err)

	vecs, keys := randVectors(300, dim, 3)
	// flush in two batches
	for i := 0; i < 150; i++ {
		idx.Add(keys[i], vecs[i])
	}
	require.NoError(t, idx.Flush())
	for i := 150; i < 300; i++ {
		idx.Add(keys[i], vecs[i])
	}
	require.NoError(t, idx.Flush())
	require.Equal(t, 300, idx.Len())

	// reopen against the same in-memory db handle
	idx2, err := OpenBtreeHNSW(db, "emb", dim, L2)
	require.NoError(t, err)
	require.Equal(t, 300, idx2.Len())
}
