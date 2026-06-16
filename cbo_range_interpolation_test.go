package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// TestCBO_RangeInterpolation_DenseIndex exercises the end-to-end plan-time
// B-tree page interpolation: on a DENSE index (every doc indexed), a selective
// range must use the index while a broad range must stay on a full scan. Before
// interpolation, every range fell back to DefaultRangeSelectivity (0.5) and the
// selective case wrongly chose a full scan.
func TestCBO_RangeInterpolation_DenseIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// 5000 docs with a = 0..4999 (uniform, dense index).
	const n = 5000
	docs := make([]*anyenc.Value, 0, n)
	for i := 0; i < n; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	t.Run("selective range uses index", func(t *testing.T) {
		// a < 50  → ~1% of the collection. Index must win.
		explain, err := coll.Find(`{"a":{"$lt":50}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "Index",
			"selective range must use the index, got: %s", explain.Sql)
	})

	t.Run("selective high range uses index", func(t *testing.T) {
		// a > 4950 → ~1% of the collection (one-sided $gt). Index must win.
		explain, err := coll.Find(`{"a":{"$gt":4950}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "Index",
			"selective one-sided range must use the index, got: %s", explain.Sql)
	})

	t.Run("broad range stays full scan", func(t *testing.T) {
		// a > 50 → ~99% of the collection. Random-fetching ~all docs via the index
		// is far costlier than a sequential scan; full scan must win.
		explain, err := coll.Find(`{"a":{"$gt":50}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan",
			"broad range must stay on full scan, got: %s", explain.Sql)
	})
}
