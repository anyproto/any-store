package anystore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// chunkedDrop creates n collections named with pattern, then drops them in
// creation order, chunkSize drops per write tx, and verifies integrity after
// the sweep. Regression for the master-table balance-shallower collapse into
// page 1 overwriting cell content when the merged child lacks the 100 free
// bytes needed to absorb the DB-header offset (collapseSingleChild guard;
// SQLite btree.c:8975 `pParent->hdrOffset<=apNew[0]->nFree`).
func chunkedDrop(t *testing.T, n, chunkSize int, pattern string) {
	fx := newFixture(t)
	ctx := context.Background()

	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf(pattern, i)
	}
	for _, name := range names {
		_, err := fx.CreateCollection(ctx, name)
		require.NoError(t, err)
	}

	for start := 0; start < n; start += chunkSize {
		end := min(start+chunkSize, n)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		for i := start; i < end; i++ {
			coll, cErr := fx.Collection(tx.Context(), names[i])
			require.NoError(t, cErr)
			require.NoError(t, coll.Drop(tx.Context()), "drop #%d (%s)", i, names[i])
		}
		require.NoError(t, tx.Commit())

		// Full catalog listing after every chunk: exercises the cursor walk
		// over the master table while its root may be a committed 0-cell
		// interior page (the skipped balance-shallower collapse).
		listed, lErr := fx.GetCollectionNames(ctx)
		require.NoError(t, lErr)
		require.Len(t, listed, n-end, "after dropping %d", end)
	}

	left, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	require.Empty(t, left)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

func TestDropSweepMasterRootCollapse(t *testing.T) {
	// 259 x %03d names land the post-merge master leaf in the <100-free-bytes
	// window where the root collapse must be skipped (drop #68 corrupted
	// page 1 before the fix).
	t.Run("chunked", func(t *testing.T) {
		chunkedDrop(t, 259, 64, "spaceA.1_c%03d")
	})
	t.Run("one tx per drop", func(t *testing.T) {
		chunkedDrop(t, 259, 1, "spaceA.1_c%03d")
	})
	t.Run("underscore names", func(t *testing.T) {
		chunkedDrop(t, 259, 128, "spaceA_1_c%03d")
	})
	// Layouts the issue reported as passing — keep them passing.
	t.Run("wide tail", func(t *testing.T) {
		chunkedDrop(t, 259, 64, "spaceA.1_c%04d")
	})
	t.Run("n300", func(t *testing.T) {
		chunkedDrop(t, 300, 64, "spaceA.1_c%03d")
	})
}
