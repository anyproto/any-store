package btree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDeleteNamespaceMasterRootCollapse sweeps namespace-create/delete layouts
// whose final master-leaf merge lands in the window where the child cannot
// absorb page 1's 100-byte header offset, so collapseSingleChild must skip the
// balance-shallower collapse (SQLite's hdrOffset<=nFree gate, btree.c:8942)
// instead of overwriting cell content. The degenerate 0-cell interior root
// this leaves behind must stay fully readable and self-heal.
func TestDeleteNamespaceMasterRootCollapse(t *testing.T) {
	for _, tc := range []struct {
		n       int
		chunk   int
		pattern string
	}{
		{259, 64, "spaceA.1_c%03d"},
		{259, 1, "spaceA.1_c%03d"},
		{280, 32, "sp.%05d"},
		{300, 64, "spaceA.1_c%03d"},
		{259, 64, "spaceA.1_c%04d"},
	} {
		t.Run(fmt.Sprintf("n%d_chunk%d_%s", tc.n, tc.chunk, tc.pattern), func(t *testing.T) {
			db := tempDBWithPageSize(t, 4096)

			names := make([]string, tc.n)
			for i := range names {
				names[i] = fmt.Sprintf(tc.pattern, i)
			}
			for _, name := range names {
				tx, err := db.BeginWrite()
				require.NoError(t, err)
				_, err = tx.CreateNamespace(name)
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
			}

			for start := 0; start < tc.n; start += tc.chunk {
				end := min(start+tc.chunk, tc.n)
				tx, err := db.BeginWrite()
				require.NoError(t, err)
				for i := start; i < end; i++ {
					require.NoError(t, tx.DeleteNamespace(names[i]), "delete #%d (%s)", i, names[i])
				}
				require.NoError(t, tx.Commit())

				// Every remaining namespace must stay resolvable and the full
				// catalog listable, including through a committed 0-cell
				// interior master root (the skipped collapse shape).
				listed, lErr := db.ListNamespaces()
				require.NoError(t, lErr)
				require.Len(t, listed, tc.n-end)
				if end < tc.n {
					_, gErr := db.GetNamespace(names[end])
					require.NoError(t, gErr)
				}
			}

			require.NoError(t, db.IntegrityCheck())
		})
	}
}
