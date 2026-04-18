package qplanner

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
)

// closeTrackingIter is a minimal Iterator whose Close call is observable.
type closeTrackingIter struct {
	closed int
}

func (c *closeTrackingIter) Next() ([]byte, []byte, error) { return nil, nil, nil }
func (c *closeTrackingIter) Close()                        { c.closed++ }
func (c *closeTrackingIter) String() string                { return "track" }

// errIter is an Iterator that always returns an error from Next.
type errIter struct{ err error }

func (e *errIter) Next() ([]byte, []byte, error) { return nil, nil, e.err }
func (e *errIter) Close()                        {}
func (e *errIter) String() string                { return "err" }

// openIsolatedBtree creates a minimal btree DB that the test can explicitly
// close to force cursor errors. Similar to coverageBtree but without the
// automatic t.Cleanup close — the test owns the lifecycle.
func openIsolatedBtree(t *testing.T, ids []string) (*btree.DB, *btree.Namespace) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "isolated.db")
	db, err := btree.Open(path, btree.Options{PageSize: 4096, CacheSize: 128, InMemory: true})
	require.NoError(t, err)

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("data")
	require.NoError(t, err)
	for _, id := range ids {
		k := anyenc.AppendAnyValue(nil, id)
		a := &anyenc.Arena{}
		obj := a.NewObject()
		obj.Set("id", a.NewString(id))
		require.NoError(t, wtx.Put(ns, k, obj.MarshalTo(nil)))
	}
	require.NoError(t, wtx.Commit())
	return db, ns
}

// invalidNamespace returns a zero-value Namespace whose rootPage points at
// the header page. Cursor ops against it fail with "invalid page number".
func invalidNamespace() *btree.Namespace { return &btree.Namespace{} }
