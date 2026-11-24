//go:build vfs

package vector

import (
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/internal/btree"
)

var errInjectedRead = errors.New("injected read failure")

// oneReadFails fails the failAt-th read of the wrapped file made while armed,
// and keeps the stack of that read when stack is set.
type oneReadFails struct {
	btree.File
	armed  *atomic.Bool
	reads  *atomic.Int64
	stack  *atomic.Pointer[string]
	failAt int64
}

func (f *oneReadFails) ReadAt(b []byte, off int64) (int, error) {
	if f.armed.Load() && f.reads.Add(1)-1 == f.failAt {
		if f.stack != nil {
			stack := string(debug.Stack())
			f.stack.Store(&stack)
		}
		return 0, errInjectedRead
	}
	return f.File.ReadAt(b, off)
}

// A paged search that fails to read a candidate's vector returns the read
// error: it does not rank the candidate by the vector read before it. Each
// search runs once per page read it makes, with that one read failing.
func TestPagedSearch_ReadError(t *testing.T) {
	const n, dim, k = 1500, 48, 10
	vecs, keys := randVectors(n, dim, 23)
	queries, _ := randVectors(1, dim, 91)
	query := queries[0]

	g := NewFlatHNSW(dim, L2, 1)
	g.EfSearch = 64
	for i := range vecs {
		g.Add(keys[i], vecs[i])
	}
	path := filepath.Join(t.TempDir(), "p.db")
	db, err := btree.Open(path, btree.Options{})
	require.NoError(t, err)
	require.NoError(t, persistPaged(g, db, "emb", L2))
	require.NoError(t, db.Checkpoint(btree.CheckpointFull))
	require.NoError(t, db.Close())
	want := g.Search(query, k)

	for _, tc := range []struct {
		name   string
		search func(p *pagedHNSW) ([]SearchResult, error)
	}{
		{"Search", func(p *pagedHNSW) ([]SearchResult, error) { return p.Search(query, k) }},
		{"SearchHybrid", func(p *pagedHNSW) ([]SearchResult, error) { return p.SearchHybrid(query, k) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// run searches a database just opened, so no page of it is
			// cached, and returns the number of reads the search made.
			run := func(failAt int64) int64 {
				var armed atomic.Bool
				var reads atomic.Int64
				var stack atomic.Pointer[string]
				btree.SetVFS(btree.VFS{OpenFile: func(name string, flag int, perm os.FileMode) (btree.File, error) {
					f, err := os.OpenFile(name, flag, perm)
					if err != nil || name != path {
						return f, err
					}
					return &oneReadFails{File: f, armed: &armed, reads: &reads, stack: &stack, failAt: failAt}, nil
				}})
				defer btree.ResetVFS()
				db, err := btree.Open(path, btree.Options{})
				require.NoError(t, err)
				defer db.Close()
				// The graph in memory with the vectors read from the file:
				// SearchHybrid routes by the graph's own copy of them.
				p, err := attachPaged(g, db, "emb", L2)
				require.NoError(t, err)
				// A read transaction before the fault is armed: the begin
				// of the search's own then reads nothing of the file.
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				require.NoError(t, rtx.Rollback())

				armed.Store(true)
				got, err := tc.search(p)
				armed.Store(false)
				if failAt >= 0 {
					// The read that failed fetched a candidate's vector,
					// and the search returns its error.
					at := stack.Load()
					require.NotNil(t, at, "read %d did not happen", failAt)
					require.True(t, strings.Contains(*at, "(*pagedHNSW).vecOf"), "read %d is not of a candidate's vector:\n%s", failAt, *at)
					require.ErrorIs(t, err, errInjectedRead, "read %d", failAt)
					return reads.Load()
				}
				require.NoError(t, err)
				require.Equal(t, len(want), len(got), "read %d", failAt)
				for i := range want {
					require.Equal(t, want[i].Key, got[i].Key, "read %d: result %d", failAt, i)
					require.InDelta(t, want[i].Distance, got[i].Distance, 1e-4, "read %d: result %d", failAt, i)
				}
				return reads.Load()
			}
			total := run(-1)
			require.NotZero(t, total)
			for failAt := int64(0); failAt < total; failAt++ {
				run(failAt)
			}
		})
	}
}

// A read that fails while pagedExists looks the index up is returned: it is
// not an index that is absent, which a caller would build over.
func TestPagedExists_ReadError(t *testing.T) {
	const n, dim = 200, 16
	vecs, keys := randVectors(n, dim, 23)
	g := NewFlatHNSW(dim, L2, 1)
	for i := range vecs {
		g.Add(keys[i], vecs[i])
	}
	path := filepath.Join(t.TempDir(), "p.db")
	db, err := btree.Open(path, btree.Options{})
	require.NoError(t, err)
	require.NoError(t, persistPaged(g, db, "emb", L2))
	require.NoError(t, db.Checkpoint(btree.CheckpointFull))
	require.NoError(t, db.Close())

	run := func(failAt int64) int64 {
		var armed atomic.Bool
		var reads atomic.Int64
		btree.SetVFS(btree.VFS{OpenFile: func(name string, flag int, perm os.FileMode) (btree.File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil || name != path {
				return f, err
			}
			return &oneReadFails{File: f, armed: &armed, reads: &reads, failAt: failAt}, nil
		}})
		defer btree.ResetVFS()
		db, err := btree.Open(path, btree.Options{})
		require.NoError(t, err)
		defer db.Close()

		armed.Store(true)
		exists, err := pagedExists(db, "emb")
		armed.Store(false)
		if failAt >= 0 && failAt < reads.Load() {
			require.ErrorIs(t, err, errInjectedRead, "read %d", failAt)
			require.False(t, exists)
			return reads.Load()
		}
		require.NoError(t, err)
		require.True(t, exists)
		return reads.Load()
	}
	total := run(-1)
	require.NotZero(t, total, "the lookup read no page: there is no read to fail")
	for failAt := int64(0); failAt < total; failAt++ {
		run(failAt)
	}
}
