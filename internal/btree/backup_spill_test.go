package btree

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBackupWriterCacheBounded guards the page-1 spill-victim exclusion (see
// the page1-exclusion drift note in docs/btree/NOTES.md): backup copies and
// releases page 1 first, making it the oldest unpinned dirty page. pagerStress
// refuses page 1 without cleaning it, so a victim search that returned page 1
// wedged the spill permanently — nothing ever spilled and the destination
// writer cache grew to the full database size instead of staying bounded at
// CacheSize.
func TestBackupWriterCacheBounded(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.CacheSize = 64

	src, err := Open(filepath.Join(dir, "src.db"), opts)
	require.NoError(t, err)
	defer src.Close()

	stx, err := src.BeginWrite()
	require.NoError(t, err)
	ns, err := stx.CreateNamespace("data")
	require.NoError(t, err)
	fat := make([]byte, 400)
	for i := 0; i < 2000; i++ {
		require.NoError(t, stx.Put(ns, fmt.Appendf(nil, "key-%05d", i), fat))
	}
	require.NoError(t, stx.Commit())
	require.NoError(t, src.Checkpoint(CheckpointFull))
	require.Greater(t, src.DatabaseSize(), uint32(3*opts.CacheSize),
		"source must be several times larger than the cache to force spilling")

	dst, err := Open(filepath.Join(dir, "dst.db"), opts)
	require.NoError(t, err)
	defer dst.Close()

	b, err := dst.BackupInit(src)
	require.NoError(t, err)

	var spilled bool
	for {
		err = b.Step(32)
		// The destination writer cache must stay bounded: copied pages beyond
		// the cache limit spill to the destination WAL instead of accumulating
		// dirty. Small slack for pages pinned by the in-flight step.
		require.LessOrEqual(t, dst.pager.writerCache.nPage, opts.CacheSize+8,
			"destination writer cache exceeded CacheSize — spill is wedged")
		if dst.pager.wal.nFrame.Load() > 0 {
			spilled = true
		}
		if err != nil {
			require.ErrorIs(t, err, ErrBackupDone)
			break
		}
	}
	require.True(t, spilled, "no frames reached the destination WAL before Finish — spill never fired")
	require.NoError(t, b.Finish())

	// The spilled copy must still be a correct backup.
	dstPath := dst.Path()
	require.NoError(t, dst.Close())
	d2, err := Open(dstPath, opts)
	require.NoError(t, err)
	defer d2.Close()
	rtx, err := d2.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, err := d2.GetNamespace("data")
	require.NoError(t, err)
	got, err := rtx.Get(ns2, []byte("key-01042"))
	require.NoError(t, err)
	require.Len(t, got, len(fat))
}
