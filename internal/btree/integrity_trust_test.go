package btree

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegrityCheck_UntrustedHeaderSize: page 1's size of 0 is not trusted
// (lockBtree takes the pager's count, btree.c:3294-3298), and the integrity
// check walks btreePagecount (btree.c:11163-11168). It used to read the raw 0
// and report a damaged, populated file as sound.
func TestIntegrityCheck_UntrustedHeaderSize(t *testing.T) {
	for _, m := range restartModes[:2] { // a file to patch
		t.Run(m.name, func(t *testing.T) {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 300, 1)
			require.NoError(t, db.Checkpoint(CheckpointTruncate))
			pageSize := int64(m.opts.PageSize)
			require.Greater(t, db.pager.dbSize.Load(), uint32(5))
			require.NoError(t, db.Close())

			f, err := os.OpenFile(path, os.O_RDWR, 0)
			require.NoError(t, err)
			_, err = f.WriteAt(make([]byte, 4), 28) // page 1's size: 0
			require.NoError(t, err)
			_, err = f.WriteAt(bytes.Repeat([]byte{0xee}, 64), 3*pageSize) // page 4: damaged
			require.NoError(t, err)
			require.NoError(t, f.Close())

			db, err = testOpen(t, path, m.opts)
			require.NoError(t, err)
			defer db.Close()
			require.Error(t, db.IntegrityCheck(), "the damaged page is found")
		})
	}
}
