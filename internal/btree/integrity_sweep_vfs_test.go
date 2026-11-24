//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

import (
	"bytes"
	"context"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// page1ReadCountFile counts the reads that begin at the start of the file
// and go past page 1's header.
type page1ReadCountFile struct {
	File
	n *atomic.Int32
}

func (f *page1ReadCountFile) ReadAt(b []byte, off int64) (int, error) {
	if off == 0 && len(b) > dbHeaderSize {
		f.n.Add(1)
	}
	return f.File.ReadAt(b, off)
}

// TestVerifyIntegritySweepPage1BodyUnreadable: when page 1 cannot be read
// whole but its header can, the sweep lists page 1 with the read's error and
// goes on, as it does for any other page it cannot read. With a WAL commit
// in the snapshot it sweeps the pages the commit recorded. With none, the
// pager's count is the file's length, and the sweep reads the header alone
// for the page count (readRawPage1Header): the file here is longer than the
// database, so only the header's count gives the database's pages. A sweep
// that reads page 1 reads it once.
func TestVerifyIntegritySweepPage1BodyUnreadable(t *testing.T) {
	cases := []struct {
		name string
		ckpt CheckpointMode // Full keeps the WAL's commit; Truncate and a reopen leave the file's length
	}{
		{name: "WAL commit recorded", ckpt: CheckpointFull},
		{name: "file length only", ckpt: CheckpointTruncate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir() + "/test.db"
			var armed atomic.Bool
			var page1Reads atomic.Int32
			SetVFS(VFS{OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
				f, err := defaultOpenFile(name, flag, perm)
				if err != nil || name != path {
					return f, err
				}
				return &page1ReadCountFile{File: &page1TailFaultFile{File: f, armed: &armed}, n: &page1Reads}, nil
			}})
			defer ResetVFS()

			db, err := testOpen(t, path, sweepPeerOpts)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			dmsPutRange(t, db, "t1", 0, 500)
			n := db.pager.dbSize.Load()
			require.NoError(t, db.Checkpoint(tc.ckpt))
			if tc.ckpt == CheckpointTruncate {
				require.NoError(t, db.Close())
				// Three pages past the database's end, with no valid tag:
				// the file's length is no longer the database's size.
				file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
				require.NoError(t, err)
				_, err = file.Write(bytes.Repeat([]byte{0xEE}, 3*int(sweepPeerOpts.PageSize)))
				require.NoError(t, err)
				require.NoError(t, file.Close())
				db, err = testOpen(t, path, sweepPeerOpts)
				require.NoError(t, err)
				defer func() { _ = db.Close() }()
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				bound := db.pager.readerDbSizeBound(rtx.cache)
				require.NoError(t, rtx.Rollback())
				require.Equal(t, n+3, bound, "setup: the pager's count is the file's length")
			}

			ctx := context.Background()
			page1Reads.Store(0)
			res, err := db.VerifyIntegrity(ctx)
			require.NoError(t, err)
			require.Equal(t, int(n), res.Pages)
			require.Empty(t, res.Errors)
			require.Equal(t, int32(1), page1Reads.Load(), "a sweep reads page 1 once")

			// A damaged page after page 1, and page 1 unreadable past its header.
			damaged := n / 2
			sweepPatchFile(t, db, int64(damaged-1)*int64(sweepPeerOpts.PageSize)+500, []byte{0xA5, 0x5A, 0xA5, 0x5A})
			armed.Store(true)
			res, err = db.VerifyIntegrity(ctx)
			armed.Store(false)
			require.NoError(t, err)
			require.Equal(t, int(n), res.Pages)
			require.Len(t, res.Errors, 2)
			require.Equal(t, uint32(1), res.Errors[0].PageNo)
			require.Equal(t, IntegrityKindUnknown, res.Errors[0].Kind)
			require.ErrorIs(t, res.Errors[0].Inner, errInjectedPage1Read)
			require.Equal(t, damaged, res.Errors[1].PageNo)
			require.Equal(t, IntegrityChecksumMismatch, res.Errors[1].Kind)
		})
	}
}
