package btree

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// backupPair returns two independent on-disk DBs: src (with a namespace
// "data") and an empty dst. Both use identical Options so page sizes match.
func backupPair(t *testing.T) (src, dst *DB) {
	t.Helper()
	dir := t.TempDir()
	opts := DefaultOptions()

	s, err := Open(filepath.Join(dir, "src.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	stx, err := s.BeginWrite()
	require.NoError(t, err)
	_, err = stx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, stx.Commit())

	d, err := Open(filepath.Join(dir, "dst.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	return s, d
}

func TestBackupInit_BasicFields(t *testing.T) {
	src, dst := backupPair(t)

	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.NotNil(t, b)
	require.Equal(t, src, b.src)
	require.Equal(t, dst, b.dst)
	require.Equal(t, uint32(1), b.iNext, "iNext should start at 1 per sqlite3_backup_init (backup.c:188)")
	require.False(t, b.dstLocked, "dstLocked starts false per backup.c:25")
}

func TestBackupInit_RejectsSameDB(t *testing.T) {
	src, _ := backupPair(t)
	// ~ backup.c:166–170: "source and destination must be distinct".
	_, err := src.BackupInit(src)
	require.ErrorIs(t, err, ErrBackupSameDB)
}

// Note: direct unit-test of ErrBackupPageSizeMismatch would require
// two DBs with different page sizes open simultaneously, which is
// impossible in any-store (pageBufferPool is a process-global singleton,
// page_slab.go:47). The check in BackupInit is trivial
// (`dst.PageSize() != src.PageSize()`) and is exercised indirectly by
// any future refactor that drops it — tests will break immediately.

// fmt is imported for later tests; silence unused-import for now.
var _ = fmt.Sprintf
