package test

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
)

// dirtyDB creates a database of two pages, page 1 the master and page 2
// the catalog's root, closes it, applies damage to its file and leaves the
// marker of a process that died after a write. It returns the path.
func dirtyDB(t *testing.T, damage func(path string)) string {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "a.db")
	db, err := anystore.Open(ctx, path, &anystore.Config{Durability: anystore.DurabilityConfig{Sentinel: true}})
	require.NoError(t, err)
	require.NoError(t, db.Close())
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.EqualValues(t, 2*4096, info.Size())
	damage(path)
	require.NoError(t, os.WriteFile(path+".lock", nil, 0o644))
	return path
}

// A dirty open whose catalog's root page number points past the end of the
// database met damage: ErrQuickCheckFailed. The page past the end is not
// read as an empty catalog, and the marker stays.
func TestOpen_DirtyCatalogRootPastTheDatabase(t *testing.T) {
	path := dirtyDB(t, func(path string) {
		// The catalog's master cell: key length, value length, the key,
		// then the 4-byte root page number, on page 1.
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		cell := append([]byte{byte(len("_system")), 4}, "_system"...)
		require.Equal(t, 1, bytes.Count(data[:4096], cell))
		at := bytes.Index(data, cell) + len(cell)
		require.EqualValues(t, 2, binary.BigEndian.Uint32(data[at:]))
		binary.BigEndian.PutUint32(data[at:], 3)
		require.NoError(t, os.WriteFile(path, data, 0o644))
	})
	// The edit breaks page 1's checksum; the open goes past it.
	_, err := anystore.Open(context.Background(), path, &anystore.Config{
		Durability:               anystore.DurabilityConfig{Sentinel: true},
		ContinueOnIntegrityError: true,
	})
	require.ErrorIs(t, err, anystore.ErrQuickCheckFailed)
	require.ErrorIs(t, err, anystore.ErrCorrupt)
	_, err = os.Stat(path + ".lock")
	require.NoError(t, err, "the marker stays")
}

// A dirty open whose catalog page the file holds only in part met damage:
// the page reads as zeros past the end of the file, as SQLite's readDbPage
// reads it, and fails its checksum. It is ErrQuickCheckFailed, not a check
// that did not complete, and the marker stays.
func TestOpen_DirtyCatalogPageCutShort(t *testing.T) {
	path := dirtyDB(t, func(path string) {
		require.NoError(t, os.Truncate(path, 4096+2048))
	})
	_, err := anystore.Open(context.Background(), path, &anystore.Config{Durability: anystore.DurabilityConfig{Sentinel: true}})
	require.ErrorIs(t, err, anystore.ErrQuickCheckFailed)
	require.ErrorIs(t, err, anystore.ErrPageIntegrity)
	_, err = os.Stat(path + ".lock")
	require.NoError(t, err, "the marker stays")
}
