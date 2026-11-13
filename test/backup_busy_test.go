package test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

// The busy error of a backup is matched from outside the package by
// errors.Is against anystore.ErrBusy.
func TestBackup_ErrBusyMatchesFromOutside(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := anystore.Open(ctx, filepath.Join(dir, "a.db"), nil)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	wtx, err := db.WriteTx(ctx)
	require.NoError(t, err)
	err = db.Backup(wtx.Context(), filepath.Join(dir, "b.db"))
	require.Error(t, err)
	require.True(t, errors.Is(err, anystore.ErrBusy), "got %v", err)
	require.NoError(t, wtx.Rollback())

	require.NoError(t, db.Backup(ctx, filepath.Join(dir, "b.db")))
}
