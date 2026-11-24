package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

// IntegrityCheck's report of a damaged database matches ErrCorrupt, so a
// caller handles it as it handles damage met by a read. A sound database
// passes, and a cancelled check is the context's error, not corruption.
func TestIntegrityCheck_DamageMatchesErrCorrupt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "a.db")
	db, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 300; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%0400d"}`, i, i))))
	}
	require.NoError(t, db.IntegrityCheck(ctx))

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	err = db.IntegrityCheck(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, anystore.ErrCorrupt)
	require.NoError(t, db.Close())

	// Damage the last page of the file: a page of the collection, which
	// the open does not read.
	info, err := os.Stat(path)
	require.NoError(t, err)
	flipByte(t, path, info.Size()-2000)

	db, err = anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()
	err = db.IntegrityCheck(ctx)
	require.ErrorIs(t, err, anystore.ErrCorrupt)
}

// IntegrityCheck on a closed database is ErrDBIsClosed: a check that cannot
// run says nothing about the file, and must not read as damage.
func TestIntegrityCheck_ClosedIsErrDBIsClosed(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "a.db"), nil)
	require.NoError(t, err)
	_, err = db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	err = db.IntegrityCheck(ctx)
	require.ErrorIs(t, err, anystore.ErrDBIsClosed)
	require.NotErrorIs(t, err, anystore.ErrCorrupt)
}
