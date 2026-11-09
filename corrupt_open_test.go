package anystore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// writeDB makes a closed database with enough documents to span many pages and returns its path.
func writeDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "corrupt.db")
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	for i := 0; i < 2000; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":"%05d","text":"%0200d"}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	require.NoError(t, db.Close())
	return path
}

func overwrite(t *testing.T, path string, offset int64, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(b, offset)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestErrCorrupt_OpenOfDamagedFile(t *testing.T) {
	garbage := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = 0xAB
		}
		return b
	}
	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, path string)
	}{
		{"header overwritten", func(t *testing.T, path string) { overwrite(t, path, 0, make([]byte, 100)) }},
		{"file cut in half", func(t *testing.T, path string) {
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.NoError(t, os.Truncate(path, info.Size()/2))
		}},
		{"file cut inside the header", func(t *testing.T, path string) { require.NoError(t, os.Truncate(path, 50)) }},
		{"text instead of a database", func(t *testing.T, path string) {
			require.NoError(t, os.WriteFile(path, []byte("this is not a database"), 0o600))
		}},
		{"garbage for the whole file", func(t *testing.T, path string) {
			info, err := os.Stat(path)
			require.NoError(t, err)
			overwrite(t, path, 0, garbage(int(info.Size())))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			path := writeDB(t)
			tc.damage(t, path)

			// when
			db, err := Open(ctx, path, nil)

			// then
			if err == nil {
				_ = db.Close()
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrHeaderCorrupt)
			assert.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

func TestErrCorrupt_DamagedPageFailsTheReadThatMeetsIt(t *testing.T) {
	// given: pages in the middle of the file are overwritten, which Open does not read
	path := writeDB(t)
	info, err := os.Stat(path)
	require.NoError(t, err)
	b := make([]byte, 20000)
	for i := range b {
		b[i] = 0xAB
	}
	overwrite(t, path, info.Size()/2, b)

	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	coll, err := db.Collection(ctx, "docs")
	require.NoError(t, err)

	// when
	_, err = coll.Count(ctx)

	// then
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPageIntegrity)
	assert.ErrorIs(t, err, ErrCorrupt)
}

func TestErrCorrupt_NotCorruption(t *testing.T) {
	for _, err := range []error{
		nil, ErrDocNotFound, ErrDBIsClosed, ErrV1Database, ErrIncompatibleVersion,
		fmt.Errorf("open: %w", os.ErrPermission),
	} {
		assert.NotErrorIs(t, err, ErrCorrupt, "%v", err)
	}

	t.Run("an empty file is a new database", func(t *testing.T) {
		// given
		path := filepath.Join(t.TempDir(), "empty.db")
		require.NoError(t, os.WriteFile(path, nil, 0o600))

		// when
		db, err := Open(ctx, path, nil)

		// then
		require.NoError(t, err)
		require.NoError(t, db.Close())
	})
}

func TestErrCorrupt_Nesting(t *testing.T) {
	// every kind of damage is also the umbrella, with its own text kept
	for _, err := range []error{ErrHeaderCorrupt, ErrWALCorrupt, ErrPageIntegrity, ErrQuickCheckFailed} {
		assert.ErrorIs(t, fmt.Errorf("open: %w", err), ErrCorrupt, "%v", err)
	}
	assert.Equal(t, "any-store: quick check failed on db open", ErrQuickCheckFailed.Error())
	assert.Equal(t, "btree: WAL is corrupt", ErrWALCorrupt.Error())
	assert.Equal(t, "btree: page integrity check failed", ErrPageIntegrity.Error())

	// the kinds stay apart
	assert.NotErrorIs(t, ErrWALCorrupt, ErrPageIntegrity)
	assert.NotErrorIs(t, ErrPageIntegrity, ErrHeaderCorrupt)
	assert.NotErrorIs(t, ErrCorrupt, ErrHeaderCorrupt)
}
