package btree

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckHeaderFormat(t *testing.T) {
	for _, tc := range []struct {
		name         string
		versioned    bool
		read, write  uint8
		schemaFormat uint32
		want         error
	}{
		{name: "current", read: 2, write: 2, schemaFormat: maxSchemaFormat},
		{name: "format never recorded", read: 2, write: 2, schemaFormat: 0},
		{name: "version bytes below the maximum", read: 1, write: 1, schemaFormat: 5},
		{name: "old format", read: 2, write: 2, schemaFormat: 4, want: ErrOldFormat},
		{name: "newer read version", read: 3, write: 2, schemaFormat: 5, want: ErrNewerFormat},
		{name: "newer write version", read: 2, write: 3, schemaFormat: 5, want: ErrNewerFormat},
		{name: "newer format under the old magic", read: 2, write: 2, schemaFormat: maxSchemaFormat + 1, want: ErrNewerFormat},
		{name: "newer format under the versioned magic", versioned: true, read: 2, write: 2, schemaFormat: maxSchemaFormat + 1, want: ErrNewerFormat},
		{name: "versioned magic with format 5", versioned: true, read: 2, write: 2, schemaFormat: 5, want: ErrHeaderCorrupt},
		{name: "versioned magic with an old format", versioned: true, read: 2, write: 2, schemaFormat: 3, want: ErrHeaderCorrupt},
		{name: "versioned magic with no format", versioned: true, read: 2, write: 2, schemaFormat: 0, want: ErrHeaderCorrupt},
		{name: "a newer version outranks an old format", read: 3, write: 2, schemaFormat: 4, want: ErrNewerFormat},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkHeaderFormat(tc.versioned, tc.read, tc.write, tc.schemaFormat)
			if tc.want == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestErrNewerFormatIsNotCorrupt(t *testing.T) {
	assert.NotErrorIs(t, ErrNewerFormat, ErrCorrupt, "a file from a newer release is intact")
}

func TestDbMagicFmtFillsTheField(t *testing.T) {
	// all 16 bytes carry the magic, so a release that compares the whole field
	// against the zero-padded magics refuses it
	assert.Len(t, dbMagicFmt, 16)
	assert.NotContains(t, dbMagicFmt, "\x00")
}

func TestHeaderSerialize_KeepsMagicFamily(t *testing.T) {
	base := dbHeader{PageSize: 4096, WriteVersion: 2, ReadVersion: 2, DatabaseSize: 1, SchemaFormat: 5}

	t.Run("versioned magic is written back", func(t *testing.T) {
		// given
		h := base
		h.versioned = true
		buf := make([]byte, dbHeaderSize)

		// when
		h.serialize(buf)

		// then
		assert.Equal(t, dbMagicFmt, string(buf[:16]))
	})

	t.Run("first magic is rewritten as the second", func(t *testing.T) {
		// given
		buf := make([]byte, dbHeaderSize)
		base.serialize(buf)
		copy(buf[:16], dbMagicV1)
		var h dbHeader
		require.NoError(t, h.deserialize(buf))

		// when
		h.serialize(buf)

		// then
		assert.False(t, h.versioned)
		assert.Equal(t, dbMagicV2, string(buf[:16]))
	})
}

// gateFixture makes a database whose last commit is still in the WAL and
// returns copies of the three files, taken while the database was open.
func gateFixture(t *testing.T) (db, wal, shm []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.db")
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = false // the wal-index is a file only in multi-process mode
	d, err := testOpen(t, path, opts)
	require.NoError(t, err)
	tx, err := d.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("ns")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k"), []byte("v")))
	require.NoError(t, tx.Commit())

	db, err = os.ReadFile(path)
	require.NoError(t, err)
	wal, err = os.ReadFile(path + "-wal")
	require.NoError(t, err)
	require.NotEmpty(t, wal, "the fixture needs a commit that is only in the WAL")
	shm, err = os.ReadFile(path + "-wal-shm")
	require.NoError(t, err)
	require.NoError(t, d.Close())
	return db, wal, shm
}

func TestOpen_RefusedFormatLeavesFilesUntouched(t *testing.T) {
	dbBytes, walBytes, shmBytes := gateFixture(t)

	for _, tc := range []struct {
		name string
		edit func(hdr []byte)
		want error
	}{
		{
			name: "read version 3",
			edit: func(hdr []byte) { hdr[19] = 3 },
			want: ErrNewerFormat,
		},
		{
			name: "write version 3",
			edit: func(hdr []byte) { hdr[18] = 3 },
			want: ErrNewerFormat,
		},
		{
			name: "schema format 6 under the old magic",
			edit: func(hdr []byte) { binary.BigEndian.PutUint32(hdr[44:48], 6) },
			want: ErrNewerFormat,
		},
		{
			name: "a format-6 file",
			edit: func(hdr []byte) {
				copy(hdr[:16], dbMagicFmt)
				binary.BigEndian.PutUint32(hdr[44:48], 6)
			},
			want: ErrNewerFormat,
		},
		{
			name: "versioned magic with schema format 5",
			edit: func(hdr []byte) { copy(hdr[:16], dbMagicFmt) },
			want: ErrHeaderCorrupt,
		},
		{
			name: "schema format 4",
			edit: func(hdr []byte) { binary.BigEndian.PutUint32(hdr[44:48], 4) },
			want: ErrOldFormat,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			path := filepath.Join(t.TempDir(), "t.db")
			edited := append([]byte(nil), dbBytes...)
			tc.edit(edited[:dbHeaderSize])
			require.NoError(t, os.WriteFile(path, edited, 0o644))
			require.NoError(t, os.WriteFile(path+"-wal", walBytes, 0o644))
			require.NoError(t, os.WriteFile(path+"-wal-shm", shmBytes, 0o644))

			// when
			_, err := testOpen(t, path, DefaultOptions())

			// then
			require.ErrorIs(t, err, tc.want)
			if tc.want == ErrNewerFormat {
				assert.NotErrorIs(t, err, ErrCorrupt)
			}
			for suffix, before := range map[string][]byte{"": edited, "-wal": walBytes, "-wal-shm": shmBytes} {
				after, rerr := os.ReadFile(path + suffix)
				require.NoError(t, rerr, "the refusal must not remove %q", suffix)
				assert.Equal(t, before, after, "the refusal must not change %q", suffix)
			}
		})
	}
}

func TestOpen_FirstMagicStillOpensAndIsRewritten(t *testing.T) {
	// given
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	require.NoError(t, db.Close())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	copy(data[:16], dbMagicV1)
	require.NoError(t, os.WriteFile(path, data, 0o644))

	// when
	db, err = testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("ns")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	// then
	data, err = os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, dbMagicV2, string(data[:16]))
	assert.Equal(t, uint32(5), binary.BigEndian.Uint32(data[44:48]))
}
