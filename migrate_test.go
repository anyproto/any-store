package anystore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SchemaFormat reports the format the file's header records: 5 for a file
// made the way releases before v2.5.0 make them, LatestSchemaFormat for one
// this release creates. A reopen keeps the file's format whatever new files
// get by then.
func TestSchemaFormat(t *testing.T) {
	skipIfInMemory(t, "the header is read from the file and the file reopened")
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			want := LatestSchemaFormat
			if legacy {
				want = 5
			}
			createFormat(t, legacy)
			path := filepath.Join(t.TempDir(), "format.db")
			d, err := Open(ctx, path, nil)
			require.NoError(t, err)
			got, err := SchemaFormat(d)
			require.NoError(t, err)
			assert.Equal(t, want, got)
			require.NoError(t, d.Close())
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, uint32(want), binary.BigEndian.Uint32(data[44:48]))

			createFormat(t, !legacy)
			d, err = Open(ctx, path, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()
			got, err = SchemaFormat(d)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

// foreignDB is a DB of the caller's own, which the package functions that
// take a DB refuse.
type foreignDB struct{ DB }

func TestSchemaFormat_UnsupportedDB(t *testing.T) {
	fx := newFixture(t)
	_, err := SchemaFormat(foreignDB{fx.DB})
	assert.ErrorIs(t, err, ErrUnsupportedDB)
	_, err = SchemaFormat(nil)
	assert.ErrorIs(t, err, ErrUnsupportedDB)
	var nilDB *db
	_, err = SchemaFormat(nilDB)
	assert.ErrorIs(t, err, ErrUnsupportedDB)
}
