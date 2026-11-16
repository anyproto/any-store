package anystore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// A backup of a database created before v2.5.0 is such a database too: the
// copy keeps the source's on-disk format, also when this release creates
// format 6 files by default.
func TestBackup_KeepsSourceFormat(t *testing.T) {
	legacySuite := os.Getenv("ANYSTORE_TEST_LEGACY_FORMAT") == "1"
	for _, legacy := range []bool{true, false} {
		if legacySuite && !legacy {
			continue
		}
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			// given
			btree.SetCreateLegacyFormatForTest(legacy)
			defer btree.SetCreateLegacyFormatForTest(legacySuite)
			dir := t.TempDir()
			db, err := Open(ctx, filepath.Join(dir, "src.db"), nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			coll, err := db.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			const n = 200
			for i := 0; i < n; i++ {
				// incompressible enough to pass the 997-byte limit of format 5
				var sb strings.Builder
				for sb.Len() < 3000 {
					fmt.Fprintf(&sb, "%x", uint64(i+1)*2654435761+uint64(sb.Len())*40503)
				}
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":"%05d","t":"%s"}`, i, sb.String()))))
			}
			btree.SetCreateLegacyFormatForTest(legacySuite) // new files get the default format again

			// when
			backupPath := filepath.Join(dir, "backup.db")
			require.NoError(t, db.Backup(ctx, backupPath))

			// then
			data, err := os.ReadFile(backupPath)
			require.NoError(t, err)
			wantMagic, wantFormat := "any-store v2 fmt", uint32(6)
			if legacy {
				wantMagic, wantFormat = "any-store v2\x00\x00\x00\x00", uint32(5)
			}
			assert.Equal(t, wantMagic, string(data[:16]))
			assert.Equal(t, wantFormat, binary.BigEndian.Uint32(data[44:48]))

			copyDB, err := Open(ctx, backupPath, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, copyDB.Close()) }()
			copyColl, err := copyDB.Collection(ctx, "docs")
			require.NoError(t, err)
			count, err := copyColl.Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, n, count)
			require.NoError(t, copyDB.QuickCheck(ctx))
		})
	}
}
