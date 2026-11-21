package test

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

// A backup's destination is a database of its own. A damaged page the copy
// reads there comes back as Backup's error, and the source's
// OnIntegrityError, which receives the source's failures only, is not
// called. Both kinds of database with page integrity are covered: the
// default checksummed one, and one encrypted by a caller's codec, which
// the destination shares with the source.
func TestBackup_DestinationIntegrityFailureIsBackupsError(t *testing.T) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	codec, err := anystore.NewAESCodec(key)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		enc  anystore.EncryptionConfig
	}{
		{"checksum", anystore.EncryptionConfig{}},
		{"codec", anystore.EncryptionConfig{Codec: codec}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			fill := func(db anystore.DB) {
				coll, err := db.CreateCollection(ctx, "c")
				require.NoError(t, err)
				for i := 0; i < 200; i++ {
					require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%0200d"}`, i, i))))
				}
			}

			// The destination exists as a database of the source's kind,
			// checkpointed, with page 2 damaged in the main file.
			dstPath := filepath.Join(dir, "dst.db")
			dst, err := anystore.Open(ctx, dstPath, &anystore.Config{Encryption: tc.enc})
			require.NoError(t, err)
			fill(dst)
			require.NoError(t, dst.Flush(ctx, 0, anystore.FlushModeCheckpointFull))
			require.NoError(t, dst.Close())
			const pageSize = 4096
			data, err := os.ReadFile(dstPath)
			require.NoError(t, err)
			require.Greater(t, len(data), 2*pageSize)
			data[pageSize+200] ^= 0x01
			require.NoError(t, os.WriteFile(dstPath, data, 0o644))

			var calls atomic.Int32
			src, err := anystore.Open(ctx, filepath.Join(dir, "src.db"), &anystore.Config{
				Encryption:       tc.enc,
				OnIntegrityError: func(anystore.IntegrityError) { calls.Add(1) },
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, src.Close()) }()
			fill(src)

			err = src.Backup(ctx, dstPath)
			require.ErrorIs(t, err, anystore.ErrPageIntegrity)
			assert.Zero(t, calls.Load(), "the destination's failure must not reach the source's OnIntegrityError")
		})
	}
}
