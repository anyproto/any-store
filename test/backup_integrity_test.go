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
// default checksummed one, and one encrypted with a Key, which the
// destination is encrypted with too.
func TestBackup_DestinationIntegrityFailureIsBackupsError(t *testing.T) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		enc  anystore.EncryptionConfig
	}{
		{"checksum", anystore.EncryptionConfig{}},
		{"key", anystore.EncryptionConfig{Key: key}},
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

// Three databases encrypted with one key: A and B each with an
// OnIntegrityError of its own, C with none. A damaged page is reported to the
// callback of the database it belongs to and to no other, whatever order the
// databases were opened in.
func TestEncryption_IntegrityCallbackPerDatabase(t *testing.T) {
	ctx := context.Background()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	enc := anystore.EncryptionConfig{Key: key}

	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.db")
	pathB := filepath.Join(dir, "b.db")
	pathC := filepath.Join(dir, "c.db")
	for _, path := range []string{pathA, pathB, pathC} {
		writeEncryptedDB(t, path, enc)
	}
	// Damage page 2 of A and of C; B stays intact.
	const pageSize, damaged = 4096, 2
	for _, path := range []string{pathA, pathC} {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		data[(damaged-1)*pageSize+200] ^= 0x01
		require.NoError(t, os.WriteFile(path, data, 0o644))
	}

	type hits struct {
		calls atomic.Int32
		page  atomic.Uint32
		kind  atomic.Int32
	}
	var hitA, hitB hits
	record := func(h *hits) func(anystore.IntegrityError) {
		return func(e anystore.IntegrityError) {
			h.page.Store(e.PageNo)
			h.kind.Store(int32(e.Kind))
			h.calls.Add(1)
		}
	}
	// Opened in the order A, B, C: B's callback is the last one given, and
	// C gives none. Open reads page 2 (the index-format record lives there)
	// and goes on past a failure, so the opens of A and C already hit their
	// damaged page once.
	dbA, err := anystore.Open(ctx, pathA, &anystore.Config{Encryption: enc, OnIntegrityError: record(&hitA)})
	require.NoError(t, err)
	defer dbA.Close()
	dbB, err := anystore.Open(ctx, pathB, &anystore.Config{Encryption: enc, OnIntegrityError: record(&hitB)})
	require.NoError(t, err)
	defer dbB.Close()
	dbC, err := anystore.Open(ctx, pathC, &anystore.Config{Encryption: enc})
	require.NoError(t, err)
	defer dbC.Close()
	assert.Zero(t, hitB.calls.Load(), "opening A and C must not reach B's callback")

	// A read of A's damaged page, through the read path and through the
	// sweep, goes to A's callback only.
	before := hitA.calls.Load()
	_, err = dbA.GetCollectionNames(ctx)
	require.ErrorIs(t, err, anystore.ErrPageIntegrity)
	assert.Greater(t, hitA.calls.Load(), before, "A's read failure must reach A's callback")
	before = hitA.calls.Load()
	rep, err := dbA.VerifyIntegrity(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, rep.Errors, "A's damaged page must fail verification")
	assert.Greater(t, hitA.calls.Load(), before, "A's sweep failure must reach A's callback")
	assert.Equal(t, uint32(damaged), hitA.page.Load())
	assert.Equal(t, anystore.IntegrityAEADAuthFail, anystore.IntegrityErrorKind(hitA.kind.Load()))
	assert.Zero(t, hitB.calls.Load(), "A's failures must not reach B's callback")

	// C has no callback: its failures reach no one.
	before = hitA.calls.Load()
	_, err = dbC.GetCollectionNames(ctx)
	require.ErrorIs(t, err, anystore.ErrPageIntegrity)
	rep, err = dbC.VerifyIntegrity(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, rep.Errors, "C's damaged page must fail verification")
	assert.Equal(t, before, hitA.calls.Load(), "C's failures must not reach A's callback")
	assert.Zero(t, hitB.calls.Load(), "C's failures must not reach B's callback")

	rep, err = dbB.VerifyIntegrity(ctx)
	require.NoError(t, err)
	require.Empty(t, rep.Errors)
	assert.Zero(t, hitB.calls.Load())
}

// writeEncryptedDB creates the database at path with enc, writes 200
// documents to it, checkpoints and closes it, so that its pages are in the
// main file.
func writeEncryptedDB(t *testing.T, path string, enc anystore.EncryptionConfig) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, path, &anystore.Config{Encryption: enc})
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "x")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%0100d"}`, i, i))))
	}
	require.NoError(t, db.Flush(ctx, 0, anystore.FlushModeCheckpointFull))
	require.NoError(t, db.Close())
}
