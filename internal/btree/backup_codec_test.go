package btree

// A backup between files that carry a page codec: which salt the destination's
// header ends up with, and which destinations a source can be copied into.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// headerSalt reads the KDF salt from the header of the database file at path.
func headerSalt(t *testing.T, path string) [SaltLen]byte {
	t.Helper()
	h, err := readInitialHeader(path)
	require.NoError(t, err)
	return h.Salt
}

// codecFill writes n values of a few pages each into namespace "t1".
func codecFill(t *testing.T, db *DB, from, n int) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("t1")
	if err != nil {
		ns, err = tx.CreateNamespace("t1")
		require.NoError(t, err)
	}
	for i := from; i < from+n; i++ {
		require.NoError(t, tx.Put(ns, u32key(i), filled(2000+i%7*1500, i)))
	}
	require.NoError(t, tx.Commit())
}

// codecRequire requires that db is sound and holds exactly the n values
// codecFill wrote from 0.
func codecRequire(t *testing.T, db *DB, n int) {
	t.Helper()
	require.NoError(t, db.IntegrityCheck())
	report, err := db.VerifyIntegrity(context.Background())
	require.NoError(t, err)
	require.Empty(t, report.Errors)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	ns, err := rtx.GetNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		v, err := rtx.Get(ns, u32key(i))
		require.NoError(t, err)
		require.Equal(t, filled(2000+i%7*1500, i), v)
	}
	_, err = rtx.Get(ns, u32key(n))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func passphraseOptions(cipher CipherType) Options {
	opts := DefaultOptions()
	opts.Key = []byte("correct horse") // not KeyLen bytes: derived with the file's salt
	opts.KDFIterations = 1000
	opts.CipherType = cipher
	return opts
}

// The destination of a backup is keyed with its own salt, at its creation or
// from its header, and its pages are sealed with that key. Its header keeps
// that salt through the copy of page 1, so the file opens again with the
// passphrase.
func TestBackup_DestinationKeepsItsSalt(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		existing bool // the destination holds a database before the backup
		update   bool // page 1 reaches the destination a second time, as an update
	}{
		{name: "new destination"},
		{name: "existing destination", existing: true},
		{name: "page 1 copied again by an update", update: true},
		{name: "existing destination, page 1 copied again by an update", existing: true, update: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			dir := t.TempDir()
			opts := passphraseOptions(CipherAES256GCM)
			srcPath, dstPath := filepath.Join(dir, "src.db"), filepath.Join(dir, "dst.db")
			src, err := testOpen(t, srcPath, opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, src.Close()) }()
			codecFill(t, src, 0, 40)

			dst, err := testOpen(t, dstPath, opts)
			require.NoError(t, err)
			if tc.existing {
				codecFill(t, dst, 0, 90)
				require.NoError(t, dst.Close())
				dst, err = testOpen(t, dstPath, opts)
				require.NoError(t, err)
			}
			srcSalt, dstSalt := headerSalt(t, srcPath), headerSalt(t, dstPath)
			require.NotEqual(t, [SaltLen]byte{}, dstSalt)
			require.NotEqual(t, srcSalt, dstSalt)

			// when
			b, err := dst.BackupInit(src)
			require.NoError(t, err)
			n := 40
			if tc.update {
				require.NoError(t, b.Step(ctx, 1)) // page 1 is copied
				tx, err := src.BeginWrite()
				require.NoError(t, err)
				ns, err := tx.CreateNamespace("t2") // a new root on page 1
				require.NoError(t, err)
				require.NoError(t, tx.Put(ns, u32key(0), filled(100, 0)))
				require.NoError(t, tx.Commit())
				codecFill(t, src, 40, 10)
				n = 50
			}
			require.ErrorIs(t, b.Step(ctx, -1), ErrBackupDone)
			require.NoError(t, b.Finish())

			// then: through the handle that received the copy
			require.Equal(t, dstSalt, dst.pager.header.Salt)
			codecRequire(t, dst, n)
			require.NoError(t, dst.Close())

			// and through a fresh open, which derives the key from the header
			require.Equal(t, dstSalt, headerSalt(t, dstPath))
			dst, err = testOpen(t, dstPath, opts)
			require.NoError(t, err)
			codecRequire(t, dst, n)
			if tc.update {
				rtx, err := dst.BeginRead()
				require.NoError(t, err)
				_, err = rtx.GetNamespace("t2")
				require.NoError(t, err)
				require.NoError(t, rtx.Rollback())
			}
			require.NoError(t, dst.Close())

			// the source is as it was
			require.Equal(t, srcSalt, headerSalt(t, srcPath))
		})
	}
}

// An in-memory database has a codec and no salt. The file its backup makes is
// an encrypted file with a salt of its own.
func TestBackup_InMemorySourceIntoEncryptedFile(t *testing.T) {
	// given
	opts := passphraseOptions(CipherAES256GCM)
	memOpts := opts
	memOpts.InMemory = true
	src, err := testOpen(t, "", memOpts)
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close()) }()
	codecFill(t, src, 0, 40)
	dstPath := filepath.Join(t.TempDir(), "dst.db")
	dst, err := testOpen(t, dstPath, opts)
	require.NoError(t, err)

	// when
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
	require.NoError(t, b.Finish())
	require.NoError(t, dst.Close())

	// then
	require.NotEqual(t, [SaltLen]byte{}, headerSalt(t, dstPath))
	dst, err = testOpen(t, dstPath, opts)
	require.NoError(t, err)
	codecRequire(t, dst, 40)
	require.NoError(t, dst.Close())
	_, err = os.Stat(dstPath)
	require.NoError(t, err)
}
