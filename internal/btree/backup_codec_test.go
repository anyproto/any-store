package btree

// A backup between files that carry a page codec: which salt the destination's
// header ends up with, and which destinations a source can be copied into.

import (
	"context"
	"encoding/binary"
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

// walPage1Salt returns the KDF salt in the header of the last committed copy
// of page 1 in the WAL at walPath, and whether there is one. With set
// non-nil it first writes set into the header of every copy of page 1 there
// and rewrites the frame checksums to match, as if the frames had been
// written so.
func walPage1Salt(t *testing.T, walPath string, set *[SaltLen]byte) (salt [SaltLen]byte, found bool) {
	t.Helper()
	data, err := os.ReadFile(walPath)
	require.NoError(t, err)
	var hdr walHeader
	require.NoError(t, hdr.deserialize(data[:walHeaderSize]))
	frameSize := walFrameSize + int(hdr.pageSize)
	o1, o2 := walChecksum(data[:24], 0, 0) // the chain as written
	n1, n2 := o1, o2                       // the chain after set
	var last [SaltLen]byte
	var seen bool
	for off := walHeaderSize; off+frameSize <= len(data); off += frameSize {
		fh, pg := data[off:off+walFrameSize], data[off+walFrameSize:off+frameSize]
		if binary.BigEndian.Uint32(fh[8:12]) != hdr.salt1 || binary.BigEndian.Uint32(fh[12:16]) != hdr.salt2 {
			break
		}
		c1, c2 := walFrameChecksum(fh[:8], pg, o1, o2)
		if c1 != binary.BigEndian.Uint32(fh[16:20]) || c2 != binary.BigEndian.Uint32(fh[20:24]) {
			break
		}
		o1, o2 = c1, c2
		if binary.BigEndian.Uint32(fh[0:4]) == 1 {
			if set != nil {
				copy(pg[72:88], set[:])
			}
			copy(last[:], pg[72:88])
			seen = true
		}
		n1, n2 = walFrameChecksum(fh[:8], pg, n1, n2)
		if set != nil {
			binary.BigEndian.PutUint32(fh[16:20], n1)
			binary.BigEndian.PutUint32(fh[20:24], n2)
		}
		if binary.BigEndian.Uint32(fh[4:8]) != 0 { // a commit frame
			salt, found = last, seen
		}
	}
	if set != nil {
		require.NoError(t, os.WriteFile(walPath, data, 0o644))
	}
	return salt, found
}

// A destination left by a backup that wrote the source's salt into page 1
// (as every backup of a passphrase database did before the salt was kept)
// and whose process died before the close checkpointed: the main file names
// the salt the destination's pages are sealed with, the WAL's committed
// page 1 another. The salt the codec was keyed with at Open is the one
// every write of page 1 carries, so a backup over that destination is
// readable, and its commit puts that salt back into the WAL's page 1.
func TestBackup_DestinationWALPage1WithAnotherSalt(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	opts := passphraseOptions(CipherAES256GCM)
	srcPath, dstPath := filepath.Join(dir, "src.db"), filepath.Join(dir, "dst.db")

	// given: the destination's committed page 1 in its WAL names another salt
	dst, err := testOpen(t, dstPath, opts)
	require.NoError(t, err)
	codecFill(t, dst, 0, 30)
	require.NoError(t, dst.Close())
	keyed := headerSalt(t, dstPath)
	dst, err = testOpen(t, dstPath, opts)
	require.NoError(t, err)
	tx, err := dst.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t0") // a new root on page 1
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	rawClose(dst)
	other := [SaltLen]byte{0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab}
	_, found := walPage1Salt(t, dstPath+"-wal", &other)
	require.True(t, found, "setup: page 1 is committed in the WAL")
	require.Equal(t, keyed, headerSalt(t, dstPath), "setup: the main file names the keyed salt")

	src, err := testOpen(t, srcPath, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close()) }()
	codecFill(t, src, 0, 40)

	// when
	dst, err = testOpen(t, dstPath, opts)
	require.NoError(t, err)
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.ErrorIs(t, b.Step(ctx, -1), ErrBackupDone)
	require.NoError(t, b.Finish())

	// then: the commit wrote the keyed salt into the WAL's page 1
	rawClose(dst)
	got, found := walPage1Salt(t, dstPath+"-wal", nil)
	require.True(t, found)
	require.Equal(t, keyed, got, "the WAL's page 1 names the salt the pages are sealed with")
	dst, err = testOpen(t, dstPath, opts)
	require.NoError(t, err)
	codecRequire(t, dst, 40)
	require.NoError(t, dst.Close())

	// and the close's checkpoint leaves it in the main file
	require.Equal(t, keyed, headerSalt(t, dstPath))
	dst, err = testOpen(t, dstPath, opts)
	require.NoError(t, err)
	codecRequire(t, dst, 40)
	require.NoError(t, dst.Close())
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

// The options for a backup's destination describe the source file as it is.
// A file keeps the checksum mode it was created with whatever the handle
// asked for, so the handle's Options do not.
func TestBackupDestinationOptions_FollowTheFile(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name            string
		create, reopen  Options // the source is created with one and opened with the other
		wantChecksum    bool
		wantMode        IntegrityMode
		wantReservedLen uint8
	}{
		{
			name:   "a plain file opened with Checksum",
			create: DefaultOptions(),
			reopen: func() Options { o := DefaultOptions(); o.Checksum = true; return o }(),
		},
		{
			name:            "a checksummed file opened without Checksum",
			create:          func() Options { o := DefaultOptions(); o.Checksum = true; return o }(),
			reopen:          DefaultOptions(),
			wantChecksum:    true,
			wantMode:        IntegrityChecksum,
			wantReservedLen: cksumOverhead,
		},
		{
			name:            "an encrypted file",
			create:          passphraseOptions(CipherXChaCha20Poly1305),
			reopen:          passphraseOptions(CipherXChaCha20Poly1305),
			wantMode:        IntegrityAEAD,
			wantReservedLen: 48,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			dir := t.TempDir()
			srcPath, dstPath := filepath.Join(dir, "src.db"), filepath.Join(dir, "dst.db")
			src, err := testOpen(t, srcPath, tc.create)
			require.NoError(t, err)
			codecFill(t, src, 0, 40)
			require.NoError(t, src.Close())
			src, err = testOpen(t, srcPath, tc.reopen)
			require.NoError(t, err)
			require.Equal(t, tc.wantMode, src.IntegrityMode())

			// when
			opts := src.BackupDestinationOptions()
			require.Equal(t, tc.wantChecksum, opts.Checksum)
			require.False(t, opts.InMemory)
			require.Equal(t, src.PageSize(), opts.PageSize)
			require.Equal(t, src.SchemaFormat(), opts.SchemaFormat)
			dst, err := testOpen(t, dstPath, opts)
			require.NoError(t, err)
			require.Equal(t, tc.wantMode, dst.IntegrityMode())
			b, err := dst.BackupInit(src)
			require.NoError(t, err)
			require.ErrorIs(t, b.Step(ctx, -1), ErrBackupDone)
			require.NoError(t, b.Finish())
			require.NoError(t, dst.Close())
			require.NoError(t, src.Close())

			// then
			h, err := readInitialHeader(dstPath)
			require.NoError(t, err)
			require.Equal(t, tc.wantReservedLen, h.ReservedSpace)
			if tc.wantMode != IntegrityAEAD {
				// Without encryption the copy is the source's pages, byte for
				// byte, the trailers included; page 1 past its header too,
				// to its last byte (the header has the copy's own schema
				// cookie and is not covered by page 1's trailer, which hashes
				// bytes 100 on).
				srcData, err := os.ReadFile(srcPath)
				require.NoError(t, err)
				dstData, err := os.ReadFile(dstPath)
				require.NoError(t, err)
				require.Equal(t, len(srcData), len(dstData))
				for off := 0; off < len(srcData); off += int(h.PageSize) {
					from, to := off, off+int(h.PageSize)
					if off == 0 {
						from = dbHeaderSize
					}
					require.True(t, string(srcData[from:to]) == string(dstData[from:to]), "page %d differs", off/int(h.PageSize)+1)
				}
			}
			dst, err = testOpen(t, dstPath, tc.reopen)
			require.NoError(t, err)
			require.Equal(t, tc.wantMode, dst.IntegrityMode())
			codecRequire(t, dst, 40)
			require.NoError(t, dst.Close())
		})
	}
}

// An in-memory source with no codec gives a plain file.
func TestBackupDestinationOptions_InMemorySource(t *testing.T) {
	// given
	memOpts := DefaultOptions()
	memOpts.InMemory = true
	src, err := testOpen(t, "", memOpts)
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close()) }()
	codecFill(t, src, 0, 40)

	// when
	opts := src.BackupDestinationOptions()
	require.False(t, opts.InMemory)
	require.False(t, opts.Checksum)
	dstPath := filepath.Join(t.TempDir(), "dst.db")
	dst, err := testOpen(t, dstPath, opts)
	require.NoError(t, err)
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
	require.NoError(t, b.Finish())
	require.NoError(t, dst.Close())

	// then: plain, and still plain under a handle that asks for checksums
	withChecksum := DefaultOptions()
	withChecksum.Checksum = true
	dst, err = testOpen(t, dstPath, withChecksum)
	require.NoError(t, err)
	require.Equal(t, IntegrityNone, dst.IntegrityMode())
	codecRequire(t, dst, 40)
	require.NoError(t, dst.Close())
}

// A destination that is open with another reserved space or another kind of
// codec than the source's is refused at BackupInit, with nothing written.
func TestBackupInit_RefusesAnotherCodec(t *testing.T) {
	plain := DefaultOptions()
	checksum := DefaultOptions()
	checksum.Checksum = true
	rawKey := DefaultOptions()
	rawKey.Key = []byte("0123456789abcdef0123456789abcdef")
	for _, tc := range []struct {
		name     string
		src, dst Options
	}{
		{"plain into checksummed", plain, checksum},
		{"checksummed into plain", checksum, plain},
		{"plain into encrypted", plain, rawKey},
		{"encrypted into plain", rawKey, plain},
		{"checksummed into encrypted", checksum, rawKey},
		{"encrypted into checksummed", rawKey, checksum},
		{"encrypted into encrypted with a longer reserve", passphraseOptions(CipherAES256GCM), passphraseOptions(CipherXChaCha20Poly1305)},
		{"encrypted with a longer reserve into encrypted", passphraseOptions(CipherXChaCha20Poly1305), passphraseOptions(CipherAES256GCM)},
	} {
		for _, existing := range []bool{false, true} {
			name := tc.name
			if existing {
				name += ", existing"
			}
			t.Run(name, func(t *testing.T) {
				// given
				dir := t.TempDir()
				src, err := testOpen(t, filepath.Join(dir, "src.db"), tc.src)
				require.NoError(t, err)
				defer func() { require.NoError(t, src.Close()) }()
				codecFill(t, src, 0, 40)
				dstPath := filepath.Join(dir, "dst.db")
				dst, err := testOpen(t, dstPath, tc.dst)
				require.NoError(t, err)
				n := 0
				if existing {
					n = 25
					codecFill(t, dst, 0, n)
					require.NoError(t, dst.Close())
					dst, err = testOpen(t, dstPath, tc.dst)
					require.NoError(t, err)
				}
				before, err := os.ReadFile(dstPath)
				require.NoError(t, err)

				// when
				_, err = dst.BackupInit(src)

				// then
				require.ErrorIs(t, err, ErrBackupCodecMismatch)
				require.False(t, dst.HasOpenTransaction())
				require.NoError(t, dst.Close())
				after, err := os.ReadFile(dstPath)
				require.NoError(t, err)
				require.True(t, string(before) == string(after), "the destination file is untouched")
				dst, err = testOpen(t, dstPath, tc.dst)
				require.NoError(t, err)
				if existing {
					codecRequire(t, dst, n)
				}
				require.NoError(t, dst.Close())
			})
		}
	}
}

// Two encrypted files of one reserved space pass BackupInit whatever their
// ciphers and keys: the destination seals the pages with its own.
func TestBackup_BetweenCiphersOfOneReserve(t *testing.T) {
	// given
	dir := t.TempDir()
	src, err := testOpen(t, filepath.Join(dir, "src.db"), passphraseOptions(CipherAES256GCM))
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close()) }()
	codecFill(t, src, 0, 40)
	dstOpts := passphraseOptions(CipherChaCha20Poly1305)
	dstOpts.Key = []byte("another passphrase")
	dstPath := filepath.Join(dir, "dst.db")
	dst, err := testOpen(t, dstPath, dstOpts)
	require.NoError(t, err)

	// when
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
	require.NoError(t, b.Finish())
	require.NoError(t, dst.Close())

	// then
	dst, err = testOpen(t, dstPath, dstOpts)
	require.NoError(t, err)
	codecRequire(t, dst, 40)
	require.NoError(t, dst.Close())
}
