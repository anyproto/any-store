package test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

// encryptedDB creates an encrypted database with a few hundred documents at
// a new path, closes it, and returns the path and the config that opens it.
func encryptedDB(t *testing.T) (string, anystore.Config) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "enc.db")
	cfg := anystore.Config{Encryption: anystore.EncryptionConfig{
		Passphrase:    []byte("right passphrase"),
		KDFIterations: 1000,
	}}
	db, err := anystore.Open(ctx, path, &cfg)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 300; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%0400d"}`, i, i))))
	}
	require.NoError(t, db.Close())
	return path, cfg
}

// integrityCalls collects the pages Config.OnIntegrityError was called for.
type integrityCalls struct {
	mu    sync.Mutex
	pages []uint32
}

func (c *integrityCalls) hook(e anystore.IntegrityError) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pages = append(c.pages, e.PageNo)
}

func (c *integrityCalls) get() []uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uint32(nil), c.pages...)
}

// flipByte inverts the byte of the file at off.
func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	var b [1]byte
	_, err = f.ReadAt(b[:], off)
	require.NoError(t, err)
	b[0] ^= 0xFF
	_, err = f.WriteAt(b[:], off)
	require.NoError(t, err)
}

// A wrong key is ErrWrongKey. It is not corruption, the integrity callback
// is not called, and the file opens with the right key afterwards.
func TestOpen_WrongKey(t *testing.T) {
	ctx := context.Background()
	path, cfg := encryptedDB(t)

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	for _, tc := range []struct {
		name string
		enc  anystore.EncryptionConfig
	}{
		{"passphrase", anystore.EncryptionConfig{Passphrase: []byte("wrong passphrase"), KDFIterations: 1000}},
		{"raw key", anystore.EncryptionConfig{Key: key}},
		{"cipher", anystore.EncryptionConfig{Passphrase: cfg.Encryption.Passphrase, KDFIterations: 1000, CipherType: anystore.CipherChaCha20Poly1305}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls integrityCalls
			wrong := anystore.Config{Encryption: tc.enc, OnIntegrityError: calls.hook}
			_, err := anystore.Open(ctx, path, &wrong)
			require.ErrorIs(t, err, anystore.ErrWrongKey)
			require.NotErrorIs(t, err, anystore.ErrCorrupt)
			require.NotErrorIs(t, err, anystore.ErrPageIntegrity)
			require.Empty(t, calls.get(), "a wrong key must not reach OnIntegrityError")
		})
	}

	right := cfg
	db, err := anystore.Open(ctx, path, &right)
	require.NoError(t, err)
	coll, err := db.OpenCollection(ctx, "c")
	require.NoError(t, err)
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 300, n)
	require.NoError(t, db.Close())

	// A database with no collection, as Open leaves a new one.
	empty := filepath.Join(t.TempDir(), "empty.db")
	db, err = anystore.Open(ctx, empty, &right)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	wrong := anystore.Config{Encryption: anystore.EncryptionConfig{Passphrase: []byte("wrong passphrase"), KDFIterations: 1000}}
	_, err = anystore.Open(ctx, empty, &wrong)
	require.ErrorIs(t, err, anystore.ErrWrongKey)
	require.NotErrorIs(t, err, anystore.ErrCorrupt)
}

// A page 1 damaged under the right key is corruption, not a wrong key:
// other pages authenticate, so the key is proven. The integrity callback is
// called for page 1 and for no other page.
func TestOpen_DamagedPage1UnderRightKey(t *testing.T) {
	ctx := context.Background()
	path, cfg := encryptedDB(t)
	// Past the 100-byte header, which is not encrypted.
	flipByte(t, path, 1000)

	var calls integrityCalls
	cfg.OnIntegrityError = calls.hook
	_, err := anystore.Open(ctx, path, &cfg)
	require.ErrorIs(t, err, anystore.ErrPageIntegrity)
	require.ErrorIs(t, err, anystore.ErrCorrupt)
	require.NotErrorIs(t, err, anystore.ErrWrongKey)
	require.Equal(t, []uint32{1}, calls.get())
}

// The two refusals of a Config.Encryption that does not fit the file are
// matchable, name the public config, are not corruption, and leave the file
// as it was.
func TestOpen_EncryptionRefusals(t *testing.T) {
	ctx := context.Background()

	t.Run("encrypted file, no key", func(t *testing.T) {
		path, cfg := encryptedDB(t)
		_, err := anystore.Open(ctx, path, nil)
		require.ErrorIs(t, err, anystore.ErrEncryptionKeyRequired)
		require.NotErrorIs(t, err, anystore.ErrCorrupt)
		require.NotErrorIs(t, err, anystore.ErrWrongKey)
		require.Contains(t, err.Error(), "Config.Encryption")

		db, err := anystore.Open(ctx, path, &cfg)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	})

	t.Run("plain file, key given", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "plain.db")
		db, err := anystore.Open(ctx, path, nil)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		_, err = anystore.Open(ctx, path, &anystore.Config{Encryption: anystore.EncryptionConfig{
			Passphrase:    []byte("a passphrase"),
			KDFIterations: 1000,
		}})
		require.ErrorIs(t, err, anystore.ErrNotEncrypted)
		require.NotErrorIs(t, err, anystore.ErrCorrupt)
		require.NotErrorIs(t, err, anystore.ErrWrongKey)
		require.Contains(t, err.Error(), "Config.Encryption")

		db, err = anystore.Open(ctx, path, nil)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	})

	// A backup's destination is opened with the source's encryption: one
	// that exists and does not fit is refused with the same errors.
	t.Run("backup destination", func(t *testing.T) {
		path, cfg := encryptedDB(t)
		plain := filepath.Join(t.TempDir(), "plain.db")
		db, err := anystore.Open(ctx, plain, nil)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		// An encrypted source over a plain file.
		src, err := anystore.Open(ctx, path, &cfg)
		require.NoError(t, err)
		defer src.Close()
		err = src.Backup(ctx, plain)
		require.ErrorIs(t, err, anystore.ErrNotEncrypted)
		require.NotErrorIs(t, err, anystore.ErrCorrupt)

		// A plain source over an encrypted file.
		plainSrc, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "src.db"), nil)
		require.NoError(t, err)
		defer plainSrc.Close()
		require.NoError(t, src.Close())
		err = plainSrc.Backup(ctx, path)
		require.ErrorIs(t, err, anystore.ErrEncryptionKeyRequired)
		require.NotErrorIs(t, err, anystore.ErrCorrupt)

		// An encrypted source over a file encrypted under another key.
		other := anystore.Config{Encryption: anystore.EncryptionConfig{
			Passphrase:    []byte("another passphrase"),
			KDFIterations: 1000,
		}}
		otherSrc, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "other.db"), &other)
		require.NoError(t, err)
		defer otherSrc.Close()
		err = otherSrc.Backup(ctx, path)
		require.ErrorIs(t, err, anystore.ErrWrongKey)
		require.NotErrorIs(t, err, anystore.ErrCorrupt)
	})
}

// The key salt in the header (bytes 72-87) is outside every page's tag. A
// passphrase database whose salt was overwritten with another value that is
// not all zeros derives another key from the passphrase: ErrWrongKey. A salt
// overwritten with zeros is the mark of a file that is not encrypted:
// ErrNotEncrypted. Neither is corruption.
func TestOpen_OverwrittenSalt(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		salt byte
		want error
	}{
		{"another salt", 0x5A, anystore.ErrWrongKey},
		{"zeros", 0, anystore.ErrNotEncrypted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, cfg := encryptedDB(t)
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			require.NoError(t, err)
			_, err = f.WriteAt(bytes.Repeat([]byte{tc.salt}, 16), 72)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			var calls integrityCalls
			cfg.OnIntegrityError = calls.hook
			_, err = anystore.Open(ctx, path, &cfg)
			require.ErrorIs(t, err, tc.want)
			require.NotErrorIs(t, err, anystore.ErrCorrupt)
			require.Empty(t, calls.get())
		})
	}
}

// fileState is the content of a database file and of its WAL ("" when there
// is none).
func fileState(t *testing.T, path string) [2]string {
	t.Helper()
	var st [2]string
	for i, p := range []string{path, path + "-wal"} {
		b, err := os.ReadFile(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		require.NoError(t, err)
		st[i] = string(b)
	}
	return st
}

// writeDocs creates the database at path with cfg, puts n documents into
// collection "c", and closes it.
func writeDocs(t *testing.T, path string, cfg anystore.Config, n int) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, path, &cfg)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%0400d"}`, i, i))))
	}
	require.NoError(t, db.Close())
}

// requireDocs opens the database at path with cfg and requires n documents
// in collection "c".
func requireDocs(t *testing.T, path string, cfg anystore.Config, n int) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, path, &cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.Equal(t, anystore.IntegrityAEAD, db.IntegrityMode())
	coll, err := db.OpenCollection(ctx, "c")
	require.NoError(t, err)
	got, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, n, got)
}

// A Key is the cipher key itself: a database created with it opens with it,
// and KDFIterations, which only a Passphrase uses, makes no difference. The
// encryption errors are those of a passphrase database.
func TestOpen_KeyRoundTrip(t *testing.T) {
	ctx := context.Background()
	key := bytes.Repeat([]byte{0x42}, 32)
	path := filepath.Join(t.TempDir(), "enc.db")
	writeDocs(t, path, anystore.Config{Encryption: anystore.EncryptionConfig{Key: bytes.Clone(key), KDFIterations: 1000}}, 100)

	for _, iter := range []int{0, 1000, math.MaxInt} {
		requireDocs(t, path, anystore.Config{Encryption: anystore.EncryptionConfig{Key: bytes.Clone(key), KDFIterations: iter}}, 100)
	}
	for _, ct := range []anystore.CipherType{anystore.CipherAES256GCM, anystore.CipherChaCha20Poly1305} {
		ctPath := filepath.Join(t.TempDir(), "enc.db")
		writeDocs(t, ctPath, anystore.Config{Encryption: anystore.EncryptionConfig{Key: bytes.Clone(key), CipherType: ct}}, 10)
		requireDocs(t, ctPath, anystore.Config{Encryption: anystore.EncryptionConfig{Key: bytes.Clone(key), CipherType: ct}}, 10)
	}

	_, err := anystore.Open(ctx, path, nil)
	require.ErrorIs(t, err, anystore.ErrEncryptionKeyRequired)
	other := bytes.Clone(key)
	other[0] ^= 1
	_, err = anystore.Open(ctx, path, &anystore.Config{Encryption: anystore.EncryptionConfig{Key: other}})
	require.ErrorIs(t, err, anystore.ErrWrongKey)
	require.NotErrorIs(t, err, anystore.ErrCorrupt)

	plain := filepath.Join(t.TempDir(), "plain.db")
	writeDocs(t, plain, anystore.Config{}, 1)
	_, err = anystore.Open(ctx, plain, &anystore.Config{Encryption: anystore.EncryptionConfig{Key: key}})
	require.ErrorIs(t, err, anystore.ErrNotEncrypted)
}

// A Passphrase is stretched whatever its length, so a Passphrase and a Key
// of the same 32 bytes are two keys. A file created with a Key, as a
// 32-byte Passphrase was used before v2.6.1, is ErrWrongKey with a
// Passphrase of the same bytes; a file created with a 32-byte Passphrase is
// ErrWrongKey with a Key of the same bytes. Neither is corruption, and
// neither changes the file.
func TestOpen_KeyAndPassphraseOfTheSameBytes(t *testing.T) {
	ctx := context.Background()
	secret := []byte("0123456789abcdef0123456789abcdef")
	require.Len(t, secret, 32)
	asKey := anystore.Config{Encryption: anystore.EncryptionConfig{Key: secret}}
	asPassphrase := anystore.Config{Encryption: anystore.EncryptionConfig{Passphrase: secret, KDFIterations: 1000}}
	asPassphraseDefaultKDF := anystore.Config{Encryption: anystore.EncryptionConfig{Passphrase: secret}}

	for _, tc := range []struct {
		name   string
		create anystore.Config
		wrong  []anystore.Config
	}{
		{"created with Key", asKey, []anystore.Config{asPassphrase, asPassphraseDefaultKDF}},
		{"created with a 32-byte Passphrase", asPassphrase, []anystore.Config{asKey}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "enc.db")
			writeDocs(t, path, tc.create, 100)
			before := fileState(t, path)
			for _, wrong := range tc.wrong {
				var calls integrityCalls
				wrong.OnIntegrityError = calls.hook
				_, err := anystore.Open(ctx, path, &wrong)
				require.ErrorIs(t, err, anystore.ErrWrongKey)
				require.NotErrorIs(t, err, anystore.ErrCorrupt)
				require.NotErrorIs(t, err, anystore.ErrPageIntegrity)
				require.Empty(t, calls.get())
				require.Equal(t, before, fileState(t, path), "a wrong key leaves the file as it was")
			}
			requireDocs(t, path, tc.create, 100)
		})
	}
}

// Open refuses a Key that is not 32 bytes long, an empty one included, and
// a Key set together with a Passphrase, with ErrInvalidConfig naming the
// field, before it creates anything.
func TestOpen_InvalidKey(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	for _, enc := range []anystore.EncryptionConfig{
		{Key: []byte{}},
		{Key: make([]byte, 31)},
		{Key: make([]byte, 33)},
		{Key: make([]byte, 64)},
		{Key: make([]byte, 32), Passphrase: []byte("pw")},
		{Key: make([]byte, 32), Passphrase: make([]byte, 32)},
	} {
		d, err := anystore.Open(ctx, path, &anystore.Config{Encryption: enc})
		require.ErrorIs(t, err, anystore.ErrInvalidConfig, "Key of %d bytes, Passphrase %q", len(enc.Key), enc.Passphrase)
		require.ErrorContains(t, err, "Encryption.Key")
		require.Nil(t, d)
		d, err = anystore.Open(ctx, "", &anystore.Config{InMemory: true, Encryption: enc})
		require.ErrorIs(t, err, anystore.ErrInvalidConfig)
		require.Nil(t, d)
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "a refused Open creates no file")
}

// Enabled reports a Passphrase or a Key, and nothing else.
func TestEncryptionConfig_Enabled(t *testing.T) {
	for _, tc := range []struct {
		enc  anystore.EncryptionConfig
		want bool
	}{
		{anystore.EncryptionConfig{}, false},
		{anystore.EncryptionConfig{KDFIterations: 1000, CipherType: anystore.CipherChaCha20Poly1305}, false},
		{anystore.EncryptionConfig{Passphrase: []byte("pw")}, true},
		{anystore.EncryptionConfig{Passphrase: []byte{}}, true},
		{anystore.EncryptionConfig{Key: make([]byte, 32)}, true},
		{anystore.EncryptionConfig{Key: []byte{}}, true},
	} {
		require.Equal(t, tc.want, tc.enc.Enabled(), "%+v", tc.enc)
	}
}
