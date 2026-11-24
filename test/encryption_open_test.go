package test

import (
	"context"
	"fmt"
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
	codec, err := anystore.NewAESCodec(key)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		enc  anystore.EncryptionConfig
	}{
		{"passphrase", anystore.EncryptionConfig{Passphrase: []byte("wrong passphrase"), KDFIterations: 1000}},
		{"raw key", anystore.EncryptionConfig{Passphrase: key}},
		{"codec", anystore.EncryptionConfig{Codec: codec}},
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

		src, err := anystore.Open(ctx, path, &cfg)
		require.NoError(t, err)
		defer src.Close()
		err = src.Backup(ctx, plain)
		require.ErrorIs(t, err, anystore.ErrNotEncrypted)
	})
}
