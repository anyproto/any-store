package anystore

import (
	"math"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// A CacheSize or ReadConcurrency past what Open can allocate is taken as its
// bound; zero and negative values take the defaults. The bounds are SQLite's
// cache_size clamp, or on a 32-bit target 2 GiB of 4 KiB pages, and 65,536
// readers.
func TestConfig_CacheSizes(t *testing.T) {
	if math.MaxInt == math.MaxInt32 {
		require.Equal(t, 524_288, maxCacheSize)
	} else {
		require.Equal(t, 2_147_418_112, maxCacheSize)
	}
	require.Equal(t, 65_536, maxReadConcurrency)

	cs, rc := (&Config{}).cacheSizes()
	require.Equal(t, 5000, cs)
	require.Equal(t, max(runtime.NumCPU()-1, 4), rc)

	cs, rc = (&Config{CacheSize: -1, ReadConcurrency: -1}).cacheSizes()
	require.Equal(t, 5000, cs)
	require.Equal(t, max(runtime.NumCPU()-1, 4), rc)

	cs, rc = (&Config{CacheSize: 7, ReadConcurrency: 3}).cacheSizes()
	require.Equal(t, 7, cs)
	require.Equal(t, 3, rc)

	cs, rc = (&Config{CacheSize: 524_288, ReadConcurrency: 65_536}).cacheSizes()
	require.Equal(t, 524_288, cs)
	require.Equal(t, 65_536, rc)

	cs, rc = (&Config{CacheSize: maxCacheSize + 1, ReadConcurrency: 65_537}).cacheSizes()
	require.Equal(t, maxCacheSize, cs)
	require.Equal(t, 65_536, rc)

	cs, rc = (&Config{CacheSize: math.MaxInt, ReadConcurrency: math.MaxInt}).cacheSizes()
	require.Equal(t, maxCacheSize, cs)
	require.Equal(t, 65_536, rc)
}

// KDFIterations is refused past 2^31-1, SQLCipher's kdf_iter range, or on a
// 32-bit target past 2^31-2, as Go's PBKDF2 loop never ends at the largest
// int, when a passphrase goes through the KDF, and only then: a 32-byte
// passphrase is the key itself, and no passphrase is no encryption. A count
// an earlier release created a database with, above 100,000,000 included,
// is taken.
func TestConfig_KDFIterationsBound(t *testing.T) {
	if math.MaxInt == math.MaxInt32 {
		require.Equal(t, math.MaxInt32-1, maxKDFIterations)
	} else {
		require.Equal(t, math.MaxInt32, maxKDFIterations)
	}
	pass := []byte("correct horse")
	for _, n := range []int{100_000_001, maxKDFIterations} {
		require.NoError(t, (&Config{Encryption: EncryptionConfig{Passphrase: pass, KDFIterations: n}}).validate())
	}
	for _, n := range []int{maxKDFIterations + 1, math.MaxInt} {
		err := (&Config{Encryption: EncryptionConfig{Passphrase: pass, KDFIterations: n}}).validate()
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.ErrorContains(t, err, "Encryption.KDFIterations")
	}
	require.NoError(t, (&Config{Encryption: EncryptionConfig{Passphrase: make([]byte, 32), KDFIterations: math.MaxInt}}).validate())
	require.NoError(t, (&Config{Encryption: EncryptionConfig{KDFIterations: math.MaxInt}}).validate())
}

// An unknown CipherType is refused with a passphrase, raw key or not, and
// ignored without one, as the rest of EncryptionConfig is.
func TestConfig_CipherTypeKnown(t *testing.T) {
	for _, pass := range [][]byte{[]byte("correct horse"), make([]byte, 32)} {
		for _, ct := range []CipherType{CipherAES256GCM, CipherChaCha20Poly1305, CipherXChaCha20Poly1305} {
			require.NoError(t, (&Config{Encryption: EncryptionConfig{Passphrase: pass, CipherType: ct}}).validate())
		}
		err := (&Config{Encryption: EncryptionConfig{Passphrase: pass, CipherType: "rot13"}}).validate()
		require.ErrorIs(t, err, ErrInvalidConfig)
		require.ErrorContains(t, err, "Encryption.CipherType")
	}
	require.NoError(t, (&Config{Encryption: EncryptionConfig{CipherType: "rot13"}}).validate())
}
