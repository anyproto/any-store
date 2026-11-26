package btree

import (
	"crypto/pbkdf2"
	"crypto/sha256"
)

// DefaultKDFIterations is the PBKDF2 iteration count used when
// Options.KDFIterations is zero. Matches SQLCipher 4.x default.
const DefaultKDFIterations = 256_000

// KeyLen is the derived AES-256 key length in bytes.
const KeyLen = 32

// SaltLen is the KDF salt length in bytes. Stored in the dbHeader.
const SaltLen = 16

// DeriveKey runs PBKDF2-HMAC-SHA256 to derive a KeyLen-byte AES key from
// a user passphrase. When iterations is zero, DefaultKDFIterations is used.
//
// PBKDF2 is a password-stretching KDF. Open runs it on every
// Options.Passphrase, whatever its length, and never on Options.Key, a raw
// key the codec takes as it is.
func DeriveKey(passphrase, salt []byte, iterations int) []byte {
	if iterations <= 0 {
		iterations = DefaultKDFIterations
	}
	key, err := pbkdf2.Key(sha256.New, string(passphrase), salt, iterations, KeyLen)
	if err != nil {
		// pbkdf2.Key only returns an error on invalid parameters (zero key
		// length, zero iterations — both prevented above) or on the hash
		// being nil. None of these can happen with our inputs.
		panic("btree: pbkdf2.Key unexpected error: " + err.Error())
	}
	return key
}
