package anystore

import "github.com/anyproto/any-store/internal/btree"

// Codec is the pluggable page-encryption interface. Implement this to
// provide a custom AEAD (HSM-backed, external key management, or any
// authenticated cipher not offered by the built-in CipherType values).
//
// The pager installs one Codec per DB and routes every file / WAL I/O
// site through it. Implementations must be safe for concurrent use:
// a single Codec serves the writer and all reader goroutines.
//
// Overhead() bytes are reserved at the end of every page for codec
// metadata (nonce, tag, any padding). The btree cell layout automatically
// respects this via (page_size - reserve_size). Must be constant for the
// codec's lifetime and a multiple of 16 (AES block size) for alignment.
//
// Encrypt/Decrypt operate on full-page buffers. src and dst are both
// exactly pageSize; dst must not alias src. The 1-based page number pgno
// is bound into the authentication tag as associated data so that moving
// a page to a different file offset invalidates its MAC.
type Codec = btree.Codec

// CipherType selects a built-in AEAD. See the individual constants for
// trade-offs.
type CipherType = btree.CipherType

// CipherAES256GCM is AES-256 in Galois/Counter Mode. Default choice;
// hardware-accelerated on modern CPUs. Per-page overhead: 32 bytes.
const CipherAES256GCM = btree.CipherAES256GCM

// CipherChaCha20Poly1305 is ChaCha20 with Poly1305 authentication.
// Constant-time in pure software. Per-page overhead: 32 bytes.
const CipherChaCha20Poly1305 = btree.CipherChaCha20Poly1305

// CipherXChaCha20Poly1305 is the 24-byte-nonce variant of ChaCha20-
// Poly1305. Per-page overhead: 48 bytes. Use only for workloads with
// very high per-key write volumes where 12-byte random-nonce collision
// is a concern.
const CipherXChaCha20Poly1305 = btree.CipherXChaCha20Poly1305

// ErrPageIntegrity is the umbrella error returned (wrapped) by every
// codec when a page fails its on-disk integrity check, regardless of
// mode. Callers match it with errors.Is to handle both the cksum-only
// and AEAD modes uniformly:
//
//	if errors.Is(err, anystore.ErrPageIntegrity) { ... }
//
// The wrapped inner detail carries the mode-specific message
// ("checksum mismatch" or "AEAD authentication failed"); callers that
// need a programmatic discriminator should use SweepError.Kind from
// VerifyIntegrity instead of parsing strings.
var ErrPageIntegrity = btree.ErrPageIntegrity

// NewAESCodec constructs a codec using AES-256-GCM with a raw 32-byte key.
// Prefer EncryptionConfig.Passphrase for the common passphrase-based case;
// use this directly when managing key material externally (HSM, KMS, etc).
func NewAESCodec(key []byte) (Codec, error) {
	return btree.NewAESCodec(key)
}

// NewChaCha20Poly1305Codec constructs a ChaCha20-Poly1305 codec with a
// raw 32-byte key.
func NewChaCha20Poly1305Codec(key []byte) (Codec, error) {
	return btree.NewChaCha20Poly1305Codec(key)
}

// NewXChaCha20Poly1305Codec constructs an XChaCha20-Poly1305 codec with a
// raw 32-byte key. 24-byte nonce.
func NewXChaCha20Poly1305Codec(key []byte) (Codec, error) {
	return btree.NewXChaCha20Poly1305Codec(key)
}

// DeriveKey stretches a user passphrase to a 32-byte AES key using
// PBKDF2-HMAC-SHA256 against the supplied salt. Iterations defaults to
// 256,000 (SQLCipher v4 default) when <= 0. Exposed for users who need
// to derive keys for BYO codecs; normal usage should set
// EncryptionConfig.Passphrase directly.
func DeriveKey(passphrase, salt []byte, iterations int) []byte {
	return btree.DeriveKey(passphrase, salt, iterations)
}

// DefaultKDFIterations is the PBKDF2 iteration count used when
// EncryptionConfig.KDFIterations is zero.
const DefaultKDFIterations = btree.DefaultKDFIterations
