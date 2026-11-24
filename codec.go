package anystore

import "github.com/anyproto/any-store/v2/internal/btree"

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

// DefaultKDFIterations is the PBKDF2 iteration count used when
// EncryptionConfig.KDFIterations is zero.
const DefaultKDFIterations = btree.DefaultKDFIterations
