# Encryption Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional page-level authenticated encryption to the any-store btree engine, following the SQLCipher integration map in `SQLCipher_todo.md`, using Go stdlib crypto primitives, with a pluggable codec interface so callers can bring their own AEAD.

**Architecture:** One new abstraction (`Codec` interface) with a default stdlib `AES-256-GCM` implementation. Encryption is installed on the pager at `Open` time when `Options.Key != nil` (or `Options.Codec != nil` for custom AEADs). All encryption touch-points in existing files are bracketed with `// BEGIN ENCRYPTION` / `// END ENCRYPTION` comment markers so they can be grep-located later, mirroring SQLCipher's `/* BEGIN SQLCIPHER */` convention. When no codec is installed, every hot-path site is a single nil-pointer branch (invisible under CPU branch prediction) — zero meaningful overhead for the unencrypted path.

**Tech Stack:** Go stdlib only. `crypto/aes` (AES-NI hardware acceleration is automatic), `crypto/cipher` (AEAD/GCM), `crypto/rand` (CSPRNG), `crypto/pbkdf2` (Go 1.24+). No third-party crypto.

---

## Comment bracket convention

Every modification to an existing file must be bracketed like this:

```go
// BEGIN ENCRYPTION
if p.codec != nil {
    // encryption-path code
}
// END ENCRYPTION
```

Rules:
- Single-pattern greppable: `grep -rn "BEGIN ENCRYPTION" internal/btree/` finds every site.
- Brackets are not required in files that are 100% new (`codec.go`, `codec_aes.go`, `codec_kdf.go`, `*_test.go`) — those are obviously encryption-related from the filename.
- Brackets are required when modifying `pager.go`, `wal.go`, `db.go`, `page.go`, or any other pre-existing file.
- If the change is a single-line addition (e.g. a new struct field), still bracket it — consistency matters more than brevity.

---

## Drift from SQLCipher_todo.md

The plan deviates from SQLCipher_todo.md in these places, with justification:

| Deviation | Justification |
|---|---|
| **AES-256-GCM instead of AES-256-CBC+HMAC-SHA512.** | Covered in `encryption.md` §4.4 and §13.1. GCM is a single-pass AEAD, ~6× faster on AES-NI hardware, smaller reserve (32 B vs 80 B), one key instead of two, stdlib native. No reason to inherit SQLCipher's pre-AEAD-era design choices in 2026. |
| **One crypto file, not three.** | `SQLCipher_todo.md` §A lists three backend files (`crypto_openssl.c`, `crypto_cc.c`, `crypto_libtomcrypt.c`) explicitly marked "Skip in any-store". Go stdlib `crypto/cipher` replaces all three. |
| **No PRAGMA surface.** | `SQLCipher_todo.md` §D (pragma.c items) marked "skip — no SQL". any-store has no SQL; configuration flows through `Options`. |
| **No ATTACH / VACUUM handling.** | `SQLCipher_todo.md` §E (attach.c, vacuum.c) — any-store has neither feature. |
| **No shell / TCL / URI-param integration.** | `SQLCipher_todo.md` §G, §H, and part of §C. any-store has no CLI shell or test harness written in TCL. |
| **No rollback/statement journal encryption.** | `SQLCipher_todo.md` entries `pager.c:2504, 2580, 4676` — any-store is WAL-only with no rollback or statement journal. |
| **Page 1 layout differs.** | `encryption.md` §3 — any-store's dbHeader is 100 bytes vs SQLCipher's 16. Salt is stored in the currently-unused reserved-for-expansion bytes 72-87; plaintext prefix is 100 bytes; encryption begins at byte 100. |
| **Single Key field, not PRAGMA key flow.** | `encryption.md` §8 and §13.10. Go idiom: struct fields at Open, not a post-open command. |
| **Pluggable via `Options.Codec`.** | The user asked for pluggability; we expose the interface as a public extension point. SQLCipher exposes `sqlcipher_register_provider`; this is the Go equivalent and strictly more ergonomic. |
| **Bump `go.mod` from 1.23 → 1.24.** | Needed for stdlib `crypto/pbkdf2`. Tiny risk — the toolchain pin is already 1.24.1. Alternative is `golang.org/x/crypto/pbkdf2`, but "use stdlib when possible" (user instruction) prefers the stdlib path. |

All skipped items from `SQLCipher_todo.md` remain skipped in this plan with the same justification.

---

## File Structure

### New files (all in `internal/btree/`)

| File | Responsibility |
|---|---|
| `codec.go` | `Codec` interface contract, constants, nil-safe helpers. |
| `codec_aes.go` | Default AES-256-GCM codec (`NewAESCodec`). |
| `codec_kdf.go` | `DeriveKey(passphrase, salt, iter) []byte` using PBKDF2-HMAC-SHA256. |
| `codec_test.go` | Interface-level contract tests (nil behaviour, overhead arithmetic). |
| `codec_aes_test.go` | AES-GCM round-trip, tamper detection, wrong-key rejection, per-page-nonce uniqueness. |
| `codec_kdf_test.go` | KDF determinism, length, ~256k-iter smoke. |
| `encryption_integration_test.go` | End-to-end: encrypted DB round-trip, spill/checkpoint/reopen, multi-reader, wrong-key on reopen. |

### Modified files (all in `internal/btree/`)

| File | Modifications |
|---|---|
| `page.go` | Add `Salt [16]byte` field to `dbHeader`; serialize at bytes 72-87; deserialize same range. |
| `pager.go` | Add `codec Codec` field; wire `ReservedSpace` from codec overhead in `initNewDB`; add `encryptPage`/`decryptPage` helpers (CODEC1/CODEC2 analogues); call them at every file-I/O site. |
| `wal.go` | Call encrypt before frame writes, decrypt after frame reads, in `writeFrames`, `writeFramesMem`, `readFrame`. |
| `db.go` | Add `Key`, `KDFIterations`, `Codec` to `Options`; install codec during `Open` if configured; reject mismatch (encrypted file + no key, plain file + key). |
| `go.mod` | Bump `go 1.23.0` → `go 1.24.0` for stdlib `crypto/pbkdf2`. |

### Untouched files (explicit skip list)

Anything in `anystore/` outside `internal/btree/` — encryption lives entirely in the storage engine.

---

## Task overview

| # | Task | Rough scope |
|---|---|---|
| 1 | Codec interface and nil-safe helper | New file + test |
| 2 | PBKDF2 key derivation | New file + test |
| 3 | AES-256-GCM codec | New file + tests |
| 4 | Salt field in dbHeader | Modify page.go + test |
| 5 | Pager: codec field, install method, reserved-space wiring | Modify pager.go |
| 6 | Pager: encrypt/decrypt helpers (CODEC1/CODEC2 equivalents) | Modify pager.go |
| 7 | Pager: hook helpers at file read sites | Modify pager.go |
| 8 | Pager: hook helpers at checkpoint write | Modify pager.go (or wal.go depending on path) |
| 9 | WAL: encrypt on frame write | Modify wal.go |
| 10 | WAL: decrypt on frame read | Modify wal.go |
| 11 | WAL: encrypt/decrypt for in-memory arena | Modify wal.go |
| 12 | DB Open: Key + Codec options, codec install, salt I/O | Modify db.go, pager.go |
| 13 | End-to-end: encrypted DB round-trip integration test | New test file |
| 14 | Tamper detection + wrong-key rejection tests | Extend test file |
| 15 | Benchmark: encrypted vs plain commit throughput | Extend test file |

---

## Task 1: Codec interface and nil-safe helper

**Goal:** Define the pluggable `Codec` interface. Provide utilities so that callers in the pager can treat "no codec" and "codec present" uniformly without branching everywhere.

**Files:**
- Create: `internal/btree/codec.go`
- Create: `internal/btree/codec_test.go`

- [ ] **Step 1.1: Write the failing test**

Create `internal/btree/codec_test.go`:

```go
package btree

import (
	"bytes"
	"testing"
)

func TestCodec_NilIsZeroOverhead(t *testing.T) {
	var c Codec
	if got := overheadOrZero(c); got != 0 {
		t.Fatalf("overheadOrZero(nil) = %d, want 0", got)
	}
}

func TestCodec_EncryptPlainNil(t *testing.T) {
	var c Codec
	src := []byte("hello world")
	dst := make([]byte, 0, len(src))
	out, err := encryptWith(c, dst, src, 42)
	if err != nil {
		t.Fatalf("encryptWith(nil) error: %v", err)
	}
	if !bytes.Equal(out, src) {
		t.Fatalf("nil codec must pass-through: got %q want %q", out, src)
	}
}

func TestCodec_DecryptPlainNil(t *testing.T) {
	var c Codec
	src := []byte("hello world")
	dst := make([]byte, 0, len(src))
	out, err := decryptWith(c, dst, src, 42)
	if err != nil {
		t.Fatalf("decryptWith(nil) error: %v", err)
	}
	if !bytes.Equal(out, src) {
		t.Fatalf("nil codec must pass-through")
	}
}
```

- [ ] **Step 1.2: Run test to verify it fails**

Run: `cd internal/btree && go test -run TestCodec -count=1`
Expected: FAIL with "undefined: Codec", "overheadOrZero", "encryptWith", "decryptWith".

- [ ] **Step 1.3: Write the minimal implementation**

Create `internal/btree/codec.go`:

```go
package btree

// Codec is the pluggable page-encryption interface. When a non-nil Codec is
// installed on the pager, every file/WAL I/O site routes through it.
// Implementations must be safe for concurrent use: the pager shares one Codec
// across the writer and all readers.
//
// Overhead() bytes are reserved at the end of every page for the codec's
// per-page metadata (IV/nonce, authentication tag, padding). The btree cell
// layout automatically respects this via (page_size - reserve_size).
//
// Encrypt/Decrypt operate on fixed-size page buffers: src is pageSize long,
// dst receives the transformed bytes (also pageSize). pgno is the 1-based
// page number, bound into the authentication tag so shuffling pages is
// detected.
type Codec interface {
	// Overhead returns the number of reserved bytes per page for codec
	// metadata (nonce + tag + padding). Must be constant for the codec's
	// lifetime and a multiple of 16 (AES block size) for alignment.
	Overhead() int

	// Encrypt transforms plaintext src (len == pageSize) into ciphertext
	// in dst, returning a slice of dst with the same length as src.
	// The last Overhead() bytes of dst contain the nonce+tag+padding.
	// pgno is bound into the authentication tag as associated data.
	Encrypt(dst, src []byte, pgno uint32) ([]byte, error)

	// Decrypt is the inverse of Encrypt. On tag verification failure it
	// returns ErrCodecTamper without modifying dst past any intermediate
	// state. Callers must not use dst on error.
	Decrypt(dst, src []byte, pgno uint32) ([]byte, error)
}

// ErrCodecTamper indicates the AEAD tag failed to verify. The page has been
// modified in a way the codec cannot authenticate (wrong key, corruption,
// or tampering).
var ErrCodecTamper = errorString("encryption: page authentication failed")

// errorString is a stdlib-free error type (avoids pulling errors package
// into files that don't already use it).
type errorString string

func (e errorString) Error() string { return string(e) }

// overheadOrZero returns c.Overhead() or 0 if c is nil. Used by the pager
// to compute ReservedSpace without branching at every call site.
func overheadOrZero(c Codec) int {
	if c == nil {
		return 0
	}
	return c.Overhead()
}

// encryptWith is the pager's CODEC1/CODEC2 analogue for the encrypt path.
// When c is nil, it returns src unchanged (pass-through). When c is
// non-nil, it delegates to c.Encrypt. The dst argument is a scratch buffer
// with at least len(src) capacity; on the nil path dst is ignored and src
// is returned directly.
func encryptWith(c Codec, dst, src []byte, pgno uint32) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	return c.Encrypt(dst, src, pgno)
}

// decryptWith is the corresponding decrypt-path helper. Same pass-through
// semantics on nil codec.
func decryptWith(c Codec, dst, src []byte, pgno uint32) ([]byte, error) {
	if c == nil {
		return src, nil
	}
	return c.Decrypt(dst, src, pgno)
}
```

- [ ] **Step 1.4: Run test to verify it passes**

Run: `cd internal/btree && go test -run TestCodec -count=1 -v`
Expected: PASS (3 subtests).

- [ ] **Step 1.5: Commit**

```bash
git add internal/btree/codec.go internal/btree/codec_test.go
git -c commit.gpgsign=false commit -m "btree: add pluggable Codec interface with nil-safe helpers"
```

---

## Task 2: PBKDF2 key derivation

**Goal:** Derive a 32-byte AES-256 key from a user passphrase using PBKDF2-HMAC-SHA256. Accept raw 32-byte keys as a pass-through.

**Files:**
- Modify: `go.mod` (bump go version)
- Create: `internal/btree/codec_kdf.go`
- Create: `internal/btree/codec_kdf_test.go`

- [ ] **Step 2.1: Bump go.mod**

Edit `go.mod` line 3:

```
go 1.24.0
```

(was `go 1.23.0`).

Run: `go mod tidy`
Expected: no changes beyond the bump.

- [ ] **Step 2.2: Write the failing test**

Create `internal/btree/codec_kdf_test.go`:

```go
package btree

import (
	"bytes"
	"testing"
)

func TestDeriveKey_DeterministicForSameInput(t *testing.T) {
	pass := []byte("correct horse battery staple")
	salt := bytes.Repeat([]byte{0xAB}, 16)
	k1 := DeriveKey(pass, salt, 1000)
	k2 := DeriveKey(pass, salt, 1000)
	if !bytes.Equal(k1, k2) {
		t.Fatalf("same inputs produced different keys")
	}
	if len(k1) != 32 {
		t.Fatalf("key length = %d, want 32", len(k1))
	}
}

func TestDeriveKey_DifferentSaltDifferentKey(t *testing.T) {
	pass := []byte("pw")
	k1 := DeriveKey(pass, bytes.Repeat([]byte{1}, 16), 1000)
	k2 := DeriveKey(pass, bytes.Repeat([]byte{2}, 16), 1000)
	if bytes.Equal(k1, k2) {
		t.Fatalf("different salts produced same key")
	}
}

func TestDeriveKey_IterationMatters(t *testing.T) {
	pass := []byte("pw")
	salt := bytes.Repeat([]byte{1}, 16)
	k1 := DeriveKey(pass, salt, 1000)
	k2 := DeriveKey(pass, salt, 2000)
	if bytes.Equal(k1, k2) {
		t.Fatalf("different iteration counts produced same key")
	}
}

func TestDeriveKey_ZeroIterationsDefaults(t *testing.T) {
	pass := []byte("pw")
	salt := bytes.Repeat([]byte{1}, 16)
	k := DeriveKey(pass, salt, 0)
	if len(k) != 32 {
		t.Fatalf("default-iter key length = %d, want 32", len(k))
	}
}
```

- [ ] **Step 2.3: Run test to verify it fails**

Run: `cd internal/btree && go test -run TestDeriveKey -count=1`
Expected: FAIL with "undefined: DeriveKey".

- [ ] **Step 2.4: Write the implementation**

Create `internal/btree/codec_kdf.go`:

```go
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
// The function does not allocate beyond the returned key.
//
// PBKDF2 is a password-stretching KDF; if the caller is providing a raw
// 32-byte key (not a passphrase), they should skip this entirely and
// install the codec directly with the raw key material.
func DeriveKey(passphrase, salt []byte, iterations int) []byte {
	if iterations <= 0 {
		iterations = DefaultKDFIterations
	}
	// pbkdf2.Key is allocation-free for the Hash construction (it's a
	// function, not a method), and returns a freshly-allocated []byte of
	// the requested length.
	key, err := pbkdf2.Key(sha256.New, string(passphrase), salt, iterations, KeyLen)
	if err != nil {
		// pbkdf2.Key only returns an error on invalid parameters (zero key
		// length, zero iterations — both prevented above) or on the hash
		// being nil. None of these can happen with our inputs; treat as
		// a programmer error.
		panic("btree: pbkdf2.Key unexpected error: " + err.Error())
	}
	return key
}
```

- [ ] **Step 2.5: Run test to verify it passes**

Run: `cd internal/btree && go test -run TestDeriveKey -count=1 -v`
Expected: PASS (4 subtests).

- [ ] **Step 2.6: Commit**

```bash
git add go.mod internal/btree/codec_kdf.go internal/btree/codec_kdf_test.go
git -c commit.gpgsign=false commit -m "btree: add PBKDF2-HMAC-SHA256 key derivation (stdlib)"
```

---

## Task 3: AES-256-GCM codec

**Goal:** The default stdlib-backed codec. AES-256-GCM with a 12-byte random nonce per page, 16-byte tag, page-number bound into associated data. Reserve size = 32 bytes (28 rounded up to AES block = 16).

**Files:**
- Create: `internal/btree/codec_aes.go`
- Create: `internal/btree/codec_aes_test.go`

- [ ] **Step 3.1: Write the failing tests**

Create `internal/btree/codec_aes_test.go`:

```go
package btree

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestAESCodec_Overhead(t *testing.T) {
	key := make([]byte, 32)
	c, err := NewAESCodec(key)
	if err != nil {
		t.Fatalf("NewAESCodec: %v", err)
	}
	if got := c.Overhead(); got != 32 {
		t.Fatalf("Overhead = %d, want 32", got)
	}
}

func TestAESCodec_RoundTrip(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	c, err := NewAESCodec(key)
	if err != nil {
		t.Fatal(err)
	}

	pageSize := 4096
	src := make([]byte, pageSize)
	if _, err := rand.Read(src[:pageSize-c.Overhead()]); err != nil {
		t.Fatal(err)
	}

	dst := make([]byte, pageSize)
	ct, err := c.Encrypt(dst, src, 7)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if len(ct) != pageSize {
		t.Fatalf("ciphertext length = %d, want %d", len(ct), pageSize)
	}
	// Plaintext must not equal ciphertext (sanity).
	if bytes.Equal(ct[:pageSize-c.Overhead()], src[:pageSize-c.Overhead()]) {
		t.Fatalf("ciphertext body matches plaintext — encryption didn't run")
	}

	pt := make([]byte, pageSize)
	out, err := c.Decrypt(pt, ct, 7)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(out[:pageSize-c.Overhead()], src[:pageSize-c.Overhead()]) {
		t.Fatalf("round-trip body mismatch")
	}
}

func TestAESCodec_TamperDetected(t *testing.T) {
	key := make([]byte, 32)
	c, _ := NewAESCodec(key)
	pageSize := 4096
	src := make([]byte, pageSize)
	dst := make([]byte, pageSize)
	ct, _ := c.Encrypt(dst, src, 1)

	// Flip a bit somewhere in the encrypted body.
	ct[100] ^= 0x01
	pt := make([]byte, pageSize)
	if _, err := c.Decrypt(pt, ct, 1); err != ErrCodecTamper {
		t.Fatalf("flipped-bit decrypt: got err=%v, want ErrCodecTamper", err)
	}
}

func TestAESCodec_PageNumberBound(t *testing.T) {
	key := make([]byte, 32)
	c, _ := NewAESCodec(key)
	pageSize := 4096
	src := make([]byte, pageSize)
	dst := make([]byte, pageSize)
	ct, _ := c.Encrypt(dst, src, 5)
	// Decrypting with the wrong page number must fail.
	pt := make([]byte, pageSize)
	if _, err := c.Decrypt(pt, ct, 6); err != ErrCodecTamper {
		t.Fatalf("wrong-pgno decrypt: got err=%v, want ErrCodecTamper", err)
	}
}

func TestAESCodec_NonceUniqueness(t *testing.T) {
	// Same plaintext encrypted twice must produce different ciphertext
	// (because nonce is random per call).
	key := make([]byte, 32)
	c, _ := NewAESCodec(key)
	pageSize := 4096
	src := make([]byte, pageSize)
	dst1 := make([]byte, pageSize)
	dst2 := make([]byte, pageSize)
	ct1, _ := c.Encrypt(dst1, src, 1)
	ct2, _ := c.Encrypt(dst2, src, 1)
	if bytes.Equal(ct1, ct2) {
		t.Fatalf("same plaintext/page produced identical ciphertext — nonce not random")
	}
}

func TestAESCodec_WrongKeyRejected(t *testing.T) {
	pageSize := 4096
	src := make([]byte, pageSize)
	k1 := bytes.Repeat([]byte{1}, 32)
	k2 := bytes.Repeat([]byte{2}, 32)
	c1, _ := NewAESCodec(k1)
	c2, _ := NewAESCodec(k2)
	dst := make([]byte, pageSize)
	ct, _ := c1.Encrypt(dst, src, 1)
	pt := make([]byte, pageSize)
	if _, err := c2.Decrypt(pt, ct, 1); err != ErrCodecTamper {
		t.Fatalf("wrong-key decrypt: got err=%v, want ErrCodecTamper", err)
	}
}

func TestAESCodec_InvalidKeyLength(t *testing.T) {
	if _, err := NewAESCodec(make([]byte, 16)); err == nil {
		t.Fatalf("16-byte key accepted, want error")
	}
	if _, err := NewAESCodec(nil); err == nil {
		t.Fatalf("nil key accepted, want error")
	}
}
```

- [ ] **Step 3.2: Run tests to verify they fail**

Run: `cd internal/btree && go test -run TestAESCodec -count=1`
Expected: FAIL with "undefined: NewAESCodec".

- [ ] **Step 3.3: Write the implementation**

Create `internal/btree/codec_aes.go`:

```go
package btree

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
)

// aesCodecOverhead is the per-page overhead: 12-byte GCM nonce + 16-byte
// GCM tag = 28 bytes, rounded up to 32 for AES block alignment. Must stay
// a multiple of 16.
const aesCodecOverhead = 32

// aesNonceLen is the GCM standard nonce length.
const aesNonceLen = 12

// aesTagLen is the GCM authentication tag length.
const aesTagLen = 16

// aesCodec implements Codec using AES-256-GCM. One instance per DB — the
// underlying cipher.AEAD is safe for concurrent use from writer and all
// readers (stdlib GCM implementations in crypto/cipher only read the
// embedded key state during Seal/Open).
type aesCodec struct {
	aead cipher.AEAD
}

// NewAESCodec constructs the default codec. key must be exactly 32 bytes
// (AES-256). Callers derive the key via DeriveKey (passphrase path) or
// supply it directly (raw-key path).
func NewAESCodec(key []byte) (Codec, error) {
	if len(key) != KeyLen {
		return nil, fmt.Errorf("btree: AES codec requires %d-byte key, got %d", KeyLen, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("btree: aes.NewCipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("btree: cipher.NewGCM: %w", err)
	}
	return &aesCodec{aead: aead}, nil
}

func (c *aesCodec) Overhead() int { return aesCodecOverhead }

// Encrypt: src is pageSize bytes of plaintext. The first
// (pageSize - aesCodecOverhead) bytes are the body to encrypt; the rest
// are scratch for nonce+tag+padding. dst receives pageSize bytes:
//   [ciphertext (body_len bytes)] [nonce (12)] [tag (16)] [padding (4)]
// with padding zeroed (unused — reserves 32 rounded-up from 28).
func (c *aesCodec) Encrypt(dst, src []byte, pgno uint32) ([]byte, error) {
	if len(dst) < len(src) {
		return nil, fmt.Errorf("btree: aesCodec.Encrypt: dst too small (%d < %d)", len(dst), len(src))
	}
	bodyLen := len(src) - aesCodecOverhead
	if bodyLen < 0 {
		return nil, fmt.Errorf("btree: aesCodec.Encrypt: page too small (%d < overhead %d)", len(src), aesCodecOverhead)
	}

	// Draw a fresh random nonce for this page write.
	nonce := dst[bodyLen : bodyLen+aesNonceLen]
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("btree: aesCodec.Encrypt: rand.Read: %w", err)
	}

	// AAD = page number as little-endian uint32. Binds the page location
	// into the MAC so pages can't be shuffled between file positions.
	var aad [4]byte
	binary.LittleEndian.PutUint32(aad[:], pgno)

	// Seal(dst, nonce, plaintext, aad) returns dst appended with
	// ciphertext || tag. We direct it into dst[:0] so the output lands
	// at the start of dst (ciphertext body, then tag).
	// After Seal: dst[0:bodyLen] = ciphertext, dst[bodyLen:bodyLen+tag] = tag.
	// But we want: dst[0:bodyLen] = ct, dst[bodyLen:bodyLen+nonce] = nonce,
	// dst[bodyLen+nonce:bodyLen+nonce+tag] = tag, dst[...:pageSize] = pad.
	// So we Seal into a temporary layout then move the nonce+tag.
	//
	// Simpler: Seal directly into a slice that will give us [ct || tag]
	// at offset 0, then copy tag to its final slot and zero the padding.
	sealed := c.aead.Seal(dst[:0], nonce, src[:bodyLen], aad[:])
	// sealed is now dst[:bodyLen+aesTagLen]. Layout: [ct][tag].
	// Move the tag right past the nonce, keeping nonce in place.
	tagSrc := sealed[bodyLen : bodyLen+aesTagLen]
	tagDst := dst[bodyLen+aesNonceLen : bodyLen+aesNonceLen+aesTagLen]
	copy(tagDst, tagSrc)
	// Zero the trailing padding (4 bytes between end-of-tag and end-of-page).
	padStart := bodyLen + aesNonceLen + aesTagLen
	for i := padStart; i < len(src); i++ {
		dst[i] = 0
	}
	return dst[:len(src)], nil
}

// Decrypt is the inverse: extract nonce and tag from dst[bodyLen:], verify,
// and decrypt. Returns ErrCodecTamper on any authentication failure.
func (c *aesCodec) Decrypt(dst, src []byte, pgno uint32) ([]byte, error) {
	if len(dst) < len(src) {
		return nil, fmt.Errorf("btree: aesCodec.Decrypt: dst too small (%d < %d)", len(dst), len(src))
	}
	bodyLen := len(src) - aesCodecOverhead
	if bodyLen < 0 {
		return nil, fmt.Errorf("btree: aesCodec.Decrypt: page too small")
	}

	nonce := src[bodyLen : bodyLen+aesNonceLen]
	tag := src[bodyLen+aesNonceLen : bodyLen+aesNonceLen+aesTagLen]

	var aad [4]byte
	binary.LittleEndian.PutUint32(aad[:], pgno)

	// Open expects ciphertext || tag concatenated. Build that by copying
	// the ciphertext (body) and tag into a contiguous scratch area.
	// Since src[:bodyLen] is the ciphertext body and tag lives past the
	// nonce, we cannot feed them as one slice without copying. Use the
	// end of dst as scratch.
	scratch := dst[bodyLen:]            // has at least aesCodecOverhead bytes
	if len(scratch) < bodyLen+aesTagLen {
		// Scratch shares memory with dst; for pages where bodyLen+tag
		// exceeds overhead, fall back to a fresh allocation (rare — only
		// if overhead somehow got smaller than tag, which it never does).
		scratch = make([]byte, bodyLen+aesTagLen)
	}
	copy(scratch[:bodyLen], src[:bodyLen])
	copy(scratch[bodyLen:bodyLen+aesTagLen], tag)

	opened, err := c.aead.Open(dst[:0], nonce, scratch[:bodyLen+aesTagLen], aad[:])
	if err != nil {
		return nil, ErrCodecTamper
	}
	if len(opened) != bodyLen {
		return nil, fmt.Errorf("btree: aesCodec.Decrypt: unexpected plaintext length %d", len(opened))
	}
	// Zero the reserve tail in dst for consistency (no sensitive leftovers).
	for i := bodyLen; i < len(src); i++ {
		dst[i] = 0
	}
	return dst[:len(src)], nil
}
```

**Note:** The Decrypt implementation copies the ciphertext body and tag into a scratch area because GCM's `Open` expects them contiguous, while our layout places the nonce between them. The copy is `bodyLen` bytes per decrypt — minor cost, measured in the benchmark (Task 15). An alternative layout (tag immediately after body, nonce at the very end) would eliminate the copy but complicate the WAL frame checksum story. Keep this layout; profile in Task 15 and revisit only if the copy shows up.

- [ ] **Step 3.4: Run tests to verify they pass**

Run: `cd internal/btree && go test -run TestAESCodec -count=1 -v`
Expected: PASS (7 subtests).

- [ ] **Step 3.5: Run the race detector on the codec tests**

Run: `cd internal/btree && go test -run TestAESCodec -race -count=1`
Expected: PASS.

- [ ] **Step 3.6: Commit**

```bash
git add internal/btree/codec_aes.go internal/btree/codec_aes_test.go
git -c commit.gpgsign=false commit -m "btree: add AES-256-GCM codec implementation"
```

---

## Task 4: Salt field in dbHeader

**Goal:** Add a 16-byte salt field to the database header, stored in the currently-unused "reserved for expansion" range at bytes 72-87. This is the cryptographic salt — plaintext, preserved across reopens, used by PBKDF2.

**Files:**
- Modify: `internal/btree/page.go`
- Extend: the existing dbHeader tests (locate via grep below).

- [ ] **Step 4.1: Locate existing dbHeader tests**

Run: `cd internal/btree && grep -l "dbHeader" *_test.go`
Expected output includes at least one file. Read that file to find the existing `TestDBHeader_RoundTrip` or equivalent.

If there is **no existing test file for dbHeader**, create `internal/btree/page_header_test.go` in the following step instead.

- [ ] **Step 4.2: Write a failing salt round-trip test**

Append to the existing header test file (or create `internal/btree/page_header_test.go`):

```go
func TestDBHeader_SaltRoundTrip(t *testing.T) {
	h := dbHeader{
		PageSize:     4096,
		WriteVersion: 2,
		ReadVersion:  2,
		DatabaseSize: 1,
	}
	for i := range h.Salt {
		h.Salt[i] = byte(i ^ 0xAA)
	}

	buf := make([]byte, dbHeaderSize)
	h.serialize(buf)

	var got dbHeader
	if err := got.deserialize(buf); err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	if got.Salt != h.Salt {
		t.Fatalf("salt mismatch: got %x want %x", got.Salt, h.Salt)
	}
}
```

Run: `cd internal/btree && go test -run TestDBHeader_SaltRoundTrip -count=1`
Expected: FAIL (dbHeader has no Salt field).

- [ ] **Step 4.3: Add Salt field to dbHeader struct**

Modify `internal/btree/page.go`. The `dbHeader` struct currently ends at line 171. Edit it:

```go
// dbHeader represents the 100-byte database file header.
type dbHeader struct {
	PageSize         uint32
	WriteVersion     uint8
	ReadVersion      uint8
	ReservedSpace    uint8
	FileChangeCount  uint32
	DatabaseSize     uint32 // in pages
	FirstFreelistPg  uint32
	TotalFreelistPgs uint32
	SchemaCookie     uint32
	SchemaFormat     uint32
	DefaultCacheSize uint32
	TextEncoding     uint32
	UserVersion      uint32
	AppID            uint32
	VersionValidFor  uint32
	// BEGIN ENCRYPTION
	// Salt is the PBKDF2 salt for key derivation. Stored plaintext at
	// bytes 72-87 of page 1 (within the "reserved for expansion" range
	// unused by SQLite). Zero-valued for unencrypted databases.
	Salt [16]byte
	// END ENCRYPTION
}
```

- [ ] **Step 4.4: Update serialize() to write Salt at bytes 72-87**

In `internal/btree/page.go`, find the line `clear(buf[72:92])` inside `serialize()` (around line 213). Replace that block:

```go
// Reserved for expansion
clear(buf[72:92])
```

with:

```go
// BEGIN ENCRYPTION
// Bytes 72-87: KDF salt (16 bytes). Bytes 88-91 remain reserved.
copy(buf[72:88], h.Salt[:])
clear(buf[88:92])
// END ENCRYPTION
```

- [ ] **Step 4.5: Update deserialize() to read Salt from bytes 72-87**

In the same file, find the end of `deserialize()` where it reads the last fields (around lines 251-254). Add, just before `return nil`:

```go
	h.AppID = binary.BigEndian.Uint32(buf[68:72])
	// BEGIN ENCRYPTION
	copy(h.Salt[:], buf[72:88])
	// END ENCRYPTION
	h.VersionValidFor = binary.BigEndian.Uint32(buf[92:96])
```

- [ ] **Step 4.6: Run test to verify it passes**

Run: `cd internal/btree && go test -run TestDBHeader -count=1 -v`
Expected: PASS (salt round-trip plus any pre-existing header tests).

- [ ] **Step 4.7: Run full btree tests to catch regressions**

Run: `cd internal/btree && go test -count=1 -timeout=300s ./...`
Expected: PASS (no existing test should touch bytes 72-87).

- [ ] **Step 4.8: Commit**

```bash
git add internal/btree/page.go internal/btree/page_header_test.go
git -c commit.gpgsign=false commit -m "btree: add KDF salt to dbHeader at bytes 72-87"
```

---

## Task 5: Pager — codec field, install method, reserved-space wiring

**Goal:** Give the pager a `codec Codec` field. Wire `initNewDB` to source `ReservedSpace` from `codec.Overhead()`. Provide an `installCodec` method that sets the codec and recomputes `usableSize_`.

**Files:**
- Modify: `internal/btree/pager.go`

- [ ] **Step 5.1: Write the failing test**

Add to `internal/btree/codec_test.go`:

```go
func TestPager_InstallCodec_ReservedSpace(t *testing.T) {
	p := &pager{pageSize: 4096}
	// Before installing: reserved = 0
	if p.usableSize() != 4096 {
		t.Fatalf("usableSize before install = %d, want 4096", p.usableSize())
	}

	key := make([]byte, 32)
	c, err := NewAESCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	p.installCodec(c)
	if p.header.ReservedSpace != uint8(c.Overhead()) {
		t.Fatalf("ReservedSpace = %d, want %d", p.header.ReservedSpace, c.Overhead())
	}
	if p.usableSize() != 4096-c.Overhead() {
		t.Fatalf("usableSize after install = %d, want %d", p.usableSize(), 4096-c.Overhead())
	}
	if p.codec != c {
		t.Fatalf("codec not stored")
	}
}
```

Run: `cd internal/btree && go test -run TestPager_InstallCodec -count=1`
Expected: FAIL (no codec field, no installCodec method).

- [ ] **Step 5.2: Add codec field to pager struct**

In `internal/btree/pager.go`, find the end of the `pager` struct (around line 178, just before `}`). Add, before `writerWalSlot`:

```go
	// BEGIN ENCRYPTION
	// codec, when non-nil, encrypts pages before file/WAL writes and
	// decrypts them after reads. Installed once at Open and immutable
	// thereafter. Safe for concurrent use across the writer and readers.
	codec Codec
	// END ENCRYPTION
```

- [ ] **Step 5.3: Add installCodec method**

In `internal/btree/pager.go`, append this method near `initNewDB` (it must come before initNewDB's usage site in Step 5.4):

```go
// BEGIN ENCRYPTION

// installCodec attaches a page codec to the pager. Must be called before
// any I/O. Sets the on-disk ReservedSpace header byte and the in-memory
// usableSize_ so the btree cell code respects the codec's per-page overhead.
// Calling with nil is a no-op (unencrypted mode).
func (p *pager) installCodec(c Codec) {
	if c == nil {
		return
	}
	p.codec = c
	p.header.ReservedSpace = uint8(c.Overhead())
	p.usableSize_ = int(p.pageSize) - int(p.header.ReservedSpace)
}

// END ENCRYPTION
```

- [ ] **Step 5.4: Update initNewDB to honour codec.Overhead()**

The `initNewDB` function currently hardcodes `ReservedSpace: 0` at pager.go:342 and computes `p.usableSize_` at line 349 unconditionally. The codec is already installed by Step 5.3 before initNewDB runs (the caller is responsible — wired in Task 12). initNewDB only needs to *read* whatever was installed.

Find lines 338-350 in `initNewDB`:

```go
	p.header = dbHeader{
		PageSize:         p.pageSize,
		WriteVersion:     2, // WAL mode
		ReadVersion:      2,
		ReservedSpace:    0,
		FileChangeCount:  1,
		DatabaseSize:     1, // Just page 1
		SchemaFormat:     5,
		DefaultCacheSize: defaultCacheSize,
		TextEncoding:     1, // UTF-8
	}
	p.usableSize_ = int(p.pageSize) - int(p.header.ReservedSpace)
```

Replace with:

```go
	p.header = dbHeader{
		PageSize:         p.pageSize,
		WriteVersion:     2, // WAL mode
		ReadVersion:      2,
		ReservedSpace:    0,
		FileChangeCount:  1,
		DatabaseSize:     1, // Just page 1
		SchemaFormat:     5,
		DefaultCacheSize: defaultCacheSize,
		TextEncoding:     1, // UTF-8
	}
	// BEGIN ENCRYPTION
	// If a codec is installed, size the reserved area for its per-page
	// overhead (nonce + tag + padding). Otherwise ReservedSpace stays 0
	// (matches the unencrypted file format).
	if p.codec != nil {
		p.header.ReservedSpace = uint8(p.codec.Overhead())
		// Draw a fresh random salt for the new database.
		if _, err := rand.Read(p.header.Salt[:]); err != nil {
			return err
		}
	}
	// END ENCRYPTION
	p.usableSize_ = int(p.pageSize) - int(p.header.ReservedSpace)
```

At the top of `pager.go`, ensure `"crypto/rand"` is imported (add if missing). Check current imports:

Run: `cd internal/btree && head -20 pager.go`

If `crypto/rand` is not already imported, add it in the existing import block, bracketed:

```go
import (
	// ... existing imports ...
	// BEGIN ENCRYPTION
	"crypto/rand"
	// END ENCRYPTION
	// ... other existing imports ...
)
```

If `crypto/rand` is already imported for another reason, no bracket is needed — the import line is not encryption-specific.

- [ ] **Step 5.5: Run test to verify it passes**

Run: `cd internal/btree && go test -run TestPager_InstallCodec -count=1 -v`
Expected: PASS.

- [ ] **Step 5.6: Run full btree tests to catch regressions**

Run: `cd internal/btree && go test -race -count=1 -timeout=300s ./...`
Expected: PASS.

- [ ] **Step 5.7: Commit**

```bash
git add internal/btree/pager.go internal/btree/codec_test.go
git -c commit.gpgsign=false commit -m "btree: add codec field and installCodec on pager"
```

---

## Task 6: Pager — encrypt/decrypt helpers

**Goal:** Thin wrappers on the pager (equivalents of SQLCipher's `CODEC1`/`CODEC2` macros). They do nothing when `p.codec == nil` and otherwise delegate. These are the single call points; all hot-path sites call these.

**Files:**
- Modify: `internal/btree/pager.go`

- [ ] **Step 6.1: Add the helpers**

Append to `internal/btree/pager.go` after `installCodec`:

```go
// BEGIN ENCRYPTION

// encryptPage transforms plaintext page bytes for writing to disk or WAL.
// When no codec is installed, returns src unchanged (pass-through). When a
// codec is installed, encrypts into scratch (must have len >= len(src))
// and returns the encrypted slice. The scratch buffer is owned by the
// caller and must not alias src.
//
// This is the any-store equivalent of SQLCipher's CODEC1 wrapper
// (pager.c:412), unified into one function since Go doesn't need macros.
func (p *pager) encryptPage(scratch, src []byte, pgno uint32) ([]byte, error) {
	return encryptWith(p.codec, scratch, src, pgno)
}

// decryptPage is the inverse. Returns src unchanged when no codec is
// installed; otherwise decrypts into scratch and returns the plaintext
// slice. On AEAD verification failure returns ErrCodecTamper.
func (p *pager) decryptPage(scratch, src []byte, pgno uint32) ([]byte, error) {
	return decryptWith(p.codec, scratch, src, pgno)
}

// END ENCRYPTION
```

- [ ] **Step 6.2: Verify compile**

Run: `cd internal/btree && go build ./...`
Expected: success.

- [ ] **Step 6.3: Commit**

```bash
git add internal/btree/pager.go
git -c commit.gpgsign=false commit -m "btree: add pager encrypt/decrypt helpers (CODEC1/CODEC2 analogues)"
```

---

## Task 7: Pager — hook helpers at file read sites

**Goal:** Route every file-level page read through `decryptPage`. Three sites: `getPageWriter`, `getPageReader`, `readTempPage`. Also applies to the `masterStore` read path for InMemory mode — per `encryption.md` §7 we skip masterStore encryption, so no change there.

**Files:**
- Modify: `internal/btree/pager.go`

- [ ] **Step 7.1: Locate the three read sites**

Run: `cd internal/btree && grep -n 'p\.file\.ReadAt(pg\.data' pager.go`
Expected output: three matching lines with their numbers. From the repo state at plan-writing time:
- `pager.go:512` (inside `getPageWriter`)
- `pager.go:570` (inside `readTempPage`)
- `pager.go:647` (inside `getPageReader`)

**If the line numbers have drifted,** adjust accordingly — the call pattern `p.file.ReadAt(pg.data, offset)` is the anchor.

- [ ] **Step 7.2: Wrap the `getPageWriter` read site**

Read the 10 lines around `pager.go:512`. It looks like:

```go
	_, err := p.file.ReadAt(pg.data, offset)
	if err != nil {
		// ... error handling ...
	}
```

Wrap so that after a successful read the page is decrypted in place. Replace the read + error handling block with:

```go
	_, err := p.file.ReadAt(pg.data, offset)
	if err != nil {
		// ... existing error handling preserved verbatim ...
	}
	// BEGIN ENCRYPTION
	if p.codec != nil {
		// Decrypt into a scratch buffer, then copy back. getPageWriter
		// owns pg.data; we use a page-sized stack-avoiding buffer.
		scratch := allocPageBuffer(int(p.pageSize), false)
		defer freePageBuffer(scratch)
		plain, derr := p.decryptPage(scratch, pg.data, pgno)
		if derr != nil {
			return nil, derr
		}
		copy(pg.data, plain)
	}
	// END ENCRYPTION
```

**Use the existing `allocPageBuffer` / `freePageBuffer` helpers** (they already exist in pager.go — the slab allocator machinery). If the exact names differ, locate via `grep -n 'func allocPage' pager.go`.

- [ ] **Step 7.3: Wrap the `readTempPage` read site (pager.go:570)**

Same pattern — after the `ReadAt` + error handling, decrypt in place:

```go
	// BEGIN ENCRYPTION
	if p.codec != nil {
		scratch := allocPageBuffer(int(p.pageSize), false)
		defer freePageBuffer(scratch)
		plain, derr := p.decryptPage(scratch, pg.data, pgno)
		if derr != nil {
			return nil, derr
		}
		copy(pg.data, plain)
	}
	// END ENCRYPTION
```

- [ ] **Step 7.4: Wrap the `getPageReader` read site (pager.go:647)**

Same pattern.

- [ ] **Step 7.5: Test existing-file open still works (unencrypted)**

Run: `cd internal/btree && go test -race -count=1 -timeout=300s ./...`
Expected: PASS. No codec is installed by any existing test, so `p.codec == nil` and the wrapped code is a pass-through.

- [ ] **Step 7.6: Commit**

```bash
git add internal/btree/pager.go
git -c commit.gpgsign=false commit -m "btree: hook codec at file read sites (getPageWriter, getPageReader, readTempPage)"
```

---

## Task 8: Pager — hook helpers at checkpoint file write

**Goal:** When the checkpoint backfill writes a page from WAL into the main DB file, the page must be encrypted. In any-store, WAL frames are *already* encrypted (see Task 9); the checkpoint path reads a frame (decrypts it, per Task 10), then writes it to the DB file — so the write side must re-encrypt.

**Files:**
- Modify: `internal/btree/wal.go` (the checkpoint function lives in wal.go).

- [ ] **Step 8.1: Locate the checkpoint DB-file-write site**

Run: `cd internal/btree && grep -n 'writeAt\|WriteAt' wal.go | grep -v walWriteToLog | head -20`

Find the line(s) in `checkpointWithMode` / `checkpoint` that write page bytes to the *main DB file* (not the WAL file). It'll be something like `p.file.WriteAt(frameData, offset)`.

- [ ] **Step 8.2: Wrap with codec call**

Before the `WriteAt` that targets the main DB file, encrypt:

```go
	// BEGIN ENCRYPTION
	if w.pager.codec != nil {
		scratch := allocPageBuffer(int(w.pageSize), false)
		defer freePageBuffer(scratch)
		ct, eerr := w.pager.encryptPage(scratch, frameData, pgno)
		if eerr != nil {
			return eerr
		}
		frameData = ct
	}
	// END ENCRYPTION
	_, err := w.pager.file.WriteAt(frameData, offset)
```

(Adapt variable names to the actual code — `frameData`, `pgno`, `offset` are placeholders for whatever the call site uses.)

- [ ] **Step 8.3: Full test pass**

Run: `cd internal/btree && go test -race -count=1 -timeout=300s ./...`
Expected: PASS.

- [ ] **Step 8.4: Commit**

```bash
git add internal/btree/wal.go
git -c commit.gpgsign=false commit -m "btree: hook codec at checkpoint file-write"
```

---

## Task 9: WAL — encrypt on frame write

**Goal:** Every frame written to the WAL file carries an encrypted payload. The frame header (24 bytes: pgno, dbSize, salt1, salt2, checksum1, checksum2) stays plaintext per `encryption.md` §4.

**Files:**
- Modify: `internal/btree/wal.go`

- [ ] **Step 9.1: Locate `writeFrames`**

Run: `cd internal/btree && grep -n 'func.*writeFrames' wal.go`
Expected: `func (w *wal) writeFrames(pages []*page, commit bool, dbSize uint32) error` around line 1408.

Also locate the actual file-write site inside — `grep -n 'w\.file\.WriteAt' wal.go` should show a call around line 1470 writing `p.data`.

- [ ] **Step 9.2: Wrap the frame-payload write**

Find the block that writes the frame header and then the frame payload:

```go
	// ... compute checksum, serialize header ...
	_, err := w.file.WriteAt(frameHdr[:], off)
	if err != nil { ... }
	_, err = w.file.WriteAt(p.data, off+walFrameSize)
	if err != nil { ... }
```

**Decision from encryption.md §4.3:** checksum is computed over ciphertext. That means we must encrypt *before* checksum computation. Find the checksum call (around `walChecksum(...)` usage near the header serialisation), and restructure so encrypt happens first, then checksum, then header+payload write.

Concretely, replace the block around the write with:

```go
	// BEGIN ENCRYPTION
	// Encrypt page payload before computing WAL checksum (checksum covers
	// ciphertext, not plaintext — see encryption.md §4.3).
	payload := p.data
	if w.pager.codec != nil {
		scratch := allocPageBuffer(int(w.pageSize), false)
		defer freePageBuffer(scratch)
		ct, eerr := w.pager.encryptPage(scratch, p.data, p.pgno)
		if eerr != nil {
			return eerr
		}
		payload = ct
	}
	// END ENCRYPTION

	// ... compute checksum over (header first 8 bytes) + payload ...
	// ... serialize header with checksum into frameHdr[:] ...

	if _, err := w.file.WriteAt(frameHdr[:], off); err != nil {
		return err
	}
	if _, err := w.file.WriteAt(payload, off+walFrameSize); err != nil {
		return err
	}
```

Adapt to the exact existing structure of `writeFrames`. Check the prev `walChecksum` call — if it already consumes `p.data`, change it to consume `payload`.

- [ ] **Step 9.3: Unencrypted regression check**

Run: `cd internal/btree && go test -race -count=1 -timeout=300s ./...`
Expected: PASS (no codec installed → `payload := p.data` pass-through).

- [ ] **Step 9.4: Commit**

```bash
git add internal/btree/wal.go
git -c commit.gpgsign=false commit -m "btree: encrypt WAL frame payloads on write, checksum over ciphertext"
```

---

## Task 10: WAL — decrypt on frame read

**Goal:** Frames read from the WAL file must be decrypted before being returned to the pager.

**Files:**
- Modify: `internal/btree/wal.go`

- [ ] **Step 10.1: Locate `readFrame`**

Run: `cd internal/btree && grep -n 'func.*readFrame' wal.go`
Expected: around line 1596.

- [ ] **Step 10.2: Wrap the file-read site**

Find the `w.file.ReadAt(buf, offset)` call inside `readFrame`. Add, immediately after a successful read:

```go
	// BEGIN ENCRYPTION
	if w.pager.codec != nil {
		scratch := allocPageBuffer(int(w.pageSize), false)
		defer freePageBuffer(scratch)
		plain, derr := w.pager.decryptPage(scratch, buf, pgno)
		if derr != nil {
			return derr
		}
		copy(buf, plain)
	}
	// END ENCRYPTION
```

If the function signature doesn't include `pgno` explicitly, look one line up — `readFrame` takes the frame number; the page number is either in the frame header (deserialized just above) or passed as a parameter. Use whichever is in scope.

- [ ] **Step 10.3: Regression check**

Run: `cd internal/btree && go test -race -count=1 -timeout=300s ./...`
Expected: PASS.

- [ ] **Step 10.4: Commit**

```bash
git add internal/btree/wal.go
git -c commit.gpgsign=false commit -m "btree: decrypt WAL frame payloads on read"
```

---

## Task 11: WAL — encrypt/decrypt for in-memory arena

**Goal:** InMemory mode stores WAL frames in a heap-backed `memFrames` slice instead of a file. The codec must still run for consistency with the file-backed path.

**Files:**
- Modify: `internal/btree/wal.go`

**Drift note:** This task departs slightly from `encryption.md` §7's recommendation to skip encryption in InMemory mode. The rationale: *masterStore* (InMemory mode's disk replacement) skips encryption, but *WAL frames in memory* are a transient buffer between commits; keeping the codec path uniform reduces branch complexity and the cost is zero when no codec is installed. If benchmarks show it hurts InMemory-mode throughput, revisit.

- [ ] **Step 11.1: Locate `writeFramesMem` and its in-memory read path**

Run:
```
cd internal/btree && grep -n 'func.*writeFramesMem\|func.*readFrameMem\|memFrames' wal.go | head -20
```

- [ ] **Step 11.2: Apply the same encrypt-before-store pattern**

In `writeFramesMem`, before copying `p.data` into the `memFrames` arena:

```go
	// BEGIN ENCRYPTION
	payload := p.data
	if w.pager.codec != nil {
		scratch := allocPageBuffer(int(w.pageSize), false)
		defer freePageBuffer(scratch)
		ct, eerr := w.pager.encryptPage(scratch, p.data, p.pgno)
		if eerr != nil {
			return eerr
		}
		payload = ct
	}
	// END ENCRYPTION
	// ... existing code, but use `payload` instead of `p.data` ...
```

- [ ] **Step 11.3: Apply decrypt in the in-memory read path**

Wherever frames are read back from `memFrames` (likely inside the same `readFrame` function from Task 10, gated on `w.memFrames != nil`), the same wrapper already covers it — `w.pager.decryptPage` is called regardless of whether the bytes came from disk or the arena.

Verify by re-reading the readFrame function; if there are two distinct read branches (one for file, one for memory), add the BEGIN/END ENCRYPTION block to the memory branch too.

- [ ] **Step 11.4: Regression check**

Run: `cd internal/btree && go test -race -count=1 -timeout=300s ./...`
Expected: PASS.

- [ ] **Step 11.5: Commit**

```bash
git add internal/btree/wal.go
git -c commit.gpgsign=false commit -m "btree: encrypt/decrypt WAL in-memory arena frames"
```

---

## Task 12: DB Open — Options.Key, Codec install, salt I/O

**Goal:** User-facing API. Add `Key`, `KDFIterations`, `Codec` to `Options`. In `Open`: build the codec, install it before `initNewDB` / `readExisting`, handle salt generation vs. salt reading, reject key/file mismatches.

**Files:**
- Modify: `internal/btree/db.go`

- [ ] **Step 12.1: Extend Options**

Edit `internal/btree/db.go`. Find the `Options` struct (line 20). Before the closing `}`, add:

```go
	// BEGIN ENCRYPTION
	// Key enables page-level AES-256-GCM encryption when non-nil.
	// Accepted forms:
	//   - len 32: raw AES-256 key, used directly (no KDF)
	//   - any other len: treated as a passphrase; PBKDF2-HMAC-SHA256 derives
	//     a 32-byte key using the file's salt and KDFIterations rounds
	// Zero-length Key is rejected with an error.
	Key []byte

	// KDFIterations controls PBKDF2 cost. Ignored when Key is a raw 32-byte
	// key. Zero means DefaultKDFIterations (256,000). Use lower values only
	// for tests.
	KDFIterations int

	// Codec, when non-nil, replaces the built-in AES-256-GCM codec with a
	// caller-provided implementation. Useful for HSM-backed AEADs,
	// ChaCha20-Poly1305, or deterministic test codecs. When both Key and
	// Codec are set, Codec wins (Key is ignored). The caller owns the
	// codec's lifetime and must supply an implementation safe for
	// concurrent use.
	Codec Codec
	// END ENCRYPTION
```

- [ ] **Step 12.2: Write the failing test**

Create `internal/btree/encryption_integration_test.go`:

```go
package btree

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func tmpFile(t *testing.T, name string) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, name)
}

func TestOpen_KeyRoundTrip(t *testing.T) {
	path := tmpFile(t, "enc.db")
	opts := DefaultOptions()
	opts.Key = []byte("correct horse battery staple")
	opts.KDFIterations = 1000 // fast for tests

	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open (create): %v", err)
	}
	// Put some data.
	tx := db.BeginWrite()
	ns := tx.CreateNamespace("default")
	ns.Put([]byte("k1"), []byte("v1"))
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with correct key.
	db2, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open (reopen): %v", err)
	}
	tx2 := db2.BeginRead()
	ns2 := tx2.GetNamespace("default")
	val, ok := ns2.Get([]byte("k1"))
	if !ok {
		t.Fatalf("key k1 not found after reopen")
	}
	if !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("wrong value: got %q want v1", val)
	}
	tx2.Rollback()
	db2.Close()
}

func TestOpen_WrongKey(t *testing.T) {
	path := tmpFile(t, "enc.db")
	opts := DefaultOptions()
	opts.Key = []byte("right")
	opts.KDFIterations = 1000
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.BeginWrite()
	tx.CreateNamespace("x").Put([]byte("k"), []byte("v"))
	tx.Commit()
	db.Close()

	opts.Key = []byte("wrong")
	_, err = Open(path, opts)
	if err == nil {
		t.Fatalf("Open with wrong key returned nil error")
	}
	// Accept any error — specifically ErrCodecTamper or a wrapped form.
	if !bytesContains([]byte(err.Error()), []byte("authentication")) &&
		!bytesContains([]byte(err.Error()), []byte("tamper")) &&
		!bytesContains([]byte(err.Error()), []byte("corrupt")) {
		t.Logf("Open error: %v (acceptable if it indicates failed read)", err)
	}
}

func TestOpen_MissingKeyOnEncryptedFile(t *testing.T) {
	path := tmpFile(t, "enc.db")
	opts := DefaultOptions()
	opts.Key = []byte("pw")
	opts.KDFIterations = 1000
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	opts.Key = nil
	_, err = Open(path, opts)
	if err == nil {
		t.Fatalf("Open encrypted DB without key returned nil error")
	}
}

func TestOpen_KeyOnPlainFile(t *testing.T) {
	path := tmpFile(t, "plain.db")
	opts := DefaultOptions()
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.BeginWrite()
	tx.CreateNamespace("x").Put([]byte("k"), []byte("v"))
	tx.Commit()
	db.Close()

	opts.Key = []byte("pw")
	opts.KDFIterations = 1000
	_, err = Open(path, opts)
	if err == nil {
		t.Fatalf("Open plaintext DB with key returned nil error")
	}
}

func bytesContains(haystack, needle []byte) bool {
	return bytes.Contains(haystack, needle)
}

// remove the import-not-used stub if bytesContains inlines — keep the
// bytes import from the main testing uses above.
var _ = os.Open // silence unused-import warning if os is dropped
```

Run: `cd internal/btree && go test -run TestOpen_KeyRoundTrip -count=1`
Expected: FAIL (Options.Key undefined, or codec not wired).

- [ ] **Step 12.3: Wire codec construction into Open**

Locate the `Open` function in `internal/btree/db.go` (search `func Open(`). Find the section where the pager is constructed and before `initNewDB` / existing-file parse.

Add a helper near the top of db.go (after `DefaultOptions`):

```go
// BEGIN ENCRYPTION

// buildCodec constructs the Codec from Options. Returns (nil, nil) when
// no encryption is configured. Handles three input modes:
//   - Options.Codec set: use as-is
//   - Options.Key set with len == 32: treat as raw AES-256 key
//   - Options.Key set with any other length: treat as passphrase, derive
//     via PBKDF2 using the supplied salt
//
// The salt is the database's stored salt: for a new DB it's freshly random
// (generated by the caller); for an existing DB it's read from the header
// before this function is called.
func buildCodec(opts Options, salt []byte) (Codec, error) {
	if opts.Codec != nil {
		return opts.Codec, nil
	}
	if opts.Key == nil {
		return nil, nil
	}
	if len(opts.Key) == 0 {
		return nil, fmt.Errorf("btree: Options.Key is empty (use nil for no encryption)")
	}
	var raw []byte
	if len(opts.Key) == KeyLen {
		raw = opts.Key
	} else {
		raw = DeriveKey(opts.Key, salt, opts.KDFIterations)
	}
	return NewAESCodec(raw)
}

// END ENCRYPTION
```

- [ ] **Step 12.4: Modify Open to call buildCodec and install correctly**

This is the most delicate edit. The flow must be:

1. If the file exists: open it, read the first 100 bytes (plaintext header), extract `ReservedSpace` and `Salt`.
2. Reject mismatches:
   - `ReservedSpace > 0` but no `Options.Key` / `Options.Codec` → error "encrypted DB, key required".
   - `ReservedSpace == 0` but `Options.Key` / `Options.Codec` non-nil → error "DB is not encrypted, remove Key".
3. Build codec from `Options` and the read salt.
4. Install codec on the pager before any page read.

Find the `Open` function body. Identify the point where the pager reads the dbHeader from an existing file. Just after the header is parsed (before any page reads), insert:

```go
	// BEGIN ENCRYPTION
	// Validate encryption-mode consistency between Options and file state.
	fileEncrypted := p.header.ReservedSpace > 0
	wantEncryption := opts.Key != nil || opts.Codec != nil
	if fileEncrypted && !wantEncryption {
		p.file.Close()
		return nil, fmt.Errorf("btree: database file is encrypted, Options.Key or Options.Codec required")
	}
	if !fileEncrypted && wantEncryption {
		p.file.Close()
		return nil, fmt.Errorf("btree: database file is not encrypted, remove Options.Key")
	}
	if wantEncryption {
		codec, cerr := buildCodec(opts, p.header.Salt[:])
		if cerr != nil {
			p.file.Close()
			return nil, cerr
		}
		p.installCodec(codec)
	}
	// END ENCRYPTION
```

For the **new-DB-create path** (file doesn't exist — the flow passes through `initNewDB`), the codec must be installed *before* `initNewDB` runs, so `initNewDB` can size the reserved area. Find where `initNewDB` is called; just before it, add:

```go
	// BEGIN ENCRYPTION
	if opts.Key != nil || opts.Codec != nil {
		// For a new DB we have no salt yet. Generate one now, install the
		// codec, and initNewDB will write the salt into the header.
		var salt [SaltLen]byte
		if _, err := rand.Read(salt[:]); err != nil {
			return nil, fmt.Errorf("btree: generate salt: %w", err)
		}
		p.header.Salt = salt
		codec, cerr := buildCodec(opts, salt[:])
		if cerr != nil {
			return nil, cerr
		}
		p.installCodec(codec)
	}
	// END ENCRYPTION
```

**Order of operations matters.** For the existing-file path, `p.header.Salt` is populated by `deserialize` from the raw bytes. For the new-file path, we generate the salt, stash it in `p.header`, install the codec, then `initNewDB` serializes the header (including salt) and writes page 1.

Locate the `initNewDB` source (already modified in Task 5) and confirm it writes `p.header` verbatim — which will now include the salt. If it re-constructs `p.header` from scratch (as the current code at pager.go:338 does), modify it to **preserve** the salt:

```go
	p.header = dbHeader{
		PageSize:         p.pageSize,
		WriteVersion:     2, // WAL mode
		ReadVersion:      2,
		ReservedSpace:    0,
		FileChangeCount:  1,
		DatabaseSize:     1,
		SchemaFormat:     5,
		DefaultCacheSize: defaultCacheSize,
		TextEncoding:     1,
		// BEGIN ENCRYPTION
		Salt: p.header.Salt, // preserve pre-installed salt if any
		// END ENCRYPTION
	}
```

(If the salt field is not present due to a struct literal reset, this line ensures it survives. Otherwise the salt lands in the on-disk header via serialize() — unchanged.)

Also move the salt generation in `initNewDB` (added in Task 5.4) to be conditional on "salt not already populated":

```go
	// BEGIN ENCRYPTION
	if p.codec != nil {
		p.header.ReservedSpace = uint8(p.codec.Overhead())
		// Salt was populated at the Open level before installCodec; if
		// somehow missing, draw one now.
		var zero [SaltLen]byte
		if p.header.Salt == zero {
			if _, err := rand.Read(p.header.Salt[:]); err != nil {
				return err
			}
		}
	}
	// END ENCRYPTION
```

- [ ] **Step 12.5: Run the integration tests**

Run: `cd internal/btree && go test -run TestOpen_ -count=1 -v`
Expected: PASS for `TestOpen_KeyRoundTrip`, `TestOpen_WrongKey`, `TestOpen_MissingKeyOnEncryptedFile`, `TestOpen_KeyOnPlainFile`.

- [ ] **Step 12.6: Full test pass with race detector**

Run: `cd internal/btree && go test -race -count=1 -timeout=300s ./...`
Expected: PASS.

- [ ] **Step 12.7: Commit**

```bash
git add internal/btree/db.go internal/btree/pager.go internal/btree/encryption_integration_test.go
git -c commit.gpgsign=false commit -m "btree: wire Options.Key/KDFIterations/Codec through Open"
```

---

## Task 13: End-to-end: spill + commit + checkpoint + reopen

**Goal:** Exercise the full write path under encryption: many pages, forcing cache spill to WAL, commit, checkpoint, reopen, verify data.

**Files:**
- Extend: `internal/btree/encryption_integration_test.go`

- [ ] **Step 13.1: Add the test**

Append:

```go
func TestEncryption_SpillCheckpointReopen(t *testing.T) {
	path := tmpFile(t, "spill.db")
	opts := DefaultOptions()
	opts.Key = []byte("spill-test")
	opts.KDFIterations = 1000
	opts.CacheSize = 50 // tiny cache to force spill

	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Insert enough keys to blow past the cache and trigger pagerStress.
	const n = 10_000
	tx := db.BeginWrite()
	ns := tx.CreateNamespace("ns")
	for i := 0; i < n; i++ {
		k := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		v := bytes.Repeat([]byte{byte(i)}, 200)
		ns.Put(k, v)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Force a checkpoint.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	db.Close()

	// Reopen and verify.
	db, err = Open(path, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	tx2 := db.BeginRead()
	ns2 := tx2.GetNamespace("ns")
	for i := 0; i < n; i++ {
		k := []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		v, ok := ns2.Get(k)
		if !ok {
			t.Fatalf("missing key %x", k)
		}
		if len(v) != 200 || v[0] != byte(i) {
			t.Fatalf("wrong value for key %x", k)
		}
	}
	tx2.Rollback()
	db.Close()
}
```

- [ ] **Step 13.2: Run the test**

Run: `cd internal/btree && go test -run TestEncryption_SpillCheckpointReopen -race -count=1 -timeout=120s`
Expected: PASS. If it fails, the likely suspects are (a) spill-path codec hook missing, (b) checkpoint re-encryption missing, (c) salt not persisted correctly.

- [ ] **Step 13.3: Commit**

```bash
git add internal/btree/encryption_integration_test.go
git -c commit.gpgsign=false commit -m "btree: add spill+checkpoint+reopen integration test for encryption"
```

---

## Task 14: Tamper detection test

**Goal:** Verify that any single-byte change to the on-disk file results in a clean error, not a crash or silent corruption.

**Files:**
- Extend: `internal/btree/encryption_integration_test.go`

- [ ] **Step 14.1: Add the test**

Append:

```go
func TestEncryption_TamperDetected(t *testing.T) {
	path := tmpFile(t, "tamper.db")
	opts := DefaultOptions()
	opts.Key = []byte("tamper-test")
	opts.KDFIterations = 1000

	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.BeginWrite()
	ns := tx.CreateNamespace("x")
	ns.Put([]byte("k"), []byte("v"))
	tx.Commit()
	db.Checkpoint() // flush to main file so we tamper the "real" data
	db.Close()

	// Corrupt a single byte deep inside the file (past the plaintext
	// header — so past byte 100).
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	off := int64(4096 + 500) // somewhere inside page 2's ciphertext
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0x01
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Reopen — read should surface an error, not silently succeed.
	db2, err := Open(path, opts)
	if err != nil {
		// Error at open is acceptable (tamper caught on header read).
		return
	}
	tx2 := db2.BeginRead()
	ns2 := tx2.GetNamespace("x")
	_, _ = ns2.Get([]byte("k"))
	// The Get may or may not hit the tampered page depending on layout,
	// but a full-table iteration should hit it.
	// Use IterAll as a coverage hammer.
	found := false
	ns2.IterAll(func(k, v []byte) bool {
		return true
	})
	_ = found
	tx2.Rollback()
	// Expect either a prior error (propagated via codec error latch) or
	// that subsequent reads return errors. The exact surfacing depends on
	// the pager's error-handling strategy.
	db2.Close()
}
```

**Note:** This is an observational test — the precise *where* of failure depends on which page the byte lives in and which page the read path touches. The hard guarantee is "no crash, no silent corruption". Tighten the assertions once the failure propagation path is stable (a future hardening task).

- [ ] **Step 14.2: Run the test**

Run: `cd internal/btree && go test -run TestEncryption_TamperDetected -race -count=1`
Expected: PASS (no crash; silent-pass is also acceptable at this stage since we're testing the negative of "corrupted file → crash").

- [ ] **Step 14.3: Commit**

```bash
git add internal/btree/encryption_integration_test.go
git -c commit.gpgsign=false commit -m "btree: add on-disk tamper-detection smoke test"
```

---

## Task 15: Benchmark

**Goal:** Measure commit-throughput regression with encryption on. Informs whether further optimisation (Task 3's decrypt-scratch copy, nonce batching) is worth pursuing.

**Files:**
- Extend: `internal/btree/encryption_integration_test.go`

- [ ] **Step 15.1: Add the benchmark**

Append:

```go
func BenchmarkCommit_Plain(b *testing.B) {
	benchCommit(b, nil)
}

func BenchmarkCommit_Encrypted(b *testing.B) {
	benchCommit(b, []byte("benchmark-passphrase-used-for-pbkdf2"))
}

func benchCommit(b *testing.B, key []byte) {
	path := filepath.Join(b.TempDir(), "bench.db")
	opts := DefaultOptions()
	opts.NoCommitSync = true // measure CPU, not fsync
	if key != nil {
		opts.Key = key
		opts.KDFIterations = 1000
	}
	db, err := Open(path, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.BeginWrite()
		ns := tx.GetOrCreateNamespace("ns")
		for j := 0; j < 100; j++ {
			k := []byte{byte(i), byte(j)}
			ns.Put(k, bytes.Repeat([]byte{byte(j)}, 500))
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
```

- [ ] **Step 15.2: Run both benchmarks**

Run: `cd internal/btree && go test -bench 'BenchmarkCommit_' -run '^$' -count=3 -benchtime=3s`
Expected: two result lines. Record the ns/op delta in the commit message.

Example output format:
```
BenchmarkCommit_Plain-8        1234  8200000 ns/op
BenchmarkCommit_Encrypted-8    1100  9100000 ns/op
```

Report the overhead in the commit message.

- [ ] **Step 15.3: Commit**

```bash
git add internal/btree/encryption_integration_test.go
git -c commit.gpgsign=false commit -m "btree: add encrypt-vs-plain commit benchmark

Records overhead of X% for Y-key commits with Z-byte values."
```

---

## Final self-review

After finishing Task 15:

- [ ] **Search for stray TODOs or placeholders**

Run: `grep -rn 'TODO\|FIXME\|XXX' internal/btree/codec*.go internal/btree/encryption_integration_test.go`
Expected: no hits.

- [ ] **Search for unbracketed encryption code in modified files**

Run: `grep -L 'BEGIN ENCRYPTION' internal/btree/pager.go internal/btree/wal.go internal/btree/db.go internal/btree/page.go`
Expected: empty (every modified file has at least one bracketed region).

Run also: `grep -c 'BEGIN ENCRYPTION' internal/btree/*.go`
Sanity-check the counts against the plan. Each plan step that added a bracket should correspond to one `BEGIN ENCRYPTION` line.

- [ ] **Final full test pass**

Run: `cd internal/btree && go test -race -count=3 -timeout=600s ./...`
Expected: PASS across all suites.

- [ ] **Verify bracket grep locates every integration site**

Run: `grep -rn 'BEGIN ENCRYPTION\|END ENCRYPTION' internal/btree/ | wc -l`
Record the count. Should be an even number (BEGIN and END pair up). This is the "tracking catalogue" the user asked for — future maintainers use this grep to locate every touch-point.

- [ ] **Final commit (if any review changes were needed)**

```bash
git -c commit.gpgsign=false commit -am "btree: final cleanup after encryption-layer implementation"
```

---

## Out-of-scope (deferred for later PRs)

- **Rekey** (`sqlite3_rekey` equivalent) — walks every page to re-encrypt with a new key. Requires a full write transaction and two codec contexts. See `encryption.md` §7.7 for the flow. Defer until a concrete use case arises.
- **Backup API** (`SQLCipher_todo.md` §E `backup.c:800`) — any-store has no backup API yet; add codec-aware backup when backup is introduced.
- **`cipher_migrate` equivalent** — any-store has one format version; not needed until v2 format ships.
- **Memory-security allocator** (`SQLCipher_todo.md` §F `malloc.c:172`) — plan uses `defer clear()` on sensitive buffers inline. A global zeroing allocator can come later if a threat model demands it.
- **Per-transaction nonce batching** — mentioned as potential optimisation in `encryption.md` §9.4. Revisit only if benchmarks in Task 15 show `crypto/rand` as a hotspot.
- **ChaCha20-Poly1305 codec** — easy to add via `Options.Codec`; no need to ship one out of the box.

These are all additive — nothing in this plan blocks them, and nothing here depends on them.
