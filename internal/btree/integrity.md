# Page-Integrity Codec

## Overview

any-store ships a no-encryption page-checksum codec that stamps an
**XXH3-128** hash into the trailer of every page and verifies it on
read. The user-facing API (`anystore.IntegrityConfig`) is uniform
across two distinct on-disk mechanisms:

| Mode | Trailer (16 B / page) | Detection | Bypass-able |
|---|---|---|---|
| Plain (no codec) | — | — | — |
| Checksum (`cksumCodec`) | XXH3-128 of body | hash recompute + compare | yes (rewriting page recomputes) |
| AEAD (`aesCodec` / `chachaCodec`) | tag + nonce (+ pad) | Poly1305 / GCM auth tag | no (cryptographic) |

Conceptually mirrors SQLite's
[`cksumvfs`](https://sqlite.org/cksumvfs.html) extension, generalized
so encrypted DBs surface identical observability for free.

## Why XXH3-128 (not Fletcher)

cksumvfs uses an 8-byte Fletcher-style sum at byte offset
`pageSize - 8`. We diverge:

- **Trailer is 16 bytes** (matches the existing AEAD codecs' alignment
  invariant — `Codec.Overhead()` is a multiple of 16). The hash uses
  all 16 bytes (`XXH3-128`), no zero pad.
- **Algorithm is XXH3-128** (`github.com/zeebo/xxh3`):
  - ~30 GB/s SSE2/NEON throughput vs Fletcher's ~3-5 GB/s; effectively
    free at typical write rates.
  - Passes the SMHasher quality suite; 128-bit collision resistance is
    overkill for non-adversarial bit-rot detection.
  - SIMD-accelerated assembly for amd64/arm64; pure-Go fallback exists.

We are not bit-compatible with stock cksumvfs. The format-compat
question only matters for cross-runtime debugging via vanilla SQLite,
which is not a real workflow for any-store files.

## On-disk layout

For every page in the file:

```
[ ........... body ........... ][ XXH3-128 trailer (16 B) ]
^ start                          ^ pageSize - 16             ^ pageSize
```

`XXH3-128(body)` is stored as `Lo (uint64 LE) || Hi (uint64 LE)`.

### Page-1 header gap (limitation)

`encryptPageWithCodec` (`codec.go:98-127`) preserves the first 100
bytes of page 1 (the SQLite-format DB header) as a plaintext prefix
and only passes `src[100:]` to the codec. So:

- Pages 2+: hash range is `[0..pageSize-16]`.
- Page 1:   hash range is `[100..pageSize-16]`.

**The first 100 bytes of page 1 are NOT covered by the per-page
checksum.** SQLite-format invariants there (magic bytes, page-size
sanity, salt, etc.) are validated separately by `dbHeader.deserialize`
at open time, so corruption of those bytes surfaces as `ErrCorrupt`
rather than `ErrCodecTamper`. Bit-rot on the rest of the page is
covered.

## On-disk marker scheme

| `Salt` (16B at offset 72) | `ReservedSpace` (1B at offset 20) | Meaning |
|---|---|---|
| zero | 0 | plain unencrypted DB |
| zero | 16 | checksum-only DB (this codec) |
| non-zero | 32 / 48 | encrypted DB (AES-GCM / XChaCha-Poly1305) |

`ReservedSpace == 16 && Salt == 0` is a specific marker — *any* other
non-zero `ReservedSpace` value is left to higher-level code to
interpret. Our integrity-test helpers synthesize off-page cells via
custom reserved-space values, and the open-time check intentionally
ignores those.

## Activation & migration

The btree level still exposes `Options.Checksum` as a boolean knob (used
for tests and direct callers); the **anystore wrapper sets it
automatically** for every new non-encrypted DB. There is no opt-out at
the public API level.

File state is authoritative on reopen:

- New DB + `Options.Checksum = true`: trailer-bearing layout, codec
  installed.
- New DB + `Options.Checksum = false`: plain (only reachable via the
  btree API, not anystore).
- Existing checksum DB (file has `ReservedSpace=16`): codec
  auto-installed regardless of `Options.Checksum`.
- Existing plain DB (file has `ReservedSpace=0`): plain regardless of
  `Options.Checksum`. No upgrade-on-reopen.
- Combining `Checksum + Key` / `Checksum + Codec`: rejected at
  `buildCodec` (mutually exclusive with encryption).

This file-state-authoritative contract is what lets us turn checksums
on by default without breaking existing databases or requiring a
migration step. Cksumvfs's `VACUUM` workaround is unnecessary here —
plain DBs simply stay plain.

## Runtime API mapping

| SQLite cksumvfs | any-store equivalent |
|---|---|
| `PRAGMA file_control reserve_bytes 8; VACUUM` | (none — automatic on every new non-encrypted DB) |
| `PRAGMA checksum_verification = ON` (default) | default — corrupt reads fail with `ErrCodecTamper` |
| `PRAGMA checksum_verification = OFF` (forensic) | `Config.ContinueOnIntegrityError = true` (cksum mode only) |
| `SELECT count(*), verify_checksum(data) FROM sqlite_dbpage GROUP BY 2` | `(*DB).VerifyIntegrity(ctx) (IntegrityReport, error)` |
| `verify_checksum(BLOB)` SQL function | `anystore.VerifyPageChecksum([]byte) bool` |
| auto-enable on open with reserve_bytes==8 | automatic: `IntegrityMode()` reports state |
| `SQLITE_IOERR_DATA` on read | `ErrCodecTamper` returned + `OnIntegrityError` callback fires |

`ContinueOnIntegrityError` is honored only in cksum mode. AEAD mode
ignores it because disabling AEAD verification would return attacker-
controlled plaintext.

Direct btree callers can subscribe to per-page failures via
`btree.Options.OnIntegrityError` (a uniform hook fired by all codecs
that implement `OnErrorSink`).

## Why `SetVerifyOnRead(false)` is rejected on AEAD

cksumvfs's "verify off" is a forensic affordance — let me read the
page anyway and inspect it. Disabling AEAD verification would return
**attacker-controlled plaintext** (the cipher's Poly1305/GCM tag is
the only thing standing between the user and a chosen-ciphertext
attack). We refuse and return `ErrAEADIntegrityVerifyMandatory`.

## How the unified `OnError` works

Every `Codec` that implements page integrity also embeds
`onErrorField` (`codec.go`). Three codecs do today:

- `cksumCodec.Decrypt` fires on XXH3 mismatch (regardless of
  verify-on-read state — the toggle suppresses only the error return).
- `aesCodec.Decrypt` and `chachaCodec.Decrypt` fire on
  `cipher.AEAD.Open` failure before returning `ErrCodecTamper`.

`Options.OnIntegrityError` is wired onto whichever codec is installed
at `Open` time. The `VerifyIntegrity` sweep also fires the cksum
codec's hook on detected mismatches so subscribers see one signal
regardless of code path.

## Verification flow

`(*DB).VerifyIntegrity` walks pages 1..`DatabaseSize`:

1. Reports `IntegrityNone` with zero pages scanned for plain DBs.
2. For checksum mode: `pager.readRawPage(pgno)` (codec-bypass) +
   `xxh3.Hash128` recomputation + trailer compare. Detected
   mismatches accumulate into `Report.Errors` and fire the codec's
   `OnError`.
3. For AEAD mode: `pager.readRawPage(pgno)` +
   `decryptPageWithCodec(...)`. The codec's auth tag does the work;
   any tag failure yields a `SweepError{Kind: IntegrityAEADAuthFail}`
   (the AEAD codec's own `OnError` fires first as a side effect of
   `Decrypt`).

The sweep is read-only; uses an internal read transaction.

## Performance

XXH3-128 is essentially free: `BenchmarkInsert_Plain` vs
`BenchmarkInsert_Checksum` should differ by <5% (the cost is dominated
by syscalls, not hashing). AES-GCM is the existing baseline for
comparison; expect ~0.5-2× write-throughput penalty depending on cipher
and CPU AES-NI support.

## See also

- `docs/plans/2026-04-27-cksumvfs-port.md` — full implementation plan.
- `internal/btree/codec_cksum.go` — codec source.
- `internal/btree/integrity_sweep.go` — sweep implementation.
- `integrity.go` (anystore root) — public surface.
