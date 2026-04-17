# SQLCipher Integration TODO

Checklist of every place SQLCipher modified the SQLite source, to be used as an implementation map for adding page-level encryption to any-store's btree engine.

**Legend**
- `[ ]` integration point not yet handled
- File paths refer to the SQLCipher tree at `/home/dev/work/sqlcipher/src/`
- 5 files are 100% SQLCipher-authored (top of list — these are new files to create in any-store, not modifications)
- 47 per-site brackets + 2 raw `#ifdef SQLITE_HAS_CODEC` regions in `wal.c` = **49 integration points** in the SQLite source
- Counted by `grep -c "BEGIN SQLCIPHER" src/*`; source: SQLCipher 4.14.0, upstream SQLite 3.51.3

---

## A. Whole-file additions (5 new files to author)

These have no SQLite equivalent. In any-store they'd live under `internal/btree/` alongside `pager.go`, `wal.go`, etc.

- [ ] `sqlcipher.h` (entire file, ~165 lines) — Public codec interface: provider struct, encryption/decryption/HMAC/KDF enums, function prototypes for codec and key management.
- [ ] `sqlcipher.c` (entire file, ~3900 lines) — Codec context and lifetime, PBKDF2 key derivation, memory locking / zeroing, logging, PRAGMA `key`/`rekey`/`cipher_*` handlers, the `sqlite3Codec` callback.
- [ ] `crypto_openssl.c` (~417 lines) — OpenSSL provider: AES-256-CBC via EVP, HMAC-SHA via EVP_MAC, PBKDF2, `RAND_bytes`. **Skip in any-store** — replace with single Go stdlib implementation (`crypto/aes` + `crypto/cipher` + `golang.org/x/crypto/pbkdf2`).
- [ ] `crypto_cc.c` (~223 lines) — CommonCrypto provider (macOS/iOS). **Skip in any-store**.
- [ ] `crypto_libtomcrypt.c` (~383 lines) — LibTomCrypt provider. **Skip in any-store**.

---

## B. Pager integration (the hot path, 15+2 brackets)

The bulk of SQLCipher's work. Every site here has a direct any-store analogue in `internal/btree/pager.go` or `internal/btree/wal.go`.

### `pager.h`
- [ ] `pager.h:148` — Declare `sqlite3PagerAlignReserve` for aligning codec reserve bytes between two pagers (used by backup).
- [ ] `pager.h:249` — Declare `sqlcipherPagerCodec` — helper returning encrypted page bytes for WAL writes.

### `pager.c`
- [ ] `pager.c:412` — Define `CODEC1` / `CODEC2` macros: the canonical per-site hook for encrypt-on-write / decrypt-on-read. In any-store this becomes the single codec call at each I/O site.
- [ ] `pager.c:713` — Add codec function pointers (`xCodec`, `xCodecSizeChng`, `xCodecFree`, `pCodec`) to the `Pager` struct. any-store equivalent: add codec fields to the `pager` struct.
- [ ] `pager.c:834` — Block memory-mapped I/O when codec is active (mmap would bypass decryption).
- [ ] `pager.c:1077` — Block mmap mode selection when codec is installed.
- [ ] `pager.c:2287` — `pagerReportSize` — propagate page-size / reserve-byte changes back to the codec so it can recompute layout.
- [ ] `pager.c:2300` — `sqlite3PagerAlignReserve` implementation — syncs reserve bytes across two pagers (for backup).
- [ ] `pager.c:2367` — Set `jrnlEnc` flag deciding whether journal pages traverse the codec (true for main DB and disk subjournals, false for in-memory journals).
- [ ] `pager.c:2504` — Encode pages into the rollback journal on write; reverse encoding after write for restoration. **any-store has no rollback journal — skip.**
- [ ] `pager.c:2519` — Encode/decode during backup when `jrnlEnc` is true.
- [ ] `pager.c:2580` — Decode pages from journal before releasing the page cache entry (journal rollback path). **Skip — no journal.**
- [ ] `pager.c:4329` — Free codec context in pager destructor via `xCodecFree`.
- [ ] `pager.c:4676` — Encode pages written to the statement journal (unless it's in-memory). **Skip — no statement journal.**
- [ ] `pager.c:5776` — Assertion that codec is inactive during initial DB parse. Guard against premature codec installation.
- [ ] `pager.c:7253` — `sqlcipherPagerSetCodec` — public installer that attaches codec function pointers to a pager, resets pager state, reports reserve size. any-store equivalent: a `pager.installCodec(aead, keyMaterial)` method.
- [ ] `pager.c:7288` — `sqlcipherPagerCodec` — returns the encrypted page bytes; called by WAL code below.
- [ ] `pager.c:8000` — Helper exports `sqlite3pager_is_sj_pgno`, `sqlite3pager_error`, `sqlite3pager_reset` for codec integration (sub-journal skipping during rekey, error introspection, pager reset after migration).

### `wal.c`
- [ ] `wal.c:3963` — Call `sqlcipherPagerCodec(pPage)` before `walEncodeFrame` / `walWriteToLog` so the frame payload written to the WAL file is ciphertext.
- [ ] `wal.c:4152` — Same codec call during checkpoint reads when frames are being copied to the main DB file (rewrap-with-fresh-IV path).

---

## C. Public API & declarations

- [ ] `sqlite.h.in:6663` — Public API surface: `sqlite3_key`, `sqlite3_key_v2`, `sqlite3_rekey`, `sqlite3_rekey_v2`, `sqlite3_activate_see` prototypes. any-store equivalent: add `Options.Key` / `Options.KDFIterations` + optional `DB.Rekey()`.
- [ ] `sqliteInt.h:5007` — Internal declaration of `sqlite3CodecQueryParameters` (no-op when codec disabled).
- [ ] `main.c:3295` — Parse URI filename parameters (`hexkey`, `key`, `textkey`) to extract key bytes before DB init. **Skip in any-store** — use `Options.Key` instead.
- [ ] `main.c:3692` — Call `sqlite3CodecQueryParameters` on open to process URI keying params. **Skip — no URI params.**

---

## D. PRAGMA surface

- [ ] `pragma.c:443` — `extern` declaration of `sqlcipher_codec_pragma`.
- [ ] `pragma.c:519` — Intercept pragmas before standard handling; route `key`/`rekey`/`cipher_*` into `sqlcipher_codec_pragma`. any-store has no SQL → **skip**; expose knobs via `Options`.
- [ ] `pragma.c:2770` — Actual `PragTyp_KEY` dispatcher: parses `PRAGMA key` / `rekey` / `hexkey` / `hexrekey` / `textkey` / `textrekey`, calls `sqlite3_key_v2` or `sqlite3_rekey_v2`. **Skip — no SQL.**

---

## E. Attach / backup / vacuum transaction paths

- [ ] `attach.c:220` — ATTACH DATABASE `...` KEY `...` support: install codec on the attached db, handle empty-key (plaintext-attach), URI-parameter keys. any-store has no ATTACH → **skip**.
- [ ] `backup.c:155` — Validate source and destination codec states match (both encrypted or both plain) before backup begins.
- [ ] `backup.c:260` — Read source and destination reserve bytes before page-copy.
- [ ] `backup.c:280` — Forbid page-size changes during backup when a codec is active; align reserve bytes between source and destination.
- [ ] `backup.c:800` — `sqlite3PagerAlignReserve(destPager, srcPager)` call during `sqlite3_backup_init` so destination sees the same reserve layout.
- [ ] `vacuum.c:251` — If codec is active, force `db->nextPagesize = 0` to prevent VACUUM from changing page size. any-store has no VACUUM → **skip**.

---

## F. Infrastructure

- [ ] `global.c:171` — Enable `SQLITE_USE_URI` by default when codec is compiled in (so URI keying params work). any-store doesn't use URI opens → **skip**.
- [ ] `malloc.c:172` — Install SQLCipher's memory-zeroing allocator (`sqlcipher_init_memmethods`) so all freed allocations are wiped. any-store equivalent: `defer clear()` on sensitive buffers; optional zeroing allocator if a threat model asks for it.
- [ ] `util.c:1498` — `sqlite3HexToBlob` — parse `x'hhhh...'` hex literals into bytes. Needed by `hexkey` PRAGMA and raw-key codec init. any-store equivalent: `encoding/hex` stdlib.

---

## G. Shell (CLI) integration

any-store has no shell. All **skip** — listed for completeness only.

- [ ] `shell.c.in:1063` — Filter `PRAGMA key` / `ATTACH ... KEY` lines out of readline history.
- [ ] `shell.c.in:12386` — Print SQLCipher version via `sqlcipher_version()` in `-version` output.
- [ ] `shell.c.in:13760` — Include SQLCipher version in `libversion` output.
- [ ] `shell.c.in:13904` — Show SQLCipher version in the shell startup banner.

---

## H. TCL test harness integration

any-store's tests are in Go. All **skip** — listed for completeness.

- [ ] `tclsqlite.c:3268` — Declare `nKey`/`pKey` locals for the Tcl `codec` subcommand.
- [ ] `tclsqlite.c:3278` — Implement Tcl `rekey` subcommand via `sqlite3_rekey`.
- [ ] `tclsqlite.c:3857` — Add `-key CODECKEY` option to Tcl usage help.
- [ ] `tclsqlite.c:3897` — Declare `pKey`/`nKey` for command-line key option processing.
- [ ] `tclsqlite.c:3929` — Report "1" when codec is compiled in (Tcl feature query).
- [ ] `tclsqlite.c:3950` — Extract key bytes from the Tcl `-key` option.
- [ ] `tclsqlite.c:4038` — Call `sqlite3_key` on the newly-opened DB in the Tcl shell.

### Test-only

- [ ] `test_config.c:272` — Set Tcl `sqlite_options(has_codec)` variable based on compile flag.
- [ ] `test_thread.c:286` — Call `sqlite3_key` after opening DB in the threading test; abort on failure.

---

## Summary counts

| Category | Brackets | Relevance to any-store |
|---|---|---|
| A. Whole-file (new sqlcipher.{c,h} + 3 crypto backends) | 5 files | Partial — collapse to one Go file (`codec.go` + use stdlib crypto) |
| B. Pager + WAL integration | 17 sites | All relevant; 3 can skip (no rollback/statement journal) |
| C. Public API & declarations | 4 sites | 2 relevant (Go Options replaces URI/PRAGMA surface) |
| D. PRAGMA surface | 3 sites | All skip (no SQL) |
| E. Attach/backup/vacuum | 6 sites | Only backup is partially relevant; no ATTACH, no VACUUM |
| F. Infrastructure | 3 sites | 1 relevant (mem zeroing); hex parsing is stdlib |
| G. Shell | 4 sites | All skip |
| H. TCL / tests | 9 sites | All skip |
| **Total** | **51** | **~14 any-store-relevant integration points** |

**Realistic any-store scope: ~14 integration sites**, the rest are either (a) SQL/CLI/TCL-specific, (b) journal-mode code paths any-store doesn't have, or (c) merged into stdlib Go.

---

## Suggested implementation order

If you want to use this as a work plan, tackle in this order:

1. **Codec skeleton** — create `internal/btree/codec.go`: AEAD instance, key derivation, nonce handling, tag verification. (Replaces items from §A.)
2. **Options & Open** — add `Options.Key` / `KDFIterations`; derive key at `Open()` time. (Replaces §C `main.c` sites.)
3. **Pager codec field + installCodec** — `pager.c:713`, `pager.c:7253` equivalents.
4. **Reserve size wiring** — set `dbHeader.ReservedSpace` at new-DB init; verify `usableSize()` flows through cell math. (No SQLCipher equivalent — any-store-specific.)
5. **CODEC1/CODEC2 equivalents** at the three file-read sites and the one file-write site in `pager.go`. (Maps to `pager.c:412` + scattered call sites.)
6. **WAL hook** — `wal.go` equivalent of `wal.c:3963, 4152`. Encrypt before `file.WriteAt`, decrypt after `file.ReadAt`, covers spill + commit + checkpoint.
7. **Checksum vs tag decision** — move `walChecksum` call to post-encryption (§4 of spec-fit.md).
8. **Page 1 layout** — reserve 16 bytes inside the 100-byte dbHeader for the salt; encrypt from offset 100 onward (§3 of spec-fit.md).
9. **Backup page-size guard** — if/when backup API is added; `backup.c:280` equivalent.
10. **Tests** — property-based tamper test (fuzz the decrypt path); commit/spill/savepoint matrix; WAL checkpoint round-trip; wrong-key open returns a clean error, not garbage.

Items 1–8 are the MVP. Items 9–10 are hardening.
