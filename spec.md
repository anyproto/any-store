# SQLCipher Specification

Research notes on SQLCipher 4.14.0 (based on upstream SQLite 3.51.3). Compiled from reading the source at `/home/dev/work/sqlcipher`. File citations use the form `file.c:N` referring to paths under `src/` unless otherwise stated.

---

## 1. What SQLCipher Is

SQLCipher is a *fork* of SQLite (not a loadable extension) that adds transparent, page-level authenticated encryption to the SQLite file format. It preserves the SQLite public C API and SQL surface, adds a handful of C APIs (`sqlite3_key*`, `sqlite3_rekey*`) and a large number of `PRAGMA cipher_*` knobs, and keeps modifications to the vanilla SQLite source tree minimal by concentrating almost all logic in two new files: `sqlcipher.c` (~3900 lines) and `sqlcipher.h` (165 lines), plus one file per crypto backend.

### Core design properties

- **Every byte is encrypted** on pages 2..N. Page 1's first 16 bytes are the PBKDF2 salt (plaintext by necessity) — optionally, an additional `cipher_plaintext_header_size` bytes may be kept in the clear.
- **AES-256-CBC** is the only block cipher. Block size 16, key size 32, IV size 16.
- **Per-page random IV.** Re-writing the same page produces different ciphertext.
- **HMAC-SHA512** (default) over ciphertext + IV + page-number is stored alongside each page and verified on read. Prevents tampering and page reordering.
- **PBKDF2-HMAC-SHA512** derives the encryption key from the user passphrase in 256,000 iterations. The HMAC key is then derived from the encryption key in 2 more iterations with a salt-mask-XOR.
- **Pluggable crypto** via a function-pointer `sqlcipher_provider` struct. Default backend is OpenSSL; CommonCrypto (Apple) and LibTomCrypt are also implemented.
- **Zero-configuration for callers**: `PRAGMA key = '...'` then SQL as usual. Without a key, SQLCipher behaves like vanilla SQLite.
- **Format compatibility within major versions.** Major version bumps change defaults; `cipher_compatibility` / `cipher_migrate` pragmas handle legacy files.

### Repository file layout

```
src/sqlcipher.h              public header, constants, provider struct (165 lines)
src/sqlcipher.c              codec context, key derivation, pragma handlers, codec callback (~3900 lines)
src/crypto_openssl.c         OpenSSL backend
src/crypto_cc.c              Apple CommonCrypto backend
src/crypto_libtomcrypt.c     LibTomCrypt backend
src/pager.c                  vanilla SQLite pager with SQLITE_HAS_CODEC hooks added
src/wal.c                    vanilla WAL with codec calls added for WAL frame encryption
src/attach.c                 ATTACH syntax extended with optional KEY clause
src/backup.c                 backup API, page-size compatibility checks
src/vacuum.c                 VACUUM constraints for encrypted DBs
src/pragma.c                 dispatch for PRAGMA key / rekey / hexkey / hexrekey
test/sqlcipher-*.test        9 Tcl test files covering crypto, pragmas, compat, rekey, etc.
```

`SQLITE_HAS_CODEC` is the master compile-time gate.

---

## 2. Build Requirements

From `sqlcipher.c:66-76` (hard `#error` checks) and `README.md`:

| Define | Required value | Reason |
|---|---|---|
| `SQLITE_HAS_CODEC` | any | Master gate for all codec integration hooks |
| `SQLITE_EXTRA_INIT` | `sqlcipher_extra_init` | Runs once per process to init providers, mutexes, memory methods |
| `SQLITE_EXTRA_SHUTDOWN` | `sqlcipher_extra_shutdown` | Tears down providers and zeroes sensitive memory |
| `SQLITE_TEMP_STORE` | `2` or `3` | Keeps temp tables in RAM — otherwise plaintext temp files would leak |
| `SQLITE_THREADSAFE` | `1` or `2` | Required because SQLCipher's mutexes assume serialized/multi-thread |
| `-D<BACKEND>` | `SQLCIPHER_CRYPTO_OPENSSL` (default), `…CC`, `…LIBTOMCRYPT`, or `…CUSTOM` | Picks the crypto backend at compile time |

One provider must be linkable at build time. Typical build:

```
./configure --with-tempstore=yes CFLAGS='-DSQLITE_HAS_CODEC \
    -DSQLITE_EXTRA_INIT=sqlcipher_extra_init \
    -DSQLITE_EXTRA_SHUTDOWN=sqlcipher_extra_shutdown' \
    LDFLAGS='-lcrypto'
```

`SQLCIPHER_TEST` additionally enables fault-injection pragmas.

---

## 3. On-Disk Page Format

### 3.1 Page layout (default settings: 4 KiB page, HMAC-SHA512 on)

```
          ┌─────────────────────────────────────────────────────────────────┐
Page 1:   │ KDF salt (16 B) │ ciphertext (4000 B)     │ IV (16 B) │ HMAC+pad (64 B) │
          └─────────────────────────────────────────────────────────────────┘
           offset 0         16                         4016         4032 ... 4095

          ┌─────────────────────────────────────────────────────────────────┐
Pages 2..N│ ciphertext (4016 B)                       │ IV (16 B) │ HMAC+pad (64 B) │
          └─────────────────────────────────────────────────────────────────┘
           offset 0                                    4016         4032 ... 4095
```

Everything after byte 16 of page 1, and all bytes of every other page, are AES-256-CBC ciphertext. The **per-page random IV** and the **HMAC tag** live in the SQLite "reserved area" at the end of each page — invisible to the SQLite B-tree code because `reserve_sz` is set at codec-attach time and SQLite honours it as usable-page-size reduction.

### 3.2 Reserve-area calculation

Defined at `sqlcipher.c:1128-1146`.

```
base_reserve = iv_sz                                    // 16
if (CIPHER_FLAG_HMAC):
    reserve = iv_sz + hmac_sz                           // 16 + 64 = 80
else:
    reserve = iv_sz                                     // 16
reserve = ceil(reserve / block_sz) * block_sz          // round up to cipher block
```

Defaults:
- AES block size = 16
- HMAC-SHA512 output = 64 → reserve = 80 (already multiple of 16, no padding added)
- HMAC-SHA256 output = 32 → reserve = ceil(48/16)·16 = 48
- HMAC-SHA1 output = 20 → reserve = ceil(36/16)·16 = 48 (12 B padding)

Usable page data = `page_sz − reserve_sz`. With default 4096/80 this is **4016 bytes** (or 4000 for page 1 with default plaintext header).

### 3.3 Page 1: the salt

- The **first 16 bytes of page 1** of the file always hold the 16-byte KDF salt — the same salt PBKDF2 uses to derive the encryption key. This is plaintext out of necessity: you cannot read the key without first reading the salt.
- On a *new* DB, the salt is generated by the provider's RNG (`sqlcipher.c:1392-1410`).
- On *open*, SQLCipher reads the first 16 bytes directly from the file via `sqlite3OsRead()` before any decryption (`sqlcipher.c:1401`).
- When page 1 is re-encrypted, those 16 bytes (the salt) are rewritten back into the output at offset 0 before the AES call (`sqlcipher.c:3304-3312`).
- When page 1 is *decrypted*, SQLCipher splices in the constant `"SQLite format 3\0"` (`SQLITE_FILE_HEADER`) at offset 0 of the output so SQLite's page-1 parser sees the magic it expects (`sqlcipher.c:3270-3271`).

### 3.4 The plaintext-header option

`PRAGMA cipher_plaintext_header_size = N` keeps the first N bytes of page 1 (0–32) in the clear. The same N bytes from the original page are written through to the encrypted image verbatim.

- Use case: letting file-type detection tools (`file(1)`, forensics, backup software) recognise an encrypted DB as a SQLite DB.
- When `N > 0`, the first N bytes must be preserved *including* the salt region — i.e. the caller is responsible for ensuring the plaintext bytes are a valid SQLite header.
- If `N == page_sz − reserve_sz` (entire page 1 plaintext), SQLCipher enters a forgiving "recovery" mode: decryption errors on page 1 don't set the permanent error state (`sqlcipher.c:3287-3288`). Used by salvage tools.

### 3.5 IV and HMAC placement per page

- `IV` occupies `page_sz − reserve_sz` through `page_sz − reserve_sz + iv_sz − 1` (e.g. bytes 4016–4031 for default layout).
- `HMAC` starts at `page_sz − reserve_sz + iv_sz` (e.g. byte 4032) and occupies `hmac_sz` bytes (64 for SHA-512). Any remaining reserve bytes are random padding filled at encrypt time (`sqlcipher.c:1671-1673`, which fills the whole reserve with random first, then HMAC overwrites its portion).

---

## 4. Cryptography

### 4.1 Cipher

- **AES-256-CBC**, no padding. SQLCipher ensures every input is a multiple of the AES block size (page sizes are powers of two ≥ 512 and `reserve_sz` is always rounded to a multiple of the block size), so CBC runs in "no-pad" mode and produces ciphertext of exactly the same length as plaintext.
- **Key: 32 bytes.** Queried from the provider via `get_key_sz()`, not hardcoded.
- **IV: 16 bytes**, fresh for every page write. Generated by the provider's CSPRNG (`sqlcipher.c:1671-1673`). Stored alongside each page.

### 4.2 HMAC

Three algorithms are supported and selectable via `PRAGMA cipher_hmac_algorithm`: HMAC-SHA1 (0), HMAC-SHA256 (1), HMAC-SHA512 (2, default).

HMAC is computed over the tuple `(ciphertext ‖ IV ‖ page_number_as_u32)` at `sqlcipher.c:1595-1626`. The page-number bytes are fed as a separate trailing buffer. Endianness is controlled by the `CIPHER_FLAG_LE_PGNO` / `CIPHER_FLAG_BE_PGNO` flags; the default is little-endian.

```c
provider->hmac(provider_ctx,
               hmac_algorithm,
               c_ctx->hmac_key, key_sz,
               in = ciphertext,  in_sz = size,
               in2 = ciphertext+size /*= IV*/, in2_sz = iv_sz,
               /* page number passed separately via length trick */
               out);
```

(Actual signature uses a two-buffer HMAC API where the second buffer is used for the page number. See `sqlcipher_page_hmac` for exact wiring.)

Coverage:
- Tamper-detect the ciphertext.
- Tamper-detect the IV (would otherwise allow an attacker to flip every bit of the first CBC block).
- Page-number binding prevents page-shuffling attacks where an attacker swaps two encrypted pages.

### 4.3 PBKDF2 for the encryption key

- **Algorithm** (selectable): PBKDF2-HMAC-SHA1 / SHA256 / **SHA512 (default)**.
- **Iterations** (default 256,000). Configurable via `PRAGMA kdf_iter`. Different compatibility modes use different defaults (see §8.3).
- **Salt**: the 16 bytes read from page 1 offset 0.
- **Output length**: 32 bytes → `ctx->read_ctx->key` / `ctx->write_ctx->key`.

Implemented as a single call to the provider's `kdf` function — each backend delegates to its native PBKDF2 (`PKCS5_PBKDF2_HMAC` in OpenSSL, `CCKeyDerivationPBKDF` in CommonCrypto, `pkcs_5_alg2` in LibTomCrypt). No manual loop.

### 4.4 HMAC key derivation

The HMAC key is **not** the encryption key. At `sqlcipher.c:1813-1835`:

1. Copy KDF salt to a second buffer `hmac_kdf_salt`.
2. XOR every byte with `HMAC_SALT_MASK = 0x3a` — produces a distinct salt deterministically.
3. Run PBKDF2 again with:
   - Password = **the 32-byte derived encryption key** (not the passphrase).
   - Salt = the XOR-masked salt.
   - Iterations = `fast_kdf_iter` (default 2).
   - Output = 32 bytes → `c_ctx->hmac_key`.

Deriving from the already-derived key with a different salt and 2 iterations is essentially a fast KDF; it ensures the HMAC key differs from the encryption key without re-running the expensive PBKDF2.

### 4.5 Random numbers

Each provider exposes `random(ctx, buf, len)` and `add_random(ctx, buf, len)`. All sources of randomness (salts, IVs) go through this interface. The OpenSSL backend wraps `RAND_bytes` in a mutex (`SQLCIPHER_MUTEX_PROVIDER_RAND`) by default to avoid crashes on OpenSSL versions that lacked internal locking. Disableable via `SQLCIPHER_OPENSSL_NO_MUTEX_RAND`.

---

## 5. Crypto Provider Interface

Defined at `sqlcipher.h:59-91`:

```c
typedef struct sqlcipher_provider sqlcipher_provider;
struct sqlcipher_provider {
  int         (*init)(void);
  void        (*shutdown)(void);
  const char* (*get_provider_name)(void *ctx);
  int         (*add_random)(void *ctx, const void *buffer, int length);
  int         (*random)(void *ctx, void *buffer, int length);
  int         (*hmac)(void *ctx, int algorithm,
                      const unsigned char *hmac_key, int key_sz,
                      const unsigned char *in,  int in_sz,
                      const unsigned char *in2, int in2_sz,
                      unsigned char *out);
  int         (*kdf)(void *ctx, int algorithm,
                     const unsigned char *pass, int pass_sz,
                     const unsigned char *salt, int salt_sz,
                     int workfactor,
                     int key_sz, unsigned char *key);
  int         (*cipher)(void *ctx, int mode,
                        const unsigned char *key, int key_sz,
                        const unsigned char *iv,
                        const unsigned char *in, int in_sz,
                        unsigned char *out);
  const char* (*get_cipher)(void *ctx);
  int         (*get_key_sz)(void *ctx);
  int         (*get_iv_sz)(void *ctx);
  int         (*get_block_sz)(void *ctx);
  int         (*get_hmac_sz)(void *ctx, int algorithm);
  int         (*ctx_init)(void **ctx);
  int         (*ctx_free)(void **ctx);
  int         (*fips_status)(void *ctx);
  const char* (*get_provider_version)(void *ctx);
  sqlcipher_provider *next;
};
```

### 5.1 Semantics per function pointer

- `init` / `shutdown` — one-time process-wide hooks. Both are `NULL` in the three shipped backends (the backends use reference counting inside `ctx_init`/`ctx_free` instead).
- `ctx_init(void**)` / `ctx_free(void**)` — per-database-connection allocation and reference counting. Called from `sqlcipher_codec_ctx_init` (`sqlcipher.c:1491`) and on close.
- `random` — must be cryptographically secure; fills `length` bytes. Returns `SQLITE_OK`/`SQLITE_ERROR`.
- `add_random` — opportunistic entropy seeding; mostly a no-op on platforms without an entropy-pool API (e.g. CommonCrypto).
- `kdf` — PBKDF2 with selectable hash. `workfactor` = iterations.
- `hmac` — two-buffer HMAC with selectable algorithm. `in2` may be `NULL`. Output buffer is pre-sized by caller to `get_hmac_sz(algorithm)`.
- `cipher` — AES-256-CBC encrypt or decrypt, no padding. `mode` ∈ {`SQLCIPHER_DECRYPT`=0, `SQLCIPHER_ENCRYPT`=1}. In-size always equals out-size.
- `get_cipher` — constant string "aes-256-cbc".
- `get_key_sz` — 32; `get_iv_sz` — 16; `get_block_sz` — 16.
- `get_hmac_sz(algo)` — 20 / 32 / 64 for SHA-1 / SHA-256 / SHA-512.
- `fips_status` — currently always returns 0 across backends.
- `get_provider_name`, `get_provider_version` — human-readable strings shown by `PRAGMA cipher_provider` / `cipher_provider_version`.
- `next` — singly linked list for provider chain (historical).

### 5.2 Selection and registration

Compile-time default (`sqlcipher.c:86-91`):

```c
#if !defined (SQLCIPHER_CRYPTO_CC) \
   && !defined (SQLCIPHER_CRYPTO_LIBTOMCRYPT) \
   && !defined (SQLCIPHER_CRYPTO_OPENSSL) \
   && !defined (SQLCIPHER_CRYPTO_CUSTOM)
#define SQLCIPHER_CRYPTO_OPENSSL
#endif
```

During `sqlcipher_extra_init` (`sqlcipher.c:503-527`) a `sqlcipher_provider` struct is allocated and filled by the selected backend's `sqlcipher_<name>_setup(p)` function, then handed to `sqlcipher_register_provider(p)`. The latter stores it as `default_provider` under `SQLCIPHER_MUTEX_PROVIDER`.

Runtime provider swapping is possible by calling `sqlcipher_register_provider` with a new struct before any codec is attached.

`sqlcipher_get_provider()` returns the current default; every new `codec_ctx` captures a pointer in `ctx->provider`.

### 5.3 Backends

#### 5.3.1 OpenSSL (`crypto_openssl.c`)

- Cipher constant: `OPENSSL_CIPHER = EVP_aes_256_cbc()`.
- Per-page encrypt/decrypt: `EVP_CIPHER_CTX_new` → `EVP_CipherInit_ex(ctx, cipher, 0, 0, 0, mode)` → `EVP_CIPHER_CTX_set_padding(ctx, 0)` → `EVP_CipherInit_ex(ctx, 0, 0, key, iv, mode)` → `EVP_CipherUpdate` → `EVP_CipherFinal_ex` → `EVP_CIPHER_CTX_free`. A new context is allocated per page.
- HMAC uses OpenSSL 3.x `EVP_MAC` API: `EVP_MAC_fetch("HMAC")` → `EVP_MAC_CTX_new` → `EVP_MAC_init(params = {digest = "sha512"|"sha256"|"sha1", key})` → `EVP_MAC_update(ctx, in, in_sz)` → optional second update → `EVP_MAC_final` twice (first to probe length, second to write).
- PBKDF2 calls `PKCS5_PBKDF2_HMAC(pass, pass_sz, salt, salt_sz, iter, EVP_sha{1,256,512}(), key_sz, key)`.
- `RAND_bytes` for CSPRNG, optionally wrapped in a mutex. `RAND_add` for `add_random`.
- Reference-counted activate/deactivate with `SQLCIPHER_MUTEX_PROVIDER_ACTIVATE`.
- Conditional for OpenSSL 1.0 vs 1.1+: `ERR_load_crypto_strings()` only for 1.0; `OpenSSL_version()` vs `OPENSSL_VERSION_TEXT`.

#### 5.3.2 CommonCrypto (`crypto_cc.c`)

- Cipher via `CCCryptorCreate(op, kCCAlgorithmAES128, 0, key, kCCKeySizeAES256, iv, &cryptor)` — the "128" refers to the *block* size. Options flag `0` ⇒ no padding, CBC mode.
- HMAC via `CCHmacInit / Update / Update / Final` using stack-allocated `CCHmacContext`.
- PBKDF2 via `CCKeyDerivationPBKDF(kCCPBKDF2, pass, sz, salt, salt_sz, kCCPRFHmacAlgSHA{1,256,512}, iter, key, key_sz)`.
- RNG via `SecRandomCopyBytes(kSecRandomDefault, …)`; `add_random` is a no-op.
- Version string derived from the `com.apple.security` / `com.apple.Security` bundle. No init/shutdown (CommonCrypto is always available).

#### 5.3.3 LibTomCrypt (`crypto_libtomcrypt.c`)

- Uses a global **Fortuna** PRNG seeded on first activation from `rng_get_bytes()` (32 bytes).
- Must register algorithms on activate: `register_cipher(&aes_desc)`, `register_hash(&sha512_desc/sha256_desc/sha1_desc)`, `register_prng(&fortuna_desc)` (`crypto_libtomcrypt.c:84-89`).
- Cipher: `cbc_start(cipher_idx, iv, key, key_sz, 0, cbc)` → `cbc_encrypt` / `cbc_decrypt` → `cbc_done`. Last argument `0` ⇒ no padding.
- HMAC via `hmac_init / hmac_process / hmac_done`.
- PBKDF2 via `pkcs_5_alg2(pass, pass_sz, salt, salt_sz, iter, hash_idx, key, &outlen)`.
- `random` uses `fortuna_read`, mutex-protected.
- HMAC and CBC contexts allocated on SQLCipher's private heap (not LibTomCrypt's default allocator).
- Reference-counted; on zero-refcount shutdown it calls `fortuna_done` and zeroes the state.

#### 5.3.4 NSS

README mentions NSS support, but **no `crypto_nss.c` is present in this tree** and `sqlcipher.c` has no `SQLCIPHER_CRYPTO_NSS` branch. Either not yet merged or maintained out-of-tree.

### 5.4 Error propagation

Providers return `SQLITE_OK` or `SQLITE_ERROR`. Errors bubble up to `sqlcipher_page_cipher` (`sqlcipher.c:1636`), which:
- zeroes the output buffer on any failure,
- logs via `sqlcipher_log`,
- sets `ctx->error` to a persistent error code (`sqlcipher.c:3290, 3326`), which latches until the connection is closed — unless the `plaintext_header_sz` recovery-mode path suppresses it.

---

## 6. Integration with the SQLite Core

SQLCipher makes small but precise edits to several vanilla SQLite files, all guarded by `#ifdef SQLITE_HAS_CODEC`.

### 6.1 The codec callback

The pager accepts three function pointers per database (`pager.c:715-719`): `xCodec`, `xCodecSizeChng`, `xCodecFree`, plus an opaque `pCodec` argument. Installed by `sqlcipherPagerSetCodec(pager, xCodec, xCodecSizeChng, xCodecFree, pCodec)` at `pager.c:7261`.

SQLCipher's callback is `sqlite3Codec` in `sqlcipher.c:3223`:

```c
void* sqlite3Codec(void *iCtx, void *pData, Pgno pgno, int mode);
```

Modes (matched against `int mode`):
- `3` = `CODEC_READ_OP` — decrypt `pData` in place, return `pData`.
- `6` = WAL frame encoding — encrypt into the codec's internal `ctx->buffer`, return it.
- `7` = `CODEC_JOURNAL_OP` — encrypt from journal to main DB (used during rekey), return buffer.
- `CODEC_WRITE_OP` = encrypt for main DB write path.

Internally the callback selects `read_ctx` or `write_ctx` based on mode, copies the 16-byte salt/plaintext header to the output, calls `sqlcipher_page_cipher`, and returns either `pData` (in-place decrypt) or `ctx->buffer` (encrypt path).

### 6.2 Where the pager invokes the codec

The pager exposes two macros (`pager.c:414-422`):

```c
#define CODEC1(P,D,N,X,E) \
  if(P->xCodec && P->xCodec(P->pCodec, D, N, X)==0){ E; }

#define CODEC2(P,D,N,X,E,O) \
  if(P->xCodec==0){ O=(char*)D; } \
  else if((O=(char*)(P->xCodec(P->pCodec, D, N, X)))==0){ E; }
```

Main invocation sites:

| Location | Macro | Mode | Purpose |
|---|---|---|---|
| `pager.c:3166` | `CODEC1` | 3 | Decrypt a page after reading from disk |
| `pager.c:2506-2509` | `CODEC2` → `CODEC1` | 7, 3 | Roll back from journal: encrypt journal bytes for DB write, then decrypt to restore in-memory state |
| `pager.c:2521-2524` | `CODEC1` → `CODEC2` | 3, 7 | Backup-update variant of journal rollback |
| `pager.c:7288` | `sqlcipherPagerCodec(pg)` → `CODEC2` | 6 | Encrypt a page for WAL write |

### 6.3 Codec attach flow

`sqlite3_key_v2(db, "main", key, n)` (`sqlcipher.c:3528-3531`) →
`sqlcipher_find_db_index(db, zDb)` →
`sqlcipherCodecAttach(db, index, pKey, nKey)` (`sqlcipher.c:3376-3497`).

`sqlcipherCodecAttach`:
1. Check for existing codec. If the key has already been used (`CIPHER_FLAG_KEY_USED`), reject.
2. Call `sqlcipher_codec_ctx_init(&ctx, pDb, pPager, pKey, nKey)` (`sqlcipher.c:1456`) — allocates the `codec_ctx`, its two `cipher_ctx`s, reads the KDF salt (or generates one for a new DB), and wires `ctx->provider = sqlcipher_get_provider()`.
3. Call `sqlcipherPagerSetCodec(pager, sqlite3Codec, sqlite3CodecSizeChng, sqlite3FreeCodecArg, ctx)`.
4. Adjust the B-tree page size and enable secure delete.

The key is **not actually derived at attach time**. Derivation is deferred until the first codec invocation, triggered from `sqlite3Codec` / `sqlcipher_codec_key_derive` — this allows pragmas like `cipher_page_size`, `cipher_use_hmac`, `cipher_compatibility` to be set after `PRAGMA key` but before any page I/O.

`sqlite3_activate_see(const char*)` is a no-op stub (`sqlcipher.c:3519-3521`) — legacy compatibility with the proprietary SEE build.

### 6.4 WAL

WAL frames are encrypted. `wal.c:3963-3972`:

```c
#if defined(SQLITE_HAS_CODEC)
  if((pData = sqlcipherPagerCodec(pPage)) == 0) return SQLITE_NOMEM_BKPT;
#else
  pData = pPage->pData;
#endif
walEncodeFrame(p->pWal, pPage->pgno, nTruncate, pData, aFrame);
rc = walWriteToLog(p, aFrame, sizeof(aFrame), iOffset);
rc = walWriteToLog(p, pData, p->szPage, iOffset+sizeof(aFrame));
```

`sqlcipherPagerCodec()` calls the codec in mode `6` (encrypt to buffer) and returns the encrypted bytes. The frame header is written plaintext, but the frame *payload* is ciphertext. On checkpoint (`wal.c:4152-4157`) pages read from WAL are fed through the encrypt path before being written to the main DB file — same mode.

There is no `SQLITE_OMIT_WAL`.

### 6.5 ATTACH with KEY

Syntax: `ATTACH DATABASE 'file' AS name KEY 'passphrase'`. Parser extension at `attach.c:486-499`. Handler at `attach.c:234-248`:

```c
nKey = sqlite3_value_bytes(argv[2]);
zKey = (char*)sqlite3_value_blob(argv[2]);
/* empty key = attach plaintext DB to an encrypted session */
if(nKey && zKey) {
  rc = sqlcipherCodecAttach(db, db->nDb-1, zKey, nKey);
}
```

Each attached DB has its own `codec_ctx`; different attached DBs may have different keys (or no key at all).

### 6.6 VACUUM

Defended at `vacuum.c:250-263`: if a page-size change is requested (`db->nextPagesize != 0`) and the DB is encrypted, the page-size change is suppressed. VACUUM otherwise preserves encryption transparently — it writes pages through the normal pager path, which encrypts them.

`VACUUM INTO` uses the same check.

### 6.7 Backup

`backup.c:281-286` — if source and destination have different page sizes and the destination is encrypted, `SQLITE_READONLY` is returned. `backup.c:801-804` — `sqlite3PagerAlignReserve` is called at init so both pagers agree on reserve space. Pages flow through both pagers' codecs transparently (decrypt on source read, encrypt on destination write).

### 6.8 `sqlcipher_export()`

A SQL function (`sqlcipher.c:3750-3891`), used as:

```sql
ATTACH DATABASE 'encrypted.db' AS encrypted KEY 'key';
SELECT sqlcipher_export('encrypted');  -- copy current main → encrypted
```

Walks the source schema in several passes:
1. Copy `CREATE TABLE` DDL (excluding shadow tables with `rootpage = 0`) and execute on target.
2. Copy `CREATE INDEX` DDL (both regular and UNIQUE).
3. For each user table, `INSERT INTO target.t SELECT * FROM source.t`.
4. Copy the `sqlite_sequence` table (AUTOINCREMENT state).
5. Copy `type='view' OR type='trigger' OR (type='table' AND rootpage=0)` rows directly into `target.sqlite_schema` — this brings over views, triggers, and virtual-table shadow rows.

This is the canonical way to encrypt an existing plaintext DB or decrypt an encrypted one.

### 6.9 Journals and temp files

- **Rollback journal**: on disk; encrypted via the pager's codec on write (see §6.2 CODEC2 path).
- **Master journal**: a small plaintext file that names the attached DB journals. Contains no page data.
- **WAL**: encrypted as above.
- **Temp DB / temp tables**: forced in-memory by `SQLITE_TEMP_STORE ∈ {2, 3}` — the build-time `#error` guard at `sqlcipher.c:66-76` enforces this.

### 6.10 Process lifecycle

`sqlcipher_extra_init(const char*)` (`sqlcipher.c:424-586`), wired via `SQLITE_EXTRA_INIT`:
1. Allocate 8 static mutexes (`SQLCIPHER_MUTEX_*`).
2. Allocate and seed the shield mask (an XOR obfuscation mask for in-memory keys).
3. Allocate a default `sqlcipher_provider` and dispatch to the compile-selected `sqlcipher_<name>_setup(p)` function.
4. Call `sqlcipher_register_provider(p)` — this triggers the provider's `init` if any.
5. Register the `sqlcipher_export` SQL function on every db connection (via the init callback).
6. Install memory methods: optionally re-wires `sqlite3_config(SQLITE_CONFIG_MALLOC)` to SQLCipher's sanitising allocator.

`sqlcipher_extra_shutdown()` tears down: provider shutdown, mutex free, shield-mask wipe, log-file close. Registered as `atexit` handler on POSIX and called from `DllMain(DLL_PROCESS_DETACH)` on Windows.

### 6.11 CLI shell integration

`shell.c.in:1064-1075` filters any input line matching `PRAGMA %key%` or `%attach%database%as%key%` out of readline history so passphrases don't land on disk.

The shell also prints a banner including `SQLCipher <version>` via `sqlcipher_version()` (`shell.c.in:12387-12395`, 13761-13772, 13905-13915).

---

## 7. Key Management

### 7.1 Input formats for `PRAGMA key` / `sqlite3_key`

Detected in `sqlcipher_cipher_ctx_key_derive` (`sqlcipher.c:1743-1807`). With `raw_key_sz = 32` (i.e. `key_sz`), `raw_salt_sz = 16` (`FILE_HEADER_SZ`):

| Input form | Length check | Behaviour |
|---|---|---|
| `x'<64 hex>'` | `pass_sz == 64 + 3` | Raw 32-byte encryption key, no PBKDF2. Salt still read/generated normally. HMAC key derived from raw key in 2 iters. |
| `x'<64 hex><32 hex>'` | `pass_sz == 64 + 32 + 3` | Raw key **and** raw salt. No PBKDF2 on encryption key. No salt generation. Salt bytes become page 1 header. HMAC key derived from raw key. |
| `x'<64 hex><64 hex><32 hex>'` | `pass_sz == 64 + 64 + 32 + 3` | Raw encryption key, raw HMAC key, and raw salt. Both PBKDF2 passes skipped entirely. Useful for deterministic test vectors and for applications that manage all key material externally. |
| Any other string | default | Treated as a UTF-8 passphrase. Runs PBKDF2 with `kdf_iter` iterations. |

Blob syntax validation (`sqlcipher.c:1773-1778`): length ≥ 5, starts with `x'`, ends with `'`, inner content is even-length valid hex. Parsing uses `cipher_hex2bin`.

### 7.2 PBKDF2 parameters and defaults

| Parameter | Default (current) | Controlled by |
|---|---|---|
| KDF algorithm | PBKDF2-HMAC-SHA512 | `PRAGMA cipher_kdf_algorithm` |
| Iterations | 256,000 | `PRAGMA kdf_iter` / `cipher_kdf_iter` |
| Fast iterations (HMAC key) | 2 | `PRAGMA fast_kdf_iter` (deprecated) |
| Key length | 32 bytes | from `provider->get_key_sz()` |
| Salt length | 16 bytes | `FILE_HEADER_SZ` |

Version history (from `CHANGELOG.md`):
- 1.x–3.0: 4,000 iters, SHA-1 KDF, SHA-1 HMAC, 1024-byte pages.
- 3.x: 64,000 iters, else same as above.
- 4.0.0 (Nov 2018): 256,000 iters, **SHA-512** KDF, SHA-512 HMAC, **4096-byte** pages.
- 4.1.0 (Mar 2019): deprecated `fast_kdf_iter` pragma.
- 4.4.1 (Oct 2020): deprecated `cipher_store_pass`.
- 4.7.0 (Mar 2025): memory security enabled by default, logging infrastructure expanded.

### 7.3 HMAC key derivation

At `sqlcipher.c:1813-1835`:

```c
if (flags & CIPHER_FLAG_HMAC && derive_hmac_key) {
    memcpy(hmac_kdf_salt, kdf_salt, kdf_salt_sz);
    for (i = 0; i < kdf_salt_sz; i++)
        hmac_kdf_salt[i] ^= hmac_salt_mask;   /* 0x3a */
    provider->kdf(ctx, kdf_algorithm,
                  /* password = */ c_ctx->key, key_sz,
                  hmac_kdf_salt, kdf_salt_sz,
                  /* workfactor = */ fast_kdf_iter,  /* 2 */
                  key_sz, c_ctx->hmac_key);
}
```

### 7.4 Salt lifecycle

- **New DB**: first write to page 1 generates a random 16-byte salt via `provider->random` (`sqlcipher.c:1403`), stored in `ctx->kdf_salt`.
- **Existing DB**: first read calls `sqlite3OsRead(fd, kdf_salt, 16, 0)` (`sqlcipher.c:1401`) before any decryption.
- **Marked** present via `CIPHER_FLAG_HAS_KDF_SALT`. The flag gates re-reading.
- **Rotating salt** requires `sqlcipher_export`; `PRAGMA rekey` does **not** change the salt.
- **Explicit set / get**: `PRAGMA cipher_salt = x'<32 hex>'` / `PRAGMA cipher_salt` (`sqlcipher.c:2861-2882`), only valid before key is used.

### 7.5 Caching and invalidation

Key material lives in `cipher_ctx.key` and `cipher_ctx.hmac_key`, each of size `ctx->key_sz` (32 bytes). Once derived, the `derive_key` flag is cleared. The passphrase itself is zeroed after derivation (unless the deprecated `cipher_store_pass=1` is set).

Invalidation (re-sets `derive_key = 1`, triggering a fresh PBKDF2 next time):
- `cipher_kdf_iter` changed (`sqlcipher.c:1321`).
- `cipher_fast_kdf_iter` changed (`sqlcipher.c:1329`).
- New passphrase set via `sqlite3_key` / `PRAGMA key` (`sqlcipher.c:1306`).

Changing algorithms after the key has been used is rejected (the codec enforces "set before first use" via `CIPHER_FLAG_KEY_USED`).

### 7.6 Memory safety

- `sqlcipher_memset(ptr, byte, len)` — volatile memset that the optimiser won't elide (`sqlcipher.c:679`).
- `sqlcipher_mlock(ptr, len)` — pages locked in RAM so they can't swap to disk (`sqlcipher.c:720`).
- `sqlcipher_free(ptr, len)` — zeros then frees (`sqlcipher.c:964`).
- `sqlcipher_shield(buf, len)` / `sqlcipher_unshield` — XOR the buffer with a random mask while it sits idle in memory; unshield before use, re-shield after. Applied to keys (`sqlcipher.c:1837-1838`, 1249, 1256, 1617, 1623, 1704).
- `PRAGMA cipher_memory_security` (ON by default in recent versions) enables the aggressive free-zeroing allocator.

### 7.7 Rekey flow

`sqlite3_rekey_v2(db, zDb, pKey, nKey)` (`sqlcipher.c:3549-3632`):

1. Derive the **new** key into `write_ctx` via `codec_set_pass_key(..., CIPHER_WRITE_CTX)`. `read_ctx` still holds the old key.
2. `sqlite3BtreeBeginTrans(pBt, 1, 0)` — write transaction.
3. `sqlite3PagerPagecount(pPager, &n)`.
4. Loop `pgno = 1..n`, skipping super-journal pages via `sqlite3pager_is_sj_pgno`:
   - `sqlite3PagerGet(pPager, pgno, &page, 0)` — decrypt with `read_ctx` (old key).
   - `sqlite3PagerWrite(page)` — mark dirty; on commit it'll be re-encrypted with `write_ctx` (new key).
   - `sqlite3PagerUnref(page)`.
5. `sqlite3BtreeCommit(pBt)` writes all dirty pages through the new key.
6. `sqlcipher_codec_key_copy(ctx, CIPHER_WRITE_CTX)` — copy the new key material from `write_ctx` back into `read_ctx`. Now both contexts agree on the new key.

Crash safety: the whole rekey runs as one transaction. If interrupted, the journal rolls everything back with the old key, and the codec is still consistent (old key in both contexts). The new key in `write_ctx` is discarded on rollback.

The salt is *not* changed by rekey. To change the salt, export to a new DB.

### 7.8 Zero-argument `PRAGMA key`

Only valid before any page I/O. `PRAGMA key = ''` is equivalent to no codec at all for ATTACH semantics — the codec does not install and the DB is treated as plaintext.

---

## 8. Pragma Catalog

### 8.1 Key-setting pragmas (dispatched via `PragTyp_KEY` in `pragma.c:2781-2815`)

| Pragma | `iArg` | Input | Action |
|---|---|---|---|
| `PRAGMA key = ...` | 0 | any string | `sqlite3_key_v2()` — set initial key |
| `PRAGMA rekey = ...` | 1 | any string | `sqlite3_rekey_v2()` — change key in-place |
| `PRAGMA hexkey = ...` | 2 | hex string | hex-decode then `sqlite3_key_v2()` |
| `PRAGMA hexrekey = ...` | 3 | hex string | hex-decode then `sqlite3_rekey_v2()` |
| `PRAGMA textkey = ...` | 4 | UTF-8 string | forces string interpretation (no `x'...'` parsing) |
| `PRAGMA textrekey = ...` | 5 | UTF-8 string | same, for rekey |

### 8.2 Cipher configuration pragmas

All are intercepted in `sqlcipher_codec_pragma` (`sqlcipher.c:2586-3210`). Pragmas named `cipher_default_*` are **process-global** and apply to new connections; unprefixed ones are **per-database** and must be set after `PRAGMA key` but before the first page I/O (enforced by `CIPHER_FLAG_KEY_USED`).

#### Page / reserve / HMAC

| Pragma | Type | Values | Default | Location |
|---|---|---|---|---|
| `cipher_page_size` | per-DB | 512..65536 power of 2 | 4096 | `sqlcipher.c:2742` |
| `cipher_default_page_size` | global | 512..65536 power of 2 | 4096 | `sqlcipher.c:2763` |
| `cipher_use_hmac` | per-DB | 0 / 1 | 1 | `sqlcipher.c:2779` |
| `cipher_default_use_hmac` | global | 0 / 1 | 1 | `sqlcipher.c:2771` |
| `cipher_hmac_algorithm` | per-DB | `HMAC_SHA1` / `HMAC_SHA256` / `HMAC_SHA512` | HMAC_SHA512 | `sqlcipher.c:2884` |
| `cipher_default_hmac_algorithm` | global | same | HMAC_SHA512 | `sqlcipher.c:2910` |
| `cipher_hmac_pgno` | per-DB (deprecated) | `le` / `be` / `native` | le | `sqlcipher.c:2793` |
| `cipher_hmac_salt_mask` | per-DB (deprecated) | `x'<byte>'` | 0x3a | `sqlcipher.c:2822` |

#### Key derivation

| Pragma | Type | Values | Default |
|---|---|---|---|
| `kdf_iter` / `cipher_kdf_iter` | per-DB | int ≥ 1 | 256000 |
| `cipher_default_kdf_iter` | global | int ≥ 1 | 256000 |
| `fast_kdf_iter` | per-DB (deprecated) | int ≥ 1 | 2 |
| `cipher_kdf_algorithm` | per-DB | `PBKDF2_HMAC_SHA1` / `...SHA256` / `...SHA512` | SHA512 |
| `cipher_default_kdf_algorithm` | global | same | SHA512 |

#### Layout / salt

| Pragma | Type | Values | Default |
|---|---|---|---|
| `cipher_plaintext_header_size` | per-DB | 0..32 | 0 |
| `cipher_default_plaintext_header_size` | global | 0..32 | 0 |
| `cipher_salt` | per-DB | `x'<32 hex>'` or read-only | auto |

#### Compatibility

| Pragma | Type | Values |
|---|---|---|
| `cipher_compatibility` | per-DB | 1..4 |
| `cipher_default_compatibility` | global | 1..4 |
| `cipher_migrate` | per-DB | (no arg) — upgrade old DB in place |

#### Introspection

| Pragma | Returns |
|---|---|
| `cipher_version` | `"4.14.0 community"` |
| `cipher_provider` | e.g. `"openssl"` |
| `cipher_provider_version` | e.g. `"OpenSSL 3.2.1 …"` |
| `cipher_fips_status` | 0 / non-zero |
| `cipher_status` | 1 = healthy, 0 = error (4.12+) |
| `cipher_settings` | list of `PRAGMA` SQL showing current per-DB settings |
| `cipher_default_settings` | same for global defaults |
| `cipher_integrity_check` | HMAC-walks every page; returns a row per failure |

#### Operations / misc

| Pragma | Purpose |
|---|---|
| `cipher_add_random = x'...'` | seed RNG with extra entropy |
| `cipher_profile = cb` | enable profiling callback |
| `cipher_memory_security` | 0/1, one-way latch (can be turned on but not off once active) |
| `cipher_store_pass` | 0/1 (deprecated) — retain passphrase after derivation |

#### Logging

| Pragma | Values |
|---|---|
| `cipher_log` | a file path (or `stderr`, `logcat`, `device` sentinels) |
| `cipher_log_level` | `NONE` / `ERROR` / `WARN` (default) / `INFO` / `DEBUG` / `TRACE` |
| `cipher_log_source` | bit mask of `CORE` / `MEMORY` / `MUTEX` / `PROVIDER` / `ANY` / `NONE` |

#### Test-only (require `-DSQLCIPHER_TEST`)

| Pragma | Values |
|---|---|
| `cipher_test` | read-only — returns current fault-injection flag bits |
| `cipher_test_on` | `fail_encrypt` / `fail_decrypt` / `fail_migrate` |
| `cipher_test_off` | same |
| `cipher_test_rand` | integer seed; 1-in-N probability of injected failure |

### 8.3 `cipher_compatibility` modes

Handled at `sqlcipher.c:2973-3029`:

| Mode | Page size | HMAC algorithm | KDF algorithm | KDF iterations | HMAC enabled | Reserve size |
|---|---|---|---|---|---|---|
| **1** (SQLCipher 1.x) | 1024 | — | PBKDF2-SHA1 | 4,000 | OFF | 16 (IV only) |
| **2** (SQLCipher 2.x) | 1024 | HMAC-SHA1 | PBKDF2-SHA1 | 4,000 | ON | 48 |
| **3** (SQLCipher 3.x) | 1024 | HMAC-SHA1 | PBKDF2-SHA1 | 64,000 | ON | 48 |
| **4** (SQLCipher 4.x, default) | 4096 | HMAC-SHA512 | PBKDF2-SHA512 | 256,000 | ON | 80 |

Setting `cipher_compatibility = N` applies all four settings atomically. Useful sequence for opening a legacy DB:

```sql
PRAGMA key = '...';
PRAGMA cipher_compatibility = 3;
SELECT count(*) FROM sqlite_master;
```

### 8.4 `cipher_migrate` — in-place legacy upgrade

At `sqlcipher.c:2013-2185`. Steps:

1. Pull raw key material from the current read context.
2. Probe: try opening with current (v4) settings. If fails, try compat 3, then 2, then 1.
3. Create a sibling temp file `<dbname>-migrated`, apply detected compat mode to main DB.
4. `ATTACH` the temp file, key it with the same passphrase (temp gets v4 settings).
5. `SELECT sqlcipher_export('migrate')` to copy schema + data from old → new.
6. Copy `user_version` and other pragmas.
7. Validate `db->autoCommit == 1` and `db->nVdbeActive <= 1`.
8. Align page size and rekey the main DB with the migrated key material.
9. Close both DB file handles.
10. Atomic rename: POSIX `rename(temp, main)` or Windows `MoveFileExW(MOVEFILE_REPLACE_EXISTING)`.
11. Reopen both files, reset the pager.

Error handling detaches the temp DB, resets schemas, deletes the temp file, and latches the codec into permanent error state.

### 8.5 `cipher_integrity_check`

At `sqlcipher.c:1937-2011`. Walks the file directly (does not require a key that unlocks data — only an HMAC key that verifies pages). For every page, re-reads from disk and calls `sqlcipher_page_hmac`; compares to the stored tag; yields a row per failure. Also checks that `file_size % page_sz == 0`.

Requires HMAC to be enabled (modes 2–4).

---

## 9. Defaults (Summary)

Current SQLCipher 4.14.0 defaults, from `sqlcipher.c:259-281`:

```
page_size                     = 4096
kdf_iter                      = 256000
fast_kdf_iter                 = 2
kdf_salt_sz                   = 16   (FILE_HEADER_SZ)
key_sz                        = 32   (AES-256)
iv_sz                         = 16
block_sz                      = 16
hmac_algorithm                = SQLCIPHER_HMAC_SHA512
kdf_algorithm                 = SQLCIPHER_PBKDF2_HMAC_SHA512
hmac_sz                       = 64   (SHA-512)
plaintext_header_sz           = 0
reserve_sz                    = 80   (16 + 64, multiple of block_sz)
flags                         = CIPHER_FLAG_HMAC | CIPHER_FLAG_LE_PGNO
log_level                     = SQLCIPHER_LOG_WARN
log_source                    = SQLCIPHER_LOG_ANY
mem_security                  = 0    (off by default pre-4.7; on since 4.7)
store_pass                    = 0    (deprecated)
hmac_salt_mask                = 0x3a
```

---

## 10. Test Suite

Under `test/`:

| File | Focus |
|---|---|
| `sqlcipher-core.test` (1078 lines) | `PRAGMA key`, `sqlite3_key`, `sqlite3_rekey`; open/close, data integrity, attach/detach, temp tables |
| `sqlcipher-pragmas.test` (499 lines) | Per-DB pragmas and their `_default_` counterparts |
| `sqlcipher-compatibility.test` (1484 lines) | Reading legacy v1/v2/v3 files, `cipher_compatibility`, `cipher_migrate` round-trips |
| `sqlcipher-integrity.test` (383 lines) | HMAC tampering detection, `cipher_integrity_check` |
| `sqlcipher-backup.test` (157 lines) | `sqlite3_backup_*` between encrypted DBs |
| `sqlcipher-rekey.test` (275 lines) | In-place rekey via API and pragma; rollback, mixed changes |
| `sqlcipher-plaintext-header.test` (473 lines) | `cipher_plaintext_header_size` behaviour, recovery mode |
| `sqlcipher-zmemory.test` (61 lines) | `cipher_memory_security`, private heap, zeroing |
| `sqlcipher-codecerror.test` (171 lines) | Error handling, key derivation failures, corrupt-page handling |

Tests are Tcl-based and run against a specially-built `testfixture` binary compiled with `-DSQLCIPHER_TEST`.

---

## 11. Security-Relevant Gotchas

Documented behaviours worth understanding before depending on them:

1. **Short-read exception in decrypt.** If HMAC verification fails and the entire page is zeros and autovacuum is on, the page is treated as a blank (past-EOF) read and the error is suppressed (`sqlcipher.c:1686-1693`). Without this, autovacuum's shrink operations would trigger spurious HMAC failures.

2. **Plaintext-header recovery mode.** When `plaintext_header_sz == page_sz − reserve_sz`, decryption errors on page 1 are logged but do not latch the codec into permanent error state (`sqlcipher.c:3287-3288`). Tools use this to recover data from partially-corrupt DBs.

3. **IV is random, not derived.** Per-page IV comes from the CSPRNG on every write. Identical plaintext pages encrypt to different ciphertexts — good for security, but means bit-exact reproducibility of a DB file requires either (a) seeding the RNG or (b) using `SQLCIPHER_TEST` fault-injection mode with a fixed salt.

4. **Page-number byte order matters for HMAC.** Default is little-endian (`CIPHER_FLAG_LE_PGNO`). Opening a DB written with a different endianness setting will fail HMAC verification. Compatibility mode does not adjust this.

5. **Rekey doesn't change salt.** A rekey gives you a new encryption key and a new HMAC key, but the 16-byte salt and therefore the input to PBKDF2 is unchanged. If your threat model includes "rotate the salt", you need `sqlcipher_export` to a freshly-seeded DB.

6. **Set-once configuration.** Most `cipher_*` pragmas must be set before the key is *used* (first codec call), not merely before the key is *set*. The `CIPHER_FLAG_KEY_USED` latch rejects late changes. Typical ordering:

   ```sql
   PRAGMA key = '...';
   PRAGMA cipher_page_size = 8192;
   PRAGMA cipher_kdf_iter = 500000;
   -- first actual query here locks the config in
   SELECT count(*) FROM sqlite_master;
   ```

7. **Per-page context reuse is *not* done in the shipped backends.** Each page encrypt/decrypt allocates a fresh `EVP_CIPHER_CTX` (OpenSSL) / `CCCryptorRef` (CC) / `symmetric_CBC` (LTC) and frees it. Hot-path allocation is present. A custom provider is free to cache and reuse.

8. **Error state is latching.** Once `ctx->error != SQLITE_OK` any further codec call fails. The only way to reset is to close the connection.

9. **The passphrase is wiped unless `cipher_store_pass` is on** (`sqlcipher.c:1873-1877`). Code that re-derives (e.g. `kdf_iter` change) after wipe will fail because the passphrase is no longer available.

10. **No NSS backend in this tree.** README advertises it, but no `crypto_nss.c` exists in the clone. If needed, use OpenSSL.

11. **VACUUM cannot change page size** on an encrypted DB; the requested new size is silently reset to 0 (`vacuum.c:250-263`). To change page size you must `sqlcipher_export` into a new DB configured with the desired size.

12. **`SQLITE_TEMP_STORE` must be 2 or 3** or `sqlcipher.c` refuses to compile. In-memory temp storage is essential for confidentiality — disk temp files would leak plaintext index spills.

13. **`sqlcipher_extra_init` / `_extra_shutdown`** must be wired via `SQLITE_EXTRA_INIT` / `SQLITE_EXTRA_SHUTDOWN` — SQLite will not call them automatically. Missing these means no crypto provider is registered and every `PRAGMA key` fails.

14. **WAL is always encrypted.** `SQLITE_OMIT_WAL` is not available. Applications using `journal_mode=DELETE` still incur WAL code in the binary but not on disk.

15. **Shared-cache mode protections.** Codec operations under shared cache are serialised with `SQLCIPHER_MUTEX_SHAREDCACHE`. Multiple connections to the same DB each get their own `codec_ctx` but share the physical pager; be careful mixing multiple keys via shared cache (it is rejected).

---

## 12. Public API Reference

### 12.1 C API

```c
/* Set the key for the main db. Derivation is deferred to first page I/O. */
int  sqlite3_key    (sqlite3 *db,                  const void *pKey, int nKey);
int  sqlite3_key_v2 (sqlite3 *db, const char *zDb, const void *pKey, int nKey);

/* Change the key. Runs a full-file re-encryption transaction. */
int  sqlite3_rekey    (sqlite3 *db,                  const void *pKey, int nKey);
int  sqlite3_rekey_v2 (sqlite3 *db, const char *zDb, const void *pKey, int nKey);

/* No-op stub for SEE compatibility. */
void sqlite3_activate_see(const char *zPassPhrase);
```

### 12.2 Custom provider API

```c
int  sqlcipher_register_provider (sqlcipher_provider *p);
sqlcipher_provider* sqlcipher_get_provider (void);
```

To supply a custom backend, build with `-DSQLCIPHER_CRYPTO_CUSTOM=<name of your setup fn>` where the function has signature `int your_setup(sqlcipher_provider *p)` and fills in every function pointer of the struct.

### 12.3 SQL functions

```sql
SELECT sqlcipher_export('target_schema');   -- export main into attached encrypted DB
SELECT sqlcipher_version();                 -- returns "4.14.0 community"
```

---

## 13. Operational Recipes

### 13.1 Open existing encrypted DB

```sql
PRAGMA key = 'passphrase';
SELECT count(*) FROM sqlite_master;   -- triggers key derivation; fails loudly on wrong key
```

### 13.2 Create encrypted DB from scratch

```sql
-- sqlite3 new.db
PRAGMA key = 'passphrase';
CREATE TABLE ...;
```

Salt is generated automatically on first write to page 1.

### 13.3 Encrypt an existing plaintext DB

```sql
-- sqlite3 plain.db
ATTACH DATABASE 'encrypted.db' AS encrypted KEY 'passphrase';
SELECT sqlcipher_export('encrypted');
DETACH DATABASE encrypted;
```

### 13.4 Decrypt an encrypted DB

```sql
-- sqlite3 encrypted.db
PRAGMA key = 'passphrase';
ATTACH DATABASE 'plain.db' AS plain KEY '';
SELECT sqlcipher_export('plain');
DETACH DATABASE plain;
```

### 13.5 Change passphrase

```sql
PRAGMA key = 'old_passphrase';
PRAGMA rekey = 'new_passphrase';
```

### 13.6 Open a legacy v3 DB in a v4 build

```sql
PRAGMA key = 'passphrase';
PRAGMA cipher_compatibility = 3;
-- now queryable; file stays in v3 format
```

### 13.7 Upgrade a legacy DB to current format in place

```sql
PRAGMA key = 'passphrase';
PRAGMA cipher_migrate;          -- auto-detects old version, swaps file on disk
```

### 13.8 Integrity-check a DB

```sql
PRAGMA key = 'passphrase';
PRAGMA cipher_integrity_check;
-- yields zero rows if all pages verify, otherwise one row per bad page
```

### 13.9 Let file(1) identify encrypted DBs

```sql
PRAGMA cipher_default_plaintext_header_size = 32;
-- creates DBs whose first 32 bytes look like a normal SQLite header
```

---

## 14. Version Notes (CHANGELOG Highlights)

| Version | Change |
|---|---|
| 4.0.0 (Nov 2018) | Page 1024→4096, KDF iter 64K→256K, SHA1→SHA512 (KDF + HMAC). Introduced `cipher_compatibility`, `cipher_migrate`, `cipher_plaintext_header_size`, `cipher_salt`, `cipher_settings`, and the `cipher_default_*` family. |
| 4.1.0 | Deprecated `fast_kdf_iter`. |
| 4.2.0 | Added `cipher_integrity_check`. |
| 4.4.1 | Deprecated `cipher_store_pass`. |
| 4.5.0 | Memory-security OFF by default (reverted in 4.7). |
| 4.7.0 (Mar 2025) | Memory security **ON** by default. Expanded logging (`cipher_log`, levels, sources, logcat/os_log). Private heap expanded to 48 KB. |
| 4.12.0 (Dec 2025) | Added `cipher_status`. |
| 4.14.0 | Current; tracks upstream SQLite 3.51.3. |

---

## 15. References

- Upstream source: <https://github.com/sqlcipher/sqlcipher>
- Official docs: <https://www.zetetic.net/sqlcipher/>
- Design paper / threat model: <https://www.zetetic.net/sqlcipher/design/>
- Discussion forum: <https://discuss.zetetic.net/c/sqlcipher>

This spec was derived by reading the source tree at SQLCipher 4.14.0 (upstream SQLite 3.51.3). All line numbers cited are from that checkout.
