# SQLCipher-on-any-store: Fit Analysis

How well SQLCipher's encryption design maps onto the any-store btree engine in `internal/btree/`. What's easy, what's awkward, what's likely to bite.

---

## TL;DR

Structurally, any-store is a good host for page-level authenticated encryption. The btree is a deliberate SQLite port; most of SQLCipher's assumptions (fixed page size, opaque-blob pages, reserve area honoured by cell math, WAL that separates frame header from payload, plaintext-cache-with-encryption-at-I/O-boundary) already hold.

The awkward parts are all in the *neighbourhood of page 1* and in *crossings between encrypted and non-encrypted data paths* (WAL spill → checkpoint, savepoints, masterStore). None are blockers; most need one careful design decision each, then the code falls out.

The two places I'd expect to get bitten:

1. **The 100-byte dbHeader overlaps where SQLCipher puts its salt.** any-store's header is bigger and carries state (FileChangeCount, SchemaCookie) that other parts of the pager *need to read without decryption*. The SQLCipher trick of "first 16 bytes of page 1 = plaintext salt" doesn't cleanly apply.
2. **WAL frames already have a paired-word checksum** over plaintext header + plaintext page data. Decide early whether the new tag *replaces* it or stacks on top, because the checksum is wired into `walWriteFrame`, `walIndex`, and the header-reseed-on-restart logic. Either answer is defensible; picking both by accident gets you an awkward hybrid.

WAL spill (dirty pages evicted to WAL pre-commit via `pagerStress`) is *not* a novel problem — SQLite does this too (`pager.c:4725` → `pagerWalFrames(..., isCommit=0)` at `pager.c:4765`), and any-store's `pagerStress` (`pager.go:1230`) is explicitly modelled on it. SQLCipher handles it automatically via the single-chokepoint rule. See §5 for the one real discipline it imposes.

Everything else is mechanical.

---

## 1. Mapping SQLCipher's 12 core assumptions onto any-store

| # | SQLCipher assumption | any-store state | Verdict |
|---|---|---|---|
| 1 | Single codec callback on the pager; pager invokes it at every read/write site | No codec hook today; pager has 3 file-read sites (`pager.go:512, 570, 647`), the WAL has its own read/write (`wal.go:1408–1525, 1596–1630`), and the masterStore is a third path | **Fits.** Need to install the hook in four functions, or add an indirection layer `pageIO.readPage/writePage` that concentrates the three paths. |
| 2 | Fixed `reserve_sz` honoured by btree cell math | `dbHeader.ReservedSpace` field at byte 20 of the header (`page.go:160, 188, 242`) is already parsed/serialised. `maxLocalPayload`/`minLocalPayload`/`overflowPageUsable`/`localPayloadSize` all take `usableSize = pageSize − reservedSpace` (`page.go:104, 109, 116, 122`). `page.usableSize(reservedSpace)` (`page.go:326`) is the single source of truth. | **Fits cleanly.** Just set `ReservedSpace = 80` at new-DB init. No cell-math changes. |
| 3 | Page 1 special: first 16 bytes plaintext salt, magic spliced back on decrypt | any-store has a **100-byte** dbHeader with magic `"BTree format 1\x00"` at bytes 0–15 and live fields (change counter, freelist, schema cookie…) at bytes 24–99 (`page.go:155–257`). | **Doesn't fit directly.** See §3 — needs a design choice. |
| 4 | WAL frames encrypted on write, decrypted on read | WAL frame layout: 24-byte plaintext header (`wal.go:262–287`) + page-size payload. Writer calls `file.WriteAt(frameHdr, off)` then `file.WriteAt(p.data, off+walFrameSize)` — two writes, payload is naturally separable. | **Fits cleanly.** Encrypt `p.data` before the second WriteAt; decrypt after ReadAt. |
| 5 | Plaintext-only WAL frame header (no encryption) | Header format is salt1/salt2/pgno/dbSize/checksum1/checksum2 — none of this is sensitive. | **Fits cleanly.** |
| 6 | Per-page random IV, stored in reserve area | No current analogue. | **Fits, but see §4** for integration with the existing checksum. |
| 7 | Page cache holds plaintext; encryption at disk boundary | pcache stores raw `[]byte` in `page.data` (allocated from slab). Nothing in the cache ever goes direct to disk — everything routes through the pager/WAL. | **Fits perfectly.** This is the cleanest part of the match. |
| 8 | Codec sees opaque bytes, not parsed cells | Pager reads/writes always operate on the full `pg.data` buffer; cell parsing happens *after* the pager returns. | **Fits perfectly.** |
| 9 | VACUUM can't change page size on encrypted DB | any-store has no VACUUM. | **Non-issue.** |
| 10 | Backup API with matching page sizes | any-store has no backup API today. | **Non-issue.** |
| 11 | ATTACH with per-DB keys | any-store has no multi-database attach; one DB = one instance. | **Non-issue.** |
| 12 | Temp tables forced in-memory | any-store has no temp tables — all data is in the single file. | **Non-issue.** |

Summary: 6 clean fits, 3 non-issues, 2 that need one design decision each (page 1 layout, checksum vs HMAC), and 1 that's genuinely novel relative to SQLite (WAL spill).

---

## 2. What maps naturally

### 2.1 The page I/O chokepoint

any-store has exactly three file-level read sites and one file-level write site (checkpoint writeback):

- `pager.getPageWriter()` at `pager.go:512`
- `pager.getPageReader()` at `pager.go:647`
- `pager.readTempPage()` at `pager.go:570`
- `wal.checkpointWithMode()` (the backfill copy from WAL → DB)

Plus the WAL's own read and write paths (`wal.readFrame`, `wal.writeFrames`, `wal.writeFramesMem`).

These are natural codec sites. The cleanest shape is **two codec hooks**: one on `pager` (called for DB-file reads and for checkpoint writes to the DB file), one on `wal` (called for WAL reads and writes, including the in-memory arena). They share the same derived keys but operate on different buffer lifetimes.

Trying to force a single hook on the pager that also covers WAL I/O means piping `wal` through `pager`, which would introduce a cyclic dependency or a callback-registry dance. Two hooks is less code.

### 2.2 The cell-layout plumbing

This is the part where any-store's deliberate SQLite fidelity pays off. `ReservedSpace` is already a field. The code already reads it, serialises it, and every cell-size calculation takes `usableSize` as a parameter — not `pageSize`. When `ReservedSpace` is currently 0, `usableSize == pageSize` and everything collapses to the present behaviour. Switching encryption on is just setting the header byte to 80 at creation time.

One watchout: `initNewDB` (`pager.go:342`) hardcodes `ReservedSpace = 0`. That's the spot to wire in the encryption-aware value.

### 2.3 The cache split

The pcache-per-connection design actually makes encryption *easier* than it is in SQLite:

- Writer owns one `writerCache`. No locking. Keys can live on the pager; the writer's hot path never contends.
- Readers each get a private `pcache` from the `readerCaches` channel. Each reader operates on its own cache; the codec gets called by the reader goroutine with no cross-reader interference.
- AES-GCM / AES-CBC contexts from Go's `crypto/aes` are safe for concurrent use once constructed (the `cipher.Block` is immutable). So one `aes.Block` per key, shared across all readers and the writer.

Compared to OpenSSL's "mutex around RAND_bytes because older versions crash" contortions in `crypto_openssl.c:125–130`, Go's stdlib makes this a non-topic.

### 2.4 Savepoints

Memory notes tell me savepoints store plaintext pre-images in `pager.savepoints[]`. That's correct for encryption too — savepoint copies are snapshots *of the cache*, i.e. plaintext. On rollback you restore the plaintext into the cache, and it gets re-encrypted on next write. No decrypt-then-re-encrypt dance needed (unlike SQLite's rollback journal, which SQLCipher has to handle specially at `pager.c:2506–2509`).

This is a *better* situation than SQLCipher's. WAL-only + in-cache pre-images means one less boundary.

### 2.5 Per-connection key state

SQLCipher has `read_ctx` and `write_ctx` inside the codec context so rekey can run with a "new write key" while reads still decrypt under the old key (`sqlcipher.c:159–192`, rekey at `sqlcipher.c:3549–3632`). Without rekey, this split is unnecessary. any-store has no rekey, so a single key material struct on the pager suffices. If rekey is added later, introduce the split then.

### 2.6 Codec scratch buffers: per-caller, not pooled

Every codec call needs two transient buffers alongside the key:

1. A page-sized buffer for the transformed bytes — `AEAD.Seal`/`Open` don't support `dst==src` aliasing, so encrypt-in-place is off the table.
2. A small nonce + AAD scratch to pass into `Seal`/`Open`.

**Placement:** both live as fields on whichever struct drives the call, not in a `sync.Pool`:

- `pager.codecScratch` + `pager.codecAEAD` — writer path (`initNewDB`, `getPageWriter`). Safe as a single instance (single-writer invariant).
- `pcache.codecScratch` + `pcache.codecAEAD` — reader path (`getPageReader` including its WAL read-through). Each reader tx owns its own `pcache`, so one scratch per in-flight reader, no locking.
- `wal.codecScratch` + `wal.codecAEAD` — WAL `writeFrame`. Writer-only.

**Why not `sync.Pool`:**

1. `pool.Put([]byte)` heap-allocates a 24-byte eface box every call — a slice header (ptr+len+cap) doesn't fit the interface data word. Per-page Put during a scan turns this into the top allocation site (staticcheck SA6002). A caller-owned field bypasses the pool entirely.
2. `var nonce [12]byte; aead.Seal(..., nonce[:], ...)` heap-allocates `nonce` on every call — `cipher.AEAD.Seal` is an interface method and Go's escape analysis is conservative. Embedding nonce/AAD as fields on a heap-allocated parent struct (pager/pcache/wal) makes `&parent.aeadScratch.nonce[:]` point at existing heap memory, so nothing escapes per call.

**Mapping to SQLCipher.** SQLCipher's `codec_ctx->buffer` (page-sized scratch) and `cipher_ctx->iv` (IV scratch) live **per connection**. Each `sqlite3_open()` produces an independent `Btree` / `Pager` / `codec_ctx`, and SQLite's concurrent-reader story is "use multiple connections." So SQLCipher's single buffer inside one codec_ctx is safe because nothing else shares that ctx.

any-store's concurrency model is structurally different: one `pager` is shared across all reader goroutines, with per-tx `pcache` snapshots. Our **`pcache` plays the role of "connection"** — each read tx has its own, and the scratch lives there for the same reason SQLCipher's lives on `codec_ctx`. Same pattern, different partitioning unit (goroutine+tx vs. connection), because Go's concurrency primitive is the goroutine not the process connection.

**Rekey implication.** If online rekey is ever added (§2.5), SQLCipher's `read_ctx`/`write_ctx` split would need its own scratch pair per ctx — same pattern, twice.

---

## 3. The page 1 problem

### 3.1 Why it's different from SQLCipher

SQLCipher picks 16 bytes at offset 0 of page 1 because that's exactly the salt length (`FILE_HEADER_SZ = 16`) and the SQLite magic is conveniently 16 bytes. The magic is reconstructed on decrypt by splicing the constant `"SQLite format 3\0"` into the decrypted output. The change counter, freelist pointers, schema cookie, etc. are all *past* byte 24 of page 1 — *inside* the encrypted region.

any-store's dbHeader is 100 bytes (`dbHeaderSize = 100`, `page.go:70`) and contains:

```
offset  field                  size
0       magic "BTree format 1\0"  16
16      PageSize                  2
18      WriteVersion              1
19      ReadVersion               1
20      ReservedSpace             1
21      maxEmbeddedPayloadFrac    1
22      minEmbeddedPayloadFrac    1
23      leafPayloadFrac           1
24      FileChangeCount           4
28      DatabaseSize              4
32      FirstFreelistPg           4
36      TotalFreelistPgs          4
40      SchemaCookie              4
44      SchemaFormat              4
48      DefaultCacheSize          4
52      (unused root ptr)         4
56      TextEncoding              4
60      UserVersion               4
64      (incremental vacuum)      4
68      AppID                     4
72      (reserved for expansion)  20
92      VersionValidFor           4
96      Version number            4
```

Bytes 72–92 are currently `clear()`'d. That's 20 free bytes of reserved room right in the header.

### 3.2 Three possible layouts

**(A) Encrypt page 1 normally, salt in the 20 reserved bytes of the header.**

Keep the header plaintext (bytes 0–99). Put the 16-byte salt at, say, bytes 72–87 (inside the reserved-for-expansion range). The existing header fields keep their semantics and are readable without the key — FileChangeCount stays accessible for cache invalidation, DatabaseSize stays accessible for initial pager sizing, etc.

The encrypted region for page 1 starts at byte 100 (where the btree page header already starts today, per `page.go:496`: `off := 0; if pgno == 1 { off = dbHeaderSize }`). Byte 100 onward gets treated as the plaintext input to AES; the reserved tail (bytes `pageSize - 80 .. pageSize - 1`) carries IV + HMAC.

This is SQLCipher's `cipher_plaintext_header_size` = 100, as its natural default. SQLCipher allows 0–32 for this value; any-store's equivalent would fix it at 100 or make it configurable with 100 as the minimum.

**Advantages:** readable file-type identity; existing code that reads the header byte-by-byte keeps working; the FileChangeCount / cache-coherence logic is unchanged; no "splice the magic back on decrypt" hack needed.

**Drawbacks:** 100 bytes of page 1 are permanently plaintext — reveals the page size and approximate DB size to anyone with file access. This is not worse than SQLCipher's default when `plaintext_header_size = 0` (SQLCipher also has to leave the salt plaintext), just more of it.

**(B) Carry the salt in its own fixed-size prefix before page 1.**

Add a sidecar — either a second file (`db.salt`) or a fixed 512-byte prefix to the DB file that shifts all page offsets by +512. Page 1 starts at file offset 512 and is fully encrypted.

**Advantages:** minimum plaintext surface.

**Drawbacks:** breaks the "page N is at offset `(N-1) * page_size`" invariant that's woven through the pager, the WAL index, every readAt/writeAt site. Introduces a prefix-aware offset math everywhere. The existing header fields (change counter, schema cookie) are now *inside* the encrypted region, so cache invalidation logic has to decrypt before it can tell whether to invalidate — which defeats the whole point of the coherence counter.

Rejected.

**(C) Encrypt page 1 except for a variable plaintext-header-size.**

The SQLCipher model, parameterised. Keep first N bytes plaintext (N >= 16 to preserve the salt; default N = 100 to preserve the whole header). Let the user opt into a smaller N if they want less plaintext exposure, understanding that then the change-counter logic must either decrypt-to-check or be moved into the plaintext region.

Essentially a generalisation of (A). I'd pick (A) as the default and only support varying N if a concrete threat model asks for it.

### 3.3 Interaction with the magic check

`deserialize` at `page.go:227` currently rejects a DB whose first 15 bytes don't match `"BTree format 1\x00"`. With layout (A) those bytes stay plaintext, so the check is unchanged and distinguishes encrypted from non-encrypted DBs only via presence of the salt and ability-to-decrypt-page-1 — same as SQLCipher.

A decryption failure on page 1 (wrong key) will surface as "HMAC failed" on the first real page access, which is equivalent to SQLCipher's behaviour of returning garbage-looking bytes to the btree parser.

---

## 4. Checksum vs HMAC: don't do both naïvely

### 4.1 What's there

`wal.go:262–338` — WAL frame checksum: cumulative paired-word Adler-style rolling checksum over frame header bytes 0–7 and the full page payload. Stored in header bytes 16–23. Rolling means frame N's checksum depends on frame N-1, so single-frame verification is not possible without replaying from the start; this is identical to SQLite's WAL design. It's a fast integrity check designed to detect torn writes and accidental corruption, not adversarial tampering.

There's also a `crc32` import in `page.go` — haven't traced its full use, but evidently at least one page-level checksum lives in the codebase.

### 4.2 What SQLCipher does

- No change to SQLite's WAL checksum. It keeps running, cumulative, over *ciphertext* frame + IV + HMAC, same as SQLite over plaintext.
- Adds a per-page HMAC-SHA512 over `ciphertext || IV || pgno`, stored in the reserved tail. Verified on decrypt.

### 4.3 What to do for any-store

Two design choices:

**(i) Checksum over ciphertext, HMAC over ciphertext.** Rolling checksum keeps its current bits-over-the-wire semantics (catches torn frames). HMAC catches tampering. The checksum's plaintext-visibility property is lost, but that was never a requirement — the checksum is a WAL-level integrity field, not a page field.

Easiest path; minimal changes to `walChecksum` call sites. Just move the call to after encryption.

**(ii) Checksum over plaintext, HMAC over ciphertext.** Mirrors SQLCipher more closely but creates asymmetry: checksum is computed at a different point in the pipeline from HMAC, checksum covers original data while HMAC covers on-disk data. You now have two "integrity" fields protecting different things; a bug in either one produces subtly different corruption modes.

**Picking (i).**

With (i), the WAL frame header stays the same layout, still has its salt1/salt2/checksum1/checksum2 fields, still plays its role in torn-write detection. The only difference is *when* in the pipeline the checksum is computed — after encryption, not before.

### 4.4 Do we need HMAC at all?

An honest question. SQLCipher adds HMAC because the CBC-no-pad mode by itself is malleable: an attacker can flip any bit of ciphertext block N and that flip lands in the corresponding plaintext block (plus a garbled block N-1). Without HMAC, tamper detection depends on the page parser noticing something wrong — unreliable.

An alternative is AES-GCM or AES-CTR+HMAC or ChaCha20-Poly1305. AES-GCM is attractive because it integrates the MAC (16-byte tag) into the cipher operation, costs less than CBC+HMAC-SHA512 (single pass), and Go's stdlib has `cipher.NewGCM` on any `cipher.Block`.

Reserve layout under AES-GCM: 12-byte nonce + 16-byte tag = 28 bytes per page. Round up to block boundary for consistency = 32 bytes reserve. Compared to 80 bytes for CBC+HMAC-SHA512, that's meaningful: at 4 KiB pages you recover 48 bytes of usable space per page (~1.2% of the DB).

Go's AES-GCM (`crypto/cipher`) has mature AES-NI paths and is ~6× faster than CBC+HMAC-SHA512 on modern CPUs for 4 KiB pages. For a database where commit throughput is limited by crypto cost, this is a big deal.

**Design choice:** pick AES-256-GCM (not CBC) from day one. It's 2026, CBC-with-HMAC is a legacy SQLCipher choice made when GCM wasn't well-supported; not a reason to inherit it.

Keep the PBKDF2 key derivation (it's the user-facing key-stretching part and not related to the bulk cipher choice).

---

## 5. WAL spill: one discipline, not a problem

### 5.1 Correction

Earlier drafts of this doc claimed WAL spill (pre-commit eviction to WAL via `pagerStress`) was novel relative to SQLite. **That's wrong.** SQLite does exactly the same thing:

- `pagerStress` (`pager.c:4725`) calls `pagerWalFrames(pPager, pPg, 0, 0)` at `pager.c:4765` — `nTruncate=0, isCommit=0`, the spill shape.
- `pagerWalFrames` at `pager.c:3280` has the assertion `pList->pDirty==0 || isCommit` (line 3299), confirming exactly one spill frame per call.
- Readers are isolated because `wal.c` only advances `mxFrame` on commit frames (those with `nTruncate>0`), so uncommitted spill frames are invisible across transactions.

any-store's `pagerStress` (`pager.go:1230–1293`) is explicitly modelled on this; the comment at `pager.go:1234` says "Modeled after SQLite's pagerStress() (pager.c:4609-4681)". The call shape `p.wal.writeFrames([]*page{pg}, false, 0)` at `pager.go:1280` is the direct Go equivalent of `pagerWalFrames(pPager, pPg, 0, 0)`.

SQLCipher handles spill correctly and has for years — `pagerWalFrames` feeds every frame through the normal pager write path, which invokes the codec at `wal.c:3963-3972` via `sqlcipherPagerCodec`. Commit frame vs spill frame is indistinguishable at the write-to-disk layer.

### 5.2 The one discipline this imposes

The codec hook must sit at the single WAL-write chokepoint. In any-store that's `wal.go:1466–1471` (`file.WriteAt(frameHdr, off)` + `file.WriteAt(p.data, off+walFrameSize)`). Install the codec on the payload write there, and spill + commit + in-memory-arena paths are all covered automatically, same as SQLCipher's approach.

What you must *not* do: give any caller a bypass. Any direct `file.WriteAt(p.data, ...)` that skips the codec writes plaintext into the WAL and corrupts the DB silently. Subsequent reads fail tag verification and look like disk corruption, not like a missing codec call. Make it a code-review invariant and ideally an API one (unexported frame-write helper, guarded by a type that only the codec path produces).

### 5.3 IV lifecycle across spill

A minor note worth checking in tests, not a blocker:

- Page spilled → writer modifies again → new WAL frame with new IV. Fine.
- Page spilled → not modified → commit does not emit a new frame; the spill frame *is* the committed version, with the IV drawn at spill time. Fine — IV uniqueness per (key, page-instance) is what matters, not per-commit.
- Transaction rolled back → spill frames logically discarded via WAL index truncation. Bytes on disk remain until overwritten. Identical to SQLite's untouched-after-rollback behaviour, and identical to SQLCipher's.

### 5.4 Savepoint + spill interaction

The memory notes on "spilled+evicted pages removed from writePages by onEvict weren't discarded on rollback" describe a pre-encryption bug fixed via `writerCache.truncate(sp.dbSize)`. With encryption on, the same class of bug would manifest as "tag verification fails on pages that were spilled-then-savepoint-rolled-back-then-reused". Worth having a test that combines savepoint rollback with spill under encryption, but no new design work needed — the fix is in the savepoint/cache bookkeeping, not in the codec.

---

## 6. Checkpoint

Checkpoint reads frames from WAL and writes them to the main DB file. For an encrypted DB, that means:

- WAL frame read → decrypt with key + frame-recorded IV → get plaintext page
- Plaintext page → encrypt with key + **fresh IV** → write to DB file

Using the same IV in the WAL and on the DB file would be fine cryptographically (same (key, IV, plaintext) produces the same ciphertext, no confidentiality loss) but would imply *copy the reserved tail verbatim from WAL frame to DB file*. That's simpler: one less IV draw, one less encrypt.

Caveat: the reserved tail in the WAL is part of the encrypted payload (because the WAL writes the full `p.data` which has the reserved bytes baked in). So "copy the reserved tail verbatim" isn't a separate operation, it's just a consequence of copying the ciphertext bytes.

**Simplest valid checkpoint path:** for each frame, read WAL bytes + verify HMAC/tag + write bytes unchanged to DB file. No re-encryption. The plaintext never materialises during checkpoint.

SQLCipher doesn't do this because its WAL-frame path applies encryption at a different layer, but any-store could. It's faster.

However: **if the WAL key and DB file key ever differ** (rekey scenarios, or if you want different IVs between WAL and DB file for defence-in-depth against IV reuse bugs), you need the decrypt-then-re-encrypt path. I'd build it that way from the start — the simplicity of "copy verbatim" is not worth the "oh no, rekey is hard now" later.

So: decrypt on WAL read → plaintext to DB write-path → encrypt on DB write. Same codec, potentially same IV, potentially fresh IV. Pluggable.

---

## 7. masterStore (InMemory mode)

`masterStore` is a `map[uint32][]byte` protected by `sync.RWMutex` (memory + pager.go:42–69). For InMemory DBs it replaces the file.

Two options:

**(A) Don't encrypt in-memory state.** The point of encryption is protection at rest. An InMemory DB has no "at rest". Skip the codec in this path. Means the codec hook has to be conditional on "backing is a file", which is a mild complication but doesn't hurt.

**(B) Encrypt it anyway, for uniformity.** Costs CPU for no security benefit. But keeps the code path identical to the file case — every read decrypts, every write encrypts. Easier to reason about: "pages are always encrypted when they cross the pager boundary".

I'd go with (A). There's no cryptographic reason to spend cycles encrypting memory that's already private to the process.

Either way: the masterStore stores whatever bytes the codec produces. If (A), plaintext. If (B), ciphertext. The storage layer doesn't care.

---

## 8. Key material & API

### 8.1 Where the key comes from

Today any-store opens with `Open(path, opts Options) (*DB, error)`. No key slot. The natural fit: add `Key []byte` and `KeyDerivation` options to `Options`. Validate at Open; if `Key` is empty, treat as plaintext DB (same as SQLCipher's "no key = vanilla SQLite"); if non-empty, install the codec.

SQLCipher's PRAGMA-based key injection (`PRAGMA key = 'pwd'` *after* open, before first page access) is a necessary hack because SQL-level access has to exist before the key is known. any-store has no SQL — users call `Open` with whatever configuration they want. The Options field is the right shape.

For raw-key vs passphrase handling, mirror SQLCipher:
- `Key` is 32 bytes → use directly as AES key (no KDF)
- `Key` is some other length → treat as passphrase, run PBKDF2-HMAC-SHA256 (or SHA512) with 256k iterations against the salt stored in page 1

A separate `Options.KDFIterations` knob lets tests use low iteration counts for speed.

### 8.2 The salt problem on first open

The chicken-and-egg: to decrypt page 1, we need the derived key; to derive the key, we need the salt; the salt is in page 1 plaintext. Solution (SQLCipher's): read 16 bytes from file offset 0 (or wherever we decided to put salt — §3 says bytes 72–87 of the header) before installing the codec, derive keys, then open normally. Done once, at Open.

For a new DB (file doesn't exist or is empty), generate a random salt, store it in the header, derive keys.

### 8.3 Rekey

Out of scope for a v1. If needed later, copy SQLCipher's model: `Rekey(newKey)` runs a write transaction, touches every page, commits. Salt stays the same.

---

## 9. Concrete problems I expect during implementation

1. **The FileChangeCount race against encryption.** Some part of the pager or reader-cache invalidation reads FileChangeCount (or `dataVersion` / `fileChangeCounter` — the memory mentions both) to detect cross-connection writes. If that read happens *through* the codec, it requires the key; if it happens *around* the codec (raw read of the header bytes), it requires the header to be plaintext. Option (A) in §3 keeps the header plaintext precisely to avoid this. If instead one decides to encrypt the whole page 1, the change-counter logic has to learn to decrypt-first, which means readers with a wrong key can't even detect staleness. Stick with plaintext-100-byte-header.

2. **Initial DB size inference.** The pager needs to know how many pages are in the file at open time. If it infers this from file size (`len / pageSize`), fine. If it reads `DatabaseSize` from the header, fine — header is plaintext under (A). If it computes by reading some btree metadata off page 1 after decrypt, then the key must be available before that. Sequence-of-init matters.

3. **HMAC on zero pages / past-EOF reads.** SQLCipher has the "if HMAC fails but the page is all zeros and autovacuum is on, treat as past-EOF read and succeed" exception at `sqlcipher.c:1686–1693`. any-store might hit a similar case during pager growth or freelist truncation. Need to figure out the cases during testing — not a blocker, but expect one or two pass-through conditions to discover.

4. **PRNG for IVs.** `crypto/rand` on Linux/Darwin/Windows delegates to the kernel; fine. But IV generation is per-page per-write, so commit of a 1 GiB transaction draws ~250k IVs. At 12 bytes per IV and Go's `rand.Read` being a syscall, that's measurable overhead. Mitigation: batch-draw IVs into a per-transaction pool, or use a reseeded CSPRNG (a ChaCha20 stream) in userspace seeded from `crypto/rand` once per transaction.

5. **Page cache invalidation when staleness check runs.** The `readerCaches` channel reuses pcaches across read transactions. If a reader's cache holds plaintext page N from transaction T1 and transaction T2 wrote a new version of N, the cache is invalidated via `dataVersion`/`walMaxFrame`/`fileChangeCounter` checks (memory: "Reader cache multi-process ABA: staleness check missed external writes…"). Encryption doesn't change these checks but does mean: when invalidation happens, the old plaintext pages in the cache are just dropped; their ciphertext on disk is unaffected. No extra logic.

6. **Overflow pages = overflow IVs.** A 100 MiB value stored via the overflow chain is 25,000 pages, each with its own IV + HMAC/GCM tag. CPU cost is the same as encrypting 100 MiB which GCM at AES-NI speeds is ~50 ms — fine. But the reserved-tail cost means you store ~2 MiB of IV+tag overhead for that value. Not a correctness problem, just size accounting.

7. **HMAC bound to page number, freelist reuse.** SQLCipher binds pgno into the HMAC to prevent page-shuffling attacks. Freelist reuse is fine — same page number, different content, different IV, different tag. No issue. But if any part of the code ever computed a tag over content *without* pgno binding and cached that tag across reuses, you'd get a collision vulnerability. Keep the binding, test that moving a page doesn't verify.

8. **Benchmarks will regress.** Current any-store commit-throughput is (per recent commits) optimised around buffer reuse, slab allocation, WAL-write elimination of intermediate buffers. Adding AES+HMAC per page lands *on the hot path*. Expect 20–40% throughput drop for small-value workloads and 5–10% for large-value workloads (where the fixed per-page overhead is amortised). Decide early whether encryption is a build flag (zero cost when off) or a runtime option (tiny branch cost when off). Build flag is cheaper; runtime option is easier to deploy.

9. **The crc32 import in page.go.** There's already a CRC32 on pages somewhere. Find it, understand whether it's also a per-page integrity check, decide whether it's redundant with HMAC/GCM-tag or if they protect different things. (Adding a third integrity mechanism on top of a WAL checksum and a page HMAC is a red flag.)

10. **Test suite needs property-based tampering tests.** SQLCipher's `sqlcipher-integrity.test` exercises bit-flipping every page position. Need the equivalent: write a test that does `file[random_byte] ^= 1` and verifies the next read raises an HMAC/tag error and not a garbage-out.

11. **Recovering from HMAC/tag failure is a policy question.** SQLCipher latches the codec into permanent error on tag failure. any-store could do the same, or could allow per-page skip (useful for salvage). Match SQLCipher's default (fail closed) and expose an explicit recovery mode only if needed.

12. **Go's `crypto/cipher` allocations.** `cipher.NewGCM(block)` is cheap (no allocation on hot path once the Block is made). `gcm.Seal(dst, nonce, plaintext, additionalData)` is allocation-free if `dst` has capacity. The encryption path should carry a per-page scratch buffer of exactly `pageSize` to `Seal` into, sized once at codec init. Same on the decrypt side with `Open`. Get this right or the GC pressure undoes the gains from the existing slab work.

---

## 10. Recommended shape (v1 plan)

If I were implementing this next week:

- **Cipher:** AES-256-GCM. Not CBC+HMAC. 12-byte nonce + 16-byte tag = 28 bytes per page, rounded up to 32.
- **KDF:** PBKDF2-HMAC-SHA256, 256k iterations, salt = 16 bytes in bytes 72–87 of dbHeader. Key length 32 bytes.
- **Reserve size:** 32 bytes (nonce + tag). Set in `dbHeader.ReservedSpace` at new-DB init.
- **Plaintext prefix on page 1:** the whole 100-byte dbHeader. From offset 100 onward is encrypted; reserved tail (last 32 bytes) holds nonce + tag. No change to header semantics.
- **Pages 2..N:** whole page is encrypted; last 32 bytes reserved hold nonce + tag.
- **Codec hooks:** one on `pager` (file read & checkpoint write), one on `wal` (frame read & frame write, including memFrames). Share one keyed `cipher.AEAD` (safe for concurrent use).
- **WAL frame checksum:** computed over ciphertext, unchanged call sites.
- **masterStore (InMemory mode):** skip encryption.
- **API:** `Options{ Key []byte, KDFIterations int }`. Zero-value `Key` = unencrypted (same file format as today; `ReservedSpace = 0`).
- **No rekey** in v1.
- **Backwards compatibility:** existing unencrypted DBs stay unencrypted — `ReservedSpace = 0` is the marker. A DB with `ReservedSpace > 0` is treated as encrypted and requires a key.

Rough effort: 3–5 days to MVP with tests; another week to harden against the problems in §9.

---

## 11. What not to take from SQLCipher

A few SQLCipher choices that are legacy and shouldn't be copied:

- **CBC mode.** Dates from pre-AES-NI era when GCM was slow. No reason to re-live that tradeoff in 2026.
- **Separate HMAC key with the 0x3a salt XOR.** The XOR-mask trick exists because HMAC + CBC need separate keys; with GCM the MAC is built in. Just have one key.
- **`cipher_compatibility = 1..4`.** any-store has one format. If/when it needs to evolve, version the dbHeader bytes; don't build a legacy matrix.
- **`cipher_migrate`.** Not needed without a multi-version history.
- **`cipher_plaintext_header_size`.** Unless a concrete use case asks for it, fix the plaintext header at 100 bytes and don't parameterise.
- **Passphrase storage (`cipher_store_pass`).** Deprecated in SQLCipher itself. Don't add.
- **`cipher_use_hmac = 0`.** Unauthenticated encryption is never the right default. Don't offer the knob.
- **Per-page cipher context alloc/free.** SQLCipher allocates `EVP_CIPHER_CTX` per page (OpenSSL), per page (CommonCrypto), per page (LibTomCrypt). Go's AEAD is one-time-setup, per-call stateless. Don't replicate the allocation pattern.
- **Shielding keys in memory with an XOR mask.** Useful in C with uncontrolled memory; marginal in Go where memory layout is GC-controlled. Not worth the ceremony.

---

## 12. Summary verdict

**Does SQLCipher's architecture wear well on any-store?** Yes, substantially better than on SQLite itself. The per-connection cache model, the WAL-only durability path, the absence of VACUUM/ATTACH/temp-tables all *remove* integration friction rather than adding it.

**What doesn't fit?** The "first 16 bytes of page 1 = salt" assumption (because any-store's header is richer) and the CBC+HMAC+PBKDF2-SHA1 / cipher_compatibility legacy baggage (not needed in a greenfield implementation).

**Real problems to watch?** (1) Page-1 header semantics vs encryption region; (2) the existing WAL checksum's relationship to the new AEAD tag. WAL spill is *not* a concern — it's inherited SQLite behaviour that SQLCipher already handles via the single-chokepoint codec rule; any-store just needs to follow the same discipline (all WAL writes go through one codec site, no bypasses).

**Recommended deviation from SQLCipher:** AES-256-GCM instead of AES-256-CBC+HMAC. Smaller reserve (32 vs 80), faster, simpler, one key instead of two, Go-stdlib-native. PBKDF2 for the user-key step, unchanged.

**Estimated MVP effort:** one person-week for a functioning encrypted write/read path with basic tests; a second week to cover the problems enumerated in §9.

---

## 13. Go-specific leverage

SQLCipher's codebase is shaped by three C-era constraints that don't apply to a Go implementation:

- **No stdlib crypto.** SQLCipher has to abstract over OpenSSL / CommonCrypto / LibTomCrypt because each platform ships a different library with a different API. The `sqlcipher_provider` struct (17 function pointers, `sqlcipher.h:59–91`) plus three ~300-line backends (`crypto_openssl.c`, `crypto_cc.c`, `crypto_libtomcrypt.c`) exist almost entirely to paper over those differences.
- **No GC.** Manual zeroing, reference-counted provider init/shutdown, `sqlcipher_shield` XOR masks to obscure in-memory keys, `sqlcipher_mlock` wrappers.
- **Pre-AEAD-era crypto choices.** AES-CBC + HMAC because portable GCM wasn't everywhere when SQLCipher 4.0 shipped. Separate HMAC key derivation with `HMAC_SALT_MASK = 0x3a` to keep CBC and HMAC keys distinct.

Go lets most of that infrastructure go away. Concrete wins:

### 13.1 Provider abstraction → single stdlib call

The entire `sqlcipher_provider` pattern is unnecessary. `crypto/aes` + `crypto/cipher` + `golang.org/x/crypto/pbkdf2` cover everything SQLCipher's three backends do, with one API, no init/shutdown, no mutex wrapping. Estimated replaced code: ~1100 lines of C provider glue → ~30 lines of Go setup.

```go
block, _ := aes.NewCipher(key)       // one-time setup
aead, _ := cipher.NewGCM(block)       // AEAD (GCM) on the same block
// aead is safe for concurrent use — share across all goroutines
```

No `get_cipher()`, `get_key_sz()`, `get_iv_sz()`, `get_block_sz()`, `get_hmac_sz()`, `ctx_init()`, `ctx_free()`, `fips_status()`, `get_provider_version()`, `add_random()`, `init()`, `shutdown()`. Those are all artefacts of having three backends to unify.

### 13.2 AES-NI is automatic

`crypto/aes` detects AES hardware acceleration at startup and dispatches to assembly (`src/crypto/aes/cipher_asm.go` in the Go tree, amd64/arm64/ppc64le/s390x). AES-256-GCM on a modern CPU runs at ~5–6 GB/s — faster than the SHA-512 portion of CBC+HMAC would be even with SHA-NI. So the performance argument for the GCM recommendation in §4.4 is strongest precisely on Go.

No `./configure` flags, no `-maes` compiler arg, nothing. You get it.

### 13.3 Zero-alloc hot path

`aead.Seal(dst, nonce, plaintext, additionalData)` returns a slice. If `cap(dst) >= len(plaintext) + aead.Overhead()`, Seal reuses the buffer without allocating. The same applies to `Open`. Combined with any-store's existing slab allocator for page buffers, the encrypt/decrypt path can be allocation-free:

```go
// scratch is a pre-allocated page-sized buffer from the slab
ct := aead.Seal(scratch[:0], nonce, plaintext, aad)
// ct is scratch[:len(plaintext)+16]; no new allocation
```

SQLCipher allocates an `EVP_CIPHER_CTX` per page (`crypto_openssl.c:299, 346`) because the OpenSSL API requires one. Go's AEAD is stateless per-call — one `cipher.AEAD` serves the whole DB's lifetime, shared by every goroutine.

### 13.4 Concurrent-safe AEAD → no mutexes

All of SQLCipher's provider-level mutexes (`SQLCIPHER_MUTEX_PROVIDER_RAND`, `SQLCIPHER_MUTEX_PROVIDER_ACTIVATE`, the RAND_bytes guard at `crypto_openssl.c:133`, the Fortuna lock at `crypto_libtomcrypt.c:143`) exist to paper over thread-unsafe library APIs. Go's `cipher.AEAD` is documented as safe for concurrent use when the same nonce is never reused (which any-store ensures by drawing a fresh nonce per write). Shared instance, no locks.

### 13.5 Type-safe buffer markers

In C you pass `unsigned char*` everywhere and rely on comments to know whether a buffer is plaintext or ciphertext. Go lets you encode that in the type:

```go
type plaintextPage []byte   // safe to hand to btree parser
type cipherPage    []byte   // only valid to write to disk / read from disk
```

The codec is `Encrypt(plaintextPage) cipherPage` and `Decrypt(cipherPage) (plaintextPage, error)`. Any attempt to pass a `cipherPage` to the cell parser is a compile error. SQLCipher has historically had bugs where journal frames were written through one codec mode and read through another — this class of bug is preventable at the type level in Go.

No runtime cost: named types erase to the same underlying `[]byte`.

### 13.6 Key material lifetime

Go doesn't have `sqlcipher_shield` / `sqlcipher_unshield` (XOR-mask keys while idle in memory). But:

- `defer` makes deterministic zeroing easy: `defer clear(key)` runs on function exit.
- `crypto/subtle.ConstantTimeCompare` is stdlib — use for tag verification to prevent timing oracles.
- `runtime.KeepAlive(key)` prevents the GC from freeing a key before the last use; useful around syscalls that take pointers.
- `golang.org/x/sys/unix.Mlock` wraps `mlock(2)` if you want to prevent swap (Linux-only, probably unnecessary).

The shield-mask pattern doesn't transfer well. It exists in SQLCipher because C code can leak keys into arbitrary heap locations that are hard to track; Go's escape analysis and controlled allocation make the attack surface smaller. Skip it.

### 13.7 Optionality: three patterns, ranked

**(a) Pointer-nil check on the codec (recommended).**

```go
type pager struct {
    codec *codec  // nil == not encrypted
}

// hot path
if p.codec != nil {
    p.codec.encrypt(dst, src, pgno)
} else {
    copy(dst, src)
}
```

One predictable branch per page I/O. CPU branch predictor hits it ~100% (always nil or always non-nil per DB instance). Cost on modern CPUs: ~0.5–1 cycle. Next to a ~1000-cycle AES-GCM operation, it's invisible. Next to a ~50ns `copy`, it's still invisible.

Runtime-configurable: one binary, users opt in per `Open()`. This is what you want 99% of the time.

**(b) Build tag (zero runtime overhead).**

```go
//go:build encryption
package btree
// includes codec plumbing

//go:build !encryption
package btree
// codec calls replaced with empty stubs, inlined away
```

The compiler deletes the codec field, the branch, and the call site entirely. Truly zero overhead. Cost: two build variants to test, two binaries to ship. Only worth it if (a) benchmarks show the branch is measurable (they won't), or (b) you want a hard assurance that an unkeyed binary *cannot* have a codec attached even by a compromised caller.

**(c) Interface with a no-op implementation.**

```go
type Codec interface {
    Encrypt(dst, src []byte, pgno uint32) ([]byte, error)
    Decrypt(dst, src []byte, pgno uint32) ([]byte, error)
}
```

Idiomatic Go but *worst* of the three for the always-off case — interface method dispatch is an indirect call that defeats inlining. The no-op implementation's body can't be inlined across the interface. Measurable overhead when encryption is off. Don't pick this unless you genuinely need pluggable alternative codecs.

**Pick (a).** Use (b) only if a specific threat model requires the binary-level guarantee.

### 13.8 Test infrastructure

Go's built-in tooling saves weeks of SQLCipher-equivalent test harness:

- **`go test -race`** catches data races in concurrent codec use.
- **`go test -bench`** with `testing.B` tracks regressions per-commit. Essential given the recent any-store commits have optimised for throughput — encryption overhead needs to be benchmarked not intuited.
- **`go test -fuzz`** on the decrypt path: feed random bytes, assert that failure is always `ErrTamper` and never a crash / UAF / panic. SQLCipher's equivalent is manual tamper tests in Tcl.
- **Standard `testing.T.Cleanup`** for test-key zeroing and tmpfile cleanup.

### 13.9 Error handling

SQLCipher's latching-error pattern (`ctx->error` in `codec_ctx`) is a workaround for C's lack of first-class errors. In Go, `ErrTamper` / `ErrKeyDerivation` / `ErrInvalidKey` as sentinel errors (`errors.Is`-friendly) give callers better recovery options without a global error latch. If you *do* want a latch, `atomic.Pointer[error]` on the pager is one line.

### 13.10 API surface

SQLCipher's public API is `PRAGMA key = '...'` because SQL is the only way to talk to an open DB. any-store has a Go API — use it idiomatically:

```go
type Options struct {
    // ... existing fields ...

    // Key enables page encryption when non-nil.
    // - len 32: raw AES-256 key, used directly
    // - other lengths: treated as a passphrase, run through PBKDF2
    // Zero value = no encryption.
    Key []byte

    // KDFIterations overrides the default (256k). Lower values
    // for tests only. Ignored when Key is a raw 32-byte key.
    KDFIterations int
}
```

No `PRAGMA key` drift. No "must call before first query" footgun. No `sqlite3_key_v2` multi-arity mess. Just struct fields.

For users who want to bring their own AEAD (HSM-backed, or ChaCha20-Poly1305 for some reason), expose an optional `Cipher cipher.AEAD` field that overrides the built-in AES-GCM. One extra field, zero extra code paths, infinite flexibility.

### 13.11 What not to do

A few patterns from the SQLCipher codebase that Go programmers should resist porting:

- **Provider struct with function pointers.** Use a concrete struct with methods, or skip the abstraction entirely.
- **Compile-time backend selection** (`SQLCIPHER_CRYPTO_OPENSSL` etc.). One binary, stdlib crypto.
- **Reference counting on init/shutdown.** `sync.Once` for any one-time setup; usually unnecessary.
- **Per-page `EVP_CIPHER_CTX_new` / free.** One `cipher.AEAD` per DB, reused.
- **Shield-mask XOR on in-memory keys.** Won't survive Go's GC relocation; doesn't prevent the attacks it's supposedly defending against.
- **`sqlcipher_malloc`-style private heap with bespoke zeroing.** `defer clear(buf)` on function exit, or a `zeroOnFree []byte` wrapper type if you want explicit sites.
- **Deferred key derivation after codec attach.** Go's `Open` can take the time to derive upfront; no need for the "PRAGMA key then first page I/O triggers derivation" dance.

### 13.12 Rough cost model

For a 4 KiB page with AES-256-GCM on AES-NI hardware:

| Operation | Estimated cost |
|---|---|
| AEAD Seal (encrypt + tag) | ~700 ns |
| AEAD Open (decrypt + verify) | ~700 ns |
| PBKDF2 key derivation (once at Open) | ~200 ms @ 256k iter |
| Nonce draw from `crypto/rand` (per page) | ~50 ns (amortised with batching) |
| Codec branch (nil check) | ~0.5 ns |

Commit of N dirty pages adds ~700 ns × N. For N = 100 that's 70 µs on top of whatever commit would take otherwise. For a fsync-bound commit (~1 ms for SSD, ~10 ms for spinning disk) this is noise. For an all-in-memory test, it's noticeable but not dominant.

The fsync-bound case is the key one: encryption on synced commits is effectively free. The regression will be most visible in:
- In-memory DBs (but §7 recommends skipping encryption there)
- No-sync configurations (`NoCommitSync`)
- Bulk-load benchmarks measuring pages/sec without fsync

Decide whether those cases matter for your user base before sweating the overhead.
