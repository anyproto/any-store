# SQLite C vs Go B-tree: Drift Analysis & Known Limitations

This document is a comprehensive comparison between the original SQLite C implementation
(btree.c, pager.c, wal.c, pcache.c, pcache1.c, btreeInt.h) and the Go B-tree
implementation in this package. Each drift is marked as:

- **Intentional** -- A deliberate design decision
- **Structural** -- An architectural difference
- **Missing** -- Feature not implemented
- **Divergent** -- Different approach to the same problem

---

## 1. Cell Format -- Leaf Cells

### SQLite (Index B-tree Leaf)

In `fillInCell()` (btree.c:7059), for index B-trees (`!pPage->intKey`):

```c
nSrc = nPayload = (int)pX->nKey;
pSrc = pX->pKey;
nHeader += putVarint32(&pCell[nHeader], nPayload);
```

Layout: `[varint(nPayload)] [payload_local] [4-byte overflow_ptr?]`

SQLite index B-trees are **key-only**: there is a single varint encoding the
total payload size, and the entire payload is treated as "the key". There is no
separate value field.

### Go (Leaf Cell)

In `writeLeafCell()` / `writeLeafCellOverflow()` (btree.go):

```go
pos += putVarint(buf[pos:], uint64(len(key)))
pos += putVarint(buf[pos:], uint64(len(value)))
copy(buf[pos:], key)
copy(buf[pos+len(key):], value)
```

Layout: `[varint(keyLen)] [varint(valLen)] [key||value] [4-byte overflow_ptr?]`

The Go implementation uses **two varints** (keyLen, valLen) because it implements
separate key/value semantics (e.g., `Put(docId, encodedDocument)`). Both lengths
are always fully on-page, so overflow detection remains I/O-free.

<a id="old-drift-leaf-cell-two-varint-keyvalue-format"></a>
### Drift

**Severity:** low

| Aspect | SQLite Index B-tree | Go Implementation |
|--------|-------------------|-------------------|
| Header varints | 1 (nPayload) | 2 (keyLen, valLen) |
| Value storage | No separate value | Separate key and value |
| Overflow blob | Single payload blob | Concatenated (key\|\|value) blob |
| Min cell overhead | 1 varint | 2 varints (1 extra byte for zero valLen) |

**Classification: Intentional** -- The Go implementation needs key/value semantics
that SQLite's index B-trees don't provide. The two-varint format adds minimal
overhead (1 byte for key-only entries where valLen=0).

---

## 2. Interior Cell Format

### SQLite (Index B-tree Interior)

Interior cells in SQLite index B-trees:

```
[4-byte leftChild] [varint(nPayload)] [key_local] [4-byte overflow_ptr?]
```

The single varint is `nPayload` (the total key size). Interior cells are key-only
(same as leaf cells for index B-trees). The overflow formula is the same as for
leaf cells, using `maxLocal`/`minLocal` from `BtShared`.

### Go (Interior Cell)

In `parseInteriorCell()` / `writeInteriorCell()` (btree.go):

```
[4-byte leftChild] [varint(keyLen)] [key_local] [4-byte overflow_ptr?]
```

Interior cells use a single varint (keyLen). No value is stored in interior cells.

---

## 3. Overflow / Local Payload Size Formula

### SQLite

From btree.c:3437-3440 and the surplus calculation at btree.c:1192-1204:

```c
pBt->maxLocal = (u16)((pBt->usableSize-12)*64/255 - 23);
pBt->minLocal = (u16)((pBt->usableSize-12)*32/255 - 23);
pBt->maxLeaf  = (u16)(pBt->usableSize - 35);
pBt->minLeaf  = (u16)((pBt->usableSize-12)*32/255 - 23);

// Surplus calculation:
minLocal = pPage->minLocal;
maxLocal = pPage->maxLocal;
surplus = minLocal + (pInfo->nPayload - minLocal) % (pBt->usableSize - 4);
if (surplus <= maxLocal) {
    pInfo->nLocal = surplus;
} else {
    pInfo->nLocal = minLocal;
}
```

SQLite has **two sets** of local payload limits:
- **Table B-trees** (intKey): use `maxLeaf` / `minLeaf`
- **Index B-trees** (non-intKey): use `maxLocal` / `minLocal`

### Go

From page.go:

```go
// index rule: interior cells, and leaf cells of schema format 5 files
func indexLimits(usableSize int) payloadLimits {
    return payloadLimits{usable: usableSize,
        maxLocal: (usableSize-12)*64/255 - 23, minLocal: (usableSize-12)*32/255 - 23}
}
// table-leaf rule: leaf cells of schema format 6 files
func tableLeafLimits(usableSize int) payloadLimits {
    return payloadLimits{usable: usableSize,
        maxLocal: usableSize - 35, minLocal: (usableSize-12)*32/255 - 23}
}
func localPayloadSize(totalPayload int, lim payloadLimits) int {
    if totalPayload <= lim.maxLocal { return totalPayload }
    surplus := lim.minLocal + (totalPayload-lim.minLocal)%overflowPageUsable(lim.usable)
    if surplus <= lim.maxLocal { return surplus }
    return lim.minLocal
}
```

<a id="old-drift-index-only-local-payload-no-maxleaf-minleaf"></a>
### Drift

**Severity:** low

| Aspect | SQLite | Go |
|--------|--------|-----|
| maxLocal / minLocal | Index leaves and index interior pages | Every interior page; leaf pages of schema format 5 files |
| maxLeaf / minLeaf | Table leaves | Every leaf page of schema format 6 files, index namespaces included |
| Where the pair is chosen | Per page, from its flag byte (`decodeFlags`, `btree.c:2043-2086`) | Per file, from the schema format (`pager.leafLimits`) |
| Surplus formula | `btreePayloadToLocal` | Identical (`localPayloadSize`) |

**Classification: Intentional.** Up to v2.4.x any-store used the index pair for every cell:
a leaf cell kept at most 997 bytes of `key||value` on a 4 KiB page with the default
16-byte checksum reserve, and a larger one kept a stub of 487 to 997 bytes (the surplus
formula) and put the rest on overflow pages that nothing shares, so a 1-4 KB document cost
a whole extra page. SQLite caps index cells that low because an index entry
is moved whole into an interior page when its leaf splits, and at least four cells must
fit there (`btreeInt.h:90-94`, `btree.c:3424-3436`). any-store never moves a leaf entry
up: on every tree the parent gets a key-only copy of the boundary key (`balance.go`,
deviation 1), written under the index pair. The cap therefore protects interior pages
only, and from schema format 6 every leaf cell uses SQLite's table-leaf pair
(`maxLeaf = usableSize - 35`, `btree.c:3439-3440`), on document trees and index trees
alike. Interior cells keep the index pair, so interior fanout stays at four or more.

Two things differ from SQLite's table trees. The pair is one per file, not one per page:
all pages keep the index page types (10 and 2), so the flag byte does not say which pair
a leaf uses and the schema format does. And an index leaf can now hold a key of up to
`usableSize - 35` bytes on the page while the copy of it in the parent spills at the
index limit.

Schema format 5 files keep the index pair on their leaves for good; nothing converts a
file in place. `TestLeafCodeTakesLimitsFromPager` keeps leaf code from computing a pair
itself. Regression tests: `internal/btree/table_leaf_format_test.go`.

---

## 4. Binary Search on Leaf Pages

### SQLite

In `sqlite3BtreeIndexMoveto()` (btree.c:6024-6203):

1. Fast path: 1-byte varint fits entirely on page -> compare directly
2. Fast path: 2-byte varint fits entirely on page -> compare directly
3. Slow path: overflow -> allocate buffer, call `accessPayload()` to read full key,
   then compare using `sqlite3VdbeRecordCompare()` (structured record comparison)

SQLite uses **record format comparison** (`xRecordCompare`), not raw byte comparison,
because index keys are serialized SQL records with type headers.

SQLite also has cursor position optimizations: if the cursor is already on the last
page, it can skip the root-to-leaf traversal.

### Go

In `searchLeafPage()` / `searchLeafWithOverflow()` (btree.go):

1. `searchLeafPage()`: Fast path for non-overflow cells. Reads keyLen and valLen
   varints, extracts key bytes, uses `bytes.Compare()`.
2. `searchLeafWithOverflow()`: Checks if `totalPayload > maxLocal`. If the key
   overflows:
   - First tries a **prefix comparison** using only the local key bytes
   - Only reads the full overflow key if the prefix comparison is inconclusive
   - Uses `leafFullKey()` to reconstruct the complete key from overflow pages

<a id="old-drift-binsearch-rawbytes-prefix-no-overflow-cache"></a>
### Drift

**Severity:** medium

| Aspect | SQLite | Go |
|--------|--------|-----|
| Comparison function | Record-format comparison (`xRecordCompare`) | Raw byte comparison (`bytes.Compare`) |
| Overflow handling | Always reads full key via `accessPayload()` | Prefix comparison first, then full read |
| Overflow page cache | `BtCursor.aOverflow[]` caches overflow page numbers | No overflow page cache |
| Cursor optimization | Skip-to-last-page optimization | `SeekNear` with leaf reuse |
| Two varints | Reads 1 varint (nPayload) | Reads 2 varints (keyLen, valLen) |

**Classification: Divergent** -- The Go implementation's prefix-comparison optimization
for overflow keys is novel and avoids I/O in many cases. SQLite always reads the full
key for overflow cells. However, SQLite's overflow page number cache (`aOverflow[]`)
amortizes repeated overflow reads on the same cursor, which Go lacks. The comparison
function itself is fundamentally different: SQLite compares structured records, Go
compares raw bytes.

---

## 5. Page Header Format

### Database File Header (First 100 bytes of page 1)

Both implementations follow the same 100-byte layout:

| Offset | Size | SQLite | Go |
|--------|------|--------|-----|
| 0 | 16 | `"SQLite format 3\000"` | `"any-store v2\000"`, zero-padded (reads also accept the legacy `"BTree format 1\000"`) |
| 16 | 2 | Page size | Page size |
| 18 | 1 | File format write version | File format write version |
| 19 | 1 | File format read version | File format read version |
| 20 | 1 | Reserved space | Reserved space |
| 21 | 1 | Max embedded payload frac (64) | Max embedded payload frac (64) |
| 22 | 1 | Min embedded payload frac (32) | Min embedded payload frac (32) |
| 23 | 1 | Leaf payload frac (32) | Leaf payload frac (32) |
| 24 | 4 | File change counter | File change counter |
| 28 | 4 | Database size in pages | Database size in pages |
| 32 | 4 | First freelist trunk page | First freelist trunk page |
| 36 | 4 | Total freelist pages | Total freelist pages |
| 40 | 4 | Schema cookie | Schema cookie |
| 44 | 4 | Schema format number | Schema format number |
| 48 | 4 | Default page cache size | Default page cache size |
| 52 | 4 | Largest root B-tree page (auto-vacuum) | 0 (unused) |
| 56 | 4 | Text encoding | Text encoding |
| 60 | 4 | User version | User version |
| 64 | 4 | Incremental vacuum mode | 0 (unused) |
| 68 | 4 | Application ID | Application ID |
| 72 | 16 | Reserved (zero) | KDF salt (16 B when encrypted; else zero) |
| 88 | 4 | Reserved (zero) | Reserved (zero) |
| 92 | 4 | Version-valid-for | Version-valid-for |
| 96 | 4 | SQLite version number | 1 (hardcoded) |

<a id="old-drift-db-file-header-magic-version-salt-vacuum"></a>
### Drift

**Severity:** low

| Aspect | SQLite | Go |
|--------|--------|-----|
| Magic string | `"SQLite format 3\000"` | `"any-store v2 fmt"` for schema format 6, which new files get; schema format 5 files carry `"any-store v2\000"` (an older `"BTree format 1\000"` is accepted and rewritten as it on the next commit). See [the header format gate](#drift-2026-11-10-1-header-format-gate) |
| Version number (offset 96) | SQLite library version (e.g., 3049000) | Hardcoded `1` |
| Auto-vacuum field (offset 52) | Largest root B-tree page number | Always 0 |
| Incremental vacuum (offset 64) | 0 or 1 | Always 0 |
| Payload fractions (offsets 21-23) | Always 64, 32, 32 | Always 64, 32, 32 |
| KDF salt (offset 72) | Reserved expansion (zero) | 16-byte PBKDF2 salt when encrypted (`codec_kdf.go`); zero otherwise |

**Classification: Intentional** -- The magic string is intentionally different to
prevent SQLite from opening Go databases and vice versa. The auto-vacuum fields are
unused because auto-vacuum is not implemented. Go repurposes 16 of SQLite's reserved
header-expansion bytes (72-87) to store the PBKDF2 salt for KDF-derived codecs; it is
load-bearing (encryption detection in `db.go`, preserved across header rewrites in
`pager.go`), not vestigial. Bytes 88-91 stay reserved/zero.

### B-tree Page Header

Both implementations use the same page header format:

| Offset | Size | Description |
|--------|------|-------------|
| 0 | 1 | Page type (2, 5, 10, 13) |
| 1 | 2 | First free block offset |
| 3 | 2 | Cell count |
| 5 | 2 | Cell content area offset |
| 7 | 1 | Fragmented free bytes |
| 8 | 4 | Right-most child (interior only) |

**No drift** -- The B-tree page header format is identical.

---

## 6. Schema Format Version

### SQLite

SQLite defines 4 schema format versions:
1. Format 1: Original format
2. Format 2: Adds DESC indexes
3. Format 3: Adds boolean storage
4. Format 4: Default since SQLite 3.3.0 -- adds key-prefix compression, other optimizations

### Go

The Go implementation creates files with `SchemaFormat = 6` (`maxSchemaFormat`, set in
`pager.go:newPage1()`); releases before v2.5.0 wrote 5.

```go
p.header = dbHeader{
    SchemaFormat: maxSchemaFormat, // 6
    versioned:    true,            // magic "any-store v2 fmt"
    ...
}
```

Format 5 introduced the unified leaf cell overflow format where key+value are treated
as a single contiguous payload blob for overflow purposes. Older Go databases (format <5)
are rejected at open time:

```go
if p.header.SchemaFormat != 0 && p.header.SchemaFormat < 5 {
    return nil, ErrOldFormat
}
```

### Go Schema Format History

| Version | Description |
|---------|-------------|
| 1-3 | Never existed in Go implementation |
| 4 | Original Go format: `[varint(keyLen)] [key] [varint(valLen)] [value]` -- only values could overflow |
| 5 | Unified overflow: `[varint(keyLen)] [varint(valLen)] [key\|\|value]` -- both keys and values can overflow |

<a id="old-drift-schema-format-5-custom-not-sqlite-fmt4"></a>
### Drift

**Severity:** low

**Classification: Intentional** -- Go's schema format is NOT SQLite's schema format
(1-4). It is the version of any-store's own on-disk format, and the Go implementation
never supported SQLite's formats.

| Schema format | Magic | Meaning | Written by |
|---|---|---|---|
| 5 | `any-store v2\0` (or the older `BTree format 1\0`) | Two-varint leaf cell with unified key/value overflow; index payload limit on every cell | v2.0.0 - v2.4.x |
| 6 | `any-store v2 fmt` | As 5, with SQLite's table-leaf payload limit on every leaf cell (see [the local-payload drift](#old-drift-index-only-local-payload-no-maxleaf-minleaf)). Above the btree, a save writes the planner's `stat_data` rows compressed in these files (tag `SK03`); format 5 files keep the `SK02` rows that v2.4.x reads | v2.5.0 and later |

A file keeps the format it was created with. Opening a format 5 file with a later release
reads and writes it under format 5 rules, including namespaces created afterwards. Open
refuses a format above the newest it knows (see
[the header format gate](#drift-2026-11-10-1-header-format-gate)).

---

## 7. WAL (Write-Ahead Log)

### WAL Header (32 bytes)

| Offset | SQLite | Go |
|--------|--------|-----|
| 0-3 | Magic: `0x377f0682` (LE cksum) or `0x377f0683` (BE cksum) | Magic: `0x42540601` (always BE cksum) |
| 4-7 | Version: `3007000` | Version: `1000000` |
| 8-11 | Page size | Page size |
| 12-15 | Checkpoint sequence number | Checkpoint sequence number |
| 16-19 | Salt-1 | Salt-1 |
| 20-23 | Salt-2 | Salt-2 |
| 24-27 | Checksum-1 | Checksum-1 |
| 28-31 | Checksum-2 | Checksum-2 |

### WAL Frame Header (24 bytes)

Both use the same 24-byte frame header layout with identical field positions.

### WAL Checksum Algorithm

Both use the same paired-word recurrence:
```
s1 += x[i]   + s2;
s2 += x[i+1] + s1;
```

SQLite supports both little-endian and big-endian word interpretation (selected by
the WAL magic number's LSB). Go always uses big-endian (the magic `0x42540601`
has LSB=1, indicating big-endian checksums).

The Go implementation includes an unrolled 8x (4-pair) fast path using `unsafe`
pointer arithmetic (wal.go).

<a id="old-drift-pagemap-same-process-lookup"></a>
**Severity:** low (same-process lookup: in-process WAL lookup via Go map (O(1)) vs SQLite hash scan; cross-process still uses SHM)
<a id="old-drift-heap-shm-single-process-fallback"></a>
**Severity:** low (heap SHM fallback (single-process) on platforms without mmap SHM; SQLite always mmaps SHM)
### WAL Index (SHM)

Both implementations use the same SHM layout:
- Region 0: WAL index header (two copies of WalIndexHdr + WalCkptInfo) + hash tables
- Region N: Hash tables mapping page numbers to WAL frame positions
- Hash table: 4096 page-number entries + 8192-slot linear-probing hash (prime 383)

| Aspect | SQLite | Go |
|--------|--------|-----|
| SHM format | mmap'd file, native byte order | mmap'd file on `(linux \|\| darwin) && (amd64 \|\| arm64)`, heap on all other platforms (windows, wasm/js, the BSDs, non-amd64/arm64 arches) |
| Lock protocol | POSIX fcntl locks on SHM | Same for mmap; no-ops for heap mode |
| Same-process lookup | Iterates `aSegment[]` in `walIterator` | Uses in-process Go map (`pageMap`) |
| Cross-process lookup | Hash table scan (`walHashGet`) | `shmHashGet()` for cross-process, Go map for same-process |
| WalIndexHdr | 48 bytes, native byte order | 48 bytes, little-endian (matching x86 native) |
| WalCkptInfo | nBackfill + aReadMark[5] + aLock[8] + nBackfillAttempted | Same layout at same offsets |
| Reader slots | 5 (WAL_NREADER) | 5 (`aReadMark[5]`) |

### Checkpoint Modes

Both support: PASSIVE, FULL, RESTART, TRUNCATE with identical semantics.

<a id="old-drift-wal-magic-version-be-checksum"></a>
**Severity:** medium (distinct WAL magic/version + BE-only checksums; prevents cross-tool open, simplifies impl)
<a id="old-drift-wal-undo-via-pager-rollback"></a>
**Severity:** low (no sqlite3WalUndo; rollback resets wal.nFrame to savedWalFrame + rollbackToFrame trims pages)
### Drift Summary

| Aspect | Classification |
|--------|---------------|
| Different magic number | **Intentional** -- prevents cross-tool opening |
| Different version number | **Intentional** -- distinct format lineage |
| Big-endian only checksums | **Intentional** -- simplifies implementation |
| Go map for same-process reads | **Divergent** -- O(1) map vs hash table scan |
| Heap SHM fallback | **Divergent** -- enables non-mmap platforms |
| WAL undo (`sqlite3WalUndo`) | **Missing** -- Go uses pager-level rollback instead. The rollback clears the whole writer cache, where `pagerRollbackWal` reloads only logged and dirty pages (`pager.c:3147-3166`): a clean page 1 is read again by the next transaction, and in a schema format 6 file that sets the change counter's `dbFileVers` (2026-11-12, [drift-77](#drift-77-filechangecount-bumped-conditionally-not-unconditionally)) |
| First-opener `ftruncate(shm, 3)` marker | **Aligned (2026-07-10)** -- `newPlatformShm` now runs SQLite's full DMS first-attacher election (`unixLockSharedMemory`, os_unix.c:4860-4913, 3.52.0): F_GETLK probe → three-way branch (F_UNLCK: WRLCK + UNCONDITIONAL truncate-to-3 + atomic downgrade to RDLCK; F_RDLCK: join shared; F_WRLCK: back off BUSY, never join — the os_unix.c:4864-4871 race). The `Size()==0` gate is gone; a crash-persisted stale shm is physically reset by the first attacher and `recoverLocked` rebuilds from the WAL. See [§drift-2026-06-25-25](#drift-2026-06-25-25-first-opener-does-not-reset-a-stale-persisted-shm-trusts-checksum-vali) (RESOLVED) and item 2 below |
| Orphan-inode handling on open race | **Aligned** -- DB-file flock serializes last-client-unlink (see §SHM open/close protocol drift) |
| Directory sync at a WAL descriptor's first sync (`UNIXFILE_DIRSYNC`) | **Aligned (2026-11-21)** -- the port gap is closed: `wal.open` opens the WAL with the create flag and sets the descriptor's `dirSync`, and its first sync (`wal.syncFile`, from the header write, a commit or a checkpoint) syncs the directory after the file, once, as `unixOpen` marks every WAL it opens with `SQLITE_OPEN_CREATE` (`os_unix.c:6569-6573`, `6777`; `sqlite3WalOpen`, `wal.c:1719-1720`) and `unixSync` consumes the mark (`os_unix.c:3936-3954`). So the WAL's name, and a creation journal's delete before it, are durable before a commit to the WAL is. The directory is that of the name the WAL was opened by, which is built from the database's full name, resolved once at open (`FullPathname`, as `pager.c:4792`, `4929`; `databaseDir`, as `openDirectory`, `os_unix.c:3874-3893`). Unix and wasip1; none on Windows and js/wasm. See [drift-63](#drift-63-new-db-page-1-written-directly-to-file-bypassing-wal) |

### SHM open/close protocol drift (resolved 2026-04-21)

`shm_mmap.go:newPlatformShm` and `mmapShm.close` implement the POSIX
dead-man-switch (DMS) fcntl protocol for shm-file lifecycle. They used to
diverge from SQLite on three points; two are now aligned, one remains
intentional.

1. **Last-client-unlink serialization — now aligned with SQLite.** Previously
   we used a 50× inode-verify retry loop in `newPlatformShm` as a local
   substitute for SQLite's DB-file-lock invariant. We now adopt the SQLite
   approach: `pager.open` takes a shared `flock` on the DB file (held for
   the pager's lifetime); `pager.close` upgrades to exclusive to prove "last
   client" and passes that result to `shm.close(isLastClient)`. Any new
   opener blocks on shared DB-file acquisition while a closer holds
   exclusive — serializing shm unlink against new opens exactly as SQLite
   does in `wal.c:2487-2551` (`sqlite3WalClose`).

   **Order (2026-11-19).** The upgrade is the first step of the close, as
   `sqlite3OsLock(pWal->pDbFd, SQLITE_LOCK_EXCLUSIVE)` is in `sqlite3WalClose`
   (`wal.c:2522-2523`); the PASSIVE checkpoint (`wal.c:2528-2530`) and the
   truncate (`wal.c:2531-2550`: the WAL is deleted unless it is persistent, `wal.c:2536-2540`, and a
   persistent one is truncated to zero only with a non-negative size limit, `else if( pWal->mxWalSize>=0 )`,
   `wal.c:2541-2548`; here the WAL is always truncated and never deleted,
   [drift-55](#drift-55-wal-file-truncated-but-never-unlinked-on-last-client-close)) run only under it, and a close that does not
   get the lock runs neither. `pager.close` used to checkpoint first, then
   upgrade, then truncate on the checkpoint's result — with the write lock
   or, when a peer's transaction outlasted the busy timeout, without it. A
   peer that committed after the checkpoint's last look at the log and whose
   process ended before the upgrade (no Close, so no checkpoint of its own)
   had its commit truncated away. Under the lock no other process is
   attached, so nothing can be committed between the checkpoint and the
   truncate. Consequences of SQLite's order: a close that is not the last no
   longer checkpoints (the processes still attached do), it no longer waits
   for a busy write lock, and the last close holds the file exclusively for
   the length of its checkpoint, during which an opener retries its shared
   lock (`pager.open`, 100 × 10 ms, then an error) where SQLite's opener
   goes through its busy handler. Under the lock the close still takes
   `WAL_WRITE_LOCK` (free by then) where SQLite switches to exclusive
   locking mode (`wal.c:2525-2527`), and truncates only if it got it. Tests:
   `TestCloseNotLastClient_LeavesWAL`,
   `TestCloseLastClient_KeepsCommitBeforeLock` (`-tags vfs`, two processes).

   See `internal/btree/dbfile_lock_unix.go` for the flock wrappers. The
   inode-verify retry loop in `newPlatformShm` is gone — a single-shot
   `osOpenFile` + `F_RDLCK(DMS)` now suffices.

   **Simplification vs. SQLite:** SQLite uses byte-range fcntl locks with a
   5-state protocol (NO/SHARED/RESERVED/PENDING/EXCLUSIVE) because it must
   also support rollback-journal mode. We only support WAL mode, so we use
   the 3-state subset we actually need (none / shared / exclusive) and BSD
   whole-file `flock` instead of fcntl byte-range locks — `flock` is
   per-file-description, dodging POSIX fcntl's "close any fd releases all
   inode locks" gotcha when multiple goroutines open the same DB path in a
   single process.

2. **`robust_ftruncate(hShm, 3)` marker — fully aligned 2026-07-10.**
   `newPlatformShm` runs the complete `unixLockSharedMemory` first-attacher
   election (`os_unix.c:4860-4913`, 3.52.0): an `F_GETLK` probe of the DMS
   byte, then a three-way branch — `F_UNLCK`: take the DMS EXCLUSIVE,
   truncate to 3 bytes UNCONDITIONALLY, atomically downgrade to SHARED;
   `F_RDLCK`: join shared (live peers maintain the shm); `F_WRLCK`: back off
   BUSY and re-probe, never join (C documents at `os_unix.c:4864-4871` that
   joining here re-opens the stale-shm window when the exclusive holder dies
   pre-truncate). The marker is smaller than `walIndexHdrSize` (48 bytes),
   and `region()`'s re-growth zero-fills, so `readHeader` can never validate
   crash-stale content and `recoverLocked` rebuilds from the WAL. The DMS
   SHARED lock is held for the attachment's lifetime; last-client unlink
   remains at the DB-file flock (item 1).

   *History:* first landed 2026-04-22 gated on `Size()==0` (fresh-create
   only); reopened 2026-06-25 because a crash-persisted stale shm has
   `Size()>0` and was adopted as-is; resolved 2026-07-10 by the election —
   see the first-opener stale-shm drift
   (#drift-2026-06-25-25-first-opener-does-not-reset-a-stale-persisted-shm-trusts-checksum-vali).

3. **Close ordering — aligned with SQLite.** Both implementations unlink
   before closing the shm fd (SQLite: `os_unix.c:5538-5541`; ours:
   `shm_mmap.go` `mmapShm.close`). With the DB-file lock now serializing
   at the outer layer (item 1), this is belt-and-suspenders rather than
   load-bearing.

---

## 8. Pager Layer

### SQLite Pager States

SQLite pager has 7 states:
```c
#define PAGER_OPEN                  0
#define PAGER_READER                1
#define PAGER_WRITER_LOCKED         2
#define PAGER_WRITER_CACHEMOD       3
#define PAGER_WRITER_DBMOD          4
#define PAGER_WRITER_FINISHED       5
#define PAGER_ERROR                 6
```

### Go Pager States

```go
const (
    pagerOpen   pagerState = iota  // 0
    pagerReader                     // 1
    pagerWriter                     // 2
    pagerError                      // 3
)
```

<a id="old-drift-pager-state-machine-simplified"></a>
**Severity:** low (WAL-only 4-state pager vs SQLite 7-state; no rollback journal / on-disk subjournal)
### Feature Comparison

| Feature | SQLite | Go |
|---------|--------|-----|
| Pager states | 7 (granular writer sub-states) | 4 (simplified) |
| Journal modes | DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF | WAL only |
| Rollback journal | Full implementation | For the creation of a database only: a header with no page records ([drift-2026-11-21-7](#drift-2026-11-21-7-creation-journal)) |
| Lock escalation | SHARED -> RESERVED -> PENDING -> EXCLUSIVE | WAL write lock only |
| Multi-process support | Full (via file locks + SHM) | mmap-backed SHM on `(linux \|\| darwin) && (amd64 \|\| arm64)`; heap fallback (single-process) elsewhere |
| Hot journal rollback | Automatic recovery | At open, for an interrupted creation: the file is truncated to nothing and created again |
| Sub-journal (savepoint journal) | Written to disk | In-memory page copies |
| NOCONTENT optimization | `PAGER_GET_NOCONTENT` flag | `getPageNoContent()` |
| DONT_WRITE flag | `PGHDR_DONT_WRITE` in page flags (no effect in WAL mode: only `pager_write_pagelist` reads it) | None — frames are written at spill and commit |
| Page size changes | `sqlite3BtreeSetPageSize()` | Fixed at open time |
| Deferred durability | `PRAGMA synchronous=NORMAL` | `NoCommitSync` option |
| In-memory databases | `:memory:` or `PRAGMA journal_mode=MEMORY` | `InMemory` option with `memFrames` WAL |
| Write-transaction page map | Pages in cache with `PGHDR_DIRTY` | Writer uses `writerCache` dirty list — matches SQLite |

**Classification: Structural** -- The Go pager is WAL-only and significantly simpler.
The main missing features are: rollback journal (DELETE/TRUNCATE/PERSIST modes),
fine-grained writer state machine (4 writer sub-states collapsed to 1), and on-disk
sub-journaling for savepoints.

---

## 9. Page Cache and Memory Management

### SQLite (pcache.c / pcache1.c)

- Two-layer design: generic `pcache.c` interface + pluggable `pcache1.c` default
- LRU eviction with configurable maximum cache size
- Reference counting with pinning
- `purgeable` flag to disable eviction
- Global slab allocator (`pcache1_g`) pre-allocates page buffers at init time
  (`sqlite3PCacheBufferSetup`, `pcache1.c:271-291`)
- Per-cache bulk allocation (`pFree` list, `pcache1InitBulk`, `pcache1.c:297-330`)
- `underPressure` flag when slab free list drops below reserve (`pcache1.c:350,389`)
- Admission control via `createFlag` (0=lookup, 1=soft, 2=hard; `pcache1.c:881-892`)
- `nRecyclable` count of unpinned clean pages in LRU (`pcache1.c:197`)
- Immediate eviction on unpin when overfull (`pcache1Unpin`, `pcache1.c:1094-1095`)
- LRU insert at HEAD (MRU), evict from TAIL (LRU) (`pcache1.c:1098-1101`, `623-624`)
- Dirty page move-to-front on unpin (`pcache.c:558`, `PCACHE_DIRTYLIST_FRONT`)
- Persistent cache across transactions (`pagerBeginReadTransaction`, `pager.c:3246-3267`)
- `PGHDR_DONT_WRITE`, `PGHDR_NEED_SYNC`, `PGHDR_MMAP`, `PGHDR_WAL_APPEND` flags
- Supports shared cache mode (multiple connections sharing one BtShared)

### Go (pcache.go, page_slab.go, db.go)

- Single-layer design, no mutex (each cache is single-goroutine owned)
- Per-connection caches: writer has `writerCache`, each reader gets a private
  cache from `sync.Pool` (`readerCachePool`)
- **Global slab allocator** (`page_slab.go`): process-global `pageSlab` singleton
  allocates one buffer and carves it into page slices. `Get()` pops from the free
  list, falls back to the `sync.Pool`, then `make()` (overflow). `Put()` returns
  slab buffers to the free list and overflow buffers to the `sync.Pool`.
  `UnderPressure()` atomic flag when the free list drops below `nReserve`
  (10% + 1 of slab size)
- **Per-cache bulk allocation** (`pcache.initBulk`): first `create()` call
  pre-allocates up to 20 page structs with data buffers from the heap
- **LRU**: insert at HEAD (`lruPrepend`), evict from TAIL (`evictOne`). Correct
  LRU semantics — most recently released at head, least recently used at tail
- **Dirty list move-to-front**: `dirtyMoveToFront()` on unpin ensures recently
  released dirty pages are preserved while stale ones are spilled
- **Admission control**: `create(pgno, createFlag)` — soft creates (readers,
  `createFlag=1`) return nil when 90% of cache is pinned or when slab is under
  pressure with low recyclable ratio. Hard creates (writers, `createFlag=2`)
  always proceed
- **`nRecyclable`**: count of unpinned clean pages in LRU, incremented in
  `lruPrepend`, decremented in `lruRemove`/`evictOne`
- **Immediate eviction on unpin**: when cache is overfull (`nPage > maxPages`),
  clean pages are discarded on `release()` instead of entering LRU. Matches
  SQLite `pcache1Unpin` (`pcache1.c:1094`): `pGroup->nPurgeable > pGroup->nMaxPage`
- **Page-cache hash**: pages are found via a chained hash table (`apHash []*page`
  + `page.hashNext`), a direct port of SQLite's `PCache1.apHash` (`pcache1.c:200`).
  Membership is carried on the page (`page.inCache`), so `release()` gates the LRU
  insert on a field read rather than a second hash probe (drift #2 resolved; see
  below)
- **Buffer lifecycle**: `clear()`, `discard()`, `truncate()` return data buffers
  to the global slab. `pFree` buffers also returned to slab on `clear()`
- **Persistent reader cache**: reader caches are returned to `readerCachePool`
  on `Rollback()` with pages intact. On next `BeginRead()`, cache is cleared
  only if `dataVersion` or `walMaxFrame` changed (dual-check to avoid ABA from
  checkpoint restart and TOCTOU race)
- **Max concurrent readers**: `readerSem` channel with capacity `MaxReaders`
  (default 4) limits concurrent read transactions per DB. Bounds memory from
  persistent reader caches. A wait ends with a slot, with the caller's ctx
  (`BeginReadContext`) or with DB close (`closeCh`)
- Pin counting (similar to SQLite's reference counting)
- `purgeable` flag matching SQLite's `pcache1.bPurgeable`
- `page.cache` backpointer for routing releases to the correct cache
- `ConfigPageCache(pageSize, nPages)` public API to pre-initialize the global
  slab (mirrors `sqlite3_config(SQLITE_CONFIG_PAGECACHE)`)

### Memory Bound

Per-DB reader cache memory: `MaxReaders × readerCacheSize × pageSize`.
Example: 4 readers × 50 pages × 4096 bytes = 800KB per DB.
With 200 open DBs: 200 × 800KB = ~156MB reader cache total.
Writer caches (1 per DB): bounded by `CacheSize × pageSize` per DB, with buffers
drawn from the global slab that enforces a process-wide soft cap.

<a id="old-drift-pcache-dirty-list-unsorted-wal-write"></a>
**Severity:** low (dirty list written to WAL unsorted (MRU->LRU); SQLite pgno-sorts. Harmless: WAL frames are pgno-addressed -- see "Dirty-list order" row)
### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| Architecture | Two-layer (pluggable `pcache2` interface) | Single-layer (no vtable, drift #6) |
| Page struct | Two structs: `PgHdr` (generic) + `PgHdr1` (pcache1-specific) | Single `page` struct (drift #5) |
| Spill victim search | `pSynced` + `pDirtyTail` two-pass (prefers non-`NEED_SYNC`) | `dirtyTail` single-pass; no `pSynced` in WAL-only (drift #19) |
| Dirty-list order | `sqlite3PcacheDirtyList` returns pgno-sorted (`pcache.c:783-818`) | `dirtyPages` walks MRU→LRU, unsorted; WAL frames written in that order (no correctness impact — frames are pgno-addressed; page-1-first holds incidentally since page 1 is dirtied last) |
| Slab allocator | Contiguous `void*` buffer, pointer arithmetic (`pcache1.c:283-288`) | One contiguous `[]byte` carved into page-capped three-index slices; the free list is a `[][]byte` stack rather than a list threaded through the slots |
| Slab buffer return | Range check `SQLITE_WITHIN` (`pcache1.c:381`) | Range check against the slab's backing array (`pageSlab.within`); overflow buffers recycle via `sync.Pool` where SQLite frees them; a slab buffer arriving at a full list is dropped where C asserts |
| Slab init | Library init `pcache1Init` (`pcache1.c:695-741`) | Explicit `ConfigPageCache()` before `Open`; `Open` with `UsePageSlab` fails with `ErrPageSlabNotInitialized` otherwise (drift #9) |
| Bulk alloc | Contiguous `pBulk` carved into slots (`pcache1.c:312-327`) | Individual page structs with slab buffers (drift #10) |
| Page flags | Bitmask on each page | None (`hasContent` mirrors `BtShared.pHasContent`, not a page flag) |
| Cache ownership | Per-connection (private) | Per-connection (private) — matches SQLite |
| Thread safety | Per-connection (no mutex needed) | Per-connection (no mutex needed) — matches SQLite |
| PGroup cross-cache stealing | Enabled in single-thread mode (`pcache1.c:718-719`) | No PGroup; each cache isolated (drift #1) |
| Hash table | `apHash[]` with chaining (`pcache1.c:199-200`) | `apHash []*page` with chaining via `page.hashNext` — matches SQLite (drift #2 resolved 2026-05-22; see "Page-Cache Hash Table" below) |
| LRU structure | Circular list with anchor node (`pcache1.c:112-115`) | Doubly-linked list with head/tail pointers (drift #3) |
| `createFlag=0` | Lookup only, no create | Dropped; `fetch()` handles lookup-only (drift #13) |
| Max page check | PGroup-level (`pcache1.c:1094`) | Per-cache + global slab pressure (drift #14) |
| Persistent cache staleness | File change counter read from DB file (`pager.c:5410-5418`) | `dataVersion` counter + `walMaxFrame` dual check (drift #11) |
| Max concurrent readers | N/A | `readerSem` channel (Go-specific addition) |
| pcache struct recycling | `malloc`/`free` | `sync.Pool` (drift #12) |

**Classification: Structural** -- The Go pcache now closely mirrors SQLite's pcache1.c
memory management model (slab allocator, bulk alloc, LRU ordering, admission control,
buffer recycling) while keeping Go-idiomatic data structures. The persistent reader
cache and max-readers limiter are additions for the many-open-databases scenario.

---

## 10. Freelist Management

### SQLite

SQLite uses a trunk/leaf freelist format (btree.c):
- Trunk page: `[4B next_trunk] [4B leaf_count] [4B leaf_pgno * N]`
- Max leaves per trunk: fills to `usableSize/4 - 8` (`freePage2`, btree.c:6913), reserving 6 slots for pre-3.6.0 backward compat; the *corruption ceiling* is `usableSize/4 - 2`
- Pages freed via `freePage2()` which handles auto-vacuum pointer-map updates
- `btreeSetHasContent()` / `btreeGetHasContent()` with `BtShared.pHasContent` bitvec
- `PGHDR_DONT_WRITE` flag on freed leaf pages

### Go

The Go implementation uses the same trunk/leaf format (pager.go):
- Same layout: `[4B next_trunk] [4B leaf_count] [4B leaf_pgno * N]`
- Fills trunks to `(usableSize - 8) / 4` == `usableSize/4 - 2` (SQLite's corruption ceiling), **not** SQLite's conservative `usableSize/4 - 8` — see Drift
- `hasContent` map (replacing SQLite's bitvec) for savepoint-safe freelist reuse
- No `PGHDR_DONT_WRITE` equivalent: in WAL mode the flag has no effect (only the rollback-journal writer reads it; frames are written at spill and commit)
- Bounds validation on trunk/leaf page numbers

<a id="old-drift-freelist-trunk-fill-corruption-ceiling"></a>
**Severity:** medium (trunk fills to usableSize/4-2 (corruption ceiling) not SQLite's conservative -8; pre-3.6.0 readers flag it corrupt)
### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| Freelist format | Trunk/leaf linked list | Identical trunk/leaf linked list |
| hasContent tracking | Bitvec (`pHasContent`) | Go map (`hasContent`) |
| dontWrite flag | Page header flag (`PGHDR_DONT_WRITE`), no I/O effect in WAL mode | None |
| Auto-vacuum integration | Pointer-map page updates during free/alloc | Not applicable |
| Leaf-first allocation | Prefers leaf pages from most-recent trunk | Same behavior |
| Trunk fill bound | Fills to `usableSize/4 - 8` (6 slots reserved for pre-3.6.0 compat) | Fills to `usableSize/4 - 2` (the corruption ceiling) — 6 more leaves/trunk; format drift vs SQLite <3.6.0 only (3.6.0+ accepts it) |

**Classification: Intentional (format) / Divergent (trunk fill)** -- The format is identical
and implementation details differ harmlessly (map vs bitvec). One behavioral drift: any-store
packs freelist trunks to the corruption ceiling (`usableSize/4 - 2`) rather than SQLite's
conservative `usableSize/4 - 8`, so trunks it writes carry up to 6 more leaves than SQLite
would — readable by SQLite 3.6.0+ but flagged corrupt by pre-3.6.0. The auto-vacuum
pointer-map integration is absent because auto-vacuum is not implemented.

---

## 11. Overflow Chain Read/Write

### SQLite (`accessPayload()` in btree.c:5121)

- Reads overflow chains via cursor-attached `aOverflow[]` cache
- Supports both read and write operations (`eOp` parameter)
- Can skip directly to a specific overflow page using the cached page numbers
- Supports `SQLITE_DIRECT_OVERFLOW_READ` optimization (bypass page cache for reads)
- Offset-based partial reads supported natively

### Go (pager.go)

Three functions:
- `writeOverflowChain()`: Allocates pages, writes data sequentially
- `readOverflowChainAt()`: Reads via writer cache (`getPageWriter`) -- for writers
- `readOverflowChainReader()`: Reads via reader's private cache (`getPageReader`) -- for readers

Key features:
- **Per-connection cache isolation**: Readers use their own private cache for overflow
  reads, matching SQLite's per-connection model
- **Circular chain protection**: `maxIter` counter prevents infinite loops on
  corrupt circular overflow chains
- **Bounds checking**: Overflow page numbers validated against `dbSize`
- No overflow page number cache (unlike SQLite's `aOverflow[]`)
- Offset-based partial reads supported via `readOverflowAt` (skips whole pre-offset
  pages by following only the next-pointer); the chain readers
  `readOverflowChainAt`/`readOverflowChainReader` read from the start

<a id="old-drift-no-aoverflow-page-cache"></a>
**Severity:** low (no aOverflow[] cursor cache; overflow chains re-walked from start each read (perf, not correctness))
### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| Overflow page cache | `aOverflow[]` on cursor | None |
| Partial reads | Offset-based reads supported | Offset reads via `readOverflowAt`; chain readers (`readOverflowChainAt/Reader`) read from start |
| Cache isolation | Per-connection caches | Per-connection caches — matches SQLite |
| Direct overflow read | Bypass page cache for large reads | Not implemented |
| Circular chain protection | Trusts page structure | `maxIter` counter + bounds checking |
| Write support | `eOp=1` writes through overflow chain | Separate `writeOverflowChain()` |

**Classification: Divergent** -- The Go implementation matches SQLite's per-connection
cache model for overflow reads. It lacks the overflow page cache (`aOverflow[]`), which
means repeated reads of the same overflow cell are more expensive.

---

## 12. MVCC / Snapshot Isolation

### SQLite

WAL mode provides snapshot isolation:
1. Reader records `mxFrame` at transaction start
2. `sqlite3WalFindFrame()` only returns frames <= `mxFrame`
3. Pages not in WAL (within snapshot) are read from the database file
4. Writer appends new frames beyond `mxFrame`; readers don't see them
5. Checkpoint copies frames to database file; old readers continue seeing old data

SQLite relies on the WAL frame lookup being bounded by `mxFrame`. The page cache
holds "current" versions, and WAL provides the historical view.

### Go

The Go implementation uses per-connection page caches matching SQLite's model:

1. **`walMaxFrame` per transaction**: Each read transaction records its snapshot point
2. **Per-connection reader caches**: Each reader gets a private `pcache` from a pool,
   pages are cached across lookups within the transaction and recycled on rollback
3. **`getPageReader()`**: Checks reader cache, validates against WAL index, reads from
   WAL/disk/masterStore on miss, populates cache for subsequent accesses
4. **Writer cache**: Writer uses `writerCache.fetch()` directly to see its own dirty
   pages — same as SQLite's per-connection PCache approach
5. **Cache staleness check**: When a cached page has been updated beyond the reader's
   snapshot (`latestFrame > walMaxFrame`), the reader evicts the stale entry and
   re-reads from WAL/disk

### Invariant

Readers use their private cache via `getPageReader()`. Overflow pages use
`readOverflowChainReader()`. The writer uses `writerCache` via `getPageWriter()`.
Each goroutine accesses only its own cache — no mutex needed.

---

## 13. Corruption Protection

### SQLite

SQLite uses extensive bounds checking throughout cell parsing:
- `SQLITE_CORRUPT_PAGE(pPage)` and `SQLITE_CORRUPT_BKPT` macros
- Cell size validated against page boundaries
- Key size validated: `nCell/pCur->pBt->usableSize > pCur->pBt->nPage` check
- `BT_MAX_LOCAL` constant: `65501` (65536 - 35) as absolute maximum
- `BTCURSOR_MAX_DEPTH`: 20 levels maximum tree depth
- `sqlite3FaultSim()` for testing error paths

### Go

The Go implementation has:
- `maxPayloadAlloc = 1 << 30` (1 GB) as a safety cap on varint-decoded sizes
- `ErrCorrupt` returned on all bounds violations
- `maxKeySize = 1 << 30` (1 GB) limit on key sizes
- `btCursorMaxDepth` for tree depth limiting (matching SQLite)
- Varint safety: `getVarintSafe()` checks buffer bounds
- Overflow chain: `maxIter` counter and page number bounds checking
- Freelist: leaf count and page number validation

### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| Max local payload | `BT_MAX_LOCAL = 65501` | `payloadLimits.maxLocal` computed from usableSize: the index limit for interior cells, the file's leaf limit (`pager.leafLimits`) for leaf cells |
| Max total payload | Validated against `nPage * pageSize` (`sqlite3BtreeMaxRecordSize`) | `maxPayloadAlloc = 1 << 30` in the parsers and for writes; `payloadInFile` (`nPage * pageSize`) before a reader allocates |
| Approach | Per-access bounds checking with macros | Per-access bounds checking with error returns |
| Overflow protection | Trust chain structure | `maxIter` + bounds checks |
| Max tree depth | `BTCURSOR_MAX_DEPTH = 20` | `btCursorMaxDepth` (same value) |

<a id="old-drift-corruption-fixed-1gb-payload-cap-vs-npage-validation"></a>
**Severity:** low

**Classification: Divergent** -- Go's cell parsers refuse, with `ErrCorrupt`, a cell whose
key and value lengths sum over 1 GiB (`maxPayloadAlloc`, tested by `payloadFits`); SQLite's
parsers read the payload length into a `u32` and test it against nothing
(`btreeParseCellPtr`, `btree.c:1280-1287`; `btreeParseCellPtrIndex`, `btree.c:1359-1366`).
The bound against the database size is SQLite's in both: a reader tests a declared length
against `pageSize * nPage` before it sizes a buffer by it (`payloadInFile`, below), though a
reader's page count is the pager's
([drift-2026-11-24-2](#drift-2026-11-24-2-a-readers-file-size-bound-takes-the-pagers-page-count)).

The cap is also the write-side limit: reads and writes apply one rule, `payloadFits`
(`page.go`). Key length plus value length is at most `maxPayloadAlloc` (`1<<30`), tested on
the 64-bit lengths a cell declares before they are converted to `int` (on a 32-bit build a
declared length of `1<<32+1` would convert to 1). The leaf-cell parsers that test both
lengths (`parseLeafCellWithSize`, `leafFullValue`, `leafKeyAt`) answer `ErrCorrupt` for a
cell that fails it, and so do `parseInteriorCell` and `interiorFullKey` for a key over the
cap. `Put` refuses such a record before it touches a page, so it never writes a record that
its own parse would reject: see
[drift-2026-11-24-1](#drift-2026-11-24-1-put-refuses-a-record-over-the-record-size-limit).

A read of a leaf cell's key bounds the key only; the value's declared length is tested where
the value is read. That length still places the key's local part, and `spilledLocalSize`
(`page.go`) computes that part without adding the two lengths, which a damaged one could
wrap. The search tests the sum only for the cell it reports as found, so every exact match
is held to it, `Has` and an exact seek included. A damaged value length therefore need not
make the cell's key, or a search for a neighbouring key, fail; a read of the value reports a
length over its bounds. A SQLite table cursor reads the rowid of a row whose payload is
corrupt the same way (`sqlite3BtreeIntegerKey`, `btree.c:4917-4923`). `leafCellPayloadLen`
(`walkSize`) tests the two lengths singly. These tests are looser than `payloadFits`, never
stricter, so nothing `Put` accepts fails there. The overflow-chain readers have no cap of
their own (their `maxIter` follows from the length asked for), and the 9-byte varint holds
any length under the cap.

A reader that allocates from a declared length first tests it against the size of the
database it reads, so that a few damaged bytes in a small file cannot ask for up to 1 GiB
before the overflow chain is found short. The test is SQLite's: `sqlite3VdbeMemFromBtree`
returns `SQLITE_CORRUPT_BKPT` when `amt+offset > sqlite3BtreeMaxRecordSize(pCur)` before it
resizes the Mem (`vdbemem.c:1428-1429`), and that size is `pageSize * nPage`
(`btree.c:4978-4982`). `payloadInFile` (`page.go`) makes it with the page count of the
reader's snapshot (`readerDbSizeBound`: the read transaction's, or the live count for the
writer); a reader's count is the pager's, not page 1's
([drift-2026-11-24-2](#drift-2026-11-24-2-a-readers-file-size-bound-takes-the-pagers-page-count)).
It runs before every allocation sized by a declared length, on the bytes that are
read: key and value for the value readers (`btree.AppendValue`, `ReadTx.AppendValue`,
`Cursor.Value`, `Cursor.AppendValue`, `leafFullValue`), the key alone for the key readers
(`leafFullKey`, `interiorFullKey`, `cellFullKey`, `collectInteriorCellsKeepBuf`).
`leafFullKey` also gives its first overflow pointer the chain readers' page test before it
allocates. SQLite's index probe likewise sizes the key it rebuilds from the key alone
(`btree.c:6159-6168`). A value length over the file therefore fails the read of that value,
not a read of the cell's key. The test is made where the bytes are asked for, as in SQLite,
not in the cell parsers, which have no pager. The 1 GiB cap is the limit of what is written,
and the tighter bound of the two in a file over 1 GiB.

`Has` answers from the cell its search finds: it tests the record size limit there, not the
file size, so it can be true for a record whose `Get` fails the file-size bound. SQLite's
table seek skips a cell's payload size and compares its rowid only (`btree.c:5880-5886`);
a later read of the payload fails at `vdbemem.c:1428`.

Tests: `TestPayloadLimit_RealConstants`,
`TestPayloadLimit_LengthsCheckedBeforeNarrowing`, `TestPayloadLimit_SpilledLocalSize`,
`TestPayloadLimit_PutRefusesWhatReadRejects`, `TestPayloadLimit_NamespaceName`,
`TestPayloadLimit_DeclaredLengthOverFileSize`,
`TestPayloadLimit_NeighbourOfDamagedCellIsFound`,
`TestPayloadLimit_CursorKeyOfDamagedCell`, `TestPayloadLimit_SeekKeyOfDamagedCell`,
`TestPayloadLimit_SeekKeyNextLeafStartsWithDamagedCell`,
`TestPayloadLimit_CountUntilOverDamagedCell`,
`TestPayloadLimit_SpilledKeyWithNoOverflowPage`,
`TestPayloadLimit_SplitBesideDamagedValueLengths`,
`TestPayloadLimit_OverflowRecordWithinFileSize`, `TestCov_LeafFullKeyBoundsTheKeyOnly`.

<a id="old-drift-additive-maxiter-circular-chain-guard"></a>
**Severity:** low

The Go implementation adds circular-chain protection that SQLite doesn't need
(SQLite trusts the page structure more due to its integrity check infrastructure).

---

## 14. Integrity Check

### SQLite (`sqlite3BtreeIntegrityCheck()` in btree.c:11126)

Checks:
- Page reference bitmap (every page referenced exactly once)
- Freelist structure (trunk/leaf counts match header)
- Specific B-tree root pages (passed as `aRoot[]` array)
- Cell pointer bounds and overlap (using min-heap)
- Key ordering within pages
- Uniform depth of children in interior pages
- Overflow chain length matches expected count
- Fragmentation byte count accuracy
- Auto-vacuum pointer-map consistency
- `PENDING_BYTE` page exclusion
- Can do partial checks (subset of root pages)

### Go (`IntegrityCheck()` in integrity.go)

Checks:
- Page reference bitmap (same approach)
- Freelist structure validation
- B-tree page types (only `pageTypeLeafIdx` and `pageTypeIntIdx`)
- Cell pointer bounds and overlap (min-heap, same algorithm)
- Key ordering (including full overflow key reads)
- Cross-boundary divider key ranges: every key threaded against `[lower, upper)` bounds
  down the recursion (`keyInBounds`, commit 97e9236) — the any-store analogue of SQLite's
  `piMinKey`/`maxKey` machinery (btree.c:10953-11029), transposed to the smallest-key-of-
  right-subtree `<,>=` divider invariant
- Uniform child depth
- Overflow chain page count
- Fragmentation byte count
- Orphan page detection
- Master table structure (page 1 -> namespace root pages)
- Database size capping against file/WAL size

### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| Multi-tree support | Checks specific root pages from `aRoot[]` | Discovers trees from master table on page 1 |
| Auto-vacuum checks | Pointer-map consistency | Not applicable |
| PENDING_BYTE page | Excluded from reference check | Not applicable |
| Partial checks | Supported via `aRoot[0]==0` | Not supported |
| Cell counts | Written to `aCnt[]` output | Not tracked |
| Heap for coverage | Uses page-sized malloc (`sqlite3PageMalloc`) | Uses dynamic `[]uint32` slice |
| Error output | String accumulator (`StrAccum`) | `[]string` slice |

**Classification: Divergent** -- Both perform the same core checks (page reference
bitmap, freelist, cell bounds, key ordering, overflow chains, fragmentation). The Go
version auto-discovers B-tree roots from the master table rather than taking them as
input. Auto-vacuum and PENDING_BYTE checks are absent because those features don't exist.

**Codec integrity sweep (`integrity_sweep.go`).** Separate from the structural
`IntegrityCheck` above, any-store adds `VerifyIntegrity` — a per-page codec/AEAD sweep
(`verifyCksumPage` / `verifyAEADPage`) that re-hashes (cksum codec) or AEAD-decrypts every
page to detect ciphertext tampering or bit-rot. No SQLite analogue: stock SQLite has no
page-level MAC, and this is the codec layer's own consistency check. Any-store-specific.
It reads every page raw, with its own read (`readRawPage`), not the codec's read path. A
checksum page it hashes itself (`verifyCksumPage`) and never passes to `cksumCodec.Decrypt`;
an AEAD page it passes to the codec's `Decrypt` (`verifyAEADPage` →
`decryptPageWithCodec`), the only way to check that tag. Its read begin reads page 1's
header only, which is outside the codec, and its page count comes from the raw page 1. So
the checksum sweep needs nothing from the codec's verify-on-read flag and leaves it as
configured: on by default, off in a handle opened with `Config.ContinueOnIntegrityError`,
the one place that sets it. The sweep once cleared that flag for its duration and restored
the saved value on return. The flag is the handle's one switch (`cksumCodec.verify`),
shared by the writer and every reader, so for that time every other read returned a
damaged page as good, a write to such a page committed it with a fresh checksum, and two
overlapping sweeps left the flag off (the second saved the first's `false`). cksumvfs keeps
its `PRAGMA checksum_verification` per open database file and its WAL partner
(`sqlite3_file_control` on the named database, `pragma.c:495`; `p->verifyCksm` and
`p->pPartner->verifyCksm`, `cksumvfs.c:563-565`): two databases attached to one connection
have two settings.
The database's integrity hook (`OnIntegrityError`) fires once for each page whose tag fails: for a checksum page the
sweep fires it, only after a comparison that differed (drift-2026-06-25-13, resolved); for
an AEAD page the codec's `Decrypt` fires it. A page the sweep could not read is listed
(`IntegrityKindUnknown`) and fires nothing, in either mode.
An in-memory database has no integrity mode (`IntegrityMode` returns `IntegrityNone`, the
sweep scans nothing): its store and WAL hold pages as the b-tree wrote them, past any
codec the options named (`wal.writeFramesMem`), so no page carries a tag. The sweep used
to try the configured codec on those pages and list every one.
The pages it covers are those of its read transaction's snapshot, counted as SQLite counts
them for `sqlite_dbpage` and the integrity check (see
[the resolved drift](#drift-2026-06-25-12-verifyintegrity-bounds-the-sweep-with-the-live-writer-header-pager-hea)).

<a id="old-drift-checklist-unconditional-size-mismatch-report"></a>
**Resolved (stale, 2026-06-25)** — Go now implements the `nErrAtStart==len(ic.errors)` suppression guard at integrity.go:108,168,172; the cited C line is stale (guard is now btree.c:10763, with nErrAtStart captured at btree.c:10713).
**Severity:** low
and it emits the size/overflow count-mismatch message
unconditionally, lacking SQLite's `nErrAtStart==pCheck->nErr` suppression (btree.c:10781),
so it can pile a redundant line onto an already-reported corruption.

---

## 15. Cursor Implementation

### SQLite (`BtCursor` in btreeInt.h:531)

```c
struct BtCursor {
    u8 eState;              // VALID, INVALID, SKIPNEXT, REQUIRESEEK, FAULT
    u8 curFlags;            // BTCF_WriteFlag, BTCF_ValidNKey, BTCF_ValidOvfl, etc.
    Pgno *aOverflow;        // Cache of overflow page locations
    void *pKey;             // Saved key for REQUIRESEEK restoration
    i64 nKey;               // Key size or integer key
    i8 iPage;               // Current depth in page stack
    u16 ix;                 // Current cell index on current page
    u16 aiIdx[19];          // Cell indices for parent pages
    MemPage *pPage;         // Current leaf page
    MemPage *apPage[19];    // Parent page stack
    CellInfo info;          // Parsed cell info cache
};
```

Key features:
- **5 cursor states** with automatic save/restore (`REQUIRESEEK`)
- **Overflow page cache** (`aOverflow[]`)
- **Cell info caching** (`info` field parsed lazily)
- **Max depth 20** (stack-allocated arrays)
- **Skip-next** optimization for delete-during-iterate
- **Write cursors** can modify the tree while iterating

### Go (`Cursor` in btree.go)

```go
type Cursor struct {
    bt         *btree
    stack      []cursorFrame
    state      cursorState // invalid, valid, skipNext, requireSeek, fault
    skipNext   int8        // side of the saved key a restored cursor landed on
    savedKey   []byte      // the key a saved cursor seeks again
    nextCursor *Cursor     // the writer's cursor list (pager.cursors)
}
type cursorFrame struct {
    pgno    uint32
    cellIdx int
    pg      *page  // pinned page (non-nil only for leaf)
}
```

Key features:
- **5 states** (`cursorState`): invalid, valid, skipNext, requireSeek, fault
- **Save/restore**: a write transaction's cursors are linked on `pager.cursors`
  (`BtShared.pCursor`, btree.c:4741). `btree.Put` / `btree.Delete` call
  `pager.saveAllCursors(root)` before a page of the tree changes
  (`sqlite3BtreeInsert` btree.c:9424, `sqlite3BtreeDelete` btree.c:9917), and copy
  the caller's key and value when a cursor let a page go — they may be slices of
  it (SQLite's writing cursor keeps its pages, `pExcept`, and its payload is the
  VDBE's own). `WriteTx.RollbackToSavepoint` saves every root before the pager
  plays the savepoint back (`sqlite3BtreeSavepoint` btree.c:4614); `DeleteNamespace`
  trips the dropped root's cursors (`sqlite3BtreeTripAllCursors` btree.c:4467),
  and the rollback of a savepoint opened before the drop revives them, while it
  ends the cursors on a tree created inside it (`pager.schemaRolledBack`; the VDBE
  trips every cursor on the rollback of a savepoint that changed the schema,
  vdbe.c OP_Savepoint). A saved cursor copies its key and releases its pages
  (`saveCursorPosition` btree.c:756); its next move or read seeks the key again
  (`btreeRestoreCursorPosition` btree.c:896). The key gone, the cursor stands on a
  neighbour in `cursorSkipNext`, and the move onward in the direction it landed —
  `Next` after the key, `Previous` before it — yields the neighbour without
  stepping (`btreeNext` btree.c:6315); a read of it reports `ErrKeyNotFound` (the
  VDBE's nullRow, vdbeaux.c:3830). A move that fails ends the cursor
  (`cursorFault`), as does the end of a write transaction in the pager's error
  state.
- **Dynamic stack** (slice, not fixed-size array)
- **Only leaf page pinned** (interior pages released during descent)
- **Read-only** (no write cursor support; writes descend on their own)
- **SeekNear optimization**: checks if target key is within current leaf bounds
- **No overflow page cache**
- **No cell info caching**

### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| States | 5 (VALID, INVALID, SKIPNEXT, REQUIRESEEK, FAULT) | 5 (the same) |
| Page stack | Fixed-size arrays (20 depth) | Dynamic slice |
| Pinned pages | All pages in stack pinned | Only leaf page pinned |
| Overflow cache | `aOverflow[]` | None |
| Cell info cache | `CellInfo info` | None |
| Write support | Full (BTCF_WriteFlag) | Read-only |
| Save/restore | Automatic via `pKey`/`nKey` | Same: `savedKey`, `saveAllCursors` at Put / Delete / savepoint rollback |
| Delete during iterate | `SKIPNEXT` mechanism | Same (`cursorSkipNext`) |
| Cursor list | `BtShared.pCursor`, every cursor | `pager.cursors`, the writer's cursors only: a reader's cursor reads a snapshot no write touches |
| Trip on tx rollback | `sqlite3BtreeTripAllCursors` | The layer above closes a transaction's iterators before the btree tx ends (tx.go tripIters); the list is reset with the write; a write ending in the pager's error state trips them |
| Writer's payload | The VDBE's own; `pExcept` keeps the writing cursor's pages | Put / Delete copy key and value when a cursor let a page go |
| Failed move | `moveToChild` falls back to the parent, the cursor stays VALID | The cursor is ended (`cursorFault`): an interior frame holds no key to save |
| Drop under an open cursor | OP_Destroy refuses (SQLITE_LOCKED) while any other statement of the connection is running — from its first step to its end, whatever it reads | The layer above refuses `Drop`, `DropIndex` and `CompactVectorIndex` while an iterator of the transaction is open on the collection, from its open to its `Close`, advanced or exhausted or not (`ErrIterOpen`, collection.go `droppable`: the planner creates its cursors at the first `Next`, so the btree cannot see every iterator); an iterator on another collection does not count — without autovacuum no other root moves; the btree trips the cursors of a dropped tree (`ErrNamespaceNotFound`), and the rollback of the drop revives them |

<a id="old-drift-readonly-two-state-cursor"></a>
**Severity:** low

**Classification: Structural** -- The Go cursor is read-only: `btree.Put()` /
`btree.Delete()` do their own tree traversal rather than driving a cursor, so there
is no `BTCF_WriteFlag` / `BTCF_Multiple` accounting and `saveAllCursors` saves every
cursor on the written root (SQLite skips the writing cursor itself). `FAULT` is
reached by a dropped tree, a failed move, or a write transaction ending in the
pager's error state: an ordinary transaction end never meets a live cursor, the
layer above closes them first.

---

## 16. Savepoints

### SQLite

SQLite savepoints in the B-tree layer (`sqlite3BtreeSavepoint()` in btree.c:4614):
- Delegates to the pager layer (`sqlite3PagerSavepoint()`)
- Pager writes modified pages to a **sub-journal** on disk
- `sqlite3WalSavepoint()` captures WAL state (4 u32 values: `aWalData[4]`)
- `sqlite3WalSavepointUndo()` truncates WAL back to savepoint position
- Supports arbitrary nesting depth
- `SAVEPOINT_RELEASE` and `SAVEPOINT_ROLLBACK` operations

### Go

Go savepoints (pager.go):
- `savepoint()`: Captures `dbSize`, a `walHdr` snapshot (`WalIndexHdr`: mxFrame + `aFrameCksum[2]`), `header` snapshot, creates empty page map
- `getWritablePage()`: Lazily copies page data before modification (copy-on-write)
- `rollbackToSavepoint()`: Restores pages from newest to oldest savepoint
- `releaseSavepoint()`: Merges page copies to parent savepoint
- All page copies stored **in memory** (no sub-journal file)
- `hasContent` map prevents NOCONTENT optimization for freed-then-reallocated pages

### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| Storage | On-disk sub-journal | In-memory page copies |
| WAL integration | `sqlite3WalSavepoint()` captures 4 u32 values | Captures a `walHdr` snapshot (mxFrame + `aFrameCksum[2]`) |
| Page restoration | Reads from sub-journal file | Copies from in-memory maps |
| Merge on release | Sub-journal frames retained | Page maps merged to parent |
| Header restoration | Implicit via page 1 journal | Explicit `header` snapshot |

<a id="old-drift-savepoint-inmemory-copies-vs-subjournal"></a>
**Severity:** medium

**Classification: Divergent** -- Both achieve the same semantics (nested savepoints
with rollback/release), but Go uses in-memory storage while SQLite uses disk-based
sub-journals. This makes Go savepoints faster but memory-intensive for large transactions.

---

## 17. Auto-vacuum / Incremental Vacuum

<a id="old-drift-autovacuum-not-implemented"></a>
**Severity:** low

**Not implemented.** The database file header fields for auto-vacuum (offsets 52 and 64)
are always set to 0. Freed pages go to the freelist and stay there until reused.

**Classification: Missing** -- Auto-vacuum and incremental vacuum are not implemented.
The freelist grows monotonically unless pages are reused by new allocations. Database
file size can only shrink via `VACUUM` (which is also not implemented).

---

## 18. Table B-trees vs Index B-trees

### SQLite

SQLite has two types of B-trees:

| Feature | Table B-tree | Index B-tree |
|---------|-------------|--------------|
| Page types | 5 (interior), 13 (leaf) | 2 (interior), 10 (leaf) |
| Key type | 64-bit integer (rowid) | Arbitrary blob |
| Value storage | Data in leaves only (`PTF_LEAFDATA`) | No separate value |
| Leaf cell format | `[varint(nPayload)] [varint(rowid)] [data]` | `[varint(nPayload)] [key]` |
| Interior cell | `[4B child] [varint(rowid)]` | `[4B child] [varint(nPayload)] [key]` |
| Overflow limits | `maxLeaf` / `minLeaf` | `maxLocal` / `minLocal` |
| Flags | `PTF_INTKEY \| PTF_LEAFDATA \| PTF_LEAF` | `PTF_ZERODATA \| PTF_LEAF` |

### Go

The Go implementation uses **only index-style B-trees** with separate key/value
semantics:

| Feature | Go B-tree |
|---------|-----------|
| Page types | 2 (interior), 10 (leaf) |
| Key type | Arbitrary blob |
| Value storage | In leaves alongside key |
| Leaf cell format | `[varint(keyLen)] [varint(valLen)] [key\|\|value]` |
| Interior cell | `[4B child] [varint(keyLen)] [key]` |
| Overflow limits | Interior cells: `maxLocal` / `minLocal`. Leaf cells: `maxLeaf` / `minLeaf` in schema format 6 files, `maxLocal` / `minLocal` in format 5 files |

<a id="old-drift-index-only-btrees-no-intkey-table"></a>
**Severity:** medium

**Classification: Structural** -- This is a fundamental architectural difference.
SQLite's table B-trees use integer keys with record-format values, optimized for
SQL table storage. The Go implementation repurposes the index B-tree format to
store key/value pairs by adding a second varint for value length. This means:

1. No integer-key lookups (all keys are byte-comparable blobs)
2. No `PTF_INTKEY` / `PTF_LEAFDATA` page types
3. Values are stored inline with keys using the two-varint format
4. The `maxLeaf` / `minLeaf` formulas apply to every leaf of a schema format 6 file, and
   to no leaf of a format 5 file (see
   [the local-payload drift](#old-drift-index-only-local-payload-no-maxleaf-minleaf))

---

## 19. Atomic `dbSize` -- Why Go Needs `atomic.Uint32` While SQLite C Doesn't

### The Problem

The `pager.dbSize` field (database size in pages) is written by the writer goroutine
in `allocatePage()` and read by concurrent reader goroutines in
`readOverflowChainAt`/`readOverflowChainReader`/`readOverflowAt` for bounds-checking
overflow page numbers. Without
synchronization this is a data race under Go's memory model.

### How SQLite C Avoids the Race

SQLite sidesteps this entirely through its process/threading model:

1. **`Pager.dbSize` is per-connection.** Each SQLite connection has its own `Pager`
   struct. A reader snapshots `dbSize` once during `sqlite3PagerSharedLock()` →
   `pagerPagecount()` → `sqlite3WalDbsize()`, which returns `pWal->hdr.nPage` --
   a local copy of the WAL index header captured at `walTryBeginRead()` time. No
   other thread ever touches that `Pager` object concurrently. There is no shared
   mutable state to race on.

2. **`BtShared.nPage` is mutex-protected.** When multiple connections share a
   `BtShared` (shared-cache mode), every access to `pBt->nPage` is guarded by
   `pBt->mutex`. Each access site has `assert(sqlite3_mutex_held(pBt->mutex))`.
   The pthread mutex provides a full acquire/release barrier, so the C memory model
   guarantees visibility.

3. **The WAL shared-memory `nPage`** (the `volatile WalIndexHdr` in `.shm`) uses a
   double-copy + `walShmBarrier()` (→ `__sync_synchronize()`) + checksum protocol
   for cross-process visibility without a mutex. OS-level `flock` byte-range locks
   provide inter-process ordering.

In summary, SQLite never has two threads concurrently accessing the same `dbSize`
field without a synchronization primitive: the field is either per-connection
(no sharing) or mutex-protected (shared cache).

### Why Go Needs Atomics

The Go implementation has a single `pager` struct shared by all goroutines within
a process. The writer goroutine increments `dbSize` in `allocatePage()` while
reader goroutines read it in the overflow readers (`readOverflowChainAt`/
`readOverflowChainReader`/`readOverflowAt`) for bounds checking.
This is a genuine concurrent access to a shared field.

Go's memory model (defined by the Go specification, not C11/POSIX) requires that
concurrent access to a shared variable be synchronized via `sync/atomic` operations,
`sync.Mutex`, or channels. A plain `uint32` read/write from different goroutines is
a data race -- even if "logically" only one goroutine writes at any given time --
unless the Go race detector can see a happens-before relationship.

Two mechanisms coexist in the implementation, each solving a distinct problem:

1. **Writer-side `atomic.Uint32`** -- the `pager.dbSize` field is the writer's
   allocation counter. The writer uses `dbSize.Add(1)` in `allocatePage()` and
   `dbSize.Store()` for rollback/init paths; any goroutine reading it uses
   `dbSize.Load()`. The atomic is what makes this shared-field access safe under
   Go's memory model with no contention (~1 ns per Load on x86). A mutex on every
   read was rejected as too expensive (readers would contend with the writer on
   every overflow bounds check).

2. **Reader-side per-snapshot `dbSize` bound** (matching SQLite's per-connection
   `pPager->dbSize` model) -- a pure reader must *not* bound-check against the
   writer's `pager.dbSize`, because that atomic is only refreshed on the write path
   (`beginWrite` → `refreshHeaderFromPage1`), so a reader would wrongly reject pages
   a peer process allocated after this process opened. Instead, each reader cache
   carries `pcache.dbSize` (pcache.go:69-81), the committed page count captured
   cross-process at `BeginRead`, set in lockstep with `walMaxFrame` at every
   snapshot-establish site. It is derived by `pager.effectiveReaderDbSize`
   (pager.go:1664-1686), which mirrors `sqlite3WalDbsize` (wal.c:3672, returning
   `pWal->hdr.nPage`) and, when the WAL header carries no committed size, falls back
   to the database file's size as `pagerPagecount` does (pager.c:3299-3305). The
   in-process and in-memory modes publish no wal-index header: their synthesized
   snapshot header carries the size of the walIndex's committed position, the
   commit frame and its size in one word (`mxframe.go`), which only a published
   commit moves. As in SQLite's header, that size is 0 until a commit records
   one (`walIndexRecover` zeroes the header and sets `nPage` from a commit frame,
   wal.c:1408, 1512) and a WAL reset keeps it, so a non-zero size is a commit's
   in every mode, and an in-process reader without one takes the file's. An
   in-memory database has no file; it is without a committed size until a WAL
   commit frame records one (a commit that writes no frame records none), and
   until then it is the one page creation wrote (`newDatabase` sets
   `nPage = 1`, btree.c:3540-3541), so its readers take 1. No reader
   there uses `pager.dbSize` either
   (until 2026-11-12 they did, and a reader of the committed snapshot failed on
   its pages above an uncommitted shrink). Until 2026-11-11 the
   fallback was `pager.dbSize` in every mode, and a WAL reset published a header
   without a size: see [drift-2026-11-11-3](#drift-2026-11-11-3-reader-size-without-a-wal-commit-is-memoized).
   This is the pager's count, the bound for page reads; the database's logical
   size, which `ReadTx.DatabaseSize` returns and a backup copies, is lockBtree's
   count from the snapshot's page 1 (`sqlite3BtreeLastPage`, btree.c:2364-2369).
   Every reader bound check consults this via
   `pager.readerDbSizeBound` (pager.go:1688-1699), which returns `cache.dbSize`
   when set and otherwise degrades safely to `pager.dbSize.Load()` for the
   writer/uncached paths. `IntegrityCheckN` sets `cache.dbSize = nPages` likewise
   (integrity.go:583).

So the `atomic.Uint32` is *not* an alternative to a per-reader snapshot -- the two
are complementary. The atomic provides safe concurrent access to the writer's
counter under Go's memory model; the per-snapshot `pcache.dbSize` provides the
correct cross-process read-after-write bound that SQLite gets for free from its
per-connection `Pager`.

### Drift

| Aspect | SQLite C | Go |
|--------|----------|-----|
| `dbSize` ownership | Per-connection (`Pager.dbSize`) -- no sharing | Writer counter is a single shared `pager.dbSize`; readers carry per-snapshot `pcache.dbSize` |
| Writer/reader isolation | Separate `Pager` instances per thread | Shared struct, goroutine concurrency |
| Synchronization | None needed (no sharing) or `pBt->mutex` | `atomic.Uint32` (Load/Store/Add) on the writer counter |
| WAL snapshot | `pWal->hdr.nPage` local copy at read-lock time | `pcache.dbSize` captured at `BeginRead` in lockstep with `walMaxFrame`, via `effectiveReaderDbSize`/`readerDbSizeBound` |
| Performance cost | Zero (no sharing = no synchronization) | ~1 ns per `atomic.Load` on x86 |

<a id="old-drift-dbsize-atomic-and-per-snapshot-reader-bound"></a>
**Severity:** medium

**Classification: Divergent** -- This drift stems from a fundamental architectural
difference: SQLite uses per-connection state isolation (each connection has its own
`Pager`), while the Go implementation shares a single `pager` across goroutines. The
Go memory model mandates explicit synchronization for any cross-goroutine field access,
even for benign races that C compilers and POSIX threads would handle correctly via
hardware cache coherence. The Go implementation addresses both halves of SQLite's
per-connection `Pager.dbSize`: the writer's allocation counter is an `atomic.Uint32`
(`pager.dbSize`) for safe shared access, *and* readers carry a per-snapshot
`pcache.dbSize` (captured cross-process at `BeginRead` in lockstep with `walMaxFrame`,
surfaced via `effectiveReaderDbSize` mirroring `sqlite3WalDbsize` and consulted via
`readerDbSizeBound`) so the reader bound check matches SQLite's
`sqlite3WalDbsize`/`pagerPagecount` snapshot semantics rather than the writer-only
counter.

---

## Summary Table

| Area | Drift Type | Severity | Notes |
|------|-----------|----------|-------|
| 1. Leaf cell format | Intentional | Low | Two varints vs one; functionally equivalent |
| 2. Interior cell format | Intentional | None | Semantically identical |
| 3. Overflow formula | Intentional | Low | Same formulas; the table-leaf pair is chosen per file (schema format 6), not per page |
| 4. Binary search | Divergent | Medium | Raw byte comparison vs record comparison; prefix optimization |
| 5. Page header | Intentional | Low | Different magic string; same layout |
| 6. Schema format | Intentional | Low | Formats 5 and 6 are Go-specific |
| 7. WAL | Intentional + Divergent | Medium | Different magic/version; Go map for lookups |
| 8. Pager | Structural | High | WAL-only; no rollback journal; simplified states |
| 9. Page cache | Structural | Low | Slab allocator, bulk alloc, LRU, admission control, persistent cache, reader limiter |
| 10. Freelist | Intentional | None | Same format; different tracking structures |
| 11. Overflow chains | Divergent | Medium | MVCC-aware reads; no overflow cache |
| 12. MVCC | Divergent | High | Explicit uncached reads for goroutine safety |
| 13. Corruption protection | Divergent | Low | Fixed 1GB cap vs page-count-based validation |
| 14. Integrity check | Divergent | Low | Same core checks; auto-discovers trees |
| 15. Cursor | Structural | Low | Read-only; save/restore as SQLite; only leaf pinned |
| 16. Savepoints | Divergent | Medium | In-memory vs on-disk sub-journal |
| 17. Auto-vacuum | Missing | Medium | Not implemented |
| 18. Table B-trees | Structural | High | Not implemented; index B-trees only |
| 19. Atomic dbSize | Divergent | Low | Go memory model requires atomics for shared pager field |

---

## Known Implementation Gaps

Practical issues and limitations with severity ratings, organized by subsystem.

### WAL

**Cache Spill (pagerStress)** -- Severity: Critical (implemented)

Implements SQLite's `pagerStress()` mechanism: when the page cache is full and
all clean pages are exhausted, the pcache stress callback spills dirty pages to
the WAL mid-transaction (non-commit frames with `dbSize=0`), marks them clean,
and makes them evictable. This bounds cache memory to approximately `CacheSize`
during large write transactions.

Key components:
- `walIndex.mxCommitFrame`: separates committed frame count (visible to readers)
  from total frame count including spilled frames (writer-internal)
- `walIndex.pendingShmFrames` + `flushPendingShmFrames()`: defers SHM hash writes
  until commit so cross-process readers cannot see uncommitted spilled frames
- `walIndex.rollbackToFrame()`: cleans up `pageMap` entries and restores
  `maxFrame` on rollback of transactions with spilled frames
- `pcache.xStress`: callback invoked when cache is full and `nRecyclable == 0`;
  finds an unpinned dirty victim page for spilling
- `pager.pagerStress()`: the stress callback implementation — saves savepoint
  data if needed, writes the page to WAL with `commit=false`, marks it clean
- `pager.doNotSpill`: bitmask (`spillFlagOff`, `spillFlagRollback`) preventing
  re-entrant spills during rollback operations

Cache spill drifts from SQLite (all intentional, marked with `DRIFT from SQLite`
comments in source):

<a id="old-drift-pagererror-eager-cleanup"></a>
1. **pagerError eager cleanup, on a failed commit only** (`pager.go:pagerError`) — **Severity:** medium.
   SQLite's `pager_error()` (`pager.c:1947-1960`) only sets `errCode` and moves to
   `PAGER_ERROR`; the cache is discarded and the WAL write transaction ended by
   `pager_unlock` (`pager.c:1853-1864`, `1904-1917`), which runs once no page
   is referenced. `pager.commit` calls `pagerError()` when its page-1 fetch or
   its `writeFrames` fails, and that does the whole cleanup at once (cache
   purge, WAL rollback, lock release, back to `pagerOpen`): a failed commit
   ends the transaction here (`WriteTx.Commit` marks it closed before the
   pager runs), no b-tree operation is running, and no later rollback call is
   guaranteed to release the WAL write lock.
   SQLite's own failed WAL commit never enters `PAGER_ERROR`: `pagerWalFrames`
   returns the error (`pager.c:6512`, `3219-3236`), and the VDBE rolls the
   transaction back as any other (`sqlite3PagerRollback` →
   `sqlite3PagerSavepoint`, `pager.c:6779-6782`, → `sqlite3WalUndo`). Here
   a commit whose frame write fails passes through `pagerError`, which stores
   the commit's error in `errCode` and trips the cursors with it
   ([drift-2026-11-24-5](#drift-2026-11-24-5-a-failed-commit-ends-its-cursors-with-its-error)),
   a state SQLite has no counterpart of. A commit whose fetch
   of page 1 meets a failed spill (page 1 can be evicted here,
   [entry 2](#old-drift-pagerstress-page1-exclusion)) is in the error state of a failed spill already, with `errCode`
   set by `pagerStress`: `pagerError` keeps that error, trips the cursors
   with it and clears it as it cleans up.
   A failed spill does not run this cleanup: `pagerStress` only sets the error
   state, and the rollback cleans up; see
   [the error state of a failed spill](#drift-2026-11-21-13-the-error-state-of-a-failed-spill).

<a id="old-drift-pagerstress-page1-exclusion"></a>
2. **Page-1 explicit exclusion** (`pager.go:pagerStress` AND
   `pcache.go:findSpillVictim`) — **Severity:** low. Consequence for the change
   counter (2026-11-12): page 1 is not held for the transaction, so a clean page 1
   can be evicted during one and read again at the commit, and that read sets
   `dbFileVers` ([schema format 6 counter](#drift-77-filechangecount-bumped-conditionally-not-unconditionally)),
   where SQLite's referenced page 1 keeps its value.
   SQLite does not check
   `pgno==1` in `pagerStress()`. Page 1 is structurally protected: it stays pinned
   (referenced) throughout the transaction, so pcache never selects it as a spill
   victim. We add an explicit guard because page 1 may become unpinned between
   b-tree operations. The guard must exist in BOTH places: `pagerStress` refuses
   page 1 WITHOUT cleaning it, so if `findSpillVictim` could still return it the
   spill wedged permanently — every attempt selected page 1, nothing ever
   spilled, and backup (which copies and releases page 1 first) accumulated the
   entire destination dirty in memory (peak RSS ≈ database size). Fixed by
   skipping page 1 in the victim search too; regression:
   `TestBackupWriterCacheBounded`.

<a id="old-drift-pcache-create-drops-xstress-error"></a>
3. **Resolved** — `pcache.create()` returns the xStress error and creates no
   page, as `sqlite3PcacheFetchStress` does with an error other than `SQLITE_BUSY`
   (`pcache.c:483-485`; the spill here returns no busy error), and every caller gives the
   fetch up (`getPageNormal`, `pager.c:5555-5556`); see
   [the error state of a failed spill](#drift-2026-11-21-13-the-error-state-of-a-failed-spill).

<a id="old-drift-batched-walindex-setbatch"></a>
4. **Batched wal-index update** (`wal.go:setBatch`) — **Severity:** medium.
   SQLite updates the wal-index
   inline in `walFrames()` — the write loop tags each page `PGHDR_WAL_APPEND`
   (set on append `wal.c:4163`, cleared on in-place reuse `wal.c:4151`), then a
   second loop replays the flag via `walIndexAppend()` (`wal.c:4225-4229`);
   rollback uses `walCleanupHash()` (`wal.c:1233`). We mirror this with one
   post-loop `setBatch` (`wal.go:603`, called at `wal.go:2218`) per
   `writeFrames` call (a single `wi.mu` acquisition for the in-process `pageMap`,
   then eager `shmHashWrite` — *not* deferred), plus a `walCleanupHash` analog on
   rollback. `shmHashWrite` also runs `walCleanupHash` (`shmCleanupHash`) before
   it writes a frame whose `aPgno` entry is already set, as `walIndexAppend`
   does (`wal.c:1327-1330`): the entry is left over from a writer that died
   after it had spilled pages, and with it the rest of that writer's entries
   above the log's last frame. The limit of that cleanup follows the batch
   ([drift-2026-11-19](#drift-2026-11-19-dead-writer-cleanup-limit-follows-the-batch)).
   Tests: `TestWalIndexDeadWriter_*` (`wal_index_dead_writer_test.go`).
   **Invariant: the appended set handed to `setBatch` MUST be recorded
   inline in the write loop (the `appended` slice ≡ `PGHDR_WAL_APPEND`), never
   re-derived.** Re-deriving the reuse predicate after the loop dropped the
   force-appended commit frame and silently corrupted recovery — see the
   Frame-Reuse note below. A `maxFrame < nFrame` guard in
   `writeFrames`/`writeFramesMem` (`wal.go:2230`) now fails loudly if any appended
   frame is unregistered.

<a id="old-drift-pagerstress-dontwrite-skip-walwrite"></a>
5. **Resolved (2026-10-08)** — `pagerStress` writes the WAL frame for every victim, as C's WAL
   branch does (`pagerWalFrames`, `pager.c:4647-4649`; the `PGHDR_DONT_WRITE` check lives only in
   the rollback-journal writer, `pager_write_pagelist` `pager.c:4471`). The skip rested on
   "dontWrite page data is never read back", which is false: a same-transaction reallocation
   through `allocateFromFreelist`'s hasContent branch reads the page, and a page grown in that
   transaction exists in neither the WAL nor the file, so the read failed with EOF after the leaf
   was unlinked and the commit leaked the page. The commit-time filter that dropped the frames of the
   same pages is gone with it, as is the `dontWritePages` map: a page grown and freed in one
   transaction then had no bytes in the WAL or the file, and a backup before the next checkpoint
   failed with EOF; a hole above about 64 KiB also tripped the checkpoint's over-grow guard
   (`wal.go`, C `wal.c:2281`) with `database is corrupt` until enough new frames accumulated.
   Files written before the fix keep their holes until the next full checkpoint extends the file
   (see the short-read drift). **dontWrite pages made clean without WAL write**
   (`pager.go:pagerStress`) — **Severity:** low.
   SQLite's `pagerStress` in WAL mode writes `PGHDR_DONT_WRITE` pages to WAL
   anyway (the data is irrelevant but the frame is still written). We skip the WAL
   write and just mark them clean, avoiding unnecessary I/O. Safe because dontWrite
   page data is never read back.

<a id="old-drift-shared-pagemap-transient-cache-miss"></a>
6. **Resolved (stale, 2026-06-25)** — the `getLatest`-based cache validation that produced transient spill-window misses has been removed; `getLatest` is now only a test/SHM helper with no production caller, and per-connection caches return pages directly. **Shared pageMap causes transient cache misses** (`wal.go:getLatest`) — **Severity:** low.
   We now use
   per-connection page caches matching SQLite's model, but we still share one `pageMap`
   across all goroutines. Spill frames are visible to `getLatest()`, causing transient
   cache misses during active spill when `latestFrame > walMaxFrame`. Each reader's
   private cache absorbs repeated misses within a transaction. This is correct and
   short-lived (only during active spill).

Additionally, the following Go-specific mechanisms have no SQLite analogue:
- `pager.writerOpMu`: mutex serializing commit/rollback with DB.Close
  force-rollback. `sqlite3_close()` returns `SQLITE_BUSY` only while a
  statement is unfinalized or a backup from the connection unfinished
  (`connectionIsBusy`, `main.c:1240-1250`, `1283`); a transaction left open
  with no statement is rolled back (`sqlite3RollbackAll`, `main.c:1386`); no
  close ends a transaction under a running statement. Go's DB.Close performs
  a force-rollback, requiring this mutex to prevent it from racing with a
  concurrent commit.
- `pagerStress` spills only in `pagerWriter` without `closeInvalidated`,
  and `writeFrames` re-checks under the WAL mutex (drift-2026-11-16-2
  below).
- `pager.wgate` (`writerGate`): the count of the write transaction's calls in
  flight, its Commit and Rollback included. DB.Close closes it and waits for
  the count to reach zero before the force-rollback, so that the teardown
  never runs under a b-tree operation and no Commit follows a refused call
  (drift-2026-11-21-25 below). SQLite's connection mutex does this for it.
- `db.writerLocksDone`: atomic CAS guard ensuring Close/Commit/Rollback lock
  cleanup runs exactly once despite concurrent goroutines
- `DB.Close` waits, with no timeout, for every open read transaction to end
  (`db.mu.Lock` against the `RLock` each `ReadTx` holds); SQLite's close
  waits at most for the step running on another thread, and refuses or
  defers the close while a statement is unfinalized (drift-2026-11-21-26
  below).
- In-memory WAL `memFrames` truncation on rollback (no SQLite equivalent since
  SQLite's in-memory WAL is a separate VFS shim)

**Checkpoint Copies Latest Frame Per Page** -- Resolved 2026-04-22

`checkpointWithMode` does a two-phase backfill: `buildBackfillMap(nBackfill,
mxSafeFrame)` (wal.go:2861) scans the safe frame range and keeps the latest frame
per pgno, then the backfill loop writes each page exactly once (the in-memory path
dedups equivalently). Matches SQLite's `WalIterator`, which writes only the latest
version per page — O(unique pages), not O(WAL frames).

**Checkpoint Buffer Allocation** -- Severity: Minor (fixed)

The checkpoint backfill loop now reuses `wal.ckptBuf`, a page-sized buffer
lazily allocated on first checkpoint. Matches SQLite's `walCheckpoint`
(wal.c:2285-2304) which reuses `pTmpSpace` from the pager.

**WAL Header Version + Page Size Validation** -- Resolved 2026-04-22

`walHeader.deserialize` now rejects WAL files whose `version` field does
not equal `walVersion` (1000000), or whose `pageSize` is not a power of
two in `[MinPageSize, MaxPageSize]`. Matches SQLite's `walIndexRecover`
validation (`sqlitec/src/wal.c:1406-1419`). Covered by
`TestWalHeaderDeserialize_RejectsBadVersion` and
`TestWalHeaderDeserialize_RejectsBadPageSize`.

**Checkpoint DB-File Over-Sync** -- Resolved 2026-05-31

`checkpointWithMode` no longer fdatasyncs the DB file before the truncate. SQLite
syncs the DB once per checkpoint — post-truncate, in the full-backfill block only
(`wal.c:2322-2327`) — and zero times on a partial backfill; copied pages stay
recoverable from the WAL synced before the copy loop.

**Eager WAL-Header Fsync on Checkpoint Reset** -- Resolved 2026-05-31

`doResetWAL` defers the on-disk WAL-header rewrite+fdatasync to the lazy
`flushHeader` path (next first-frame write), matching SQLite `walRestartHdr`
(`wal.c:2363-2389`): the reset publishes only the SHM header (fresh salt), and
TRUNCATE leaves a 0-byte WAL. Avoids an eager header fsync per RESTART/TRUNCATE
checkpoint (routine via auto-checkpoint escalation, DRIFT-53).

**Checkpoint Page-Size-Mismatch + Over-Grow Corruption Guards** -- Resolved 2026-05-31 (was DRIFT-52)

`checkpointWithMode` adds the over-grow guard before the backfill loop
(`nSize+65536+mxFrame*szPage < nReq` -> `ErrCorrupt`, `wal.c:2276-2294`), and
`recoverLocked` validates the WAL header page size against the DB page size
instead of adopting it (`walPagesize!=nBuf`, `wal.c:4386-4387`).

<a id="old-drift-inmemory-wal-skips-checksums"></a>
**In-Memory WAL Mode Skips Checksums** -- Severity: Minor (accepted)

**Severity:** low

Intentional design choice for the `InProcess + NoSync` fast path. No disk
persistence means checksums add overhead without value.

<a id="old-drift-autocheckpoint-threshold-10000-vs-1000"></a>
**Auto-Checkpoint Uses PASSIVE Mode** -- Severity: Minor

**Severity:** low

Default threshold is 10,000 frames vs SQLite's 1,000. Auto-checkpoint runs as
PASSIVE (`tryCheckpoint()` -> `checkpointPassive()`), matching SQLite's default
auto-checkpoint behavior. PASSIVE mode does not block writers or readers.

<a id="known-gap-wal-frame-counter-has-no-bound"></a>
**WAL Frame Counter Has No Bound** -- Severity: Minor (accepted; SQLite has the same)

A frame number is a `uint32` that `writeFrames` advances with no check, as
`walFrames` does (`iFrame++`, `wal.c:4225-4229`). A log of 2^32 frames (about
17 TB of 4 KiB pages, with every checkpoint held back) wraps it: frame
`0xffffffff` is also `READMARK_NOT_USED` (`wal.c:401`), and a transaction whose
first frame number wraps to 0 reuses no frame (`if( iFirst && ...`,
`wal.c:4138`; `canReuse` in `writeFrames` and `writeFramesMem`). Neither engine
supports such a log. The frame write in `wal.go` says the same.

**Frame-Reuse Within a Transaction (`walRewriteChecksums`)** -- Resolved 2026-04-22

SQLite's `walRewriteChecksums` (`sqlitec/src/wal.c:3966-4009`) overwrites
an earlier WAL frame when a page is re-dirtied within the same
transaction, instead of appending a new frame. `pWal->iReCksum`
(`sqlitec/src/wal.c:533`) tracks the earliest overwritten frame; on
commit SQLite re-reads frames `iReCksum..mxFrame`, recomputes the
running checksum chain starting from the preceding frame's checksums,
and rewrites frame headers so the chain stays consistent.

any-store now ports this. The reuse branch in `writeFrames`
(internal/btree/wal.go) mirrors `walFrames` (`wal.c:4117-4156`):

- `iFirst = mxCommitFrame.LoadLocal() + 1` is the first frame in the
  current tx eligible for in-place overwrite. We use `mxCommitFrame`
  rather than `writerHdr.mxFrame` because `writerHdr` is only
  maintained in multi-process mode; `mxCommitFrame` is updated in both
  modes on every commit.
- A spilled page is overwritten in place when `getInTxRange` finds a
  prior frame for the same `pgno` in `[iFirst..nf]`. The SHM hash
  entry is unchanged (still points at the reused frame).
- `iReCksum` advances backward to the earliest overwritten frame.
  `rewriteChecksums(iLast)` (analog of `wal.c:3966-4009`) rewrites
  frame headers `iReCksum..iLast` on commit, before fdatasync.
- The final commit frame is always a fresh append (it carries the
  `dbSize` marker — `wal.c:4124-4140`).
- Savepoint rollback resets `iReCksum` if it now points past the
  rolled-back frontier (`wal.c:3832` analog in
  `pager.rollbackToSavepoint`).
- `writeFramesMem` overwrites the in-memory `memFrames` slot for the
  InMemory mode equivalent — no checksum chain to rewrite.

**Commit-frame registration bug (fixed 2026-05-24).** The wal-index update
(`setBatch`) originally *re-derived* which pages had been appended by re-running
the reuse predicate after the write loop — but without the `!(commit && isLast)`
guard the write loop uses. When the commit frame's page had also been spilled
earlier in the same tx (common — a hot B-tree interior/index page is touched all
tx long and is often the last page flushed), the re-derivation misclassified the
force-appended commit frame as reused and dropped it, so `setBatch` undercounted
`maxFrame`/`mxCommitFrame` by one. The next tx then computed `iFirst =
mxCommitFrame+1` over the prior tx's committed commit frame and rewound the append
cursor onto it, overwriting it and re-seeding the checksum from the wrong base —
breaking the WAL checksum chain there. Invisible in the live process (the page map
self-heals as later writes re-register the hot page) but **fatal on crash
recovery**: `recoverLocked`'s chain walk stops at the break, silently discarding
every later committed transaction. Caught only under cache-spill pressure by
`TestStressRecovery_CrashedWriterDuringOverflowAlloc` (R-5, `CacheSize=10`); the
5000-page default rarely spills, which is why it stayed latent. **Fix:** record
the appended set inline in the write loop (≡ SQLite's `PGHDR_WAL_APPEND`), never
re-derive; plus the `maxFrame < nFrame` invariant guard above. This was a drift
from SQLite's inline `walIndexAppend` (NOTES drift item 4) — SQLite records the
decision per page and replays it, so it cannot desync.

**Bench evidence** (`BenchmarkWAL_SpillHeavyRepeatedDirty`, 1000 docs ×
2KB, CacheSize=4, nUpdates=5):

  variant   appends  reuses  wal_bytes
  -------   -------  ------  ---------
  NoReuse       581       0  10,843,872
  Reuse         130     451   8,969,272   -1.87 MB / -17%

78% of writeFrames calls hit the in-place overwrite path under
spill-heavy load. Production motivation: WAL bloat observed on real
Anytype workloads with long write transactions. The page cache dedups
naturally for small working sets; reuse only fires when the cache
spills mid-tx (working set > CacheSize).

**mmap for Database File Reads** -- Resolved 2026-04-22

Opt-in via `Config.MmapSize int64` (bytes, 0 disables — matches SQLite's
`PRAGMA mmap_size` default). When enabled, DB-file page reads memcpy
from an `mmap`-backed region instead of calling `ReadAt` per page.
Writes remain `WriteAt` and are coherent via the OS unified page
cache (linux/darwin).

Model: SQLite's `unixFetch` / `unixUnfetch`
(`sqlitec/src/os_unix.c:5714-5772`) simplified by dropping the
zero-copy fetch pointer (we copy out) and the `nFetchOut` reference
counting that goes with it. Mapping is lazy (first fetch calls
`syscall.Mmap`, analog to `os_unix.c:5727`), remaps on miss for
growth (`os_unix.c:5570-5640` analog via `munmap` + `mmap`), and
unmaps on `pager.close` (`os_unix.c:5562-5566`). Read-path gate
follows `getPageMMap → getPageNormal` shape at `pager.c:5670-5710`.

Measured delta on `BenchmarkOverflow10MB_FindId` with
`MmapSize=64 MiB` (warm-cache): sec/op -6.8% trend, p=0.093 — not
statistically significant, but direction right. See
`any-store-tests/results/session_perf/benchstat_mmap_reads.txt`.
Larger wins expected on cold-cache first reads (not benched) and
future partial-read APIs. Linux/darwin + amd64/arm64 only; no-op on
other platforms.

**Race safety:** `dbMmap.readAt(dst, off)` copies into `dst` inside
its RLock so concurrent `remap`/`unmap` (under Lock) cannot munmap
the backing memory mid-copy. Covered by
`TestDBMmap_ConcurrentReadVsRemap` which fails under `-race` in the
earlier `fetch(off, n) → slice` design and passes post-fix.

**Known follow-ups:** (1) If VACUUM / DB-file shrink is ever added,
`dbMmap.remap` must also shrink the mapping to avoid SIGBUS on the
now-unbacked tail (SQLite handles this in `unixTruncate` at
`os_unix.c:3999-4001`). (2) Dynamic resize via FCNTL is not
supported — `MmapSize` is fixed at `Open`. (3) The copy-out model
loses SQLite's zero-copy win; a follow-up could add a zero-copy path
with `nFetchOut`-style refcounting if the memcpy cost becomes
material (measurement shows it is secondary to syscall cost).

**VersionValidFor Integrity Check** -- Resolved 2026-04-22

`pager.open` compares `VersionValidFor` against `FileChangeCount`
after deserializing the DB header and returns `ErrCorrupt` on a
nonzero mismatch (zero is treated as "field not tracked" — legacy /
test fixtures skip the check). Commit path now advances both fields
in lockstep. Catches crash-mid-header-update corruption. Since
2026-11-11 the comparison is made by `lockBtreePageCount` on the page 1
the open snapshot reads (`pager.readPage1AtOpen`): a WAL page 1 that
supersedes a damaged file header is what gets judged. This is
stricter than SQLite, which only stops trusting the in-header size:
see [drift-2026-11-11-2](#drift-2026-11-11-2-nonzero-change-counter-mismatch-is-corruption).

**Auto-Checkpoint Errors Surfaced** -- Resolved 2026-04-22

`tx.pager.tryCheckpoint()`'s result (error or nil) is stored on
`DB.lastAutoCheckpointErr` and exposed via
`DB.LastAutoCheckpointError() error`. Monitoring code can poll the
accessor; the auto-checkpoint itself remains best-effort (returning
the error to the commit caller would change the application contract).

### Page Cache Memory Management (page_slab.go, pcache.go, db.go)

**Global Slab Allocator** -- Severity: N/A (implemented)

Process-global `pageSlab` singleton allocates one buffer at init and carves it
into page slices. Configured via `ConfigPageCache(pageSize, nPages)` before
opening any database; `Open` with `UsePageSlab` fails with
`ErrPageSlabNotInitialized` otherwise.
Matches SQLite's `sqlite3_config(SQLITE_CONFIG_PAGECACHE)` /
`sqlite3PCacheBufferSetup` (`pcache1.c:271-291`).

Key components:
- `pageSlab.Get()`: pops from free list; falls back to the `sync.Pool`, then `make()` (overflow)
- `pageSlab.Put()`: returns slab buffers to the free list; overflow buffers go to the `sync.Pool`
- `pageSlab.UnderPressure()`: atomic bool, true when `len(freeList) < nReserve`
  (`nReserve`: if nPages > 90 then 10 else nPages/10 + 1, matching `pcache1.c:279`)
- All page buffer allocations (pcache create, bulk alloc, temp pages) go through
  the slab; all deallocations (clear, discard, truncate, recycle temp page)
  return buffers to the slab

**Per-Cache Bulk Allocation** -- Severity: N/A (implemented)

`pcache.initBulk()` pre-allocates up to 20 page structs (matching
`SQLITE_DEFAULT_PCACHE_INITSZ`) with data buffers from the heap on first
`create()` call. In slab mode, initBulk is skipped (SQLite disables bulk init
when `SQLITE_CONFIG_PAGECACHE` is set, `pcache1.c:730-737`). Subsequent creates
pop from the `pFree` list. Matches SQLite `pcache1InitBulk`
(`pcache1.c:297-330`).

**Admission Control (createFlag)** -- Severity: N/A (implemented)

`pcache.create(pgno, createFlag)` with createFlag 1 (soft, readers) or 2 (hard,
writers). Soft creates return nil when:
- 90% of `maxPages` are pinned (thrashing), or
- Global slab is under pressure AND `nRecyclable < nPinned` (low recycling ratio)

Readers fall back to uncached `readTempPage()` when soft create returns nil.
Matches SQLite `pcache1.c:881-892` (step 3 guards).

**Persistent Reader Cache** -- Severity: N/A (implemented)

Reader caches are returned to `readerCachePool` on `Rollback()` with pages
intact. On next `BeginRead()`, the cache is cleared only if:
- `dataVersion` differs (monotonic counter incremented on every write commit), OR
- `walMaxFrame` differs

The dual check avoids two failure modes: `walMaxFrame` alone suffers ABA after
checkpoint restart; `dataVersion` alone has a TOCTOU race (WAL mxFrame updated
before dataVersion increment). Matches SQLite `pager.c:3246-3267`
(`pagerBeginReadTransaction` — `pager_reset` only if change-counter changed).

**Max Concurrent Readers** -- Severity: N/A (implemented)

`Options.MaxReaders` (default 4) configures a buffered channel semaphore
(`readerSem`) that limits concurrent read transactions per DB. Bounds total
memory from persistent reader caches. `closeCh` channel unblocks goroutines
waiting on the semaphore when `DB.Close()` or `DB.SetClosing()` is called.
No SQLite equivalent — our addition for the many-open-databases scenario.

**In-Process Write Lock and Context-Bounded Waits** -- Severity: N/A (implemented)

SQLite's default build never blocks on a lock (an `SQLITE_ENABLE_SETLK_TIMEOUT`
build blocks on the WRITER lock, `btree.c:3664-3737`): a busy lock returns
`SQLITE_BUSY`, and the busy handler sleeps and retries (`sqlite3InvokeBusyHandler`
`main.c:1770-1780`, the sleep in `sqliteDefaultBusyCallback` `main.c:1718-1757`, the
loop in `btreeBeginTrans`); `sqlite3_interrupt` does not end that sleep, only an
application busy handler that consults `sqlite3_is_interrupted` can. Readers retry
`walTryBeginRead` on a sleep schedule of its own (`wal.c:3040-3070`, up to
`WAL_RETRY_PROTOCOL_LIMIT`). The Go port serializes the writers of one process on
`writeSem`, a capacity-1 channel (`DB.lockWriter`), and its readers on `readerSem`;
both waits end with the resource, with the caller's ctx (`BeginWriteContext`,
`BeginReadContext`: `ctx.Err()`) or with close (`ErrClosed`). ctx is checked
before the wait, as `vdbe.c:902` checks `isInterrupted` before a statement starts,
so a ctx that has ended takes nothing. A capacity-1 channel hands the lock to its
waiters in FIFO order and gives the release→acquire happens-before the writer-owned
state relies on. The waits taken while the write lock is held — the busy handler
behind another process's writer (`walBusyLock`), the BUSY_SNAPSHOT retry, the
WAL_RETRY loop — keep SQLite's shape: bounded by the busy timeout and by the
retry limit (`ErrProtocol`), not by ctx. Our addition for the
many-goroutines-one-DB scenario.

**Implemented Optimizations:**
- `dirtyTail` pointer for O(1) spill victim search — walks backward from tail
  to find the oldest unpinned dirty page, matching SQLite's `pDirtyTail`
  search direction (`pcache.c:463-469`). No `pSynced` pointer needed because
  `PGHDR_NEED_SYNC` is irrelevant in WAL-only mode.

**Page-Cache Hash Table (apHash port)** -- Resolved 2026-05-22 (drift #2)

The page→struct lookup was a Go `map[uint32]*page`. It is now a SQLite-`pcache1`-style
chained hash (`pcache.apHash []*page` + `page.hashNext`), a direct port of
`PCache1.apHash`/`PgHdr1.pNext` (`pcache1.c:200,122`):
- `hashFind` is a single chained-bucket probe — `apHash[pgno & (nHash-1)]` walked
  by `hashNext` — matching `pcache1FetchNoMutex` (`pcache1.c:1009-1010`). `nHash` is
  always a power of two so the bucket index is a mask. `fetch`/`create` share it.
- `hashInsert`/`hashRemove` link/unlink at the bucket head and maintain `nPage`,
  matching `pcache1RemoveFromHash` (`pcache1.c:601-613`). `resizeHash` doubles and
  rehashes at load factor 1.0 (`nPage >= nHash`), matching `pcache1ResizeHash`
  (`pcache1.c:535-567`, floor 256).
- **Ghost-page invariant:** unlike SQLite (whose invariants forbid removing a
  pinned page from the hash), v2's `discard`/`truncate`/savepoint-rollback can
  remove a page while a caller still holds it pinned. Membership is therefore
  carried on the page via `page.inCache` (set by `hashInsert`, cleared by
  `hashRemove`); `release` adds to the LRU only when `inCache` is true. This
  replaces the former second Go-map lookup (the `pc.pages[p.pgno]==p` re-probe in
  `release`) with a field read, and keeps `pcache1Unpin`'s hash-op-free release
  path (`pcache1.c:1076-1103`).
- Eliminates the two `runtime.mapaccess1_fast32` calls per page touched on every
  fetch/release; measured ~50% faster `Fullscan/Count` (81µs → 40µs) and ~59%
  faster `IterParse/FullScanCount` on the 10k-row `noIdxColl`, with no change to
  B/op or allocs/op. No on-disk, WAL, dirty-list, or eviction-order change.

**Known Drifts in Page Cache:**
<a id="old-drift-pcache-buffer-reuse-on-eviction"></a>
- Buffer reuse on eviction: **Severity:** none — matches SQLite step 4 (`pcache1.c:897-914`) — since
  commit `acf91a0`, `create()` keeps the evicted victim as `recycled` and reuses
  its buffer in place (`resetPage` keeps the content; the caller fills or clears it) for **both** writer and reader
  caches (gated on `pc.purgeable`, not `xStress`). Only *surplus* evicted buffers
  beyond the kept one go back to the slab in `clear()`/`discard()`/`truncate()`;
  `evictOne` does not free the kept victim's buffer.
<a id="old-drift-release-no-reuse-unlikely-hint"></a>
- No `reuseUnlikely` on unpin: **Severity:** low — SQLite's `pcache1Unpin` accepts a
  `reuseUnlikely` flag (`pcache1.c:1079`); when true, pages are immediately
  freed. Our `release()` does not have this hint. Overfull eviction
  (`nPage > maxPages`) matches SQLite's `pGroup->nPurgeable > nMaxPage`
  check (`pcache1.c:1094`). `sqlite3PcacheDrop` maps to our `discard()` method.
<a id="old-drift-merged-fetch-fetchstress"></a>
- Merged Fetch+FetchStress: **Severity:** low — SQLite splits page acquisition into
  `sqlite3PcacheFetch` (soft create, may return NULL) and
  `sqlite3PcacheFetchStress` (spill + hard retry) as separate calls from the
  pager (`pcache.c:403-490`). Our `create()` merges both into a single
  function with inline stress handling, and returns the stress callback's
  error as `sqlite3PcacheFetchStress` returns one other than `SQLITE_BUSY`
  (the spill here returns no busy error).
<a id="old-drift-no-ecreate-state-machine"></a>
- No `eCreate` state machine: **Severity:** low — SQLite's `PCache.eCreate` toggles between 1
  (soft, when dirty list non-empty) and 2 (hard, when dirty list empty) so
  that `createFlag & eCreate` auto-selects the allocation strategy
  (`pcache.c:50,216-228,423`). Our `create()` takes `createFlag` directly —
  readers always use 1 (soft), writers always use 2 (hard). The `eCreate`
  optimization avoids a futile stress callback when there are no dirty pages
  to spill; our merged `create()` handles this inline via `findSpillVictim`.
- No `xRekey`: page renumbering (`pcache1.c:1111-1152`) is absent because
  auto-vacuum is not implemented (see "Not Implemented" section).
- No `xShrink`: `pcache1Shrink` (`pcache1.c:837-847`) frees all unpinned
  pages on demand. No external memory pressure API exists yet.
<a id="old-drift-non-purgeable-skip-lru"></a>
- Non-purgeable caches skip LRU: **Severity:** none — SQLite's `pcacheUnpin` (`pcache.c:265-271`)
  is a no-op for non-purgeable caches. Our `release()` matches this by
  guarding LRU operations with `pc.purgeable`.
<a id="old-drift-initialized-lock-free-atomic"></a>
- `Initialized()` is lock-free: **Severity:** none — uses `atomic.Bool` for the `initialized` flag.
  `pageSize` is immutable after Init, read without mutex via acquire semantics
  from the atomic load. Matches SQLite's mutex-free reads of `pcache1.isInit`
  and `pcache1.szSlot` (`pcache1.c:220-222`).
  **Status:** the `Initialized()` accessor is removed as unused; the lock-free
  atomic pattern it documented lives on in `pageSlab.initialized` / `Reset`.

**Future Improvements:**
- Shrink API (`sqlite3PcacheShrink` equivalent) for external memory pressure
- Slab telemetry: expose nTotal, nOverflow, underPressure via metrics

### B-tree Operations

<a id="old-drift-balance-nonroot-3sibling-index-btree-deviations"></a>
**Full 3-Sibling Redistribution (`balance_nonroot` port)** -- Resolved (insert side commit 4834f89; delete side a57d3d7)

**Severity:** low

A faithful port of SQLite's `balance_nonroot()` lives in `balance.go` (`balanceNonroot`):
it gathers the over-/under-full child plus up to two adjacent siblings (NB=3), pools their
cells with the parent dividers, recomputes the minimum output-page count k (from nOld-1 up to
nOld+2 = `nbMaxOut`; more than nOld+1 only with cells larger than half a page, which schema
format 6 leaves can hold), packs each page full then backs off the last for balance, and rewrites the parent
dividers — producing SQLite's high, even fill. It is now the live path for **both** sides:
inserts funnel an over-full leaf through it (`splitLeafAndInsertWithPath` → `balanceNonroot`,
btree.go:2055), and deletes funnel an under-full (>2/3 free) leaf through the same balancer
with `inject.active=false` (`deleteRebalanceLeaf` → `balanceNonroot`, btree.go:2799;
`completeMergeUpward` cascades parent under-fullness, gating single-child collapse on root per
btree.c:8942). A root leaf goes through `balance_deeper` and then the same balancer
(`splitRootLeafAndInsert`), and so does an update that grows a cell past its page
(`updateLeafCell`); no 2-way leaf split remains. `balance_quick` is retained as the rightmost-append fast path
(below). See `balance.go`'s header for the enumerated index-btree deviations, and
`any-store-tests:docs/any-store/btree/plans/2026-05-23-balance-nonroot-3sibling.md` /
`2026-05-23-delete-time-rebalancing.md`. Deferred (optional): first-key divider advance on
delete and retiring the now-dead `tryMergeLeaf`.

<a id="old-drift-balance-quick-first-key-divider"></a>
**Rightmost-Append Fast Path (balance_quick port)** -- Resolved 2026-04-23

**Severity:** none (intentional, correctness-required semantic adaptation — not a gap)

SQLite's `balance_quick` (`btree.c:7992-8086`, dispatched at
`btree.c:9169-9192`) handles the "rightmost append into the rightmost
leaf of a non-root parent" case without redistributing cells: it
allocates a fresh right sibling, puts only the new cell there, leaves
the old page 100% full, and adds a divider to the parent.

Any-store's port lives in `splitLeafRightmostAppend` (btree.go:1969-2008;
divider `:= bytes.Clone(key)` at btree.go:1998) with dispatch at the top of
`splitLeafAndInsertWithPath` (btree.go:2016-2027; preconditions at
btree.go:2022-2024). Four of SQLite's five preconditions
(`btree.c:9170-9174`) map directly:

    idx == pg.header.cellCount
    len(path) > 0
    path[len-1].cellIdx == path[len-1].nCell  (reached via rightChild)
    path[len-1].pgno != bt.rootPage           (SQLite: pParent->pgno != 1)

SQLite's fifth precondition `pPage->intKeyLeaf` is intkey-specific and
does not apply to any-store's index-btree semantics. Any-store's interior
search (`btree.go searchInterior`) invariant is "left child keys `<`
separator, right child keys `>=` separator" whereas intkey tables use
"left child keys `<=` separator". The consequence is that any-store's
divider must be the *first* key of the new right sibling — the new key
itself — rather than the largest key of pPage (which SQLite uses per
`btree.c:8066-8070`). This semantic adaptation is documented in
`splitLeafRightmostAppend`'s function comment.

Measured on 5000 monotonic appends at pageSize=1024, valSize=80:

    leaves:          714 → 488  (−31.6%; +56.9% overhead → +7.3%)
    avg leaf fill:  60.7% → 88.7%
    median fill:    60.6% → 95.3%

Guarded by `TestBalanceQuick_AppendFillFactor` (the former diagnostic,
now asserting `avgFill >= 0.85` on the monotonic case) and the
`TestBalanceQuick_*` matrix in `btree_balance_quick_test.go`
(HappyPath, RootIsParent, CascadeToParentSplit, InterleavedInserts,
OverflowBearingCell, SavepointRollback, ConcurrentReader,
AllocFreelistCorruptResilience).

Benchmark: `BenchmarkBalanceQuick_MonotonicAppend` reports rows/sec,
final leaf count, and leaves/row. At pageSize=4096, valSize=128,
10000 rows: 474 leaves (≈0.047 leaves/row, near the ideal packing).

<a id="old-drift-no-freeblock-chain-fragbytes-rebuild"></a>
**No Full Freeblock Chain** -- Severity: Low (partially addressed)

**Severity:** low

SQLite maintains a sorted linked list of free blocks within each page for
fine-grained space reuse. We implemented in-place update (when new cell <=
old cell size), in-place delete (with fragmentation tracking), and
defragmentation-before-split. Our approach tracks fragmentation in `fragBytes`
and triggers a full rebuild when it exceeds 60 bytes.

### Freelist

<a id="old-drift-freelist-trunk-fill-bound-corruption-ceiling"></a>
**Freelist Formula Respects Reserved Space** -- Resolved (stale drift note)

**Severity:** low

`freelistMaxLeaves()` uses `(p.usableSize() - 8) / 4` where `usableSize`
is `pageSize - ReservedSpace`. Correct regardless of ReservedSpace
value. See `pager.go:1528-1529`; the append site is `pager.go:1569`
(guarded by `maxLeaves` at `pager.go:1561`) and the integrity bound is
`integrity.go:136`. Note this equals `usableSize/4 - 2` — SQLite's
freelist *corruption ceiling* (`btree.c:6891`), not its conservative fill
bound `usableSize/4 - 8` (`btree.c:6895`), so any-store packs up to 6 more
leaves per trunk than SQLite (see §10 drift). The divergence still exists,
but only ancient pre-3.6.0 SQLite would report these DBs as corrupt; modern
SQLite (>=3.6.0) reads them without error and the formula respects reserved
space, so severity is low.

<a id="old-drift-no-btalloc-exact-le-modes"></a>
**No BTALLOC_EXACT / BTALLOC_LE Modes** -- Severity: Minor

**Severity:** low

Only `BTALLOC_ANY` allocation mode. `BTALLOC_EXACT` and `BTALLOC_LE` are only
needed for auto-vacuum and locality hints.

<a id="old-drift-multiprocess-wal-structural-drift"></a>
### Multi-Process WAL Fix — Drift from SQLite

**Severity:** low

1. **writeHeader parameterization**: SQLite's `walIndexWriteHdr` (wal.c:942-954)
   copies `pWal->hdr` directly to SHM — no parameters. Our `walIndex.writeHeader`
   takes explicit `frameCksum`/`salt` parameters because `walIndex.hdr` and `wal`
   fields are separate structs.

2. **Separate snapshot vs checksum state**: SQLite uses a unified `pWal->hdr`
   for both the snapshot comparison in `beginWriteTransaction` and the checksum
   chaining in `walEncodeFrame`. We pass the reader's per-connection `tx.walHdr` as
   the snapshot (`readSnap`) and use `wal.cksum1/cksum2` for chaining because our
   checksum fields are separate.

3. **Explicit re-sync in beginWrite**: SQLite doesn't re-sync state in
   `sqlite3WalBeginWriteTransaction` — if headers match, `pWal->hdr` is already
   correct (because `walIndexTryHdr` populated it). We must re-sync `nFrame`,
   `cksum1/2`, and salts because they are separate fields. Also clears
   `writerCache` when state changed (SQLite's pager cache invalidation handles
   this differently).

4. **Internal retry — resolved 2026-04-22**: `DB.BeginWrite` now tight-retries
   at most `busySnapshotInnerRetries` (= 3) times, then routes through the
   configured `BusyHandler` for delays[]-table backoff (matching
   `sqliteDefaultBusyCallback` in `sqlitec/src/main.c:1717`), then surfaces
   `ErrBusySnapshot` to the caller — matching SQLite's caller contract
   (`sqlitec/src/wal.c:3714`). The 1000-attempt hidden loop is gone.

5. **Per-connection snapshot (superseded)**: this previously saved a single
   process-wide `readSnapshot` in `pager.beginWrite`. It was replaced by the
   per-connection `tx.walHdr`, captured at `BeginRead` and passed into
   `beginWriteWithSnapshot(readSnap)`; the `saveReadSnapshot`/`wal.readSnapshot`
   fields are deleted. See the "Per-connection `walHdr`" section below for the
   current design.

### Lazy `ensureHeaderInitialized` helper (commits 2026-04-18; removed 2026-11-11)

`wal.go:ensureHeaderInitialized` was an experimental lazy counterpart to
`walIndexReadHdr` (wal.c:2654-2759) that only tests called; production code
used eager SHM init via `wal.open` and never repaired an invalid header
afterwards. It is replaced by `wal.indexReadHdr`, the production port of
`walIndexReadHdr`, called from the multi-process read path and from
`checkpointWithMode`: see
[drift-2026-11-11-1](#drift-2026-11-11-1-attached-wal-index-recovery).
`ensureHeaderInitialized`, its in-process variant, the `inProcessInit` flag
and `syncFromSHMLocked` (its only caller) are deleted; `indexReadHdr` does
not sync process-global state on the valid-header path, which the reader
path must not do.

**Drifts from SQLite's `walIndexReadHdr`, as they stand for `indexReadHdr`:**

<a id="old-drift-ensureheader-triple-lock-recovery-gate"></a>
**Severity:** none

1. **Recovery lock set.** SQLite `walIndexRecover` holds the caller's
   `WAL_WRITE_LOCK` and takes `WAL_ALL_BUT_WRITE` up to `READ_LOCK(0)`
   exclusive, i.e. `WAL_CKPT_LOCK` (unless the caller holds it) and
   `WAL_RECOVER_LOCK` (wal.c:1395-1401). `indexReadHdr` takes the same set,
   `lockWrite → lockCheckpoint → lockRecover`, each a non-blocking `F_SETLK`;
   under contention with `checkpointWithMode` (which uses `CKPT → WRITE`
   order) the loser observes `ErrBusy` — no deadlock. Not a divergence.

2. **BUSY_RECOVERY signal.** SQLite's `walTryBeginRead` returns
   `SQLITE_BUSY_RECOVERY` when the `WAL_RECOVER_LOCK` probe is itself busy
   (`sqlitec/src/wal.c:3084-3108`). `tryBeginReadMultiProcessHdr` returns
   `ErrBusyRecovery` there, and `pager.beginReadHdr` hands it to the
   `BusyHandler` for every begin (see drift-2026-11-11-1). Not a divergence.

<a id="old-drift-headerondisk-mxframe-inference"></a>
**Severity:** none

3. **`headerOnDisk` shortcut in `syncFromSHMLocked`** (removed 2026-11-11
   with the function; kept for the history of the anchor): we inferred "on-disk WAL header exists" from `hdr.mxFrame > 0` rather than
   `fstat`-ing, relying on the invariant that `flushHeader` (the lazy
   header-write gate in `writeFrames`, `wal.go:2054-2061`) precedes every
   frame write. This is faithful to SQLite: `sqlite3WalFrames`
   (`sqlitec/src/wal.c:4060-4090`) likewise gates the wal-header write on
   `hdr.mxFrame==0` rather than `fstat`. A documented internal assumption,
   not a behavioral divergence.

### Per-connection `walHdr` — resolved (2026-04-18)

**Historical:** Earlier attempts to make `wal.open` cold (match SQLite's
`sqlite3WalOpen` at wal.c:1641-1739) regressed multi-process reliability
from ~87% to 0-27% because a lazy SHM-header-init from any reader path
mutated the writer's in-flight state. SQLite's `Wal*` is per-connection;
any-store multiplexes many goroutines on one `*wal`.

**Resolved by per-tx `walHdr` migration** (commits `f11bc5c` → `9c53a02`,
7 steps; see
`any-store-tests/docs/superpowers/specs/2026-04-18-per-connection-hdr-design.md`
for the design). Summary:

- `ReadTx.walHdr WalIndexHdr` holds the reader's SHM snapshot captured at
  BeginRead time (inherited by `WriteTx`). Replaces the shared
  `wal.readSnapshot` field.
- `ensureHeaderInitialized()` returns `(WalIndexHdr, error)` so callers
  stamp the hdr onto their tx rather than depending on a helper that
  mutates process-global state.
- Reader callsites route through `tx.walHdr.mxFrame` instead of
  `tx.walMaxFrame` (both populated during migration; step 5 narrowed the
  field semantics).
- `BUSY_SNAPSHOT` in `wal.beginWriteWithSnapshot` now compares live SHM
  hdr against the caller-supplied `readSnap` (from `tx.walHdr`) rather
  than `w.readSnapshot`. `saveReadSnapshot` deleted.
- `syncFromSHMLocked` narrowed to only update shared `walIndex` atomics
  (`maxFrame`, `mxCommitFrame`, `maxPage` via CAS-monotonic, `nBackfill`)
  plus `w.nFrame` (shared append cursor). Writer-private fields
  (`w.header.salt*`, `w.cksum1/2`, `w.writerHdr`) stay owned by
  `wal.beginWriteWithSnapshot`'s inline sync under `lockWrite`.
- Savepoint state widened: scalar `walFrame/walCksum1/walCksum2` →
  `walHdr WalIndexHdr`.
- Zero-arg `pager.beginWrite()` escape hatch — **resolved 2026-04-22.**
  The zero-arg form has been deleted; `pager.beginWrite(readSnap WalIndexHdr)`
  is now the single canonical entry point, making BUSY_SNAPSHOT bypass a
  compile-error rather than a code-path footgun. Tests that don't run
  multi-process scenarios pass `WalIndexHdr{}` explicitly — the compiler
  now forces an affirmative decision.

**Known preserved drifts:**
<a id="old-drift-inprocess-skips-shm-hdr-on-commit"></a>
**Severity:** low

- In-process mode skips SHM hdr updates on commit (`writeFrames`
  `!w.inProcess` guard). `db.BeginRead`/`BeginWrite` synthesize a minimal
  `walHdr{isInit:1, mxFrame:maxFrame}` in that mode so read paths
  consuming `tx.walHdr.mxFrame` see the correct frame ceiling.
- (Resolved 2026-04-22: the zero-arg `pager.beginWrite` escape hatch was
  deleted — see above.)

**Reliability impact:** `TestMultiProcessIndex_ConcurrentSketchUpdates`
30-run samples at each step:

| Step | 30-run |
|------|--------|
| Baseline (step 0) | 28/30 |
| Step 1 (walHdr field) | 27/30 |
| Step 3 (reader routing) | 30/30 |
| Step 4 (BUSY_SNAPSHOT rewire) | 29/30 |
| Step 5 (delete readSnapshot) | 28/30 |
| Step 6 (savepoint widen) | 30/30 |

Post-migration reliability sits in the ~93-100% band per 30-run sample,
up from the ~77-95% pre-migration band.

<a id="old-drift-not-implemented-by-design"></a>
### Not Implemented (by design)

**Severity:** low

These SQLite features are intentionally absent:

- **Rollback journal mode** -- WAL-only
- **Auto-vacuum / incremental vacuum** -- no PTRMAP pages
- **Shared cache mode** -- single connection per database
- **SQL layer** -- no schema, no rowid tables, no SQL parsing
- **Integer-key (table) B-trees** -- index B-trees only
- **Multi-database transactions** -- single database per connection
- **WAL2 mode** -- standard WAL only
- **Database file locking (RESERVED/PENDING/EXCLUSIVE)** -- WAL mode uses SHM locks

---

## Online Backup (backup.go)

Port of SQLite's `sqlite3_backup_*` API from `sqlite/src/backup.c`. See
`any-store-tests:docs/any-store/btree/plans/2026-04-22-sqlite-backup-port.md` for the full drift
register and C↔Go coverage table. Key entry points:

- `(*DB).BackupInit(src *DB) (*Backup, error)` -- ~ `sqlite3_backup_init`
- `(*Backup).Step(n int) error` -- ~ `sqlite3_backup_step`
- `(*Backup).Finish() error` -- ~ `sqlite3_backup_finish`
- `(*Backup).Remaining() uint32` -- ~ `sqlite3_backup_remaining`
- `(*Backup).PageCount() uint32` -- ~ `sqlite3_backup_pagecount`

`pager.commit()` (after `wal.writeFrames`) and `pager.pagerStress()` (a
spill) dispatch the C equivalent of `sqlite3BackupUpdate`
(`pager.dispatchBackupUpdate`); a rollback, the error cleanup and any other
end of a write transaction that still owes the backups dispatch
`sqlite3BackupRestart` (`pager.dispatchBackupRestart`, `dischargeBackups`).
Another process's writes restart the copy through the change epoch each
Step's read transaction reports (`ReadTx.ChangeEpoch`). A `Step` returns
`ErrBusy`, without effect, while this process has a write transaction open on
the source. See
[drift-2026-11-13-1](#drift-2026-11-13-1-the-change-state-is-one-value-under-compare-and-swap),
"Backups".

Anystore-level `(*db).Backup(ctx, path)` drives the engine by opening
the destination at `path` with `(*DB).BackupDestinationOptions()`: the
source handle's options with the page geometry of the source file as it is
(see [drift-2026-11-19-2](#drift-2026-11-19-2-a-backup-destination-has-the-geometry-of-the-source-file)).

<a id="old-drift-backup-intentional-simplifications"></a>
**Key intentional simplifications** (full list in the plan document):

**Severity:** low

- Always same-page-size: any-store is WAL-only, so `backup.c:378-383`'s
  `SQLITE_READONLY` for WAL+size-mismatch is our `ErrBackupPageSizeMismatch`
  at init. The cross-size packing path at `backup.c:449-528` is
  therefore unreachable.
- Always the same reserved space and kind of page codec: `ErrBackupCodecMismatch`
  at init ([drift-2026-11-19-2](#drift-2026-11-19-2-a-backup-destination-has-the-geometry-of-the-source-file)).
- No `PENDING_BYTE_PAGE` handling -- any-store has no 1GB lock byte.
- No attached-db name resolution (`findDatabase`, backup.c:82) -- one b-tree per DB.
- No `nBackup` counter on source -- nothing for it to block (no
  VACUUM, immutable page size).

The KDF salt of an encrypted destination is the one its own codec was keyed
with, not the source's: see
[drift-2026-11-21-19](#drift-2026-11-21-19-the-kdf-salt-belongs-to-the-key-setup-not-to-page-1).

---

## Query Planner -- Index-for-ORDER-BY Matching (qplanner)

The cost-based planner decides whether an index satisfies a query's sort
order (so the index scan can stream rows in-order and stop at LIMIT,
avoiding an in-memory sort). That decision mirrors SQLite's
`wherePathSatisfiesOrderBy` (`sqlitec/src/where.c:5148`). The Go side lives
in `internal/qplanner/planner.go`: `IndexSortMatch` (planner.go:1499) for
*whether* an index covers the ORDER BY, and `shouldReverse` (planner.go:1101)
for *which* direction to scan it.

Two rules carry the equivalence:

1. **Equality-prefix skip.** SQLite skips leading index columns that are
   pinned by `==` / `IS` / `IN` constraints before matching ORDER BY terms --
   the `if( (eOp & eqOpMask)!=0 ){ ... continue; }` arm over
   `j < pLoop->u.btree.nEq` (`where.c:5314-5346`). Those columns are constant
   within the scanned range, so a sort on the *following* columns is still
   satisfied. `IndexSortMatch` takes an `equalityPrefix` count and tries the
   ORDER BY match both at offset 0 and at `equalityPrefix` (`matchAt(0)` /
   `matchAt(equalityPrefix)`, planner.go:1534-1539), keeping the longer match.

2. **Consistent composite asc/desc direction.** SQLite fixes a composite
   reverse flag on the *first* matched ORDER BY column --
   `rev = revIdx ^ desc; revSet = 1` -- then requires every subsequent column
   to satisfy `(rev ^ revIdx) == desc`, else it clears `isMatch` and stops
   (`where.c:5412-5426`). I.e. the sort must be consistently in-order or
   consistently reversed *relative to the index*; you cannot mix. `IndexSortMatch`
   encodes the same rule with `curSame := idxRev == sf.Reverse` and a
   `sameMode` latch: the first matched field sets `sameMode`, and any later
   field with `curSame != sameMode` breaks the match (planner.go:1518-1530).
   This is what lets `Sort("a","-b")` match the composite index `(a,-b)`
   (both fields "same" -> exact match) while `Sort("a","b")` does not.

Scan direction (SQLite's `*pRevMask |= MASKBIT(iLoop)` when `rev` is set,
`where.c:5422`) maps to `shouldReverse`, which returns the leading sort
field's `Reverse` to drive `IndexIter.Reverse` for the chosen index.

A full ORDER BY coverage returns `exactSort` (no `SortIter` in the chain --
see `BuildPlan` "Plan C: Index Scan", planner.go:415-504); a prefix-only
match returns `partialSort`, which still feeds a `SortIter` but lets it
exploit the partial ordering (`PartiallySorted`). This is verified by the
benchmark `compound_rev/SortAscDesc` + `compound_rev/FilterSort` scenarios
(any-store-tests), whose `Sort("a","-b")` now plans
`IndexScan(a,-b) -> Fetch -> Limit` instead of `FullScan -> TopK`.

## Audit-Discovered Drifts (2026-05-29)

The following drifts were found by an automated per-function C-vs-Go audit of the
b-tree port against sqlitec and deduplicated by root cause (the encryption/sqlcipher
codec is excluded here and tracked separately).

<a id="drift-4-beyond-file-pages-silently-zero-filled-skipping-header-valid"></a>
### Drift: Beyond File Pages Silently Zero Filled Skipping Header Validation
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.getPage` (`internal/btree/pager.go:963`).

When a requested pgno is beyond the physical file AND beyond the `dbSize` bound, the
Go readers (`getPageWriter` `pager.go:995-999`, `readTempPage` `pager.go:1098-1102`,
`getPageReader` `pager.go:1225-1229`) `clear(pg.data)` and return a zero page with no error;
the subsequent header parse is gated on `pg.data[off] != 0` (`pager.go:1065/1173/1294`),
so the page is returned with an empty `pageHeader{}` (`pageType=0`) and no
validation. C's `getAndInitPage` instead returns `SQLITE_CORRUPT_BKPT` for any
`pgno > btreePagecount(pBt)` before fetching (`btree.c:2386-2389`). The consequence is
an un-validated zero page entering the descent/read path in place of a corruption
error; severity is low because it is the same root failure surfaced by drift-3 and is
bounded by the per-snapshot `dbSize` check.

<a id="drift-6-wal-frame-read-failure-falls-through-to-disk-read"></a>
### Drift: WAL Frame Read Failure Falls Through To Disk Read
- **Category:** changed-logic  -  **Severity:** high  -  **Status:** RESOLVED 2026-07-10 (2026-07 pre-beta review): all four getters (`getPageWriter`, `readTempPage`, `getPageReader`, `readRawPage`) now propagate `readFrame`/`readFrameRaw` errors wrapped with page/frame context, matching C `readDbPage`, where with `iFrame != 0` the DB-file read is the unreachable else branch and a WAL read error becomes the page-get error (`sqlitec/src/pager.c:3035-3046`, 3.52.0). The "benign reset between lookup and read" TOCTOU race the fallthrough covered (introduced with commit `1e2cec7`) is structurally excluded: the writer path holds `lockWrite`, which any restart requires (`checkpointWithMode`; PASSIVE never resets, `checkpointPost`); readers hold a slot 1-4 shared lock that `tryResetWALWithBusy` must take exclusively; slot-0 snapshots carry `minFrame = mxFrame+1` so `walIndex.get` never resolves a frame for them (C `readLock==0` short-circuit); and the in-process slot-0 fallback now re-validates after acquiring read-0 (see the 2026-07-10 fallback re-validation fix, mirroring C's READ_LOCK(0) re-check at `wal.c:3136-3139`). Caveat: API calls that begin pager reads without the `db.mu` drain (`GetNamespace`, `ListNamespaces`, `IntegrityCheckN`) and race `DB.Close`'s WAL truncate may now surface a read error where they previously silently succeeded (contract-gray; follow-up). The adjacent residual in `refreshHeaderFromPage1` (a failed page-1 frame read fell back to the DB file's older page 1 under the write lock) is closed as of 2026-11-11: see the error-propagation paragraph of drift-76. Regression tests: `TestWALReadFrameErrorPropagates{,_Writer,_TempPage}`, `TestReadRawPageWALErrorPropagates`, `TestWALDecryptFailurePropagatesFromReadFrame`, `TestReadFrameNoBenignFailuresUnderTruncateStress`, `TestInProcessSlot0FallbackTruncateStress`.
- **Affected functions:** `pager.go:*pager.getPageWriter` (`internal/btree/pager.go:1014-1028`), `pager.go:*pager.readTempPage` (`internal/btree/pager.go:1114-1123`); the same fall-through also appears in the `getPageReader` cache path (`internal/btree/pager.go:1247`) and the `readFrameRaw` path (`internal/btree/pager.go:1357`).

In C `readDbPage`, once `sqlite3WalFindFrame` resolves a WAL frame (`iFrame != 0`) the
page's current version lives only in the WAL: it reads that frame via
`sqlite3WalReadFrame` and returns the result directly, with the DB-file read placed in
the `else` branch and therefore unreachable, so a WAL read failure propagates as the
page-get error (`pager.c:3031-3045`). The Go getters (`getPageWriter`
`pager.go:1014-1028`, `readTempPage` `pager.go:1114-1123`, and likewise the
`getPageReader` cache path `pager.go:1247` and the `readFrameRaw` path `pager.go:1357`),
after the WAL index reports a frame > 0, attempt `wal.readFrame` and return on success
but on failure fall through to a DB-file read. Go now documents this fall-through as
intentional rather than an oversight: a comment at `pager.go:1024-1027` explains it
covers the case where the WAL was reset (checkpointed and truncated) between the
`index.get` lookup and the `readFrame`, so the page has already been written to the DB
file by the checkpoint; a test-only fault-injection hook `walReadFrameFaultHook`
(`wal.go:2478-2486`) exercises the path. The residual hazard is that the same `err ==
nil` gate also swallows genuine I/O and codec-integrity failures: when the
authoritative WAL copy of a page cannot be read for those reasons, Go silently
substitutes the older committed-DB-file version instead of surfacing the WAL error,
which can return outdated page content as if it were current.

<a id="drift-2026-07-10-1-readframe-past-tail-read-failure-remapped-to-errwalcorrupt"></a>
### Drift: A Short Read Of A WAL Frame
- **Category:** error-handling  -  **Severity:** low  -  **Status:** RESOLVED: a short read of a WAL frame is `ErrWALShortRead`, the counterpart of `SQLITE_IOERR_SHORT_READ`, and it does not match `ErrCorrupt`.
- **Affected functions:** `wal.go:frameReadErr`, `wal.go:frameReadResult`, `wal.go:*wal.readFramePageHook`, `wal.go:*wal.readFrameRaw`, `wal.go:*wal.readFramePgno`, `wal.go:*wal.buildBackfillMap`, `wal.go:*wal.checkpointWithMode`, `wal.go:*wal.rewriteChecksums`, `wal.go:*wal.recoverLocked`, `pager.go:*pager.readWalFrameDataRaw`.

C `sqlite3WalReadFrame` (`wal.c:3649-3664`) is a bare `sqlite3OsRead` with no bounds check: `return sqlite3OsRead(pWal->pWalFd, pOut, (nOut>sz ? sz : nOut), iOffset);`. A read past the physical WAL tail surfaces as the OS layer's code (`SQLITE_IOERR_SHORT_READ` from `unixRead`), and `readDbPage` forgives short reads only on the DB-file branch, never the WAL branch (`pager.c:3042-3045`). The checkpoint's reads and a commit's reread of the frames whose checksums it rewrites return `sqlite3OsRead`'s code too (`walCheckpoint`, `wal.c:2311-2312`; `walRewriteChecksums`, `wal.c:4002`, `4010`).

Every read of a WAL frame outside recovery that does not return all its bytes with no error goes through `frameReadResult`, and nothing else does: the page readers, the checkpoint's reads of the frame headers and pages, and the commit's reread in `rewriteChecksums`. It judges the read by its count as well as its error. All the bytes with `io.EOF`, which `ReadAt` may return for a read that ends at the end of the file, are a read that succeeded. Fewer are a short read (`frameReadErr`), whatever the error that came with them: `ErrWALShortRead`, with the read's `io.EOF` or `io.ErrUnexpectedEOF` in the chain, and `io.ErrUnexpectedEOF` when a file system returned a short count with no error, as some do, which would otherwise hand a partial page to the codec, a checksum failure, damage. `ErrWALShortRead` means the wal-index points at a frame the file does not hold. Like `SQLITE_IOERR_SHORT_READ` it is an I/O error and not damage: the database file is sound, and an open after every handle has closed rebuilds the index from the log. Every other failure of the read is returned as it is, as in C. Recovery reads the log's header and its frames only within the file's size, which it took under its locks, and judges each read by its count too (`recoveryReadErr`): all the bytes with `io.EOF` are a read that succeeded, and fewer came up short on their own: the read's error (`io.ErrUnexpectedEOF` when it gave none), not a header of zeros, which would start the log over, and not `ErrWALShortRead`, whose remedy, an open after every handle has closed, runs the recovery that failed. `walIndexRecover` returns the read's code (`wal.c:1429-1431`, `1502-1503`).

The frame-number guards in front of the read (frame 0, a frame above the count of a log only this process writes, an in-memory frame that is missing) are assertions in SQLite's terms: they return `errWalFrameRange`, which matches no exported error.

Tests: `TestFrameReadErr`, `TestWALClosedFileReadIsErrClosed`, `TestRewriteChecksums_ShortReadIsErrWALShortRead` (`wal_test.go`), `TestWALFrameReadErrorsThroughEveryReader` (`pager_readfault_test.go`), `TestWALFrameRead_CountsItsBytes`, `TestWALRecovery_ShortReadOfTheHeader`, `TestWALRecovery_ShortReadOfAFrame` (`pager_short_read_vfs_test.go`), `TestCheckpointWithMode_FileReadPageDataError` (`pager_test.go`), `TestIntegrityCheck_ShortWALReadKeepsItsIdentity` (`integrity_test.go`), `TestFlush_ShortWALReadIsErrWALShortRead` (`test/db_test.go`).


<a id="drift-7-short-db-file-read-treated-as-hard-error"></a>
### Drift: A Short Read Of A Page Of The Database File Is An Error, Not Zeros
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.dbFileReadErr`, and its callers `pager.go:*pager.readDBPage` (every page getter's read of the database file, `readRawPage` and `readRawPage1Header` included), `pager.go:*pager.readPage1AtOpen`, `pager.go:*pager.proveKeyAtOpen`; the open's first read of the header, `pager.go:*pager.open`, and `pager.go:*pager.headerErrorOfUnwritableFile`.

C `readDbPage` maps a short read of a page of the database file to success: after
`sqlite3OsRead`, `if( rc==SQLITE_IOERR_SHORT_READ ){ rc = SQLITE_OK; }`
(`pager.c:3042-3044`), and `unixRead` has zero-filled what it did not read
(`os_unix.c:3576`). The page getters call it only for a page within the database's
size (`getPageNormal`, `pager.c:5590`, `5620`), so the zeros stand in for bytes the file does not
hold. A checksum, an authentication tag or the page's format can catch them; in a plain
database nothing does when they fall in a payload: a value whose last overflow page the file
ends inside reads back with zeros for its missing bytes, and a backup copies them.

Here a read of a page of the database file that does not return the whole page with no error
goes to `dbFileReadErr`, and nothing else does: a read that succeeds pays nothing.

- The whole page with `io.EOF`, which `ReadAt` may return for a read that ends at the end of
  the file, is a read that succeeded.
- Fewer bytes, with `io.EOF`, `io.ErrUnexpectedEOF` or no error: the file's size (`Stat` on the
  descriptor) decides. A file that holds the page's bytes had a read come up short on its own,
  as a file system that can return a short read does: the read's error is returned
  (`io.ErrUnexpectedEOF` when it gave none), and the page is not damaged. A file that ends
  inside or before the page holds it only in part: `errPageCutShort`, which matches
  `ErrCorrupt`, for a plain page as for one with a checksum or a tag.
- Any other error of the read is returned as it is, as in C.

At open, the first read of the header, under the DB-file lock, is judged by the file's size
that the open took under the lock: a file shorter than the header is `ErrHeaderCorrupt`, and a
read that comes up short of a header the file holds is the read's error, where
`sqlite3PagerReadFileheader` zero-fills its buffer before the read (`memset(pDest, 0, N);`,
`pager.c:3899`) and takes a short read for success (`pager.c:3911-3913`), and `lockBtree` then
fails the zeros' magic. A file that opens only for reading has its header judged only from a
read that returned all of it (`headerErrorOfUnwritableFile`); any other read leaves the open's
own error.
Page 1 is then read the same way as every page, and a page 1 the file ends inside past its
header is reported after the header and the size have been judged, which it does not change
(`readPage1AtOpen`). The key probe counts a page the file ends inside with the pages that fail
their authentication, and a short read of bytes the file holds as a page it could not read
([drift-2026-11-24-3](#drift-2026-11-24-3-open-tells-a-wrong-key-from-a-damaged-page-1)).
`VerifyIntegrity`'s read of page 1's header alone, for a page 1 that cannot be read whole, is a
read of the database file as every other (`readRawPage1Header`).

A database written by a release before the dontWrite removal (resolved entry 5 under Known
Implementation Gaps) can have a freed page at the end that is in neither the WAL nor the file
until its next full checkpoint extends the file. A read of it (`Backup`, `VerifyIntegrity`) is
`ErrCorrupt` here, where C reads zeros.

**Known, left as is:**
- After that checkpoint the page is zeros on disk.
- In a plain file it reads as zeros.
- In a checksummed or encrypted file its checksum or tag fails. `Backup` and `VerifyIntegrity` then report `ErrPageIntegrity` until a write reuses the page.
- `IntegrityCheck`, which does not read freelist leaves, passes.
- SQLite's cksumvfs would pass such a page: the checksum of a page of zeros is zero (`cksmCompute`, `ext/misc/cksumvfs.c`).
- Only files written by v2.0.0-v2.2.1 can hold such a page. v2.3.0 writes every freed page's frame.

Tests: `TestReadDBPage_PageCutShortIsCorrupt` (`pager_test.go`), `TestReadDBPage_ShortReadOfBytesTheFileHolds`, `TestOpen_ShortReadInTheKeyProofIsAReadError`, `TestOpen_ShortReadOfPage1IsAReadError`, `TestOpen_ShortReadOfTheHeaderIsAReadError`, `TestOpen_ShortReadOfAnUnwritableFilesHeader`, `TestVerifyIntegrity_ShortReadOfPage1sHeader` (`pager_short_read_vfs_test.go`), `TestOpen_DirtyCatalogPageCutShort` (`test/dirty_open_test.go`), `TestReadFault_IterShortRead` (`iterator_vfs_test.go`).

<a id="drift-nocontent-realloc-savepoint-copy"></a>
### Drift: NOCONTENT Freelist Reallocation Under A Savepoint Is Journaled
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.allocateFromFreelist` (the `hasContent==false`, savepoint-open branch).

C fetches the leaf NOCONTENT and the fetch marks the page in every open savepoint's bitvec
(`getPageNormal`, `pager.c:5599-5611`), so the `sqlite3PagerWrite` that follows
(`btree.c:6728`) does not sub-journal it and ROLLBACK TO leaves the post-allocation bytes on
the freelist leaf. Go copies the zeroed page into the innermost savepoint, so the rollback
restores it as a dirty page that is written at spill or commit. Kept deliberately: the leaf's
bytes stay reachable without C's short-read zero-fill (the drift above), including for files
written before the dontWrite removal.

<a id="drift-8-max-page-count-sqlite-full-enforcement-absent"></a>
### Drift: Max Page Count SQLITE_FULL Enforcement Absent
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.getPage` (`pager.go:971 (getPageWriter, no mxPgno check); errors.go:37 (ErrFull defined but unused)`), `pager.go:*pager.getPageNoContent` (`internal/btree/pager.go:1309`), `pager.go:*pager.getPageReader` (`internal/btree/pager.go:1186`), `pager.go:newPager` (`pager.go:294 (newPager, missing mxPgno init); enforcement gap at pager.go:1480-1502 (allocatePageNear: p.dbSize.Add(1) at pager.go:1495 with no bound check)`).

SQLite initializes `pPager->mxPgno = SQLITE_MAX_PAGE_COUNT` (0xfffffffe) in
`sqlite3PagerOpen` (`pager.c:5049`) and enforces it at page-acquire/grow time: a
not-yet-cached page with `pgno > pPager->mxPgno` returns `SQLITE_FULL`, releasing the
page if `pgno <= dbSize` (`pager.c:5591-5598`). This both caps database growth (PRAGMA
`max_page_count`) and prevents the page number from overflowing the 32-bit pgno space.
The Go pager has no `mxPgno`/`maxPageCount` concept at all: `newPager`
(`pager.go:294`) initializes no such field, the getters (`getPageWriter`,
`getPageReader`, `getPageNoContent`) perform no ceiling check, `allocatePageNear` grows
via `p.dbSize.Add(1)` (`pager.go:1495`) with no bound, and the defined `ErrFull`
(`errors.go:37`) is unused. This gap is now explicitly tagged in-code via `DRIFT`
comments referencing this anchor (e.g. above `newPager`, `getPageWriter`,
`getPageReader`, `getPageNoContent`). The consequence is that database growth is never
capped and the 32-bit pgno guard SQLite relies on is absent.

<a id="drift-10-missing-refcount-greater-than-one-in-use-page-corruption-det"></a>
### Drift: Missing Refcount Greater Than One In Use Page Corruption Detection
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.getPageNoContent` (`internal/btree/pager.go:1309`).

SQLite fetches freelist/grow pages through `btreeGetUnusedPage` (`btree.c:2449`), which
after `getPageNormal` rejects any page whose pager refcount > 1 --
`if( sqlite3PagerPageRefcount((*ppPage)->pDbPage)>1 ){ releasePage; return
SQLITE_CORRUPT_BKPT; }` (`btree.c:2457-2461`) -- because a page pulled off the freelist
or appended past EOF that already has another outstanding reference means the same page
is simultaneously in use, i.e. corruption. The Go grow/freelist fetch path
(`getPageNoContent`, `pager.go:1309`) has no equivalent in-use / refcount detection.
The consequence is that a freelist or grow page that is corruptly aliased to an
already-in-use page is accepted silently instead of being rejected as `ErrCorrupt`.

<a id="drift-11-movetochild-child-page-ncell-greater-than-equal-one-descent-"></a>
### Drift: moveToChild Child Page nCell Greater Than Equal One Descent Guard Missing
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `btree.go:*Cursor.First` (`internal/btree/btree.go:3482-3518`), `btree.go:*Cursor.Last` (`btree.go:3524-3559`), `btree.go:*Cursor.Seek` (`btree.go:3564-3591`), `btree.go:*btree.descendChild` (`internal/btree/btree.go:123-136`), `db.go:*ReadTx.txDescendChild` (`db.go:1413-1422`), `db.go:*ReadTx.leftmostKeyAfter` (`db.go:1668-1704`).

C validates every child page entered during descent: `moveToChild` (and its inlined
copy inside `sqlite3BtreeIndexMoveto` / the seek paths) returns `SQLITE_CORRUPT_PGNO`
when a freshly loaded child has `pPage->nCell < 1` -- `btree.c:5459-5464`, inlined at
`btree.c:6235-6240`. The Go cursor descent paths now enforce this interior guard:
`Cursor.First` (`btree.go:3482-3518`), `Cursor.Last` (`btree.go:3524-3559`), and
`Cursor.Seek` (`btree.go:3564-3591`) load each child via `descendChild`
(`btree.go:123-136`), which rejects an interior child whose `cellCount < 1` as
`ErrCorrupt` (`btree.go:131-134`), so the guard is no longer wholly missing. Two
residual divergences remain: (a) Go does not treat an empty *leaf* reached during
descent as corruption -- `descendChild` guards only interior pages, and
`leftmostKeyAfter` maps an empty reached leaf to `ErrKeyNotFound` (`db.go:1702-1704`)
where SQLite would have returned `SQLITE_CORRUPT`; and (b) `txDescendChild`
(`db.go:1413-1422`), the descent helper used by `leftmostKeyAfter`
(`db.go:1668-1704`), bounds the child pgno but lacks even the interior `nCell < 1`
guard. The consequence is that a corrupt empty leaf, or an empty interior reached via
the `leftmostKeyAfter` index-cost path, is silently accepted during descent rather than
rejected as corruption. (The per-page intKey/page-type consistency aspect is tracked
separately by drift-12.)

<a id="drift-12-b-tree-kind-consistency-check-omitted-on-descent"></a>
### Drift: B Tree Kind Consistency Check Omitted On Descent
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*Cursor.Seek` (`btree.go:3564 (Cursor.Seek); btree.go:3574 (for pg.header.isInterior()); btree.go:123-136 (descendChild); page.go:308-310 (isInterior accepts both interior types)`).

SQLite validates the b-tree KIND (intKey / page-type) of every page touched during a
seek: `moveToRoot` rejects a root whose `intKey` flag disagrees with the cursor's
expectation (`(pCur->pKeyInfo==0)!=pRoot->intKey` -> `SQLITE_CORRUPT_PAGE`,
`btree.c:5597`), and `moveToChild` rejects any child where `pPage->intKey !=
pCur->curIntKey` (`btree.c:5460`, inlined seek at `btree.c:6236`). The Go `Cursor.Seek`
(`btree.go:3564`) path branches purely on `pg.header.isInterior()` (`btree.go:3574`),
and `isInterior` (`page.go:308-310`) returns true for both `pageTypeIntIdx`(2) and
`pageTypeIntTbl`(5) without ever checking that the page's kind matches the b-tree being
searched; `descendChild` (`btree.go:123-136`) likewise only guards `cellCount < 1`, not
the page kind. The consequence is that a page of the wrong b-tree kind (intKey vs index)
encountered during a seek is accepted and traversed instead of being rejected as
corruption. Because any-store only ever materializes index page types, the omitted check
is corruption-hardening rather than a functional intKey/index divergence; the inline
DRIFT marker already sits at `btree.go:3563`.

<a id="drift-14-b-plus-tree-traversal-drops-interior-cell-keys-versus-sqlite"></a>
### Drift: B Plus Tree Traversal Drops Interior Cell Keys Versus SQLite B Tree
- **Category:** changed-logic  -  **Severity:** none
- **Affected functions:** `btree.go:*Cursor.Next` (`btree.go:3965-4049`), `btree.go:*btree.countPage` (`btree.go:3375-3424`), `db.go:*ReadTx.Count` (`btree.go:3384-3388 (leaf-only count) vs 3390-3423 (interior: recurse, no cellCount add)`).

any-store is a B+tree: index keys live exclusively on leaves, so an interior cell is
only a separator/router, never a stored entry. SQLite's index B-tree instead treats
interior cells as first-class keys -- `btreeNext` returns `SQLITE_OK` positioned on the
interior separator after walking up via `moveToParent` (`btree.c:6357-6361`), and
`sqlite3BtreeCount` adds `pPage->nCell` on interior pages too because
`pPage->leaf || !pPage->intKey` (`btree.c:10511-10513`). Go's `Cursor.Next`
(`btree.go:3965-4049`) never pauses on interior positions and `countPage` adds
`cellCount` only on leaf pages (`btree.go:3375-3424`), recursing through interior pages
without counting them. The consequence is a structural design divergence that surfaces
in traversal (no interior-key stops) and in counting (interior cells excluded); for the
B+tree shape both results are correct, but they differ from SQLite's B-tree semantics.

<a id="drift-16-count-traversal-missing-interrupt-cancellation-check"></a>
### Drift: Count Traversal Missing Interrupt Cancellation Check — Resolved 2026-11-06; ctx polled per leaf
- **Category:** platform-support  -  **Severity:** low (the remaining granularity difference)
- **Affected functions:** `btree.go:*btree.countPage`, `btree.go:*Cursor.Next`, `btree.go:*Cursor.Previous`, `db.go:*ReadTx.NewCursorContext`, `db.go:*ReadTx.CountContext`.

SQLite's `sqlite3BtreeCount` (`btree.c:10489`) gates its page-walk loop on
`!AtomicLoad(&db->u1.isInterrupted)` (`btree.c:10502`), and the VDBE checks the flag
once per visited row at the loop-bottom opcodes (`vdbe.c:1078-1091`), so a long count
or scan can be cancelled via `sqlite3_interrupt` mid-walk. The Go port polls the caller's
ctx in the flag's place: `countPage` once per interior page, one poll per fan-out of leaves
(`ReadTx.CountContext`; a leaf costs only its header read, so a poll on every page was a
tenth of the walk), and a cursor
once per leaf it leaves in `Next`/`Previous` (`ReadTx.NewCursorContext`), which covers
`Skip`, `SkipBackward` and `CountUntil`. A ctx that has ended fails the move with
`ctx.Err()` and leaves the cursor on the entry it stood on, not tripped (`cursorFault`
is for I/O and schema faults); `Skip`, `SkipBackward` and `CountUntil` fail at the leaf
they were crossing, their count partial — their callers pass the error up and stop. Remaining divergence, deliberate: the VDBE's per-row check is a per-leaf check here,
where the cursor already pays for I/O, and the count's per-page check is per interior
page; one leaf's rows, or one fan-out of leaf headers, is the longest a walk runs past
its ctx. A plan that scans an index and fetches each matching document polls the index
cursor only, so the document fetches for one index leaf (hundreds of entries, an
overflowing document several pages each) run between polls, and a filter that rejects
most fetched documents runs past its ctx by that much; a poll every N fetches in the
fetch step would close the gap.

<a id="drift-17-count-return-type-truncates-i64-entry-total-to-go-int"></a>
### Drift: Count Return Type Truncates i64 Entry Total To Go int
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.Count` (`btree.go:3368`), `btree.go:*btree.countPage` (`btree.go:3375`).

SQLite declares the entry count as `i64 *pnEntry` (`btree.c:10489`), accumulates into a
local `i64 nEntry` (`btree.c:10490`, `nEntry += pPage->nCell` at `btree.c:10512`), and
writes it back via `*pnEntry = nEntry` (`btree.c:10529`), guaranteeing a 64-bit total
regardless of platform. Go's `Count`/`countPage` declare and accumulate the total as a Go
`int` (`btree.go:3368`, `total := 0` at `btree.go:3393`, with `total += c` at
`btree.go:3413`), whose width is platform-dependent. The consequence is that on a 64-bit target the behavior is
practically equivalent, but on a 32-bit target the total is 32-bit and could overflow for
a very large tree, whereas SQLite is always `i64`.

<a id="drift-26-leaf-cell-size-missing-four-byte-minimum-clamp"></a>
### Drift: Leaf Cell Size Missing Four Byte Minimum Clamp
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:leafCellSize` (`internal/btree/btree.go:298-300`), `btree.go:leafCellSizeFromLengths` (`btree.go:416-425`), `btree.go:leafCellSizeWithOverflow` (`btree.go:402-411`), `btree.go:parseLeafCellWithSize` (`internal/btree/btree.go:166-237`).

SQLite's `cellSizePtrTableLeaf` / `cellSizePtrIdxLeaf` and the cell parsers
(`btreeParseCellPtr` / `btreeParseCellPtrIndex`) clamp the computed non-overflow cell size
up to a 4-byte minimum -- `if( nSize<4 ) nSize = 4;` (`btree.c:1551`, `1478`, `1341`,
`1380`). This floor
is a hard on-disk-format invariant: when such a cell is later freed it is converted into an
intra-page freeblock, whose header needs at least 4 bytes (2-byte next-pointer + 2-byte
size). Go's size and parse routines (`leafCellSize` `btree.go:298-300`,
`leafCellSizeFromLengths`, `leafCellSizeWithOverflow`, and `parseLeafCellWithSize`) all
omit this minimum-cell-size clamp, returning the raw `hdr + payload` with no lower bound.
The consequence is that a degenerate tiny cell (sub-4-byte) would be sized below the
freeblock minimum, so freeing it could not store a valid freeblock header -- a deviation
from SQLite's free-space format guarantee that is harmless here because freed cells are
tracked as fragment bytes, not as freeblocks. Such cells are real: a one-byte key with an
empty value is a three-byte cell, placed at `usableSize-3` on an empty leaf. The integrity
check used to keep SQLite's `usableSize-4` bound on cell offsets (`btree.c:10954`) and
reported that cell as out of range; it now bounds the offset by the page end and relies on
the per-cell extent check (2026-11-11, `TestIntegrityCheck_ThreeByteCell`).

<a id="drift-28-searchleafpage-missing-overflow-cell-compare-guard"></a>
### Drift: searchLeafPage Missing Overflow Cell Compare Guard
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:searchLeafPage` (`btree.go:510-589`).

SQLite's `indexCellCompare` classifies an index cell by inspecting its payload-size varint
into three cases (`btree.c:5952-5982`): a single-byte payload or a 2-byte varint whose
`nCell <= maxLocal` may be compared against the cell's local bytes, but otherwise the record
overflows the page and it returns `c=99` to skip the local-bytes fast path and force the
full-key comparison. Go's `searchLeafPage` (`btree.go:510-589`) has no equivalent on-page
vs. overflow guard in its fast path. The consequence is that for an index cell whose payload
spills into an overflow chain, Go can compare against only the locally stored prefix as if it
were the whole key, yielding a silent wrong-key comparison and a potentially incorrect search
result. In practice `searchLeafPage` currently has no production callers (it is test-only),
so this wrong-key risk is latent unless a future caller routes overflow-bearing pages to it;
production search paths use `searchLeafWithOverflow` (`btree.go:601`), which handles overflow.

<a id="drift-29-root-interior-overflow-uses-2-way-split-not-balance-deeper-p"></a>
### Drift: Root Interior Overflow Uses 2 Way Split Not balance_deeper plus balance_nonroot
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.insertSepIntoInterior` (root-overflow block), `btree.go:interiorSplitPointFitting`, `btree.go:*btree.splitRoot`.

When a separator must be inserted into a full interior page that is the btree ROOT (no
grandparent to gather siblings from), Go does not run SQLite's `balance_deeper` +
`balance_nonroot` even-fill packing (`btree.c:9134-9151` routes a root overflow to
`balance_deeper`; `btree.c:9195-9214` then redistributes via `balance_nonroot`). Instead
`insertSepIntoInterior` falls to a classic 2-way split: collect the root's interior cells,
splice in the new divider, pick a split via `interiorSplitPointFitting` (a ~2/3 left-fill
target, moved until both halves fit), rebuild into two interior pages, and grow a new root
through `splitRoot`. The consequence is that root-level interior overflow produces a
different (and looser) page-fill distribution than SQLite's balanced redistribution. A root
LEAF does follow SQLite (`splitRootLeafAndInsert`). The parent rewrite after a balance
splits in place too; see
[its own entry](#drift-2026-11-11-7-parent-rewrite-after-a-balance-splits-in-place).

<a id="drift-31-rebuildinteriorpage-accepts-zero-cell-pages"></a>
### Drift: rebuildInteriorPage Accepts Zero Cell Pages
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.rebuildInteriorPage` (`btree.go:1879-1935`).

C's `rebuildPage` hard-asserts `nCell>0` (`btree.c:7648`) and its only callers
(`balance_quick`, the `editPage` fallback) guarantee at least one cell. Go's
`rebuildInteriorPage` is instead deliberately invoked with zero cells -- e.g. the
`removeChildFromParent` / collapse paths and `rebuildInteriorPage(rootPg, nil, ...)` -- and
faithfully produces an interior page carrying only a `rightChild` with `cellCount=0`. For a
ROOT this is a state SQLite has too: `balance_deeper` leaves the root with no cells and one
child (`btree.c:9074-9075`), and page 1 keeps it when the child does not fit back
(`btree.c:8942`); Go's root-leaf path and `collapseSingleChild` produce exactly that. The
drift is the rebuild helper's contract, and non-root pages: Go's structural contract for
interior pages is looser than SQLite's, which is the foundation that lets the
underfullness-cascade drifts (see Drift 20/21) leave single-child interior pages in the tree.

<a id="drift-33-missing-balance-self-ancestor-refcount-corruption-guard"></a>
### Drift: Missing balance Self Ancestor Refcount Corruption Guard
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.splitLeafAndInsertWithPath` (`btree.go:2016-2066`).

On the non-root general-balance path, SQLite's `balance()` driver rejects a corrupt tree
before redistributing cells: if the over-full non-root page being balanced has pager
refcount > 1 it returns `SQLITE_CORRUPT_PAGE`, because the only way a non-root page can hold
more than one reference at that point is if it is one of its own ancestor pages -- a cyclic
tree (`btree.c:9155-9159`). Go's dispatcher `splitLeafAndInsertWithPath`
(`btree.go:2016-2066`) has no refcount>1 / self-ancestor corruption guard. The consequence
is that a cyclic (self-referential) tree that SQLite would detect and reject as corruption
is instead followed by Go, which can loop or corrupt state while balancing.

<a id="drift-34-splitroot-missing-anothervalidcursor-corruption-guard"></a>
### Drift: splitRoot Missing anotherValidCursor Corruption Guard
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.splitRoot` (`btree.go:2476-2521`).

SQLite gates the `balance_deeper` (root-overflow) path on a corruption precondition: the
root-overflow branch only proceeds when `anotherValidCursor(pCur)==SQLITE_OK`
(`btree.c:9135`), where `anotherValidCursor` (`btree.c:9092-9103`) walks all other cursors on
the same `BtShared` and returns `SQLITE_CORRUPT_PAGE` if any other cursor is `CURSOR_VALID`
and positioned on the same page about to be deepened. Go's `splitRoot` (`btree.go:2476`)
deepens the root with no equivalent check. The consequence is that a state SQLite treats as
corruption -- another live cursor pinned to the page being restructured -- is allowed by Go,
risking that the second cursor is left referencing a now-stale/repurposed root page.

<a id="drift-35-legacy-superseded-insert-path-functions-undocumented"></a>
### Drift: Legacy Superseded Insert Path Functions Undocumented
- **Category:** changed-logic  -  **Severity:** low  -  **Status:** RESOLVED 2026-11-10: the cluster (`insertIntoPage`, `insertIntoLeaf`, `insertIntoInterior`, `splitLeafAndInsert`, the non-path `insertIntoParent`) and `leafSplitPoint` are removed; their tests drive `Put`.
- **Affected functions:** `btree.go:*btree.splitLeafAndInsert` (`btree.go:2362`).

The live insert path is `Put` (`btree.go:1196`) -> `path := pathBuf[:0]` (`btree.go:1216`)
-> `insertIntoLeafWithPath` (call `btree.go:1240`, def `btree.go:1255`) ->
`splitLeafAndInsertWithPath` -> `balanceNonroot`. A second, closed cluster --
`splitLeafAndInsert` (`btree.go:2362`), `insertIntoPage` (`btree.go:1247`), `insertIntoLeaf`
(`btree.go:1305`, falling through to `splitLeafAndInsert` at `btree.go:1350`),
`insertIntoInterior` (`btree.go:2524`), and the non-path `insertIntoParent` (`btree.go:2423`,
which falls back to `splitRoot`) -- forms a self-referential set with no production
(non-test, non-`WithPath`) entry point. It is dead/legacy code superseded by the `WithPath`
family. `splitLeafAndInsert` now bears a DRIFT-35 annotation marking it superseded
(`btree.go:2361`), but the remaining cluster members (`insertIntoPage`, `insertIntoLeaf`,
`insertIntoInterior`, and the non-path `insertIntoParent`) are still unannotated, so a reader
may mistake them for a live, divergent insert path. The consequence is purely a
documentation/maintenance drift.

<a id="drift-37-delete-rebalance-underfull-trigger-counts-fragbytes-as-used"></a>
### Drift: Delete Rebalance Underfull Trigger Counts fragBytes As Used
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.Delete` (`btree.go:2567`).

SQLite gates post-delete rebalancing on the page's *true* free space: after `dropCell`,
`balance()` is skipped iff `pCur->pPage->nFree*3 <= usableSize*2` (`btree.c:9987`), where
`nFree` (maintained by `freeSpace`, `btree.c:2012`) counts *all* bytes reclaimed from the
dropped cell as free, including non-coalescible fragmentation. Go's `btree.Delete`
computes its trigger as `nFree := usable - bt.leafUsedSpace(wpg)` then
`underfull := nFree*3 > usable*2` (`btree.go:2710-2712`), but `leafUsedSpace`
(`btree.go:2898-2908`) returns `cellPtrEnd + (usable - contentOff)`, i.e. it treats every
byte in the unallocated/fragmented region as *used*. Because Go's `nFree` excludes the
fragmentation bytes that SQLite's `nFree` includes, Go's underfull test fires *less* eagerly
than SQLite's. The consequence is that some pages SQLite would rebalance after a delete are
left under-occupied by the Go port, a benign space-utilization divergence rather than a
correctness defect.

<a id="drift-41-backup-empty-source-finalization-path-missing"></a>
### Drift: Backup Empty Source Finalization Path Missing
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `backup.go:*Backup.Step` (`internal/btree/backup.go:263-272`),
  `backup.go:*Backup.finalize` (`internal/btree/backup.go:307-356`, `backup.go:355`,
  `pager.go:3100-3101`).

On the final (DONE) iteration SQLite special-cases a zero-page source: inside the
`rc==SQLITE_DONE` block it runs `if(nSrcPage==0){ rc=sqlite3BtreeNewDb(pDest); nSrcPage=1; }`
(`backup.c:417-421`) -- rebuilding a fresh 1-page destination -- *before* bumping the schema
cookie and truncating. Go's done-path (`b.iNext > nSrcPage`) calls `b.finalize(nSrcPage)`
directly (`backup.go:263-272`) with no equivalent empty-source handling, so for
`nSrcPage==0` it invokes `finalize(0)` whose `truncateTo(0)` returns
`btree: cannot truncate to zero pages` (`pager.go:3100-3101`). The consequence is that
backing up an empty source database -- a no-op success in SQLite -- errors out in the Go
port instead of producing a valid 1-page destination.

<a id="drift-42-backup-finalize-omits-setversion-for-wal-destination"></a>
### Drift: Backup Finalize Omits SetVersion For WAL Destination
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `backup.go:*Backup.finalize` (`internal/btree/backup.go:307-356`).

When the destination is in WAL mode, SQLite's `finalize` calls
`sqlite3BtreeSetVersion(pDest,2)` to force page-1 file-format read/write version bytes 18/19
to 2 (`backup.c:429-431`; `btree.c:11509-11510` writes `aData[18]=aData[19]=2`). Go's
`finalize` (`backup.go:307-356`) performs only the schema-cookie bump and `DatabaseSize`
re-application and omits this `SetVersion` step. For any-store this is benign by invariant:
the engine is always WAL and every release has created files with both bytes set to 2, so
the source bytes copied into the destination are already 2. The destination's own header
does not guarantee it: `finalize` reloads the in-memory header from the source's page 1, so
a source whose bytes were 1 (accepted by the format gate, which only refuses values above 2)
is copied as 1. The consequence is a latent divergence that would only matter for such a
source, which no release writes.

<a id="drift-43-backup-commit-point-moved-from-step-to-finish"></a>
### Drift: Backup Commit Point Moved From Step To Finish
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `backup.go:*Backup.Finish` (`internal/btree/backup.go:263-272`,
  `backup.go:404-437`).

In SQLite the destination write transaction is committed *inside* `sqlite3_backup_step` on
the final DONE iteration via `sqlite3BtreeCommitPhaseTwo(pDest,0)` (`backup.c:535-539`), after
which `p->rc` becomes `SQLITE_DONE`; by the time `sqlite3_backup_finish` runs the destination
is already committed and durable, and finish's only transaction action is a no-op rollback
(`backup.c:600`). The Go port defers the destination commit to `Finish`
(`backup.go:404-437`): `Step`'s done-path only finalizes in-memory state (`backup.go:263-272`)
and the actual commit happens later. The consequence is that the durability boundary and the
point at which a commit error surfaces both move from `Step` to `Finish`, so a caller who
treats a successful final `Step` as "backup committed" can be wrong, and a commit failure is
reported from a different call than in SQLite.

<a id="drift-44-backup-finish-double-call-returns-error-not-no-op"></a>
### Drift: Backup Finish Double Call Returns Error Not No Op
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `backup.go:*Backup.Finish` (`internal/btree/backup.go:408-410`).

SQLite's `sqlite3_backup_finish(p)` is NULL-tolerant and idempotent: it returns `SQLITE_OK`
immediately when `p==0` (`backup.c:577`, within `sqlite3_backup_finish` `backup.c:571-619`) and
treats a second finish on a freed handle as a
benign no-op. The Go port adds an explicit `finished` flag (struct field, `backup.go:73-75`):
`Finish` returns `ErrBackupFinished` on a second call (`backup.go:408-410`) and `Step`
returns `ErrBackupFinished` after `Finish` (`backup.go:188-190`). The consequence is a
stricter, non-SQLite contract -- repeated finalization is surfaced as an error rather than
silently absorbed -- which can break callers (or wrappers) that rely on SQLite's idempotent
finish semantics.

<a id="drift-45-beginreadfast-skips-page-1-staleness-counter-reads"></a>
### Drift: Read Begin Takes The Page 1 Counters From A Memo
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*DB.beginRead`, `snapcounters.go:*snapCountersMemo`,
  `pager.go:*pager.readHeaderCountersAt`, `page1_check.go:*pager.checkSnapshotCounters`.

A read transaction carries the page-1 `FileChangeCount` and `SchemaCookie` of its snapshot
(`ReadTx.SnapshotHeaderCounters`, `SnapshotSchemaCookie`). SQLite has the cookie from the
transaction's own page 1: `sqlite3BtreeBeginTrans` runs `lockBtree` while `pBt->pPage1==0`
(`btree.c:3707`), `unlockBtreeIfUnused` drops the page when the transaction ends
(`btree.c:3489-3499`), and the cookie is `get4byte(&pBt->pPage1->aData[40])`
(`btree.c:3786`, consumed by `OP_Transaction`, `vdbe.c:4128`). `lockBtree` gets page 1
through the pager cache, and in WAL mode that cache outlives the transaction:
`pagerBeginReadTransaction` resets it only when `sqlite3WalBeginReadTransaction` reports a
change (`pager.c:3246-3267`), which is a `memcmp` of the whole wal-index header
(`walIndexTryHdr`, `wal.c:2624-2626`). The one per-transaction read of the counters from
the file (`dbFileVers`, `pager.c:5410`) is inside `if( !pagerUseWal(pPager) && ... )`
(`pager.c:5266`): rollback-journal mode only. So a WAL read begin reads page 1 from the
WAL or the database file once per snapshot, and nothing past the snapshot.

The Go port has the same rule with a different holder. A reader's page cache is one of a
pool, not the connection's, so the two counters are kept apart from it in
`snapCountersMemo` (`snapcounters.go`): one entry, keyed by what identifies a snapshot to
this process: the wal-index header (change counter, frame checksum, salts), or the header
synthesized in the in-process and in-memory modes (frame number, page count, restart
count), kept with the `dataVersion`. This is the key the reader caches are kept or cleared
by. `beginRead` takes the pair from the memo at the remembered snapshot and
otherwise reads page 1 strictly within the snapshot's `[minFrame, maxFrame]`
(`readHeaderCountersAt`) and remembers it. A commit of this process stores its own pair as
it becomes visible (`wal.onPublish`), so the first reader of a commit reads nothing either.

What the memo must never do is serve one snapshot's counters to another. The key is
SQLite's. The synthesized header has no salts; the restart count, which moves around a
restart's zeroing of the frame numbers like a seqlock, tells its generations apart. Within
a generation the published frame number only rises and a published frame is never
rewritten (frame reuse starts after the last commit, `iFirst`; a commit that fails before
publication may reuse frame numbers, but no reader was given them), so the synthesized
header alone identifies a snapshot. The `dataVersion` kept with it is a backstop. In test
builds (`SetCheckPage1ForTest`) every begin the memo serves is checked against a
page-1 read within the snapshot's bounds (`checkSnapshotCounters`), so every suite holds
the memo to page 1.

The memo is not SQLite's cache in three ways, none of which changes which counters a
snapshot gets:
- **Lifetime.** It has one entry for the process, replaced by whichever begin or commit
  stores last; it is not cleared on a failed begin or a wal-index recovery, where SQLite
  resets the cache (`pager.c:3261`; `*pChanged = 1` after `walIndexRecover`,
  `wal.c:2724-2725`), because its key already tells those snapshots apart. "Once per
  snapshot" is the common case, not a bound: a delayed begin at an older snapshot replaces
  the entry, and the next begin at the newer one reads page 1 again.
- **Scope.** It is shared by every reader of the process; SQLite's cache belongs to one
  connection.
- **Error timing.** A begin at the remembered snapshot reads nothing, so an unreadable
  page 1 fails the first page read that needs it, not the begin. SQLite fails the begin in
  `lockBtree` only when page 1 is not in its cache, which it may evict between
  transactions.

`readHeaderCounters`, which raises the frame bound to the live header, is left for `Open`.
`Open` holds a reader slot for that read (`db.go`, after `wal.onPublish` is set), but what
it reads seeds the handle's schema cookie (`LocalSchemaCookie`), not a transaction's
snapshot pair.

<a id="drift-46-public-multi-process-staleness-api-diverges-from-sqlite-auto"></a>
### Drift: Public Multi Process Staleness API Diverges From SQLite Auto Tracking
- **Category:** new-feature  -  **Severity:** medium
- **Affected functions:** `db.go:*WriteTx.MarkSchemaChanged`, `db.go:*WriteTx.Commit`,
  `pager.go:*pager.commit`, `db.go:*ReadTx.SnapshotHeaderCounters`,
  `db.go:*ReadTx.SnapshotSchemaCookie`, `db.go:*ReadTx.DiskSchemaCookie`,
  `db.go:*DB.LocalSchemaCookie`.

any-store's schema cookie is caller-driven where SQLite's is automatic. SQLite moves the
Schema Cookie (offset 40) as the statement that changes the schema executes
(`OP_SetCookie`, `vdbe.c:4249`). any-store makes the bump opt-in:
`WriteTx.MarkSchemaChanged()` sets `tx.schemaChanged`, `Commit` forwards it to
`pager.commit`, and `pager.commit` moves the cookie only when it is set. (The File Change
Counter, offset 24, has its own rule per schema format: see
[drift-77](#drift-77-filechangecount-bumped-conditionally-not-unconditionally). In WAL mode
SQLite writes it only into a page 1 the commit writes anyway, `pager_write_changecounter`
from `pagerWalFrames`, `pager.c:3218`; the every-commit `pager_incr_changecounter`,
`pager.c:6317`, is the rollback-journal path.) A caller
that changes the schema and forgets `MarkSchemaChanged` leaves the cookie as it was, and
no other connection learns of the change from it.

Change detection is the wal-index header's (`ReadTx.ChangeEpoch`,
[the change state](#drift-2026-11-13-1-the-change-state-is-one-value-under-compare-and-swap)),
not the counters'. A transaction has one pair of counters, its snapshot's:
`ReadTx.SnapshotHeaderCounters`/`SnapshotSchemaCookie`, fixed at begin for the captured
`[minFrame, maxFrame]` window ([drift-45](#drift-45-beginreadfast-skips-page-1-staleness-counter-reads)
for where a read begin has it from); `DiskSchemaCookie` is the same value under the name
the write paths use. Nothing reads them past the snapshot, as in SQLite:
`sqlite3BtreeBeginTrans` returns the cookie from the transaction's own page 1
(`btree.c:3786`), and the `SQLITE_SCHEMA` reload consumes the cookie read through that
same transaction (`prepare.c:288,293`) — detection, reload and consumption are all
snapshot-bounded. The consumers judge snapshot contents with the snapshot's cookie: the
schema-cookie observation at a begin (consuming a cookie the snapshot does not contain
would mark another process's DDL as seen that the snapshot never held) and the
index-visibility checks (admitting an index whose namespace the snapshot cannot resolve
returns wrong results).

The handle also keeps the schema cookie of its own last commit (`LocalSchemaCookie()`; of
the file as opened before it commits). No transaction takes its counters from it: it serves
a backup's capture of the destination's schema (`backup.go`, `iDestSchema`).

<a id="drift-47-checkpoint-omits-open-transaction-guard"></a>
### Drift: Checkpoint Omits Open Transaction Guard
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*DB.Checkpoint` (`db.go:1005`).

SQLite's `sqlite3BtreeCheckpoint` refuses to run if the *calling connection* has any
transaction open: `if( pBt->inTransaction!=TRANS_NONE ){ rc = SQLITE_LOCKED; }`
(`btree.c:11325-11326`, inside the `sqlite3BtreeCheckpoint` span `btree.c:11320-11332`),
invoking `sqlite3PagerCheckpoint` only when `TRANS_NONE`. Go's
`DB.Checkpoint` (`db.go:1005-1014`) has no equivalent guard -- it only checks `db.closing`
(twice) and takes a `db.mu.RLock()` -- so it can proceed to checkpoint while the same handle
holds an open read or write transaction. The consequence is that a self-deadlock/inconsistency case
SQLite explicitly rejects with `SQLITE_LOCKED` is instead allowed by the Go port, letting a
connection attempt to checkpoint against its own in-flight transaction state.

<a id="drift-48-checkpoint-drops-frame-count-out-parameters"></a>
### Drift: Checkpoint Drops Frame Count Out Parameters
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*DB.Checkpoint` (`db.go:1005`).

SQLite's checkpoint API returns, via out-parameters, the final number of frames in the WAL
(`pnLog`) and the number of frames checkpointed/backfilled (`pnCkpt`):
`sqlite3PagerCheckpoint` forwards `pnLog`/`pnCkpt` into `sqlite3WalCheckpoint`
(`pager.c:7510-7536`), where `*pnLog = pWal->hdr.mxFrame` and `*pnCkpt` is the backfill
count; these are surfaced by `sqlite3_wal_checkpoint_v2` and let callers monitor checkpoint
progress. Go's `DB.Checkpoint` signature (`db.go:1005`) drops both out-parameters entirely; the internal chain `pager.checkpointWithMode` (`pager.go:2747`) and `wal.checkpointWithMode` (`wal.go:3331`) all return a bare `error`.
The consequence is a reduced observability surface: callers cannot inspect how much of the
WAL existed or was backfilled by a checkpoint, a benign API-completeness divergence rather
than a correctness defect.

<a id="drift-53-auto-checkpoint-escalates-to-wal-restart-beyond-passive"></a>
### Drift: Auto Checkpoint Escalates To WAL Restart Beyond Passive
- **Category:** new-feature  -  **Severity:** medium  -  **Status:** RESOLVED 2026-11-12: `tryCheckpoint` is a PASSIVE checkpoint and nothing else. The escalation existed because nothing else recycled the WAL; the writer now starts the log over itself, as SQLite's does (see [the RESTART port](#resolved-2026-11-12-restart-checkpoint-reset-the-wal)). What follows describes the code before that.
- **Affected functions:** `pager.go:*pager.tryCheckpoint` (`internal/btree/pager.go:2757-2783`).

SQLite's auto-checkpoint is strictly PASSIVE: the default WAL hook
(`sqlite3WalDefaultHook`, `main.c:2462-2474`) fires when `nFrame >= nAutoCheckpoint` and calls
`sqlite3_wal_checkpoint(db, zDb)` (`main.c:2609-2613`), which uses `SQLITE_CHECKPOINT_PASSIVE` and flows through
`sqlite3PagerCheckpoint` (`pager.c:7510-7539`) with no post-PASSIVE escalation. Go's
`tryCheckpoint` (`pager.go:2757-2783`) first performs a pure PASSIVE backfill via
`checkpointPassive` and then, when that backfill completed, escalates to a best-effort
WAL-RESTART (`pager.go:2775-2781`) to reset the WAL. The consequence is a behavioral extension
beyond SQLite: an automatic checkpoint can reset/restart the WAL rather than leaving it for a
later explicit checkpoint, changing when the WAL is recycled relative to stock SQLite -- a new
feature that callers tuning checkpoint behavior should be aware of.

<a id="drift-55-wal-file-truncated-but-never-unlinked-on-last-client-close"></a>
### Drift: WAL File Truncated But Never Unlinked On Last Client Close
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*DB.Close` (`internal/btree/wal.go:3876-3882`),
  `pager.go:*pager.close` (`pager.go:3308` (truncate call) and `wal.go:3876-3882`
  (truncateFile)).

SQLite's `sqlite3WalClose`, after a successful checkpoint under the EXCLUSIVE DB-file lock,
deletes the `-wal` file in its default non-persistent-WAL mode: it queries
`SQLITE_FCNTL_PERSIST_WAL` and, if the result is not `1`, sets `isDelete = 1`
(`wal.c:2522-2526`) and unlinks the file via `sqlite3OsDelete(pWal->pVfs, pWal->zWalName, 0)`
(`wal.c:2541-2545`); only the persistent-WAL branch (`bPersist==1 && mxWalSize>=0`) instead
truncates the WAL to zero via `walLimitSize`. The Go `pager.close` path
(`pager.go:3308`) calls `truncateFile` (`wal.go:3876-3882`), truncating the WAL to zero
length but never unlinking it. The consequence is that after the last client closes, a
zero-length `-wal` file is left behind on disk rather than being removed as stock SQLite would
do; this is benign in operation but diverges from SQLite's default file-lifecycle cleanup.

<a id="drift-58-wal-read-begin-backoff-off-by-one"></a>
### Drift: WAL Read Begin Backoff Off By One
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.beginReadHdr` (`internal/btree/wal.go:2603`).

SQLite's `walTryBeginRead` increments its back-off counter `*pCnt` at the *top* of the function
(`wal.c:3029` `(*pCnt)++;`), making it a 1-based invocation count, and gates its retry sleeps
on that 1-based value (first sleep at `if( *pCnt>5 )`, quadratic ramp at `if( *pCnt>=10 )`).
Go's `wal.beginReadHdr` -- driven by `db.beginRead` through `pager.beginReadHdr` -- increments
or tests the counter at a different point, so the quadratic back-off ramp starts one retry
later than SQLite (`wal.go:2630`). The consequence is a minor timing divergence in the
read-transaction retry path: under contention the Go reader sleeps one iteration behind
SQLite's schedule, which affects retry pacing only and not correctness.

<a id="drift-61-out-of-range-savepoint-release-errors-instead-of-no-op"></a>
### Drift: Out Of Range Savepoint Release Errors Instead Of No Op
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*WriteTx.ReleaseSavepoint` (`internal/btree/pager.go:2707`),
  `pager.go:*pager.releaseSavepoint` (`pager.go:2707`).

SQLite's `sqlite3PagerSavepoint` guards its entire body with
`if( rc==SQLITE_OK && iSavepoint<pPager->nSavepoint )` (`pager.c:7017`), and its header comment
makes the contract explicit: "If iSavepoint is greater than (Pager.nSavepoint-1), then this
function is a no-op." (`pager.c:6990-6991`) -- so a RELEASE with an index `>= nSavepoint` silently
succeeds (returns `SQLITE_OK`), destroying nothing. Go's `releaseSavepoint`
(`pager.go:2711-2713`) instead does `if id < 0 || id >= len(p.savepoints) { return
ErrInvalidSavepoint }`, returning an error for an out-of-range savepoint id. The consequence is
that any higher layer relying on the documented no-op behavior of releasing an already-gone or
never-opened savepoint will hit a hard error in Go where SQLite would have quietly succeeded.

<a id="drift-62-full-in-transaction-rollback-isavepoint-minus-one-unsupporte"></a>
### Drift: Full In Transaction Rollback iSavepoint Minus One Unsupported
- **Category:** new-feature  -  **Severity:** low
- **Affected functions:** `db.go:*WriteTx.RollbackToSavepoint` (`internal/btree/db.go:1969`), `pager.go:*pager.rollbackToSavepoint` (`internal/btree/pager.go:2588`).

SQLite's `sqlite3BtreeSavepoint`/`sqlite3PagerSavepoint` explicitly accept `iSavepoint == -1`
(`SAVEPOINT_ROLLBACK`) to mean "roll back the entire transaction contents but keep the transaction
open and the locks held" -- `btree.c:4609-4612` documents that "no locks are released and the
transaction remains open" (the `iSavepoint==-1` assert is `btree.c:4619`), `pager.c:7015` asserts
`iSavepoint>=0 || op==ROLLBACK`, and the
playback path at `pager.c:3426` uses `pPager->dbOrigSize` / `pagerRollbackWal` for this whole-txn
case. Go's `RollbackToSavepoint` -> `rollbackToSavepoint` (`internal/btree/pager.go:2588`) has no
support for this in-transaction full rollback: there is no `-1`/`SAVEPOINT_ROLLBACK` sentinel -- the
guard `if id < 0 || id >= len(p.savepoints)` (`pager.go:2592`) rejects a negative id with
`ErrInvalidSavepoint` -- and no path that rewinds the transaction to its origin while keeping it open. The consequence is a missing SQLite
capability -- callers cannot perform a full in-transaction rollback that preserves the open
transaction and its locks, and must instead abort and re-begin.

<a id="drift-63-new-db-page-1-written-directly-to-file-bypassing-wal"></a>
### Drift: New DB Page 1 Written Directly To File Bypassing WAL
- **Category:** changed-logic  -  **Severity:** low  -  **Status:** NARROWED 2026-11-21: creation runs under the exclusive DB-file lock and inside a rollback journal committed by its delete, a WAL beside a database of zero pages is deleted, and the codec is settled on the header read under the lock. What remains is listed below.
- **Affected functions:** `pager.go:*pager.open`, `pager.go:*pager.createIfEmpty`,
  `pager.go:*pager.lockDBForCreate`, `pager.go:*pager.lockDBShared`,
  `pager.go:*pager.downgradeDBLock`, `pager.go:*pager.createPage1`, `pager.go:*pager.newPage1`,
  `creation_journal.go` (all of it), `syncdir_unix.go` (all of it), `wal.go:*wal.syncFile`,
  `db.go:Open` (the `settleCodec` closure and the first look at the header).

**SQLite.** A file of zero bytes is not a WAL database, and SQLite never opens a WAL on one:
`pagerOpenWalIfPresent`, called from `sqlite3PagerSharedLock` after the SHARED lock is held
(`pager.c:5272`, `pager.c:5436`), takes the page count and, when it is zero, deletes the `-wal`
instead of opening it (`pager.c:3355-3356`); the page count for the snapshot is taken after that
(`pager.c:5444-5448`). A database becomes a WAL database through `PRAGMA journal_mode=WAL`:
`OP_JournalMode` calls `sqlite3BtreeSetVersion(pBt, 2)` in a transaction that "always uses a
rollback journal" (`vdbe.c:8061-8066`), whose write begin runs `newDatabase` on an empty file
(`btree.c:3715`, `btree.c:3506-3541`: page 1 through `sqlite3PagerWrite`, `nPage = 1`) and whose
commit writes page 1, bytes 18 and 19 set to 2, into the main file under the EXCLUSIVE lock
(`sqlite3PagerExclusiveLock`, `pager.c:6423-6433`, from `syncJournal` on the commit path). The WAL
is opened only on a later `lockBtree` that reads those bytes (`btree.c:3339`). So page 1 of a
WAL database is in the main file, written under the EXCLUSIVE DB-file lock, before any WAL
exists. Page 1 of an empty file is not copied into the journal either: `pager_write` journals a
page only `if( pPg->pgno<=pPager->dbOrigSize )` and otherwise marks it `PGHDR_NEED_SYNC`
(`pager.c:6093-6100`). What the journal holds is its header, which records the original size, zero
pages (`pager.c:1520`), and page 1 is written to the file only after that journal is synced
(`syncJournal` at `pager.c:6612`, `pager_write_pagelist` at `pager.c:6650`). A creator that dies
before its commit point leaves the journal hot (`hasHotJournal`, `pager.c:5134-5199`), and the next
`sqlite3PagerSharedLock` plays it back before anything in the file is used (`pager.c:5281-5356`):
`pager_truncate(pPager, mxPg)` with `mxPg` 0 empties the file (`pager.c:2895-2897`).

**any-store.** Every database is a WAL database from its first open, and `Open` of a new path
leaves a database of one page rather than an empty file. `pager.open` takes its DB-file lock
(shared in multi-process mode, exclusive in in-process mode) and asks whether a creation is
pending (`creationPending`): the file is empty, or it has content and a hot creation journal
beside it. If so it calls `createIfEmpty`, which takes the exclusive lock (`lockDBForCreate`,
multi-process mode), asks again under it, rolls a dead creator's file back to zero bytes
(`playbackCreationJournal`) or deletes a journal it finds beside an empty file
(`deleteJournalOfEmptyDatabase`, `pager.c:5178-5181`), deletes `<path>-wal`, creates page 1 in a
journaled transaction
(`createPage1`, below), and goes back to the shared lock. The open then continues exactly as for
an existing database: header read from the file, WAL open, page 1 and page count at one snapshot
(`readPage1AtOpen`). An open that finds a database in the file under the exclusive lock, or sees
one appear while waiting for that lock, lost the creation to another process; it takes the shared
lock and asks once more, because the look that found the database was made without a lock. No
process attached to a database can be holding the exclusive lock's counterpart on a file whose
creation is pending: every attached handle keeps the shared lock on a file that has a committed
page 1.

The creation is SQLite's journaled one in `journal_mode=DELETE`, with the journal reduced to its
header (`creation_journal.go`). Under the exclusive lock, after the stale WAL's removal: (1)
`<path>-journal` is opened with the create flag and its header written, recording an original
size of zero pages; (2) the journal is fdatasynced, and, as that is the first sync of a
descriptor opened with the create flag, its directory is fsynced after it (`dirSync`, SQLite's
`UNIXFILE_DIRSYNC`: `os_unix.c:6569-6573`, `6777`, `3936-3954`); (3) page 1 is written; (4) the
database file is fdatasynced; (5) the journal is deleted, which is the commit point
(`pager_end_transaction`, `pager.c:2109`). Whatever a creator killed before step 5 left in the
file — nothing, a whole page 1 that was never synced, or part of one after a power loss — the
next open finds the journal hot and truncates the file before it creates the database itself, so
no process ever attaches to a page 1 whose sync did not complete, and no acknowledged commit rests
on one. The directory sync of step 2 comes after the stale WAL's removal and before page 1
exists, so it also makes that removal durable on a file system whose directory fsync covers every
change to the directory made before it (the POSIX reading, and what ext4, XFS and APFS do; not
measured here, and SQLite, which deletes the WAL with `dirSync` 0 at `pager.c:3356`, relies on the
same).

The delete of step 5 syncs no directory; SQLite passes `pPager->extraSync` to its delete
(`pager.c:2109`). A power loss
before the directory is synced again can bring the journal back, hot, beside the committed
page 1, and the open after it rolls the creation back and creates the database again. Nothing is
lost by that as long as nothing was committed to the database, and nothing is durable in the
database before a sync of its WAL: every WAL descriptor is opened after the delete (a creation
runs only while no process is attached) and with the create flag, so the first sync of each
syncs the directory too, before a commit to it is durable (`wal.syncFile`, the WAL row of the
[WAL drift summary](#7-wal-write-ahead-log)). The divergences of the journal itself are
[drift-2026-11-21-7](#drift-2026-11-21-7-creation-journal).

`Open`'s first, unlocked look at the header does not judge a file with content: a header there
that cannot be read, or that disagrees with the key options, can be gone by the time the open
holds the DB-file lock (another process may roll a dead creator's page 1 back and create the
database in between). `pager.open` settles the codec on the header it reads under the lock and
returns that read's errors, as `sqlite3PagerSharedLock` reads the header only under SHARED
(`pager.c:5272-5282`). Key options that cannot make a codec for a new file are refused by the
first look, before anything is created (`db.go:Open`).

The passphrase codec follows the same rule. `Open` builds it from a first, unlocked read of the
header when it can (so the key derivation mostly runs with no lock held), and `pager.open` calls
`settleCodec` again on the header it read under the lock, and with "no database" before it asks
for the exclusive lock. A later call does nothing while the codec was built for the salt the
file carries: the salt in the header of a database, or, while the file holds no database, a salt
drawn for a new file. That covers every open of an existing database and the open that creates
one, whose page 1 carries the salt drawn for it, so each derives the key once, as SQLCipher
derives a codec context's key once, at its first use (`sqlcipher_codec_key_derive`,
`sqlcipher.c:1849-1880`, SQLCipher v4.14.0). When another process created the database in
between, the codec is rebuilt from the salt in the file; when the first look read a header and
the open then finds the creation pending (a dead creator's page 1, rolled back under the lock),
it is rebuilt from a salt drawn for a new file. A new database keeps the salt drawn for it only
if this open is the one that writes page 1.

**What still differs from SQLite.**

1. *The journal exists for the creation only*, and is any-store's own file:
   [drift-2026-11-21-7](#drift-2026-11-21-7-creation-journal). A file with content and no hot
   journal beside it is judged by its header, as before: one that is shorter than the header, or
   whose header does not parse, is `ErrHeaderCorrupt`, and one that holds a whole header and less
   than a whole page 1 opens, with the missing bytes read as zeros (`readPage1AtOpen`, as
   `readDbPage` reads a short page). A creation cannot leave such a file any more; a file cut by
   other means can.
2. *Created at open, not at the first write transaction.* An `Open` that never writes still
   leaves a one-page file. The hot-journal test runs once per open for the same reason, where
   SQLite runs it at every acquisition of the SHARED lock (`pager.c:5281-5282`).
3. *The lock is a flock.* SQLite goes SHARED → RESERVED → EXCLUSIVE with fcntl byte ranges, and
   two connections cannot both hold RESERVED. A flock has no such step: several creators all
   hold the shared lock at first and none can upgrade. A refused creator drops its lock, waits
   holding nothing, asks whether the creation is still pending and requests the lock again. The
   same loop serves the playback of a hot journal, for which SQLite asks for
   EXCLUSIVE once and returns busy (`pager.c:5308`). The exclusive lock becomes shared
   again before the WAL is opened, so a kernel that lets go of a flock inside a conversion (see
   the two flock entries below) leaves this handle unlocked only while it is attached to
   nothing.
4. *The waits for the DB-file lock.* SQLite waits for its SHARED lock in `pager_wait_on_lock`
   (`pager.c:3946-3962`, from `sqlite3PagerSharedLock`, `pager.c:5272`): `pagerLockDb` is
   repeated while it returns `SQLITE_BUSY` and the busy handler agrees, and the result is
   `SQLITE_BUSY` when the handler gives up. `pager.lockDBShared` is that loop, on
   `DefaultBusyTimeout(BusyTimeout)`, SQLite's default handler (`sqliteDefaultBusyCallback`,
   `main.c:1718-1747`), and an error that matches `ErrBusy` when the handler gives up. Two
   things differ.

   *The open's waits do not share one count.* In SQLite the busy handler counts its calls in
   the connection's `nBusy` (`sqlite3InvokeBusyHandler`, `main.c:1770-1780`), which is zeroed
   when a statement step begins (`vdbe.c:901`), so the busy-handler waits of one step share one
   budget. Here one count serves the open's wait for the shared lock and a creation's waits for
   the exclusive one (`lockDBForCreate`); the open's other waits, for example a conversion back
   to the shared lock (`downgradeDBLock`) and the WAL's waits in `wal.open`, have budgets of
   their own, so an open that meets contention at several of them can wait longer than one
   timeout. SQLite's downgrade asks no busy handler (with flock it only records the new level,
   `os_unix.c:2747-2750`); a flock conversion here can be refused, so `downgradeDBLock` waits.

   *A multi-process open cannot tell a process that holds the file in in-process mode*
   (exclusive for the handle's life) from a peer inside its close or its creation, and waits
   for each the same way: it goes on as soon as a request for the shared lock is granted, and
   fails with `ErrBusy` if the lock is still held when the busy handler gives up.

   The in-process open itself asks for its exclusive lock once and does not wait: where the
   lock is real (Unix, Windows) it is refused at once with `ErrInProcessLocked`. On wasm the
   lock functions are no-ops, so the btree lock keeps no second runtime out
   ([drift-2026-06-25-07](#drift-2026-06-25-07-acquireexclusivedblock-is-a-no-op-on-plan9-wasip1-removing-the-cross-p)).
   A storage backend can refuse one on its own: the wasm module's OPFS backend opens every file
   with an exclusive sync access handle (`createSyncAccessHandle`, `wasm/vfs_opfs.go`), so
   while one worker has the database open, another worker's `Open` fails when its open of the
   file is refused, with the backend's error (an `os.PathError`) rather than
   `ErrInProcessLocked`.
5. *The WAL is deleted under the exclusive lock*, where SQLite deletes it under SHARED and
   relies on an empty database not being in WAL mode (`pager.c:3330-3337`). The wal-index file
   is not deleted, as in SQLite: the first attacher's election truncates a stale one
   (`newPlatformShm`).
6. *In-process mode* holds the exclusive DB-file lock for the handle's whole life (Windows,
   linux/386 and any other target without mmap shm), so the same sequence runs with no lock
   change. On wasm the lock functions are no-ops: nothing serializes two runtimes that share one
   storage (two WASI instances over one directory, two browser tabs over one IndexedDB store),
   and one of them can delete the other's WAL or roll its creation back. One runtime per database
   is the caller's to ensure there
   ([drift-2026-06-25-07](#drift-2026-06-25-07-acquireexclusivedblock-is-a-no-op-on-plan9-wasip1-removing-the-cross-p)).

**Known limitations.**

- *A database file renamed or unlinked while a process is attached to it.* The `-wal` and
  `-wal-shm` names are derived from the path, here and in SQLite (`os_unix.c:4976`, `5005`). A
  second process that creates a database at the old path deletes the `-wal` name, which was the
  attached process's log, and attaches to the attached process's live `-wal-shm`. In SQLite this
  corrupts too ("unlinking or renaming a database file while in use", howtocorrupt §2.4; SQLite
  only logs `file unlinked while open`, `os_unix.c:1657`). One thing is worse here: the new
  opener finds its WAL shorter than a header and initializes the shared wal-index
  (`wal.open` → `initHeaderStateLocked`), which SQLite does not do to an index whose header is
  valid (`walIndexReadHdr` recovers only `if( badHdr )`, `wal.c:2694-2723`). The attached process
  then loses sight of its commits that were not checkpointed. That reset is
  [drift-2026-07-10-3](#drift-2026-07-10-3-region-create-false-never-maps-existing-content):
  every fresh attach rebuilds or initializes the index.
- *A database path replaced while an open waits for the lock.* The pending test is made on the
  descriptor and the WAL is deleted by name, as `pagerOpenWalIfPresent` does (`pager.c:3352-3356`).
- *Two spellings of one database file through a symbolic link* give two `-wal` names, two
  wal-indexes and two creation journals: every name beside the database is built from the path
  as the caller spelled it. SQLite resolves symbolic links before it builds them
  (`unixFullPathname`, `os_unix.c:6975`). A layout that reaches one database file by two paths
  is not supported.

Tests (`multiprocess_create_test.go`, two processes, one held at a named point of its open by
`testOpenStageHook`): `TestMultiProcessCreateRaceOneDatabase` (a creator held before its page-1
write while a second process opens the path; plain and passphrase),
`TestMultiProcessCreateManyAtOnce` (six processes released together),
`TestOpenWaitsForCreatorHoldingDBLock`, `TestOpenWaitsForLastCloserHoldingDBLock` (vfs tag) and
`TestOpenAgainstInProcessHolder` (`multiprocess_dblock_wait*_test.go`: the wait for a holder of
the exclusive lock, and the refusal of an in-process open),
`TestMultiProcessCreateLoserKeyFromWinnersSalt` (an open that saw no file and then finds another
process's encrypted database), `TestOpen_PassphraseKeyDerivedOncePerOpen`
(`encryption_integration_test.go`: one key derivation by an open that creates an encrypted
database and one by an open of it), `TestStaleWALBesideEmptyDatabaseNotReplayed` (a killed process's
WAL beside a removed or truncated main file, whole database or its tail in the WAL, plain and
passphrase, both wal-index modes), `TestCreationJournal_CreatorKilled` (a creator killed with
SIGKILL at each step of its creation, plain and passphrase),
`TestCreationJournal_WaitingOpenerRollsBack` (the creator is killed once the opener has been
refused the lock); `TestInitNewDB_Page1_CrashRecoverable`,
`TestInitNewDB_Page1_CrossProcessSerialization` (`pager_test.go`); the single-process tests of
the journal in `creation_journal_test.go`; and, under the `vfs` tag, in
`creation_journal_vfs_test.go`, on a VFS that keeps a file's content as of its last sync and a
name as of its directory's last sync: `TestCreation_CommitOverADeadCreatorsPage1SurvivesPowerLoss`
(a synced commit made after a creator died before its page-1 sync),
`TestCreation_FirstCommitToANewWALSurvivesPowerLoss`,
`TestCreation_CommitToAWALAnotherHandleCreatedSurvivesPowerLoss` (a WAL whose creator never
synced it), `TestCreation_CommitAfterTheCreationsLastJournalStepSurvivesPowerLoss` (the delete
not yet durable when a peer commits), `TestCreation_PowerLossBeforeAnyCommitCreatesTheDatabaseAgain`,
`TestCreation_PowerLossAfterAKilledPlayback`,
`TestCreationJournal_JournalFoundIsDeletedAfterTheDatabaseFilesSync`,
`TestCreationJournal_OrderOfTheSyncs` (the writes and syncs as the file system sees them: one
directory sync at the first sync of each journal and WAL descriptor, and none after) and
`TestOpen_FirstLookRacingARecoveryDoesNotFail`; `TestDirSync_DirectoryIsTheOneResolvedAtOpen`,
`TestOpen_ChdirBeforeTheFilesOpenKeepsThemInTheResolvedDirectory` (`syncdir_unix_test.go`);
`TestDatabaseDir_IsTheNameUpToItsLastSeparator`, `TestDatabaseDir_NameTooLongFailsTheOpen`
(`dbdir_unix_test.go`).

<a id="drift-2026-11-21-7-creation-journal"></a>
### Drift: The Creation Journal Is A Header Of any-store's Own
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `creation_journal.go:*pager.hasHotCreationJournal`,
  `creation_journal.go:*pager.creationPending`, `creation_journal.go:*pager.playbackCreationJournal`,
  `creation_journal.go:*pager.deleteJournalOfEmptyDatabase`,
  `creation_journal.go:*pager.finalizeJournalOfEmptyDatabase`,
  `creation_journal.go:*pager.openCreationJournal`,
  `creation_journal.go:*creationJournal.sync`, `creation_journal.go:*creationJournal.commit`,
  `creation_journal.go:creationJournalHdr`, `pager.go:*pager.createPage1`.

The creation of a database is a port of SQLite's rollback-journal transaction around
`newDatabase` in `journal_mode=DELETE` (see
[drift-63](#drift-63-new-db-page-1-written-directly-to-file-bypassing-wal) for the sequence and
SQLite's lines): the journal is opened with the create flag, synced, with its directory at that
first sync, before page 1 is written, and its delete, with no directory sync, is the commit
point. It differs from `pager.c` in these points.

1. *The journal has its own magic and no page records.* The header has SQLite's 28-byte layout
   (`writeJournalHdr`, `pager.c:1438-1530`) with the magic `anyjrnl\x01`, an original size of zero
   pages and the page size; the record count, the checksum seed and the sector size are zero,
   and the header is not padded to a sector, since nothing follows it. `hasHotCreationJournal`
   calls a journal hot only when it holds that whole header with an original size of zero.
   SQLite calls any journal with a non-zero first byte hot (`pager.c:5185-5205`); `pager_playback`
   stops at a magic it does not recognise (`readJournalHdr` returns `SQLITE_DONE`,
   `pager.c:1612-1614`) before it truncates anything, syncs, and deletes the journal
   (`pager.c:2978-2983`, `2109`). Beside a database with content, any-store leaves a `-journal`
   file that is not a hot creation journal where it is: it rolls nothing back from it and does
   not delete it. Beside a database of zero pages a file found under the name is deleted before
   the creation, whatever it holds; `hasHotJournal` deletes a journal there
   (`pager.c:5178-5181`). With native file access the file is found by a stat, as SQLite's
   existence test is one (`unixAccess`, `os_unix.c:6895-6898`).
2. *An empty database file is synced before a journal beside it is deleted.* `hasHotJournal`
   deletes a journal beside a database of zero pages at once. `finalizeJournalOfEmptyDatabase`
   syncs the database file first: the journal may be the hot journal of a playback that was
   killed after its truncation and before its sync, and then the disk may still hold the dead
   creator's page 1, which only that journal marks. The sync is made only when a journal is
   found. The playback itself is `pager_playback`'s: the truncation, the sync, and then the
   journal's delete (`pager.c:2895-2897`, `2978-2983`), with nothing written to the journal.
   `pagerSyncHotJournal` (`pager.c:4048`), which syncs a hot journal before playing it back
   because the process that left it may not have, has no counterpart: a creation journal is hot
   only beside a file with content, and page 1 is written only after the journal and its
   directory were synced.
3. *"No RESERVED lock" is "the caller holds the DB-file lock".* `hasHotJournal` asks
   `sqlite3OsCheckReservedLock` whether a writer is alive. A creator holds the DB-file lock
   exclusively from before its journal is opened until after it is deleted, so an open that holds
   the lock, shared or exclusive, and sees a hot journal is looking at a dead creator's.
4. *Platforms.* The directory sync at a descriptor's first sync (`dirSync`) is built on Unix and
   wasip1: SQLite's unix VFS also serves its WASI builds and keeps `UNIXFILE_DIRSYNC` there
   (`os_unix.c:326-330`); whether a WASI host makes it durable is the host's. On Windows and
   js/wasm a sync is the file's alone, as SQLite's Windows VFS syncs no directory. The database,
   the journal and the WAL are opened by the database's full name, resolved once at open
   (`FullPathname`, as `pager.c:4792`, `4915`, `4929`, `4948`), and the directory synced is that
   of the name the file was opened by, cut at its last `/` (`databaseDir`, as `openDirectory`,
   `os_unix.c:3874-3893`, which `unixSync` gives `pFile->zPath`, `os_unix.c:3945`). A full name
   too long for the OS fails the open. What differs in the full name itself is
   [drift-2026-11-23-1](#drift-2026-11-23-1-full-pathname-follows-no-symbolic-link). As in
   `unixSync`, a directory that does not open is skipped and the result of its
   fsync is not looked at (`os_unix.c:3937-3951`). The journal and the database file are synced
   with `fdatasync`, the package's sync primitive
   ([drift-121](#drift-121-fdatasync-durability-primitive-platform-split-undocumented)), which is
   `F_FULLFSYNC` on Darwin, where SQLite's default is a plain `fsync`.
5. *A short read of the header is judged by the journal's size.* `hasHotJournal` reads the
   journal's first byte and takes a short read for a zero byte, a journal that is not hot
   (`pager.c:5198-5205`). `hasHotCreationJournal` reads the whole header, and when the read comes
   up short it looks at the journal's size, and only then: a journal shorter than its header is
   not hot, and one that holds it fails the open with the read's error (`io.ErrUnexpectedEOF`
   when it gave none). A dead creator's journal taken for one that is not hot would stay beside
   the database the handle commits to, and the next open would play it back and truncate the
   database to nothing. `readJournalHdr` reads a header only within the journal's size
   (`pager.c:1597-1599`) and returns a failed read's code (`pager.c:1608-1611`).

Tests: `creation_journal_test.go` (`TestCreationJournal_WhichHeaderIsHot`,
`_HotJournalRollsBackWhateverIsInTheFile`, `_JournalThatIsNotHotIsLeftAlone`,
`_JournalBesideEmptyFileIsDeleted`), `TestCreationJournal_ShortReadOfAHotJournalFailsTheOpen`
(`pager_short_read_vfs_test.go`), and the process-kill and power-loss tests listed under
drift-63.

<a id="drift-68-pageslab-and-configpagecache-idempotent-versus-reconfigurabl"></a>
### Drift: pageSlab And ConfigPageCache Idempotent Versus Reconfigurable Setup
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `page_slab.go:*pageSlab.Init`, `page_slab.go:slabUsable`,
  `page_slab.go:ConfigPageCache`.

SQLite configures its page-cache buffer in two steps. `sqlite3_config(SQLITE_CONFIG_PAGECACHE,
pMem, sz, N)` records the three arguments (`main.c:524-532`); it is refused with
`SQLITE_MISUSE` once the library is initialized (`main.c:434-440`), so a configuration changes
only between `sqlite3_shutdown`, which clears the init flags (`main.c:380-395`), and the next
`sqlite3_initialize`. Initialization then checks it: `sqlite3MallocInit` clears a configuration
it cannot use, `if( sqlite3GlobalConfig.pPage==0 || sqlite3GlobalConfig.szPage<512 ||
sqlite3GlobalConfig.nPage<=0 )` setting `pPage = 0; szPage = 0;` (`malloc.c:165-168`), and
`sqlite3PCacheBufferSetup` (`pcache1.c:271-291`) builds the slot list, where `if( pBuf==0 ) sz =
n = 0;` and `if( n==0 ) sz = 0;` (`pcache1.c:274-275`) leave a cleared one with no slots. Every page
buffer then comes from `sqlite3Malloc` (`pcache1Alloc`, `pcache1.c:344` and `361`), but the cleared
configuration still counts: `sqlite3MallocInit` keeps `nPage`, and with `pPage==0`, `pcache1Init`
sets `pcache1.nInitPage = sqlite3GlobalConfig.nPage;` when each cache has its own group
(`pcache1.separateCache`, `pcache1.c:730-737`), so each cache's initial bulk allocation is sized
from that count (`pcache1InitBulk`, `pcache1.c:303-311`: `nPage` pages, or `-1024*nPage` bytes for
a negative count, at most the cache's own size; none for 0). With `pPage==0` the caches are
separate in every build but one: `SQLITE_ENABLE_MEMORY_MANAGEMENT` sets `pcache1.separateCache =
0;` (`pcache1.c:715-716`), and then `pcache1.nInitPage = 0;` (`pcache1.c:735-736`) and no cache
makes a bulk allocation.

Go's `pageSlab.Init` (behind `ConfigPageCache` and `anystore.InitPageBuffer`) keeps SQLite's rule
for which arguments give slots: `slabUsable` requires `pageSize >= MinPageSize` (512) and
`nPages > 0`, the `malloc.c:165-168` check. Go adds two conditions of its own, because it
allocates the slab where C is handed one, and C has neither: `pageSize <= MaxPageSize` (65536),
since a larger slot serves no database (no page size is larger, and the slab serves only a page of
its own slot size, see drift-2026-06-25-22), and `nPages*pageSize` must fit an int. Any other
arguments leave an initialized slab with no slots and slot size 0, which serves no request
(`pageSlab.serves`), and `btree.Open` resolves `useSlab` to false for every database. Such a
database runs as one that never opted in: its caches make the default initial bulk allocation, up
to 20 pages (`pcache.initBulk`, `SQLITE_DEFAULT_PCACHE_INITSZ`), where C, with separate caches,
sizes it from the cleared configuration's `nPage`.

What diverges is the lifecycle. Go has no shutdown, and `Init` is first-call-wins: a
double-checked `s.initialized` guard makes every later call a no-op, so the slab is configured once
for the life of the process and cannot be resized, turned off or turned on later, as a SQLite
process can across `sqlite3_shutdown`. Pinned by `TestPageSlab_InitIdempotent`,
`TestPageSlab_InitWithoutPagesIsEmpty` and `TestPageSlab_InitAtMinPageSize`.

<a id="drift-69-underpressure-drops-heap-nearly-full-fallback"></a>
### Drift: UnderPressure Drops Heap Nearly Full Fallback
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `page_slab.go:*pageSlab.UnderPressure` (`page_slab.go:226-228`).

C's `pcache1UnderMemoryPressure` (`pcache1.c:517-523`) has two branches: when a slab is configured
and the page fits a slot it returns the atomic `bUnderPressure` flag, but ELSE (no slab configured,
or a page too big for a slot) it falls back to `sqlite3HeapNearlyFull()`, still reporting memory
pressure from the global soft-heap-limit. Go's `UnderPressure()` (`page_slab.go:226-228`)
implements only the first branch and drops the `sqlite3HeapNearlyFull()` fallback entirely. The
consequence is that in the default no-slab configuration Go always reports no memory pressure,
whereas SQLite would still surface heap-based pressure, so the cache never receives the
pressure signal that would otherwise prompt it to shed pages.

<a id="drift-72-freeoverflowchain-omits-refcount-and-fixed-count-versus-term"></a>
### Drift: freeOverflowChain Omits Refcount And Fixed Count Versus Terminator Walk
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.freeOverflowChain` (`pager.go:3046-3086`),
  `pager.go:*pager.freeOverflowChain` (`pager.go:3062-3084`).

C's `clearCellOverflow` computes the exact expected overflow-page count from parsed payload
metadata -- `nOvfl = (nPayload - nLocal + ovflPageSize - 1)/ovflPageSize` (`btree.c:6987`) --
and frees exactly that many via a `while(nOvfl--)` loop, skipping `getOverflowPage` on the final
iteration (guarded by `if( nOvfl )`, `btree.c:7000-7002`) so it never reads the next-pointer of the last
page; additionally, for each page already in the cache it checks
`sqlite3PagerPageRefcount(pOvfl->pDbPage)!=1` and returns `SQLITE_CORRUPT_BKPT` instead of freeing
a page with more than one outstanding reference (`btree.c:7005-7018`), detecting a mis-typed or
shared "overflow" page still in use by a cursor. Go's `freeOverflowChain` (`pager.go:3046-3086`)
instead walks next-pointers to a zero terminator and omits both the fixed expected-count derivation
and the `refcount==1` outstanding-reference corruption check. Go does add bounds
(`pgno >= 2 && pgno <= dbSize`) and a maxIter guard (`pager.go:3063-3072`) that catch out-of-range
and circular chains, so the residual gap is specifically the `refcount==1` shared-reference check
(and reading the last page's next-pointer): Go can still free a page that is actually still
referenced by a cursor, where SQLite would have stopped with a corruption error.

<a id="drift-73-freepage2-trunk-decision-and-page-invalidation-drifts"></a>
### Drift: freePage2 Trunk Decision And Page Invalidation Drifts
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.freePage` (`internal/btree/pager.go:1527-1617`),
  decision at `internal/btree/pager.go:1548-1550`, leaf path at `internal/btree/pager.go:1569-1589`.

C's `freePage2` decides whether to append a freed page as a leaf of the existing first trunk or to
start a new trunk by testing the page-1 free-page COUNT -- `if( nFree!=0 )` where
`nFree = get4byte(&pPage1->aData[36])` (the total-freelist-pages counter at offset 36,
`btree.c:6846,6876`). Go's `freePage` instead branches on the FIRST-TRUNK POINTER at offset 32 (the
field C reads as `iTrunk = get4byte(&pPage1->aData[32])` at `btree.c:6879`),
`trunkPgno := p.header.FirstFreelistPg` with `if trunkPgno != 0` (`pager.go:1548-1550`); the two
predicates only coincide in a header that is consistent between offsets 32 and 36. Additionally, C
always reaches `freepage_out:` and sets `pPage->isInit = 0` on the freed page for both the leaf and
new-trunk cases (`btree.c:6947-6949`), invalidating any cached parse of that page. Go's new-trunk
path does clear the freed page's header (`clear(newTrunkPg.data)`/`newTrunkPg.header = pageHeader{}`,
`pager.go:1619-1622`), so the missing-invalidation gap applies only to Go's leaf
path (`pager.go:1569-1589`), which never fetches or touches the freed page object -- it only updates the
trunk's leaf array and records `setHasContent` by page number. The consequence is that
on an inconsistent header Go can pick the wrong free-list shape, and a stale cached header parse for
a freed leaf page can survive where SQLite would have discarded it.

<a id="drift-74-secure-delete-page-zeroing-on-free-unsupported"></a>
### Drift: secure_delete Page Zeroing On Free Unsupported
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.freePage` (`internal/btree/pager.go:1527-1617`).

C's `freePage2` honors `BTS_SECURE_DELETE`: when the pragma is enabled it fetches the freed page and
`memset(pPage->aData, 0, pPage->pBt->pageSize)` to scrub the deleted data (`btree.c:6849-6859`), and
it also suppresses the `PagerDontWrite` optimization in that mode so the zeroed page is actually
persisted (`btree.c:6919-6920`). Go's `freePage` has no secure-delete concept at all
(`pager.go:1527-1617`): freed leaf-page contents are never zeroed. The consequence is that deleted row/cell data physically remains in freed pages
on disk in Go where SQLite's secure_delete mode would have scrubbed it.

<a id="drift-76-beginwrite-re-reads-page-1-header-on-state-change"></a>
### Drift: beginWrite Re Reads Page 1 Header On State Change
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.beginWrite`
  (`internal/btree/pager.go:916-957` and `refreshHeaderFromPage1` at `internal/btree/pager.go:1988-2064`).

SQLite refreshes the page-1 header and `dbSize` on the READ path
(`sqlite3PagerSharedLock` resets the cache on a changed file version at `pager.c:5418-5419` and
recomputes `dbSize` via `pagerPagecount` at `pager.c:5448`), not inside `sqlite3PagerBegin`
(`pager.c:5922-5986`, which performs no page-1 re-read and only copies the already-known `dbSize`
into the writer size fields at `pager.c:5973-5975`). Go
relocates that refresh into the write path: `pager.beginWrite` calls both `p.writerCache.clear()`
and `p.refreshHeaderFromPage1()` when `wal.beginWriteWithSnapshot` reports `stateChanged=true`
(`pager.go:932-947`). `refreshHeaderFromPage1` (`pager.go:1988-2064`) re-reads page 1 from WAL or
disk and overwrites BOTH `p.header` and `p.dbSize` (`p.dbSize.Store(p.header.DatabaseSize)` at
`pager.go:2016,2034`). The consequence is a structural divergence in when and where the cached
header/size are reconciled with the on-disk image: Go performs this reconciliation lazily at the
start of a write when the underlying WAL state changed, rather than during reads as SQLite does.

Error propagation and the page-count rule (2026-11-11). The re-read is now what `lockBtree` does at
the start of a transaction (`btree.c:3284-3298`, `3401-3408`):

- Page 1 is fetched whole through the writer cache (`btreeGetPage` → `readDbPage`,
  `pager.c:3031-3046`), from exactly one source: the WAL frame when the wal-index resolves one, the
  DB file otherwise. The DB file's older page 1 never stands in for a frame whose read failed.
  The page stays in the cache the begin has just cleared, where the commit would load it anyway,
  so the full-page read replaces a later one rather than adding one.
- The size is `lockBtreePageCount` (`btree.c:3294-3298`, `3401-3408`) against `pagerPageCount`
  (`pagerPagecount`, `pager.c:3279-3306`: the live wal-index `nPage`, else the file size): a zero
  or untrusted page-1 size gives way to the pager's count, and a trusted size above it is
  `ErrHeaderCorrupt`. `pager.open` uses the same two helpers in `readPage1AtOpen`, which reads
  page 1 and the count under one WAL read transaction (`pagerBeginReadTransaction` then
  `pagerPagecount`, `pager.c:5444-5448`; `lockBtree`, `btree.c:3284-3298`), so a peer's checkpoint
  during open cannot pair a stale file header with a newer WAL size. It reads page 1 whole from
  either source, as `readDbPage` does (`pager.c:3031-3046`), so a read error past the header fails
  the open (test `TestOpenPage1ReadErrorPastHeaderFails`, vfs tag); a short file read is not an
  error there, as in `readDbPage` (the rest reads as zeros), unless the header itself is cut off. Open used to take the larger
  of the file-derived size and the WAL's, which brought back the pages of a WAL-committed shrink
  at the first commit.
- A failed lookup, a failed read, a page-1 image that does not decode as a database header, and a
  rejected size all abort `beginWrite` and leave `p.header` / `p.dbSize` untouched.

Before this, a failed frame read fell back to the DB file and returned nil, so a writer could
start with a peer-outgrown `dbSize` and freelist and allocate over the peer's pages; a failed
decode and a lone DB-file read failure were ignored, only the 100-byte prefix was read, and the
page-1 size was installed unchecked (a zero made every page unreachable).

Error classes. SQLite's `SQLITE_NOTADB` for a page 1 that is not a database header
(`btree.c:3306`) is `ErrHeaderCorrupt` here, as at open: any-store has one error for "unusable
header", and it matches `ErrCorrupt`. A DB file too short to hold page 1 is a read error
(`io.EOF`) rather than a zero-filled `NOTADB`, because the page getters do not zero-fill a short
read ([drift-7](#drift-7-short-db-file-read-treated-as-hard-error)); the begin fails either way.

Every write begin (2026-11-12). The refresh now runs at every `beginWrite`, not only when another process's change was seen: `lockBtree` reads page 1 at the start of every SQLite transaction (`sqlite3BtreeBeginTrans`, `btree.c:3707`), and a schema format 6 commit no longer rewrites page 1 each time, so the header must not be carried over from the last transaction. Page 1 normally comes from the writer cache; the cache is cleared first only when a change was seen, or a refresh failed. The count is `committedPageCount`: the live wal-index header's size, the file's size when the WAL has no commit (the readers' memo, so no `fstat` per begin), the committed position's size in the in-process and in-memory modes, with the same fallback. Rollback and `pagerError` restore the page count the transaction began with (`savedDbSize`), not the header's size field, which is 0 for an untrusted header.

Residual divergence: SQLite needs no "refresh pending" state, because a transaction whose page-1
read failed never keeps the page (`lockBtree` releases it and leaves `pBt->pPage1` zero,
`btree.c:3288-3289`, `3451-3454`) and the next transaction reads it again. Go keeps the header in
a struct and detects the peer change in the WAL layer, whose `writerHdr` baseline has already
moved to the live header by the time the page-1 re-read runs. `pager.headerStale` carries the
pending refresh across a failed begin: it is set before the re-read and cleared only when one
succeeds, and `beginWrite` refreshes when either `stateChanged` or `headerStale` is set.
Regression tests: `TestBeginWritePage1Refresh_WALReadFailureNoStaleFallback`,
`TestBeginWritePage1Refresh_PageCountRule`,
`TestRefreshHeaderFromPage1_DecodeErrorPropagates`,
`TestRefreshHeaderFromPage1_DBFileReadErrorPropagates`,
`TestRefreshHeaderFromPage1_DoubleIOFailure`, `TestBeginWriteHeaderRefreshError_DoubleIOFailure`.

<a id="drift-2026-11-11-2-nonzero-change-counter-mismatch-is-corruption"></a>
### Drift: A Non-Zero Change-Counter Mismatch On Page 1 Is Corruption, Not An Untrusted Size
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:lockBtreePageCount` (callers `pager.go:*pager.refreshHeaderFromPage1`, `pager.go:*pager.open`).

SQLite's `lockBtree` uses the in-header page count (offset 28) only when it is non-zero and bytes 24..27 (change counter) equal bytes 92..95 (version-valid-for); on any mismatch it falls back to the pager's count and carries on (`btree.c:3294-3298`). The mismatch is a normal state there: a library older than 3.7.0 updates the change counter without knowing about offsets 28 and 92, so the stored size may be stale and is simply not trusted.

`lockBtreePageCount` applies the same rule for a zero size and for `VersionValidFor == 0` (files and fixtures that never tracked the field), but returns `ErrHeaderCorrupt` when `VersionValidFor` is non-zero and differs from `FileChangeCount`. No any-store writer can produce that state: every write of page 1 sets the two fields together before serializing it (`pager.commit`; a schema format 6 commit that leaves page 1 out changes neither), both live in the first 100 bytes of the page so no sector boundary separates them, and there is no older any-store format that maintains one without the other. A non-zero mismatch therefore means the header bytes were damaged, and the same damage may have hit the freelist head and count next to them (offsets 32 and 36), which nothing cross-checks; continuing on the pager's count would let the next commit write that header back. SQLite's leniency exists for a compatibility case any-store does not have.

The cost is that a database whose page-1 header is damaged in exactly these bytes is refused where SQLite would open it. Revisit if a writer that updates the counters separately is ever introduced. The rule applies at open (`readPage1AtOpen`, which reads page 1 at the open snapshot, so the check is on the page 1 that snapshot reads, the WAL's when it holds a newer one) and at every write begin (`refreshHeaderFromPage1`). Regression tests: `TestBeginWritePage1Refresh_PageCountRule/change-counter_mismatch_is_corrupt`, and the open-time `VersionValidFor` test in `pager_test.go`.

<a id="drift-77-filechangecount-bumped-conditionally-not-unconditionally"></a>
### Drift: FileChangeCount Bumped Conditionally Not Unconditionally
- **Category:** changed-logic  -  **Severity:** low (was medium)  -  **Status:** schema format 5 only since 2026-11-12. The premise below is wrong for WAL mode: SQLite's `pager_write_changecounter` stores `dbFileVers+1`, page 1's counter as last read plus one, and only into page-1 frames (`pager.c:3084-3097`, `3218`); `pager_incr_changecounter` is a rollback-journal path (`pager.c:6363`, reached from `6574-6590`). So consecutive commits of one connection write the same value, and the counter is not a commit count. A schema format 6 file now does exactly that: when a commit writes page 1 ([which commits do](#drift-2026-11-12-5-page-1-written-when-its-header-changes)) the counter and VersionValidFor get `dbFileVers+1`, computed once page 1 is fetched, and `dbFileVers` is set where `readDbPage` and `pager_playback_one_page` set it (`pager.c:3047-3065`, `2481-2484`): at open, at every page-1 load into the writer cache, and at a savepoint rollback that restores page 1, or that undoes a header or page-count change, which in SQLite would have journaled page 1. The sequence of values still depends on when page 1 is read, which follows any-store's cache: a rollback clears the whole writer cache ([wal undo](#old-drift-wal-undo-via-pager-rollback)), and page 1 is not held for the transaction ([page-1 exclusion](#old-drift-pagerstress-page1-exclusion)), so the values can run ahead of SQLite's, by one for each such reload. Equality of the two fields is all anything reads. Schema format 5 keeps the rule below for the v2.4.x processes that read it. The writer rule below is otherwise unchanged; what depended on it is not. Since 2026-11-13 nothing in this code base detects another process's commit by this counter: derived state and backups follow the wal-index header ([the change state](#drift-2026-11-13-1-the-change-state-is-one-value-under-compare-and-swap)), so a commit that does not mark its data is found like any other. The counter is still written, as described here, for v2.4.x processes attached to the same file, which do watch it; the `IsDataStale` consumer the last sentence names is removed.
- **Affected functions:** `pager.go:*pager.commit` (`internal/btree/pager.go:2247`).

SQLite always advances the page-1 change counter (header offset 24, mirrored at offset 92) on a
committing transaction that writes pages: `pager_write_changecounter` (`pager.c:3084`) is an
unconditional update invoked from the commit machinery itself (`pagerWalFrames` at `pager.c:3218`
via `if( pList->pgno==1 ) pager_write_changecounter(pList);`, and `pager_incr_changecounter` at
`pager.c:6363`). Go's `commit` increments `p.header.FileChangeCount++` only `if dataChanged` (and
`p.header.SchemaCookie++` only `if schemaChanged`), gating the bump on the caller-supplied flag
rather than on the fact that data pages were written (`pager.go:2308-2319`, with `VersionValidFor`
kept in lockstep with `FileChangeCount`). The consequence is that a committing transaction that
writes pages without the caller setting `dataChanged=true` leaves the change counter unadvanced, so
other connections relying on the counter to detect that the file changed may miss the modification,
whereas SQLite would always have bumped it: Go always writes page 1 on a non-empty commit
(`getWritablePage(1)` at `pager.go:2322`), so SQLite would unconditionally bump here. In practice the
any-store wrapper masks this -- `writeTx.Commit` calls `MarkDataChanged` whenever the tx is modified
(`tx.go:123-124`) -- while direct btree users remain exposed for `IsDataStale`/backup-restart
correctness.

<a id="drift-78-commit-does-not-prune-dirty-pages-above-dbsize-before-wal-wr"></a>
### Drift: Commit Does Not Prune Dirty Pages Above dbSize Before WAL Write
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.commit`
  (`internal/btree/pager.go:2247`, dirty-list collect at `pager.go:2338`, and `wal.go:2036` with no `pgno<=dbSize` filter).

SQLite's `pagerWalFrames` explicitly removes pages whose `pgno > nTruncate` (the post-commit
database size) from the dirty list before logging them, so no page beyond the database image is ever
written to the WAL and the commit frame is guaranteed to belong to a page within the image
(`pager.c:3199-3212`, called with `nTruncate = pPager->dbSize` at `pager.c:6512`). Go's `commit`
collects the entire dirty list unfiltered (`pager.go:2338` `appendDirtyPages`, `pcache.go:571-576`)
and `writeFrames` (`wal.go:2036`) applies no `pgno <= dbSize` filter before
emitting frames, so there is no `nTruncate`-style pruning. The consequence is that a dirty page with
`pgno > dbSize` -- e.g. one left over from a since-truncated region -- can still be logged to the
WAL, and the commit (size-bearing) frame may end up attached to an over-size page rather than one
within the database image, which SQLite structurally prevents.

<a id="drift-79-truncateto-eager-dirty-page-drop-and-extra-guards"></a>
### Drift: truncateTo Eager Dirty Page Drop And Extra Guards
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.truncateTo` (`pager.go:3107`),
  `pager.go:*pager.truncateTo` (`pager.go:3096`).

C's `sqlite3PagerTruncateImage` is a one-line operation -- its entire body is
`pPager->dbSize = nPage;` -- and it does NOT touch the page cache at all; the documentation states
it is "only called right before committing a transaction" and that "it is not safe to call this
function and then continue writing" (`pager.c:4017-4031`). Leaving above-size pages in the cache is
deliberate: a rollback to a savepoint taken BEFORE the truncate (which restores the larger `dbSize`)
still finds those pages intact. Go's `truncateTo` instead calls `p.writerCache.truncate(newDbSize)`
(`pager.go:3107`), which eagerly removes ALL cached pages above `newDbSize` -- including DIRTY ones,
unlinking them from the dirty list and decrementing `nDirty` (`pcache.go:725-738`) -- and adds two
non-C guards: it rejects truncate-to-zero with an error (`pager.go:3100-3102`) and silently no-ops
when `newDbSize >= cur` (`pager.go:3104-3106`), where C instead asserts
`pPager->dbSize >= nPage || CORRUPT_DB` (`pager.c:4018`). The consequence is a savepoint-rollback
hazard SQLite avoids: eagerly discarding dirty above-size pages means a later rollback to a
pre-truncate savepoint can no longer restore them, plus the added guards diverge from C's documented
shrink-only contract. In practice the sole production caller is backup finalization
(`backup.go:355`, invoked right before commit), matching C's documented single-call-site contract,
so the savepoint hazard is latent.

<a id="drift-80-inprocessshm-close-non-terminal-teardown"></a>
### Drift: inProcessShm close Non Terminal Teardown
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `shm.go:*inProcessShm.close` (`shm.go:122`).

C's `unixShmUnmap` (`os_unix.c:5503-5546`) performs a true teardown: it removes the connection from `pShmNode->pFirst`, frees
it, decrements `pShmNode->nRef`, and at `nRef==0` calls `unixShmPurge` (`os_unix.c:4811`) which munmaps/frees every
region, closes the shm fd, and nulls the node -- after which the shm structure is gone and cannot be
used. Go's heap-fallback `inProcessShm.close` (`shm.go:122-128`) ignores the `isLastClient`
argument (`_ = isLastClient`), has no refcount equivalent, simply sets `s.regions = nil` under
`regMu`, and does NOT reset the per-slot lock state. The consequence is that close is a non-terminal,
partial teardown: the object remains reusable with its mapped regions cleared but its lock state
stale, diverging from SQLite's terminal connection-free/munmap-at-zero-refs semantics.

<a id="drift-81-in-process-shm-lock-collapses-per-connection-masks-to-single"></a>
### Drift: In Process Shm Lock Collapses Per Connection Masks To Single Refcount
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `shm.go:*inProcessShm.lock`
  (`shm.go:79-99`, esp. `l.state++` at `shm.go:91`; counter defined `shm.go:50-53`, state field at `shm.go:58`),
  `shm_mmap.go:*mmapShm.lock` (`internal/btree/shm_mmap.go:198-242`).

SQLite's `unixShmLock` distinguishes two layers of state: per-connection bitmasks
`p->sharedMask`/`p->exclMask` and a process-wide counter array `pShmNode->aLock[]`
(`os_unix.c:5291-5301`). The "is there work to do" guard
(`flags==(SQLITE_SHM_SHARED|SQLITE_SHM_LOCK) && 0==(p->sharedMask & mask)`, `os_unix.c:5351-5352`)
uses the per-connection mask so that a SHARED lock re-requested by the SAME connection that already
holds it is a NO-OP returning `SQLITE_OK` without touching `aLock[ofst]`. Go collapses both layers
into a single refcount: `inProcessShm.lock` does `l.state++` (`shm.go:91`, counter at
`shm.go:50-53`) and `mmapShm.lock` increments `s.locks[slot]` (`shm_mmap.go:225`); `mmapShm.lock`
now dedups the underlying fcntl syscall on a repeat shared lock (the `current==0` guard at
`shm_mmap.go:219`) but still NOT the in-process refcount, so the per-connection-mask collapse is
unchanged. The consequence is non-idempotent re-locking: a same-connection repeat
SHARED lock that SQLite treats as a free no-op instead bumps the shared refcount in Go, so the
lock/unlock accounting can drift from SQLite's behavior when a connection re-requests a lock it
already holds.

<a id="drift-88-wal-recovery-does-not-pre-seed-read-mark-slot-1"></a>
### Drift: WAL Recovery Does Not Pre Seed Read Mark Slot 1
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.recoverLocked`
  (`internal/btree/wal.go`, `recoverLocked`).

After recovery SQLite seeds read-mark slot 1 with the recovered `mxFrame` so the first reader can immediately
reuse it: `for(i=1;i<WAL_NREADER;i++){ ... if(i==1 && pWal->hdr.mxFrame){ pInfo->aReadMark[i]=pWal->hdr.mxFrame; } else { pInfo->aReadMark[i]=READMARK_NOT_USED; } ... }` (`wal.c:1576-1583`). Go's `recoverLocked`
instead stores `readMarkNotUsed` (`0xFFFFFFFF`) into every slot 1..4 and only sets `aReadMark[0]=0`
(`wal.go:2006-2009`). The consequence is a missed optimization rather than a correctness bug: the first
reader after recovery cannot reuse a pre-seeded slot at the recovered `mxFrame` and must instead carve out
a fresh read-mark, diverging from SQLite's seeded fast path.

<a id="drift-90-wal-index-szpage-field-not-encoded-or-decoded"></a>
### Drift: WAL Index szPage Field Not Encoded Or Decoded
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*walIndex.readHeader`
  (`internal/btree/wal.go:841-895` writeHeader never sets szPage; `wal.go:444` / `wal.go:902-960`
  readHeader returns szPage undecoded).

C's `walIndexTryHdr` decodes the page size from the SHM header on every successful read —
`pWal->szPage = (pWal->hdr.szPage&0xfe00) + ((pWal->hdr.szPage&0x0001)<<16);` (`wal.c:2613`) — mapping the
on-wire encoding (where 1 means 65536) back to the real page size. The raw 16-bit `szPage` field IS
round-tripped by Go's serialize/deserialize/computeCksum (`wal.go:426,444,465`), but `writeHeader`
(`wal.go:841-895`) never populates `hdr.szPage` (it stays zero) and `readHeader` never applies the decode
transform. The consequence is that Go neither encodes a meaningful page size into the wal-index header on
write nor reconstructs it on read; the field carries no usable page-size information, diverging from
SQLite's per-read `szPage` decode (low impact because Go derives page size elsewhere).

<a id="drift-92-shmhashget-skips-segment-on-map-io-error"></a>
### **Resolved (2026-11-19)** — Drift: shmHashGet Skips Segment On Map IO Error
- **Category:** changed-logic  -  **Severity:** was recorded low; the effect was a silent stale read
- **Affected functions:** `wal.go:*walIndex.shmHashGet`.

C's `walFindFrame` aborts the entire frame lookup on any hash-segment map failure:
`rc = walHashGet(pWal, iHash, &sLoc); if( rc!=SQLITE_OK ){ return rc; }` (`wal.c:3576-3579`), so an SHM
map/extend error (`walHashGet` -> `walIndexPage` -> `sqlite3OsShmMap`) reaches the reader:
`readDbPage` returns it (`pager.c:3032-3033`) and `getPageNormal` drops the page and fails the get
(`pager_acquire_err`, `pager.c:5629`). Go's `shmHashGet` did `if err != nil { continue }`: it skipped
the segment and went on to the older ones, so the lookup answered with an older frame or with "not in
the WAL", and the caller read the page from before a commit: a reader returned the old value with no
error, and a writer that changed such a page committed the earlier commit away. `shmHashGet` now
returns the mapping error (wrapped, `errors.Is` still matches it). Every caller already failed its
read on a lookup error (the over-probed-chain `ErrCorrupt`, drift-93): `walIndex.get` → `getPageWriter`
(the page is discarded from the writer cache, as `pager_acquire_err` drops it), `readTempPage`,
`getPageReader`, `readRawPage`, `headerCountersAt`, the page 1 read of `open`; `getInTxRange` →
`writeFrames`/`writeFramesMem`; `getLatest`. The in-process index looks pages up in `pageMap` and maps
nothing. Tests: `TestWalIndexMapError_ReaderFails`, `_WriterFails`, `_LookupsFail`
(`wal_index_map_error_test.go`).

More of `walFindFrame`'s rules came with it (2026-11-19), since a lookup that can fail must not fail, or be
wrong, where C's cannot:

- **An empty window touches no segment.** A snapshot that is entirely in the database file (a reader on slot
  0: `minFrame == maxFrame+1`) returned before the loop in C (`wal.c:3538-3541`). `shmHashGet` mapped and
  probed the segment of `maxFrame` for it, so a segment it could not map failed a read that needs no WAL, and
  it read hash tables a log restart may be clearing. `shmHashGet`, `get` and `getInTxRange` return `(0, nil)`
  for `minFrame > maxFrame`. Tests: `TestWalIndexLookup_EmptyWindowMapsNothing`,
  `TestWalIndexMapError_CheckpointedReaderReads`.
- **A lookup does not extend the wal-index file.** `shmHashGet` mapped with `region(seg, true)`, which
  extends a short file with zeros and maps them: a segment missing from the file read as "no page here"
  instead of failing. C passes `pWal->writeLock` as the extend flag (`wal.c:784-786`), `unixShmMap` leaves the
  region unmapped for a reader (`os_unix.c:5163-5164`), and `walHashGet` makes that an error
  (`wal.c:1185-1187`). `shmHashGet` now takes what is mapped (`region(seg, false)`) and otherwise goes through
  `mapSegmentForLookup`, which fails with `errShmSegmentMissing` when the file does not reach the end of the
  segment's mapping group and maps the segment otherwise. The group is what `region(seg, true)` maps: one
  segment, or several when the OS page is larger than a segment (64 KiB pages), so a 32 KiB file reaches the
  end of segment 0 and still fails the lookup there; `unixShmMap` compares the file's size with
  `nReqRegion*szRegion`, rounded up to a whole group (`os_unix.c:5140-5165`). A writer's lookups do not extend either, where C's would (its extend
  flag is set): a segment of a frame in the window was made by the writer of that frame. Test:
  `TestWalIndexLookup_MissingSegmentFails`.
- **Bounds before page number; atomic slots.** The probe tested `aPgno` before the frame bounds and read and
  wrote the 16-bit slots as plain bytes, so a reader goroutine read page numbers and slots of entries above
  its snapshot while the writer of the same process changed them (an append, a rollback's
  `shmCleanupFromFrame`, the removal of a dead writer's entries). C loads the slot with `AtomicLoad`
  (`wal.c:3583`), tests `iFrame<=iLast && iFrame>=pWal->minFrame` before `sLoc.aPgno[iH-1]==pgno`
  (`wal.c:3585`) and stores the slot with `AtomicStore` (`wal.c:1338`). The probe now has C's order. A
  probe and an append access a slot through `htSlotLoad`/`htSlotStore`: Go has no 16-bit atomics, so the
  aligned 32-bit word that holds two slots is loaded and stored; a store rewrites the neighbouring slot with
  the value it has just loaded, which is safe because only the holder of the write lock stores. The cleanup
  of a rollback or of a dead writer's entries (`shmCleanupHashSegment`) does not call `htSlotStore`: it
  loads each word of slots with `atomic.LoadUint32` and stores a word it changed with `atomic.StoreUint32`
  itself, where C's `walCleanupHash` stores plainly
  (`wal.c:1260-1264`): a plain store that a reader loads concurrently is a data race in Go. The page
  numbers stay plain, as in C (the read after the bounds, the cleanup's `memset`, `wal.c:1271`): a reader
  reads only those of its window, written before its snapshot was published and not changed while it is in
  use. The whole-segment clears (a segment's first frame, the segments past a rollback target,
  `shmClearHash`) stay plain: no snapshot reaches those segments. Recovery's publication stays plain too:
  `recoverLocked` copies each rebuilt segment over the shared one, slots included, as `walIndexRecover` does
  with `memcpy` in C's default build (`wal.c:1524-1533`; only a `SQLITE_SAFER_WALINDEX_RECOVERY` build
  stores just the words that differ, `wal.c:1534-1550`). A reader goroutine of the same handle can be
  probing a segment while it is copied; the copy rewrites the entries of the frames in that reader's window
  with the bytes they already hold. It is a data race by Go's memory model, accepted as C accepts it. Test:
  `TestWalIndexDeadWriter_CleanupUnderReader` under `-race` (the detector sees the heap index only; it does
  not track the mmap'd one).

<a id="drift-93-wal-hash-probe-full-chain-corruption-signal-dropped"></a>
### **Resolved (stale, 2026-06-25)** — shmHashGet now bounds the per-segment probe and propagates full-chain corruption as ErrCorrupt, matching C's walFindFrame (write path shmHashWrite likewise returns ErrCorrupt); cited wal.go:1081 is stale. Drift: WAL Hash Probe Full Chain Corruption Signal Dropped
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*walIndex.get`
  (`internal/btree/wal.go:1081` shmHashGet probe loop with no corruption return; reached from
  `wal.go:765` multi-process get and `wal.go:743` get).

SQLite's `walFindFrame` bounds each hash-table probe chain with an `nCollide` counter initialized to
`HASHTABLE_NSLOT` (8192); if the inner while-loop walks that many non-zero slots without hitting a zero slot
the table is full/corrupt and SQLite aborts the read with `*piRead = 0; return SQLITE_CORRUPT_BKPT;`
(`wal.c:3592-3594`). Go's read path caps the probe with `for range htNSlot` (`wal.go:1081`,
`htNSlot=8192=HASHTABLE_NSLOT`) but, on walking a full chain of non-zero slots, simply exits the loop and
reports "not found" with no corruption return. The consequence is that the full-chain corruption signal
SQLite raises is dropped: a corrupt wal-index hash segment yields a silent miss in Go rather than a propagated
`SQLITE_CORRUPT`, hiding the underlying corruption.

<a id="drift-94-htsegmentinfo-adds-nentry-bound-replacing-c-mask"></a>
### Drift: htSegmentInfo Adds nEntry Bound Replacing C Mask
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:htSegmentInfo`
  (`internal/btree/wal.go:1034` definition; `wal.go:1176,1189-1198` use in shmHashGet).

C's `walHashGet` (`wal.c:1167`) populates a `WalHashLoc` with only `aHash`, `aPgno` (base) and `iZero`
(`wal.c:1146-1151`) — no entry count — and `walFindFrame`'s lookup indexes `sLoc.aPgno[iH-1]==pgno`
(`wal.c:3571`) directly, with no upper bound or mask, relying on the frame-range and `nCollide` invariants
for safety. Go's `htSegmentInfo` (`wal.go:1034`) additionally returns `nEntry` (`htNPageOne=4062` for
segment 0, `htNPage=4096` otherwise), and `shmHashGet` uses it as a defensive index bound:
`idx := int(entry)-1; if idx < nEntry { storedPgno = region[pgnoBase+idx*4] }` (`wal.go:1189-1198`). The
consequence is a behavioral divergence in how the `aPgno` index is constrained: Go adds an explicit
`idx < nEntry` upper-bound check that C lacks, so an index C would read unconditionally Go instead
rejects/skips — a different (and stricter) handling of out-of-range hash entries.

<a id="drift-2026-11-19-dead-writer-cleanup-limit-follows-the-batch"></a>
### Drift: Dead-Writer Cleanup Limit Follows The Batch
- **Category:** changed-logic  -  **Severity:** low (strictly keeps more; closes a window C has)
- **Affected functions:** `wal.go:*walIndex.setBatch`, `wal.go:*walIndex.shmHashWrite`, `wal.go:*walIndex.shmCleanupHash`.

`walIndexAppend` runs `walCleanupHash` when the frame's `aPgno` entry is already set (`wal.c:1327-1328`), and
`walCleanupHash` removes every entry above `pWal->hdr.mxFrame` (`wal.c:1258-1271`). `walFrames` advances
`pWal->hdr.mxFrame` after its `walIndexAppend` loop (`wal.c:4243`), so for every frame of a batch the limit is
the last frame before the batch. That is right when the leftover is a dead writer's spill: its entries start at
the first frame after the log's end, the batch's first frame finds its `aPgno` set, and the cleanup runs before
the batch has indexed anything. It is wrong for a writer that died inside a rollback's cleanup, which clears
the hash slots and then the page numbers (`walCleanupHash`'s `memset`, `wal.c:1271`; here
`shmCleanupHashSegment`). Neither `memset` nor Go's `clear` promises an order of stores, so a writer killed
inside it may leave any part cleared, a prefix among them: with the page numbers of the first frames cleared
and the later ones not, the batch
indexes its first frames, finds a later `aPgno` set, and cleans back to the limit — removing the entries it has
just written. The commit succeeds and those pages read as they were before it. By reading, C behaves the same
in that state (the limit is `pWal->hdr.mxFrame`, the batch's own entries are above it, and the `assert(
!sLoc.aPgno[idx-1] )` at `wal.c:1329` holds); the state needs a process killed inside one `memset`.

Go passes `shmHashWrite` the last frame whose entry is to be kept: the last frame before the batch for the
batch's first frame, then each frame as it is indexed (`setBatch`). The cleanup therefore never removes an
entry of the batch, and removes the same entries as C's whenever it runs at the batch's first frame. Test:
`TestWalIndexDeadWriter_PartlyClearedTail`.

<a id="drift-95-shmcleanupfromframe-zeros-all-segments-above-target"></a>
### Drift: shmCleanupFromFrame Zeros All Segments Above Target
- **Category:** changed-logic  -  **Severity:** low (strictly-more-work, no correctness risk)
- **Affected functions:** `wal.go:*walIndex.rollbackToFrame`
  (`internal/btree/wal.go:685-721` shmCleanupFromFrame, called from rollbackToFrame `wal.go:657`;
  lazy zero-init now present in shmHashWrite `wal.go:1085-1088`).

SQLite's `walCleanupHash` (`wal.c:1233-1288`) deliberately cleans up ONLY the single hash table that contains
`pWal->hdr.mxFrame`: it calls `walHashGet(walFramePage(mxFrame))` (`wal.c:1252`) to obtain that one segment,
then zeroes `aHash[i] > iLimit` and memsets `aPgno[iLimit..]` within just that segment — the header comment
is explicit that "At most only the hash table containing `pWal->hdr.mxFrame` ..." needs cleaning, because the
`idx==1` zero-init in `walIndexAppend` (`wal.c:1315-1319`) lazily clears higher segments on reuse. Go's
`shmCleanupFromFrame` (`wal.go:685-721`) instead loops over ALL segments above the target frame (loop at
`wal.go:691`), zeroing each trailing segment wholesale (`wal.go:701-705`). Go now also HAS the equivalent
lazy zero-init: `shmHashWrite` clears the whole region on a segment's first entry (`idx==0`, `wal.go:1085-1088`).
The consequence is a benign redundancy rather than a compensating necessity: the eager scrub on rollback is
strictly more work than SQLite's single-segment cleanup contract, but it is harmless because the lazy
zero-init would re-clear any reused segment anyway — no correctness risk. (The stale code comment at
`wal.go:681-684` still claims setBatch lacks the zero-init step and should be reconciled.)

<a id="drift-96-wal-index-change-counter-ichange-never-incremented"></a>
### Drift: WAL Index Change Counter iChange Never Incremented
- **Category:** changed-logic  -  **Severity:** low  -  **Status:** RESOLVED 2026-11-13: a commit publishes the replaced header's `iChange` plus one (`wal.c:4245`), a WAL restart or reset keeps it (`walRestartHdr`, `wal.c:2150-2154`), and a recovery leaves 0 (`wal.c:1408`); the process-local `wi.iChange` is gone, the value always comes from the live header read under the write lock (`wal.publishHeader`). A v2.4.x writer on the same file still publishes 0; nothing orders headers by `iChange`, here or in SQLite. Test: `TestChangeEpoch_RecoveryAndIChange`. What follows describes the code before that.
- **Affected functions:** `wal.go:*wal.writeFrames`
  (`internal/btree/wal.go:853` writeHeader writes wi.iChange, never incremented; commit path
  `wal.go:2036`),
  `wal.go:*walIndex.writeHeader` (`internal/btree/wal.go:841`; decl `wal.go:540-541`, read-only).

SQLite's `walFrames` increments the wal-index header change counter once per committed transaction — inside
`if(isCommit)` it executes `pWal->hdr.iChange++;` (`wal.c:4231`, block 4230-4233) just before `walIndexWriteHdr(pWal)`
(`wal.c:4236`), so each commit publishes an incremented `iChange` into the SHM header. any-store reserves the
same field (`WalIndexHdr.iChange uint32` at `wal.go:407,540-541`, serialized at bytes 8..11) and `writeHeader`
copies it via `wi.hdr.iChange = wi.iChange` (`wal.go:853`), but `wi.iChange` is NEVER incremented anywhere in
the package despite the declaration comment claiming it is "incremented on each write transaction"
(`wal.go:540`). The consequence is that the wal-index change counter is always published as 0: the
per-transaction monotonic counter SQLite uses (e.g. to let other connections cheaply detect that the schema
or content changed) is effectively dead in the Go port, diverging from SQLite's per-commit increment.

<a id="drift-97-wal-restart-randomizes-both-salts-instead-of-incrementing-sa"></a>
### Drift: WAL Restart Randomizes Both Salts Instead Of Incrementing Salt0
- **Category:** changed-logic  -  **Severity:** low  -  **Status:** RESOLVED 2026-11-12: a restart (`doResetWAL` → `restartHeaderStateLocked`) publishes the replaced log's first salt plus one and draws only the second, as `walRestartHdr` does (`wal.c:2152-2153`). The replaced log's salt is read from the live wal-index header under the write lock, since a process that has not written since a peer's reset holds an older one. A WAL with no log to continue (`initHeaderStateLocked`, `writeHeader`) still draws both, as SQLite does for the first frame of a WAL it has no checkpoint sequence for (`wal.c:4087`). Test: `TestWalCheckpointTruncate_ResetsAndIncrementsSalt`. What follows describes the code before that.
- **Affected functions:** `wal.go:*wal.initHeaderStateLocked` (`internal/btree/wal.go:1754-1761`, the restart path reached from `doResetWAL` at `wal.go:3870`); `wal.go:*wal.writeHeader` (`internal/btree/wal.go:1801-1808`).

SQLite's `walRestartHdr` (`wal.c:2146-2153`) deterministically advances salt-1 by `aSalt[0] = 1 + aSalt[0]`
(`wal.c:2152`) and only randomizes salt-2 via `memcpy(&aSalt[1], &salt1, 4)` (`wal.c:2153`), so each new WAL
generation's salt-1 is a monotonic successor of the previous header's value. Go's restart path is now
`initHeaderStateLocked` (reached from `doResetWAL` at `wal.go:3870`, the analog of `walRestartHdr`); `doResetWAL`
defers the on-disk header write to `flushHeader` but still regenerates BOTH salts in-memory as fresh independent
random 32-bit values — `salt1: rand.Uint32()` and `salt2: rand.Uint32()` (`wal.go:1760-1761`; `writeHeader`
carries the same pattern at `wal.go:1807-1808`) — with no relation to the prior generation's salt-1. The consequence is that the
deterministic monotonic relationship SQLite maintains across WAL restarts is lost: any reasoning or tooling
that relies on salt-1 incrementing by one per restart no longer holds, though correctness still rests on the
salt pair simply differing from the previous generation.

<a id="drift-98-wal-checkpoint-sequence-number-nckpt-never-incremented"></a>
### Drift: WAL Checkpoint Sequence Number nCkpt Never Incremented
- **Category:** changed-logic  -  **Severity:** low  -  **Status:** kept deliberately (2026-11-12), together with one more difference of the same origin: the restart header's `aFrameCksum`. SQLite's `walRestartHdr` leaves the old `aFrameCksum` in the header and the first frame of the new log replaces it with the checksum of the WAL header it writes, `nCkpt` included (`wal.c:4086`, `4095-4096`). Here the restart computes the new WAL header at once and publishes its checksum as `aFrameCksum`; a writer then writes the WAL header from its own fields, with checkpoint sequence 0, and seeds its frame chain from the published `aFrameCksum` (`beginWriteWithSnapshot`). Every released version does this, v2.4.x included. Publishing the old checksum, or a non-zero `nCkpt`, would make such a writer chain its frames from a seed that does not match the header on disk: recovery (`recoverLocked`) would reject frame 1 and an acknowledged commit would be lost. So `nCkpt` stays 0 and the seed stays the new header's while any such writer may attach. What `nCkpt` tells a SQLite savepoint, that the log was started over inside the transaction (`wal.c:3829-3835`), comes from a process-local generation count instead (`wal.logGen`, `wal.base`).
- **Affected functions:** `wal.go:*wal.doResetWAL`
  (`internal/btree/wal.go:1801-1806` writeHeader sets `checkpoint:0`; doResetWAL `wal.go:3833-3871` calls
  writeHeader; initHeaderStateLocked `wal.go:1754-1759`),
  `wal.go:*wal.writeHeader` (`internal/btree/wal.go:1801-1806`).

SQLite's `walRestartHdr` does `pWal->nCkpt++` on every WAL restart (`wal.c:2150`) and serializes it into the
WAL header (`wal.c:4072`), making the on-disk WAL
header "Checkpoint sequence number" field (offset 12) a monotonically increasing counter that advances on each
TRUNCATE checkpoint and each writer-initiated log wrap, the two callers of `walRestartHdr` (`wal.c:2377`, `3887`); a RESTART checkpoint does not run it (`wal.c:2357-2362`). The Go reset path always constructs the header
with `checkpoint: 0` at both sites — `writeHeader` (`wal.go:1801-1806`) and `initHeaderStateLocked` (`wal.go:1754-1759`)
— and `doResetWAL` (`wal.go:3833-3871`) invokes one of those on every reset, so the field is serialized at
offset 12 (`wal.go:244`), deserialized (`wal.go:262`), but never fed back to re-increment. The consequence is
that the checkpoint-sequence counter is always 0: SQLite's monotonic per-restart sequence number is dead in
the port, so any cross-process logic keyed off an advancing nCkpt cannot distinguish WAL generations.

<a id="drift-99-wal-restart-read-mark-reset-diverges-from-walrestarthdr"></a>
### Drift: WAL Restart Read Mark Reset Diverges From walRestartHdr
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*walIndex.reset`
  (`internal/btree/wal.go:824-826` read-mark clobber, full reset at `internal/btree/wal.go:816-829`;
  the dropped nCkpt/salt increments live in `wal.writeHeader` (`internal/btree/wal.go:1801-1830`) and
  `initHeaderStateLocked` (`internal/btree/wal.go:1754-1776`), invoked via `doResetWAL`
  (`internal/btree/wal.go:3833-3871`)).

C's `walRestartHdr` treats slot-0's read-mark as a permanent invariant fixed at 0 (it never touches
`aReadMark[0]` and asserts `aReadMark[0]==0`, `wal.c:2159`), explicitly sets `aReadMark[1]=0` (`wal.c:2157`),
sets only `aReadMark[2..4]=READMARK_NOT_USED` (`wal.c:2158`), and additionally performs `pWal->nCkpt++`
(`wal.c:2150`), `aSalt[0] = 1 + aSalt[0]` (`wal.c:2152`), and a fresh random `aSalt[1]` (`wal.c:2153`). Go's
`reset()` instead clobbers ALL five slots — including slot 0 and slot 1 — to `readMarkNotUsed` (0xFFFFFFFF)
via `for i := range wi.aReadMark { wi.aReadMark[i].Store(readMarkNotUsed) }` (`wal.go:824-826`), and the
`doResetWAL`/`writeHeader` pair drops the nCkpt and salt-1 increments (`wal.go:1801-1830`, `wal.go:1754-1776`;
`doResetWAL` at `wal.go:3833-3871`). This is the same
root as the restart counter/salt drifts (97, 98) surfacing on the read-mark/reset path (the fourth
`walRestartHdr` rule, that `hdr.nPage` survives the reset, was missing too and is ported as of 2026-11-11:
`doResetWAL` passes the last commit's size to `restartHeaderStateLocked`): the consequence is
that the `aReadMark[0]==0` invariant is broken on restart (a slot-0 reader normally views only the db file)
and the deterministic restart sequence/salt advances are lost. Note the recovery path (`wal.go:2006-2009`)
does correctly restore `aReadMark[0]=0` after clobbering all slots, unlike `reset()`.

<a id="drift-2026-11-11-3-reader-size-without-a-wal-commit-is-memoized"></a>
### Drift: A Reader's Database Size Without A WAL Commit Is The File Size Memoized Per wal-index Header
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.effectiveReaderDbSize`; `wal.go:*wal.doResetWAL`, `wal.go:*wal.restartHeaderStateLocked` (ported rule); `integrity.go:*DB.IntegrityCheckN` (uses the same size).

SQLite sizes the database at the start of every transaction with `pagerPagecount` (`pager.c:3279-3306`): `sqlite3WalDbsize`, i.e. `pWal->hdr.nPage`, when it is non-zero, else an `fstat` of the database file. `walRestartHdr` zeroes `mxFrame` and leaves `hdr.nPage` alone (`wal.c:2146-2160`), so the first branch keeps answering after a RESTART or TRUNCATE.

Go had neither half. `doResetWAL` published the reset header with `nPage = 0`, and `effectiveReaderDbSize` fell back to the process-local `pager.dbSize`, which only a write begin refreshes. A process that had not written since a peer grew the database therefore bounded its readers by its own stale size as soon as any process reset the WAL, and valid pages failed with `ErrCorrupt` ("invalid page number" from `IntegrityCheck`, whose cap used the equally process-local `walIndex.maxPage`). Both rules are now ported: the reset header keeps the last commit's size, and without a committed size a multi-process reader takes the file's. A failed file-size query fails the read begin, as `pagerPagecount` returns the `fstat` error (`pager.c:3299-3303`); until 2026-11-12 it fell back to the local size (regression test `TestReaderSizeStatErrorPropagates`, vfs tag). The in-process and in-memory modes publish no wal-index header; their readers take the size of the committed position, frame and size kept in one word and moved only by a published commit (`commitFrameCounter`). Until 2026-11-12 they took `pager.dbSize`, which an open write transaction moves. That size is 0 until a commit records one, as in SQLite's header, and an in-process reader without one takes the file's, memoized like a multi-process reader's; an in-memory database has no file, and its reader without one, before the first commit, takes the one page creation wrote.

The remaining difference is that the file size is read once per wal-index header, not once per transaction (`pager.noWalCommitSize`). Measured on an M2: the `fstat` costs about 390 ns and one allocation, a sixth of a point read on a freshly opened, checkpointed database — the state a read-mostly process stays in. The memo is exact as long as the file's size cannot change under an unchanged 48-byte header that carries no size: the file changes only when a checkpoint copies frames into it; frames need a commit; a commit publishes a header with a size, and every later reset keeps it; a header without a size reappears only when a process rebuilds the index over a WAL holding no commit, which after a TRUNCATE means freshly drawn salts. The one way back to the identical header is a WAL whose frames have been destroyed behind an intact WAL header, and then the stale (smaller) size fails closed: pages beyond it read as corrupt, none are served wrongly.

Of the other halves of `walRestartHdr`, the salts are ported as of 2026-11-12 (drift-97, resolved); the checkpoint sequence stays 0, deliberately (drift-98); the read-mark values remain as drift-99. Regression tests (`internal/btree/walindex_restart_npage_test.go`, two processes): `TestWalRestartKeepsNPage_PeerResets` (TRUNCATE; a writer's restart is `TestWalWriterRestart_PeerDiesAfterRestart`), `TestWalRestartKeepsNPage_OwnCheckpoint`, `TestReaderSizeWithoutWalCommit`, `TestIntegrityCheckAfterPeerGrowth`.

<a id="drift-2026-11-12-1-wal-restart-clears-the-index-before-it-publishes-the-reset-header"></a>
### Drift: A WAL Restart Clears The Index Before It Publishes The Reset Header
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.restartHeaderStateLocked`, `wal.go:*walIndex.reset`, `wal.go:*wal.doResetWAL`.

SQLite's `walRestartHdr` (`wal.c:2146-2160`) writes the reset wal-index header first and then zeroes `nBackfill` and `nBackfillAttempted` and resets the read marks. It leaves the hash tables alone: `walIndexAppend` clears each one when the next generation adds its first frame to it (`wal.c:1312-1319`). Go's `restartHeaderStateLocked` runs `walIndex.reset` first, which clears the hash tables and publishes `nBackfill = 0`, `nBackfillAttempted = 0` and the read marks, and only then writes the header.

The order decides what peers see when the resetting process dies between the two steps. In SQLite's order they see a header with no frames over a `nBackfill` still at the old frontier N. The next writer's frames 1..k then lie below the frontier: readers skip them (their `minFrame` is N+1, or they take slot 0 once k reaches N), and a checkpoint counts them as backfilled, so a later reset discards them. In Go's order the peers see the old header over the intact WAL and the fully backfilled database file, with an empty hash and a zero frontier. Readers resolve no frame and read the file, which holds the same content. The next writer appends after frame N, a checkpoint copies only its frames, and recovery reads the whole WAL. The window is a few stores to shared memory with no system call in either order, but only Go's order survives a death inside it.

Both steps come before the truncate, as in SQLite (`walRestartHdr`, then `sqlite3OsTruncate`, `wal.c:2377-2378`). Until 2026-11-12 `doResetWAL` truncated the WAL between the index clear and the header publish. A checkpointer that died there left peers the old header over an empty WAL: a surviving writer acknowledged its commit as frames after the missing ones, and the next recovery stopped at the hole and dropped it. Regression test (two processes, the checkpointer exits at each reset step): `TestWalResetCrash_SurvivorCommitsKept` (`internal/btree/wal_reset_crash_test.go`).

<a id="resolved-2026-11-12-restart-checkpoint-reset-the-wal"></a>
### Resolved 2026-11-12: A RESTART Checkpoint Reset The WAL; No Writer Started The Log Over
- **Category:** changed-logic  -  **Severity:** medium  -  **Status:** RESOLVED 2026-11-12
- **Affected functions:** `wal.go:*wal.restartLogLocked` (new), `wal.go:*wal.waitReadersAndTruncate` (was `tryResetWALWithBusy`), `wal.go:*wal.checkpointPost`, `wal.go:*wal.checkpointWithMode`, `wal.go:*wal.doResetWAL`, `wal.go:*wal.base` (new), `pager.go:*pager.tryCheckpoint`, `pager.go:*pager.walBase` (new).

SQLite recycles the WAL in the writer. A RESTART checkpoint, once the whole log is in the database file, takes the reader locks and releases them: it only makes sure no reader still uses the log (`wal.c:2357-2362`, `2380`). A TRUNCATE checkpoint also resets the wal-index header and truncates the file (`wal.c:2363-2378`). The next writer, before the first frame of a transaction, starts the log over if it holds read lock 0, the log is fully backfilled and it can take the other reader locks without waiting (`walRestartLog`, `wal.c:3870-3888`, called from `walFrames` at `wal.c:4070`); its first frame then writes and syncs the new WAL header (`wal.c:4074-4115`).

Until 2026-11-12 the port had no writer-side restart. `doResetWAL` stood in for it inside the checkpoint: RESTART reset the header at once, and the automatic checkpoint escalated from PASSIVE to RESTART so that the WAL was recycled at all (drift-53). The cost showed once another process's commits were to be detected by the wal-index header instead of a page-1 counter: a RESTART by a process that committed nothing changed the header under everyone else, where SQLite's leaves it alone.

Now, as in SQLite:

- **RESTART** waits for reader slots 1-4 and releases them. **TRUNCATE** resets the header and truncates, also when no frame is committed: the `mxFrame == 0` shortcut skips only the backfill, so the frames of a rolled-back transaction or of an abandoned restart are cut off (`wal.c:2216`, `2352-2380`).
- **A reader that keeps a slot past the busy handler makes both `ErrBusy`** (`wal.c:2361`); before, the checkpoint reported success and did nothing.
- **The writer** calls `restartLogLocked` at the top of `writeFrames` and `writeFramesMem`. It restarts when no frame of the transaction is in the log yet, the authoritative `nBackfill` equals the committed `mxFrame` and is not zero, and the checkpoint lock and reader slots 1-4 can be taken without waiting. A busy lock means append (`wal.c:3889-3890`). The heap-shm modes (in-process, in-memory) relied on the same escalation and get the same restart; in in-memory mode it is what reuses the frame arena.
- **The automatic checkpoint** is PASSIVE (drift-53 resolved).
- **A checkpoint re-checks the salts** once it holds reader slot 0 and copies nothing if the log was started over since it read the header. This is SQLite's fix for the race between a checkpointer and a wrapping writer, which is in `version-3.52.0` (`wal.c:2256-2267`) and not in the sibling `sqlitec` working tree, which is an earlier commit.

**Rollback after a restart.** A transaction saves its position in the log when it begins and at each savepoint. After a restart inside the transaction those frame numbers and checksums belong to the previous log. SQLite's undo reloads the restarted header (`wal.c:3775`) and its savepoint undo tests `nCkpt` (`wal.c:3829-3835`). Here the WAL keeps a generation count and the checksum seed of the current log (`wal.logGen`, `genSeed1/2`), every saved position carries its generation, and `wal.base` maps a position of an earlier generation to frame 0 of the current one. The pager never reads a saved position directly. `rollbackLocked`, `pagerError` and `rollbackToSavepoint` take the whole position from `wal.base` (through `pager.walBase` for the transaction's own). `hasChanges`, the "was the commit published" test of `unwindWriter`, the "did this transaction spill" test of `commit` and `rollbackForClose` need the frame number alone and take it from `pager.walBaseFrame`, the same rule over atomics only, because `rollbackForClose` runs on the closing goroutine. A transaction's changes are "frames above its base", so a transaction whose pages were all spilled after a restart still commits, and one whose frames were all undone is still empty. Nothing has to run between the restart and a failure for the cleanup to be right.

**Crash ordering is SQLite's.** The database file is synced before `nBackfill` is published (`wal.c:2322-2332`), so a log that may be started over is entirely in a synced database file. The restart writes nothing to the WAL file. The new WAL header is written and synced before the first frame of the new log (`flushHeader`; `wal.c:4099-4115`). At every point in between, the file recovers to the last commit (`TestWalWriterRestart_CrashPoints`).

**What each mode does to the file.** RESTART and a writer's restart reuse the file from its start; neither gives space back. Only TRUNCATE (and the last close) shrinks it. There is no size limit ([drift-105](#drift-105-journal-size-limit-wal-truncation-feature-unimplemented)).

**With v2.4.x processes attached** (they have no writer-side restart and reset on RESTART):

- The header a restart publishes is the one their checkpoint-time reset publishes (see drift-98 for the checksum seed and `nCkpt`), so a v2.4.x writer or reader that meets it behaves as it does today, whether the restarting writer then commits, rolls back or dies.
- A v2.4.x writer whose automatic checkpoint is off, next to a newer process that only checkpoints: RESTART from the newer process no longer recycles the WAL. Use TRUNCATE there.
- v2.4.3 decides a reset from a process-local `nBackfill`. After any new log of M frames, where M is not above the count it remembers, its RESTART can reset frames that are not in the database file and lose an acknowledged commit. This is in v2.4.3 as shipped and happens whoever started the new log, a checkpoint-time reset or a writer; nothing here prevents it.

Tests: `internal/btree/wal_writer_restart_test.go` (three WAL modes; logs written after a rollback are recovered from a crash copy, not after an orderly close), `wal_writer_restart_crash_test.go` (crash points, a panic after a restart and after a restarted commit is published, two-process cases, the salt re-check), `wal_writer_restart_fault_vfs_test.go` (WAL-header write and sync failures).

To measure (not yet): `restartCandidate` runs on every write of frames and reads the shared `nBackfill`, which in shared-memory mode goes through the shm region lookup.

<a id="drift-2026-11-12-2-writer-restart-takes-the-checkpoint-lock"></a>
### Drift: A Writer Takes The Checkpoint Lock To Start The Log Over
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.restartLogLocked`.

SQLite's `walRestartLog` holds the write lock and takes the reader locks (`wal.c:3876`); it does not touch `WAL_CKPT_LOCK`. A checkpointer that read the header before the restart is kept from copying stale frames by the salt re-check it makes once it holds read lock 0 (`version-3.52.0`, `wal.c:2256-2267`).

`restartLogLocked` also try-locks `lockCheckpoint` exclusive, and appends if it is busy. Reason: a v2.4.x process may be attached to the same file, and its checkpoint has no salt re-check. It was safe only because every reset held the checkpoint lock for as long as a checkpoint does; a restart that did not take the lock could land between that checkpoint's header read and its backfill, and the checkpoint would then copy frames of the old log, read at their old offsets from a file the new log is overwriting, over newer pages. With the lock, the invariant "every reset holds the checkpoint lock" still holds for both versions.

Cost: a writer appends instead of restarting while any checkpoint runs, in any process. The salt re-check is ported as well, so the lock can go when no version without the re-check can attach. The re-check is reachable only against a writer that does not take the lock: `TestWalCheckpoint_SkipsBackfillAfterLogWrapped` reaches it through a hook.

<a id="drift-2026-11-12-3-writer-keeps-slot-0-after-a-restart"></a>
### Drift: A Writer Keeps Reader Slot 0 After It Started The Log Over
- **Category:** structural  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.restartLogLocked`.

After `walRestartHdr`, SQLite's `walRestartLog` releases read lock 0 and takes a WAL read lock through `walTryBeginRead` (`wal.c:3893-3899`). It has to: a connection on lock 0 ignores the WAL (`walFindFrame` returns no frame when `readLock == 0`), and the writer must read back the frames it is about to write.

The writer here does not read through its reader slot. `getPageWriter` looks pages up to `wal.nFrame`, above the live checkpoint frontier (`liveMinFrame`), whatever slot the transaction's snapshot took. So it keeps slot 0 until the transaction ends. Every first writer after a reset already wrote from slot 0 before this change (a reset left `mxFrame == 0`, and a begin then takes slot 0).

For the same reason the restart does not test "the writer is on slot 0" as `wal.c:3870` does: a writer whose snapshot is on slots 1-4 holds that slot shared, and the exclusive locks the restart needs fail on it.

**Other holders of slot 0.** With shared memory, slot 0 is granted only after a full backfill, with `minFrame = mxFrame + 1`: such a reader resolves no frame, of the old log or the new. The heap-shm read begin also falls back to slot 0 with part of the log still to read (`tryBeginReadInProcessHdr`, when slots 1-4 are briefly held exclusive); SQLite retries instead (`wal.c:3128-3136`, then WAL_RETRY). That reader blocks the backfill, so `nBackfill` stays below `mxFrame` for as long as it lives, and the restart, which tests `nBackfill == mxFrame` explicitly where SQLite asserts it (`wal.c:3872`), does not happen. Tests: `TestWalWriterRestart_NotBeforeBackfill`, `_NotAfterPartialBackfill`. Whether the fallback itself should become SQLite's retry is a separate question of reader admission latency.

<a id="drift-2026-11-12-4-a-commit-that-spilled-restarts-attached-backups"></a>
### Drift: A Commit That Spilled Pages Restarts The Attached Backups
- **Category:** changed-logic  -  **Severity:** low  -  **Status:** RESOLVED 2026-11-13: `pagerStress` sends the spilled page to the attached backups, and `Backup.Step` is kept out of a source write transaction of the process, so the interim restart is gone (see [the change state](#drift-2026-11-13-1-the-change-state-is-one-value-under-compare-and-swap), "Backups"). Test: `TestBackupChange_OwnSpilledCommit`. What follows describes the interim rule.
- **Affected functions:** `pager.go:*pager.commit`, `pager.go:*pager.pagerStress`.

SQLite sends an attached backup every page it writes to the WAL, a spill as much as a commit (`pagerWalFrames`, `pager.c:3219-3225`, called from `pagerStress` at `pager.c:4649`), and restarts the backup only when such a transaction is rolled back (`pagerUndoCallback`, `pager.c:3139`). `backup_step` cannot run in between: it returns `SQLITE_BUSY` while the source is in a write transaction (`backup.c:340-341`).

`pagerStress` here writes the frame and marks the page clean without telling the backups, and `commit` sends them only the pages that are still dirty. A backup that had copied a spilled page kept its old content next to the commit's other pages. Before 2026-11-12 the automatic checkpoint reset the WAL and restarted every backup with it, which hid this for the default configuration.

Until the spill sends its page (which needs `Step` kept out of a source write transaction, as SQLite does, so that an uncommitted page never reaches a finished backup), a commit whose transaction has frames in the log before the commit's own write restarts the attached backups after its update dispatch. A rolled-back transaction needs nothing: its spilled pages never reached a backup. Cost: a backup running beside transactions larger than the writer cache starts over at each of them. Test: `TestBackup_SpilledCommitRestartsBackup`.

<a id="drift-2026-11-12-5-page-1-written-when-its-header-changes"></a>
### Drift: A Schema Format 6 Commit Writes Page 1 When Its Header Changes, Not Whenever A Write Touched It
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.commitNeedsPage1`, `pager.go:*pager.commit`, `pager.go:*pager.page1OnChange`.

SQLite puts page 1 in a WAL commit when it is on the dirty list, and fetches it as the frame that carries the commit mark when the list is empty, every dirty page having been spilled (`pager.c:6500-6512`, ticket 2d1a5c67). Every write to the header makes page 1 dirty first: a freelist pop or push (`btree.c:6555-6557`, `6844-6847`), growth (`6760`, `6785`), a meta write such as the schema cookie (`10466-10468`), the size fix-up at write begin (`3774-3778`). The change counter goes into each page-1 frame ([the counter](#drift-77-filechangecount-bumped-conditionally-not-unconditionally)).

any-store keeps the header as a struct that `commit` serializes into page 1. In a schema format 6 file `commit` writes page 1 when the struct differs from the one the transaction began with (compared once the schema cookie has moved, before the counters are set), when page 1 itself is dirty (the catalog's root, a savepoint restore), or when no page is dirty (the carrier). Schema format 5 files write page 1 in every non-empty commit, for the v2.4.x processes that may be attached to them. The contract: after every commit, page 1's bytes 0..99, apart from the change counter (24) and VersionValidFor (92), serialize the header that commit committed, and its b-tree content is the committed content.

The difference is the transactions whose header writes net to zero and that leave page 1's content alone. SQLite writes page 1 in them, with only 24 and 92 changed; any-store leaves the frame out. Such transactions are a page taken from the freelist and another freed onto it, or a page freed and taken back, with the trunk and count back where they were; in particular a value whose cell changes size and keeps an overflow chain of the same length, when the freelist can supply it: `sqlite3BtreeInsert` builds the new cell with `fillInCell`, which takes the chain's pages, then frees the old cell's chain with `BTREE_CLEAR_CELL` (`btree.c:9586`, `9607`). For a value rewritten at the same size SQLite writes neither page 1 nor the freelist: `btreeOverwriteCell` overwrites the cell and its chain in place (`btree.c:9482-9490`, `9307-9336`). any-store frees and allocates the chain even then (`btree.go:*btree.updateLeafCell`), a net-zero freelist change, so both leave page 1 out, for different reasons.

How often it matters was measured on the prototype, on the schema format 5 layout, in an indexed small-update workload of 3,000 commits: page 1 went into 39 of them under this rule (37 freelist changes, 2 growths). An instrumentation model that counted every allocation, free, growth and truncation in Go put it into all 3,000, because the index's sketch blob is an overflow chain rewritten at every commit. That model is not SQLite: SQLite would overwrite a same-size blob in place and write page 1 only when its size changed. So the measurement bounds the saving against Go's own free-and-allocate path, not against SQLite running the workload; it is to be repeated on schema format 6, with a port of `btreeOverwriteCell` as the comparison if that is cheap.

A savepoint is where the two can also differ in the change counter, not only in frames. SQLite journals page 1 at the first header write after a savepoint, and its rollback restores `dbFileVers` from the journaled page (`pager_playback_one_page`, `pager.c:2481-2484`). Here `rollbackToSavepoint` restores it when page 1 itself was restored, or when the header or the page count differs from the savepoint's at the rollback. A net-zero header change inside the savepoint is missed: take a page from the freelist and free it again, roll back to the savepoint, then commit a catalog change. SQLite writes counter 3 there (the restored image's 2, plus one, `pager.c:3089`), any-store writes 2. Nothing reads the counter's progression (see [drift-77](#drift-77-filechangecount-bumped-conditionally-not-unconditionally)).

Nothing reads what the skipped frame would have carried. A write begin derives the header from page 1 at every transaction (`refreshHeaderFromPage1`, as `lockBtree` does at every SQLite transaction, `btree.c:3707`); open reads the newest page-1 frame of its snapshot; readers take the schema cookie at their snapshot's bounds; the counters are compared only with each other (`lockBtreePageCount`). Two inherited dirtying differences are not part of the rule: SQLite's large-sector path makes every page of a sector writable, page 1 included (`pager.c:2728-2740`, `6176-6187`, `6244-6246`), and SQLite does not dirty a cell rewritten with identical bytes (`btree.c:9275-9282`), where any-store makes the leaf writable first (`btree.go:1392-1400`), so a catalog cell rewritten unchanged writes page 1. In test builds every btree, root and `test/` suite checks the contract at every write begin and after every commit (`page1_check.go`). Regression tests: `page1_rule_test.go` (`TestPage1Rule_FrameShape`, `_Format5EveryCommit`, `_FCCLikeSQLite`, `_FCCCycles`, `_FCCWraps`, `_Page1NeverSpilled`, `_SpilledOnlyCommitCarriesPage1`, `_BackupDestinationHeader`), `page1_begin_test.go`, `page1_fault_test.go` and `page1_fault_vfs_test.go` (`TestPage1Rule_FailedCommitThenRetry`), `page1_prop_test.go` (`TestPage1Prop_Lockstep`, two processes in lockstep from a seed, and `_MutationIsCaught`, which leaves the header comparison out and must fail), `page1_crash_test.go` (`TestPage1Crash_Matrix`: six kill points, a fresh-open and an attached-checkpoint recovery), `cache_invalidation_test.go:TestCacheInvalidation_Format6_CounterMovesWithPage1`. The SQLite values the counter tests cite come from the CLI of SQLite 3.52.0 built from the `version-3.52.0` tag (source id `2026-03-06 16:01:44 557aeb43…`).

<a id="drift-2026-11-13-1-the-change-state-is-one-value-under-compare-and-swap"></a>
### Drift: Another Process's Commit Is Found By The wal-index Header, Through One Process-Wide Value Under Compare-And-Swap
- **Category:** structural  -  **Severity:** low
- **Affected functions:** `walchange.go` (`*wal.noteSnapshot`, `announce`, `settle`, `publishHeader`, `*walChange.exact`), `pager.go:*pager.beginReadHdrBusy`, `db.go:*ReadTx.ChangeEpoch`, `backup.go:*Backup.Step`; root `collection.go:*collection.refreshSketches`, `newCollection`.

**SQLite.** A connection keeps the wal-index header it last used in `pWal->hdr`. `walIndexTryHdr` compares the header it reads for a new transaction with it, and on a difference sets `*pChanged` and copies the new one in (`wal.c:2624-2626`); `pagerBeginReadTransaction` then calls `pager_reset` (`pager.c:3261-3263`), which raises `iDataVersion`, restarts the connection's backups and clears its cache (`pager.c:1772-1776`). The connection's own commit writes `pWal->hdr` before it publishes it (`wal.c:4243-4250`), and so does its WAL restart (`wal.c:2151-2154`), so neither is a change to it; the btree cancels the `iDataVersion++` of its own commit (`pager.c:6709`, `btree.c:4417`). A recovery always is a change (`wal.c:2724-2725`), and so is a checkpoint that loaded a header the connection had not seen (`wal.c:4399-4407`).

**Before 2026-11-13.** The port had the header comparison for its page caches only. Derived state (the root package's sketches) and backups watched the page-1 `FileChangeCount`, which is why every commit wrote page 1, and which missed a commit that did not mark its data (drift-77) and took every marked commit of the process itself for news.

**Now.** The comparison is SQLite's, the state it is made against is not a connection's. One `*wal` serves every transaction of a process, at many snapshots at once, so the header last seen and the count of resets cannot be two fields changed by one thread: a consumer could read a count that already includes a change whose header is not stored yet, load state at an older snapshot, and stamp it current for good. They are one immutable value, `walChange{hdr, epoch, pending}`, behind one pointer, changed by compare-and-swap:

- **A begin** (`pager.beginReadHdrBusy`, which every begin of every kind passes) loads the value before it takes its snapshot. A snapshot equal to the value's header, or to a pending own header, is no news. Any other is a change: the epoch is raised, and the header becomes the snapshot's if it is still the one loaded before the snapshot was taken (the snapshot is then the newer of the two). If the header moved in between, by an own commit or another begin, the two cannot be ordered: the change is counted and the header left for the next begin to move. The header therefore never goes back.
- **An own publication** (a commit, a WAL restart, a TRUNCATE reset, a recovery, all under the write lock, all through `wal.publishHeader`) announces its header as `pending` before the first byte is written, so a reader that snapshots it before the committing goroutine has finished its bookkeeping does not take it for another process's. If the value's header is not the one being replaced, a change made elsewhere lies between them and the new header will hold it: its epoch step is taken at the announcement. A recovery always takes one. On every way out, a panic included, `settle` decides from what shared memory holds: the header if it is the announced one, one more epoch step and no pending header if it is not.
- **A consumer** asks for the epoch and whether its transaction's snapshot is exact for it (`ReadTx.ChangeEpoch`): equal to the pending header while there is one, to the value's header otherwise. State loaded through an exact snapshot holds every change the epoch counts, and any change it lacks will raise the epoch. A transaction at an older snapshot is not exact and stamps nothing.

What differs from SQLite, and why it is kept:

1. **Process-wide, under CAS,** for the reason above.
2. **The "order unknown" step** has no SQLite counterpart. It is a false positive only: one extra reload or backup restart.
3. **No epoch step for a failed begin.** SQLite resets on `rc != SQLITE_OK` as well, because `walIndexTryHdr` may already have replaced `pWal->hdr`; here nothing is stored until the snapshot is held, and the next begin finds the change again.
4. **Heap-shm modes** (in-process, in-memory) have no state at all: no other process can attach, the epoch stays 0 and every snapshot is exact. Which mode a database is in is its flags, not its salts (`TestChangeEpoch_ZeroSaltsStillDetect`).

**Backups** (`backup.go`, `pager.go`). With the epoch as the restart signal, the copy has to be kept right across the process's own commits by SQLite's other half, which the old signal's restart-at-every-commit had been hiding:

- A spill sends its page to the attached backups, as a commit does (`pagerWalFrames`, `pager.c:3219-3225`, from `pagerStress` at `pager.c:4649`).
- `Step` holds the write lock of the source's process from its start to its end, taken without waiting, and returns `ErrBusy` if it cannot have it (`sqlite3_backup_step` under the source mutexes, `backup.c:323-324`, and `SQLITE_BUSY` for a source in a write transaction, `backup.c:340-341`). So a commit and the updates it sends are wholly before a Step's snapshot or wholly after the Step, and a page of a transaction that is not committed never reaches a finished copy. The reader slot is taken without waiting too. The destination's write transaction is begun first, with the ordinary waits.
- A transaction that sent pages and did not commit restarts the backups: a rollback that undid anything (`pagerUndoCallback`, `pager.c:3139`), the error cleanup (`pager_reset`, `pager.c:1910-1912`), and, through the writer-owned `pager.backupOwed`, every other way out, including a commit that unwinds after it was published and before all its pages were sent. That flag has no SQLite counterpart: nothing there unwinds past a commit.
- `Step` restarts when its snapshot, or the previous Step's, is not exact for the epoch, or the epoch moved. The process's own commits, WAL restarts and TRUNCATE checkpoints restart nothing.
- `update` and `restart` leave a finished backup alone: the source calls them outside its list's lock, and `Finish` may have run in between.

**Close.** `Step` holding the write lock made visible that `Close` took any holder of that lock for a write transaction and ran the writer's teardown on it. A write transaction now registers under a small mutex, at the point where its teardown is valid; a `BeginWrite` that finds the database closing there backs out with `ErrClosed`, and a `Close` that finds no registered transaction waits for the holder.

Tests: `internal/btree/walchange_test.go`, `backup_change_test.go`, `backup_change_fault_vfs_test.go`, `cache_invalidation_test.go`; root `change_epoch_test.go`; `test/backup_busy_test.go`.

To measure (not yet): allocations per changed begin (one `walChange`) and per publication (two, at the announcement and at the settle, plus the copy of the pending header), CAS retries under concurrent readers, the fast read path, and how far a backup gets between the write transactions of a busy writer.

<a id="how-long-a-backup-can-wait"></a>
### How Long A Backup Can Wait

Not a divergence: the waits are SQLite's, added up. `sqlite3_backup_step` begins a transaction on the source and, the first time, on the destination, each through the btree's ordinary busy loop (`backup.c:351`, `367`; `btree.c:3736-3737`), and every read attempt inside a begin starts a fresh WAL protocol count (`cnt = 0` in `walBeginReadTransaction`, `wal.c:3370`, `3405-3407`).

- `T` is the busy timeout (`BusyTimeout`, 5 s). `H(T)` is the number of times the busy handler agrees to retry: 12 steps cover the first 328 ms, then one per 100 ms (`DefaultBusyTimeout`; SQLite's `delays[]`, `main.c:1717-1720`), so `H = 12 + ceil((T - 328 ms) / 100 ms)` for `T ≥ 328 ms`; `H(5 s) = 59`.
- `P` is the WAL protocol allowance of one read attempt: 100 retries with sleeps of `(cnt-9)² × 39 µs`, about 10 s in all, after which the attempt fails with `ErrProtocol` (`wal.beginReadHdrRetry`).
- A write begin makes at most `N_w = 1 + 3 + H(T)` read attempts: the first, `busySnapshotInnerRetries = 3` tight retries, and one per handler call (`DB.BeginWriteContext`). It takes at most `T + N_w × (P + one write-lock try)`.
- A read begin makes at most `N_r = 1 + H(T)` attempts and takes at most `T + N_r × P`.

The public `Backup` gives up on busy Steps once `T` has passed since the last Step that copied. Its worst case is that window, plus the begins of the last Step (one read begin on the source; one write begin on the destination, at the first Step), plus one begin each for opening the destination, for the bookkeeping on the committed copy, and for the write-lock attempt of the destination's close.

With `T = 5 s`, `N_w = 63`: one write begin can take about `5 + 63 × 10 ≈ 635 s` if another process leaves the header torn for just under `P` before each of its commits. That takes an adversarial peer; an ordinary one costs a begin milliseconds. A ctx is looked at between Steps, not inside a begin, as in every other transaction begin.

Tests (`backup_public_test.go`, with short budgets and a pause far below `P`): `TestBackupPublic_PausedPublisher` (one protocol wait, waited out; and the full allowance ending in `ErrProtocol`), `_EveryBusyMatchesErrBusy/destination-snapshot` (a snapshot overtaken at every attempt, no protocol wait), and `_PausedPublisherAndSnapshotInvalidation`, which combines the two on one write begin: every attempt first waits for a paused publisher and is then overtaken, each attempt's error is checked to be `ErrBusySnapshot`, and the public return is timed against `T + N_w × (pause + slack)`. The full-length bound (`P` of ten seconds at every attempt) is not run.

<a id="drift-2026-11-13-2-a-backup-step-that-met-a-recovery-can-be-retried"></a>
### Drift: A Backup Step That Met A Recovery In Progress Can Be Retried
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `backup.go:backupBusy`, `backup.go:*Backup.Step`.

SQLite's `isFatalError` keeps exactly `SQLITE_BUSY` and `SQLITE_LOCKED` out of a backup's sticky `rc` (`backup.c:217-219`). It compares whole codes, so `SQLITE_BUSY_RECOVERY` and `SQLITE_BUSY_SNAPSHOT` (`SQLITE_BUSY | n<<8`) are fatal for a backup there.

`Step` treats `ErrBusy`, `ErrBusySnapshot`, `ErrBusyRecovery`, `ErrReadersBusy` and the ctx's own error alike: returned, not kept. (Any other error stays sticky even if the ctx has ended meanwhile.) `Finish` returns the last Step's busy error when the backup is neither done nor failed, as `sqlite3_backup_finish` returns the `p->rc` the step left (`backup.c:558`, `603`). A begin that failed its admission holds no transaction and has copied nothing, so a later Step on the same `Backup` continues correctly, and another process rebuilding the wal-index is an ordinary event of multi-process use, not a reason to abandon a copy. `ErrProtocol` stays fatal, as `SQLITE_PROTOCOL` is. Test: `TestBackupChange_StepBusy`; the recovery case is exercised by `TestBackupChange_StepRunsRecovery`, where the Step's own begin does the rebuild.

<a id="drift-2026-11-16-1-a-long-backup-step-ends-early-when-its-ctx-ends"></a>
### Drift: A Long Backup Step Ends Early When Its ctx Ends
- **Category:** added-logic  -  **Severity:** low
- **Affected functions:** `backup.go:*Backup.Step`, `backup.go:*Backup.Restarts`, `backup.go:*Backup.restartLocked`.

`sqlite3_backup_step` copies `nPage` pages, or every remaining page when `nPage` is negative, in one loop with no way out but an error (`backup.c:390-401`); the caller bounds a call by the `nPage` it passes.

`Step` takes a ctx, and looks at it once before it takes anything. A Step of more than `backupCtxPages` (256) pages now also looks at it every 256 pages, and when it has ended stops copying and returns the ctx's error. The pages copied stay copied and the Step's tail runs as for a Step of that many pages (`nRemaining`, `nPagecount`, the attach for updates), so a later Step on the same `Backup` goes on; the error is not sticky, like the other "not now" results (`backupBusy`). The reason is the public `Backup`: after `backupRestartLimit` restarts it copies the rest with `Step(ctx, -1)`, which holds this process's write lock on the source for the whole copy, and a cancelled ctx must still end it in bounded time.

`Restarts` has no C counterpart either: `sqlite3BackupRestart` only sets `iNext = 1` (`backup.c:701-707`). The count (restarts that threw copied pages away) changes nothing in the copy; the public `Backup` reads it to decide when to stop stepping in batches. The rollback rule that causes the restarts is SQLite's and is unchanged (`pagerUndoCallback`, `pagerRollbackWal`, `pager.c:3112-3168`). Regression tests: `TestBackupPublic_FinishesUnderRollbacks` (root package), `TestBackup_RestartsCounted`, `TestBackup_LongStepEndsWithItsCtx`.

<a id="drift-2026-11-21-19-the-kdf-salt-belongs-to-the-key-setup-not-to-page-1"></a>
### Drift: The KDF Salt Belongs To The Key Setup, Not To Page 1
- **Category:** added-logic  -  **Severity:** none (was critical: until 2026-11-19 a backup copied the source's salt, and the backup of a passphrase database could not be opened)
- **Affected functions:** `codec.go:encryptPageWithCodec`, `pager.go:*pager.adoptHeader`, `wal.go:*wal.checkpointWithMode`, `db.go:Open`, `backup.go:*Backup.finalize`.

SQLite's header has no KDF salt: bytes 72-91 are reserved for expansion and are zero. A backup copies page 1 whole (`memcpy(zOut, zIn, nCopy)`, `backup.c:269`) and patches the page count (`backup.c:271-273`); the destination's commit then rewrites the change counter and `VersionValidFor` from the destination pager's own `dbFileVers` (`pager_write_changecounter`, `pager.c:3084-3097`, called for page 1 at `pager.c:3218`).

any-store's header has one, at bytes 72-87 (`dbHeader.Salt`), which is also the marker of an encrypted file. `Open` keys the codec with it (`buildCodec`: PBKDF2 of the passphrase and the salt, unless the key is 32 raw bytes or the codec is the caller's): read from the main file's header for an existing file, drawn at random for a new one, zero in memory. The pager keeps that salt as `keySalt`, and the rule is SQLCipher's:

- **Every write of page 1 carries the keyed salt.** `encryptPageWithCodec` writes `keySalt` over bytes 72-87 of each page 1 it encrypts, whatever the image holds: `initNewDB`, and the WAL's frames at commit and at a spill. SQLCipher's codec copies its ctx's `kdf_salt` over the head of page 1 at every encrypt (`sqlcipher.c:3304-3312`, v4.14.0), in its default header mode; with `cipher_plaintext_header_size` set it keeps the page's own bytes there. The checkpoint's copy of page 1 into the main file carries it too: `checkpointWithMode` writes `keySalt` over the salt bytes of the page-1 frame it copies, the one change it makes to a frame. Those bytes are in the plaintext header, which the AEAD does not cover ([drift-2026-06-25-14](#drift-2026-06-25-14-page-1-db-header-bytes-0-99-is-excluded-from-both-checksum-and-aead-in)), so the page stays valid. SQLite's checkpoint, and SQLCipher's, copy each frame they select verbatim (`walCheckpoint` skips the others at `wal.c:2306`, then does `sqlite3OsRead` of the frame and `sqlite3OsWrite` of the same buffer, `wal.c:2311-2315`); in its default header mode, every page-1 frame SQLCipher writes already carries its ctx's salt.
- **No image of page 1 sets it.** `adoptHeader` installs a header decoded from page 1 with `keySalt` in place of the image's salt, at open, at a write begin's refresh, at a savepoint rollback that restores page 1, and in the backup's `finalize`. SQLCipher reads the salt once, from the main file (`sqlcipher_codec_ctx_init_kdf_salt`, `sqlcipher.c:1392-1409`), and in its default header mode puts the magic in place of the first 16 bytes of every page 1 it decrypts (`sqlcipher.c:3271`; with `cipher_plaintext_header_size` set it keeps the page's own bytes there), so no image of page 1 reaches its key.

So a backup's destination ends with the salt its own codec was keyed with, on the first copy of page 1 and on every later one (an update, a restart): the copy is sealed with the destination's key, and its header names the salt that key came from. Until 2026-11-19 the copy of page 1 carried the source's salt into the destination's header (`finalize` reloaded the header from the copied bytes, and the commit wrote it back); the next open derived the key from the passphrase and the source's salt and failed AEAD authentication on page 1. Every backup of a database opened with a passphrase was lost that way, and a backup written over an older, readable one destroyed it. A raw 32-byte key and a caller's codec do not use the salt, so those backups opened. A backup of an in-memory encrypted database, whose header has a zero salt, made a file whose header said it was not encrypted.

A destination such a backup left, committed and never checkpointed, names one salt in its main file and the source's in the WAL's page 1, with its pages sealed under the main file's. It opens (the key comes from the main file), and the next write of its page 1, a backup's commit among them, writes the keyed salt into the WAL. A commit that does not write page 1 leaves the WAL's page 1 as it is. Either way the WAL's page 1 reaches the main file only through a checkpoint, which writes the keyed salt into it, so the main file keeps the salt the pages are sealed with. That holds when a reader in another process limits the checkpoint to the commit whose page 1 names the other salt: the checkpoint then copies that old page 1, with the keyed salt. SQLCipher cannot produce such a file, so it has no counterpart. The invariant checks of the test suite (`page1_check.go`) compare page 1 with the header without the salt, and check that a commit that wrote page 1 wrote the keyed salt.

Why not the other way round (create the destination with the source's salt, so the two files share a key): an existing destination has a salt already and would have to be re-keyed under an open handle and a live WAL; and one key would then seal two files' worth of pages with random nonces, where the nonce collision bound is per key. A destination that `Backup` creates has a salt of its own, so with a passphrase it has a key of its own. An existing destination keeps the salt it has, which is the source's if the file is a copy of the source made outside `Backup`. A raw 32-byte key or a caller's codec is the same key in both files, as before; the built-in codecs draw random nonces, so that is a matter of how many pages one key seals and not of a repeated counter.

Regression tests: `backup_codec_test.go` (`TestBackup_DestinationKeepsItsSalt`, `TestBackup_DestinationWALPage1WithAnotherSalt`, `TestCheckpoint_WALPage1WithAnotherSalt`, `TestCheckpoint_WALPage1WithAnotherSaltAtClose`, `TestBackup_WALPage1WithAnotherSaltUnderAReader`, `TestBackup_InMemorySourceIntoEncryptedFile`), `test/backup_roundtrip_test.go`.

<a id="drift-2026-11-19-2-a-backup-destination-has-the-geometry-of-the-source-file"></a>
### Drift: A Backup Destination Must Already Have The Reserved Space And Codec Kind Of The Source File
- **Category:** changed-logic  -  **Severity:** low (was important: until 2026-11-19 a backup of a plain file opened with default options damaged every page of the copy)
- **Affected functions:** `backup.go:*DB.BackupInit`, `backup.go:*DB.BackupDestinationOptions`, root `db.go:*db.Backup`.

**SQLite.** Before the destination is first locked, `sqlite3_backup_step` tries to give it the source's page size: `setDestPgsz` calls `sqlite3BtreeSetPageSize(p->pDest, sqlite3BtreeGetPageSize(p->pSrc), 0, 0)` (`backup.c:112-116`), and only an out-of-memory result stops the step (`backup.c:361`). A destination whose page size is fixed, which `lockBtree` makes of any non-empty file, keeps its own (`SQLITE_READONLY`, `btree.c:3082-3085`, `3415`), and a WAL or in-memory destination whose page size still differs is refused (`backup.c:378-383`). `setDestPgsz` passes no reserved size. The reserved bytes reach the destination with page 1, like every other header field: `backupOnePage` copies the page whole (`backup.c:269`), and the next `lockBtree` on the destination computes `usableSize = pageSize - page1[20]` (`btree.c:3384`, `3417`). Nothing in a connection is bound to the old value, so a destination of any reserved size becomes one of the source's. Where SQLite creates the destination itself it passes the reserve explicitly: VACUUM gives the new file `sqlite3BtreeGetRequestedReserve(pMain)` (`vacuum.c:233`, `281`). SQLite 3.52.0 has no codec in the pager; the checks on reserved bytes and codec presence in `backupOnePage` are SQLCipher's (`backup.c:261-299` in SQLCipher v4.14.0: align the destination's reserve to the source's with `sqlite3PagerSetPagesize`, `SQLITE_READONLY` if that fails), as is the refusal of a backup between a keyed and a plaintext database (`backup.c:156-175` there).

**any-store.** The usable size and the codec that owns the reserved bytes belong to the open handle: `Open` installs the codec and fixes `usableSize_` before the first read, from the file's header for an existing file (zero salt and 16 reserved bytes mean checksums, a non-zero salt means encryption) and from `Options` only for a new one. Two things follow.

1. *The destination must be created like the source file, not like the source handle.* `Options.Checksum` is what the handle asked for; a plain file opened with it stays plain, and `Options()` still says `Checksum`. The root package's `Backup` built its destination from `Options()`, so the backup of such a file went into a checksummed destination: the pages were copied whole with content up to their last byte, and at commit the destination's codec wrote a 16-byte XXH3 trailer over the end of each, which is cell content. `finalize` then took `ReservedSpace = 0` from the copied page 1, and the file reopened as a plain database with damaged cells. `Backup` returned nil. The one public way to a plain file is the backup of an in-memory database (the root package gives an in-memory database no checksums); reopening that backup with defaults and backing it up again hit this. `BackupDestinationOptions` now derives the page size, the checksum mode and the schema format from the open file, and `Backup` uses it. This is what `setDestPgsz` and the copied page 1 achieve in SQLite, done at the one moment any-store can do it.
2. *An existing destination of another geometry is refused.* `BackupInit` returns `ErrBackupCodecMismatch` when the two handles differ in usable size or in the kind of codec they have installed (none, checksum, encryption; `codecKind`), before anything is written; the destination keeps its content. The kinds are those of the installed codecs, not what `IntegrityMode` reports: an in-memory database opened with a key or a codec has the encryption kind, as a keyed in-memory database has a key in SQLCipher, whose backup compares whether each side has one (`backup.c:155-176` there). SQLite would overwrite it. Doing that here means replacing the codec and the usable size of an open pager, under its WAL and under any other handle on the file, which is the same obstacle as for the schema format ([drift-2026-11-11-8](#drift-2026-11-11-8-backup-refuses-a-destination-of-another-schema-format)). Before this check an in-memory backup over a file a default `Open` had made returned an error after it had written, and left that file unopenable. Two encrypted files of one reserved size pass the check whatever their ciphers and keys: the destination seals the pages with its own key and keeps its own salt ([drift-2026-11-21-19](#drift-2026-11-21-19-the-kdf-salt-belongs-to-the-key-setup-not-to-page-1)). A destination that cannot be read with the source handle's credentials fails earlier, at its open or at the first `Step`'s write begin, also before a page is written.

The backup of an in-memory database is a plain file, or an encrypted one when the database was opened with a key or a codec. What changed is that a plain file's backup is plain too.

Regression tests: `backup_codec_test.go` (`TestBackupDestinationOptions_FollowTheFile`, `TestBackupDestinationOptions_InMemorySource`, `TestBackupInit_RefusesAnotherCodec`, `TestBackup_BetweenCiphersOfOneReserve`), `test/backup_roundtrip_test.go` (`TestBackupRoundTrip`, `TestBackupRoundTrip_OverExistingOfAnotherKind`).

<a id="drift-2026-11-16-2-pagerstress-spills-only-for-the-live-writer"></a>
### Drift: pagerStress Spills Only For The Live Writer
- **Category:** added-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.pagerStress`, `pager.go:*pager.rollbackForClose`, `wal.go:*wal.writeFrames`.

SQLite's `pagerStress` (`pager.c:4609-4680`) has no test of whether the write transaction is still the caller's. What it tests of the pager's state is the error code, behind a `NEVER` (`pager.c:4632`), and, on the rollback-journal branch, `PAGER_WRITER_CACHEMOD` to decide on a journal sync (`pager.c:4662`); the WAL branch tests nothing. It needs no more: its pages are dirty only inside a write transaction (`pager_write` asserts a writer state, `pager.c:6056-6059`), `walFrames` asserts the write lock (`wal.c:4049`), and its connection mutex keeps a close from running under a statement (`sqlite3_step`, `vdbeapi.c:923`; `sqlite3Close`, `main.c:1263`).

`DB.Close` does end a write transaction whose goroutine is still running: `rollbackForClose` rewinds the log to the transaction's base frame and releases the WAL write lock. A spill from that goroutine afterwards would land where the next writer, in this process or another, appends.

`DB.Close` first closes the writer gate and waits for the transaction's call in flight (see [the gate entry below](#drift-2026-11-21-25-close-waits-for-the-writers-call-in-flight)), so its rescue does not run under a b-tree operation of the transaction and no call of it starts afterwards: through `DB.Close` neither step below is reached. They stay as the pager's own guard, for a rescue that would run without the gate (the tests below run `rollbackForClose` from inside an operation), and cost one atomic load per spill. The guard has two steps:

1. `pagerStress` spills only in `pagerWriter` with `closeInvalidated` unset; the state is loaded before the flag, the reverse of `rollbackForClose`'s stores. This refuses a spill that starts after the rescue, but not one that passed the test just before it: that spill goes on into `writeFrames`.
2. `writeFrames` tests `wal.writeEndedByClose` under the WAL mutex (`w.mu`) before it restarts the log or writes anything, and returns `errWriteEndedByClose` (an `ErrClosed`), which `pagerStress` takes as a refused spill. `rollbackForClose` sets the flag, reads the base frame, rewinds the wal-index and `nFrame`, and releases the write lock (`endWrite`, with its `iReCksum` store) in one critical section under `w.mu`, which a spill holds from the test to its last write and its wal-index update. The spill therefore writes either entirely before the rewind, which drops its frame and its hash entries, or nothing. The flag is never cleared: the database is closing. A commit never meets it, because it holds `writerOpMu`, which the rescue takes, and tests the writer state under it. `writeFramesMem` has no test: an InMemory writer cache is not purgeable and never spills.

The lock order is `writerOpMu` → `w.mu`, as a commit's `writeFrames` takes them; under `w.mu` the rescue takes only the wal-index's own mutex and the shm's internal mutexes, and the shm unlock does not wait. A refused spill leaves the page dirty and the cache grows past its limit, as under `doNotSpill`. Regression tests: `TestCloseRescue_AbandonedWriterDoesNotSpill`, `TestCloseRescue_SpillSparesPeerWriterFrames` (step 1); `TestCloseRescue_SpillPastGuardWritesNothing`, `TestCloseRescue_SpillPastGuardSparesPeerWriterFrames` (step 2: `testStressGuardPassedHook` runs the rescue once a spill has passed step 1).

<a id="drift-2026-11-21-25-close-waits-for-the-writers-call-in-flight"></a>
### Drift: Close Waits For The Writer's Call In Flight, Then Ends Its Transaction
- **Category:** added-logic  -  **Severity:** low
- **Affected functions:** `writer_gate.go:*writerGate.enter`, `leave`, `close`, `CloseState.EndedByClose`; `db.go:*DB.Close`, `*DB.releaseWriterLocks`, `*ReadTx.CloseState`; `pager.go:*pager.close`, `destroyEndedWriterCache`, `returnLent`; every call a write transaction makes into the pager: `db.go:*WriteTx.Commit`, `Rollback`, `Put`, `Delete`, `GetNamespace`, `HasChanges`, `Savepoint`, `RollbackToSavepoint`, `ReleaseSavepoint`, `CreateNamespace`, `DeleteNamespace`, `RenameNamespace`, `*DB.CreateNamespace`, `DeleteNamespace`, `RenameNamespace`, and, when the transaction is a write transaction, `*ReadTx.AppendValue`, `Has`, `AppendSeekKey`, `CountContext`, `GetNamespace`, `NamespaceSize`, `newCursor` and the moves and reads of `btree.go:*Cursor` (`First`, `Last`, `Seek`, `SeekNear`, `SeekExact`, `Next`, `Previous`, `Skip`, `SkipBackward`, `Key`, `Value`, `AppendValue`, `AppendValueByKey`, `CountUntil`, `RangeFraction`, `Close`; `enterGate`, `leaveGate`, `abandon`, `readsPinnedLeaf` and the `gated…` functions); a backup's writes to its destination, `backup.go:*Backup.Step`, `update`.

SQLite cannot close a connection under a running statement. In serialized mode, the default build, `sqlite3_step` holds the connection mutex for the whole step (`vdbeapi.c:923`, `sqlite3_mutex_enter(db->mutex)`), `sqlite3Close` takes the same mutex (`main.c:1263`), and once it has it, it refuses with `SQLITE_BUSY` while a statement is unfinalized or a backup that reads from the connection unfinished (`connectionIsBusy`, `main.c:1283`; `backup.c:204`); `sqlite3_close_v2` marks the connection a zombie and closes it when the last of them goes. A b-tree operation therefore never sees its pager closed. In multi-thread mode an ordinary build gives the connection no mutex (`main.c:3388-3393`), and a close during another thread's step is the application's error. `sqlite3_backup_step` and `backupUpdate` hold the destination's connection mutex the same way (`backup.c:326`, `676`); the application must not pass the destination to any other API, a close included, until `sqlite3_backup_finish` (`sqlite.h.in:9712-9715`).

`DB.Close` here does not refuse: it rolls back the write transaction it finds and releases its locks (the force-rollback, `rollbackForClose`), by design, so that a transaction the application abandoned cannot keep the database open. But the transaction's goroutine can be inside a b-tree operation at that moment, and `Put`, the balance and the cursor moves hold no lock; `writerOpMu` is taken only by commit and rollback. A teardown that runs under the operation takes its pages from under it: `pcache.destroy` sets every page's buffer to nil and returns the buffers to the process-wide pool, the wal-index is unmapped and the files are closed.

The port's counterpart of the connection mutex is the writer gate (`writerGate`, on the pager): a count of the write transaction's calls in flight, with a closed bit. Every call listed above enters the gate and leaves it in a `defer` (a b-tree operation can panic on a damaged page). A cursor's methods keep that `defer` out of their own bodies: a write transaction's call goes through a small gated function (`gatedNext`, `gatedKey`, `gatedMove` and the like) that counts it in and makes the call again, so a read transaction's cursor runs the method as it is, after one test of the writable flag. `DB.Close`, when it finds a registered write transaction, sets the closed bit and waits for the count to reach zero, and only then takes `writerOpMu`, rolls back, releases the transaction's locks and closes the pager. Once the bit is set, the outermost call of the transaction is refused at entry with `ErrClosed` and touches nothing; a call made from inside a call that was let in (a write saves the transaction's cursors, and each reads its key) is let in too, so the call in flight completes undisturbed. A refused call is not counted: `enter` raises the count by compare-and-swap and refuses at a closed, empty gate without touching it, so once the count is down to zero every call is refused, also two at once from two goroutines. A refused call that counted itself in and out again would, for that moment, be a call in flight, and another goroutine's call would pass for one nested in it. A cursor counts the calls it makes on itself as one (`Cursor.gated`): `SeekExact` is a `SeekNear`, which is a `Seek`.

`Commit` and `Rollback` are gated calls. Setting the closed bit is thus the one decision that ends the transaction: between it and the forced rollback the pager is still in the writer state, and a `Commit` let through there would publish the writes of a transaction whose later calls, a statement's own savepoint rollback among them, were refused. A `Commit` refused at the gate returns `ErrClosed` and a `Rollback` nil, neither touches the pager, and Close rolls back and releases the locks. One in flight is waited for to its end, the unwind of a panic included (`unwindWriter` keeps a commit that panicked after it was published; the rescue, run in its place, would rewind it). A `Commit` in flight when Close closed the gate returns its own result: nil once the transaction is committed, or the error that failed it, a WAL write's for one.

The count is of calls, not of transactions, so a `Commit` or a `Rollback` leaves the gate in `releaseWriterLocks`, after the rest of what it releases and before the write lock: the next writer cannot begin with this transaction's count still in the gate, where its calls, its `Commit` included, would pass for nested ones after the gate closed. That is SQLite's order: the COMMIT's step releases the write lock under the connection mutex (`sqlite3VdbeHalt`, `vdbe.c:4042`, down to `sqlite3WalEndWriteTransaction`, `pager.c:2142`), and the next BEGIN is another step, which takes the mutex first. A Close waiting at the gate goes on as the count leaves, and waits for the write lock in its turn.

What differs from SQLite, and what follows from it:

- The exclusion is per b-tree call, not per statement. The layer above runs one of its operations as many calls; Close waits for one of them, and the operation meets `ErrClosed` at its next call. A per-statement mutex in SQLite's place would make `DB.Close` called inside a `query.Modifier`, which runs between two calls of its operation, wait for itself.
- The wait has no bound of its own. A call is as long as its work: a point lookup is a descent, `CountContext`, `NamespaceSize` and the drop of a namespace walk a whole tree, a `Backup.Step` copies as many pages as it was asked to, and each waits for its I/O. The WAL write lock stays held until the pager ends the write transaction, and a writer in another process retries behind it until it gets the lock or its busy handler gives up. A `Commit` or a `Rollback` in flight can end the write transaction before the call is over (`pager.commit` and `pager.rollback` release the WAL write lock as they end, `wal.endWrite`), so Close can still be waiting after the lock is free. A descent that never ends, through a child pointer that leads back to an ancestor in a damaged file, holds Close for good: the depth of every descent has to be bounded for that, as `moveToChild` bounds it (`btree.c:5448`), which is the cursor's and the lookups' own business and not the gate's.
- `DB.Close` must not run on the transaction's own goroutine from inside one of its calls: it waits for that call. That is any code a gated call runs synchronously: the integrity hook (`Options.OnIntegrityError`), the methods of a caller's VFS that a page read or a log write goes through, the `Err` of the caller's context, which `CountContext` and the cursor's moves poll, and the test hooks.
- Only a write transaction pays: an atomic load and a compare-and-swap in, an atomic add out and a deferred call per call, on a word nothing else writes, and one call more for a cursor's. A cursor's read or step served entirely from the leaf it pins pays one atomic load instead and runs outside the gate (`Cursor.readsPinnedLeaf`); one that needs more, an overflow chain or a step off the leaf, takes the gate. `sqlite3BtreeNext` and `sqlite3BtreePrevious` too step `pCur->ix` within a leaf without calling `btreeNext` or `btreePrevious` (`btree.c:6376-6383`, `6467-6474`). Close does not wait for such a call; it completes on the leaf, which the pager keeps back, as the next point says. A read transaction's calls test `tx.writable` (or `bt.writable`) and go on; Close waits for readers on `db.mu`.
- A key or a value a cursor returned is a slice of the leaf the cursor pins, valid until the cursor's next move, and the transaction's goroutine can hold one when Close comes between two of its calls. `pager.close` therefore takes the leaves that the transaction's cursors pin out of the writer cache before it destroys it (`destroyEndedWriterCache`), and writes neither those pages nor the cursors: a call outside the gate may be using them. A lent buffer of the default allocator stays with its page and goes when the cursor does. One of the slab is noted (`pager.lent`) until the cursor that pins it is moved or closed, the refused call at which the slice stops being valid (`Cursor.abandon`), and returns its slot then; a cursor that is never used again keeps its slot out of the slab. The cursors are taken off the pager's list, and one made after Close stays off it. `pager.close` also releases the transaction's savepoints, as `sqlite3PagerClose` does (`pager_unlock`, `releaseAllSavepoints`, `pager.c:1850`; asserted gone at `4228`), and drops the last commit's page list (`dirtyBuf`), through which a lent leaf would stay reachable from the closed handle after its cursor.
- The layer above holds what an iterator read from the transaction (a parsed document, sorted rows, aggregation groups) outside any call of it, and must not hand it out once Close has ended the transaction. It asks a `CloseState` (`ReadTx.CloseState`), taken as the iterator is built: it holds the database's writer gate, so it stays valid when the transaction ends on another goroutine and goes back to the pool, where the next `BeginWrite` rewrites it.
- The two-step spill guard of [drift-2026-11-16-2](#drift-2026-11-16-2-pagerstress-spills-only-for-the-live-writer) is not reached through `DB.Close`: the guard is for a rescue that runs under an operation.

Regression tests, in `close_writer_gate_test.go`: `TestClose_WaitsForWriterCallInFlight` (Close started from inside a page rebuild and from inside a spill returns only after that `Put`, which completes; every later call is `ErrClosed`), `TestClose_CommitRefusedOnceGateClosed` and `TestClose_CommitRightAfterCallInFlight` (Close held between the gate and the forced rollback), `TestClose_NextWriterCommitRefusedAfterHandoff` (the next writer begins as the write lock passes on, and Close comes after its first call), `TestClose_WaitsForCommitThatPanicsAfterPublish`, `TestClose_CommitAfterFailedSpillAndClose`, `TestClose_WaitsForBackupWritingDestination`, `TestClose_KeepsBackTheLeavesCursorsLent` and `TestClose_CursorAbandonedBeforeCacheDisposal` (the lent leaves, slab and pool), `TestClose_ReleasesSavepointsAndCommitPageList`, `TestClose_DuringKeyAndValueOfPinnedLeaf` and `TestClose_DuringInLeafStepsAndAppendValue` (the calls outside the gate, for `-race`), `TestWriterGate_PinnedLeafCallsStayOutside` (which calls take the gate), `TestClose_DuringRunningWriter` (the race, for `-race`), `TestClose_ByWriterGoroutineBetweenCalls`, `TestClose_AfterWriterCallPanicked`, `TestWriterGate`, `TestWriterGate_DrainedRefusesRacingCalls` (two goroutines on a closed, empty gate); through the public API `TestDb_Close` and `TestIterator_DocOfHeldDocumentWhileTxEnds` (for `-race`) in the root package and `TestDb_CloseDuringWrite`, `TestDb_CloseFromModifier` in `test/`.

<a id="drift-2026-11-21-26-close-waits-for-every-read-transaction-to-end"></a>
### Drift: Close Waits For Every Read Transaction To End
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*DB.Close` (the `db.mu.Lock` once the write transaction is dealt with), `*DB.beginRead` (a read transaction holds `db.mu.RLock` from its begin), `*ReadTx.Rollback` (which releases it).

SQLite's close waits for no read transaction and no statement. `sqlite3Close` enters the connection mutex (`sqlite3_mutex_enter(db->mutex);`, `main.c:1263`). In serialized mode, the default build (`bFullMutex` is `SQLITE_THREADSAFE==1`, `global.c:241`; `main.c:3346-3354`, `3388-3393`), `sqlite3_step` holds that mutex for the step it runs (`vdbeapi.c:923`), so a close waits for a step running on another thread, not for its statement to be reset or finalized. On the way a close takes other mutexes too, and can wait for them: the shared b-tree mutexes of a shared cache (`disconnectAllVtab`, `main.c:1269`, takes them through `sqlite3BtreeEnterAll`, `main.c:1213`, `btmutex.c:200-201`), and the Unix VFS's global mutex as it closes a file (`unixClose`, `os_unix.c:2350`, `unixEnterMutex`, `901`). In multi-thread mode, or under `SQLITE_OPEN_NOMUTEX`, an ordinary build gives the connection no mutex (`main.c:3388-3393`), and a close during another thread's step is the application's error.

With the mutex held, `connectionIsBusy` (`main.c:1240-1250`) is true while a statement is unfinalized (`if( db->pVdbe ) return 1;`, `main.c:1243`) or a backup that reads from the connection is unfinished (`p->pSrc->nBackup++`, `backup.c:204`). There the two closes part (`int sqlite3_close(sqlite3 *db){ return sqlite3Close(db,0); }` and `int sqlite3_close_v2(sqlite3 *db){ return sqlite3Close(db,1); }`, `main.c:1351-1352`):

- `sqlite3_close` refuses and leaves the connection open: `if( !forceZombie && connectionIsBusy(db) ){`, then `sqlite3ErrorWithMsg(db, SQLITE_BUSY, "unable to close due to unfinalized "` `"statements or unfinished backups");` and `return SQLITE_BUSY;` (`main.c:1283-1288`).
- `sqlite3_close_v2` returns `SQLITE_OK` and leaves a zombie: `db->eOpenState = SQLITE_STATE_ZOMBIE;` and `sqlite3LeaveMutexAndCloseZombie(db);` (`main.c:1307-1308`), which only leaves the mutex while the connection is busy (`if( db->eOpenState!=SQLITE_STATE_ZOMBIE || connectionIsBusy(db) ){`, `main.c:1371-1374`). The last `sqlite3_finalize` frees it (`vdbeapi.c:115`), or the last backup's finish (`backup.c:608`, `617`).

A transaction left open with every statement finalized, a `BEGIN` that has read among them, is no reason to refuse: the close rolls it back (`sqlite3RollbackAll(db, SQLITE_OK);`, `main.c:1386`), as `DB.Close` rolls back a write transaction.

`DB.Close` here waits for every read transaction of the handle to end. Once the write transaction is dealt with, it takes `db.mu.Lock` against the `db.mu.RLock` that each read transaction holds from `beginRead` to `ReadTx.Rollback`, with no timeout and no error, and only then destroys the reader caches and closes the pager. It neither refuses, as `sqlite3_close` does, nor returns and leaves the close to the last reader, as `sqlite3_close_v2` does. What follows from it:

- A `ReadTx` that is never rolled back blocks `Close` forever, and `Close` called on a goroutine that holds one deadlocks on itself. In the root package an `Iterator` opened with a context that carries no transaction holds a read transaction of its own until its `Close`, so a `defer it.Close()` that runs after `db.Close()` deadlocks.
- While `Close` waits, the handle is closing (`SetClosing`): a new transaction fails with `ErrClosed`, and a reader waiting for a slot is woken with it. The open read transactions go on reading their snapshots until they end.
- The wait keeps the teardown from running under a reader: its cache, and the wal-index mapping and the files its page reads go through, are what `Close` frees after it.

The wait is the documented contract of `DB.Close` (its godoc here and in the root package, README "Transactions"). Regression tests: `TestDb_Close` in the root package (`waits for an open read transaction`, `waits for an open iterator`).

<a id="drift-2026-11-21-13-the-error-state-of-a-failed-spill"></a>
### Drift: The Error State Of A Failed Spill Is Cleaned Up By The Rollback, And Only The Writer Sees It
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pcache.go:*pcache.create`, `pager.go:*pager.pagerStress`, `pager.go:*pager.getPageWriter`, `pager.go:*pager.writeStateErr`, `pager.go:*pager.halt`, `pager.go:*pager.commit`, `pager.go:*pager.rollbackLocked`, `pager.go:*pager.savepoint`, `pager.go:*pager.beginReadHdrBusy`, `btree.go:*Cursor.fault`, `btree.go:*Cursor.halt`, `btree.go:*Cursor.fail`, `btree.go:*Cursor.restorePosition`, `db.go:*ReadTx.halt`, `db.go:*WriteTx.Savepoint`, `db.go:*WriteTx.ReleaseSavepoint`, `db.go:*DB.Close`.

SQLite: `sqlite3PcacheFetchStress` returns an error of the stress callback other than `SQLITE_BUSY` without creating the page (`pcache.c:483-485`; a busy one is dropped and the hard fetch retried, line 488), and `getPageNormal` gives the fetch up (`goto pager_acquire_err`, `pager.c:5555-5556`), so the b-tree operation that asked for the page fails with the error. `pagerStress` ends in `pager_error` (`pager.c:4680`), which for `SQLITE_FULL` and `SQLITE_IOERR` stores `errCode`, sets `PAGER_ERROR` and installs `getPageError` (`pager.c:1947-1960`): nothing is discarded while pages are referenced. From then on every page fetch returns `errCode` (`getPageError`, `pager.c:5710-5722`), as do the release and the rollback of a savepoint (`sqlite3PagerSavepoint`, `pager.c:7008`; opening one, `sqlite3PagerOpenSavepoint`, does not test it, `pager.c:6965-6974`), and `sqlite3PagerRollback` returns it without rolling anything back (`pager.c:6777`). The cleanup is `pager_unlock`'s, reached through `pagerUnlockIfUnused` when the last page reference is dropped (`pager.c:5471-5476`): it ends the WAL write transaction, discards the whole cache (`pager_reset`), clears `errCode`, puts the normal getter back and returns to `PAGER_OPEN` (`pager.c:1853-1864`, `1904-1920`). The VDBE halts such a statement: `sqlite3VdbeHalt` closes the statement's cursors (`closeAllCursors`, `vdbeaux.c:3335`) and rolls the transaction back with the trip code `SQLITE_ABORT_ROLLBACK` (`vdbeaux.c:3358-3384` for an I/O error, `3459-3472` when the statement rollback a full disk asks for fails). `sqlite3RollbackAll` passes `!schemaChange` as `writeOnly` (`main.c:1505`). `sqlite3BtreeTripAllCursors` then faults the write cursors and saves the read cursors of the connection's other statements, and faults every cursor, the read cursors included, when the transaction changed the schema (`writeOnly` 0) or when a save fails (`btree.c:4475-4488`); every cursor's pages are released. `sqlite3BtreeNext` itself tests no error state (`btree.c:6369-6383`): a cursor meets `errCode` only through a fetch. The error state can outlive the statement: while another statement of the connection still reads, `btreeEndTransaction` downgrades the transaction to `TRANS_READ` instead of ending it (`btree.c:4344-4349`), page 1 stays referenced, and `PAGER_ERROR` lasts until the last page reference drops; a saved read cursor's restore meets `getPageError` meanwhile.

Here: `pcache.create` returns the spill's error and creates no page; `getPageWriter`, `getPageNoContent`, the `freePage` fallback and `rollbackToSavepoint` return it. A spill never returns a busy error: a busy lock in the log's restart means append (`restartLogLocked`, as `walRestartLog`, `wal.c:3889-3890`). `pagerStress` stores the error in `pager.errCode` and sets `pagerError`, and nothing else: the cache is left as it is under the running operation, the victim still dirty. Every page fetch of the writer returns `errCode` (`getPageWriter` tests it first, `getPageError`'s rule), a cached page's as much as one to be read, and so does every operation that tests for `pagerWriter` (`getWritablePage`, the allocation and free paths, the overflow writers, the savepoint operations, opening one included) through `writeStateErr`. The transaction cannot commit. Every call of the write transaction that returns `errCode` passes it through `pager.halt` on its way out (`ReadTx.halt`, `Cursor.halt`, and `Cursor.fail`, the error exit of a move), as every SQLite statement that ends in an error halts: a call whose fetch failed, `Savepoint` and `ReleaseSavepoint`, which test the state, and a call on a cursor already faulted, which reports its fault. In the error state the halt faults every writer cursor with `errCode` and releases its pages, as `sqlite3VdbeHalt` ends the statement's; a cursor opened since the failure is ended by the next such call. `Commit` returns `errCode` without the halt: it rolls the transaction back, and the rollback ends the cursors. The call that met the failed spill has stopped using its pages by then. Without the halt, a cursor positioned on another tree would go on stepping through the leaf it pins, which needs no fetch. A transaction `DB.Close` ended is left alone. What differs from SQLite:

1. **Who cleans up, and when; the cursors.** The error state outlives the call that met it, until the caller ends the transaction; SQLite's lasts until the last page reference drops, at the statement's halt or, while another statement of the connection reads, after it. At the halt every writer cursor is faulted with `errCode` and none is saved, where SQLite faults the write cursors with `SQLITE_ABORT_ROLLBACK` and saves the read cursors (it faults those too when the transaction changed the schema). None is saved here: the writer's cursors carry no read or write flag, and a saved cursor could only seek its key again through a fetch, which fails until the rollback, and the rollback ends every cursor for good (`faultSeq` 0, never revived). A save would also read an overflow key through such a fetch. There is no "last reference dropped" event to hang `pager_unlock` on, so the cleanup is `rollbackLocked`'s: it ends the transaction's cursors with `errCode` once more (`tripCursors`, as the failed commit's `pagerError` does), discards every dirty page and clears the cache, rewinds the log and the wal-index to the transaction's base, restores the header, clears `errCode` and releases the WAL write lock. `WriteTx.Rollback` reaches it and returns nil, where `sqlite3PagerRollback` returns `errCode`. `pager.commit` in the error state returns `errCode` (as `sqlite3PagerCommitPhaseOne`, `pager.c:6480`) and runs the same rollback, because a failed commit ends the transaction here. `DB.Close` ends a transaction in the error state as it ends one in `pagerWriter` (`rollbackForClose`). Until one of the three runs, the write lock stays held, as for any open write transaction.
2. **Every spill error.** Any error of the spill's `writeFrames` puts the transaction in the error state, not only the I/O and disk-full classes `pager_error` tests for: a wal-index error there leaves the log as unknown as a failed write does. `errWriteEndedByClose` stays a refused spill ([the live writer](#drift-2026-11-16-2-pagerstress-spills-only-for-the-live-writer)).
3. **Opening a savepoint fails.** `pager.savepoint` returns `errCode`, where `sqlite3PagerOpenSavepoint` does not test it: a savepoint opened in a transaction that can only be rolled back has nothing to keep.
4. **Only the writer sees the state.** `pagerError` is the state of the open write transaction. A read begin (`beginReadHdrBusy`) does not test for it. In SQLite a connection with a private cache has its own `Pager`, and no other connection meets its error state; connections that share a cache share one `Pager` (`btree.c:2638-2639`), which any-store has no counterpart of. A read begin here takes its snapshot from the wal-index header, bounded by the committed frame: a spill indexes its frame past that bound (`setBatch`) but never publishes it as a commit, so no read sees it. A first spill after a full backfill may publish a restart header (`walRestartHdr`, `wal.c:2151-2154`, as here), which changes no committed content.

A failed commit keeps [the eager cleanup](#old-drift-pagererror-eager-cleanup).

Regression tests: `TestCreate_FailedSpillFailsTheCreate`, `TestBeginRead_PagerErrorState`, `TestCloseRescue_ErrorState`; with `-tags vfs`, `TestSpillFault_Sweep`, `TestSpillFault_DefaultCache`, `TestSpillFault_SavepointRollbackAfterSpill`, `TestSpillFault_RootSplitStoppedHalfWay`, `TestSpillFault_RollbackTripsCursors`, `TestSpillFault_FailedCallEndsCursors`, `TestSpillFault_CreateReadBackEndsCursors`, `TestSpillFault_LaterCallEndsCursors`, `TestSpillFault_RenameReadBack`, `TestSpillFault_CloseInErrorState` (`internal/btree/spill_fault_vfs_test.go`) and `TestSpillFault_WriteTx` (`test/`).

<a id="drift-102-reader-slot-tie-break-selects-lowest-not-highest"></a>
### Drift: Reader Slot Tie Break Selects Lowest Not Highest
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.tryBeginReadMultiProcessHdr` (`internal/btree/wal.go:2852-2862`; the slot-1..4 selection loop, with the decisive `mark > bestMark` at `internal/btree/wal.go:2858`). The cited `*wal.tryBeginReadMultiProcess` is now a thin wrapper at `internal/btree/wal.go:2784`.

SQLite's slot-selection loop initializes `mxReadMark=0, mxI=0` and uses a non-strict comparison `if(
mxReadMark<=thisMark && thisMark<=mxFrame )` (`wal.c:3164`), so when two slots hold the SAME read-mark value the
later (higher-index) slot overwrites `mxI` and SQLite ends up locking the highest-numbered tied slot. Go's port
initializes `bestSlot=-1, bestMark=0` and selects with a strict comparison, so on equal marks it keeps the
first (lowest-index) slot it found (`wal.go:2852-2862`). The consequence is a behavioral tie-break divergence:
Go pins the lowest-numbered slot among equals where SQLite pins the highest. This does not affect read
correctness but changes which physical slot is held, altering slot-occupancy patterns that other processes
observe.

<a id="drift-104-padtosectorboundary-sector-padding-of-commit-frames-not-port"></a>
### Drift: padToSectorBoundary Sector Padding Of Commit Frames Not Ported
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `wal.go:*wal.open` (`wal.go:1431`),
  `wal.go:*wal.writeFrames` (`internal/btree/wal.go:2249`, single fdatasync on commit; no padding loop).

SQLite's `sqlite3WalOpen` sets `pRet->padToSectorBoundary = 1` (`wal.c:1715`, cleared at `wal.c:1733` only on
POWERSAFE_OVERWRITE devices), and `walFrames` (`wal.c:4172-4191`) uses it on a synchronous commit to compute a
sync point at the next disk sector boundary and repeatedly re-write the last frame (with its commit mark) up to
that boundary before fsyncing only that region — so the durably-synced region never ends mid-sector / mid-frame.
Go's `wal.open` has no `padToSectorBoundary` concept (`wal.go:1431`) and `writeFrames` performs a single
`fdatasync(w.file)` on commit with no padding loop (`wal.go:2249`); a package-wide search finds no sector / iSyncPoint / nExtra /
powersafe logic anywhere. The consequence is that on devices where a torn write at the synced offset can
corrupt an adjacent partially-written sector, the Go WAL lacks SQLite's defensive sector alignment, so a
power-loss mid-write could leave the committed region straddling a sector that hardware partially overwrote.

<a id="drift-2026-11-11-10-writeframes-writes-appended-frames-in-chunks-not-two-writes-per-frame"></a>
### Drift: writeFrames Writes Appended Frames In Chunks, Not Two Writes Per Frame
- **Category:** performance  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.writeFrames` (`internal/btree/wal.go`, the `frameBatch` staging in the append branch; `walFrameBatchMax`).

C `walFrames` writes every frame through `walWriteOneFrame` (`wal.c:4159`, `wal.c:4194`), which issues two `sqlite3OsWrite` calls per frame: the 24-byte header, then the page (`wal.c:3954-3970`, through `walWriteToLog`, `wal.c:3929-3949`). Go's `writeFrames` stages each appended frame's header and payload contiguously in `w.frameBatch` and writes the staged frames with one `WriteAt` whenever the next frame would not fit in `walFrameBatchMax` (256 KiB) and once after the loop, so a commit of up to 63 frames on 4 KiB pages is one write instead of 2 × frames. Frames reused in place (`wal.c:4138-4153`) are not contiguous with the appends and keep their direct write.

The bytes written and their offsets are the same, so the file and crash recovery are unchanged: appended frames lie past the last commit frame until the commit frame itself lands, recovery stops at the first frame whose checksum fails, and a torn chunk is the same state as a crash partway through the per-frame writes. A failed chunk returns before `nFrame` and the checksums advance, as a failed per-frame write did. Each Go `WriteAt` is a syscall plus runtime overhead, so writing per frame costs relatively more than it does in C; a small commit (two leaves, or a leaf and page 1) goes from four writes to one. `walWriteToLog` also splits a write at `iSyncPoint` for sector padding; Go has no sync point (drift-104), and porting padding would have to split a chunk there too.

The writer restart keeps SQLite's order around the chunks. `restartLogLocked` (~ `walRestartLog`, `wal.c:3866-3906`, called at `wal.c:4070`) runs before the first frame is staged, and a restarted log's WAL header is written and synced by `flushHeader` before the first chunk goes out (`wal.c:4074-4116`); the header is never part of a chunk, so a chunk carries frames only, numbered from 1 of the new log. A chunk that fails after a restart leaves `nFrame` at 0 and the checksums at the new log's seed, and the unwind goes to `walBase()`, the start of the new log, with the size the transaction began with. Tests: `TestWriteFrames_RestartThenChunkedCommit`, `TestWriteFrames_ChunkFaultAfterRestart`, `TestWriteFrames_Format6NoPage1Chunks`, `TestWriteFrames_ReusedFramesWithChunks` (frames written over in place between chunks, then `walRewriteChecksums` before the sync, `wal.c:4166-4169`) (`wal_frame_batch_restart_vfs_test.go`).

<a id="drift-105-journal-size-limit-wal-truncation-feature-unimplemented"></a>
### Drift: journal_size_limit WAL Truncation Feature Unimplemented
- **Category:** new-feature  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.endWrite` (`wal.go:3056`),
  `wal.go:*wal.truncateFile` (`internal/btree/wal.go:3876`),
  `wal.go:*wal.writeFrames`
  (`internal/btree/wal.go:2034-2036`, commit block has no size-limit truncation).

SQLite implements `PRAGMA journal_size_limit` via the `truncateOnCommit`/`mxWalSize` machinery: after the
first transaction completing a WAL, `walFrames` does `if(isCommit && pWal->truncateOnCommit &&
pWal->mxWalSize>=0)` to shrink the WAL toward the configured limit and then clears the flag (`wal.c:4193-4204`),
and `sqlite3WalEndWriteTransaction` resets `truncateOnCommit=0` alongside `writeLock`/`iReCksum`
(`wal.c:3729-3736`). The Go port omits the entire feature: `endWrite` resets only `w.iReCksum` and unlocks
`lockWrite` with no `truncateOnCommit` field or reset (`wal.go:3056`); `writeFrames`' commit block does
`rewriteChecksums` + `fdatasync` + header publish with no size-limit truncation (`wal.go:2034-2036`); and
`truncateFile` ports only the close-time truncate, not the commit-time `walLimitSize` call site
(`wal.go:3876`). The consequence is that any-store WAL files grow without the periodic commit-time shrink-back
SQLite provides under `journal_size_limit`; the bound is enforced only by full checkpoint/reset rather than
incremental truncation.

<a id="drift-106-wal-read-only-fallback-not-ported"></a>
### Drift: WAL Read Only Fallback Not Ported
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.open` (`wal.go:1431`).

SQLite's `sqlite3WalOpen` opens the WAL with `SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE` and, if the VFS
downgrades the open to `SQLITE_OPEN_READONLY`, records `pRet->readOnly = WAL_RDONLY` (`wal.c:1719-1723`) so the
connection can still attach to a database sitting on read-only media. Go's `wal.open` always opens with
`os.O_RDWR|os.O_CREATE` (`wal.go:1447`) and returns the OS error directly on failure with no read-only retry;
the `wal` struct has no `readOnly`/RDONLY field anywhere. The consequence is that any-store cannot open a
WAL-mode database on read-only storage at all, whereas SQLite degrades gracefully to a read-only attachment —
a platform-support gap rather than a correctness bug on writable media.

<a id="drift-107-syncheader-device-characteristic-tuning-not-ported"></a>
### Drift: syncHeader Device Characteristic Tuning Not Ported
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.open` (`wal.go:1430-1431`).

SQLite's `sqlite3WalOpen` sets `syncHeader = 1` and then clears it when the device reports
`SQLITE_IOCAP_SEQUENTIAL` (`wal.c:1714`, `wal.c:1731`); `syncHeader` gates the explicit header fsync before the
first frame (`wal.c:4098`), so on sequential-write devices that header sync is safely skipped. Go's `wal.open`
has no `syncHeader` field and unconditionally calls `fdatasync(w.file)` for the header in both `flushHeader`
(`wal.go:1791`) and `writeHeader` (`wal.go:1817`). The consequence is a strictly-more-conservative behavior:
Go always pays the header fsync that SQLite would elide on `SQLITE_IOCAP_SEQUENTIAL` devices — correct but
potentially a needless durability cost where the device guarantees ordered sequential writes.

<a id="drift-108-busy-handler-retry-count-resets-per-call"></a>
### Drift: Busy Handler Retry Count Resets Per Call
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:walBusyLock` (`wal.go:211`).

C's `walBusyLock` invokes the busy handler through `sqlite3InvokeBusyHandler(&db->busyHandler)`, passing the
CONNECTION-WIDE counter `db->busyHandler.nBusy` that accumulates across retries (`nBusy++` each attempt) and is
reset to 0 only when `sqlite3_busy_handler()`/`sqlite3_busy_timeout()` is re-set — so the retry count persists
across successive lock attempts on the same connection. Go's `walBusyLock` declares a function-local
`var count int` (`wal.go:212`) that starts at 0 on every call and only increments within that single call
(`wal.go:224`), with an intentionally stateless handler (DefaultBusyTimeout). The consequence is that the
busy-handler back-off no longer accumulates per-connection: each `walBusyLock` call restarts the retry/back-off
sequence from zero rather than continuing the connection-wide progression SQLite maintains.

<a id="drift-109-walbusylock-locks-single-slot-not-n-consecutive"></a>
### Drift: walBusyLock Locks Single Slot Not n Consecutive
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `wal.go:walBusyLock` (`wal.go:211`).

C's `walBusyLock(pWal, xBusy, pBusyArg, lockIdx, n)` forwards `n` to `walLockExclusive` ->
`sqlite3OsShmLock(pDbFd, lockIdx, n, EXCLUSIVE)` (`wal.c:1104`), acquiring `n` consecutive lock slots in ONE
atomic OS shm-lock operation; the only `n>1` call site is `wal.c:2347`
`walBusyLock(pWal, xBusy, pBusyArg, WAL_READ_LOCK(1), WAL_NREADER-1)`, which locks reader slots 1..4 as a single
range. Go's `walBusyLock` signature is instead `(wi, xBusy, slot, lockType)` and locks a single slot
(`wal.go:211`). The consequence is that any-store cannot express SQLite's atomic multi-slot range lock; where
SQLite grabs reader slots 1..4 in one operation, Go's `tryResetWALWithBusy` must lock them individually in a per-slot loop (`wal.go:3805`), which changes the lock-granularity
contract but is acceptable because Go does not exercise the multi-slot reader range the way C does.

<a id="drift-113-integrity-freeblock-walk-and-coverage-diagnostics-diverge"></a>
### Drift: Integrity Freeblock Walk And Coverage Diagnostics Diverge
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `integrity.go:*integrityChecker.checkPageCoverage`
  (`internal/btree/integrity.go:186-241`, `:197-220`, `:200-213`).

Go's `checkPageCoverage` freeblock handling diverges from SQLite in three ways. First, it reports a diagnostic and
`break`s on the FIRST malformed freeblock (out-of-range, size<4, extends-off-page, or unordered chain,
`integrity.go:197-220`) yet then falls through unconditionally into the overlap/fragmentation coverage analysis
(`L222-240`) using a heap that is missing every freeblock at or beyond the break point — producing a spurious
extra diagnostic SQLite never emits. Second, its ordering check is weaker: it flags an unordered chain only when
`nextFb != 0 && nextFb <= fb` (`integrity.go:215`), requiring the next link to merely start after the current
freeblock's START, whereas SQLite's invariant (`btree.c:11051`: `j==0 || j>i+size`) requires it to start after the
current freeblock's END. Third, it adds runtime range/size validation (`fb < contentOffset` lower-bound,
`fbSize < 4`, `fb+fbSize > usableSize`, `integrity.go:200-213`) where SQLite relies on prior
`btreeComputeFreeSpace` asserts and only upper-bound-checks. The consequence is purely diagnostic divergence:
different, sometimes spurious or differently-ordered integrity messages, with no impact on data correctness.

<a id="drift-114-integrity-report-inverts-zero-error-budget-and-omits-progres"></a>
### Drift: Integrity report Inverts Zero Error Budget And Omits Progress Hook
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `integrity.go:*integrityChecker.report`
  (`internal/btree/integrity.go:57-62`, `tooManyErrors` `:52-54`).

C's `checkAppendMsg` gates on `if(!pCheck->mxErr) return;` (`btree.c:10608`) and the integrity walk loops are
guarded `&& pCheck->mxErr`, so an initial `mxErr==0` means STOP IMMEDIATELY / report nothing — and helpers like
`checkOom` deliberately set `mxErr=0` (`btree.c:10568`) to halt the entire check. Go's equivalent `report()` -> `tooManyErrors()`
evaluates `ic.maxErrors > 0 && len(ic.errors) >= ic.maxErrors` (`integrity.go:52-54`), which inverts the meaning of a
zero budget: with `maxErrors==0` Go treats it as "unlimited" and keeps reporting rather than halting. Separately,
C's `checkAppendMsg` unconditionally calls `checkProgress(pCheck)` as its first action on every message-append
(`btree.c:10607`, defined at `btree.c:10576-10596`), reading the interrupt flag and optional `xProgress` callback and aborting the check on
interrupt/cancel; Go's `report()` omits this per-message progress/interrupt hook entirely (`integrity.go:57-62`). The
consequence is two behavioral divergences: a zero error budget no longer aborts as SQLite intends, and a long
integrity check cannot be interrupted or report progress mid-walk.

<a id="drift-115-cursor-skip-and-appendvaluebykey-new-apis-beyond-sqlite-surf"></a>
### Drift: Cursor Skip And AppendValueByKey New APIs Beyond SQLite Surface
- **Category:** new-feature  -  **Severity:** low
- **Affected functions:** `btree.go:Cursor.Skip` (`internal/btree/btree.go:4154`),
  `btree.go:Cursor.SkipBackward` (`btree.go:4181`),
  `btree.go:Cursor.AppendValueByKey` (`internal/btree/btree.go:3738`).

any-store adds cursor APIs with no SQLite `BtCursor` counterpart. `Skip(n)`/`SkipBackward(n)` advance or rewind the
cursor by `n` positions using an O(1) in-page `cellIdx` bump and only cross leaf boundaries via `Next()`/
`Previous()`, giving a batched O(N/entries_per_page) skip used in production by the query planner's offset/limit
path (`internal/qplanner/fullscan_iter.go:59-61`). `AppendValueByKey` (`btree.go:3738`) composes `SeekNear` + an
exact-key check + value extraction, appending the value bytes directly into a caller buffer to avoid extra
parse/copy work — for non-overflow cells it appends `cell.value` (a slice into the pinned page buffer) directly and
falls back to `Cursor.Value()` reconstruction only for overflow payloads. The consequence is added public surface
beyond the documented cursor API (NOTES.md section 15 lists only `SeekNear` as an any-store extension), so the
cursor contract a maintainer infers from NOTES is incomplete.

<a id="drift-116-open-forces-inprocess-on-non-mmap-shm-platforms-and-build-ta"></a>
### Drift: Open Forces InProcess On Non Mmap SHM Platforms And Build Tag Scope Drift
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `db.go` (`internal/btree/db.go:413-415` force InProcess when `!hasMmapShm`),
  `dbfile_lock_other.go` (`internal/btree/dbfile_lock_other.go:1,13` `//go:build !unix && !windows` no-op stubs),
  `dbfile_lock_windows.go` (`internal/btree/dbfile_lock_windows.go:1,29-51` `//go:build windows` real exclusive `LockFileEx`),
  `dbfile_lock_unix.go` (`internal/btree/dbfile_lock_unix.go:1,34-73` `//go:build unix` real flock),
  with `shm_mmap.go` (`//go:build (linux||darwin)&&(amd64||arm64)`) / `shm_other.go`
  (`//go:build !((linux||darwin)&&(amd64||arm64))`) and the `isLastClient` gate at `pager.go:3274-3285,3315`.

`Open` silently coerces `opts.InProcess = true` whenever `!hasMmapShm` (`db.go:413-415`), forcing single-process
heap-SHM mode on any platform lacking mmap SHM (e.g. Windows) with no error. The build-tag matrix diverges and is
mis-documented: `dbfile_lock_*.go` split on `unix` / `windows` / `!unix && !windows`, while `shm_*.go` split on
`(linux||darwin)&&(amd64||arm64)` vs its negation — so a unix platform that is NOT linux/darwin-on-amd64/arm64
(linux/386, linux/riscv64, freebsd, darwin on an unsupported arch) compiles the REAL flock implementation yet still
gets `hasMmapShm=false` and is forced into single-process mode. Only the wasm/js (`!unix && !windows`) target is a
pure no-op, where `tryUpgradeDBLockExclusive` returns `(true, nil)` unconditionally (`dbfile_lock_other.go:13`);
Windows now ships a REAL whole-file exclusive `LockFileEx` in `acquireExclusiveDBLock` (`dbfile_lock_windows.go:29-51`)
and only its `tryUpgradeDBLockExclusive`/shared/downgrade funcs remain no-op stubs. `pager.close` no longer relies on
the stub's `(true, nil)`: it short-circuits via `if p.inProcess { isLastClient = true }` (`pager.go:3274-3285`) before
calling `tryUpgradeDBLockExclusive`, then gates `wal.truncateFile()` and `wal.close(isLastClient)` (`pager.go:3315`) on
`isLastClient`, so forced-InProcess closers reach the last-client path via the in-process short-circuit, not the lock
stub. NOTES.md still never documents the no-op lock stubs at all. The consequence is undocumented platform behavior
that could mislead a maintainer auditing the multi-process lock protocol on non-mainstream platforms.

<a id="drift-117-heap-and-inprocess-shm-implement-real-locks-contradicting-no"></a>
### Drift: Heap And InProcess SHM Implement Real Locks Contradicting NOTES
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `shm.go` (`internal/btree/shm.go:42-46` newHeapShm/inProcessShm,
  `:79-119` lock/unlock), `shm_other.go` (`internal/btree/shm_other.go:57-102`
  heapShm.lock/unlock), with selection paths `wal.go:552-556` and `db.go:409-416`/`db.go:448`.

NOTES.md describes a single undifferentiated "Heap SHM fallback" with no-op locks (NOTES.md:382-383, 406), but the
code ships two distinct heap-backed shm types, both implementing REAL per-slot shared/exclusive lock semantics.
`newHeapShm()` returns `*inProcessShm` (`shm.go:42-46`), whose `lock`/`unlock` (`shm.go:79-119`) maintain genuine
per-slot lock counts backed by `sync.Mutex` + an int state (0=unlocked, >0=shared count, -1=exclusive) and return
`ErrBusy` on conflict; these are actually exercised by the WAL code (`lockWrite`/`lockCheckpoint`/`lockRecover`
exclusive, `lockRead0`... shared) in InProcess/InMemory mode. The separate `heapShm` (`shm_other.go:57-102`) on
non-mmap platforms implements the same real per-slot counter semantics. Crucially `inProcessShm` is selected on ALL
platforms — including mmap-capable linux/darwin amd64/arm64 — whenever `inProcess==true` (forced for InMemory and
InProcess, `db.go:409-416`/`db.go:448`, `wal.go:552-556`). The consequence is that NOTES is wrong on both counts: the
"no-op locks" claim and the platform matrix both misrepresent the real, always-on heap lock implementation.

<a id="drift-121-fdatasync-durability-primitive-platform-split-undocumented"></a>
### Drift: fdatasync Durability Primitive Platform Split Undocumented
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `osfuncs_sync_linux.go` (`internal/btree/osfuncs_sync_linux.go:11` ; `internal/btree/osfuncs_sync_other.go:7`),
  `osfuncs_vfs_sync_*.go` (`internal/btree/osfuncs_vfs_sync_linux.go:7`; `internal/btree/osfuncs_vfs_sync_other.go:5`;
  `internal/btree/osfuncs_sync_linux.go:11`; `internal/btree/osfuncs_sync_other.go:7`).

The `fdatasync` durability primitive invoked on every WAL commit/header write (`wal.go:1791/1817/2249`) and
checkpoint (`wal.go:3529/3729`, `pager.go:831`) is selected by a build-tag matrix that changes its sync semantics
per platform; this is now documented here and flagged by a source `DRIFT:` comment (`osfuncs_sync_linux.go:10`).
On Linux the non-vfs build calls `syscall.Fdatasync(int(f.Fd()))` (`osfuncs_sync_linux.go:11`, tag `!vfs && linux`),
a true data-only sync matching SQLite's `HAVE_FDATASYNC`; on every other platform (darwin, windows, the BSDs) it
falls back to `f.Sync()` (`osfuncs_sync_other.go:7`, tag `!vfs && !(js && wasm) && !linux`), i.e. a full fsync that
also flushes inode metadata. The vfs / wasm build mirrors this split for `defaultFdatasync`
(`osfuncs_vfs_sync_linux.go:7` `syscall.Fdatasync` vs `osfuncs_vfs_sync_other.go:5` `f.Sync()`). Crucially this
split MATCHES SQLite's own `os_unix.c` `full_fsync`, which uses `fsync` on `__APPLE__` (`os_unix.c:3831`) and
`fdatasync` elsewhere (`os_unix.c:3837`) — the upstream comment even notes fdatasync is "always adequate" for
SQLite (`os_unix.c:3769-3775`) — so this is a faithful port, not a divergence. The only residual nuance is that
the cost and exact durability guarantee of the commit/checkpoint hot path still differs by OS — Linux gets the
cheaper data-only flush, while other platforms pay for full metadata fsync.

<a id="drift-122-inmemory-masterstore-disk-emulation-undocumented"></a>
### Drift: InMemory masterStore Disk Emulation Undocumented
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `pager.go` (`internal/btree/pager.go:43` masterStore type, `:49` readPageInto,
  `:60` writePage, `:84` master field comment, `:455-456` open creates masterStore,
  `:1368-1373` readRawPage fallback, `:2130-2142` readHeaderCounters InMemory branch).

For InMemory databases there is no file on disk, so `pager.open` creates a `masterStore` (`pager.go:455-456`) — an `RWMutex`-protected
`map[uint32][]byte` (`pager.go:43`) — that REPLACES the database file as the "disk" backing, holding checkpointed
page data flushed out of the WAL. Its `readPageInto`/`writePage` (`pager.go:49,60`) form a VFS-disk emulation:
`checkpointPassive` writes pages into the map, and the page-read paths (`readRawPage` at `:1368-1373`,
`readHeaderCounters` at `:2130-2142`) fall back to it whenever `p.file == nil`. This is an in-process stand-in for the
real file VFS with no SQLite analogue, and it is entirely undocumented in NOTES.md. The consequence is that a
maintainer tracing where checkpointed pages physically land for an InMemory DB has no documentation pointing at the
map-backed "disk", and any reasoning about durability or post-checkpoint page state must reverse-engineer this layer.

<a id="drift-123-process-global-page-buffer-pool-single-page-size-constraint"></a>
### Drift: Process Global Page Buffer Pool Single Page Size Constraint
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `page_slab.go` (`internal/btree/page_slab.go:55-68` initPageBufferPool,
  `:47-49` pageBufferPoolSize, `errors.go:87-90` ErrPageBufferPoolSizeMismatch, `db.go:422` enforcement at Open,
  `pager.go:612` second enforcement on header read),
  `page_slab.go` (`internal/btree/page_slab.go:43-49` pageBufferPool/pageBufferPoolSize,
  `:78-109` allocPageBuffer/freePageBuffer dispatch, `:167-174` + `:193-194` slab overflow recycles via default pool).

In the default (non-slab) mode, page-buffer allocation routes through a single process-global `sync.Pool`
(`pageBufferPool`, `page_slab.go:43-49`) shared across every DB in the process, keyed to one global page size held in
the atomic `pageBufferPoolSize`. `initPageBufferPool` (`page_slab.go:55-68`) CAS-sets that size on first init and
thereafter returns `ErrPageBufferPoolSizeMismatch` (`errors.go:87-90`) for any DB opened with a different `PageSize`;
`db.Open` enforces this unconditionally on every open (`db.go:422`), and the pager re-enforces it against the on-disk
page size after reading the header (`pager.go:612`). Because `useSlab` defaults false
(`pcache.go:36,133`, `pager.go:229-231`), all page buffers for all DBs come from and return to this one shared
pool, and even slab-mode OVERFLOW buffers recycle through the same default pool (`page_slab.go:167-174,193-194`). The
consequence is an undocumented process-wide constraint with no SQLite counterpart — SQLite's `pcache1Alloc`
(`pcache1.c:341-361`) sizes each allocation per request and falls back to `sqlite3Malloc`, so it imposes no
process-wide single-page-size constraint. Two DBs in the same process cannot use different page sizes in default
mode, and the shared-pool/overflow-recycling design is invisible to a maintainer relying on NOTES.

<a id="drift-124-debug-tracing-subsystem-undocumented"></a>
### Drift: Debug Tracing Subsystem Undocumented
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `debug_trace.go` (`internal/btree/debug_trace.go:1-7`;
  `internal/btree/debug_trace_on.go:1-32` trace at `:30-32`, BTREE_TRACE env handling at `:16-27`),
  `debug_trace_on.go` (`internal/btree/debug_trace_on.go:15` init; build-tag pair
  `debug_trace_on.go:1` `//go:build debugtrace` vs `debug_trace.go:1` `//go:build !debugtrace`).

The btree package ships a Go-only, build-tag-gated debug tracing facility with no SQLite C counterpart, split across
two files (now documented in this section; a `// DRIFT:` comment at `debug_trace_on.go:14` links back to this anchor). The default build (`debug_trace.go:1`, tag `!debugtrace`) defines
`const debugTrace = false` and a no-op `func trace(format string, args ...any) {}` so tracing compiles away entirely.
Under `-tags debugtrace` (`debug_trace_on.go:1`) `debugTrace = true` (`debug_trace_on.go:10`) and a package-init `init()` (`debug_trace_on.go:15`)
reads the `BTREE_TRACE` environment variable at process startup, routing log output: empty / `"1"` / `"stderr"` go to
stderr, while any other value is treated as a file path opened with `os.OpenFile(v, O_CREATE|O_WRONLY|O_APPEND, 0644)`
(`debug_trace_on.go:16-27`, `os.OpenFile` at `:20`), creating the file if absent. The consequence is an undocumented diagnostic subsystem with
an environment-driven side effect (file creation) now documented in this section, and whose remaining genuine drift is simply that it has no place
in the C-to-Go mapping because it is purely a Go-side addition.

<a id="drift-125-dead-or-non-protocol-crc32-checksum-helpers"></a>
### Drift: Dead Or Non Protocol CRC32 Checksum Helpers
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `page.go` (`internal/btree/page.go:568-572`), `wal.go` (`wal.go:3912-3915`).

Two CRC32-IEEE helpers exist that are unrelated to the actual WAL checksum, which NOTES.md §7 documents as a
paired-word Fletcher-style additive recurrence (`s1 += x[i]+s2; s2 += x[i+1]+s1`), and both are misleading. `page.go:570`
defines `func checksum(data []byte) uint32 { return crc32.ChecksumIEEE(data) }` carrying the doc comment "checksum
computes a CRC32 checksum for data (used in WAL frames)" at `page.go:568`, which is false — the false comment persists
even though a DRIFT note now sits at `page.go:569` — the function has ZERO production callers
(only `page_test.go` references it) and the real WAL framing uses the custom paired-word algorithm, not CRC32. Separately
`walPageChecksum` (`wal.go:3912-3915`) also computes `crc32.ChecksumIEEE`, a distinct algorithm from the documented
`walChecksum`/`walFrameChecksum` recurrence in NOTES.md §7 (lines 344-359), and is itself undocumented. The consequence
is documentation/code confusion: a maintainer reading the `checksum` comment would wrongly believe CRC32 protects WAL
frames, and `walPageChecksum`'s separate CRC32 usage is invisible in the §7 checksum spec.

<a id="drift-126-pcache-truncate-and-clear-omit-page-1-zero-and-preserve-spec"></a>
### Drift: pcache Truncate And Clear Omit Page 1 Zero And Preserve Special Case
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pcache.go:*pcache.clear` (`internal/btree/pcache.go:593`),
  `pcache.go:*pcache.truncate` (`pcache.go:713-748`).

C's `sqlite3PcacheTruncate` (and `sqlite3PcacheClear`, which delegates to `Truncate(pCache, 0)`) carries a `pgno==0`
special case (`pcache.c:713-721`): when an outstanding reference to page 1 still exists (`pCache->nRefSum>0`), it does
NOT drop page 1 — instead it fetches it, zeroes its buffer in place (`memset(pPage1->pBuf, 0, szPage)`), and bumps
`pgno` to 1 so the final `xTruncate(pgno+1) == xTruncate(2)` RETAINS page 1 in cache with zeroed content. Go's
`clear()` (`pcache.go:593`) and `truncate()` (`pcache.go:713-748`) omit this page-1 zero-and-preserve branch for
referenced caches. The consequence is a behavioral divergence in the rare path where page 1 is still referenced during
a cache clear/truncate: SQLite keeps a live, zeroed page-1 header while Go does not, which could surface as a different
cache state for a still-pinned root page.

<a id="drift-127-pcache-recycle-and-spill-thresholds-off-by-one"></a>
### Drift: pcache Recycle And Spill Thresholds Off By One
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pcache.go:*pcache.create` (`internal/btree/pcache.go:342`),
  `pcache.go:*pcache.create` (`pcache.go:364`).

Go's merged `pcache.create()` diverges from SQLite on two cache-pressure thresholds, both shifting the trigger by one
page. For LRU recycle/buffer reuse, C's `pcache1FetchStage2` step 4 enters at `(pCache->nPage+1 >= pCache->nMax)`
(`pcache1.c:898-900`) and recycles AT MOST ONE page per create (net `nPage` unchanged), whereas Go uses
`for pc.nPage >= pc.maxPages && ...` (`pcache.go:342`) — both an off-by-one threshold (`nPage>=maxPages` vs
`nPage+1>=nMax`) and a loop instead of a single recycle. For dirty-page spill, C's `sqlite3PcacheFetchStress` gates on
strict `sqlite3PcachePagecount(pCache) > pCache->szSpill` (`pcache.c:453`) while Go gates the inline stress branch on
`pc.nPage >= spill` (`pcache.go:364`), firing the `xStress` spill callback one page earlier. The consequence is a subtle eviction/spill-timing divergence:
for the Step-4 recycle, C's `nPage+1>=nMax` triggers one page earlier than Go's `nPage>=maxPages`, while for the
dirty-page spill Go's `>=` fires one page earlier than C's strict `>`; either way the difference could affect cache
occupancy and spill frequency under memory pressure.

<a id="drift-129-resetpage-zeroes-buffer-on-every-page-creation"></a>
### Drift: resetPage Zeroes Buffer On Every Page Creation
- **Category:** changed-logic  -  **Severity:** low  -  **Status:** RESOLVED 2026-10-10: `resetPage` resets only the page bookkeeping, as `pcacheFetchFinishWithInit` does; every `create()` caller reads the whole page into the buffer (`getPageWriter`, `getPageReader`, savepoint restore) or clears it explicitly (`getPageNoContent`, a page beyond the snapshot's size, the new freelist trunk in `freePage`), so the per-creation 4 KB wipe was pure overhead on every cache miss.
- **Affected functions:** `pcache.go:*pcache.resetPage` (`pcache.go:429`).

SQLite's pcache layer never zeroes the page data buffer at fetch/recycle time: `pcacheFetchFinishWithInit`
(`pcache.c:501-520`) does `memset(&pPgHdr->pDirty, 0, sizeof(PgHdr)-offsetof(PgHdr,pDirty))`, clearing only the `PgHdr`
bookkeeping fields and leaving the page content buffer as-is (the buffer is overwritten by the subsequent read). Go's
`resetPage` (`pcache.go:420`) begins with `clear(p.data)` (`pcache.go:421`), unconditionally zeroing the full page content buffer on
every page-struct init — heap-alloc, `pFree` reuse, `initBulk`, and recycled-victim reuse alike. The consequence is an
extra full-buffer wipe on every page creation that SQLite does not perform; functionally safe but an added per-page
cost and a behavioral divergence (stale buffer contents are always cleared in Go) that NOTES.md does not record.

<a id="drift-130-newpcache-hash-table-pre-sized-to-capacity"></a>
### Drift: newPcache Hash Table Pre Sized To Capacity
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pcache.go:newPcache` (`internal/btree/pcache.go:135`; hashSizeFor at `pcache.go:160-166`;
  minHashSize at `pcache.go:156`).

SQLite always seeds the pcache hash table to exactly 256 buckets — `pcache1Create` (`pcache1.c:789`) calls
`pcache1ResizeHash` once and grows the table on demand thereafter (`pcache1.c:894`, with the 256 floor at
`pcache1.c:543-544`). Go's `newPcache` (`pcache.go:135`) instead pre-sizes
`apHash := make([]*page, hashSizeFor(maxPages))`, where `hashSizeFor` (`pcache.go:160-166`) returns the smallest power
of two `>= maxPages` and `>= minHashSize` (256, `pcache.go:156`). With the default `defaultCacheSize=5000` this
allocates 8192 buckets up front, and a larger configured cache allocates proportionally more. The consequence is a
larger eager hash-table allocation at cache creation than SQLite's fixed 256-bucket seed — an intentional
memory-vs-rehash tradeoff, now recorded in an in-code DRIFT comment at `pcache.go:134`.

<a id="drift-131-build-tag-gated-test-fault-hooks"></a>
### Drift: Build-Tag-Gated Test Fault Hooks
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `test_hooks.go` (`internal/btree/test_hooks.go` `walTestHooks=false`, default build) /
  `test_hooks_on.go` (`internal/btree/test_hooks_on.go` `walTestHooks=true`, tag `btreetesthooks`);
  hook field `wal.forceBusySnapshotForTest` and its check in `wal.beginWriteWithSnapshot`;
  consumers `busy_snapshot_hook_test.go` (BeginWrite bounded-retry + BusyHandler dispatch tests).

SQLite compiles fault-injection helpers only under `SQLITE_TEST` (e.g. the busy/fault simulation hooks around
`sqlite3InvokeBusyHandler`, `main.c:1700-1715`); production builds contain none of them. The Go port mirrors that
with the `btreetesthooks` build tag: default builds define `const walTestHooks = false`, so
`if walTestHooks && w.forceBusySnapshotForTest.Load()` is dead-code-eliminated and `BeginWrite` pays no atomic
load; `-tags btreetesthooks` enables the hook and the two ErrBusySnapshot retry-contract tests. The drift is only
the mechanism (build tag + const vs `#ifdef`); the gating structure follows upstream.

Two fault hooks are compiled into every build instead, as nil-checked atomic pointers: `walReadFrameFaultHook`
(`wal.readFrame`, `wal.readFrameRaw`, `pager.readWalFrameData`) and `rebuildPageFaultHook` (`btree.rebuildLeafPage`,
`btree.rebuildInteriorPage`); the plain function-variable test hooks (for example `testRecycleTempPageHook` and
`testCommitPrePublishHook`) are always compiled as well. Each costs one nil check on a path that already moves a
page of data, and the tests behind them run in the default `go test` (no Makefile or CI target passes
`-tags btreetesthooks`, so a tag-gated hook would leave them unrun).

---

## Audit-Discovered Drifts (2026-06-25)

A follow-up per-function C-vs-Go audit refresh (2026-06-25). Each drift below was discovered per source file, adversarially vetted by independent reviewers, and its cites re-confirmed against current source. The encryption/sqlcipher codec is excluded here (tracked separately).

<a id="drift-2026-06-25-25-first-opener-does-not-reset-a-stale-persisted-shm-trusts-checksum-vali"></a>
### Drift: First Opener Does Not Reset A Stale Persisted Shm; Trusts Checksum-Valid WAL-Index Header After A Crash Instead Of Forcing WAL Recovery
- **Category:** missing-feature  -  **Severity:** high  -  **Status:** RESOLVED 2026-07-10 (2026-07 pre-beta review): `newPlatformShm` now runs the complete `unixLockSharedMemory` first-attacher election (`os_unix.c:4860-4913`, 3.52.0) — `F_GETLK` probe of the DMS byte, then: `F_UNLCK` → DMS EXCLUSIVE + **unconditional** `Truncate(3)` + atomic downgrade to SHARED; `F_RDLCK` → join shared (a live process maintains the shm); `F_WRLCK` → BUSY back-off and re-probe, never a shared-join (the `os_unix.c:4864-4871` crash-mid-election race). Bounded 100×10ms in-open retry on contention (see [drift-2026-07-10-2](#drift-2026-07-10-2-dms-busy-retried-in-open-instead-of-surfacing)). Intra-process soundness rests on `openDBs` forbidding same-process double-open by file identity (fcntl record locks are per-(process,inode)), registered in `pager.open` at DB-fd open before any lock, the WAL open or the shm attach (SQLite's `findInodeInfo` point, `os_unix.c:6138`, since 2026-10-15), so a refused open never opens the shm; the shm file must never be open+closed by an attached process (any fd close drops the process's record locks). One mechanism correction to the original entry: the adopt fast paths cited below were in fact unreachable on a *fresh* attach, because `region(create=false)` never maps existing file content — every cold attach already funneled into `recoverLocked` (see [drift-2026-07-10-3](#drift-2026-07-10-3-region-create-false-never-maps-existing-content)); the hazard was latent behind that accident rather than live, and the election now makes the guarantee structural instead of accidental. Regression tests: `TestDMSFirstAttacherTruncatesStaleShm`, `TestDMSNonFirstAttacherDoesNotTruncate`, `TestDMSMidElectionBacksOff`, `TestDMSDowngradeFailureFailsOpen`, `TestStaleShmMxFrameBehind_CommittedTailVisible`, `TestStaleShmMxFrameAhead_UncommittedNotResurrected`, `TestStaleShmGarbageDiscardedOnReopen`, `TestShmStumpCrashBeforeRecoverySelfHeals`, plus the multi-process election tests (`multiprocess_dms_test.go`) and the refused same-process open tests (`double_open_test.go`: the live handle's wal-index header and DMS lock are unchanged, a live reader survives repeated refused opens).
- **Affected functions:** `shm_mmap.go:newPlatformShm` (`internal/btree/shm_mmap.go:93`, `internal/btree/shm_mmap.go:105`), `wal.go:*wal.ensureHeaderInitialized` (`internal/btree/wal.go:1589`; removed 2026-11-11, see `wal.open`'s adopt path).

In SQLite's `unixLockSharedMemory`, the connection that finds the DMS byte unlocked (`F_GETLK` reports `F_UNLCK`, `os_unix.c:4876`) is the first attacher: it takes an EXCLUSIVE DMS lock (`unixShmSystemLock(..., F_WRLCK, ...)`, `os_unix.c:4893`) and then *unconditionally* truncates the `*-shm` file to 3 bytes (`robust_ftruncate(pShmNode->hShm, 3)`, `os_unix.c:4902`) before downgrading to SHARED — and any peer that observes the DMS already held EXCLUSIVE backs off with `SQLITE_BUSY` (`os_unix.c:4906`) so the truncation is never raced. The 3-byte stump is smaller than the wal-index header, so the next `readHeader` necessarily fails validation and `walIndexRecover` rebuilds the index from the authoritative WAL file. The header comment (`os_unix.c:4864-4871`) spells out exactly why the truncate is unconditional: the shm is volatile and may be torn or stale after a power failure or crash, and trusting it can corrupt the database. The Go `newPlatformShm` acquires the DMS lock (`internal/btree/shm_mmap.go:93`) but then only truncates the shm to the 3-byte marker when the file was *freshly created* (`info.Size() == 0`, `internal/btree/shm_mmap.go:105`); if a `*-shm` file persists from a process that died without running `mmapShm.close` (so the file was never unlinked), the new opener sees `Size() > 0`, skips the truncate entirely, and `wal.ensureHeaderInitialized` takes its fast path (`if hdr, valid := w.index.readHeader(); valid`, `internal/btree/wal.go:1589`) — trusting the stale-but-checksum-valid header's `mxFrame`/salts/hash slots instead of recovering from the WAL. The consequence is that Go can resurrect a crashed writer's uncommitted frames, miss the real committed tail, or read against a mismatched salt generation, where SQLite's mandatory first-open truncate-to-3 would have forced a full WAL rebuild.

<a id="drift-2026-07-10-2-dms-busy-retried-in-open-instead-of-surfacing"></a>
### Drift: DMS Election BUSY Retried In-Open Instead Of Surfacing To The Caller
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `shm_mmap.go:newPlatformShm` (the election retry loop).

SQLite's `unixLockSharedMemory` returns `SQLITE_BUSY` when it observes a peer mid-election (`os_unix.c:4906-4908`, 3.52.0) and relies on higher-level retry (the comment at `os_unix.c:4865`: "it will try again"). Go's shm attach is a one-shot call inside `wal.open` with no busy-handler plumbing above it, so `newPlatformShm` retries the probe in-line, 100 tries 10 ms apart, and only after exhaustion fails the open with an error wrapping `ErrBusy` (`errors.Is(err, ErrBusy)` holds). The retry above SQLite's shm open is `walTryBeginRead`'s `WAL_RETRY` loop (`wal.c:3405-3407`), not the busy handler: a busy `xShmMap` is taken for transient (`wal.c:3094-3102`), retried up to `WAL_RETRY_PROTOCOL_LIMIT` times with growing sleeps (`wal.c:3043-3051`), and then `SQLITE_PROTOCOL` (`wal.c:3047-3049`). The exclusive-DMS window is microseconds (WRLCK → ftruncate(3) → downgrade, nothing else under it), so a single retry normally suffices. The divergence is the retry's shape and how it ends, `ErrBusy` here and `SQLITE_PROTOCOL` in SQLite. Neither returns the busy election to the application at once.

<a id="drift-2026-07-10-3-region-create-false-never-maps-existing-content"></a>
### Drift: region(create=false) Never Maps Existing Shm Content On A Fresh Attach
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `shm_mmap.go:*mmapShm.region` (the `!create` early return), `wal.go:*walIndex.readHeader` (first caller on the open path).

C `unixShmMap` with `isWrite==0` still maps *existing* regions of the shm file — a fresh attacher can read a peer-written (or crash-persisted) wal-index header through it. Go's `mmapShm.region(index, create=false)` returns "region not available" for any region not already mapped *by this instance* and never maps existing file content on demand. Consequence: on a fresh attach, `readHeader` (which starts with `region(0, false)`) always reports invalid, so `wal.open`'s adopt fast paths are dead on a cold start and every fresh multi-process attach funnels into `recoverLocked`. This accident is what kept the crash-stale-shm hazard (drift-2026-06-25-25) latent rather than live before the DMS election landed; it also means a cold attach pays a full WAL scan where C would adopt a live peer's header. **Sequencing constraint:** any future alignment of `region()` with `unixShmMap`'s map-existing behavior is safe ONLY because the DMS first-attacher election (2026-07-10) now provides the structural stale-shm guarantee — do not revert the election. Adjacent follow-up (separate issue): `recoverLocked` publishes a reset `aReadMark` without taking the reader-slot locks, so recovery triggered by a late attach while peers hold read slots could stomp a live reader's mark; C's `walIndexRecover` resets each `aReadMark[i]` only after taking `WAL_READ_LOCK(i)` EXCLUSIVE for that slot and leaves BUSY (live-reader-held) slots untouched (`wal.c:1575-1587`, 3.52.0). **RESOLVED (2026-07-17):** all SHM read-mark publication now goes through `walIndex.shmPublishReadMarks`, which either relies on the caller's slots-1-4 exclusive locks (RESTART/TRUNCATE path, `wal.c:2361`) or per-slot try-locks that skip BUSY slots (recovery/open paths, `wal.c:1573-1588`); the same pass removed the larger hazard of `shmWriteCkptInfo` bulk-rewriting marks on every checkpoint's counter publish — checkpoints now publish `nBackfill`/`nBackfillAttempted` as single words (`shmWriteNBackfill`/`shmWriteNBackfillAttempted`, `wal.c:2331`/`wal.c:2268`), restoring the `wal.c:367-370` invariant. Regression: `TestMultiProcessCheckpointPreservesPeerReadMark`.


<a id="drift-2026-10-15-1-empty-file-registry-key-is-the-path"></a>
### Drift: On Volumes With Unstable Empty-File Inodes, An Empty File's openDBs Key Is Its Path Until It Has Content
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.open` (registration and the create/existing decision), `pager.go:*pager.rekeyRegistry`, `pager.go:*pager.createIfEmpty`, `db.go:registryKey`, `db.go:unstableOpenMu`, `inode_unstable_darwin.go`.

SQLite's `findInodeInfo` keys its per-process inode registry on an fstat of the just-opened fd, under the process-wide `unixBigLock`; on macOS msdos volumes, where a zero-size file reports a wrong inode (ticket #3260; a FAT32 image on macOS 2026-10-15 reported 2^64−13 for an empty file and 13 after its first byte), it first writes one byte and fsyncs so the fstat sees a real inode (`os_unix.c:1553-1576`, 3.52.0) — safe there because 'S' is the first byte of every SQLite header and `unixFileSize` reports a size of 1 as 0. any-store's header does not start with that byte and a 1-byte file would not read as empty to `readInitialHeader`, the create/existing decision or another process, so on such a volume (darwin, `fstatfs` type `msdos` or `exfat`) an empty file is keyed by its absolute path instead, and `rekeyRegistry` moves the entry to the inode once the file has content: in `pager.open` after the create/existing decision, when the file holds a page 1 that this open (`createIfEmpty`, written and fdatasynced) or another process wrote — before any WAL open. Every open on such a volume takes `unstableOpenMu` for its key stat and store, and a path-keyed handle keeps it until its re-key, so the move from path to inode is atomic to the other openers in the process: two spellings of one empty file, or a joiner and its creator, meet on the mutex, the later one stats after the earlier one is keyed by inode and is refused at registration, and only one handle ever runs `createIfEmpty` on the file. The mutex is held across the open's waits for the DB-file lock (the busy handler's, against a closing or creating peer; [drift-63](#drift-63-new-db-page-1-written-directly-to-file-bypassing-wal), item 4), which SQLite's lock never is: latency only, for other opens on such volumes in this process. Wherever a (dev,ino) identity exists on a stable volume the key is the inode from the first stat, as in SQLite, and the mutex is never taken; Windows and wasm, and a VFS handle whose FileInfo carries no identity, key by path throughout, as before. Two processes creating one empty file at once are serialized by the exclusive DB-file lock ([drift-63](#drift-63-new-db-page-1-written-directly-to-file-bypassing-wal)). Regression tests (`openregistry_vfs_test.go`, vfs tag, a VFS that reports one inode for every empty file with `emptyFileInodeUnstable` forced on): `TestOpen_RegistryFollowsInodeOfNewFile`, `TestOpen_OpensOnUnstableVolumeSerializeOnCreate`, `TestOpen_JoinerDuringCreatorFdatasyncWaits`, `TestOpen_PathKeyedOpenJoinsContentByInode`, `TestOpen_TwoSpellingsSettleBeforeWAL`.

<a id="drift-2026-06-25-01-transient-busy-locked-equivalent-errors-are-made-sticky-in-b-rc-so-a-b"></a>
### Drift: Transient BUSY/LOCKED-Equivalent Errors Are Made Sticky In b.rc So A Backup Cannot Be Retried Or Resumed
- **Category:** error-handling  -  **Severity:** medium
- **Affected functions:** `backup.go:*Backup.Step` (`internal/btree/backup.go:191-193`, `backup.go:199-203`, `backup.go:218-222`, `backup.go:248-251`).

SQLite distinguishes transient contention from real failure: `isFatalError` (`backup.c:217-219`) returns false for `SQLITE_BUSY` and `SQLITE_LOCKED`, and `sqlite3_backup_step` gates its whole copy loop on `if(!isFatalError(rc))` (`backup.c:329-330`). Although the step still records `p->rc = rc` at the end (`backup.c:558`), a BUSY/LOCKED value is non-fatal, so a subsequent `sqlite3_backup_step` re-enters the loop and resumes copying from `iNext` — the caller simply calls step again to retry. The Go port has no `isFatalError` equivalent: `Step` short-circuits on *any* non-nil, non-`ErrBackupDone` rc (`backup.go:191-193`) and records every failure verbatim into the sticky `b.rc` at each site — source `BeginRead` (`backup.go:199-203`), destination `BeginWrite` (`backup.go:218-222`), and the per-page `getPageReader`/`onePage` copy (`backup.go:241-251`, e.g. `backup.go:248-251`) — so a momentary destination write-lock, a `BUSY_SNAPSHOT` race, or a busy-recovery error is treated identically to corruption. The consequence is that any transient busy/lock during a step permanently poisons the backup: every later `Step` re-returns the same error and `Finish` rolls the destination back, forcing callers to tear down and restart the entire copy from page 1 instead of retrying the step as SQLite allows.

<a id="drift-2026-06-25-04-point-lookup-mutation-descent-loops-omit-sqlite-s-btcursor-max-depth-b"></a>
### Drift: Point-Lookup And Mutation Descent Loops Omit SQLite's BTCURSOR_MAX_DEPTH Bound
- **Category:** error-handling  -  **Severity:** medium  -  **Status:** RESOLVED 2026-11-19: cursor navigation, the point lookups, the namespace lookup and the descents of `Put` and `Delete` apply `moveToChild`'s rule (`btree.c:5448`, inlined at `btree.c:6227`): with the number of child pointers followed from the root standing for `pCur->iPage`, a further step returns `ErrCorrupt` once that number reaches `btCursorMaxDepth-1`. A chain of 20 pages, root and leaf included, is read; a 21st level is corruption, as in SQLite. `btree.Put`, `btree.Delete` and `pathToChild` test the length of the path they already build, which is the cursor's `apPage`/`aiIdx` stack; `btree.AppendValue`, `ReadTx.AppendValue`, `ReadTx.Has`, `ReadTx.AppendSeekKey` and `DB.resolveNamespace` (the master-tree lookup) carry a counter. `ReadTx.leftmostKeyAfter`, the walk into the next subtree that `AppendSeekKey` makes when the leaf it reached holds nothing at or after the key, continues the count from the level of the page it restarts at, as a cursor that pops back to that ancestor and descends again would. The entry below named only the `btree.go` loops; the four `db.go` descents were as unbounded, and they are the ones a `ReadTx` lookup runs. The path growth it mentions was real for `Put`, `Delete` and `pathToChild`: a test process with two `Put`s on a cyclic tree reached about 3 GB in ten seconds. The walks over a whole tree are not held to that rule, and SQLite's are not all cursor walks either (`clearDatabasePage` recurses directly, `btree.c:10225`). Not changed: `countPage` and `walkSize` stop one level later than SQLite (`depth > btCursorMaxDepth`, counted from 0 at the root) and `keyRank` at `depth >= btCursorMaxDepth`, so each reads a chain one page longer than a cursor would before it reports corruption; `freeTreePages` has its own bound of 64 (`maxTreeDepth`), standing in for `clearDatabasePage`'s reference-count check. Tests: `TestDescentDepthBound_Cycle`, `TestDescentDepthBound_SeekKeyNextSubtree`, `TestDescentDepthBound_MasterTreeCycle`, `TestDescentDepthBound_AcceptsMaxDepth`, and `TestErrCorrupt_TreeWithACycleOfInteriorPages` at the collection level. Two known limitations, both as in SQLite. A write is not held to the bound: `splitRoot` deepens the tree with no look at its height, as `balance` calls `balance_deeper` on an over-full root (`btree.c:9134-9145`, `9034-9079`). An insert whose split reaches the root of a tree of 20 levels returns nil, and from then on a descent that has to go below level 20 returns `ErrCorrupt`, where SQLite's `moveToChild` refuses the step (`btree.c:5448-5450`): "If a tree that appears to be taller than this is encountered, it is assumed that the database is corrupt" (`btreeInt.h:494-495`). SQLite sizes the bound for "a maximum database size of 2^31 pages a minimum fanout of 2 for a root-node and 3 for all other internal nodes" (`btreeInt.h:490-492`). And the deepening moves every branch one level down, not only the one written: on a damaged tree whose branches differ in depth, a root split reached through a short branch pushes a branch of 20 levels elsewhere to 21, and the keys on it read as `ErrCorrupt` from then on; `balance_deeper` copies the whole root whatever the depth of its branches. Test: `TestDescentDepthBound_RootSplitAtMaxDepth`. What follows describes the code before that.
- **Affected functions:** `btree.go:AppendValue` (`internal/btree/btree.go:1076`), `btree.go:Put` (`internal/btree/btree.go:1217`), `btree.go:Delete` (`internal/btree/btree.go:2576`), `btree.go:*btree.pathToChild`, `btree.go:descendChild` (`internal/btree/btree.go:123`).

Go's point-lookup and mutation paths walk the tree with unbounded `for` loops that carry no depth counter. `AppendValue` (the engine behind `Get`/`Has`) spins `for {` until it hits a leaf (`internal/btree/btree.go:1076`), while `Put`, `Delete` and `pathToChild` (the re-descent of a three-way parent split) loop on interior pages without a counter. Each iteration descends via `descendChild` (`internal/btree/btree.go:123`), which validates only that the child pgno is in range (`childPgno != 0 && childPgno <= readerDbSizeBound`) and that a descended interior page has `cellCount >= 1`; neither check tracks how many levels have been traversed. SQLite instead caps cursor descent at `BTCURSOR_MAX_DEPTH` (20, `btreeInt.h:497`): `moveToChild` returns `SQLITE_CORRUPT_BKPT` once `pCur->iPage >= BTCURSOR_MAX_DEPTH-1` (`btree.c:5448`), and the inlined descent in `sqlite3BtreeIndexMoveto` applies the identical guard (`btree.c:6227`), so any chain deeper than 20 levels — including a cyclic interior loop — is rejected as corruption rather than followed indefinitely.

The consequence is that a corrupt or malicious database whose interior pages form a cycle of in-bounds page numbers (each cell passing `descendChild`'s pgno-range and `nCell >= 1` checks) makes a plain `Get`/`Has`/`Put`/`Delete` spin forever — a hang/DoS — whereas SQLite returns `SQLITE_CORRUPT` after 20 levels; the `pathBuf [8]pathEntry` stacks in `Put`/`Delete`/`insertIntoParent` additionally reallocate and grow without bound on every loop, leaking memory until the process is killed.

<a id="drift-2026-06-25-06-listnamespaces-swallows-every-cursor-first-error-as-empty-master-table"></a>
### Drift: ListNamespaces Swallows Every Cursor First Error As Empty Master Table
- **Category:** error-handling  -  **Severity:** medium
- **Affected functions:** `db.go:ListNamespaces` (`internal/btree/db.go:1330-1332`).

`ListNamespaces` opens a read-only cursor over the master B-tree (root page 1) and calls `cursor.First()`; on a non-nil error it unconditionally does `return nil, nil // empty master table` (`internal/btree/db.go:1330-1332`). But `Cursor.First` does not signal emptiness through an error — an empty root returns `nil` with `c.valid` left false, while a non-nil error is exactly the failure code from `getPage(c.bt.rootPage)`, e.g. `ErrCorrupt` or an underlying I/O error from reading page 1 (`internal/btree/btree.go:3477-3485`). Collapsing that error to `(nil, nil)` therefore makes a genuine read failure indistinguishable from a legitimately empty database.

SQLite keeps the two cases strictly apart. `sqlite3BtreeFirst` calls `moveToRoot` and only the `SQLITE_EMPTY` result sets `*pRes=1` and returns `SQLITE_OK`; any other `rc` is propagated to the caller (`btree.c:5676-5692`). `moveToRoot` returns the raw error code from `getAndInitPage` when the root page cannot be read (`btree.c:5574-5579`), and `getAndInitPage` returns `SQLITE_CORRUPT_BKPT` for an out-of-range or corrupt page (`btree.c:2386-2389`), so corruption and I/O errors always reach the caller rather than masquerading as an empty table. The consequence is that a corrupt or unreadable master B-tree (page 1) makes `ListNamespaces` return `(nil, nil)` — a successful, empty result — instead of surfacing `ErrCorrupt`/I-O error. A caller enumerating namespaces concludes the database has no namespaces and may treat live data as absent (e.g. re-create namespaces, skip migration, or report the DB as empty), masking corruption that SQLite would have reported.

<a id="drift-2026-06-25-12-verifyintegrity-bounds-the-sweep-with-the-live-writer-header-pager-hea"></a>
### Drift: VerifyIntegrity Bounds The Sweep With The Live Writer Header Instead Of The Read-Tx Snapshot Page Count
- **Category:** changed-logic  -  **Severity:** medium  -  **Status:** RESOLVED 2026-11-19: the sweep counts and reads the pages of its read transaction's snapshot. The count is `lockBtreePageCount` over page 1 of the snapshot and `pager.readerDbSizeBound(tx.cache)`, the rule of `ReadTx.DatabaseSize`, checked against the size the snapshot's WAL commit recorded when there is one; every page is looked up in the WAL within the transaction's frame window, floor included (`readRawPage` takes the reader cache and uses `readerMinFrame`).
- **Affected functions:** `integrity_sweep.go:*DB.VerifyIntegrity`, `integrity_sweep.go:*DB.sweepPageCount`, `pager.go:*pager.readRawPage`, `pager.go:*pager.readRawPage1Header`.

`VerifyIntegrity` opened a read transaction and then drove the page loop from `db.pager.header.DatabaseSize`, the pager's in-memory header, while reading each page at the transaction's `walMaxFrame`. That header is the writer's: it is refreshed at open, at a write begin (`refreshHeaderFromPage1`) and by a commit, never by a read transaction, and `pager.commit` sets its size before it writes a frame. The count and the page reads described two different database images, in both directions:

- **Too few pages.** In a process that had only read since a peer process grew the database, the header still held the size from open. The sweep verified those pages and reported the rest clean without reading them (`TestVerifyIntegritySweepsPagesAPeerAdded`: 2 pages swept of 170, a damaged page among the others).
- **Too many pages.** A sweep that began while a commit which grew the database had not yet published its frames read past its snapshot: the new pages are in no frame at or below its `walMaxFrame` and not in the database file, so each was listed as a page error on a healthy database. With mmap on the read went to mapped memory past the end of the file and the process died of SIGBUS (`dbMmap.remap` maps the requested length whatever the file's size). `TestVerifyIntegritySweepBesideGrowingCommit` covers both read paths. The header field was also read with no lock against the committing goroutine.

SQLite has one page count per read transaction. `sqlite3PagerPagecount` returns `pPager->dbSize` (`pager.c:3925-3929`), which `pagerPagecount` sets at the read begin from `sqlite3WalDbsize`, `pWal->hdr.nPage` of the snapshot, or from the file's size when the WAL has no commit (`pager.c:3279-3306`, `wal.c:3669-3674`). `lockBtree` derives the b-tree's `nPage` from page 1 and that count (`btree.c:3294-3298`, `3401-3408`), and both whole-file walks use it: `sqlite3BtreeIntegrityCheck` (`btreePagecount`, `btree.c:11163`) and the `sqlite_dbpage` table (`sqlite3BtreeLastPage`, `dbpage.c:260`), which is what a `verify_checksum(data) FROM sqlite_dbpage` scan of a cksumvfs database is bounded by. `IntegrityCheckN` and `Backup.Step` already took their count from the snapshot's page 1 and the pager's count for the snapshot. They do not treat a header that is not valid alike: `Backup.Step` goes through `lockBtreePageCount` and fails with `ErrHeaderCorrupt` on a size above the pager's count or on unequal non-zero change counters, as `lockBtree` fails the first (`SQLITE_CORRUPT_BKPT`, `btree.c:3401-3408`); `IntegrityCheckN` caps the size at the pager's count, takes the pager's count on any counter mismatch, and goes on. The sweep follows `lockBtreePageCount`, with the difference below when a WAL commit recorded the size.

Three things in the sweep remain its own, with no SQLite counterpart to align with:

- **Page 1 is read raw for the count.** The sweep reads every page without the codec, page 1 included, and takes the header from its first 100 bytes, which the codec does not cover. `ReadTx.DatabaseSize` decodes page 1 through the codec; on an AEAD database a damaged page 1 would then be the sweep's error instead of a listed page (`TestVerifyIntegrity_DamagedPage1Listed`). A cksumvfs database with verification on fails every read of a damaged page (`cksmRead`, `SQLITE_IOERR_DATA`), page 1 at the transaction's begin, so a `sqlite_dbpage` scan that reads the page from the file, a full-page VFS read, gets past damage only with verification off (a page already in the pager cache is returned without a read, `pager.c:5568`); the sweep reads raw so that it lists damage whatever the setting. When page 1 cannot be read whole (an I/O error past its header), the sweep reads the header alone (`readRawPage1Header`), lists page 1 with the read's error and goes on from page 2; with no WAL commit to give the count, the header's count is the one it sweeps to (`TestVerifyIntegritySweepPage1BodyUnreadable`, over a file longer than the database, so the file's length would sweep too far). A sweep that can read page 1 reads it once.
- **A page that cannot be read is listed, and the sweep goes on** (`SweepError` with `IntegrityKindUnknown`, pinned by `TestReadRawPageWALErrorPropagates`), where an SQLite scan stops at the first read error. The function's error is a read transaction that cannot begin, a cancelled context, and a page count that cannot be established: with no WAL commit to give it, a page-1 header that cannot be read, that `dbHeader.deserialize` rejects (magic, format versions, page size, payload fractions), or that fails `lockBtreePageCount`. The hook side of this is drift-2026-06-25-13. **Known limitation:** a read begin at a snapshot whose page-1 counters the handle does not hold yet reads them (`readHeaderCountersAt`), so a page-1 header that cannot be read is then the function's error and there is no report. At a snapshot whose counters it holds, the read begins, and the sweep lists page 1 and goes on only when a WAL commit of the snapshot gives the page count; without one (a reopened, checkpointed database whose WAL has had no commit since), the header that cannot be read is the function's error, as above. A `sqlite_dbpage` scan does the same in the first case: every transaction begins by reading page 1 and returns that read's error (`rc = btreeGetPage(pBt, 1, &pPage1, 0); if( rc!=SQLITE_OK ) return rc;`, `btree.c:3288-3289`), and in WAL mode the pager cache keeps page 1 across transactions only while the snapshot is unchanged (`pager.c:3261-3264`).

- **Page 1's count is checked against the WAL commit's.** The header is outside both codecs, so its page count carries no tag, and a count damaged downwards with the change counters intact is a well-formed header: `lockBtree` accepts a size below the pager's count (`btree.c:3294-3298`), because a file may hold pages past the database's end. A sweep that trusted it reported the remaining pages clean without reading them (41 pages of 169 after one flipped bit). cksumvfs has no such gap: its checksum covers the whole page, header included (`cksmCompute((u8*)zBuf, iAmt-8, cksum)`, `cksumvfs.c:450`). When the pager's count for the snapshot is the size a WAL commit recorded (`sqlite3WalDbsize`, `wal.c:3669-3674`), it is a second count of the same database: `pager.commit` writes page 1 whenever the size changes and passes the same size to the commit frame. In every mode a snapshot's header carries a size only once a commit has recorded one, as SQLite's does (`walIndexRecover` zeroes the header and sets `nPage` from a commit frame, `wal.c:1408`, `1512`), and a WAL reset keeps it (`walRestartHdr` zeroes `mxFrame` only, `wal.c:2146-2160`), so a non-zero size is the commit's after a TRUNCATE checkpoint too. `sweepPageCount` then sweeps the pages the commit recorded and lists page 1 (`IntegrityKindUnknown`) when the header is not valid or counts differently, in either direction, with the header's own error in `Inner`: `ErrHeaderCorrupt`, or `dbHeader.deserialize`'s `ErrNewerFormat`, `ErrOldFormat` or `ErrSQLiteFormat` for a header of another format (`TestVerifyIntegritySweepDamagedPageCount`, after a FULL and after a TRUNCATE checkpoint). A page 1 that cannot be read whole is listed with the read's error instead. That is SQLite's order: `lockBtree` returns a failed read of page 1 before it looks at the header (`btree.c:3288-3289`), and keeps the header's codes apart after it (`SQLITE_NOTADB` for the magic and the read version, `btree.c:3306-3312`, `3325`; `SQLITE_CORRUPT_BKPT` for a size above the pager's count, `btree.c:3401-3403`). The header of such a page 1, read alone, is listed as well, with `ErrHeaderCorrupt`, only when it counts fewer pages than the commit. When the pager's count is the file's length, a smaller count in page 1 stands, as for every reader (`TestVerifyIntegritySweepLogicalSizeBelowFile`): damage to the count is then not told apart from a longer file, here or in SQLite's b-tree layer. Only the sweep makes the cross-check; `ReadTx.DatabaseSize`, `IntegrityCheckN` and `Backup.Step` take the smaller count as SQLite does.

The slot-0 case of the frame floor (`TestReadRawPageSlot0IgnoresNewWALGeneration`): a snapshot of a fully checkpointed WAL reads nothing from the WAL (`walFindFrame`, `wal.c:3538-3541`), a WAL restart needs only read slots 1-4 and can run under it, and the next writer's frames are numbered from 1 again, below the snapshot's `mxFrame`. `readRawPage` looked pages up from the live checkpoint frontier and returned those frames.

<a id="drift-2026-06-25-18-walksize-has-no-visited-page-set-so-a-corrupt-dag-cycle-triggers-expon"></a>
### Drift: walkSize Has No Visited-Page Set So A Corrupt DAG Cycle Triggers Exponential Re-Traversal
- **Category:** performance  -  **Severity:** medium
- **Affected functions:** `namespace_size.go:*btree.walkSize` (`internal/btree/namespace_size.go:43`).

Go's `walkSize` recursively descends every interior child (`internal/btree/namespace_size.go:107`) and the right-most child (`internal/btree/namespace_size.go:114`), guarding only against unbounded depth via `if depth > btCursorMaxDepth` (`internal/btree/namespace_size.go:43-46`, cap 20). It keeps no set of already-visited page numbers, so it has no way to notice that two interior cells point at the same child page; each shared page is re-entered and its whole subtree re-walked once per inbound reference. SQLite's analogous tree walker `checkTreePage` calls `checkRef` on entry and returns immediately the first time a page is seen twice -- `if( checkRef(pCheck, iPage) ) return 0;` (`btree.c:10879`), where `checkRef` flags `"2nd reference to page"` via the per-page reference bitmap (`btree.c:10662`) -- so any page is descended into at most once and a cyclic/shared-child DAG is reported as corruption rather than traversed.

The consequence is that a small crafted or corrupt file whose interior cells point at shared in-bounds interior pages makes `NamespaceSize` do work proportional to (cells-per-page)^depth (depth capped at 20) -- effectively an unbounded CPU hang in a read-only introspection API -- instead of terminating, and even on benign-but-corrupt DAGs the returned page/entry counts double-count the shared pages.

<a id="drift-2026-06-25-23-open-silently-ignores-a-wal-page-1-frame-read-failure-and-keeps-the-st"></a>
### Drift: open() Silently Ignores A WAL Page-1 Frame Read Failure And Keeps The Stale DB-File Header
- **Category:** error-handling  -  **Severity:** medium  -  **Status:** RESOLVED 2026-07-10 (pre-beta BUG-05): `pager.open` now propagates both the `readFrame` and `header.deserialize` errors, matching SQLite `lockBtree`'s page-1 propagation. Regression test `TestOpenPage1WALReadErrorFailsOpen` (wal_test.go) via the readFrame fault hook.
- **Affected functions:** `pager.go:*pager.open` (`internal/btree/pager.go:671-688`).

After WAL recovery, `pager.open` refreshes the full in-memory header from the WAL's copy of page 1 precisely because the DB-file header may be stale (freelist pointers, schema cookie, change counter) when a crash occurred before checkpoint. It locates the page-1 frame via `p.wal.index.get` and, when `frame > 0`, reads it with `p.wal.readFrame` (`internal/btree/pager.go:676-681`). The frame lookup error is propagated (`internal/btree/pager.go:673-675`), but the `readFrame` call is guarded by `if err := p.wal.readFrame(frame, walBuf, nil, nil); err == nil` (`internal/btree/pager.go:678`): on a non-nil error the `deserialize` is simply skipped and `open` continues without surfacing the failure, so `p.header` retains the stale DB-file `FirstFreelistPg`/`TotalFreelistPgs`/`SchemaCookie`/`FileChangeCount`. SQLite never reaches the equivalent state silently: `readDbPage` returns the WAL-frame read result directly and the DB-file branch is unreachable once a frame is resolved (`pager.c:3031-3045`, returned at `pager.c:3073`), and `lockBtree` propagates any page-1 acquisition error before adopting header fields — `rc = btreeGetPage(pBt, 1, &pPage1, 0); if( rc!=SQLITE_OK ) return rc;` (`btree.c:3288-3289`). The consequence is that on a transient I/O error reading the authoritative page-1 WAL frame at open, the pager keeps the stale DB-file header and the very next commit serializes those stale freelist pointers back into page 1, dropping recently-freed pages or re-adding allocated ones, which can lead to double-allocation and on-disk corruption — exactly the failure the refresh comment says it prevents.

<a id="drift-2026-06-25-32-cross-process-online-backup-can-silently-produce-a-torn-snapshot-backu"></a>
### Drift: Cross-process online backup can silently produce a torn snapshot: backup restart-detection depends on FileChangeCounter that the conditional-bump commit (drift-77) leaves unadvanced for external data commits
- **Category:** changed-logic  -  **Severity:** medium  -  **Status:** RESOLVED 2026-11-13: `Backup.Step` restarts by the change epoch (`ReadTx.ChangeEpoch`), which moves when any transaction of the process begins at a wal-index header the process neither wrote nor had seen, as SQLite's Step restarts in the `pager_reset` of its own read transaction (`pager.c:1774`, `backup.c:351`). The page-1 counter plays no part. The same change ended the opposite fault, a restart at every marked commit of the process itself, which could keep a backup from ever finishing beside a steady writer. Tests: `TestBackupChange_ForeignCommits` (the peer marks nothing), `_ForeignCommitSeenByAnotherReader`, `_OwnCommits`, `_FinishesBesideOwnWriter`. What follows describes the code before that.
- **Affected functions:** `backup.go:*Backup.Step` (`internal/btree/backup.go:206-214`), `pager.go:2308-2319`.

`Backup.Step` detects external (other-process) writes and forces a full restart (`b.iNext = 1`) purely on `rtx.diskFileChangeCounter != b.lastFCC` (`backup.go:206-214`), with a comment asserting "Page-1 FileChangeCounter changes on every write commit, including from other processes." That assumption is false here: `pager.commit` bumps `FileChangeCount` only when the caller-supplied `dataChanged` flag is true (`pager.go:2308-2319`), and the btree mutation entry points `Put`/`Delete` (`db.go:1881-1896`) do not auto-set it — it is opt-in via `WriteTx.MarkDataChanged` (`db.go:1868-1872`), the documented [drift-77](#drift-77-filechangecount-bumped-conditionally-not-unconditionally) behavior. SQLite's `pager_write_changecounter` bumps unconditionally on the first write of a transaction (`pager.c:3084`, `pager.c:6363`), so `sqlite3BackupRestart`/`backupUpdate` (`backup.c:661-707`) reliably restart. The consequence is that a peer process committing real data-page changes without `MarkDataChanged` leaves page-1 `FileChangeCount` unchanged; a concurrent cross-process `Backup` then fails to restart after already copying pre-write versions of some pages while later Steps copy post-write versions — yielding a silently inconsistent (torn) destination that passes `Finish()` without error.

<a id="drift-2026-06-25-02-step-after-errbackupdone-re-enters-and-re-runs-finalize-and-can-copy-n"></a>
### Drift: Step After ErrBackupDone Re-Enters And Re-Runs Finalize Instead Of Being A No-Op
- **Category:** changed-logic  -  **Severity:** low (critical once backups stopped restarting at own commits)  -  **Status:** RESOLVED 2026-11-13: `Step` returns `b.rc` whenever it is set, `ErrBackupDone` included, as `isFatalError(SQLITE_DONE)` makes `sqlite3_backup_step` return at once (`backup.c:217-219`, `329-330`). While a backup restarted at every marked commit of its own process, the re-entered Step started over and the result was at least consistent; with the restart by the change epoch it would have copied only the pages the source grew by since, next to pages no update had refreshed. Test: `TestBackupChange_DoneIsTerminal`. What follows describes the code before that.
- **Affected functions:** `backup.go:*Backup.Step` (`internal/btree/backup.go:191`).

In C, `sqlite3_backup_step` reads `rc = p->rc` and only does work inside `if( !isFatalError(rc) )` (`backup.c:329-330`); since `isFatalError` returns true for any non-`OK`/`BUSY`/`LOCKED` code (`backup.c:217-218`), a sticky `SQLITE_DONE` left in `p->rc` after the prior completion (`backup.c:405-406`) makes the whole body a no-op — the function skips the copy loop and finalize and just re-returns `SQLITE_DONE`. The Go `Step` guard at `internal/btree/backup.go:191` instead reads `if b.rc != nil && b.rc != ErrBackupDone { return b.rc }`, explicitly excluding `ErrBackupDone` from the early return, so a Step call after completion falls through and re-executes the full body: it re-opens a fresh source read transaction (`backup.go:199`), re-reads the source size from that snapshot (`nSrcPage := rtx.DatabaseSize()`, `backup.go:234`), runs the copy loop again, and re-enters the `b.iNext > nSrcPage` finalize branch (`backup.go:263-266`) that re-runs `finalize` and re-sets `b.rc = ErrBackupDone`.

Because `nSrcPage` is re-derived from a new snapshot on each call rather than frozen at first completion, if the source database grew between the completing Step and the extra Step call, `b.iNext` (already past the old size) can again be `<= nSrcPage`, so the loop copies the newly-grown pages and finalize truncates/commits the destination to the larger size. The consequence is that an extra, normally-idempotent Step after `ErrBackupDone` is not a no-op in any-store: it re-opens a read tx and re-runs destination finalize, and on a grown source it copies additional pages and re-finalizes the destination to a larger image, changing what `Finish` ultimately commits.

<a id="drift-2026-06-25-03-balancenonroot-omits-sqlite-s-per-gathered-sibling-refcount-guard-so-a"></a>
### Drift: balanceNonroot Omits SQLite's Per-Gathered-Sibling Refcount Guard So A Corrupt Parent With Duplicate Aliased Child Pointers Aliases Siblings Instead Of Being Rejected
- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `balance.go:*btree.balanceNonroot` (`internal/btree/balance.go:194`).

On the non-root general-balance path, Go's `balanceNonroot` gathers the `nOld` child siblings by walking the parent's divider slots and pinning each one via `getWritablePage` (`internal/btree/balance.go:261-288`), where each pin increments the page's `pinCount` (`internal/btree/pcache.go:240-249`). The only structural validation before redistribution is that all gathered siblings share the same leaf/interior type (`internal/btree/balance.go:291-299`); there is no check that each gathered page is referenced exactly once. In SQLite's matching reuse loop, after taking an old sibling as a new sibling and calling `sqlite3PagerWrite`, the page's pager refcount is asserted to be exactly `1 + (i==(iParentIdx-nxDiv))` and otherwise returns `SQLITE_CORRUPT_BKPT` (`btree.c:8658-8662`, in the reuse block at `btree.c:8650-8663`). That guard rejects a corrupt parent whose two child slots point at the same page (or a self-ancestor cycle that re-pins a page already held), because such a page would be pinned more than once. Go then reuses/rebuilds the gathered pages in place, moving `apOld[g]` into `apNew[g]` (`internal/btree/balance.go:577-598`), so an aliased page is pooled twice, rebuilt under two `apNew` slots (last write wins, dropping cells), and released/freed multiple times. The consequence is that a detectable on-disk corruption — a parent with duplicate/aliased child pointers — is turned into in-memory `pinCount` underflow, dropped cells, and a potential double-free of a page buffer instead of a clean `ErrCorrupt`.

<a id="drift-2026-06-25-05-updateleafcell-grow-and-overflow-uses-an-inline-2-way-leafsplitpoint-s"></a>
### Drift: updateLeafCell Grow-And-Overflow Uses An Inline 2-Way leafSplitPoint Split Not The Faithful balanceNonroot The Insert Path Uses
- **Category:** changed-logic  -  **Severity:** low  -  **Status:** RESOLVED 2026-11-10: when the replacement cell does not fit, `updateLeafCell` rebuilds the page without the old cell (`dropCell`, `btree.c:9632`) and hands the new cell to `splitLeafAndInsertWithPath`, the dispatch an insert uses (`balance_quick`, `balanceNonroot`, or `balance_deeper` for a root leaf). A root leaf follows `balance()` too (`btree.c:9134-9151`): `balanceDeeper` moves its cells to a new child and `balanceNonroot` balances that child under the zero-cell root, so the cells are spread over as many pages as they need, and balance-shallower copies the child back when it took them all. Regression tests: `split_release_test.go` (`TestUpdateLeafCell_ReleasesPagesOnRebuildError`, `TestSplitRootLeafAndInsert_ReleasesPagesOnRebuildError`).
- **Affected functions:** `btree.go:*btree.updateLeafCell` (`internal/btree/btree.go:1432`), `btree.go:leafSplitPoint` (`internal/btree/btree.go:312`), `balance.go:*btree.balanceNonroot` (`internal/btree/balance.go:194`).

When an in-place update grows a value so the replacement cell no longer fits, Go's `updateLeafCell` does not route through the sibling-gathering balancer the insert path uses. After collecting the leaf's cells and confirming they overflow one page (`internal/btree/btree.go:1536`), it performs an inline 2-way median split: `leafSplitPoint(cells, pageUsable)` (`internal/btree/btree.go:1542`) picks a single split index targeting ~2/3 left-fill (`internal/btree/btree.go:312-345`), the page is rebuilt into the original plus one freshly allocated `rightPg`, and the separator is pushed up via `insertIntoParentWithPath` / `insertIntoParent` (`internal/btree/btree.go:1566-1568`). This always manufactures a new page even when an immediate sibling has free room. SQLite's `sqlite3BtreeInsert` treats the grow-an-existing-entry case identically to any other overflow: it `dropCell`s the old cell (`btree.c:9632`), re-inserts the new one, and when the page overflows calls `balance(pCur)` (`btree.c:9665`), whose `balance_nonroot` redistributes cells across up to three siblings (the path any-store itself reproduces in `balanceNonroot`, `internal/btree/balance.go:194`) and only adds a page when the gathered siblings genuinely cannot absorb the cells. The consequence is that value-growing updates produce a different, less-balanced tree shape than both SQLite and any-store's own insert path -- they unconditionally allocate a new page, raising page count and lowering fill factor -- a correctness-preserving but structural/performance divergence.

<a id="drift-2026-06-25-07-acquireexclusivedblock-is-a-no-op-on-plan9-wasip1-removing-the-cross-p"></a>
### Drift: acquireExclusiveDBLock Is A No-Op On plan9/wasip1, Removing The Cross-Process Double-Open Guard That Unix And Windows Enforce
- **Category:** missing-feature  -  **Severity:** low
- **Affected functions:** `dbfile_lock_other.go:acquireExclusiveDBLock` (`internal/btree/dbfile_lock_other.go:15`), `dbfile_lock_unix.go:acquireExclusiveDBLock` (`internal/btree/dbfile_lock_unix.go:46`), `dbfile_lock_windows.go:acquireExclusiveDBLock` (`internal/btree/dbfile_lock_windows.go:29`), `pager.go:*pager.open` (`internal/btree/pager.go:566`).

For file-backed databases that run in forced in-process mode (heap-backed WAL-index SHM, which is process-local), `pager.open` defends against a second OS process opening the same DB by taking a non-blocking whole-file EXCLUSIVE lock via `acquireExclusiveDBLock` (`internal/btree/pager.go:566`); a same-process double-open is refused earlier, by the `openDBs` registration at DB-fd open (`ErrDatabaseOpen`), so `ErrBusy` here means a different-process holder, or on Windows another spelling of the file (returning `ErrInProcessLocked`). On unix this is a real `flock(LOCK_EX|LOCK_NB)` (`internal/btree/dbfile_lock_unix.go:46`) and on Windows a real `LockFileEx` exclusive lock (`internal/btree/dbfile_lock_windows.go:29`), mirroring the OS-level whole-file lock that SQLite's VFS layers apply in `unixLock` (`os_unix.c:1866`) and `winLock` (`os_win.c:3321`). On the catch-all `!unix && !windows` targets (plan9, wasip1/wasm/js), `acquireExclusiveDBLock` is a bare `return nil` stub (`internal/btree/dbfile_lock_other.go:15`), and the file comment asserts these platforms "have no second-process notion" (`internal/btree/dbfile_lock_other.go:8`). The consequence is that on plan9 and wasip1/wasm — where two OS processes (or two WASI instances over a shared preopened directory) genuinely can open the same file-backed DB — the exclusive-lock guard silently succeeds for both, the `openDBs` registry only catches same-process reopens, and each side runs its own process-local heap SHM and WAL-index state so concurrent writes can corrupt the database, exactly the outcome unix/windows prevent via `ErrInProcessLocked`.

<a id="drift-2026-06-25-08-tryupgradedblockexclusive-can-silently-lose-the-caller-s-shared-lock-w"></a>
### Drift: tryUpgradeDBLockExclusive Can Silently Lose The Caller's Shared Lock When The Flock Conversion Fails
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `dbfile_lock_unix.go:tryUpgradeDBLockExclusive` (`internal/btree/dbfile_lock_unix.go:55-65`), `dbfile_lock_unix.go:flockNB` (`internal/btree/dbfile_lock_unix.go:74-85`), `pager.go:*pager.close` (`internal/btree/pager.go:3277-3284`).

To prove "last client" at close, the Go pager upgrades its lifetime DB-file lock from shared to exclusive by issuing a single `syscall.Flock(fd, LOCK_EX|LOCK_NB)` through `flockNB` (`internal/btree/dbfile_lock_unix.go:78`), with `tryUpgradeDBLockExclusive` mapping `ErrBusy` to `(false, nil)` (`internal/btree/dbfile_lock_unix.go:60-61`). Per BSD `flock(2)` semantics a shared→exclusive conversion on one open file description is not atomic: the kernel first drops the existing `LOCK_SH` and only then tries to install `LOCK_EX`, so when the exclusive grant cannot be satisfied immediately the `LOCK_NB` attempt fails *after* the shared lock has already been released. The caller therefore observes `(false, nil)` and treats itself as a non-last client (`internal/btree/pager.go:3277-3284`), unaware that its DB-file SHARED lock is now gone. SQLite's flock VFS sidesteps this entirely: in `flockLock` every lock at or above `SHARED_LOCK` is physically an exclusive `flock` (`os_unix.c:2682-2685`), so once any lock is held an upgrade just bumps the in-memory level and returns `SQLITE_OK` without re-issuing `robust_flock` (`os_unix.c:2696-2701`), and `flockCheckReservedLock` documents that the connection's lock is already exclusive (`os_unix.c:2643-2656`); the held lock is never momentarily dropped, and SQLite's real DB-file close protocol additionally uses atomic fcntl byte-range conversions that do not release the prior lock on failure. The consequence is that a non-last client whose upgrade fails returns `isLastClient=false` but has silently dropped its lifetime DB-file shared lock, opening a window (until its fd closes) in which a peer process's concurrent `tryUpgradeDBLockExclusive` sees no contention, wrongly declares itself last client, and truncates the WAL / unlinks the shm while this client is still attached — re-opening the early-truncation / orphan-inode race the DB-file lock protocol exists to prevent. Since 2026-11-19 the upgrade is the first step of `pager.close` (§SHM open/close protocol drift, item 1): a closer whose upgrade fails runs no checkpoint and no truncate, and only closes its descriptors after it, so the unlocked window is no longer than it was; the conversion itself is unchanged.

The second caller, `pager.lockDBForCreate` (the creation of a new database, [drift-63](#drift-63-new-db-page-1-written-directly-to-file-bypassing-wal)), does not depend on which way the kernel behaves: after a refused upgrade it releases the lock itself (`unlockDBFile`), and it is attached to no WAL or wal-index while it holds nothing.

<a id="drift-2026-06-25-09-downgradedblocktoshared-can-leave-the-db-file-fully-unlocked-or-spurio"></a>
### Drift: downgradeDBLockToShared Can Leave The DB File Fully Unlocked Or Spuriously Return ErrBusy
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `dbfile_lock_unix.go:downgradeDBLockToShared` (`internal/btree/dbfile_lock_unix.go:67-70`), `dbfile_lock_unix.go:flockNB` (`internal/btree/dbfile_lock_unix.go:74-85`).

To convert an exclusive DB-file lock back to shared, the Go code calls `downgradeDBLockToShared`, which simply issues `flockNB(fd, syscall.LOCK_SH)` (`internal/btree/dbfile_lock_unix.go:67-70`); that wrapper performs a real `syscall.Flock(fd, LOCK_SH|LOCK_NB)` and maps `EWOULDBLOCK`/`EAGAIN` to `ErrBusy` (`internal/btree/dbfile_lock_unix.go:74-85`). On BSD `flock` a lock-mode conversion is not atomic — the kernel first drops the existing exclusive lock and then tries to acquire the shared one — so with `LOCK_NB` set the re-acquire can fail if another process grabs a lock in the window, leaving the file with no lock at all while the call returns `ErrBusy`. SQLite's `flockUnlock` deliberately avoids any syscall for an exclusive→shared down-conversion: it observes that an exclusive lock already subsumes shared access and merely updates the in-memory state, `pFile->eFileLock = SHARED_LOCK; return SQLITE_OK;` (`os_unix.c:2747-2751`), reserving an actual `robust_flock(LOCK_UN)` only for the full `NO_LOCK` release path (`os_unix.c:2734-2763`), so its downgrade can never fail and never relinquishes the kernel lock.

The consequence is that a caller invoking downgrade after a successful exclusive upgrade may end up with the DB file silently unlocked (losing the mutual exclusion that serializes last-client-unlink against new openers) while receiving an unexpected `ErrBusy` from an operation that semantically cannot be busy; this is currently latent because no caller invokes `downgradeDBLockToShared` today, but the hazard is inherited by any future use of this cross-platform lock API (also stubbed in `dbfile_lock_windows.go`). The two exclusive→shared conversions in the open path do not use it: after a new database's page 1 is written (`createIfEmpty`), and when an open gets the exclusive lock and finds that another process has created the database (`lockDBForCreate`), both [drift-63](#drift-63-new-db-page-1-written-directly-to-file-bypassing-wal). They go through `pager.downgradeDBLock`, which repeats the shared request through the busy handler while it is refused, and they run before the WAL is opened, so a lock dropped inside the conversion leaves the handle unlocked only while it is attached to nothing.

<a id="drift-2026-06-25-10-flocknb-does-not-retry-on-eintr-no-robust-flock-equivalent"></a>
### Drift: flockNB Does Not Retry On EINTR (No robust_flock Equivalent)
- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `dbfile_lock_unix.go:flockNB` (`internal/btree/dbfile_lock_unix.go:74-85`).

The Go DB-file lock wrapper `flockNB` issues a single `syscall.Flock` call (`internal/btree/dbfile_lock_unix.go:78`) and only special-cases `EWOULDBLOCK`/`EAGAIN`, which it maps to `ErrBusy`; every other errno — including `EINTR` — is wrapped and returned as a hard error (`internal/btree/dbfile_lock_unix.go:79-82`). All callers (`acquireSharedDBLock` `internal/btree/dbfile_lock_unix.go:35`, `acquireExclusiveDBLock` `internal/btree/dbfile_lock_unix.go:47`, `tryUpgradeDBLockExclusive` `internal/btree/dbfile_lock_unix.go:56`, `downgradeDBLockToShared` `internal/btree/dbfile_lock_unix.go:69`) route through this single non-retrying call. SQLite, by contrast, never calls `flock()` directly: it wraps it in `robust_flock`, which loops `do{ rc = flock(fd,op); }while( rc<0 && errno==EINTR )` (`os_unix.c:2615-2618`), and both `flockLock` (`os_unix.c:2705`) and `flockUnlock` (`os_unix.c:2754`) acquire and release the whole-file lock exclusively through that retry helper.

The consequence is that a signal delivered to the calling goroutine's OS thread mid-syscall can surface a spurious hard error from a lock that would otherwise have succeeded — turning, for example, an `acquireSharedDBLock` open or a `tryUpgradeDBLockExclusive` upgrade into a failure path instead of completing; the probability is low because every call sets `LOCK_NB` (the flock returns immediately, leaving only a narrow EINTR window), but the defensive EINTR retry SQLite relies on is absent.

<a id="drift-2026-06-25-11-checktreepage-anchors-child-depth-comparison-on-the-first-child-and-ne"></a>
### Drift: checkTreePage Anchors Child-Depth Comparison On The First Child And Never Adopts A Divergent Depth

- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `integrity.go:checkTreePage` (`internal/btree/integrity.go:488-493`, `internal/btree/integrity.go:507-514`).

When walking an interior page, Go's `checkTreePage` anchors the expected subtree depth on the FIRST child it recurses into (`if i == 0 { depth = childDepth }`, `internal/btree/integrity.go:489-490`) and thereafter only compares against that fixed anchor: every later child whose depth differs emits a separate `"child page depth differs"` message without ever updating `depth` (`internal/btree/integrity.go:491-493`), and the right child is checked against the same un-updated anchor at the end (`internal/btree/integrity.go:508-511`). SQLite instead anchors `depth` on the right child first (`depth = checkTreePage(...)`, `src/btree.c:10935`) and, in the per-cell loop, RE-anchors after each mismatch: `d2 = checkTreePage(...)`; `if( d2!=depth ){ checkAppendMsg(pCheck, "Child page depth differs"); depth = d2; }` (`src/btree.c:11000-11005`). Because SQLite reassigns `depth = d2` it compares each child with the one checked before it (child depths 2, 3, 2 give two messages, one per change); Go, holding the first child's depth as the immutable reference, reports every child that differs from the first. The consequence is that on a corrupt interior page whose first-anchored child carries the outlier depth, Go emits one redundant "child page depth differs" message per remaining child (and the right child) where SQLite emits a single message and re-anchors, draining the bounded `maxErrors` budget faster and potentially suppressing other distinct corruption diagnostics later in the walk; it is a read-only diagnostic divergence with no data-correctness impact.

<a id="drift-2026-06-25-13-checksum-sweep-fires-the-onerror-errcksummismatch-hook-even-for-plain-"></a>
### Drift: Checksum Sweep Fires The OnError(errCksumMismatch) Hook Even For Plain File-I/O Read Errors
- **Category:** error-handling  -  **Severity:** low  -  **Status:** RESOLVED 2026-11-19: the sweep fires the hook only for a page whose checksum was compared and differed (`Kind == IntegrityChecksumMismatch`); a page it could not read is listed and fires nothing (`TestReadRawPageWALErrorPropagates`). A page whose WAL frame cannot be read is listed with the read's error: `ErrWALShortRead` for a short read ([drift-2026-07-10-1](#drift-2026-07-10-1-readframe-past-tail-read-failure-remapped-to-errwalcorrupt)), any other error as it is.
- **Affected functions:** `integrity_sweep.go:*DB.VerifyIntegrity`, `integrity_sweep.go:verifyCksumPage`.

In `VerifyIntegrity`'s checksum branch, any non-nil `SweepError` returned by `verifyCksumPage` triggered the database's integrity hook (`Options.OnIntegrityError`, which the pager holds) with a hard-coded `errCksumMismatch`, regardless of which failure produced it. But `verifyCksumPage` returns two distinct error shapes: a genuine trailer mismatch carries `Kind: IntegrityChecksumMismatch` (its `want != got` compare mirrors `cksumCodec.Decrypt`), while a failed raw-page read returns `&SweepError{PageNo: pgno, Inner: err}` with `Kind` left at the zero value `IntegrityKindUnknown`. The sweep loop did not inspect the kind before firing the hook, so a transient file-I/O read fault was reported through the same `errCksumMismatch` notification as an actual corrupt page.

SQLite's cksumvfs keeps the two cases strictly separate: `cksmRead` computes and compares the trailer only when the underlying `xRead` succeeded (`rc==SQLITE_OK`), surfacing a real trailer mismatch as `SQLITE_IOERR_DATA` (`cksumvfs.c:451`, `cksumvfs.c:455`), whereas a plain read failure propagates the original `xRead` error code (e.g. `SQLITE_IOERR_READ`) and never reaches the checksum comparison at all — C never relabels an I/O fault as a data/checksum fault. Subscribers wired to `OnIntegrityError` (anystore corruption handlers) therefore received a checksum-mismatch event for a transient read error during a sweep, which could mark a healthy database corrupt or start a recovery. The severity was low because the sweep's report gave such a page the read's kind, `IntegrityKindUnknown`: only the hook misclassified it.

<a id="drift-2026-06-25-14-page-1-db-header-bytes-0-99-is-excluded-from-both-checksum-and-aead-in"></a>
### Drift: Page-1 DB Header Bytes 0..99 Excluded From Both Checksum And AEAD Integrity Coverage

- **Category:** missing-feature  -  **Severity:** low
- **Affected functions:** `integrity_sweep.go:verifyCksumPage`, `integrity_sweep.go:verifyAEADPage`, `codec.go:encryptPageWithCodec`.

For page 1 the Go integrity sweep starts its hash at `dbHeaderSize` rather than 0: `verifyCksumPage` sets `start = dbHeaderSize` when `pgno == 1` and hashes only `raw[start:bodyEnd]`, so the first 100 bytes of page 1 never enter the XXH3-128. That exclusion is structural, baked into the codec write path: `encryptPageWithCodec` copies the first `dbHeaderSize` bytes as a plaintext prefix and only encrypts/authenticates the remainder, and the cksumCodec documents that "the page-1 DB header is therefore NOT covered by the per-page checksum" (`internal/btree/codec_cksum.go:26-31`). For the AEAD codec, `verifyAEADPage` decrypts only that same post-prefix slice, so the header bytes carry no authentication tag. What is checked of them is their format, not their integrity: `dbHeader.deserialize` at open, at every write begin, and in every sweep, which decodes its snapshot's header (`sweepPageCount`) and applies `lockBtreePageCount`. A header that stops being valid after open (a payload-fraction byte, the magic) is found by the next sweep: listed against page 1 when a WAL commit gives the page count, the sweep's `ErrHeaderCorrupt` otherwise. A header `DatabaseSize` of 0 is not an error: the pager's count stands in. A page count that differs from the one the snapshot's WAL commit recorded is listed (drift-2026-06-25-12); any other field changed to another valid value passes.

SQLite's cksumvfs, by contrast, checksums the entire page including page 1's 100-byte database header: the read path computes `cksmCompute((u8*)zBuf, iAmt-8, cksum)` starting at offset 0 of the page buffer (`cksumvfs.c:450`), and the `verify_checksum()` SQL function does the same with `cksmCompute(data, nByte-8, cksum)` over the whole blob (`cksumvfs.c:353`); `cksmCompute` walks from `a[0]` to `a[nByte]` with no header carve-out (`cksumvfs.c:299`). The consequence is that bit-rot or tampering confined to the page-1 header — e.g. a flipped freelist-trunk pointer or schema cookie that still parses as structurally valid, or a `DatabaseSize` lowered in a snapshot with no WAL commit — passes `VerifyIntegrity` clean, whereas an equivalent SQLite cksumvfs database would flag it; for the AEAD codec this is an authentication gap on file-structure metadata.

<a id="drift-2026-06-25-15-remap-grows-mapping-to-a-single-page-end-and-always-full-unmaps-re-mma"></a>
### Drift: Remap Always Unmaps And Re-mmaps The Whole Region Instead Of Extending In Place
- **Category:** performance  -  **Severity:** low
- **Affected functions:** `mmap_db.go:*dbMmap.remap` (`internal/btree/mmap_db.go:81-128`), `pager.go:readDBPage` (`internal/btree/pager.go:351-353`).

Go's `remap` grows the database mapping by computing `target = need` capped at `maxSize` (`internal/btree/mmap_db.go:87-90`), and whenever the target exceeds the current region it unconditionally `munmap`s the entire existing region (`internal/btree/mmap_db.go:98-103`) and re-`mmap`s the whole `[0, target)` range from offset 0 (`internal/btree/mmap_db.go:119`). The sole grower, `readDBPage`, requests `need = offset + len(buf)` — just past the page being fetched (`internal/btree/pager.go:351`) — so every miss extends the window by only a single page-end. SQLite instead maps the whole file at once: `unixMapfile` `fstat`s the file and maps `st_size` (capped at `mmapSizeMax`) when `nMap < 0` (`os_unix.c:5682-5696`), and its non-`HAVE_MREMAP` branch extends in place — it keeps the reusable prefix and `mmap`s only the new tail at `pReq = &pOrig[nReuse]` with file offset `nReuse` (`os_unix.c:5607-5645`), fully unmapping the old region only if that in-place extension fails (`os_unix.c:5637-5639`). The code comment at `internal/btree/mmap_db.go:95-97` asserting that "SQLite's non-HAVE_MREMAP branch does the same: unmap+remap" misreads the C: that branch extends the mapping rather than tearing it down wholesale.

The consequence is that a forward scan over N pages not yet in the window triggers N separate `munmap`+`mmap` syscall pairs, each re-mapping the entire growing region from scratch and flushing the TLB, where SQLite maps once (or cheaply extends in place); for sequential reads this can make the mmap fast-path slower than plain `pread`, undermining the feature, and the misleading comment overstates the alignment with SQLite.

<a id="drift-2026-06-25-16-mmap-syscall-failure-does-not-permanently-disable-mmap-sqlite-sets-mma"></a>
### Drift: mmap() Syscall Failure Does Not Permanently Disable mmap (SQLite Sets mmapSizeMax=0 To Stop Retrying)

- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `mmap_db.go:*dbMmap.remap` (`internal/btree/mmap_db.go:119-124`).

In Go's `dbMmap.remap`, when the `syscall.Mmap` call fails the error is treated as purely transient: the failure branch (`internal/btree/mmap_db.go:120-124`) returns a wrapped error and leaves `m.maxSize` unchanged, so the next page fetch re-enters `remap` (gated only on `m.maxSize == 0` at `internal/btree/mmap_db.go:84`) and re-attempts the same failing `syscall.Mmap` again. The in-code comment at `internal/btree/mmap_db.go:121-123` cites SQLite's "continue accessing the database using the xRead()/xWrite() methods" fallback note but stops short of mirroring the second half of that behavior. SQLite's `unixRemapfile` instead assumes that once an `mmap()` fails, all subsequent ones will likely fail too, so it permanently disables memory-mapped I/O for the file by setting `pFd->mmapSizeMax = 0` (`os_unix.c:5652-5655`), causing every later access to go straight to xRead/xWrite with no further syscall attempts. The consequence is that under sustained mmap failure (address-space/ENOMEM pressure or `vm.max_map_count` exhaustion) the Go port issues — and fails — a fresh `mmap` syscall on every page read that would otherwise be served from the mapping, adding one failing syscall per read, whereas SQLite gives up once and never retries.

<a id="drift-2026-06-25-17-walksize-descends-child-right-child-page-numbers-without-the-bounds-gu"></a>
### Drift: walkSize Descends Child/Right-Child Page Numbers Without The Bounds Guard Used By descendChild And SQLite getAndInitPage

- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `namespace_size.go:*btree.walkSize` (`internal/btree/namespace_size.go:43`).

`walkSize` (the recursive worker behind `ReadTx.NamespaceSize`) fetches every page it visits with a bare `bt.getPage(pgno)` (`internal/btree/namespace_size.go:47`) and, on an interior page, recurses straight into each cell's `c.leftChild` (`internal/btree/namespace_size.go:107`) and then the page-header `rightChild` (`internal/btree/namespace_size.go:114`). Apart from the `depth > btCursorMaxDepth` cycle guard, it applies no validation to those page numbers before descending. The cursor descent path deliberately routes the identical interior->child step through `descendChild`, which first rejects an out-of-range pointer -- `if childPgno == 0 || childPgno > bt.pager.readerDbSizeBound(bt.cache) { return nil, ErrCorrupt }` (`internal/btree/btree.go:124`) -- precisely because the pager getters zero-fill above-bound pages (drift-4) instead of erroring. SQLite likewise refuses to descend a wild pointer: `getAndInitPage` guards every child fetch with `if( pgno>btreePagecount(pBt) ){ return SQLITE_CORRUPT_BKPT; }` (btree.c:2386), and the size-walking analogue `checkTreePage` runs each page number through `checkRef`, which fails `if( iPage>pCheck->nCkPage || iPage==0 )` with "invalid page number" (btree.c:10657-10658) before getting the page (btree.c:10879).

Because `walkSize` skips this guard, a corrupt or partially-truncated namespace whose interior cell carries a child `pgno` past the snapshot bound (or `pgno == 1`, the DB header page that lives outside the namespace tree) is followed rather than rejected: `getPage` returns a fabricated zero page, `res.Pages++` counts that phantom page, and the descent continues until a downstream parse fails with `ErrInvalidPage`/`ErrCorrupt` or, in the worst case, a zero-filled page is silently summed into an inflated/garbage size. The consequence is that `NamespaceSize` on a damaged tree does not fail fast with `ErrCorrupt` at the point the wild pointer is observed, the way the cursor path and SQLite's `getAndInitPage`/`checkRef` do, but instead traverses into fabricated pages and returns a late, less-specific error or a polluted size count.

<a id="drift-2026-06-25-19-osopenfile-cannot-force-exact-file-permissions-for-wal-journal-shm-fil"></a>
### Drift: osOpenFile Cannot Force Exact File Permissions For WAL/Journal/SHM Files
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `osfuncs.go:osOpenFile` (`internal/btree/osfuncs.go:10-12`), `pager.go:*pager.open` (`internal/btree/pager.go:460`), `wal.go:*wal.open` (`internal/btree/wal.go:1447`), `shm_mmap.go:newPlatformShm` (`internal/btree/shm_mmap.go:83`).

Go opens every on-disk file through `osOpenFile`, a thin wrapper that just forwards to `os.OpenFile(name, flag, perm)` (`internal/btree/osfuncs.go:10-12`) and never re-stats or `fchmod`s the result. All three call sites hand it a hard-coded `0666` permission word — the main DB file (`internal/btree/pager.go:460`), the WAL file (`internal/btree/wal.go:1447`), and the shm file (`internal/btree/shm_mmap.go:83`) — which the process umask then reduces, so the effective mode is always `0666 & ~umask` regardless of how the DB file itself is permissioned. SQLite instead routes file creation through `robust_open`, which after opening a freshly created (`st_size==0`) file whose mode does not already match the requested `m` calls `osFchmod(fd, m)` to force the exact permission bits past the umask (`os_unix.c:834-872`, specifically the `fchmod` at line 864); and for WAL and journal files it first derives `m` in `findCreateFileMode` by `stat`ing the associated database file so that "WAL and journal files are created using the same permissions as the associated database file" (`os_unix.c:6440-6460`). The consequence is that when the database file's permissions are broader than the umask allows (e.g. a group/other-writable shared DB created via chmod), any-store's WAL/SHM/journal files are created with umask-reduced `0666` instead of matching the DB, so a second user who can write the DB may be unable to open or recover the WAL or shared-memory file after a crash; this only matters for multi-user/shared-permission deployments and has no visible effect on the typical single-user embedded store.

<a id="drift-2026-06-25-20-linux-fdatasync-uses-raw-syscall-fdatasync-with-no-eintr-retry-unlike-"></a>
### Drift: Linux fdatasync Uses Raw syscall.Fdatasync With No EINTR Retry, Unlike Sibling f.Sync() Path
- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `osfuncs_sync_linux.go:fdatasync` (`internal/btree/osfuncs_sync_linux.go:11`).

The Linux build of `fdatasync` is a single bare syscall — `syscall.Fdatasync(int(f.Fd()))` (`internal/btree/osfuncs_sync_linux.go:11`) — with no loop around `EINTR`, so a sync interrupted by a Go-runtime signal (async-preemption `SIGURG`, etc.) returns `EINTR` verbatim to the caller. Every durability call site forwards that error unchanged: the WAL header flush (`internal/btree/wal.go:1791`), commit (`internal/btree/wal.go:1817`, `internal/btree/wal.go:2249`), checkpoint (`internal/btree/wal.go:3529`), and the page-1 DB sync (`internal/btree/pager.go:831`) all do `if err := fdatasync(...); err != nil { return ... }`. The non-Linux sibling (`internal/btree/osfuncs_sync_other.go:7`) instead calls `f.Sync()`, whose Go-runtime implementation retries the underlying fsync through `ignoringEINTR`, so the identical interruption is transparently re-issued on macOS and the other platforms.

SQLite's `full_fsync` likewise issues a raw `fdatasync(fd)` with no EINTR loop on the Linux path (`os_unix.c:3837`), but it wraps its other low-level syscalls in explicit retry loops — e.g. `seekAndWriteFd` retries `osPwrite` while `rc<0 && errno==EINTR` (`os_unix.c:3604`). The consequence is platform-asymmetric durability robustness: on Linux (the primary production target) a spuriously-interrupted fdatasync aborts a WAL commit, checkpoint, or DB-create as an I/O failure even though an immediate retry would have succeeded, whereas the macOS/other build path silently recovers via `ignoringEINTR`.

<a id="drift-2026-06-25-21-dbheader-deserialize-does-not-validate-file-format-read-write-version-"></a>
### Drift: dbHeader.deserialize Does Not Validate File-Format Read/Write Version Bytes (Offsets 18/19)
- **Category:** error-handling  -  **Severity:** low  -  **Status:** RESOLVED 2026-11-10: `deserialize` calls `checkHeaderFormat`, which refuses a read or write version above 2 and a schema format above the newest known; see [the header format gate](#drift-2026-11-10-1-header-format-gate) for what still differs (a newer write version refuses instead of opening read-only, and a header reloaded after another process commits is checked for the format only).
- **Affected functions:** `page.go:*dbHeader.deserialize` (`internal/btree/page.go:262-264`).

`dbHeader.deserialize` reads the file-format write-version (byte 18) and read-version (byte 19) verbatim into `h.WriteVersion = buf[18]` / `h.ReadVersion = buf[19]` without any range check (`internal/btree/page.go:262-264`). It validates the magic string, the page-size byte, and the embedded-payload-fraction constants at offsets 21-23, but never inspects the version bytes themselves. SQLite, in `lockBtree`, treats these bytes as a compatibility gate: `page1[18]>2` forces `BTS_READ_ONLY` and `page1[19]>2` does `goto page1_init_failed`, which has already set `rc = SQLITE_NOTADB`, so an unknown read-version makes the open fail (`btree.c:3314-3327`, with the `SQLITE_OMIT_WAL` build using the stricter `>1` bounds at `btree.c:3315-3320`).

The consequence is that a database whose read-version byte is greater than 2 (a future or unknown on-disk format) is opened and read by Go as if it were a current file rather than being refused the way SQLite refuses it with `SQLITE_NOTADB`, and a write-version greater than 2 is not forced read-only, so the forward-compatibility / wrong-format protection SQLite performs at open is absent here.

<a id="drift-2026-06-25-22-slab-get-has-no-per-request-size-guard-and-its-initialized-pagesize-va"></a>
### Drift: Slab Get Has No Per-Request Size Guard And Its Initialized PageSize Validator Is Dead Code
- **Category:** missing-feature  -  **Severity:** low  -  **Status:** RESOLVED: the per-request guard is ported.
- **Affected functions:** `page_slab.go:allocPageBuffer`, `page_slab.go:*pageSlab.serves`, `db.go:Open`.

`pcache1Alloc(nByte)` (`pcache1.c:341-374`) serves a request from the buffer only under `if( nByte<=pcache1.szSlot )` (`pcache1.c:344`) and otherwise calls `sqlite3Malloc(nByte)` (`pcache1.c:361`). `allocPageBuffer(pageSize, useSlab)` takes a buffer from the slab only when `globalPageSlab.serves(pageSize)`, and otherwise takes the default allocator (`pageBufferPool`, then `make`) at the requested size. `freePageBuffer` needs no guard: `pageSlab.Put` keeps only slices of the slab's backing array and passes everything else to the pool. A slab configured for one page size is never the source of a page for a database of another. Pinned by `TestAllocPageBuffer_SlabServesOnlyItsOwnPageSize` and, through the public API in one process per configuration, by `TestInitPageBuffer_MisSizedPoolIsNotUsed`.

What remains different:

- **Equality, not "fits".** The C request is the whole allocation, `pCache->szAlloc = szPage + szExtra + ROUND8(sizeof(PgHdr1))` (`pcache1.c:787`, passed at 449), and a slot is addressed by pointer, so any slot at least that large serves it and a slot of exactly the page size does not. A Go page buffer is a slice whose length is the page size: the slab serves a page size equal to its slot size, and serving a larger slot would mean cutting it down on `Get` and restoring its full extent on `Put`. A slab with larger slots than the page size is unused in Go. In C it is used only when its slot, rounded down to a multiple of 8 (`sz = ROUNDDOWN8(sz);`, `pcache1.c:276`), holds the whole `szAlloc`; a slot larger than the page but smaller than that is unused in C too.
- **Resolved per database.** `btree.Open` sets `useSlab` only when the database opted in and the slab serves its page size; the page size cannot change after that, since the file's must match the process's buffer pool. A database the slab does not serve runs as one that did not opt in: its caches make their initial bulk allocation and return buffers without the slab's lock. In C a configured buffer turns the bulk allocation off for every cache (`pcache1Init` sets `nInitPage` only when `pPage==0`, `pcache1.c:730-737`), whether its slots fit or not. In a Go process every database has one page size, so a slab that does not serve it serves none.
- **No overflow counter.** The pool-overflow status counters C updates on the fallback (`SQLITE_STATUS_PAGECACHE_OVERFLOW`, `pcache1.c:365-369`) have no counterpart: `nOverflow` counts only exhaustion of a slab that does serve the size.

Pinned by `TestOpen_UsesSlabOnlyWhenItServesThePageSize`.

<a id="drift-2026-06-25-24-truncate-always-full-scans-all-hash-buckets-sqlite-uses-imaxkey-to-sca"></a>
### Drift: Truncate Always Full-Scans All Hash Buckets; SQLite Uses iMaxKey To Scan Only Affected Slots
- **Category:** performance  -  **Severity:** low  -  **Status:** RESOLVED 2026-10-10: `pcache` tracks `maxKey` (`PCache1.iMaxKey`), raised by `hashInsert` and lowered by `truncate`; a truncate at or above it returns at once (`pcache1Truncate`, `pcache1.c:1157`), and one whose span `maxKey - maxPage` is at most `nHash` walks only the slots from `(maxPage+1) & (nHash-1)` to `maxKey & (nHash-1)`, as `pcache1TruncateUnsafe` does (`pcache1.c:653-666`). The commit-time truncate at the database size is the no-op case whenever the transaction freed no tail page. Regression test: `TestPcacheTruncateMaxKeyWindow`.
- **Affected functions:** `pcache.go:*pcache.truncate` (`internal/btree/pcache.go:727-780`).

Go's `truncate` unconditionally iterates every bucket in the hash table — `for bi := range pc.apHash` — and walks each chain in place to drop pages with `pgno > maxPage` (`internal/btree/pcache.go:714-747`), so the cost is always O(nHash) regardless of how many pages are actually removed. SQLite's `pcache1TruncateUnsafe` first compares the highest cached key against the limit: when `pCache->iMaxKey - iLimit < pCache->nHash` it concludes it is "just shaving the last few pages off the end of the cache" and scans only the slots from `iLimit % nHash` to `iMaxKey % nHash`, falling back to a full-table scan only in the general case where many pages are removed (`pcache1.c:653-666`). Because Go tracks no `iMaxKey`-equivalent, it cannot take the shave-the-tail shortcut and always pays for the whole table. The consequence is that every truncate (DB shrink, savepoint rollback, or commit-time size reduction) costs O(nHash) — up to roughly 8K bucket walks for a 5000-page cache — even when only a couple of trailing pages are dropped, which is purely a throughput cost with no correctness impact.

<a id="drift-2026-06-25-26-shm-fcntl-lock-maps-non-transient-errnos-to-a-hard-error-but-unixshmsy"></a>
### Drift: SHM fcntl lock maps non-transient errnos to a hard error, but unixShmSystemLock collapses every fcntl failure to BUSY
- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `shm_mmap.go:*mmapShm.fcntlLock` (`internal/btree/shm_mmap.go:311-316`).

any-store's shm fcntl wrapper allowlists only a transient errno set — `EACCES, EAGAIN, ETIMEDOUT, EBUSY, EINTR, ENOLCK` map to `ErrBusy`, and every other errno returns a wrapped hard error (`internal/btree/shm_mmap.go:311-316`). The comment claims it mirrors `sqliteErrorFromPosixError` (`os_unix.c:1024-1038`), but that helper governs the DB-file lock path; the actually-ported shm path `unixShmSystemLock` (`os_unix.c:4751-4756`) returns `SQLITE_BUSY` for any fcntl failure so the WAL busy-handler retries. The consequence is that a shm lock failing with an errno outside the Go allowlist (e.g. `EPERM`, `EINVAL`, `ENOTSUP`/`EOPNOTSUPP` on filesystems lacking POSIX locks, `EDEADLK`) aborts the WAL read/checkpoint/recovery operation with a hard error where SQLite would spin its busy-handler.

<a id="drift-2026-06-25-27-region-create-false-never-maps-an-existing-on-disk-region-and-returns-"></a>
### Drift: region(create=false) never maps an existing on-disk region and returns an error instead of (nil, OK)
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `shm_mmap.go:*mmapShm.region` (`internal/btree/shm_mmap.go:115-125`).

`mmapShm.region` returns an already-mapped region (`shm_mmap.go:120-121`) but, when `create=false` and the region is not yet mapped in this process, returns an error (`shm_mmap.go:124-125`) instead of mapping an existing on-disk region or returning `(nil, OK)` for a region past EOF. SQLite's `unixShmMap` (`os_unix.c:5159-5230`) maps any region already present in the shm file even when not extending, and returns `(NULL, SQLITE_OK)` for a not-yet-allocated region. The WAL lookup works around this without extending the file: it checks the file's size and only then maps with `create=true` (`mapSegmentForLookup`, 2026-11-19; until then it passed `create=true` outright, which also extended a short file); the remaining impact is on best-effort callers (`shmClearHash`, `shmCleanupFromFrame`, `shmReadCkptInfo`). The consequence is that a peer process's freshly-extended region is unreachable to a `create=false` caller, and the mapping/return semantics diverge from the ported function.

<a id="drift-2026-06-25-28-recoverlocked-truncates-the-wal-and-discards-committed-frames-on-a-cor"></a>
### Drift: recoverLocked truncates the WAL and discards committed frames on a corrupt/incompatible WAL header instead of preserving it / returning CANTOPEN
- **Category:** error-handling  -  **Severity:** low  -  **Status:** PARTIALLY RESOLVED 2026-07-10 (pre-beta BUG-10): a checksum-valid header with a different version now returns `ErrWALVersion` and recovery refuses to open without touching the WAL, matching SQLite's `SQLITE_CANTOPEN` (`wal.c:1441-1446`). Regression test `TestWALVersionMismatchRefusesOpenWithoutTruncate`. The checksum-fail path still truncates+rewrites (SQLite leaves bytes intact and treats the log as empty) — a deliberate deviation, safe for durability because a checksum-invalid header cannot describe replayable frames; documented in the recoverLocked code comment. Forensic-evidence loss remains the only cost.
- **Affected functions:** `wal.go:*walIndex.recoverLocked` (`internal/btree/wal.go:1860-1865`).

On WAL recovery, when the 32-byte header fails to deserialize (bad magic/version/checksum), `recoverLocked` truncates the WAL file to zero and writes a fresh header (`wal.go:1860-1865`: `w.file.Truncate(0)` then `w.writeHeader()`). SQLite's `walIndexRecover` instead returns `SQLITE_CANTOPEN` on a version mismatch (`wal.c:1441-1446`) and, on a header-checksum failure, leaves the on-disk WAL bytes intact and treats the log as empty (`wal.c:1466`) without overwriting it. The consequence is that a WAL written by an incompatible (e.g. future) any-store version, or one with a transiently-corrupt header, is silently truncated and overwritten — destroying any un-checkpointed committed transactions and all forensic evidence — where SQLite refuses to open or preserves the bytes.

<a id="drift-2026-06-25-29-partial-checkpoint-backfills-latest-in-nbackfill-mxsafeframe-page-vers"></a>
### Drift: Partial checkpoint backfills latest-in-[nBackfill,mxSafeFrame] page version; SQLite skips any page whose absolute-latest frame exceeds mxSafeFrame
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*walIndex.checkpoint` (`internal/btree/wal.go:3209-3220`, `wal.go:3624`).

During a reader-blocked partial checkpoint, any-store backfills the latest frame for each page within `[nBackfill, mxSafeFrame]` (`wal.go:3209-3220`, `wal.go:3624`), writing intermediate page versions to the DB file. SQLite's `walCheckpoint` skips any page whose absolute-latest frame exceeds `mxSafeFrame` (`wal.c:1961-1977`, guard at `wal.c:2293`), since those pages are still shadowed by the WAL for every live reader. The consequence is redundant DB-file writes of intermediate versions on hot pages; no read returns a wrong version (slot-0 DB-direct reads are impossible while `mxSafeFrame < mxFrame`, and crash replay heals the file), so this is extra I/O only.

<a id="drift-2026-06-25-30-b-tree-mutations-open-no-implicit-per-statement-savepoint-a-mid-balanc"></a>
### Drift: B-tree mutations open no implicit per-statement savepoint; a mid-balance failure leaves the writer cache structurally corrupt and silently committable
- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `db.go:*WriteTx.Put` / `*WriteTx.Delete` (`internal/btree/db.go:1881-1896`), `btree.go:1196-1243`, `balance.go:600-703`.

SQLite wraps every multi-page row mutation in an implicit anonymous statement-level savepoint — the VDBE calls `sqlite3BtreeBeginStmt -> sqlite3PagerOpenSavepoint` (`btree.c:4583-4600`) before INSERT/DELETE, so a part-way failure in `balance_deeper`/`balance_quick`/`balance_nonroot` (`btree.c:9143-9213`) is undone by `sqlite3VdbeCloseStatement(SAVEPOINT_ROLLBACK)`, restoring the exact pre-statement page image while the rest of the transaction stays committable. The Go port has the rollback machinery (`pager.savepoint`; `pagerError` purges the writer cache and rolls back spilled WAL frames, `pager.go:2498-2527`) but never opens an implicit per-statement savepoint around `Put`/`Delete` (`db.go:1881-1896`, `btree.go:1196-1243`, `btree.go:2074-2127`, `balance.go:600-703`). The consequence is that a write failing mid-balance (an I/O error reading a not-yet-cached sibling/freelist-trunk page, or `ErrCorrupt` on an already-slightly-corrupt page) is not atomic: it can amplify one bad page into broad structural corruption or silently drop a sub-tree's keys, and because the pager is not poisoned and the tx is not invalidated, that corrupt state is committable.

<a id="drift-2026-06-25-31-wal-exclusive-mode-skip-rationale-is-wrong-heap-memory-wal-is-implemen"></a>
### Drift: wal_exclusive_mode skip rationale is wrong: heap-memory WAL IS implemented (Options.InProcess) and is the forced mode on Windows/wasm/InMemory
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `docs/btree/mappings/sqlite_skip.json` group `wal_exclusive_mode`; `db.go:85-93`, `wal.go:543-552`.

The function-coverage map `docs/btree/mappings/sqlite_skip.json` places `sqlite3WalHeapMemory` + `sqlite3WalExclusiveMode` in group `wal_exclusive_mode` with the rationale that "any-store always runs in shared WAL mode with file-backed shm." That clause is false: `Options.InProcess` (`db.go:85-93`) selects heap-backed shared memory for the WAL index ("Equivalent to SQLite's PRAGMA locking_mode=EXCLUSIVE (WAL_HEAPMEMORY_MODE): SHM locks become no-ops ... and no .db-wal-shm file is created"), and the `walIndex.inProcess` flag (`wal.go:543-552`) is the forced mode on Windows/wasm and for `InMemory` DBs. `sqlite3WalHeapMemory`'s capability (`wal.c:4492`) is therefore ported and load-bearing; only the runtime `locking_mode` toggle (`sqlite3WalExclusiveMode`, `wal.c:4448`) is genuinely unported. The consequence is that the skip rationale misleads a maintainer auditing WAL coverage into believing any-store always uses file-backed cross-process shm, when on several platforms it uses single-process heap shm with no shm file and no SHM locks.

<a id="drift-2026-06-25-34-pageheader-deserialize-omits-btreeinitpage-corruption-validation-inval"></a>
### Drift: pageHeader.deserialize omits btreeInitPage corruption validation (invalid page-type byte and nCell over MX_CELL upper bound)
- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `page.go:*pageHeader.deserialize` (`internal/btree/page.go:325-334`).

`pageHeader.deserialize` reads the type byte and cell count with no validation (`page.go:325-334`): an out-of-set `pageType` (e.g. 7, 1, 3) makes both `isInterior()` and `isLeaf()` return false (`page.go:303-310`), `headerSize()` returns 8 (`page.go:295-299`), and the page is parsed as a leaf-index page on garbage geometry; an impossible `cellCount` is likewise accepted. SQLite's `btreeInitPage`/`decodeFlags` reject both at the page boundary — an unknown flag byte -> `SQLITE_CORRUPT_PAGE` (`btree.c:2052-2057`, `btree.c:2076-2081`) and `nCell > MX_CELL` -> `SQLITE_CORRUPT_PAGE` (`btree.c:2228-2232`, `btree.c:2242-2246`). The consequence is that header corruption is masked at read time and surfaces only lazily/indirectly via downstream bounds checks (`getCellOffsetSafe`'s `base+2 > len(data)`, `contentAreaOffset`'s `top < gap`) when those paths run — or not at all — instead of being caught as `ErrCorrupt` on every page init.

<a id="drift-2026-06-25-33-walindexhdr-bigendcksum-is-never-populated-stays-0-even-though-the-wal"></a>
### Drift: WalIndexHdr.bigEndCksum is never populated (stays 0) even though the WAL frame checksums are big-endian — same struct-fidelity gap as the documented szPage drift
- **Category:** format  -  **Severity:** none
- **Affected functions:** `wal.go:*walIndex.writeHeader` (`internal/btree/wal.go:841-866`), struct field at `wal.go:409`.

The Go `WalIndexHdr` mirrors SQLite's struct and reserves byte 13 as `bigEndCksum` (`wal.go:409`, serialized at `wal.go:424-425`), but no code path assigns it a non-zero value: `walIndex.writeHeader` sets only `isInit`/`iVersion`/`mxFrame`/`nPage`/`iChange`/`aFrameCksum`/`aSalt` (`wal.go:841-866`), leaving `bigEndCksum = 0`. Meanwhile the WAL uses big-endian frame checksums — `walMagic = 0x42540601` has the low (big-endian) bit set (`wal.go:76`) and `walChecksum` byte-reverses words to big-endian (`wal.go:331-369`). SQLite sets `pWal->hdr.bigEndCksum = magic&1` on recovery (`wal.c:1448`). The consequence is none functionally — the Go always computes checksums big-endian regardless of this field, never reads it back, and the wal-index is process-local — but the SHM header field disagrees with the actual checksum endianness; documenting it completes the `WalIndexHdr` field-fidelity picture alongside the `szPage` drift ([drift-90](#drift-90-wal-index-szpage-field-not-encoded-or-decoded)).


<a id="drift-2026-07-11-renamenamespace-delete-put-vs-sqlite-master-row-update"></a>
### Drift: RenameNamespace Re-Keys The Master Cell (Delete+Put) Where SQLite ALTER TABLE RENAME Updates The sqlite_master Row In Place
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:DB.RenameNamespace` / `db.go:WriteTx.RenameNamespace` (`internal/btree/db.go`, after `DeleteNamespace`).

SQLite's ALTER TABLE ... RENAME is an UPDATE of the existing `sqlite_master` row's `name` column: the row (and the table's `rootpage`) stays put, the statement rewrites SQL text that references the old name (triggers, views, FK clauses — `alter.c:renameTableFunc`), and the schema cookie is bumped by the statement itself. The Go master table (page 1) is keyed BY the namespace name with the root page as the 4-byte value, so a rename is necessarily a re-key: `Delete(oldName)` + `Put(newName, rootPgBuf)` inside the same write tx — the entry is transiently absent within the tx but atomic at commit, and the root page (hence every `*Namespace` handle addressing the tree by root) is untouched. There is no SQL layer, so no stored-text rewriting exists to port. Divergently from the SQLite statement (but consistently with this package's `CreateNamespace`/`DeleteNamespace`), `RenameNamespace` does NOT bump the schema cookie — the caller must invoke `tx.MarkSchemaChanged()`, and the any-store layer's `renameCollection` does, which is what lets cross-process peers detect the rename via `IsSchemaStale`. Error surface: `ErrNamespaceNotFound` (old absent), `ErrNamespaceExists` (new taken), `ErrTxClosed`; a failure between the Delete and the Put dooms the tx (caller must roll back), the same contract as a `CreateNamespace` failure after page allocation. The page-1 spill exclusion (see the pagerStress page-1 guard notes) applies to the master-table dirtying here exactly as it does for create/delete. Regression tests: `internal/btree/namespace_rename_test.go` (happy path, interior master table, rollback/savepoint, delete interplay, freelist accounting).

<a id="drift-2026-11-05-leafkeyat-serves-a-local-key-from-a-value-spilling-cell"></a>
### Drift: leafKeyAt Serves A Fully Local Key From A Cell Whose Value Spills, Where indexCellCompare Declines Any Overflowing Record
- **Category:** changed-logic  -  **Severity:** none
- **Affected functions:** `btree.go:leafKeyAt`; callers `btree.go:*Cursor.SeekNear`, `btree.go:*Cursor.currentKeyEqual`.

SQLite's `indexCellCompare` (`btree.c:5952-5982`) compares a record in place only when its size varint is one or two bytes and the whole record fits on the page; a record that extends into overflow pages returns 99, and the already-positioned shortcut of `sqlite3BtreeIndexMoveto` (`btree.c:6060-6081`) falls back to `moveToRoot`. An index record has no separate value, so "the record spills" and "the key spills" coincide there. The Go leaf cell carries `key||value`, and a document cell with a short id and a large body spills while its key is wholly on the page. `leafKeyAt` returns such a key from the page and declines (`errLeafKeySpilled`) only when the key itself continues in the overflow chain, i.e. when `keyLen > nLocal` under the same `pager.leafLimits`/`localPayloadSize` split that every leaf writer uses (in a schema format 6 file a key of up to `usableSize-35` bytes is local), so a key it serves is always complete. It also accepts length varints of any width, capped by `maxPayloadAlloc`. The consequence is that the in-page fast path of `SeekNear` and `currentKeyEqual` stays available for `Iterator.Doc()`'s re-seek by id on overflowing documents; a literal port would send every such lookup through `leafFullKey` and the overflow chain. Regression tests: `internal/btree/leafkeyat_overflow_test.go` (`TestLeafKeyAt_SpilledKey` serves the local key of a value-spilling cell and refuses spilled keys; `TestCursor_SeekNearSpilledKeys/local-first-and-last-keys` runs the in-leaf check over spilled cells).

<a id="drift-2026-11-10-1-header-format-gate"></a>
### Drift: The Header Format Gate Uses A Second Magic, And A Newer Write Version Refuses The Open
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `page.go:checkHeaderFormat`, `page.go:*dbHeader.deserialize`, `page.go:*dbHeader.serialize`, `pager.go:*pager.open` (the WAL copy of page 1), `db.go:readInitialHeader`.

SQLite never changes its magic. A file from a later release is told apart by the version bytes, which `lockBtree` checks on every read of page 1 (`btree.c:3322-3327`: a write version above 2 makes the connection read-only, a read version above 2 is `SQLITE_NOTADB`), and by the schema file format, which `sqlite3InitOne` bounds with "unsupported file format" (`prepare.c:344`). any-store wrote both version bytes as 2 and the schema format as 5 but checked only the format's lower bound, so releases up to v2.4.3 open a file of any later format and parse it under their own rules. `checkHeaderFormat` now applies all three bounds before any other header field is trusted, and reports a value above the maximum as `ErrNewerFormat`, which is not under `ErrCorrupt`: the file is intact and a caller that discards a corrupt database must keep it.

Two things differ from SQLite. First, the bounds cannot protect a file from the releases that never checked them, so schema formats 6 and later are written under a second magic, `"any-store v2 fmt"` (v2.5.0 is the first release to create such files). It fills all 16 bytes, and those releases compare the whole field, so they refuse the file at the header (as a corrupt file: `ErrHeaderCorrupt` from v2.4.3, plain `ErrCorrupt` before it) before reading a page of it. The magic changes this once: later formats raise the schema format only. The magic and the schema format must agree (the second magic with a format below 6, or a zero-padded magic with format 6, is `ErrHeaderCorrupt`; a format above the newest known is `ErrNewerFormat` under either magic, because that check runs first). `serialize` writes back the family of the magic the header was read with (an older `"BTree format 1"` becomes `"any-store v2"`), so no commit moves a file between the two families; a backup refuses a destination of the other family for the same reason (see [its entry](#drift-2026-11-11-8-backup-refuses-a-destination-of-another-schema-format)); and the copy of page 1 that `pager.open` reads from the WAL must name the same family as the file. Second, a write version above the maximum refuses the open instead of opening read-only: the btree has no read-only open.

The checks live in `dbHeader.deserialize`, whose first caller on the open path is `readInitialHeader`, before the file is locked or its WAL and wal-index are touched; a refused open leaves all three files as they were. A newer format found only in the WAL's copy of page 1 is refused after WAL recovery has run, which may have rewritten the wal-index. A handle that is already open reloads the header after another process commits (`refreshHeaderFromPage1`); there every check of `dbHeader.deserialize`, `ErrNewerFormat` included, and a change of leaf rule (`checkRefreshedHeader`) fail the transaction, as SQLite re-runs all of `lockBtree` on every such read. Regression tests: `internal/btree/format_gate_test.go` (`TestCheckHeaderFormat`, `TestOpen_RefusedFormatLeavesFilesUntouched`, `TestHeaderSerialize_KeepsMagicFamily`, `TestOpen_FirstMagicStillOpensAndIsRewritten`), `corrupt_open_test.go:TestOpen_NewerFormat`.

<a id="drift-2026-11-10-2-leaf-insert-fit-check-counts-the-cell-pointer-twice"></a>
### Drift: The Leaf Insert Fit Check Counts The New Cell's Pointer Twice
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.insertIntoLeafWithPath`, `btree.go:*btree.insertSepIntoInterior`, `btree.go:*btree.splitRootLeafAndInsert`.

SQLite's `insertCellFast` sends a cell to `balance()` when `sz+2 > pPage->nFree` (`btree.c:7433`): the cell and its two-byte pointer must fit the free space. `insertIntoLeafWithPath` computes the gap as the content start minus the header and the pointer array *including the new cell's slot*, then asks for `cellSize+2` of it, so the pointer is counted twice and a cell that fits with less than two bytes to spare is treated as over-full. On a non-root leaf this only means a balance that redistributes cells that would have fit. On a root leaf the cell arrives at `splitRootLeafAndInsert`, where the child made by `balanceDeeper` then holds every cell on one page; balance-shallower (`btree.c:8942-8967`) copies it back into the root, as SQLite does for a root left with no cells, and the tree keeps its depth. `insertSepIntoInterior` makes the same double count for a divider, so a divider that fits its parent exactly is treated as over-full and the parent is balanced or split. Fixing the checks would change which inserts split and is left for a change of its own. Regression tests: `root_leaf_balance_test.go` (`TestRootLeaf_ExactFitCollapsesBack`, `TestRootLeaf_Page1ExactFitCollapsesBack`, `TestRootLeaf_Page1ChildDoesNotFitBack`).

<a id="drift-2026-11-10-3-leaf-payload-limits-come-from-the-pager-not-the-page"></a>
### Drift: A Leaf Cell's Local-Payload Limits Come From The Pager, Not From The Page
- **Category:** changed-logic  -  **Severity:** none
- **Affected functions:** `page.go:payloadLimits`, `page.go:indexLimits`, `page.go:localPayloadSize`, `pager.go:*pager.leafLimits`, `pager.go:*pager.leafLimitsFor`, every leaf-cell parser and writer in `btree.go`.

SQLite's `decodeFlags` copies `maxLocal`/`minLocal` into each `MemPage` from `BtShared`, choosing the index pair or the table-leaf pair by the page's flag byte (`btree.c:2043-2086`), and `btreePayloadToLocal` reads them from the page (`btree.c:1213-1226`). any-store has one leaf page type, so the pair is not chosen per page: leaf code takes a `payloadLimits` from `pager.leafLimits`, which returns the table-leaf pair for a schema format 6 file and the index pair for a format 5 one, and interior code uses `indexLimits`. `TestLeafCodeTakesLimitsFromPager` fails when production code outside the interior-cell functions calls `maxLocalPayload`, `minLocalPayload` or `indexLimits` directly.

<a id="drift-2026-11-11-7-parent-rewrite-after-a-balance-splits-in-place"></a>
### Drift: The Parent Rewrite After A Balance Splits The Parent In Place Instead Of Balancing It
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `balance.go:*btree.rewriteParentAfterBalance`, `balance.go:*btree.splitParentThreeWays`, `btree.go:interiorSplitPointFitting`, `btree.go:*btree.pathToChild`.

SQLite's `balance_nonroot` inserts each new divider into the parent with `insertCell` (`btree.c:8870`); a divider that does not fit becomes an overflow cell of the parent, and the `balance()` loop then balances the parent itself (`btree.c:9213`, `9232-9236`): against its own siblings with `balance_nonroot`, or through `balance_deeper` when it is the root. A parent that overflows is therefore redistributed over up to three siblings and as many pages as it needs. any-store rebuilds the parent wholesale from the spliced child and divider lists (`balance.go` header, deviation 2) and, when the list does not fit, splits that one page in place and passes the promoted divider up with `insertSepIntoAncestor` (deviation 6): its siblings are not gathered, so the fill of interior pages differs from SQLite's. The header's "semantically identical" describes the cascade order, not the result.

The split index is chosen so that both halves fit (`interiorSplitPointFitting`), because the new dividers can be much longer than the ones they replace. Two pages are enough unless the new dividers alone exceed a page: a balance writes at most `nbMaxOut` pages, hence at most four new dividers, and four interior cells of the largest size take slightly more than a page once the usable size passes about 10 KiB (`maxLocal` is `(usable-12)*64/255-23`, a little over a quarter). Then `splitParentThreeWays` writes three pages and promotes two dividers. The first goes up the path as usual; that can rewrite the pages above, so the parent of the middle page is found again from the root by key (`pathToChild`) before the second is inserted. Regression tests: `root_leaf_balance_test.go` (`TestInteriorSplitPointFitting_LongTail`, `TestRewriteParentAfterBalance_ThreeWaySplit`).

<a id="drift-2026-11-11-8-backup-refuses-a-destination-of-another-schema-format"></a>
### Drift: A Backup Refuses A Destination Of Another Schema Format
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `backup.go:*DB.BackupInit`, `db.go:Open` (`Options.SchemaFormat`), root `db.go:*db.Backup`.

`sqlite3_backup` copies every page of the source, page 1 included, so the destination becomes a file of the source's format whatever it was before; the next read of page 1 by any connection re-runs `lockBtree` and each page carries its own payload rule in its flag byte (`btree.c:2028-2085`). In any-store the leaf payload rule is held once per open file (`pager.leafLimits`), fixed at open. Replacing a format 6 destination with a format 5 source under it would leave other handles, and read transactions opened before the backup committed, parsing the new pages by the old rule, and a crash before the next checkpoint would leave a database file and a WAL whose headers name different magics. `BackupInit` therefore returns `ErrBackupFormatMismatch` when the two files' leaf rules differ, and a destination is created in the source's format with `Options.SchemaFormat` (the root package's `Backup` does this), so a backup of a database created before v2.5.0 is a schema format 5 file. With that, the format of a file never changes after creation. Regression tests: `table_leaf_format_test.go` (`TestBackup_SchemaFormats`, `TestOpen_SchemaFormatOption`), root `format_test.go` (`TestBackup_KeepsSourceFormat`).

<a id="drift-2026-11-11-9-interior-collection-rewrites-overflowing-divider-chains"></a>
### Drift: Collecting An Interior Page Frees And Rewrites The Overflow Chains Of Its Dividers
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.collectInteriorCellsKeepBuf`, `btree.go:*btree.rebuildInteriorPage`.

`balance_nonroot` moves cells as raw bytes: a cell whose payload overflows keeps its 4-byte pointer and its chain is neither read nor written (`btree.c:8447-8493`, `rebuildPage` at `7629`). any-store does the same for leaf cells (`collectLeafCells` passes them through as `rawCell`), but an interior cell whose key overflows is materialised: `collectInteriorCellsKeepBuf` reads the full key from the chain and frees the chain, and `rebuildInteriorPage` writes a new one. Rewriting a parent that holds an unchanged divider longer than the index limit therefore reads and reallocates its overflow pages, and can fail on a read SQLite would not make. The result on disk is equivalent. A read, parse or free failure is returned rather than replaced by an empty divider.

<a id="drift-2026-11-11-4-root-collapse-tests-the-contiguous-gap-only"></a>
### Drift: The Root Collapse On Page 1 Tests The Child's Contiguous Gap, Not Its Free Space
- **Category:** changed-logic  -  **Severity:** none
- **Affected functions:** `btree.go:*btree.collapseSingleChild`.

Balance-shallower copies the only child back into a root with no cells when `pParent->hdrOffset <= apNew[0]->nFree` (`btree.c:8942`), after `defragmentPage` (`btree.c:8959-8967`); `nFree` counts freeblocks and fragment bytes. `collapseSingleChild` compares `dbHeaderSize` with the gap between the child's pointer array and its content area, and does not defragment: a child with a 90-byte gap and 20 fragment bytes is left under a zero-cell page 1 where SQLite would collapse it. The tree is one level deeper until a later balance rebuilds the child packed. The copy itself keeps cell content at the same offsets, so it is exact whenever it is made.

<a id="drift-2026-11-11-6-backup-reloads-the-header-from-the-copied-page-1"></a>
### Drift: Backup Finalize Reloads The In-Memory Header From The Copied Page 1
- **Category:** changed-logic  -  **Severity:** none
- **Affected functions:** `backup.go:*Backup.finalize`.

`sqlite3_backup` copies page 1 like any other page and then patches fields in the destination's page image (the schema cookie through `sqlite3BtreeUpdateMeta`, the version bytes through `sqlite3BtreeSetVersion`; `backup.c:420-431`); there is no header kept apart from the page. any-store's pager keeps a parsed `dbHeader` and writes it over page 1 at every commit, so `finalize` deserialises the copied page 1 into the destination's header, bumps the schema cookie there, and lets the commit serialise it back. The header that reaches disk is the source's, field for field, except for the cookie, the page count, the KDF salt, which stays the one the destination's codec was keyed with ([drift-2026-11-21-19](#drift-2026-11-21-19-the-kdf-salt-belongs-to-the-key-setup-not-to-page-1)), and, in a schema format 6 file, the change counter and `VersionValidFor` (offsets 24 and 92), which the commit sets from the destination's own `dbFileVers`, as SQLite's `pager_write_changecounter` does (`pager.c:3084-3097`, called at `pager.c:3218`); a field `dbHeader` does not model would not survive, and there is none today. The magic family and schema format are the source's as well, which is why the destination must already have them (see [the backup format entry](#drift-2026-11-11-8-backup-refuses-a-destination-of-another-schema-format)).

<a id="drift-2026-11-11-1-attached-wal-index-recovery"></a>
### Drift: Recovery Of A Torn wal-index Header With Peers Attached Runs On The Process-Shared `*wal`
- **Category:** structural  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.indexReadHdr`, `wal.go:*wal.tryBeginReadMultiProcessHdr`, `wal.go:*wal.checkpointWithMode`, `wal.go:*wal.authoritativeMxFrame`, `pager.go:*pager.close`.

SQLite's `walIndexReadHdr` (`wal.c:2654-2759`) reads the wal-index header without a lock; when it is invalid it takes `WAL_WRITE_LOCK`, reads again, and if the header is still invalid runs `walIndexRecover` (`wal.c:2694-2729`). Every read transaction goes through it (`walTryBeginRead`, `wal.c:3076`), and so does every checkpoint before it copies a frame (`sqlite3WalCheckpoint`, `wal.c:4373`). A header left torn by a writer that died between its two header copies is therefore repaired by the next connection that looks at it.

Until 2026-11-11 the Go port did neither. A reader that found the header invalid only fenced on `lockRecover` and retried, ending in `ErrProtocol` after the retry budget (about 10 s); a write begin went through the same read path. A checkpoint took its frontier from `authoritativeMxFrame` / `authoritativeMxFrameAndPage`, which fell back to the process-local frame count on an invalid header: a surviving process that had not read since a peer's last commits backfilled only through its own stale count, judged the backfill complete, and a RESTART/TRUNCATE (including the close-time checkpoint) then discarded the peer's acknowledged frames.

`wal.indexReadHdr(haveWriteLock, haveCkptLock)` is now the port of `walIndexReadHdr`, called from `tryBeginReadMultiProcessHdr` and at the top of `checkpointWithMode` (whose frontier `nf`/`mxPage` is the header it returns, as `walCheckpoint` works from `pWal->hdr`). A busy lock yields `ErrBusy`: the reader probes `lockRecover` and retries, or reports `ErrBusyRecovery` when the probe is busy (`wal.c:3084-3108`); the checkpoint returns it (`SQLITE_BUSY`). `pager.close` holds the write lock around its close-time checkpoint, so it calls `indexReadHdr(true, false)` itself first (the `pWal->writeLock` branch, `wal.c:2706-2711`); otherwise the PASSIVE checkpoint inside could not take the lock for the repair. `authoritativeMxFrame` no longer substitutes the local count in multi-process mode: an unreadable header reads as `mxFrameUnknown` (larger than any frame), so every "whole log backfilled" test fails closed. `authoritativeMxFrameAndPage` is gone.

What still differs from SQLite, and why:

1. **Process-shared state.** A SQLite `Wal` belongs to one connection, and recovery rewrites that connection's `pWal->hdr`. One Go `*wal` serves every transaction of the process, so `recoverLocked` runs under `w.mu` and resets the writer-owned fields (`nFrame`, `cksum1/2`, salts, the local index atomics). It only does so while `indexReadHdr` holds `lockWrite`, `lockCheckpoint` and `lockRecover` exclusively; the in-process lock table makes that exclusive against this process's own writer and checkpointer as well as against peers, so no user of those fields is active. In-flight readers of the process are unaffected for SQLite's reasons: hash tables are rebuilt privately and copied over identical content for every frame a live snapshot can reach, and a read-mark whose slot is held is left alone.
2. **`pChanged`** (ported 2026-11-13). After recovery SQLite sets `*pChanged = 1` (`wal.c:2724-2725`), which resets the pager (`pager.c:3261-3263`), and a checkpoint that loaded a new header zeroes `pWal->hdr` (`wal.c:4399-4407`). Here the rebuilt header is published through `wal.publishHeader` as a recovery, which always raises the change epoch, and a reset over a header the process had not seen raises it too. Reader caches are still keyed by the full header plus `dataVersion`, and the writer still compares the live header with `writerHdr` at write begin.
3. **Where the busy handler runs for `ErrBusyRecovery`.** The mapping itself is SQLite's (`wal.c:3084-3108`): a busy lock with the `WAL_RECOVER_LOCK` probe free is a retry, a busy probe is `ErrBusyRecovery` (`SQLITE_BUSY_RECOVERY`), any other probe error is returned. SQLite invokes the busy handler for it in `sqlite3BtreeBeginTrans`, from the one loop around the whole begin, with one count (`btree.c:3730-3737`; `sqlite3InvokeBusyHandler`, `main.c:1770-1780`, never asks again once the handler has declined). Go has several begin entry points (read, write, backup step, integrity sweep, and open's read of page 1, which is below the pager), so a read begin's loop sits at the WAL read begin, `wal.beginReadHdrBusy`, with the begin's count in a `beginBusy`. `DB.BeginWriteContext` is a write begin's one loop: it makes single read attempts (`beginReadHdrBusy(nil)`), tries the write lock once per attempt (`beginWriteWithSnapshot`, as `walBeginWriteTransaction` does, `wal.c:3714-3717`), and retries `ErrBusyRecovery`, a busy write lock (`ErrBusy`) and, after its tight retries, `ErrBusySnapshot` under one `beginBusy`, each retry from a fresh read snapshot. Until 2026-11-12 there were three counts: the pager's recovery loop and `BeginWriteContext`'s loop each had one, and the inner loop restarted from 0 on every outer retry (with a recovery that did not finish, the default 5 s budget became about 300 s, the process's writer reservation held throughout); and `beginWriteWithSnapshot` waited for the write lock through `walBusyLock` with a count of its own, so a peer that committed just before each timeout made every snapshot retry start a fresh budget (about 60 retries, 5 minutes, at the default). `walBusyLock`'s per-call count (drift-108) is now left only to the checkpoint's, open's and close's lock waits. Tests: `TestTornWalIndexHeader_BusyRecoveryOneBudgetPerBegin`, `_WALReadBeginWaitsOutRecovery`, `TestBeginWrite_PeerCommitsBeforeTimeoutOneBudget`.
4. **`mxFrameUnknown`.** SQLite needs no such value: after `walIndexReadHdr` it works from its private `pWal->hdr`, and the one live re-read (`wal.c:2322`) is unvalidated. The Go callers outside `checkpointWithMode`'s frontier (`checkpointPassive`, `checkpointPost`, the DB-file truncate guard, `tryCheckpoint`) re-read the shared header and can catch a peer between its two copies; treating that as "more frames may exist" costs at most a skipped reset or truncate.

Three further rules of the same SQLite routines were ported with it (2026-11-11), none of them a divergence any more:

- **A read error during the recovery scan is an error.** `recoverLocked` used to `break` on a failed frame read and publish the prefix it had as the whole log; with recovery now reachable from attached readers and checkpointers, a transient I/O error would have presented an earlier commit as the end of the log and let the next TRUNCATE delete the commits behind it (open-time recovery had the same defect). `walIndexRecover` keeps the read's `rc` (`wal.c:1502-1503`) and publishes only under `rc==SQLITE_OK` (`wal.c:1561-1566`); `recoverLocked` now returns the error with nothing published. Tests (`-tags vfs`): `TestRecoverReadErrorNotEndOfLog_AttachedRecovery`, `_Open`.
- **Reset decisions read the shared `nBackfill`.** `checkpointPost`, `checkpointPassive`'s completeness result (which gates the close-time WAL truncate) and `tryCheckpoint`'s restart test compared the process-local `walIndex.nBackfill` mirror with the shared frontier. The mirror is refreshed only at some begins, so after a peer's WAL restart it can hold the previous generation's larger count; `checkpointPost` is reached without any backfill when reader slot 0 is busy, and then reset a log none of which had been copied. `walCheckpoint` tests `pInfo->nBackfill` in shared memory (`wal.c:2355`); the three sites now go through `walIndex.liveNBackfill`. Test: `TestCheckpointResetUsesSharedBackfill`.
- **A wal-index of another version is refused, not rebuilt.** `readHeader` reported a consistent, checksummed header with a foreign `iVersion` as invalid, which `indexReadHdr` would have "repaired" over another build's live index. `walIndexReadHdr` returns `SQLITE_CANTOPEN` for it without recovery (`wal.c:2740-2741`); `readHeaderState` separates the case and `indexReadHdr` returns an error matching `ErrWALVersion`. `wal.open` refuses it the same way, under its recovery locks and before it rebuilds anything: a process attaching to a database a newer build keeps open must not overwrite that build's index. On a fresh attach `region(create=false)` maps none of the peer's index ([drift-2026-07-10-3](#drift-2026-07-10-3-region-create-false-never-maps-existing-content)), so the gate maps region 0 first, as `walIndexReadHdr` does through `walIndexPage`; a consistent header of this version still goes to recovery, as before. Tests: `TestWalIndexFutureVersionRefused`, `TestWalIndexFutureVersionRefusedAtOpen` (two processes).

Regression tests (`internal/btree/walindex_torn_header_test.go`): `TestTornWalIndexHeader_CheckpointKeepsPeerCommits`, `_PassiveCheckpointKeepsPeerCommits`, `_CloseKeepsPeerCommits`, `_ReaderRecovers`, `_WriterRecovers` (two processes: the writer dies between the header copies), `_ReaderRetriesWhileWriteLockBusy`, `_BusyRecoveryWhenRecoverLockHeld`, `_CheckpointBusyWhenWriteLockHeld`.

<a id="drift-2026-11-24-1-put-refuses-a-record-over-the-record-size-limit"></a>
### Drift: Put Refuses A Record Over The Record Size Limit; SQLite Tests Its Length Limit Above The B-Tree
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.Put`, `btree.go:recordSizeErr`, `page.go:payloadFits`; `db.go:*DB.CreateNamespace`, `db.go:*DB.RenameNamespace` (the master-table record).

SQLite's length limit is `SQLITE_LIMIT_LENGTH`, `SQLITE_MAX_LENGTH` by default, 1,000,000,000 (`sqliteLimit.h:24`), and it is tested where a value or a record is built, above the b-tree. `OP_MakeRecord` tests `nByte+nZero>db->aLimit[SQLITE_LIMIT_LENGTH]` (`vdbe.c:3697`) and jumps to `too_big:`, which sets "string or blob too big" and returns `SQLITE_TOOBIG` (`vdbe.c:9298-9300`). That test runs only when the output register must grow to hold the record: it is in the `else` of `if( nByte+nZero<=pOut->szMalloc )` (`vdbe.c:3690`). `sqlite3BtreeInsert` only asserts its lengths (`btree.c:9485`, `9540`).

Here the limit is in the b-tree, and it is the cell parsers' cap ([old-drift-corruption-fixed-1gb-payload-cap-vs-npage-validation](#old-drift-corruption-fixed-1gb-payload-cap-vs-npage-validation)): key length plus value length at most `maxPayloadAlloc`, 2^30 bytes (`payloadFits`). `Put` tests every record against it before it touches a page, and refuses one over it with one of two errors (`recordSizeErr`): `ErrKeyTooLarge` when the key alone is over the limit, `ErrValueTooLarge` otherwise, where SQLite has the one code. `CreateNamespace` and `RenameNamespace` apply the same test to the master-table record (the name and a 4-byte root page number) before they allocate the root or delete the old name. Holding writes to the parsers' cap means `Put` never writes a record its own parse would reject, and the b-tree is the lowest layer every writer here passes through. Only `Put` brings a new record into a tree, and it tests that record at entry, before any path writes it; the rest of what a split or a rebuild writes, under `Put` or `Delete`, comes from cells already in the tree. Tests: `TestPayloadLimit_RealConstants`, `TestPayloadLimit_PutRefusesWhatReadRejects`, `TestPayloadLimit_NamespaceName`.

<a id="drift-2026-11-24-2-a-readers-file-size-bound-takes-the-pagers-page-count"></a>
### Drift: A Reader's File-Size Bound On A Declared Length Takes The Pager's Page Count, Not Page 1's
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `page.go:payloadInFile` (its page count is `pager.go:*pager.readerDbSizeBound`'s).

`sqlite3BtreeMaxRecordSize` is `pCur->pBt->pageSize * (sqlite3_int64)pCur->pBt->nPage` (`btree.c:4978-4982`). `pBt->nPage` is the count `lockBtree` sets at the start of every transaction (`btree.c:3707`, `3448`): page 1's in-header size (offset 28) when it is non-zero and the change counter equals the version-valid-for number, else the pager's count (`btree.c:3294-3298`); a trusted size larger than the pager's count is `SQLITE_CORRUPT_BKPT` (`btree.c:3401-3407`). A write then grows it as it allocates pages (`allocateBtreePage`, `btree.c:6763-6786`).

`payloadInFile` multiplies the page size by `readerDbSizeBound`. For the writer that is `pager.dbSize`, which a write begin sets by the same rule (`refreshHeaderFromPage1` through `lockBtreePageCount`) and `allocatePage` grows: SQLite's count. For a reader it is `pcache.dbSize`, the pager's count at its snapshot (`effectiveReaderDbSize`: the size the WAL's last commit records, else the database file's size), and page 1 is not read for it. In a file any-store writes the two agree: a commit that changes the page count writes page 1 with it (`commitNeedsPage1`). They differ when page 1's size field is damaged, or when the count is the database file's size and the file holds pages past its committed size. A trusted page-1 size below the pager's count then bounds a SQLite reader more tightly than this one, and one above it ends the SQLite transaction at its start with `SQLITE_CORRUPT`, where this reader goes on under the pager's count. Where page 1's size is untrusted, SQLite takes the pager's count too. The bound decides only whether a declared length is refused before a buffer is sized by it; a length within it whose bytes are not in the file still fails on the overflow chain.

The key readers bound a key's length by `payloadInFile` too, `pageSize * nPage`, where SQLite's index probe uses `nCell/usableSize > nPage` (`btree.c:6164`), with the same `nPage`. Both bounds are at least `nPage * usableSize`, more than the file's pages can hold of one payload, so a key whose length falls between them fails on the overflow chain under either. The page numbers a reader follows (`descendChild`, the overflow-chain readers) are bounded by the same pager's count, where SQLite tests `btreePagecount`, that is `pBt->nPage` (`btree.c:2365`, `2386`, `5227`). A namespace's root is the exception: `resolveNamespace` bounds it by `pBt->nPage`'s rule, from the page 1 its search starts on (`page1DatabaseSize`), once per resolution, so a root on a page the file holds past the database is `ErrCorrupt` and not a tree of stale records (test `TestGetNamespace_RootPastPage1sCountIsCorrupt`). Tests: `TestPayloadLimit_DeclaredLengthOverFileSize`, `TestPayloadLimit_OverflowRecordWithinFileSize`.

<a id="drift-2026-11-23-1-full-pathname-follows-no-symbolic-link"></a>
### Drift: The Full Pathname Follows No Symbolic Link And Keeps Its "." And ".." Elements
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `fullpath_unix.go:FullPathname`.

SQLite resolves a database's name once, `rc = sqlite3OsFullPathname(pVfs, zFilename, nPathname, zPathname);` (`pager.c:4792`), builds the `-journal` and `-wal` names from the result (`pager.c:4915`, `4929`) and opens the database by it, `rc = sqlite3OsOpen(pVfs, pPager->zFilename, pPager->fd, vfsFlags, &fout);` (`pager.c:4948`). `FullPathname` is that resolution, with each platform's `xFullPathname`:

- **Unix.** `unixFullPathname` starts a path that is not absolute from the working directory (`if( zPath[0]!='/' )`, then `osGetcwd(zPwd, sizeof(zPwd)-2)`, `os_unix.c:7021-7028`). `FullPathname` returns `cwd + "/" + path` for such a path, with the kernel's working directory (`syscall.Getwd`, as `getcwd`), and an absolute path as given.
- **Windows.** `winFullPathname` calls `osGetFullPathNameW((LPCWSTR)zConverted, nByte, zTemp, 0)` (`os_win.c:6034`); `FullPathname` calls `syscall.FullPath`, which is `GetFullPathNameW`.
- **js/wasm and wasip1.** The name as given, as `kvvfsFullPathname` copies it (`memcpy(zOut, zPath, nPath);`, `os_kv.c:996`).

On Unix two things differ. `unixFullPathname` appends the name element by element: it drops a `.` element, backs up one element at a `..` (`os_unix.c:6930-6939`), and after each element it appends it calls `osLstat` and, for a symbolic link, `osReadlink`, and goes on from the link's target (`os_unix.c:6953-6975`). `FullPathname` follows no link, and so it removes no `.` or `..` either: a `..` after a link names the parent of the link's target, which only the OS resolves. The full name is therefore not SQLite's string, and the OS resolves each `.` and `..` it keeps: a database reached through a link has its `-wal` beside the link, where SQLite puts it beside the target. A name too long for the OS is not measured: it fails when the file is opened by it, where `unixFullPathname` returns `SQLITE_CANTOPEN` for a name that does not fit its buffer (`os_unix.c:6940-6942`, `7030`).

Tests: `TestFullPathname_Unix`, `_UnixWorkingDirectoryOfTheCall` (`fullpath_unix_test.go`), `TestFullPathname_Windows` (`fullpath_windows_test.go`), `TestFullPathname_AsGiven` (`fullpath_other_test.go`).

<a id="drift-2026-11-24-3-open-tells-a-wrong-key-from-a-damaged-page-1"></a>
### Drift: Open Tells A Wrong Key From A Damaged Page 1
- **Category:** added-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.readPage1AtOpen`, `pager.go:*pager.page1Unauthenticated`, `pager.go:*pager.proveKeyAtOpen`, `pager.go:keyProbePgno`, `pager.go:page1HeaderAtOpen`.

SQLCipher cannot tell the two apart and does not try. Its codec decrypts page 1 when `lockBtree` first fetches it (`sqlite3Codec`, `sqlcipher.c:3263-3295`, v4.14.0). In the default header mode the first 16 bytes of page 1 in the buffer are the SQLite magic, put there in place of the salt (`memcpy(ctx->buffer, ctx->plaintext_header_sz ? pData : (void *) SQLITE_FILE_HEADER, offset)`, `sqlcipher.c:3270-3271`). A page whose HMAC does not match is an error of `sqlcipher_page_cipher` (`if(sqlcipher_memcmp(hmac_in, hmac_out, ctx->hmac_sz) != 0)`, `sqlcipher.c:1685-1699`), on which `sqlite3Codec` zeroes the rest of the buffer, sets the codec's error, and copies the buffer back (`sqlcipher.c:3286-3295`). `lockBtree` then finds the magic (`btree.c:3310`), but the payload fractions at bytes 21-23 are zeros and not 64, 32, 32 (`if( memcmp(&page1[21], "\100\040\040",3)!=0 )`, `btree.c:3361-3362`), and it returns the `SQLITE_NOTADB` it set before the checks (`btree.c:3306`). So a wrong key and a page 1 with a broken HMAC both give "file is not a database" (`wont-open-with-invalid-derived-key`, `test/sqlcipher-core.test:139-145`; `hmac-tamper-resistence-first-page`, `test/sqlcipher-integrity.test:44-75`). A broken HMAC on a later page is not one code: the codec returns `SQLITE_ERROR` for it (`sqlcipher_page_cipher`'s `error:` label, `sqlcipher.c:1723-1725`) and records that code as the pager's error (`sqlcipher_codec_ctx_set_error` → `sqlite3pager_error`, `sqlcipher.c:1385-1389`), which the pager's later fetches return (`getPageError`, `pager.c:5710-5721`); the zeroed page the fetch hands back can fail the b-tree's checks first. The cited test of a tree page, `hmac-tamper-resistence` (`test/sqlcipher-integrity.test:123-154`), gets `SQLITE_CORRUPT`, "database disk image is malformed", from its `SELECT count(*)`.

any-store reports damage as `ErrCorrupt`, and a caller is told it may discard and rebuild a database on that error. A wrong key must not reach that branch, and a damaged page 1 must. So `Open` of a file opened under a key (`Options.Key` or `Options.Codec`; `pager.keyed`) authenticates page 1 where it reads it (`readPage1AtOpen`, the counterpart of the codec's decrypt under `lockBtree`), and when that fails it looks further:

- **The probe.** `proveKeyAtOpen` tries up to `keyProbePages` (16) other pages under the same codec, spread evenly over pages 2..N (`keyProbePgno`), where N is the database's size at the snapshot. Where the snapshot's WAL commit recorded a size (the pager's count, `sqlite3WalDbsize`), N is that size: page 1, which failed its authentication, has a count of its own in its plaintext header, and a damaged one smaller than the commit's (1, say, with the change counters intact) would leave no page to try under the right key. Where the WAL has no commit, N is `lockBtreePageCount` of page 1's header against the file's length, the size a successful open adopts, which keeps out the pages the file holds past the database. Pages past N are not the database's, and a page there sealed under another key proves nothing (`sweepPageCount` takes the commit's size over page 1's the same way). Each page is read as the open's snapshot holds it: from its WAL frame when the wal-index resolves one, else from the database file, where a page the file ends inside is damaged whatever the key and proves nothing, as a page that fails its authentication does not, and a read that came up short of bytes the file holds is a page that could not be read ([drift-7](#drift-7-short-db-file-read-treated-as-hard-error)).
- **One page authenticates: the key is right.** A wrong key passes a page's tag with probability about 2^-128. Page 1 is then damaged, and the open returns the page-1 error, which matches `ErrPageIntegrity` and `ErrCorrupt`. `Options.OnIntegrityError` is called for page 1, as for any damaged page.
- **None authenticates: `ErrWrongKey`.** It does not match `ErrCorrupt`. `Options.OnIntegrityError` is not called, for page 1 or for the pages tried: with a wrong key every page fails, and none of them is damaged.
- **A page cannot be read.** An I/O error, a failed wal-index lookup, a WAL frame the log does not hold, or a codec error that is not a failed authentication (`ErrPageIntegrity`), in the file or in the WAL, leaves that page untried, and the probe goes on to the next. When no page authenticates, the open returns the first such error and not `ErrWrongKey`: the unread page might have proved the key, as SQLite returns the read's code from `readDbPage`.

The pages are spread and not the first ones because damage tends to be a contiguous stretch: with the head of the file overwritten, page 1 and its neighbours fail together. The bound keeps a wrong key cheap on a large file: 16 page reads, not a scan.

What is still misreported is reported as a wrong key, the answer on which a caller keeps the file: a database of one page, a database damaged on page 1 and on every page tried, and a file whose header salt (bytes 72-87, outside the tag: [drift-2026-06-25-14](#drift-2026-06-25-14-page-1-db-header-bytes-0-99-is-excluded-from-both-checksum-and-aead-in)) was overwritten with another value that is not all zeros, since the key derived from a passphrase is then another key. A salt overwritten with zeros is the marker of a file that is not encrypted, and the codec settle refuses the key on the header's fields alone (`ErrNotEncrypted`). A wrong key is never reported as damage.

The header is judged before the key: a file of another format, a header that does not decode, a page 1 read again at open (from a WAL frame or the file) whose magic is not of the file's family or whose page size or reserved bytes are not the ones the file was opened with, or a header whose trusted size exceeds the pager's count is refused with that error (`ErrHeaderCorrupt` for the last three) whatever the key, as before. The family is compared before the page-1 copy is decoded (`page1HeaderAtOpen`): a WAL page 1 with SQLite's magic over an any-store file is `ErrHeaderCorrupt`, not a v1 file. The geometry is compared after: the pager and the codec are set up from the file's first read, the WAL's frames have its page size, and a header declaring another would be adopted under the right key (it is outside the authentication) and written to the database file by the next checkpoint ([drift-page-1-read-again-at-open-keeps-the-files-geometry](#drift-page-1-read-again-at-open-keeps-the-files-geometry)). Page 1's authentication is decided only after these checks, which read nothing the key protects. The checksum codec's page 1 in the database file is not verified by the open; the first read of it does that, as before. A page 1 in a WAL frame was read through the codec at open before, under any codec, and still is. After a successful open the key is proven, and every later authentication failure is damage. A successful open whose page 1 is in the database file pays one extra decrypt of it, and no probe.

`VerifyIntegrity` could list a damaged page 1 of an encrypted database only on a handle opened before the damage: an open after it now fails. The public `Open` failed there already, at its first write begin.

Tests: `TestOpen_WrongKeyIsErrWrongKey`, `TestOpen_WrongRawKeyAndCodec`, `TestOpen_DamagedPage1UnderRightKeyIsCorrupt`, `TestOpen_DamagedHeadOfFileUnderRightKeyIsCorrupt`, `TestOpen_DamagedOnePageFileIsErrWrongKey`, `TestOpen_DamagedLaterPageOpens`, `TestOpen_DamagedPage1FrameUnderRightKeyIsCorrupt`, `TestOpen_EveryPageTriedDamagedIsErrWrongKey`, `TestOpen_KeyProbeStaysInsideTheDatabase`, `TestOpen_KeyProbeTakesTheWALCommitsSize`, `TestOpen_HeaderIsJudgedBeforeTheKey`, `TestOpen_UnreadableProbePageIsNotAWrongKey`, `TestOpen_UnreadableProbePageThenAProvingOne`, `TestOpen_CodecErrorOnAProbePageIsNotAWrongKey`, `TestOpen_PanickingIntegrityHookReleasesTheFile` (`pager_test.go`), `test/encryption_open_test.go`.

<a id="drift-2026-11-24-4-integrity-checks-findings-are-an-error-that-matches-errcorrupt"></a>
### Drift: The Integrity Check's Findings Are An Error That Matches ErrCorrupt
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `integrity.go:*DB.IntegrityCheckN`, `integrity.go:*IntegrityError.Unwrap`, `integrity.go:*integrityChecker.reportRead`.

`sqlite3BtreeIntegrityCheck` returns its findings as text (`*pzOut = sqlite3StrAccumFinish(&sCheck.errMsg);`, `btree.c:11268`) and a count (`*pnErr = sCheck.nErr;`, `btree.c:11263`) and returns `sCheck.rc` (`btree.c:11273`), which a finding leaves at `SQLITE_OK` (`sCheck` starts zeroed, `btree.c:11159`): its only other values are `SQLITE_NOMEM` and `SQLITE_INTERRUPT` (`btree.c:10567`, `10579`, `10590`, `10885`). `checkAppendMsg` adds each finding to one text, a line each (`btree.c:10601-10624`). `OP_IntegrityCk` puts that text in a register (`vdbe.c:7273-7285`), and `PRAGMA integrity_check` returns it as one result row for the b-tree checks of a database (`pragma.c:1782-1789`): a damaged database is a successful statement whose result is not `ok`. A page the check cannot fetch is one more line, `"unable to get the page. error code=%d"` (`btree.c:10883-10884`), whatever the code; a fetch that failed with `SQLITE_IOERR_NOMEM` also sets the check's result, as `SQLITE_NOMEM` (`btree.c:10885`).

`IntegrityCheck` returns an error, `*IntegrityError`, with the same lines. An error needs a kind a caller can match, and the kind of damage here is `ErrCorrupt`, on which a caller may discard and rebuild the database. So the report matches it, with one exception that SQLite's rows do not need: a page that failed to read for a reason other than damage. `reportRead` lists it like SQLite does and keeps the read's error unless that error is itself damage (`ErrCorrupt`, which a page failing its checksum or authentication matches, or `ErrInvalidPage`, a pointer out of the database). A short read of a WAL frame (`ErrWALShortRead`) is kept: the index points past the log, which an open after every handle has closed rebuilds. A report with such a read is inconclusive as a whole, not only on that line: the pages under an unread interior page are then listed as never used, and its subtree as of another depth. It matches the read's error and not `ErrCorrupt`.

No new sentinel: the check's caller knows where the error came from, and the action on its findings is the action on `ErrCorrupt`.

Tests: `TestIntegrityErrorMatches`, `TestIntegrityCheck_DamagedPageMatchesErrCorrupt`, `TestIntegrityCheck_FailedReadIsNotErrCorrupt`, `TestIntegrityCheck_FailedKeyReadIsNamedWithItsError` (`integrity_test.go`), `test/integrity_check_test.go`.

<a id="drift-2026-11-24-5-a-failed-commit-ends-its-cursors-with-its-error"></a>
### Drift: A Failed Commit Ends Its Cursors With Its Error
- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.pagerError`.

SQLite's `pager_error` moves the pager to `PAGER_ERROR` and stores the code in `Pager.errCode` only for an `SQLITE_IOERR` or `SQLITE_FULL` (`if( rc2==SQLITE_FULL || rc2==SQLITE_IOERR )`, `pager.c:1955-1958`), and `pager_unlock` clears it (`pager.c:1919`). A failed WAL commit does not pass through it: `sqlite3PagerCommitPhaseOne` returns `pagerWalFrames`' code (`pager.c:6512`). `sqlite3VdbeHalt` then rolls the transaction back with `sqlite3RollbackAll(db, SQLITE_OK)` (`rc = vdbeCommit(db, p);` ... `sqlite3RollbackAll(db, SQLITE_OK);`, `vdbeaux.c:3417-3425`), which hands `SQLITE_OK` to `sqlite3BtreeRollback` (`main.c:1505`). With it the rollback saves every cursor (`rc = tripCode = saveAllCursors(pBt, 0, 0);`, `btree.c:4526-4527`): the cursors go on when the save succeeds. A save that fails sets `writeOnly` to 0 and trips all of them with its error (`btree.c:4528`, `4532-4533`), the ones saved before it included. A statement that fails before it reaches its commit takes another path in `sqlite3VdbeHalt`: an `SQLITE_FULL` or `SQLITE_NOMEM` of a statement with a statement journal rolls back that statement alone (`eStatementOp = SAVEPOINT_ROLLBACK;`, `vdbeaux.c:3372-3373`), and otherwise the transaction is rolled back with trip code `SQLITE_ABORT_ROLLBACK` (`sqlite3RollbackAll(db, SQLITE_ABORT_ROLLBACK);`, `vdbeaux.c:3378`).

`pagerError` ends a failed commit at once ([the eager cleanup](#old-drift-pagererror-eager-cleanup)). It stores the commit's error in `errCode`, whatever the error is, unless a failed spill has stored one already, and trips the transaction's cursors with it (`tripCursors` with `writeStateErr`) before the cleanup clears it. A cursor of the failed transaction reports why the commit failed (an I/O error, a full disk), where it reported the read-only refusal of a pager without a writer, and where SQLite's would have been saved by the rollback, unless a save failed. That error is not damage: it matches `ErrCorrupt` only when the commit's error did.

Test: `TestPagerError_CursorReportsTheCommitError` (`pager_test.go`).

<a id="drift-2026-11-24-6-the-integrity-check-runs-in-a-read-transaction"></a>
### Drift: The Integrity Check Runs In A Read Transaction
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `integrity.go:*ReadTx.IntegrityCheckN`, `integrity.go:*DB.IntegrityCheckN`.

`sqlite3BtreeIntegrityCheck` runs inside the transaction its connection has open (`assert( p->inTrans>TRANS_NONE && pBt->inTransaction>TRANS_NONE );`, `btree.c:11156`), a read or a write one, and reads the pages through the connection's pager: in a write transaction it checks the transaction's own pages, committed or not.

`ReadTx.IntegrityCheckN` runs in the read transaction it is called on, and takes no other transaction or reader slot, so a caller that holds the only slot can check, and the check and its caller see one snapshot. It reads that snapshot through a page cache of its own, bounded by the transaction's frame window and page count. The read view of a write transaction is refused: it has no reader cache to bound such a walk, and its uncommitted pages are in the writer's cache, which only the writer's goroutine may read. `DB.IntegrityCheckN` begins a read transaction for the check, as `PRAGMA integrity_check` begins one when the connection has none. The public `IntegrityCheck` checks in the read transaction its context carries, and in one of its own otherwise, also when the context carries a write transaction: that transaction's changes are not checked, and its read transaction does not wait for a reader slot (`TryBeginRead`): the caller holds the writer, and may hold the only slot in a read transaction it ends once the check returns. With every slot held the check is busy.

Tests: `TestIntegrityCheck_InTheCallersReadTx` (`integrity_test.go`), `TestIntegrityCheck_TheSnapshotItChecks` (`db_test.go`), `TestIntegrityCheck_ReadSlot` (`test/integrity_check_test.go`).

<a id="drift-page-1-read-again-at-open-keeps-the-files-geometry"></a>
### Drift: Page 1 Read Again At Open Must Declare The File's Page Size And Reserved Bytes
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:page1HeaderAtOpen` (for `pager.go:*pager.readPage1AtOpen` and `pager.go:*pager.page1FrameHeader`).

`lockBtree` takes the page size and the reserved bytes from the page 1 it fetches, which under a WAL is the snapshot's newest frame of page 1. A page size other than the pager's is adopted: `lockBtree` releases page 1, sets the size and the reserve on the pager and returns (`if( (u32)pageSize!=pBt->pageSize ){` ... `rc = sqlite3PagerSetPagesize(pBt->pPager, &pBt->pageSize, pageSize-usableSize);`, `btree.c:3384-3399`), and `sqlite3BtreeBeginTrans` calls it again (`while( pBt->pPage1==0 && SQLITE_OK==(rc = lockBtree(pBt)) );`, `btree.c:3707`). With the same page size, the usable size is page 1's (`pBt->usableSize = usableSize;`, `btree.c:3417`).

Here the pager and the codec are set up from the open's first read of the header, from the database file, and the WAL's frames have that page size (recovery refuses a WAL whose header declares another). Page 1 read again at open (`readPage1AtOpen`), from a WAL frame or from the file, must declare the same page size and reserved bytes: one that declares others is `ErrHeaderCorrupt`, whatever the codec and the key, and is not adopted. No write here changes either: a file keeps the page size and the reserve it was created with. A header that declared others would size the pager's pages and the codec's trailer by bytes that are outside the page's checksum and authentication, and the next checkpoint would write it to the database file.

Test: `TestOpen_HeaderIsJudgedBeforeTheKey` (`pager_test.go`: a WAL page 1 of another page size, and of other reserved bytes, under the right key and a wrong one).

<a id="drift-a-namespace-whose-root-is-page-1-is-corrupt"></a>
### Drift: A Namespace Whose Root Is Page 1 Is ErrCorrupt
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*DB.resolveNamespace`.

SQLite opens a b-tree on a root of 1: `btreeCursor` refuses only a root below 1 (`if( iTable<1 ){ return SQLITE_CORRUPT_BKPT; }`, `btree.c:4715-4717`), and the schema load, with its extra checks on (the default), refuses a root that does not parse or lies past the database (`prepare.c:135-140`), not 1. Root 1 is the schema table's, so a table whose schema row names it reads and writes the b-tree of `sqlite_schema` as its own.

`resolveNamespace` refuses a namespace whose root is outside 2..N, where N is the database's size, with `ErrCorrupt` when the name is resolved: 1 with 0 and with a root past N. Page 1 is the root of the master table, which holds the namespaces' names and roots, and `CreateNamespace` gives every namespace a page of its own. A namespace on page 1 is damage: a check of it would walk the master table and pass, and a write to it would put its records among the namespaces'.

Tests: `TestGetNamespace_RootOfTheMasterIsCorrupt` (`db_test.go`), `TestOpen_DirtyCatalogRootOutsideTheDatabase` (`test/dirty_open_test.go`).
