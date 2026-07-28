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
func maxLocalPayload(usableSize int) int {
    return ((usableSize - 12) * 64 / 255) - 23
}
func minLocalPayload(usableSize int) int {
    return ((usableSize - 12) * 32 / 255) - 23
}
func localPayloadSize(totalPayload, usableSize int) int {
    maxLocal := maxLocalPayload(usableSize)
    if totalPayload <= maxLocal { return totalPayload }
    minLocal := minLocalPayload(usableSize)
    ovflUsable := overflowPageUsable(usableSize)
    surplus := minLocal + (totalPayload-minLocal)%ovflUsable
    if surplus <= maxLocal { return surplus }
    return minLocal
}
```

<a id="old-drift-index-only-local-payload-no-maxleaf-minleaf"></a>
### Drift

**Severity:** low

| Aspect | SQLite | Go |
|--------|--------|-----|
| maxLocal (index) | `(usableSize-12)*64/255 - 23` | `(usableSize-12)*64/255 - 23` |
| minLocal (index) | `(usableSize-12)*32/255 - 23` | `(usableSize-12)*32/255 - 23` |
| maxLeaf (table) | `usableSize - 35` | Not applicable (no table B-trees) |
| minLeaf (table) | `(usableSize-12)*32/255 - 23` | Not applicable |
| Surplus formula | Identical | Identical |

**Classification: Structural** -- The formulas are identical for index B-trees. The
Go implementation only has index-style B-trees and therefore only implements the
`maxLocal`/`minLocal` pair. The `maxLeaf`/`minLeaf` pair (for table B-trees) is
absent because Go does not implement table B-trees.

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
| 0 | 16 | `"SQLite format 3\000"` | `"BTree format 1\000"` |
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
| Magic string | `"SQLite format 3\000"` | `"BTree format 1\000"` |
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

The Go implementation uses `SchemaFormat = 5` (set in `pager.go:initNewDB()`).

```go
p.header = dbHeader{
    SchemaFormat: 5,
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

**Classification: Intentional** -- Go's "format 5" is NOT the same as SQLite's schema
format 4. It is a custom version number for the Go implementation's own format evolution.
The Go implementation never supported SQLite's schema formats 1-4. Format 5 specifically
signifies the two-varint leaf cell format with unified key/value overflow.

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
| WAL undo (`sqlite3WalUndo`) | **Missing** -- Go uses pager-level rollback instead |
| First-opener `ftruncate(shm, 3)` marker | **Aligned (2026-07-10)** -- `newPlatformShm` now runs SQLite's full DMS first-attacher election (`unixLockSharedMemory`, os_unix.c:4860-4913, 3.52.0): F_GETLK probe → three-way branch (F_UNLCK: WRLCK + UNCONDITIONAL truncate-to-3 + atomic downgrade to RDLCK; F_RDLCK: join shared; F_WRLCK: back off BUSY, never join — the os_unix.c:4864-4871 race). The `Size()==0` gate is gone; a crash-persisted stale shm is physically reset by the first attacher and `recoverLocked` rebuilds from the WAL. See [§drift-2026-06-25-25](#drift-2026-06-25-25-first-opener-does-not-reset-a-stale-persisted-shm-trusts-checksum-vali) (RESOLVED) and item 2 below |
| Orphan-inode handling on open race | **Aligned** -- DB-file flock serializes last-client-unlink (see §SHM open/close protocol drift) |

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
| Rollback journal | Full implementation | Not implemented |
| Lock escalation | SHARED -> RESERVED -> PENDING -> EXCLUSIVE | WAL write lock only |
| Multi-process support | Full (via file locks + SHM) | mmap-backed SHM on `(linux \|\| darwin) && (amd64 \|\| arm64)`; heap fallback (single-process) elsewhere |
| Hot journal rollback | Automatic recovery | Not applicable (no rollback journal) |
| Sub-journal (savepoint journal) | Written to disk | In-memory page copies |
| NOCONTENT optimization | `PAGER_GET_NOCONTENT` flag | `getPageNoContent()` |
| DONT_WRITE flag | `PGHDR_DONT_WRITE` in page flags | `dontWritePages` map |
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
  pre-allocates `[]byte` page buffers. `Get()` pops from free list, falls back to
  `make()` overflow. `Put()` returns buffers. `UnderPressure()` atomic flag when
  free list drops below `nReserve` (10% + 1 of slab size)
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
  persistent reader caches. `closeCh` unblocks waiters on DB close
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
| Slab allocator | Contiguous `void*` buffer, pointer arithmetic (`pcache1.c:283-288`) | `[][]byte` slice, Go-idiomatic (drift #7) |
| Slab buffer return | Range check `SQLITE_WITHIN` (`pcache1.c:381`) | Caps free list at `nSlab`; overflow buffers recycle via `sync.Pool` (drift #8, since commit `01af9d6`) |
| Slab init | Library init `pcache1Init` (`pcache1.c:695-741`) | Lazy init on first `Open()` or explicit `ConfigPageCache()` (drift #9) |
| Bulk alloc | Contiguous `pBulk` carved into slots (`pcache1.c:312-327`) | Individual page structs with slab buffers (drift #10) |
| Page flags | Bitmask on each page | Separate maps (`dontWritePages`, `hasContent`) |
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
- `dontWritePages` map (replacing SQLite's `PGHDR_DONT_WRITE` flag)
- Bounds validation on trunk/leaf page numbers

<a id="old-drift-freelist-trunk-fill-corruption-ceiling"></a>
**Severity:** medium (trunk fills to usableSize/4-2 (corruption ceiling) not SQLite's conservative -8; pre-3.6.0 readers flag it corrupt)
### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| Freelist format | Trunk/leaf linked list | Identical trunk/leaf linked list |
| hasContent tracking | Bitvec (`pHasContent`) | Go map (`hasContent`) |
| dontWrite flag | Page header flag (`PGHDR_DONT_WRITE`) | Separate map (`dontWritePages`) |
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
| Max local payload | `BT_MAX_LOCAL = 65501` | `maxLocalPayload()` computed from usableSize |
| Max total payload | Validated against `nPage * usableSize` | `maxPayloadAlloc = 1 << 30` |
| Approach | Per-access bounds checking with macros | Per-access bounds checking with error returns |
| Overflow protection | Trust chain structure | `maxIter` + bounds checks |
| Max tree depth | `BTCURSOR_MAX_DEPTH = 20` | `btCursorMaxDepth` (same value) |

<a id="old-drift-corruption-fixed-1gb-payload-cap-vs-npage-validation"></a>
**Severity:** low

**Classification: Divergent** -- Both implementations protect against corruption, but
the specific cap values differ. SQLite's `nCell/usableSize > nPage` check is more
precise (validates against actual database size), while Go uses a fixed 1 GB cap.

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
    bt    *btree
    stack []cursorFrame
    valid bool
}
type cursorFrame struct {
    pgno    uint32
    cellIdx int
    pg      *page  // pinned page (non-nil only for leaf)
}
```

Key features:
- **2 states**: valid or invalid (no REQUIRESEEK/SKIPNEXT/FAULT)
- **Dynamic stack** (slice, not fixed-size array)
- **Only leaf page pinned** (interior pages released during descent)
- **Read-only** (no write cursor support)
- **SeekNear optimization**: checks if target key is within current leaf bounds
- **No overflow page cache**
- **No cell info caching**

### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| States | 5 (VALID, INVALID, SKIPNEXT, REQUIRESEEK, FAULT) | 2 (valid, invalid) |
| Page stack | Fixed-size arrays (20 depth) | Dynamic slice |
| Pinned pages | All pages in stack pinned | Only leaf page pinned |
| Overflow cache | `aOverflow[]` | None |
| Cell info cache | `CellInfo info` | None |
| Write support | Full (BTCF_WriteFlag) | Read-only |
| Save/restore | Automatic via `pKey`/`nKey` | Not supported |
| Delete during iterate | `SKIPNEXT` mechanism | Not supported |

<a id="old-drift-readonly-two-state-cursor"></a>
**Severity:** low

**Classification: Structural** -- The Go cursor is significantly simpler, designed
for read-only iteration. Write operations go through `btree.Put()` / `btree.Delete()`
which do their own tree traversal rather than using a cursor. The lack of
save/restore means cursors become invalid if the tree is modified.

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
| Overflow limits | `maxLocal` / `minLocal` (index formulas) |

<a id="old-drift-index-only-btrees-no-intkey-table"></a>
**Severity:** medium

**Classification: Structural** -- This is a fundamental architectural difference.
SQLite's table B-trees use integer keys with record-format values, optimized for
SQL table storage. The Go implementation repurposes the index B-tree format to
store key/value pairs by adding a second varint for value length. This means:

1. No integer-key lookups (all keys are byte-comparable blobs)
2. No `PTF_INTKEY` / `PTF_LEAFDATA` page types
3. Values are stored inline with keys using the two-varint format
4. The `maxLeaf` / `minLeaf` overflow formulas are never used

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
   `pWal->hdr.nPage`) and falls back to `pager.dbSize` when the WAL header carries
   no committed size -- mirroring `pagerPagecount`'s file-size fallback
   (pager.c:3299-3305). Every reader bound check consults this via
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
| 3. Overflow formula | Structural | None | Same formulas; table B-tree variants absent |
| 4. Binary search | Divergent | Medium | Raw byte comparison vs record comparison; prefix optimization |
| 5. Page header | Intentional | Low | Different magic string; same layout |
| 6. Schema format | Intentional | Low | Format 5 is Go-specific |
| 7. WAL | Intentional + Divergent | Medium | Different magic/version; Go map for lookups |
| 8. Pager | Structural | High | WAL-only; no rollback journal; simplified states |
| 9. Page cache | Structural | Low | Slab allocator, bulk alloc, LRU, admission control, persistent cache, reader limiter |
| 10. Freelist | Intentional | None | Same format; different tracking structures |
| 11. Overflow chains | Divergent | Medium | MVCC-aware reads; no overflow cache |
| 12. MVCC | Divergent | High | Explicit uncached reads for goroutine safety |
| 13. Corruption protection | Divergent | Low | Fixed 1GB cap vs page-count-based validation |
| 14. Integrity check | Divergent | Low | Same core checks; auto-discovers trees |
| 15. Cursor | Structural | High | Read-only; no save/restore; only leaf pinned |
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
1. **pagerError eager cleanup** (`pager.go:pagerError`) — **Severity:** medium.
   SQLite's `pager_error()`
   only sets errCode and transitions to `PAGER_ERROR`, deferring cleanup to a
   subsequent `sqlite3PagerRollback()`. We perform eager cleanup (cache purge,
   WAL rollback, lock release, transition to pagerOpen) because there is no
   guaranteed subsequent rollback call — if the caller's goroutine panics or
   abandons the transaction, the WAL write lock would remain held, blocking the
   next `BeginWrite`.

<a id="old-drift-pagerstress-page1-exclusion"></a>
2. **Page-1 explicit exclusion** (`pager.go:pagerStress` AND
   `pcache.go:findSpillVictim`) — **Severity:** low.
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
3. **`pcache.create()` drops xStress error** (`pcache.go:304`) — **Severity:** low.
   SQLite's
   `sqlite3PcacheFetchStress` (`pcache.c:481-485`) propagates non-BUSY errors from
   xStress to the caller, allowing the pager to abort page acquisition. Our
   `create()` has no error return (it always returns a `*page`, `pcache.go:304`),
   so the `xStress` return value is silently discarded (`pcache.go:367`). In
   practice, `pagerStress` calls `pagerError` on WAL write failure
   (`pager.go:2227-2234`), which performs eager cleanup, so the dropped error is
   harmless.

<a id="old-drift-batched-walindex-setbatch"></a>
4. **Batched wal-index update** (`wal.go:setBatch`) — **Severity:** medium.
   SQLite updates the wal-index
   inline in `walFrames()` — the write loop tags each page `PGHDR_WAL_APPEND`
   (set on append `wal.c:4149`, cleared on in-place reuse `wal.c:4137`), then a
   second loop replays the flag via `walIndexAppend()` (`wal.c:4212-4221`);
   rollback uses `walCleanupHash()` (`wal.c:1233`). We mirror this with one
   post-loop `setBatch` (`wal.go:603`, called at `wal.go:2218`) per
   `writeFrames` call (a single `wi.mu` acquisition for the in-process `pageMap`,
   then eager `shmHashWrite` — *not* deferred), plus a `walCleanupHash` analog on
   rollback. **Invariant: the appended set handed to `setBatch` MUST be recorded
   inline in the write loop (the `appended` slice ≡ `PGHDR_WAL_APPEND`), never
   re-derived.** Re-deriving the reuse predicate after the loop dropped the
   force-appended commit frame and silently corrupted recovery — see the
   Frame-Reuse note below. A `maxFrame < nFrame` guard in
   `writeFrames`/`writeFramesMem` (`wal.go:2230`) now fails loudly if any appended
   frame is unregistered.

<a id="old-drift-pagerstress-dontwrite-skip-walwrite"></a>
5. **dontWrite pages made clean without WAL write** (`pager.go:pagerStress`) — **Severity:** low.
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
  force-rollback. SQLite does not allow `sqlite3_close()` during active
  transactions (returns `SQLITE_BUSY`); Go's DB.Close performs a force-rollback,
  requiring this mutex to prevent it from racing with a concurrent commit.
- `db.writerLocksDone`: atomic CAS guard ensuring Close/Commit/Rollback lock
  cleanup runs exactly once despite concurrent goroutines
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
in lockstep. Catches crash-mid-header-update corruption.

**Auto-Checkpoint Errors Surfaced** -- Resolved 2026-04-22

`tx.pager.tryCheckpoint()`'s result (error or nil) is stored on
`DB.lastAutoCheckpointErr` and exposed via
`DB.LastAutoCheckpointError() error`. Monitoring code can poll the
accessor; the auto-checkpoint itself remains best-effort (returning
the error to the commit caller would change the application contract).

### Page Cache Memory Management (page_slab.go, pcache.go, db.go)

**Global Slab Allocator** -- Severity: N/A (implemented)

Process-global `pageSlab` singleton pre-allocates `[]byte` page buffers at init.
Configured via `ConfigPageCache(pageSize, nPages)` before opening any databases,
or lazily initialized with defaults (2000 buffers) on first `Open()` call.
Matches SQLite's `sqlite3_config(SQLITE_CONFIG_PAGECACHE)` /
`sqlite3PCacheBufferSetup` (`pcache1.c:271-291`).

Key components:
- `pageSlab.Get()`: pops from free list; falls back to `make()` overflow
- `pageSlab.Put()`: returns buffer to free list
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
  its buffer in-place (`resetPage` → `clear(p.data)`) for **both** writer and reader
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
  function with inline stress handling.
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
cells with the parent dividers, recomputes the minimum output-page count k ∈ {nOld-1, nOld,
nOld+1}, packs each page full then backs off the last for balance, and rewrites the parent
dividers — producing SQLite's high, even fill. It is now the live path for **both** sides:
inserts funnel an over-full leaf through it (`splitLeafAndInsertWithPath` → `balanceNonroot`,
btree.go:2055), and deletes funnel an under-full (>2/3 free) leaf through the same balancer
with `inject.active=false` (`deleteRebalanceLeaf` → `balanceNonroot`, btree.go:2799;
`completeMergeUpward` cascades parent under-fullness, gating single-child collapse on root per
btree.c:8960). The former 2-way `leafSplitPoint` split survives only as the root-leaf fallback
(`splitRootLeafAndInsert`); `balance_quick` is retained as the rightmost-append fast path
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

### Lazy `ensureHeaderInitialized` helper (commits 2026-04-18)

`wal.go:ensureHeaderInitialized` is an experimental lazy counterpart to
`walIndexReadHdr` (wal.c:2640-2745). Structure matches SQLite:

- Fast path: `readHeader()` → if valid, return.
- Slow path: `walLockExclusive(WAL_WRITE_LOCK)` → retry `readHeader` → if still
  invalid, recover.
- Busy-path fence: `walLockShared(WAL_RECOVER_LOCK)` then release, matching
  wal.c:3089.

Currently the helper is invoked defensively from 3 targeted tests
(`TestEnsureHeaderInitialized_*`) but NOT from production code. Production
code continues to use eager SHM init via `wal.open` + `adoptSHMState` /
`recoverLocked` / `initHeaderStateLocked`. See "SQLite-alignment blocker"
below for why cold-open was attempted and reverted.

**Drifts from SQLite's `walIndexReadHdr` preserved:**

<a id="old-drift-ensureheader-triple-lock-recovery-gate"></a>
**Severity:** low

1. **Triple-lock recovery gate**: SQLite `walIndexRecover` takes
   `WAL_ALL_BUT_WRITE..READ_LOCK(0)` exclusive (wal.c:1400-1401). Our
   `recoverLocked` requires callers to hold `lockCheckpoint + lockRecover`
   exclusive. The helper takes `lockWrite → lockCheckpoint → lockRecover` under
   non-blocking `F_SETLK`; under contention with `checkpointWithMode` (which
   uses `CKPT → WRITE` order) the loser observes `ErrBusy` — no deadlock.

2. **BUSY_RECOVERY signal — resolved 2026-04-22.** The three contended
   branches in `ensureHeaderInitialized` now return `ErrBusyRecovery`
   (distinct from the generic `errWALRetry`) when a peer holds the
   RECOVER / CKPT exclusive locks — matching SQLite's
   `SQLITE_BUSY_RECOVERY` (`sqlitec/src/wal.c:3063-3090`, probe at
   `sqlitec/src/os_unix.c:4872-4907`). Upstream `DB.BeginWrite` routes
   `ErrBusyRecovery` through the configured `BusyHandler` for proper
   backoff instead of spinning.

<a id="old-drift-headerondisk-mxframe-inference"></a>
**Severity:** none

3. **`headerOnDisk` shortcut in `syncFromSHMLocked`** (`wal.go:1694-1721`):
   we infer "on-disk WAL header exists" from `hdr.mxFrame > 0` rather than
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

Hooks in `pager.commit()` (post-`wal.writeFrames`) and
`pager.checkpointWithMode()` (Restart/Truncate modes) dispatch the C
equivalents of `sqlite3BackupUpdate` and `sqlite3BackupRestart`
respectively. See `pager.dispatchBackupUpdate` /
`pager.dispatchBackupRestart`. External-process writes are detected at
each Step's read tx via the page-1 `FileChangeCounter` and trigger the
same restart.

Anystore-level `(*db).Backup(ctx, path)` drives the engine by opening
a fresh destination DB at `path` with matching options.

<a id="old-drift-backup-intentional-simplifications"></a>
**Key intentional simplifications** (full list in the plan document):

**Severity:** low

- Always same-page-size: any-store is WAL-only, so `backup.c:378-383`'s
  `SQLITE_READONLY` for WAL+size-mismatch is our `ErrBackupPageSizeMismatch`
  at init. The cross-size packing path at `backup.c:449-528` is
  therefore unreachable.
- No `PENDING_BYTE_PAGE` handling -- any-store has no 1GB lock byte.
- No attached-db name resolution (`findDatabase`, backup.c:82) -- one b-tree per DB.
- No `nBackup` counter on source -- nothing for it to block (no
  VACUUM, immutable page size).

**GAP -- KDF-salt poisoning when backing up an encrypted DB.** `onePage` copies page 1
verbatim (patching only the DatabaseSize field at offset 28), so the destination inherits
the **source's** 16-byte KDF salt (header bytes 72-87, `codec_kdf.go`). For raw-key codecs
this is harmless, but a KDF-derived (passphrase) destination will, on reopen, re-derive its
key from (dst passphrase, **src** salt) — the wrong key, leaving it permanently
undecryptable. SQLCipher rejects the cross-codec case at `sqlite3_backup_init`
(`backup.c:156-175`); any-store does not. Recorded in
`docs/btree/mappings/sqlcipher_codec.json` (`backup.c:156-175`). Fix options: reject the
cross-codec case at `BackupInit`, or refresh dst's salt on page 1 after copy.

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
- **Category:** changed-logic  -  **Severity:** high  -  **Status:** RESOLVED 2026-07-10 (2026-07 pre-beta review): all four getters (`getPageWriter`, `readTempPage`, `getPageReader`, `readRawPage`) now propagate `readFrame`/`readFrameRaw` errors wrapped with page/frame context, matching C `readDbPage`, where with `iFrame != 0` the DB-file read is the unreachable else branch and a WAL read error becomes the page-get error (`sqlitec/src/pager.c:3035-3046`, 3.52.0). The "benign reset between lookup and read" TOCTOU race the fallthrough covered (introduced with commit `1e2cec7`) is structurally excluded: the writer path holds `lockWrite`, which any restart requires (`checkpointWithMode`; PASSIVE never resets, `checkpointPost`); readers hold a slot 1-4 shared lock that `tryResetWALWithBusy` must take exclusively; slot-0 snapshots carry `minFrame = mxFrame+1` so `walIndex.get` never resolves a frame for them (C `readLock==0` short-circuit); and the in-process slot-0 fallback now re-validates after acquiring read-0 (see the 2026-07-10 fallback re-validation fix, mirroring C's READ_LOCK(0) re-check at `wal.c:3136-3139`). Caveat: API calls that begin pager reads without the `db.mu` drain (`GetNamespace`, `ListNamespaces`, `IntegrityCheckN`) and race `DB.Close`'s WAL truncate may now surface a read error where they previously silently succeeded (contract-gray; follow-up). Adjacent residual: `refreshHeaderFromPage1`'s single-failure-with-successful-fallback path (deliberate 2026-07-10 design) still adopts a DB-file page-1 header under the write lock — open follow-up. Regression tests: `TestWALReadFrameErrorPropagates{,_Writer,_TempPage}`, `TestReadRawPageWALErrorPropagates`, `TestWALDecryptFailurePropagatesFromReadFrame`, `TestReadFrameNoBenignFailuresUnderTruncateStress`, `TestInProcessSlot0FallbackTruncateStress`.
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
### Drift: readFrame Past-Tail Read Failure Remapped To ErrWALCorrupt
- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.readFrame` (`internal/btree/wal.go`, past-tail remap in the `ReadAt` error branch), `wal.go:*wal.readFrameRaw` (same pattern).

C `sqlite3WalReadFrame` (`sqlitec/src/wal.c:3649-3664`, 3.52.0) is a bare `sqlite3OsRead` with no bounds check — a read past the physical WAL tail surfaces as the raw OS error (typically `SQLITE_IOERR_SHORT_READ` from `unixRead`), and `readDbPage` forgives short reads only on the DB-file branch, never the WAL branch (`pager.c:3042-3045`). Go's `readFrame`/`readFrameRaw` instead remap a `ReadAt` failure at `frame > nFrame` (a stale process-local tail view in multi-process mode) to `ErrWALCorrupt` rather than leaking the raw I/O error. Both C and Go treat the condition as an error; the divergence is only the error identity. Since the drift-6 resolution these errors propagate to page-get callers, so the remap is now user-visible: callers see `ErrWALCorrupt` where C would surface an I/O error code. Kept deliberately — a lookup-resolved frame that cannot be read within the reader's validated snapshot implies index/tail inconsistency, which `ErrWALCorrupt` describes more accurately than a short-read errno.


### Drift: Short DB File Read Treated As Hard Error
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.readTempPage` (`internal/btree/pager.go:1085-1178`; hard-error path `1126-1132`; underlying `readDBPage` `internal/btree/pager.go:339-368`).

C `readDbPage` explicitly maps a short read of an in-bounds DB page to success: after
`sqlite3OsRead`, `if( rc==SQLITE_IOERR_SHORT_READ ){ rc = SQLITE_OK; }`
(`pager.c:3042-3044`), and because the pcache buffer is pre-zeroed the page is returned
zero-padded rather than as an error (`os_unix.c:3575-3577` zero-fills the unread tail).
Go's `readTempPage` instead treats a short read of an in-bounds page (`pgno <= dbSizeBound`)
in a physically-short DB file as a hard error. The consequence is divergent error
behavior for a physically-truncated-but-logically-valid file: Go fails where SQLite
returns a partially-read, zero-padded page as success.

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

<a id="drift-13-empty-interior-root-treated-as-empty-btree-not-corruption"></a>
### Drift: Empty Interior Root Treated As Empty Btree Not Corruption
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `btree.go:*Cursor.First` (`internal/btree/btree.go:3477-3519`), `btree.go:*Cursor.Last` (`btree.go:3524-3559`).

SQLite's `moveToRoot` treats a 0-cell interior root as a benign "virtual root" only
when `pRoot->pgno==1`, and otherwise returns `SQLITE_CORRUPT_BKPT`
(`btree.c:5606-5618`, with the `if( pRoot->pgno!=1 ) return SQLITE_CORRUPT_BKPT` guard at
`btree.c:5610`): a non-page-1 interior root with zero cells is corruption. Go's
`Cursor.First` descent loop, when it reaches a 0-cell interior page (root or deeper),
does `releasePage(pg); return nil` and leaves the cursor invalid (`btree.go:3488-3492`),
i.e. it silently reports an empty b-tree; `Cursor.Last`'s rightChild descent
(`btree.go:3535-3549`) likewise has no `rootPage==1` guard. The consequence is that a
corrupt zero-cell interior root is accepted as a benign empty cursor instead of being
flagged `ErrCorrupt`, and First/Last are asymmetric on this case. An in-code DRIFT marker
for this anchor now sits in the `Cursor.Last` doc comment (`btree.go:3522-3523`).

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
### Drift: Count Traversal Missing Interrupt Cancellation Check
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.Count` (`btree.go:3368-3370`), `btree.go:*btree.countPage` (`btree.go:3375-3424`).

SQLite's `sqlite3BtreeCount` (`btree.c:10489`) gates its page-walk loop on
`!AtomicLoad(&db->u1.isInterrupted)` (`btree.c:10502`), so a long count over a huge tree
can be cancelled via `sqlite3_interrupt` mid-walk and returns the in-progress rc
(allowing `SQLITE_INTERRUPT`). Go's `countPage` (`btree.go:3375-3424`) has no
interrupt/context/cancellation hook and always runs to completion or to an error. The
consequence is that a `Count()` over a very large tree cannot be aborted; this is a
missing SQLite runtime feature with low correctness impact.

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
freeblock minimum, so freeing it could not store a valid freeblock header -- a latent
deviation from SQLite's free-space format guarantee, mitigated in practice only because
real key/value cells comfortably exceed 4 bytes.

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
- **Affected functions:** `btree.go:*btree.insertSepIntoInterior` (`internal/btree/btree.go:2193`, root-overflow block `btree.go:2300-2357`), `btree.go:*btree.insertIntoParent` (`internal/btree/btree.go:2466-2468`), `btree.go:*btree.rewriteParentAfterBalance` (`balance.go:905-940`).

When a separator must be inserted into a full interior page that is the btree ROOT (no
grandparent to gather siblings from), Go does not run SQLite's `balance_deeper` +
`balance_nonroot` even-fill packing (`btree.c:9134-9154` routes a root overflow to
`balance_deeper`; `btree.c:9195-9214` then redistributes via `balance_nonroot`). Instead
`insertSepIntoInterior` falls to a classic
2-way median split (`btree.go:2300-2357`): collect the root's interior cells, splice in the
new divider, pick a split via `interiorSplitPoint` (a ~2/3 left-fill target,
`btree.go:357-371`, called at `btree.go:2322`), rebuild into two interior pages, and grow a new root through
`splitRoot`. The same fill-factor deviation recurs in `rewriteParentAfterBalance`'s
over-full fallback (`balance.go:905-940`, `interiorSplitPoint` at `balance.go:911`) and in the legacy non-path `insertIntoParent`,
which on a failed re-descent unconditionally calls `splitRoot` as a "safety net"
(`btree.go:2466-2468`) rather than reporting corruption. The consequence is that root-level
interior overflow produces a different (and looser) page-fill distribution than SQLite's
balanced redistribution.

<a id="drift-31-rebuildinteriorpage-accepts-zero-cell-pages"></a>
### Drift: rebuildInteriorPage Accepts Zero Cell Pages
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.rebuildInteriorPage` (`btree.go:1879-1935`).

C's `rebuildPage` hard-asserts `nCell>0` (`btree.c:7648`) and its only callers
(`balance_quick`, the `editPage` fallback) guarantee at least one cell. Go's
`rebuildInteriorPage` is instead deliberately invoked with zero cells -- e.g. the
`removeChildFromParent` / collapse paths and `rebuildInteriorPage(rootPg, nil, ...)` -- and
faithfully produces an interior page carrying only a `rightChild` with `cellCount=0`. The
consequence is that Go's structural contract for interior pages is looser than SQLite's:
degenerate single-child interior pages are a normal, accepted state rather than an asserted
impossibility, which is the structural foundation that lets the underfullness-cascade drifts
(see Drift 20/21) leave such pages in the tree.

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
- **Category:** changed-logic  -  **Severity:** low
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
the engine is always WAL, so the source bytes copied into the destination are already 2 and
the destination's in-memory header carries the WAL version regardless. The consequence is a
latent divergence that would only matter if a non-WAL source were ever backed up to a WAL
destination, which any-store's always-WAL design precludes.

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
### Drift: BeginReadFast Skips Page 1 Staleness Counter Reads
- **Category:** new-feature  -  **Severity:** medium
- **Affected functions:** `db.go:*DB.BeginRead` (`db.go:840-842`),
  `db.go:*DB.BeginReadFast` (`internal/btree/db.go:847-849`, `db.go:754-767`, `db.go:831-834`,
  `db.go:1801-1821`), `db.go` (`internal/btree/db.go:727`, `db.go:840-849`).

Go parameterizes its read-transaction opener as `beginRead(readCounters bool)` (`db.go:727`)
and adds a public `BeginReadFast()` that passes `readCounters=false` (`db.go:847-849`); on
that fast path `pager.readHeaderCounters` is skipped and the transaction's
`diskFileChangeCounter`/`diskSchemaCookie` are seeded from the process-local cached counters
(`db.go:754-767`, `db.go:831-834`) rather than from on-disk page-1 metadata. Snapshot
isolation for actual data reads is preserved -- the path still fixes the WAL `maxFrame`/reader
slot and clears the per-connection reader cache on change-counter mismatch, matching SQLite's
`pagerBeginReadTransaction` reset behavior -- so the divergence is purely in the staleness
reporting layer, which has no SQLite analogue to begin with. The consequence is that on a
fast read `IsDataStale`/`IsSchemaStale` always return false and
`DiskFileChangeCounter`/`DiskSchemaCookie` return possibly-stale local values
(`db.go:1801-1821`); this is an undocumented new API whose semantics a caller must understand
to avoid mistaking a fast read's "not stale" for a verified cross-process check.

<a id="drift-46-public-multi-process-staleness-api-diverges-from-sqlite-auto"></a>
### Drift: Public Multi Process Staleness API Diverges From SQLite Auto Tracking
- **Category:** new-feature  -  **Severity:** medium
- **Affected functions:** `db.go` (`internal/btree/db.go:1868-1878`, `db.go:1855-1856`,
  `db.go:1903-1915`, `db.go:1798-1821`, `db.go:1017-1024`), `pager.go`
  (`internal/btree/pager.go:2308-2319`).

any-store exposes a caller-driven multi-process staleness protocol with no analogue in stock
SQLite. SQLite *automatically* increments the page-1 File Change Counter (offset 24) and
Schema Cookie (offset 40) inside the pager on every commit / schema change
(`pager_incr_changecounter`, `pager.c:6317`, `pager.c:6295-6304`). any-store instead
makes counter bumping opt-in: `WriteTx.MarkDataChanged()`/`MarkSchemaChanged()`
(`db.go:1868-1878`) merely set `tx.dataChanged`/`tx.schemaChanged` flags (`db.go:1855-1856`);
`Commit` forwards those flags to `pager.commit` (`db.go:1903-1915`), and `pager.commit` only
increments the on-disk counters when those flags are set (`pager.go:2308-2319`), with
`UpdateLocalCounters` (`db.go:1017-1024`) and the `IsDataStale`/`IsSchemaStale`/
`DiskFileChangeCounter`/`DiskSchemaCookie` accessors (`db.go:1798-1821`) forming the rest of
the surface. The consequence is a fundamentally different contract from SQLite's automatic
tracking: a caller who forgets to call `MarkDataChanged`/`MarkSchemaChanged` will leave the
cross-process change counters unbumped, so other connections' staleness checks silently fail
to observe the change.

A second divergence inside this surface: the begin-time counters behind
`DiskFileChangeCounter`/`DiskSchemaCookie` are read with the frame bound RAISED to the newest
committed frame (`pager.readHeaderCounters`), so a begin racing a commit — or a reader slot
pinned behind — reports counters its own snapshot cannot see. SQLite has no such raise:
`sqlite3BtreeBeginTrans` returns the cookie from the transaction's own page 1
(`sqlitec/src/btree.c:3785-3786`), and the `SQLITE_SCHEMA` reload consumes the cookie read
through that same transaction (`sqlitec/src/prepare.c:288,293`) — detection, reload, and
consumption are all snapshot-bounded. The raise is kept for cross-process staleness
detection, but judgments about snapshot contents must use the snapshot-bounded pair —
`ReadTx.SnapshotHeaderCounters`/`SnapshotSchemaCookie`, baked at begin from the tx's captured
`[minFrame, maxFrame]` window (a second bounded page-1 read is paid only when the detection
read was actually raised) — which restores the SQLite-aligned semantics. Consumers: the
staleness pass (reconcile + counter consumption — consuming the raised counters would mark
peer DDL as reconciled that the reconcile snapshot never contained, silently detaching later
write transactions from a peer-created index) and the index-visibility fast paths
(`visibleIndexes`, range/fts `visibleTo`, `vectorIndex.forTx` — judging with the raised
cookie admits an index whose namespace the snapshot cannot resolve, returning silently wrong
query results).

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
- **Category:** new-feature  -  **Severity:** medium
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
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `db.go:Open`
  (`internal/btree/pager.go:552-564 (Open new-DB path calls initNewDB before any txn)`),
  `pager.go:*pager.initNewDB` (`internal/btree/pager.go:763-865`).

SQLite defers page-1 creation to the first write transaction: `newDatabase` dirties page 1 via
`sqlite3PagerWrite(pP1->pDbPage)` (`btree.c:3518`) and never touches the file directly, so the
actual disk write of the new page 1 happens later through the normal commit path (a WAL frame or
rollback-journal transaction) when the enclosing write transaction commits. Go's
`Open` -> `p.open()` -> `initNewDB()` instead eagerly builds the page-1 image (DB header + empty
leaf-index page) into a local buffer and writes it straight to the main DB file via
`p.file.WriteAt(writeBuf, 0)` followed by `fdatasync(p.file)` (`pager.go:827-833`), all inside
`Open()` before any transaction begins and entirely bypassing the WAL. The consequence is that
initial database creation is not a transactional, WAL-mediated change as in SQLite -- the new
page 1 is durably committed to the main file outside of any transaction, so a crash mid-creation
or any reader/recovery logic that assumes page 1 first appears via the WAL sees a state SQLite
would never produce.

<a id="drift-68-pageslab-and-configpagecache-idempotent-versus-reconfigurabl"></a>
### Drift: pageSlab And ConfigPageCache Idempotent Versus Reconfigurable Setup
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `page_slab.go:*pageSlab.Init` (`internal/btree/page_slab.go:112-136`),
  `page_slab.go:ConfigPageCache`
  (`internal/btree/page_slab.go:112-136 (guard at 113,118); wrapper page_slab.go:246-248`),
  `page_slab.go:ConfigPageCache`
  (`internal/btree/page_slab.go:121-130 (nReserve/freeList for nPages==0); Put logic page_slab.go:185-193`).

C's `sqlite3PCacheBufferSetup` (`pcache1.c:271-291`) is fully reconfigurable: on every call while
initialized it unconditionally overwrites the entire global slab config (`szSlot`,
`nSlot`/`nFreeSlot`, `nReserve`, `pStart`, `pFree`, `bUnderPressure`, `pEnd`) and rebuilds the free
list, and it has two explicit disable branches up front -- `if(pBuf==0) sz=n=0;` and `if(n==0)
sz=0;` (`pcache1.c:274-275`) -- so passing `n==0` drives `szSlot` to 0 and effectively turns the
static page cache OFF, routing all allocations to `sqlite3Malloc`. Go's `pageSlab.Init` (the
worker behind `ConfigPageCache`) is permanently first-call-wins idempotent: a double-checked
`s.initialized.Load()` guard (`page_slab.go:113,118`) makes every subsequent `Init` a silent no-op
("If already initialized, this is a no-op."), so re-configuration with different parameters is
ignored, and there is no disable path -- `nPages==0` still sets `initialized=true` and is treated
as an initialized but empty slab rather than a disabled cache. The consequences are that the page
cache cannot be reconfigured after first setup and cannot be turned off via a zero-size config the
way SQLite allows; both diverge from SQLite's reconfigurable, disable-capable setup semantics.

<a id="drift-69-underpressure-drops-heap-nearly-full-fallback"></a>
### Drift: UnderPressure Drops Heap Nearly Full Fallback
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `page_slab.go:*pageSlab.UnderPressure` (`page_slab.go:199-201`).

C's `pcache1UnderMemoryPressure` (`pcache1.c:517-523`) has two branches: when a slab is configured
and the page fits a slot it returns the atomic `bUnderPressure` flag, but ELSE (no slab configured,
or a page too big for a slot) it falls back to `sqlite3HeapNearlyFull()`, still reporting memory
pressure from the global soft-heap-limit. Go's `UnderPressure()` (`page_slab.go:199-201`)
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
- **Affected functions:** `pager.go:*pager.freePage` (`internal/btree/pager.go:1536-1628`),
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
trunk's leaf array and records `dontWrite`/`setHasContent` by page number. The consequence is that
on an inconsistent header Go can pick the wrong free-list shape, and a stale cached header parse for
a freed leaf page can survive where SQLite would have discarded it.

<a id="drift-74-secure-delete-page-zeroing-on-free-unsupported"></a>
### Drift: secure_delete Page Zeroing On Free Unsupported
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.freePage` (`internal/btree/pager.go:1536-1628`).

C's `freePage2` honors `BTS_SECURE_DELETE`: when the pragma is enabled it fetches the freed page and
`memset(pPage->aData, 0, pPage->pBt->pageSize)` to scrub the deleted data (`btree.c:6849-6859`), and
it also suppresses the `PagerDontWrite` optimization in that mode so the zeroed page is actually
persisted (`btree.c:6919-6920`). Go's `freePage` has no secure-delete concept at all
(`pager.go:1536-1628`): freed leaf-page contents are never zeroed and `dontWrite` is applied
unconditionally (`pager.go:1580-1581`). The consequence is that deleted row/cell data physically remains in freed pages
on disk in Go where SQLite's secure_delete mode would have scrubbed it, and this gap is currently
undocumented.

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

<a id="drift-77-filechangecount-bumped-conditionally-not-unconditionally"></a>
### Drift: FileChangeCount Bumped Conditionally Not Unconditionally
- **Category:** changed-logic  -  **Severity:** medium
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
  (`/Users/roma/anytype/any-store/internal/btree/wal.go:2006-2009`).

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
### Drift: shmHashGet Skips Segment On Map IO Error
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*walIndex.shmHashGet` (`internal/btree/wal.go:1152`; the skip-on-error path is `wal.go:1171-1174`).

C's `walFindFrame` (`wal.c:3505`) aborts the entire frame lookup on any hash-segment map failure:
`rc = walHashGet(pWal, iHash, &sLoc); if(rc!=SQLITE_OK){ return rc; }` (`wal.c:3562-3565`), so an SHM
map/extend IO error (`walHashGet` -> `walIndexPage` -> `sqlite3OsShmMap`) propagates up to the reader. Go's
`shmHashGet` instead does `region, err := wi.shm.region(seg, true); if err != nil { continue }`
(`wal.go:1171-1174`), silently skipping that hash segment and continuing the scan. The consequence is that a
genuine SHM mapping/IO error is swallowed rather than reported: the lookup proceeds over the remaining
segments and may return a stale or missing-frame result where SQLite would have failed the read with the
underlying error.

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
- **Category:** changed-logic  -  **Severity:** low
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
- **Category:** changed-logic  -  **Severity:** low
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
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.doResetWAL`
  (`internal/btree/wal.go:1801-1806` writeHeader sets `checkpoint:0`; doResetWAL `wal.go:3833-3871` calls
  writeHeader; initHeaderStateLocked `wal.go:1754-1759`),
  `wal.go:*wal.writeHeader` (`internal/btree/wal.go:1801-1806`).

SQLite's `walRestartHdr` does `pWal->nCkpt++` on every WAL restart (`wal.c:2150`) and serializes it into the
WAL header (`wal.c:4072`), making the on-disk WAL
header "Checkpoint sequence number" field (offset 12) a monotonically increasing counter that advances on each
RESTART/TRUNCATE checkpoint and each writer-initiated log wrap. The Go reset path always constructs the header
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
root as the restart counter/salt drifts (97, 98) surfacing on the read-mark/reset path: the consequence is
that the `aReadMark[0]==0` invariant is broken on restart (a slot-0 reader normally views only the db file)
and the deterministic restart sequence/salt advances are lost. Note the recovery path (`wal.go:2006-2009`)
does correctly restore `aReadMark[0]=0` after clobbering all slots, unlike `reset()`.

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
- **Affected functions:** `page_slab.go` (`internal/btree/page_slab.go:53-66` initPageBufferPool,
  `:45-47` pageBufferPoolSize, `errors.go:87-90` ErrPageBufferPoolSizeMismatch, `db.go:422` enforcement at Open,
  `pager.go:612` second enforcement on header read),
  `page_slab.go` (`internal/btree/page_slab.go:41-47` pageBufferPool/pageBufferPoolSize,
  `:78-107` allocPageBuffer/freePageBuffer dispatch, `:160-166` + `:186-190` slab overflow recycles via default pool).

In the default (non-slab) mode, page-buffer allocation routes through a single process-global `sync.Pool`
(`pageBufferPool`, `page_slab.go:41-47`) shared across every DB in the process, keyed to one global page size held in
the atomic `pageBufferPoolSize`. `initPageBufferPool` (`page_slab.go:53-66`) CAS-sets that size on first init and
thereafter returns `ErrPageBufferPoolSizeMismatch` (`errors.go:87-90`) for any DB opened with a different `PageSize`;
`db.Open` enforces this unconditionally on every open (`db.go:422`), and the pager re-enforces it against the on-disk
page size after reading the header (`pager.go:612`). Because `useSlab` defaults false
(`pcache.go:36,133`, `pager.go:229-231`), all page buffers for all DBs come from and return to this one shared
pool, and even slab-mode OVERFLOW buffers recycle through the same default pool (`page_slab.go:160-166,186-190`). The
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
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pcache.go:*pcache.resetPage` (`pcache.go:420`).

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

---

## Audit-Discovered Drifts (2026-06-25)

A follow-up per-function C-vs-Go audit refresh (2026-06-25). Each drift below was discovered per source file, adversarially vetted by independent reviewers, and its cites re-confirmed against current source. The encryption/sqlcipher codec is excluded here (tracked separately).

<a id="drift-2026-06-25-25-first-opener-does-not-reset-a-stale-persisted-shm-trusts-checksum-vali"></a>
### Drift: First Opener Does Not Reset A Stale Persisted Shm; Trusts Checksum-Valid WAL-Index Header After A Crash Instead Of Forcing WAL Recovery
- **Category:** missing-feature  -  **Severity:** high  -  **Status:** RESOLVED 2026-07-10 (2026-07 pre-beta review): `newPlatformShm` now runs the complete `unixLockSharedMemory` first-attacher election (`os_unix.c:4860-4913`, 3.52.0) — `F_GETLK` probe of the DMS byte, then: `F_UNLCK` → DMS EXCLUSIVE + **unconditional** `Truncate(3)` + atomic downgrade to SHARED; `F_RDLCK` → join shared (a live process maintains the shm); `F_WRLCK` → BUSY back-off and re-probe, never a shared-join (the `os_unix.c:4864-4871` crash-mid-election race). Bounded 100×10ms in-open retry on contention (see [drift-2026-07-10-2](#drift-2026-07-10-2-dms-busy-retried-in-open-instead-of-surfacing)). Intra-process soundness rests on `openDBs` forbidding same-process double-open by file identity (fcntl record locks are per-(process,inode)); the shm file must never be open+closed by an attached process (any fd close drops the process's record locks). One mechanism correction to the original entry: the adopt fast paths cited below were in fact unreachable on a *fresh* attach, because `region(create=false)` never maps existing file content — every cold attach already funneled into `recoverLocked` (see [drift-2026-07-10-3](#drift-2026-07-10-3-region-create-false-never-maps-existing-content)); the hazard was latent behind that accident rather than live, and the election now makes the guarantee structural instead of accidental. Regression tests: `TestDMSFirstAttacherTruncatesStaleShm`, `TestDMSNonFirstAttacherDoesNotTruncate`, `TestDMSMidElectionBacksOff`, `TestDMSDowngradeFailureFailsOpen`, `TestStaleShmMxFrameBehind_CommittedTailVisible`, `TestStaleShmMxFrameAhead_UncommittedNotResurrected`, `TestStaleShmGarbageDiscardedOnReopen`, `TestShmStumpCrashBeforeRecoverySelfHeals`, plus the multi-process election tests (`multiprocess_dms_test.go`).
- **Affected functions:** `shm_mmap.go:newPlatformShm` (`internal/btree/shm_mmap.go:93`, `internal/btree/shm_mmap.go:105`), `wal.go:*wal.ensureHeaderInitialized` (`internal/btree/wal.go:1589`).

In SQLite's `unixLockSharedMemory`, the connection that finds the DMS byte unlocked (`F_GETLK` reports `F_UNLCK`, `os_unix.c:4876`) is the first attacher: it takes an EXCLUSIVE DMS lock (`unixShmSystemLock(..., F_WRLCK, ...)`, `os_unix.c:4893`) and then *unconditionally* truncates the `*-shm` file to 3 bytes (`robust_ftruncate(pShmNode->hShm, 3)`, `os_unix.c:4902`) before downgrading to SHARED — and any peer that observes the DMS already held EXCLUSIVE backs off with `SQLITE_BUSY` (`os_unix.c:4906`) so the truncation is never raced. The 3-byte stump is smaller than the wal-index header, so the next `readHeader` necessarily fails validation and `walIndexRecover` rebuilds the index from the authoritative WAL file. The header comment (`os_unix.c:4864-4871`) spells out exactly why the truncate is unconditional: the shm is volatile and may be torn or stale after a power failure or crash, and trusting it can corrupt the database. The Go `newPlatformShm` acquires the DMS lock (`internal/btree/shm_mmap.go:93`) but then only truncates the shm to the 3-byte marker when the file was *freshly created* (`info.Size() == 0`, `internal/btree/shm_mmap.go:105`); if a `*-shm` file persists from a process that died without running `mmapShm.close` (so the file was never unlinked), the new opener sees `Size() > 0`, skips the truncate entirely, and `wal.ensureHeaderInitialized` takes its fast path (`if hdr, valid := w.index.readHeader(); valid`, `internal/btree/wal.go:1589`) — trusting the stale-but-checksum-valid header's `mxFrame`/salts/hash slots instead of recovering from the WAL. The consequence is that Go can resurrect a crashed writer's uncommitted frames, miss the real committed tail, or read against a mismatched salt generation, where SQLite's mandatory first-open truncate-to-3 would have forced a full WAL rebuild.

<a id="drift-2026-07-10-2-dms-busy-retried-in-open-instead-of-surfacing"></a>
### Drift: DMS Election BUSY Retried In-Open Instead Of Surfacing To The Caller
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `shm_mmap.go:newPlatformShm` (the election retry loop).

SQLite's `unixLockSharedMemory` returns `SQLITE_BUSY` when it observes a peer mid-election (`os_unix.c:4906-4908`, 3.52.0) and relies on higher-level retry (the comment at `os_unix.c:4865`: "it will try again"). Go's shm attach is a one-shot call inside `wal.open` with no busy-handler plumbing above it, so `newPlatformShm` retries the probe in-line — bounded at 100×10ms, mirroring `pager.open`'s DB-flock retry loop — and only after exhaustion fails the open with an error wrapping `ErrBusy` (`errors.Is(err, ErrBusy)` holds). The exclusive-DMS window is microseconds (WRLCK → ftruncate(3) → downgrade, nothing else under it), so a single retry normally suffices; the observable divergence is worst-case ~1s of open latency under pathological contention instead of an immediate BUSY to the application.

<a id="drift-2026-07-10-3-region-create-false-never-maps-existing-content"></a>
### Drift: region(create=false) Never Maps Existing Shm Content On A Fresh Attach
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `shm_mmap.go:*mmapShm.region` (the `!create` early return), `wal.go:*walIndex.readHeader` (first caller on the open path).

C `unixShmMap` with `isWrite==0` still maps *existing* regions of the shm file — a fresh attacher can read a peer-written (or crash-persisted) wal-index header through it. Go's `mmapShm.region(index, create=false)` returns "region not available" for any region not already mapped *by this instance* and never maps existing file content on demand. Consequence: on a fresh attach, `readHeader` (which starts with `region(0, false)`) always reports invalid, so `wal.open`'s adopt fast paths are dead on a cold start and every fresh multi-process attach funnels into `recoverLocked`. This accident is what kept the crash-stale-shm hazard (drift-2026-06-25-25) latent rather than live before the DMS election landed; it also means a cold attach pays a full WAL scan where C would adopt a live peer's header. **Sequencing constraint:** any future alignment of `region()` with `unixShmMap`'s map-existing behavior is safe ONLY because the DMS first-attacher election (2026-07-10) now provides the structural stale-shm guarantee — do not revert the election. Adjacent follow-up (separate issue): `recoverLocked` publishes a reset `aReadMark` without taking the reader-slot locks, so recovery triggered by a late attach while peers hold read slots could stomp a live reader's mark; C's `walIndexRecover` resets each `aReadMark[i]` only after taking `WAL_READ_LOCK(i)` EXCLUSIVE for that slot and leaves BUSY (live-reader-held) slots untouched (`wal.c:1575-1587`, 3.52.0). **RESOLVED (2026-07-17):** all SHM read-mark publication now goes through `walIndex.shmPublishReadMarks`, which either relies on the caller's slots-1-4 exclusive locks (RESTART/TRUNCATE path, `wal.c:2361`) or per-slot try-locks that skip BUSY slots (recovery/open paths, `wal.c:1573-1588`); the same pass removed the larger hazard of `shmWriteCkptInfo` bulk-rewriting marks on every checkpoint's counter publish — checkpoints now publish `nBackfill`/`nBackfillAttempted` as single words (`shmWriteNBackfill`/`shmWriteNBackfillAttempted`, `wal.c:2331`/`wal.c:2268`), restoring the `wal.c:367-370` invariant. Regression: `TestMultiProcessCheckpointPreservesPeerReadMark`.

<a id="drift-2026-06-25-01-transient-busy-locked-equivalent-errors-are-made-sticky-in-b-rc-so-a-b"></a>
### Drift: Transient BUSY/LOCKED-Equivalent Errors Are Made Sticky In b.rc So A Backup Cannot Be Retried Or Resumed
- **Category:** error-handling  -  **Severity:** medium
- **Affected functions:** `backup.go:*Backup.Step` (`internal/btree/backup.go:191-193`, `backup.go:199-203`, `backup.go:218-222`, `backup.go:248-251`).

SQLite distinguishes transient contention from real failure: `isFatalError` (`backup.c:217-219`) returns false for `SQLITE_BUSY` and `SQLITE_LOCKED`, and `sqlite3_backup_step` gates its whole copy loop on `if(!isFatalError(rc))` (`backup.c:329-330`). Although the step still records `p->rc = rc` at the end (`backup.c:558`), a BUSY/LOCKED value is non-fatal, so a subsequent `sqlite3_backup_step` re-enters the loop and resumes copying from `iNext` — the caller simply calls step again to retry. The Go port has no `isFatalError` equivalent: `Step` short-circuits on *any* non-nil, non-`ErrBackupDone` rc (`backup.go:191-193`) and records every failure verbatim into the sticky `b.rc` at each site — source `BeginRead` (`backup.go:199-203`), destination `BeginWrite` (`backup.go:218-222`), and the per-page `getPageReader`/`onePage` copy (`backup.go:241-251`, e.g. `backup.go:248-251`) — so a momentary destination write-lock, a `BUSY_SNAPSHOT` race, or a busy-recovery error is treated identically to corruption. The consequence is that any transient busy/lock during a step permanently poisons the backup: every later `Step` re-returns the same error and `Finish` rolls the destination back, forcing callers to tear down and restart the entire copy from page 1 instead of retrying the step as SQLite allows.

<a id="drift-2026-06-25-04-point-lookup-mutation-descent-loops-omit-sqlite-s-btcursor-max-depth-b"></a>
### Drift: Point-Lookup And Mutation Descent Loops Omit SQLite's BTCURSOR_MAX_DEPTH Bound
- **Category:** error-handling  -  **Severity:** medium
- **Affected functions:** `btree.go:AppendValue` (`internal/btree/btree.go:1076`), `btree.go:Put` (`internal/btree/btree.go:1217`), `btree.go:Delete` (`internal/btree/btree.go:2576`), `btree.go:insertIntoParent` (`internal/btree/btree.go:2438`), `btree.go:descendChild` (`internal/btree/btree.go:123`).

Go's point-lookup and mutation paths walk the tree with unbounded `for` loops that carry no depth counter. `AppendValue` (the engine behind `Get`/`Has`) spins `for {` until it hits a leaf (`internal/btree/btree.go:1076`), while `Put` (`internal/btree/btree.go:1217`), `Delete` (`internal/btree/btree.go:2576`), and `insertIntoParent` (`internal/btree/btree.go:2438`) loop on `for pg.header.isInterior()`. Each iteration descends via `descendChild` (`internal/btree/btree.go:123`), which validates only that the child pgno is in range (`childPgno != 0 && childPgno <= readerDbSizeBound`) and that a descended interior page has `cellCount >= 1`; neither check tracks how many levels have been traversed. SQLite instead caps cursor descent at `BTCURSOR_MAX_DEPTH` (20, `btreeInt.h:497`): `moveToChild` returns `SQLITE_CORRUPT_BKPT` once `pCur->iPage >= BTCURSOR_MAX_DEPTH-1` (`btree.c:5448`), and the inlined descent in `sqlite3BtreeIndexMoveto` applies the identical guard (`btree.c:6227`), so any chain deeper than 20 levels — including a cyclic interior loop — is rejected as corruption rather than followed indefinitely.

The consequence is that a corrupt or malicious database whose interior pages form a cycle of in-bounds page numbers (each cell passing `descendChild`'s pgno-range and `nCell >= 1` checks) makes a plain `Get`/`Has`/`Put`/`Delete` spin forever — a hang/DoS — whereas SQLite returns `SQLITE_CORRUPT` after 20 levels; the `pathBuf [8]pathEntry` stacks in `Put`/`Delete`/`insertIntoParent` additionally reallocate and grow without bound on every loop, leaking memory until the process is killed.

<a id="drift-2026-06-25-06-listnamespaces-swallows-every-cursor-first-error-as-empty-master-table"></a>
### Drift: ListNamespaces Swallows Every Cursor First Error As Empty Master Table
- **Category:** error-handling  -  **Severity:** medium
- **Affected functions:** `db.go:ListNamespaces` (`internal/btree/db.go:1330-1332`).

`ListNamespaces` opens a read-only cursor over the master B-tree (root page 1) and calls `cursor.First()`; on a non-nil error it unconditionally does `return nil, nil // empty master table` (`internal/btree/db.go:1330-1332`). But `Cursor.First` does not signal emptiness through an error — an empty root returns `nil` with `c.valid` left false, while a non-nil error is exactly the failure code from `getPage(c.bt.rootPage)`, e.g. `ErrCorrupt` or an underlying I/O error from reading page 1 (`internal/btree/btree.go:3477-3485`). Collapsing that error to `(nil, nil)` therefore makes a genuine read failure indistinguishable from a legitimately empty database.

SQLite keeps the two cases strictly apart. `sqlite3BtreeFirst` calls `moveToRoot` and only the `SQLITE_EMPTY` result sets `*pRes=1` and returns `SQLITE_OK`; any other `rc` is propagated to the caller (`btree.c:5676-5692`). `moveToRoot` returns the raw error code from `getAndInitPage` when the root page cannot be read (`btree.c:5574-5579`), and `getAndInitPage` returns `SQLITE_CORRUPT_BKPT` for an out-of-range or corrupt page (`btree.c:2386-2389`), so corruption and I/O errors always reach the caller rather than masquerading as an empty table. The consequence is that a corrupt or unreadable master B-tree (page 1) makes `ListNamespaces` return `(nil, nil)` — a successful, empty result — instead of surfacing `ErrCorrupt`/I-O error. A caller enumerating namespaces concludes the database has no namespaces and may treat live data as absent (e.g. re-create namespaces, skip migration, or report the DB as empty), masking corruption that SQLite would have reported.

<a id="drift-2026-06-25-12-verifyintegrity-bounds-the-sweep-with-the-live-writer-header-pager-hea"></a>
### Drift: VerifyIntegrity Bounds The Sweep With The Live Writer Header Instead Of The Read-Tx Snapshot Page Count
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `integrity_sweep.go:*DB.VerifyIntegrity` (`internal/btree/integrity_sweep.go:101-103`).

`VerifyIntegrity` opens a read transaction and then drives the page loop from `total := db.pager.header.DatabaseSize` (`internal/btree/integrity_sweep.go:101`), the live writer's global header page count, while reading each page against the captured snapshot frame `maxFrame := tx.walMaxFrame` (`internal/btree/integrity_sweep.go:102-103`). The two bounds come from different points in time: the sweep ceiling tracks the writer, but `readRawPage(pgno, maxFrame)` resolves bytes against the reader's WAL snapshot. SQLite derives the per-transaction page count from `sqlite3PagerPagecount`, which returns `pPager->dbSize` and asserts a read transaction is open (`pager.c:3925-3928`); that field is seeded for the reader by `sqlite3WalDbsize` -> `pWal->hdr.nPage` (`wal.c:3655-3657`, consumed at `pager.c:3292`), i.e. the snapshot's own page count, so the ceiling and the page reads always describe the same database image. This package already exposes that snapshot-consistent bound via `ReadTx.DatabaseSize` (`internal/btree/db.go:1774-1775`) and `pager.readerDbSizeBound(tx.cache)` (`internal/btree/pager.go:1946-1951`), but the sweep bypasses both and reads the unsynchronized live header. The consequence is a data race and snapshot inconsistency: if a concurrent writer or peer process grows the DB during the sweep, `total` exceeds the snapshot's page count and `readRawPage` finds no WAL frame at or below `maxFrame` for the new pages, falling back to a stale/past-EOF `readDBPage` and recording phantom `SweepError`s, while a shrink reads freed pages or skips live ones — false corruption that can drive callers into destructive recovery.

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
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `backup.go:*Backup.Step` (`internal/btree/backup.go:206-214`), `pager.go:2308-2319`.

`Backup.Step` detects external (other-process) writes and forces a full restart (`b.iNext = 1`) purely on `rtx.diskFileChangeCounter != b.lastFCC` (`backup.go:206-214`), with a comment asserting "Page-1 FileChangeCounter changes on every write commit, including from other processes." That assumption is false here: `pager.commit` bumps `FileChangeCount` only when the caller-supplied `dataChanged` flag is true (`pager.go:2308-2319`), and the btree mutation entry points `Put`/`Delete` (`db.go:1881-1896`) do not auto-set it — it is opt-in via `WriteTx.MarkDataChanged` (`db.go:1868-1872`), the documented [drift-77](#drift-77-filechangecount-bumped-conditionally-not-unconditionally) behavior. SQLite's `pager_write_changecounter` bumps unconditionally on the first write of a transaction (`pager.c:3084`, `pager.c:6363`), so `sqlite3BackupRestart`/`backupUpdate` (`backup.c:661-707`) reliably restart. The consequence is that a peer process committing real data-page changes without `MarkDataChanged` leaves page-1 `FileChangeCount` unchanged; a concurrent cross-process `Backup` then fails to restart after already copying pre-write versions of some pages while later Steps copy post-write versions — yielding a silently inconsistent (torn) destination that passes `Finish()` without error.

<a id="drift-2026-06-25-02-step-after-errbackupdone-re-enters-and-re-runs-finalize-and-can-copy-n"></a>
### Drift: Step After ErrBackupDone Re-Enters And Re-Runs Finalize Instead Of Being A No-Op
- **Category:** changed-logic  -  **Severity:** low
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
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.updateLeafCell` (`internal/btree/btree.go:1432`), `btree.go:leafSplitPoint` (`internal/btree/btree.go:312`), `balance.go:*btree.balanceNonroot` (`internal/btree/balance.go:194`).

When an in-place update grows a value so the replacement cell no longer fits, Go's `updateLeafCell` does not route through the sibling-gathering balancer the insert path uses. After collecting the leaf's cells and confirming they overflow one page (`internal/btree/btree.go:1536`), it performs an inline 2-way median split: `leafSplitPoint(cells, pageUsable)` (`internal/btree/btree.go:1542`) picks a single split index targeting ~2/3 left-fill (`internal/btree/btree.go:312-345`), the page is rebuilt into the original plus one freshly allocated `rightPg`, and the separator is pushed up via `insertIntoParentWithPath` / `insertIntoParent` (`internal/btree/btree.go:1566-1568`). This always manufactures a new page even when an immediate sibling has free room. SQLite's `sqlite3BtreeInsert` treats the grow-an-existing-entry case identically to any other overflow: it `dropCell`s the old cell (`btree.c:9632`), re-inserts the new one, and when the page overflows calls `balance(pCur)` (`btree.c:9665`), whose `balance_nonroot` redistributes cells across up to three siblings (the path any-store itself reproduces in `balanceNonroot`, `internal/btree/balance.go:194`) and only adds a page when the gathered siblings genuinely cannot absorb the cells. The consequence is that value-growing updates produce a different, less-balanced tree shape than both SQLite and any-store's own insert path -- they unconditionally allocate a new page, raising page count and lowering fill factor -- a correctness-preserving but structural/performance divergence.

<a id="drift-2026-06-25-07-acquireexclusivedblock-is-a-no-op-on-plan9-wasip1-removing-the-cross-p"></a>
### Drift: acquireExclusiveDBLock Is A No-Op On plan9/wasip1, Removing The Cross-Process Double-Open Guard That Unix And Windows Enforce
- **Category:** missing-feature  -  **Severity:** low
- **Affected functions:** `dbfile_lock_other.go:acquireExclusiveDBLock` (`internal/btree/dbfile_lock_other.go:15`), `dbfile_lock_unix.go:acquireExclusiveDBLock` (`internal/btree/dbfile_lock_unix.go:46`), `dbfile_lock_windows.go:acquireExclusiveDBLock` (`internal/btree/dbfile_lock_windows.go:29`), `pager.go:*pager.open` (`internal/btree/pager.go:526`).

For file-backed databases that run in forced in-process mode (heap-backed WAL-index SHM, which is process-local), `pager.open` defends against a second OS process opening the same DB by taking a non-blocking whole-file EXCLUSIVE lock via `acquireExclusiveDBLock` (`internal/btree/pager.go:526`); on `ErrBusy` it distinguishes a same-process double-open already recorded in the `openDBs` registry (`internal/btree/db.go:549`, returning `ErrDatabaseOpen`) from a different-process holder (returning `ErrInProcessLocked`, `internal/btree/db.go:539`). On unix this is a real `flock(LOCK_EX|LOCK_NB)` (`internal/btree/dbfile_lock_unix.go:46`) and on Windows a real `LockFileEx` exclusive lock (`internal/btree/dbfile_lock_windows.go:29`), mirroring the OS-level whole-file lock that SQLite's VFS layers apply in `unixLock` (`os_unix.c:1866`) and `winLock` (`os_win.c:3321`). On the catch-all `!unix && !windows` targets (plan9, wasip1/wasm/js), `acquireExclusiveDBLock` is a bare `return nil` stub (`internal/btree/dbfile_lock_other.go:15`), and the file comment asserts these platforms "have no second-process notion" (`internal/btree/dbfile_lock_other.go:8`). The consequence is that on plan9 and wasip1/wasm — where two OS processes (or two WASI instances over a shared preopened directory) genuinely can open the same file-backed DB — the exclusive-lock guard silently succeeds for both, the `openDBs` registry only catches same-process reopens, and each side runs its own process-local heap SHM and WAL-index state so concurrent writes can corrupt the database, exactly the outcome unix/windows prevent via `ErrInProcessLocked`.

<a id="drift-2026-06-25-08-tryupgradedblockexclusive-can-silently-lose-the-caller-s-shared-lock-w"></a>
### Drift: tryUpgradeDBLockExclusive Can Silently Lose The Caller's Shared Lock When The Flock Conversion Fails
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `dbfile_lock_unix.go:tryUpgradeDBLockExclusive` (`internal/btree/dbfile_lock_unix.go:55-65`), `dbfile_lock_unix.go:flockNB` (`internal/btree/dbfile_lock_unix.go:74-85`), `pager.go:*pager.close` (`internal/btree/pager.go:3277-3284`).

To prove "last client" at close, the Go pager upgrades its lifetime DB-file lock from shared to exclusive by issuing a single `syscall.Flock(fd, LOCK_EX|LOCK_NB)` through `flockNB` (`internal/btree/dbfile_lock_unix.go:78`), with `tryUpgradeDBLockExclusive` mapping `ErrBusy` to `(false, nil)` (`internal/btree/dbfile_lock_unix.go:60-61`). Per BSD `flock(2)` semantics a shared→exclusive conversion on one open file description is not atomic: the kernel first drops the existing `LOCK_SH` and only then tries to install `LOCK_EX`, so when the exclusive grant cannot be satisfied immediately the `LOCK_NB` attempt fails *after* the shared lock has already been released. The caller therefore observes `(false, nil)` and treats itself as a non-last client (`internal/btree/pager.go:3277-3284`), unaware that its DB-file SHARED lock is now gone. SQLite's flock VFS sidesteps this entirely: in `flockLock` every lock at or above `SHARED_LOCK` is physically an exclusive `flock` (`os_unix.c:2682-2685`), so once any lock is held an upgrade just bumps the in-memory level and returns `SQLITE_OK` without re-issuing `robust_flock` (`os_unix.c:2696-2701`), and `flockCheckReservedLock` documents that the connection's lock is already exclusive (`os_unix.c:2643-2656`); the held lock is never momentarily dropped, and SQLite's real DB-file close protocol additionally uses atomic fcntl byte-range conversions that do not release the prior lock on failure. The consequence is that a non-last client whose upgrade fails returns `isLastClient=false` but has silently dropped its lifetime DB-file shared lock, opening a window (until its fd closes) in which a peer process's concurrent `tryUpgradeDBLockExclusive` sees no contention, wrongly declares itself last client, and truncates the WAL / unlinks the shm while this client is still attached — re-opening the early-truncation / orphan-inode race the DB-file lock protocol exists to prevent.

<a id="drift-2026-06-25-09-downgradedblocktoshared-can-leave-the-db-file-fully-unlocked-or-spurio"></a>
### Drift: downgradeDBLockToShared Can Leave The DB File Fully Unlocked Or Spuriously Return ErrBusy
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `dbfile_lock_unix.go:downgradeDBLockToShared` (`internal/btree/dbfile_lock_unix.go:67-70`), `dbfile_lock_unix.go:flockNB` (`internal/btree/dbfile_lock_unix.go:74-85`).

To convert an exclusive DB-file lock back to shared, the Go code calls `downgradeDBLockToShared`, which simply issues `flockNB(fd, syscall.LOCK_SH)` (`internal/btree/dbfile_lock_unix.go:67-70`); that wrapper performs a real `syscall.Flock(fd, LOCK_SH|LOCK_NB)` and maps `EWOULDBLOCK`/`EAGAIN` to `ErrBusy` (`internal/btree/dbfile_lock_unix.go:74-85`). On BSD `flock` a lock-mode conversion is not atomic — the kernel first drops the existing exclusive lock and then tries to acquire the shared one — so with `LOCK_NB` set the re-acquire can fail if another process grabs a lock in the window, leaving the file with no lock at all while the call returns `ErrBusy`. SQLite's `flockUnlock` deliberately avoids any syscall for an exclusive→shared down-conversion: it observes that an exclusive lock already subsumes shared access and merely updates the in-memory state, `pFile->eFileLock = SHARED_LOCK; return SQLITE_OK;` (`os_unix.c:2747-2751`), reserving an actual `robust_flock(LOCK_UN)` only for the full `NO_LOCK` release path (`os_unix.c:2734-2763`), so its downgrade can never fail and never relinquishes the kernel lock.

The consequence is that a caller invoking downgrade after a successful exclusive upgrade may end up with the DB file silently unlocked (losing the mutual exclusion that serializes last-client-unlink against new openers) while receiving an unexpected `ErrBusy` from an operation that semantically cannot be busy; this is currently latent because no caller invokes `downgradeDBLockToShared` today, but the hazard is inherited by any future use of this cross-platform lock API (also stubbed in `dbfile_lock_windows.go`).

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

When walking an interior page, Go's `checkTreePage` anchors the expected subtree depth on the FIRST child it recurses into (`if i == 0 { depth = childDepth }`, `internal/btree/integrity.go:489-490`) and thereafter only compares against that fixed anchor: every later child whose depth differs emits a separate `"child page depth differs"` message without ever updating `depth` (`internal/btree/integrity.go:491-493`), and the right child is checked against the same un-updated anchor at the end (`internal/btree/integrity.go:508-511`). SQLite instead anchors `depth` on the right child first (`depth = checkTreePage(...)`, `src/btree.c:10935`) and, in the per-cell loop, RE-anchors after each mismatch: `d2 = checkTreePage(...)`; `if( d2!=depth ){ checkAppendMsg(pCheck, "Child page depth differs"); depth = d2; }` (`src/btree.c:11000-11005`). Because SQLite reassigns `depth = d2` it treats a single outlier child as the new expected depth, so a lone divergent child yields exactly one message; Go, holding the first child's depth as the immutable reference, reports against every subsequent divergence. The consequence is that on a corrupt interior page whose first-anchored child carries the outlier depth, Go emits one redundant "child page depth differs" message per remaining child (and the right child) where SQLite emits a single message and re-anchors, draining the bounded `maxErrors` budget faster and potentially suppressing other distinct corruption diagnostics later in the walk; it is a read-only diagnostic divergence with no data-correctness impact.

<a id="drift-2026-06-25-13-checksum-sweep-fires-the-onerror-errcksummismatch-hook-even-for-plain-"></a>
### Drift: Checksum Sweep Fires The OnError(errCksumMismatch) Hook Even For Plain File-I/O Read Errors
- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `integrity_sweep.go:*DB.VerifyIntegrity` (`internal/btree/integrity_sweep.go:109`), `integrity_sweep.go:verifyCksumPage` (`internal/btree/integrity_sweep.go:132`).

In `VerifyIntegrity`'s checksum branch, any non-nil `SweepError` returned by `verifyCksumPage` (`internal/btree/integrity_sweep.go:109`) triggers the codec's OnError hook with a hard-coded `errCksumMismatch` (`internal/btree/integrity_sweep.go:113`), regardless of which failure produced it. But `verifyCksumPage` returns two distinct error shapes: a genuine trailer mismatch carries `Kind: IntegrityChecksumMismatch` (`internal/btree/integrity_sweep.go:149`, whose `want != got` compare mirrors the codec at `codec_cksum.go:96`), while a failed raw-page read returns `&SweepError{PageNo: pgno, Inner: err}` with `Kind` left at the zero value `IntegrityKindUnknown` (`internal/btree/integrity_sweep.go:132-134`). The sweep loop never inspects the kind before calling `c.fire`, so a transient file-I/O read fault is reported through the same `errCksumMismatch` notification as an actual corrupt page.

SQLite's cksumvfs keeps the two cases strictly separate: `cksmRead` computes and compares the trailer only when the underlying `xRead` succeeded (`rc==SQLITE_OK`), surfacing a real trailer mismatch as `SQLITE_IOERR_DATA` (`cksumvfs.c:451`, `cksumvfs.c:455`), whereas a plain read failure propagates the original `xRead` error code (e.g. `SQLITE_IOERR_READ`) and never reaches the checksum comparison at all — C never relabels an I/O fault as a data/checksum fault. The consequence is that subscribers wired to the codec OnError hook (anystore corruption handlers) receive a checksum-mismatch event for a transient read error during a sweep, potentially marking a healthy database corrupt or kicking off recovery; severity is low because the collected `SweepError` itself still carries the correct kind and inner error, so only the side-channel OnError notification is misclassified.

<a id="drift-2026-06-25-14-page-1-db-header-bytes-0-99-is-excluded-from-both-checksum-and-aead-in"></a>
### Drift: Page-1 DB Header Bytes 0..99 Excluded From Both Checksum And AEAD Integrity Coverage

- **Category:** missing-feature  -  **Severity:** low
- **Affected functions:** `integrity_sweep.go:verifyCksumPage` (`internal/btree/integrity_sweep.go:131`), `integrity_sweep.go:verifyAEADPage` (`internal/btree/integrity_sweep.go:157`), `codec.go:encryptPageWithCodec` (`internal/btree/codec.go:149`).

For page 1 the Go integrity sweep starts its hash at `dbHeaderSize` rather than 0: `verifyCksumPage` sets `start = dbHeaderSize` when `pgno == 1` (`internal/btree/integrity_sweep.go:137-138`) and hashes only `raw[start:bodyEnd]` (`internal/btree/integrity_sweep.go:144`), so the first 100 bytes of page 1 never enter the XXH3-128. That exclusion is structural, baked into the codec write path: `encryptPageWithCodec` copies the first `dbHeaderSize` bytes as a plaintext prefix and only encrypts/authenticates the remainder (`internal/btree/codec.go:153-158`), and the cksumCodec documents that "the page-1 DB header is therefore NOT covered by the per-page checksum" (`internal/btree/codec_cksum.go:26-31`). For the AEAD codec, `verifyAEADPage` decrypts only that same post-prefix slice (`internal/btree/integrity_sweep.go:164`), so the header bytes carry no authentication tag and the SQLite-format invariants there are instead checked only by `dbHeader.deserialize` at open time.

SQLite's cksumvfs, by contrast, checksums the entire page including page 1's 100-byte database header: the read path computes `cksmCompute((u8*)zBuf, iAmt-8, cksum)` starting at offset 0 of the page buffer (`cksumvfs.c:450`), and the `verify_checksum()` SQL function does the same with `cksmCompute(data, nByte-8, cksum)` over the whole blob (`cksumvfs.c:353`); `cksmCompute` walks from `a[0]` to `a[nByte]` with no header carve-out (`cksumvfs.c:299`). The consequence is that bit-rot or tampering confined to the page-1 header — e.g. a flipped DatabaseSize or freelist-trunk pointer that still parses as structurally valid — passes `VerifyIntegrity` clean, whereas an equivalent SQLite cksumvfs database would flag it; for the AEAD codec this is an authentication gap on file-structure metadata.

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
- **Category:** error-handling  -  **Severity:** low
- **Affected functions:** `page.go:*dbHeader.deserialize` (`internal/btree/page.go:262-264`).

`dbHeader.deserialize` reads the file-format write-version (byte 18) and read-version (byte 19) verbatim into `h.WriteVersion = buf[18]` / `h.ReadVersion = buf[19]` without any range check (`internal/btree/page.go:262-264`). It validates the magic string, the page-size byte, and the embedded-payload-fraction constants at offsets 21-23, but never inspects the version bytes themselves. SQLite, in `lockBtree`, treats these bytes as a compatibility gate: `page1[18]>2` forces `BTS_READ_ONLY` and `page1[19]>2` does `goto page1_init_failed`, which has already set `rc = SQLITE_NOTADB`, so an unknown read-version makes the open fail (`btree.c:3314-3327`, with the `SQLITE_OMIT_WAL` build using the stricter `>1` bounds at `btree.c:3315-3320`).

The consequence is that a database whose read-version byte is greater than 2 (a future or unknown on-disk format) is opened and read by Go as if it were a current file rather than being refused the way SQLite refuses it with `SQLITE_NOTADB`, and a write-version greater than 2 is not forced read-only, so the forward-compatibility / wrong-format protection SQLite performs at open is absent here.

<a id="drift-2026-06-25-22-slab-get-has-no-per-request-size-guard-and-its-initialized-pagesize-va"></a>
### Drift: Slab Get Has No Per-Request Size Guard And Its Initialized PageSize Validator Is Dead Code
- **Category:** missing-feature  -  **Severity:** low
- **Affected functions:** `page_slab.go:pageSlab.Get` (`internal/btree/page_slab.go:142-166`), `page_slab.go:pageSlab.Initialized` (`internal/btree/page_slab.go:210-217`), `db.go:Open` (`internal/btree/db.go:418-420`).

Go's `pageSlab.Get()` (`internal/btree/page_slab.go:142-166`) takes no size argument: it pops the last buffer off `freeList`, or on exhaustion returns `make([]byte, s.pageSize)` — always the slab's own configured `pageSize` (`internal/btree/page_slab.go:160,165`). `allocPageBuffer(pageSize, useSlab)` (`internal/btree/page_slab.go:78-86`) honors the caller's requested `pageSize` only on the non-slab `sync.Pool`/`make` branch and discards it entirely when `useSlab` is true, calling `globalPageSlab.Get()` with no size check. The one validator that could catch a mismatch, `pageSlab.Initialized(pageSize)` (`internal/btree/page_slab.go:210-217`), does compare the requested size against `s.pageSize`, but its doc comment names callers in `pcache.initBulk()` and `create()` that do not exist — it has no non-test callers and is dead code. The only size-related enforcement at open is the all-or-nothing `UsePageSlab && !globalPageSlab.initialized.Load()` guard in `Open` (`internal/btree/db.go:418-420`), which checks that the slab is initialized but never that its page size matches the DB being opened.

SQLite's `pcache1Alloc(nByte)` (`pcache1.c:341-374`) instead guards every request with `if( nByte<=pcache1.szSlot )` (`pcache1.c:344`): only requests that fit the slot size are served from the slab, and anything larger falls through to `sqlite3Malloc(nByte)` (`pcache1.c:361`) at the correct size. The consequence is that if a process configures the slab for one page size and then opens a DB with a different page size (an existing file whose header page size differs, or a second DB), the pager receives buffers sized for the slab's page rather than its own — a too-small buffer triggers index-out-of-range panics on page read/write while a too-large one wastes memory — with no per-request size guard and no graceful heap fallback at the correct size as SQLite provides.

<a id="drift-2026-06-25-24-truncate-always-full-scans-all-hash-buckets-sqlite-uses-imaxkey-to-sca"></a>
### Drift: Truncate Always Full-Scans All Hash Buckets; SQLite Uses iMaxKey To Scan Only Affected Slots
- **Category:** performance  -  **Severity:** low
- **Affected functions:** `pcache.go:*pcache.truncate` (`internal/btree/pcache.go:713-748`).

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

`mmapShm.region` returns an already-mapped region (`shm_mmap.go:120-121`) but, when `create=false` and the region is not yet mapped in this process, returns an error (`shm_mmap.go:124-125`) instead of mapping an existing on-disk region or returning `(nil, OK)` for a region past EOF. SQLite's `unixShmMap` (`os_unix.c:5159-5230`) maps any region already present in the shm file even when not extending, and returns `(NULL, SQLITE_OK)` for a not-yet-allocated region. The hot WAL read path works around this by always passing `create=true` (`wal.go:1171`), so impact is confined to best-effort callers (`shmClearHash`, `shmCleanupFromFrame`, `shmReadCkptInfo`). The consequence is that a peer process's freshly-extended region is unreachable to a `create=false` caller, and the mapping/return semantics diverge from the ported function.

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
