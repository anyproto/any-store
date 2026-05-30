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

### Drift

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

### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| Format | `[4B child] [varint(nPayload)] [key] [ovfl?]` | `[4B child] [varint(keyLen)] [key] [ovfl?]` |
| Semantics | Key-only (nPayload = key size) | Key-only (keyLen) |

**Classification: Intentional** -- The formats are semantically identical. The varint
means the same thing (the key size). Interior cells are key-only in both implementations.

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

### Drift

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

### Drift

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

### Drift

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

### Drift

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

### Drift Summary

| Aspect | Classification |
|--------|---------------|
| Different magic number | **Intentional** -- prevents cross-tool opening |
| Different version number | **Intentional** -- distinct format lineage |
| Big-endian only checksums | **Intentional** -- simplifies implementation |
| Go map for same-process reads | **Divergent** -- O(1) map vs hash table scan |
| Heap SHM fallback | **Divergent** -- enables non-mmap platforms |
| WAL undo (`sqlite3WalUndo`) | **Missing** -- Go uses pager-level rollback instead |
| First-opener `ftruncate(shm, 3)` marker | **Aligned** (resolved 2026-04-22) -- `newPlatformShm` truncates a fresh shm to 3 B after the DMS lock; see item 2 below |
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

   See `internal/btree/dbfile_lock.go` for the flock wrappers. The
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

2. **`robust_ftruncate(hShm, 3)` marker — resolved 2026-04-22.**
   `newPlatformShm` now truncates a freshly created shm file to 3 bytes
   immediately after acquiring the DMS lock — matches SQLite's
   `os_unix.c:4902`. The marker is smaller than `walIndexHdrSize`
   (48 bytes), so subsequent openers that mmap a 3-byte file know it's
   blank-slate. The first `region(0, true)` call grows the file to
   `shmRegionSize`, so the marker state is transient.

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

### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| Isolation mechanism | WAL frame lookup bounded by `mxFrame` | WAL frame lookup bounded by `walMaxFrame` — matches SQLite |
| Cache ownership | Per-connection private caches | Per-connection private caches — matches SQLite |
| Writer dirty pages | In-cache with `PGHDR_DIRTY` flag | In `writerCache` dirty list via `writerCache.fetch()` — matches SQLite |
| Overflow reads | Per-connection cache | Per-connection reader cache — matches SQLite |
| Concurrent readers | Multiple readers, each with own `mxFrame` | Same, each with own `walMaxFrame` and private cache |

**Classification: Structural** -- The Go implementation now matches SQLite's per-connection
cache model. Each reader has its own private cache, and the writer has its own cache.
No mutex is needed on the page cache since each cache is accessed by a single goroutine.
The writer uses `writerCache` directly for dirty-page access, matching SQLite's PCache approach.

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

**Classification: Divergent** -- Both implementations protect against corruption, but
the specific cap values differ. SQLite's `nCell/usableSize > nPage` check is more
precise (validates against actual database size), while Go uses a fixed 1 GB cap.
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

**Two diagnostic-quality drifts in `checkList`** (corrupt-input only, no effect on healthy
DBs): on an over-large trunk leaf-count it `return`s (aborting the whole freelist walk)
where SQLite reports and continues to the next trunk (btree.c:10778) — so later trunk pages
are then reported as orphans; and it emits the size/overflow count-mismatch message
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
| WAL truncation | `sqlite3WalSavepointUndo()` truncates WAL | No WAL truncation on rollback |
| Page restoration | Reads from sub-journal file | Copies from in-memory maps |
| Merge on release | Sub-journal frames retained | Page maps merged to parent |
| Header restoration | Implicit via page 1 journal | Explicit `header` snapshot |

**Classification: Divergent** -- Both achieve the same semantics (nested savepoints
with rollback/release), but Go uses in-memory storage while SQLite uses disk-based
sub-journals. This makes Go savepoints faster but memory-intensive for large transactions.

---

## 17. Auto-vacuum / Incremental Vacuum

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

1. **pagerError eager cleanup** (`pager.go:pagerError`): SQLite's `pager_error()`
   only sets errCode and transitions to `PAGER_ERROR`, deferring cleanup to a
   subsequent `sqlite3PagerRollback()`. We perform eager cleanup (cache purge,
   WAL rollback, lock release, transition to pagerOpen) because there is no
   guaranteed subsequent rollback call — if the caller's goroutine panics or
   abandons the transaction, the WAL write lock would remain held, blocking the
   next `BeginWrite`.

2. **Page-1 explicit exclusion** (`pager.go:pagerStress`): SQLite does not check
   `pgno==1` in `pagerStress()`. Page 1 is structurally protected: it stays pinned
   (referenced) throughout the transaction, so pcache never selects it as a spill
   victim. We add an explicit guard because page 1 may become unpinned between
   b-tree operations.

3. **`pcache.create()` drops xStress error** (`pcache.go:create`): SQLite's
   `pcache1Fetch` propagates non-BUSY errors from xStress to the caller, allowing
   the pager to abort page acquisition. Our `create()` has no error return (it
   always returns a `*page`), so xStress errors are silently dropped. In practice,
   `pagerStress` calls `pagerError` on WAL write failure, which performs eager
   cleanup, so the dropped error is harmless.

4. **Batched wal-index update** (`wal.go:setBatch`): SQLite updates the wal-index
   inline in `walFrames()` — the write loop tags each page `PGHDR_WAL_APPEND`
   (set on append, cleared on in-place reuse), then a second loop replays the flag
   via `walIndexAppend()` (`wal.c:4154/4166/4228-4233`); rollback uses
   `walCleanupHash()`. We mirror this with one post-loop `setBatch` per
   `writeFrames` call (a single `wi.mu` acquisition for the in-process `pageMap`,
   then eager `shmHashWrite` — *not* deferred), plus a `walCleanupHash` analog on
   rollback. **Invariant: the appended set handed to `setBatch` MUST be recorded
   inline in the write loop (the `appended` slice ≡ `PGHDR_WAL_APPEND`), never
   re-derived.** Re-deriving the reuse predicate after the loop dropped the
   force-appended commit frame and silently corrupted recovery — see the
   Frame-Reuse note below. A `maxFrame < nFrame` guard in
   `writeFrames`/`writeFramesMem` now fails loudly if any appended frame is
   unregistered.

5. **dontWrite pages made clean without WAL write** (`pager.go:pagerStress`):
   SQLite's `pagerStress` in WAL mode writes `PGHDR_DONT_WRITE` pages to WAL
   anyway (the data is irrelevant but the frame is still written). We skip the WAL
   write and just mark them clean, avoiding unnecessary I/O. Safe because dontWrite
   page data is never read back.

6. **Shared pageMap causes transient cache misses** (`wal.go:getLatest`): We now use
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

**In-Memory WAL Mode Skips Checksums** -- Severity: Minor (accepted)

Intentional design choice for the `InProcess + NoSync` fast path. No disk
persistence means checksums add overhead without value.

**Auto-Checkpoint Uses PASSIVE Mode** -- Severity: Minor

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

**Missing Salt Cross-Check** -- Severity: Minor

No validation that the WAL file's salt matches the database header's salt.
Would detect stale or mismatched WAL files.

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
- Buffer reuse on eviction: matches SQLite step 4 (`pcache1.c:897-914`) — since
  commit `acf91a0`, `create()` keeps the evicted victim as `recycled` and reuses
  its buffer in-place (`resetPage` → `clear(p.data)`) for **both** writer and reader
  caches (gated on `pc.purgeable`, not `xStress`). Only *surplus* evicted buffers
  beyond the kept one go back to the slab in `clear()`/`discard()`/`truncate()`;
  `evictOne` does not free the kept victim's buffer.
- No `reuseUnlikely` on unpin: SQLite's `pcache1Unpin` accepts a
  `reuseUnlikely` flag (`pcache1.c:1079`); when true, pages are immediately
  freed. Our `release()` does not have this hint. Overfull eviction
  (`nPage > maxPages`) matches SQLite's `pGroup->nPurgeable > nMaxPage`
  check (`pcache1.c:1094`). `sqlite3PcacheDrop` maps to our `discard()` method.
- Merged Fetch+FetchStress: SQLite splits page acquisition into
  `sqlite3PcacheFetch` (soft create, may return NULL) and
  `sqlite3PcacheFetchStress` (spill + hard retry) as separate calls from the
  pager (`pcache.c:403-490`). Our `create()` merges both into a single
  function with inline stress handling.
- No `eCreate` state machine: SQLite's `PCache.eCreate` toggles between 1
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
- Non-purgeable caches skip LRU: SQLite's `pcacheUnpin` (`pcache.c:265-271`)
  is a no-op for non-purgeable caches. Our `release()` matches this by
  guarding LRU operations with `pc.purgeable`.
- `Initialized()` is lock-free: uses `atomic.Bool` for the `initialized` flag.
  `pageSize` is immutable after Init, read without mutex via acquire semantics
  from the atomic load. Matches SQLite's mutex-free reads of `pcache1.isInit`
  and `pcache1.szSlot` (`pcache1.c:220-222`).

**Future Improvements:**
- Shrink API (`sqlite3PcacheShrink` equivalent) for external memory pressure
- Slab telemetry: expose nTotal, nOverflow, underPressure via metrics

### B-tree Operations

**Full 3-Sibling Redistribution (`balance_nonroot` port)** -- Resolved (insert side commit 4834f89; delete side a57d3d7)

A faithful port of SQLite's `balance_nonroot()` lives in `balance.go` (`balanceNonroot`):
it gathers the over-/under-full child plus up to two adjacent siblings (NB=3), pools their
cells with the parent dividers, recomputes the minimum output-page count k ∈ {nOld-1, nOld,
nOld+1}, packs each page full then backs off the last for balance, and rewrites the parent
dividers — producing SQLite's high, even fill. It is now the live path for **both** sides:
inserts funnel an over-full leaf through it (`splitLeafAndInsertWithPath` → `balanceNonroot`,
btree.go:1872), and deletes funnel an under-full (>2/3 free) leaf through the same balancer
with `inject.active=false` (`deleteRebalanceLeaf` → `balanceNonroot`, btree.go:2546;
`completeMergeUpward` cascades parent under-fullness, gating single-child collapse on root per
btree.c:8960). The former 2-way `leafSplitPoint` split survives only as the root-leaf fallback
(`splitRootLeafAndInsert`); `balance_quick` is retained as the rightmost-append fast path
(below). See `balance.go`'s header for the enumerated index-btree deviations, and
`docs/btree/plans/2026-05-23-balance-nonroot-3sibling.md` /
`2026-05-23-delete-time-rebalancing.md`. Deferred (optional): first-key divider advance on
delete and retiring the now-dead `tryMergeLeaf`.

**Rightmost-Append Fast Path (balance_quick port)** -- Resolved 2026-04-23

SQLite's `balance_quick` (`btree.c:7992-8086`, dispatched at
`btree.c:9169-9192`) handles the "rightmost append into the rightmost
leaf of a non-root parent" case without redistributing cells: it
allocates a fresh right sibling, puts only the new cell there, leaves
the old page 100% full, and adds a divider to the parent.

Any-store's port lives in `splitLeafRightmostAppend` with dispatch at
the top of `splitLeafAndInsertWithPath`. Four of SQLite's five
preconditions (`btree.c:9170-9174`) map directly:

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

**No Full Freeblock Chain** -- Severity: Important (partially addressed)

SQLite maintains a sorted linked list of free blocks within each page for
fine-grained space reuse. We implemented in-place update (when new cell <=
old cell size), in-place delete (with fragmentation tracking), and
defragmentation-before-split. Our approach tracks fragmentation in `fragBytes`
and triggers a full rebuild when it exceeds 60 bytes.

**Path Tracking Stores Only Page Numbers** -- Resolved 2026-04-23

The cursor path used to store only page numbers. The descent path is now
`[]pathEntry{pgno, cellIdx, nCell}`, mirroring SQLite's `apPage[]`/`aiIdx[]`
cursor stack (`btreeInt.h:553-556`). `cellIdx` is populated from
`searchInterior`'s second return value (which was previously discarded).

Commit 2 (`insertSepIntoInterior`): takes `insertIdx` directly, mirroring
SQLite's `balance_nonroot(iIdx=...)` at `btree.c:8230, 9213`. The O(nCell)
linear parent re-scan before inserting a divider is gone.
`BenchmarkInsertSepIntoInterior_DeepTree` pins the win.

Commit 3 (`removeChildFromParent`): uses `path[len-1].cellIdx` directly to
locate the child slot, replacing the linear scans, with defensive bounds-checks
against path-builder drift. (`tryMergeLeaf` — the original non-faithful 2-page
merge — is now **dead code**: delete-time merging funnels through `balanceNonroot`
instead, see the 3-Sibling note above. `tryMergeLeaf` and its helper
`removeMergedRightSeparator` remain only as test-exercised code pending retirement
per the delete-rebalance plan.)

**Nearby Allocation Hint for Overflow Pages** -- Resolved 2026-04-22

`pager.allocatePageNear(nearby)` accepts an optional locality hint
(nearby == 0 keeps the legacy last-leaf pop). When nearby > 0 and a
freelist trunk is selected, the leaf with minimum absolute distance
to `nearby` is picked — matches SQLite `btree.c:6678-6699`
(BTALLOC_ANY + nearby path).

`writeOverflowChainMulti` threads the previous overflow page's pgno
as the hint on each subsequent allocation, matching SQLite's
`fillInCell` at `btree.c:7197` (`allocateBtreePage(pBt, &pOvfl,
&pgnoOvfl, pgnoOvfl, 0)`) and the first-page zero-hint init at
`btree.c:7131` (`pgnoOvfl = 0`). Result: overflow chains for a single
cell allocated from scattered freelist leaves live in clustered
runs on disk, improving sequential-read and OS-prefetch locality —
relevant for large-blob workloads (10 MB payload = ~2560 overflow
pages).

Measured delta on `BenchmarkOverflow10MB_FindIdCold`: +2.9% trend
(p=0.132, within noise). The bench uses a fresh DB where overflow
chains are allocated via monotonic `dbSize` growth and are already
contiguous — the hint only changes allocation order when the
freelist has scattered leaves. Real workloads with document churn
(update/delete/re-insert cycles) will see benefit; current bench
doesn't model that. See
`any-store-tests/results/session_perf/benchstat_nearby_overflow.txt`.

Covered by `TestAllocateFromFreelist_NearbyHint`,
`TestAllocateFromFreelist_NearbyZeroIsLegacyBehavior`, and
`TestWriteOverflowChain_ContiguousOnFreshFreelist` (in
`pager_test.go`).

**Out of scope:** `BTALLOC_EXACT` and `BTALLOC_LE` modes (used in
SQLite for auto-vacuum relocation; any-store has no auto-vacuum, see
`btree.c:6515`). btree split / root allocation callers don't use
the hint — a future optimization could pass the parent-page pgno as
nearby on split (matches `btree.c:8666` style) but would require
benchmark justification.

**Reader-Begin BUSY Conversion (SQLite walTryBeginRead)** -- Resolved 2026-04-23

SQLite's `walTryBeginRead` (`sqlitec/src/wal.c:3000-3252`) never surfaces
`SQLITE_BUSY` to its caller from the reader-slot claim path. Three
conversion sites turn BUSY into the internal sentinel `WAL_RETRY`
(`#define WAL_RETRY (-1)` at wal.c:2626):

  - slot-0 fast path BUSY (wal.c:3144-3146): falls through to slot-1..4
  - all slots 1..4 busy (wal.c:3186-3188): explicit `rc==SQLITE_BUSY ? WAL_RETRY : ...`
  - final shared-acquire BUSY (wal.c:3203): same conversion

The caller `walBeginReadTransaction` (wal.c:3391-3393) consumes WAL_RETRY
in an unbounded `do { ... } while(rc==WAL_RETRY)` with internal back-off
(wal.c:3022-3056): first 5 retries Gosched-equivalent, then 1 µs sleep,
then quadratic ramp `(cnt-9)² × 39 µs` capped at WAL_RETRY_PROTOCOL_LIMIT
= 100 (wal.c:2943) for ~10 s total budget before returning
`SQLITE_PROTOCOL`.

any-store previously surfaced `ErrBusy` from two of the three sites
(`wal.tryBeginReadMultiProcessHdr`'s slot-0 lock fail and no-slot-claim
fallback) and ran a flat 5000-iteration `runtime.Gosched()`-only retry
loop. Under concurrent readers + checkpoints,
`TestRapidCheckpointDuringOverflowWrites` produced ~130k transient
ErrBusy returns per run despite the database being fully consistent
(`QuickCheck` clean); an additional 2–16 residual `ErrProtocol` returns
came from retry-budget exhaustion because 5000 Gosch'd iterations
completed before peer readers could release their slot locks.

All three SQLite conversions are now ported plus the SQLite back-off
ramp. The same ErrBusy→errWALRetry conversion applies symmetrically to
the in-process path's slot-0 fallback (`tryBeginReadInProcessHdr`) for
consistency.

**Test evidence:** `TestRapidCheckpointDuringOverflowWrites` 10/10 PASS
after fix vs ~130k read errors before. Full any-store unit suite + the
storetest stress slice (`TestStressSavepoint`, `TestStressReaderWriter`,
`TestStressCheckpoint`) remain green.

**Covered by:** `TestTryBeginReadMultiProcessHdr_AllSlotsBusyReturnsRetry`
(internal/btree/wal_reader_retry_test.go).

### Freelist

**Freelist Formula Respects Reserved Space** -- Resolved (stale drift note)

`freelistMaxLeaves()` uses `(p.usableSize() - 8) / 4` where `usableSize`
is `pageSize - ReservedSpace`. Correct regardless of ReservedSpace
value. See `pager.go:1190-1192`. Note this equals `usableSize/4 - 2` — SQLite's
freelist *corruption ceiling*, not its conservative fill bound `usableSize/4 - 8`,
so any-store packs up to 6 more leaves per trunk than SQLite (see §10 drift).

**No BTALLOC_EXACT / BTALLOC_LE Modes** -- Severity: Minor

Only `BTALLOC_ANY` allocation mode. `BTALLOC_EXACT` and `BTALLOC_LE` are only
needed for auto-vacuum and locality hints.

**Reserved Space Used in Overflow Computations** -- Resolved (stale drift note)

`overflowPageUsable(usableSize int)` takes `usableSize` as its parameter
and all 7 callers pass `p.usableSize()` (which is
`pageSize - ReservedSpace`). See `page.go:116-118`.

### Multi-Process WAL Write Safety -- FIXED

When `InProcess` is false (default on linux/darwin amd64/arm64), two separate OS
processes can open the same database file. Multi-process writes are now safe
thanks to three mechanisms:

1. **BUSY_SNAPSHOT check** (`wal.beginWrite`): After acquiring the SHM write lock,
   compares the reader's snapshot (`tx.walHdr`, passed as `readSnap` to
   `beginWriteWithSnapshot`) against the current SHM header. If they differ, another
   process committed between our `beginRead` and `beginWrite`, so we return
   `ErrBusySnapshot`. Matches `sqlite3WalBeginWriteTransaction` (wal.c:3712).

2. **WAL state re-sync** (`wal.beginWrite`): After the BUSY_SNAPSHOT check passes,
   re-syncs `nFrame`, `cksum1/2`, and salts from the SHM header so `writeFrames`
   uses correct offsets and checksum chains. Also clears `writerCache` via
   `stateChanged` if another process committed since our last write.

3. **SHM header sync in `tryBeginRead`**: For multi-process mode, reads `mxFrame`
   and `nBackfill` from SHM instead of stale process-local atomics, so readers
   see the latest committed state from other processes.

**Internal retry in `DB.BeginWrite`** (resolved 2026-04-22): When `ErrBusySnapshot`
is returned, the retry loop ends the stale read, clears `writerCache`, and
re-calls `pager.beginRead` to get a fresh snapshot. Previously the loop had
a hidden cap of 1000 attempts with no backoff. Now the loop tight-retries at
most `busySnapshotInnerRetries` (= 3) times, then delegates to the configured
`BusyHandler` for delays[]-table backoff (matching `sqliteDefaultBusyCallback`
in `sqlitec/src/main.c:1717`), and finally surfaces `ErrBusySnapshot` to the
caller — matching SQLite's `SQLITE_BUSY_SNAPSHOT` caller contract
(`sqlitec/src/wal.c:3714`).

**`writeHeader` parameterization**: `walIndex.writeHeader` now accepts explicit
`frameCksum` and `salt` parameters so the SHM header always contains the correct
running checksums and salts for frame chaining.

Verified by `TestMultiProcessWALCorruption` which spawns a child OS process that
writes to the same database file concurrently with the parent.

### Multi-Process WAL Fix — Drift from SQLite

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

3. **`headerOnDisk` shortcut in `syncFromSHMLocked`**: we infer "on-disk WAL
   header exists" from `hdr.mxFrame > 0` rather than `fstat`-ing, relying on
   the invariant that `flushHeader` precedes every frame write.

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

### Checkpoint latest-frame-per-page (resolved 2026-04-22)

**Previously:** the backfill loop in `checkpointWithMode` wrote every
frame's page to the DB file — a page rewritten N times in the WAL got
written N times, most of them overwritten by later frames.

**Now:** two-phase backfill — (1) `buildBackfillMap(lo, hi)` scans
frame headers `[lo, hi)` and returns `map[pgno]frameIdx` keeping the
latest frame per pgno; (2) iterate pgnos ascending for sequential
DB-file write locality, writing each page exactly once. Matches
SQLite's `walIteratorNext` (`sqlitec/src/wal.c:1758-1786`).

**Invariants preserved:** `nBackfill` is still a frame-index cursor
(crash recovery works unchanged); `nBackfillAttempted` is set before
the write phase (partial-write detection still correct); in-memory
mode uses the same dedup logic on `w.memFrames`.

**Measured delta** (see
`any-store-tests/results/session_perf/benchstat_ckpt_dedup_scratch.txt`):
`Crud/BatchUpdate` sec/op **-7.97%** (p=0.022), alloc-neutral. Geomean
flat (most benches don't stress checkpoint). The original dedup commit
introduced a per-checkpoint map/slice allocation (naively matching
SQLite's `walIteratorInit` + `walIteratorFree` at
`sqlitec/src/wal.c:1929-1933` / `:1948` which also `malloc`s/`free`s
per checkpoint). We can do better than SQLite here: any-store's
single-writer invariant (`lockCheckpoint` held exclusive) lets us
reuse `w.ckptLatest` + `w.ckptPgnos` scratch across checkpoints,
erasing the allocation cost entirely.

### Checkpoint mxFrame source fix (commit `9023f5b`)

A ~4-5% residual failure persisted through all per-tx walHdr work
because none of those steps touched `checkpointWithMode`'s mxFrame
source. Root cause: `wal.go:checkpointWithMode` used
`nf := w.index.mxCommitFrame.Load()` which is process-local. In
multi-process mode, a sibling's committed frames appear in the SHM
header but NOT in this process's local `mxCommitFrame`. Close-time
`checkpointPassive` only backfilled THIS process's own frames, then
`truncateFile` wiped the WAL — destroying sibling's uncopied frames.

Fix: in multi-process mode, read `nf` from the live SHM hdr at
checkpoint start. Matches SQLite's `walCheckpoint` which reads
`pWal->hdr.mxFrame` populated by `walIndexTryHdr`.

Reliability: 100/100 on the 100-run multi-process index harness.

**Follow-up: same bug class in sibling sites.** A drift review flagged
two more call sites with identical process-local reads:
`checkpointPassive`'s completeness probe
(`nBackfill >= mxCommitFrame.Load()` — controls whether `pager.close`
truncates) and `checkpointPost`'s reset gate
(`backfill < mxCommitFrame.Load()` — controls `tryResetWAL`). Stale
local `mxCommitFrame` would falsely report "complete" / "ready to
reset" after only backfilling our own frames.

Fix: extracted helper `(w *wal) authoritativeMxFrame()` that reads SHM
hdr in multi-process mode, falls back to `mxCommitFrame.LoadLocal()`
otherwise. `checkpointWithMode`, `checkpointPassive`, and
`checkpointPost` all use the helper. Mirrors SQLite's
`pWal->hdr.mxFrame` usage throughout `walCheckpoint` (wal.c:2216,
2227, 2309, 2341). Reader-path call sites (`shmHashGet`,
`tryBeginReadInProcess`) intentionally retain the local read — those
are per-tx snapshots that must not observe frames we haven't sync'd.

**Compile-time guard added 2026-04-22.** `walIndex.mxCommitFrame` is
now a `commitFrameCounter` (see `mxframe.go`) whose only reader is
`LoadLocal()`; there is no plain `Load()`. Callers that want the
cross-process authoritative value must go through
`authoritativeMxFrame()`. A regressing commit that reads
`mxCommitFrame.Load()` directly no longer compiles. The one production
site still reading the local cursor in multi-process mode
(`tryCheckpoint` in `pager.go`) now uses the authoritative accessor.

### pager.close lockWrite gate

Close-time checkpoint+truncate must hold `lockWrite` exclusive across
both operations so a peer's in-flight `writeFrames` cannot race the
truncate. Earlier code called `walBusyLock(lockWrite)` but discarded
the error: on 5s timeout we proceeded without the lock, racing the
peer and corrupting the WAL. The unconditional `unlock` at the end
also released a slot we never acquired.

Fix: track acquisition with a `lockedWrite bool`; truncate only when
`lockedWrite || inProcess`; unlock only when we hold it. If lock
acquisition fails, `checkpointPassive` still runs (it doesn't need
the write lock) but truncate is skipped — WAL stays intact for the
next opener. Mirrors SQLite's `sqlite3WalClose` which only calls
`walLimitSize` inside the `SQLITE_OK==sqlite3OsLock(EXCLUSIVE)` arm
(wal.c:2508-2536).

**Structural guard added 2026-04-22.** The manual `lockedWrite bool` +
trailing manual unlock has been replaced by
`(*pager).withWriteLock(fn func(locked bool) error) error`, which
acquires (or skips) the lock in one place and releases via `defer`. A
future early-return or panic inside the closure cannot leak the lock.
The close body is now the closure; correctness of the truncate gate
no longer depends on the code path walking past the unlock line.
Regression test: `TestWithWriteLock_AlwaysReleases` covers clean /
error / panic return paths.

### Race fixes evaluated and not ported

#### SQLite `fe57e14b49` — checkpointer vs. writer WAL wrap (evaluated 2026-04-22, not applicable)

Upstream SQLite commit `fe57e14b49` (2026-03-03, "Avoid an obscure race
condition between a checkpointer and a writer wrapping around to the
start of the wal file") adds a salt-revalidation check in
`walCheckpoint()` right after acquiring `WAL_READ_LOCK(0)` exclusive:

```c
WalIndexHdr *pLive = (WalIndexHdr*)walIndexHdr(pWal);
if( 0==memcmp(pLive->aSalt, pWal->hdr.aSalt, sizeof(pWal->hdr.aSalt)) ){
    /* ... proceed with backfill ... */
}
```

**The race in SQLite:**
1. Checkpointer C snapshots `pWal->hdr.mxFrame=N, aSalt=S` via
   `walIndexReadHdr`.
2. C reads `pInfo->nBackfill` live (not snapshotted).
3. Writer W runs `sqlite3WalBeginWriteTransaction → walRestartLog`,
   briefly grabs `WAL_READ_LOCK(0)` exclusive, writes new salts `S'`,
   sets `pInfo->nBackfill=0`, releases.
4. C's `walBusyLock(WAL_READ_LOCK(0))` now succeeds (W released).
5. Without the fix, C sees `nBackfill(0) < hdr.mxFrame(N)`, proceeds
   to iterate frames 1..N using its stale snapshot, reads whatever W
   has begun writing at those offsets, copies to wrong DB pages.

**Key enabler in SQLite:** writers wrap the WAL without holding
`CHECKPOINT_LOCK`.

**Why the race cannot occur in any-store:**

Salt regeneration (`rand.Uint32()`) happens at exactly two call sites:
- `initHeaderStateLocked` (wal.go:1608-1609)
- `writeHeader` (wal.go:1653-1654)

All callers are either first-time open/recovery paths (no concurrent
checkpointer yet exists) or `doResetWAL`. `doResetWAL` is reachable
only via `tryResetWALWithBusy → checkpointPost → checkpointWithMode`,
and `checkpointWithMode` holds `lockCheckpoint` exclusive throughout
(wal.go:2780 + deferred unlock covering the entire body including
`checkpointPost`). `lockCheckpoint` is fcntl-backed (shm_mmap.go:147),
so it's genuinely cross-process exclusive.

Writers never regenerate salts: `beginWriteWithSnapshot` copies
existing salts from SHM (wal.go:2518-2519), `writeFrames` rewrites
the SHM header preserving the existing `w.header.salt1/salt2`
(wal.go:2054).

Therefore: while any checkpointer is inside `checkpointWithMode`, no
other actor — writer or checkpointer — can mutate salts. The
snapshot-vs-backfill window SQLite's fix protects cannot observe a
salt change in any-store.

**Architectural difference:** SQLite permits writer-initiated wrap
via `walRestartLog` inside `beginWriteTransaction` when
`nBackfill == mxFrame`; any-store only wraps via
`CheckpointRestart/Truncate` modes, which require `lockCheckpoint`.

**Consequence:** fe57e14b49 is a no-op in any-store. If a future
change introduces a writer-wrap path (e.g. auto-wrap in
`beginWriteWithSnapshot` when `nBackfill == mxFrame`), that change
must re-evaluate this invariant and include the salt-revalidation.

### Not Implemented (by design)

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
`docs/plans/2026-04-22-sqlite-backup-port.md` for the full drift
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

**Key intentional simplifications** (full list in the plan document):
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

**Classification: Aligned** -- the matching predicate (equality-prefix skip +
single consistent composite direction) is a faithful port of SQLite's rule.
any-store does not implement SQLite's `isOrderDistinct` / UNIQUE-NOT-NULL
refinements (`where.c:5300,5363-5377`, tag-20210426-1) or the
`WHERE_BIGNULL_SORT` NULLS-ordering handling -- they only affect whether
*trailing* unconstrained columns can be elided or how NULLs sort, neither of
which any-store's index/sort model exposes.

## Audit-Discovered Drifts (2026-05-29)

The following drifts were found by an automated per-function C-vs-Go audit of the
b-tree port against sqlitec and deduplicated by root cause (the encryption/sqlcipher
codec is excluded here and tracked separately).

<a id="drift-4-beyond-file-pages-silently-zero-filled-skipping-header-valid"></a>
### Drift: Beyond File Pages Silently Zero Filled Skipping Header Validation
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*ReadTx.txGetPage` (`internal/btree/pager.go:962`).

When a requested pgno is beyond the physical file AND beyond the `dbSize` bound, the
Go readers (`getPageReader` `pager.go:970`, `getPageWriter` `pager.go:778-779`,
`readTempPage` `pager.go:855`) `clear(pg.data)` and return a zero page with no error;
the subsequent header parse is gated on `pg.data[off] != 0` (`pager.go:806/896/999`),
so the page is returned with an empty `pageHeader{}` (`pageType=0`) and no
validation. C's `getAndInitPage` instead returns `SQLITE_CORRUPT_BKPT` for any
`pgno > btreePagecount(pBt)` before fetching (`btree.c:2396-2399`). The consequence is
an un-validated zero page entering the descent/read path in place of a corruption
error; severity is low because it is the same root failure surfaced by drift-3 and is
bounded by the per-snapshot `dbSize` check.

<a id="drift-6-wal-frame-read-failure-falls-through-to-disk-read"></a>
### Drift: WAL Frame Read Failure Falls Through To Disk Read
- **Category:** changed-logic  -  **Severity:** high
- **Affected functions:** `pager.go:*pager.getPageWriter` (`internal/btree/pager.go:751-768`), `pager.go:*pager.readTempPage` (`internal/btree/pager.go:831-846`).

In C `readDbPage`, once `sqlite3WalFindFrame` resolves a WAL frame (`iFrame != 0`) the
page's current version lives only in the WAL: it reads that frame via
`sqlite3WalReadFrame` and returns the result directly, with the DB-file read placed in
the `else` branch and therefore unreachable, so a WAL read failure propagates as the
page-get error (`pager.c:3035-3045`). The Go getters (`getPageWriter`,
`readTempPage`), after the WAL index reports a frame > 0, attempt `wal.readFrame` and
return on success but on failure deliberately ignore the error and fall through to a
stale DB-file read. The consequence is a correctness hazard: when the authoritative
WAL copy of a page cannot be read, Go silently substitutes the older committed-DB-file
version instead of surfacing the WAL error, which can return outdated page content as
if it were current.

<a id="drift-7-short-db-file-read-treated-as-hard-error"></a>
### Drift: Short DB File Read Treated As Hard Error
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.readTempPage` (`internal/btree/pager.go:847-855`).

C `readDbPage` explicitly maps a short read of an in-bounds DB page to success: after
`sqlite3OsRead`, `if( rc==SQLITE_IOERR_SHORT_READ ){ rc = SQLITE_OK; }`
(`pager.c:3042-3044`), and because the pcache buffer is pre-zeroed the page is returned
zero-padded rather than as an error (`os_unix.c:3575-3577` zero-fills the unread tail).
Go's `readTempPage` instead treats a short read of an in-bounds page (`pgno <= dbSize`)
in a physically-short DB file as a hard error. The consequence is divergent error
behavior for a physically-truncated-but-logically-valid file: Go fails where SQLite
returns a partially-read, zero-padded page as success.

<a id="drift-8-max-page-count-sqlite-full-enforcement-absent"></a>
### Drift: Max Page Count SQLITE_FULL Enforcement Absent
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.getPage` (`pager.go:734-811 (getPageWriter, no mxPgno check); errors.go:37 (ErrFull defined but unused)`), `pager.go:*pager.getPageNoContent` (`internal/btree/pager.go:1012`), `pager.go:*pager.getPageReader` (`internal/btree/pager.go:962-993`), `pager.go:newPager` (`pager.go:266 (newPager, missing mxPgno init); enforcement gap at pager.go:1173 (allocatePageNear: p.dbSize.Add(1) with no bound check)`).

SQLite initializes `pPager->mxPgno = SQLITE_MAX_PAGE_COUNT` (0xfffffffe) in
`sqlite3PagerOpen` (`pager.c:5049`) and enforces it at page-acquire/grow time: a
not-yet-cached page with `pgno > pPager->mxPgno` returns `SQLITE_FULL`, releasing the
page if `pgno <= dbSize` (`pager.c:5591-5598`). This both caps database growth (PRAGMA
`max_page_count`) and prevents the page number from overflowing the 32-bit pgno space.
The Go pager has no `mxPgno`/`maxPageCount` concept at all: `newPager`
(`pager.go:266-275`) initializes no such field, the getters (`getPageWriter`,
`getPageReader`, `getPageNoContent`) perform no ceiling check, `allocatePageNear` grows
via `p.dbSize.Add(1)` (`pager.go:1173`) with no bound, and the defined `ErrFull`
(`errors.go:37`) is unused. The consequence is that database growth is never capped and
the 32-bit pgno guard SQLite relies on is absent.

<a id="drift-10-missing-refcount-greater-than-one-in-use-page-corruption-det"></a>
### Drift: Missing Refcount Greater Than One In Use Page Corruption Detection
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.getPageNoContent` (`internal/btree/pager.go:1017`).

SQLite fetches freelist/grow pages through `btreeGetUnusedPage` (`btree.c:2459`), which
after `getPageNormal` rejects any page whose pager refcount > 1 --
`if( sqlite3PagerPageRefcount((*ppPage)->pDbPage)>1 ){ releasePage; return
SQLITE_CORRUPT_BKPT; }` (`btree.c:2464-2469`) -- because a page pulled off the freelist
or appended past EOF that already has another outstanding reference means the same page
is simultaneously in use, i.e. corruption. The Go grow/freelist fetch path
(`getPageNoContent`, `pager.go:1017`) has no equivalent in-use / refcount detection.
The consequence is that a freelist or grow page that is corruptly aliased to an
already-in-use page is accepted silently instead of being rejected as `ErrCorrupt`.

<a id="drift-11-movetochild-child-page-ncell-greater-than-equal-one-descent-"></a>
### Drift: moveToChild Child Page nCell Greater Than Equal One Descent Guard Missing
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `btree.go:*Cursor.First` (`internal/btree/btree.go:3196-3200 (and getPageReader pager.go:908-959 lacking nCell validation)`), `btree.go:*Cursor.Last` (`btree.go:3223-3237 (cf. First btree.go:3178-3200)`), `btree.go:*Cursor.Seek` (`btree.go:3260-3277 (descent loop; child load at 3273; n==0 interior handling at 939-948)`), `btree.go:*btree.searchInterior` (`internal/btree/btree.go:910-948`), `db.go:*ReadTx.leftmostKeyAfter` (`db.go:1527-1542`), `db.go:*ReadTx.leftmostKeyAfter` (`db.go:1505-1508`).

C validates every child page entered during descent: `moveToChild` (and its inlined
copies inside `sqlite3BtreeIndexMoveto` / the seek paths) returns `SQLITE_CORRUPT_PGNO`
when a freshly loaded child has `pPage->nCell < 1` (or its `intKey` disagrees with the
cursor) -- `btree.c:5477-5482`, inlined at `btree.c:6253-6258` -- so an empty or
structurally garbage child page can never be descended into. The Go descent paths
(`Cursor.First` `btree.go:3196`, `Cursor.Last` `btree.go:3223-3237`, `Cursor.Seek`
`btree.go:3273`, `searchInterior` `btree.go:910-948`, `leftmostKeyAfter`
`db.go:1505-1508,1527-1542`) load each child via `getPage`/`getPageReader` -- which
deserialize only the header and never check `cellCount >= 1` -- and loop straight back
without a per-child structural guard. The consequence is that a corrupt interior page
with zero cells (or a wrong-type page) is silently accepted during descent rather than
rejected; the Go `leftmostKeyAfter` even maps an empty reached leaf to `ErrKeyNotFound`
(`db.go:1505-1508`) where SQLite would never have reached it.

<a id="drift-12-b-tree-kind-consistency-check-omitted-on-descent"></a>
### Drift: B Tree Kind Consistency Check Omitted On Descent
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*Cursor.Seek` (`btree.go:3260 (for pg.header.isInterior()); page.go:296-298 (isInterior accepts both interior types)`).

SQLite validates the b-tree KIND (intKey / page-type) of every page touched during a
seek: `moveToRoot` rejects a root whose `intKey` flag disagrees with the cursor's
expectation (`(pCur->pKeyInfo==0)!=pRoot->intKey` -> `SQLITE_CORRUPT_PAGE`,
`btree.c:5615-5617`), and `moveToChild` rejects any child where `pPage->intKey !=
pCur->curIntKey` (`btree.c:5478`, inlined at `btree.c:6254`). The Go `Cursor.Seek` path
branches purely on `pg.header.isInterior()` (`btree.go:3260`), and `isInterior`
(`page.go:296-298`) returns true for both `pageTypeIntIdx`(2) and `pageTypeIntTbl`(5)
without ever checking that the page's kind matches the b-tree being searched. The
consequence is that a page of the wrong b-tree kind (intKey vs index) encountered
during a seek is accepted and traversed instead of being rejected as corruption.

<a id="drift-13-empty-interior-root-treated-as-empty-btree-not-corruption"></a>
### Drift: Empty Interior Root Treated As Empty Btree Not Corruption
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `btree.go:*Cursor.First` (`internal/btree/btree.go:3178-3182`), `btree.go:*Cursor.Last` (`btree.go:3223-3237`).

SQLite's `moveToRoot` treats a 0-cell interior root as a benign "virtual root" only
when `pRoot->pgno==1`, and otherwise returns `SQLITE_CORRUPT_BKPT`
(`btree.c:5624-5635`): a non-page-1 interior root with zero cells is corruption. Go's
`Cursor.First` descent loop, when it reaches a 0-cell interior page (root or deeper),
does `releasePage(pg); return nil` and leaves the cursor invalid (`btree.go:3178-3182`),
i.e. it silently reports an empty b-tree; `Cursor.Last` (`btree.go:3223-3237`) likewise
has no `rootPage==1` guard. The consequence is that a corrupt zero-cell interior root is
accepted as a benign empty cursor instead of being flagged `ErrCorrupt`, and First/Last
are asymmetric on this case.

<a id="drift-14-b-plus-tree-traversal-drops-interior-cell-keys-versus-sqlite"></a>
### Drift: B Plus Tree Traversal Drops Interior Cell Keys Versus SQLite B Tree
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*Cursor.Next` (`btree.go:3697-3728`), `btree.go:*btree.countPage` (`btree.go:3083-3087`), `db.go:*ReadTx.Count` (`btree.go:3083-3087 (leaf-only count) vs 3089-3122 (interior: recurse, no cellCount add)`).

any-store is a B+tree: index keys live exclusively on leaves, so an interior cell is
only a separator/router, never a stored entry. SQLite's index B-tree instead treats
interior cells as first-class keys -- `btreeNext` returns `SQLITE_OK` positioned on the
interior separator after walking up via `moveToParent` (`btree.c:6361-6385`), and
`sqlite3BtreeCount` adds `pPage->nCell` on interior pages too because
`pPage->leaf || !pPage->intKey` (`btree.c:10529-10531`). Go's `Cursor.Next`
(`btree.go:3697-3728`) never pauses on interior positions and `countPage` adds
`cellCount` only on leaf pages (`btree.go:3083-3087`), recursing through interior pages
without counting them. The consequence is a structural design divergence that surfaces
in traversal (no interior-key stops) and in counting (interior cells excluded); for the
B+tree shape both results are correct, but they differ from SQLite's B-tree semantics.

<a id="drift-16-count-traversal-missing-interrupt-cancellation-check"></a>
### Drift: Count Traversal Missing Interrupt Cancellation Check
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.Count` (`btree.go:3077-3123`), `btree.go:*btree.countPage` (`btree.go:3077`), `db.go:*ReadTx.Count` (`btree.go:3077-3123`).

SQLite's `sqlite3BtreeCount` gates its page-walk loop on
`!AtomicLoad(&db->u1.isInterrupted)` (`btree.c:10520`), so a long count over a huge tree
can be cancelled via `sqlite3_interrupt` mid-walk and returns the in-progress rc
(allowing `SQLITE_INTERRUPT`). Go's `countPage` (`btree.go:3077-3123`) has no
interrupt/context/cancellation hook and always runs to completion or to an error. The
consequence is that a `Count()` over a very large tree cannot be aborted; this is a
missing SQLite runtime feature with low correctness impact.

<a id="drift-17-count-return-type-truncates-i64-entry-total-to-go-int"></a>
### Drift: Count Return Type Truncates i64 Entry Total To Go int
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.countPage` (`btree.go:3073`).

SQLite returns the entry count as `i64` via `*pnEntry` (`btree.c:10508,10547`),
guaranteeing a 64-bit total regardless of platform. Go's `Count`/`countPage` declare and
accumulate the total as a Go `int` (`btree.go:3073`, with `total += c`), whose width is
platform-dependent. The consequence is that on a 64-bit target the behavior is
practically equivalent, but on a 32-bit target the total is 32-bit and could overflow for
a very large tree, whereas SQLite is always `i64`.

<a id="drift-26-leaf-cell-size-missing-four-byte-minimum-clamp"></a>
### Drift: Leaf Cell Size Missing Four Byte Minimum Clamp
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:leafCellSize` (`internal/btree/btree.go:230`), `btree.go:leafCellSizeFromLengths` (`btree.go:354`), `btree.go:leafCellSizeWithOverflow` (`btree.go:341`), `btree.go:parseLeafCellWithSize` (`internal/btree/btree.go:162-175`).

SQLite's `cellSizePtrIdxLeaf` and the cell parsers (`btreeParseCellPtr` /
`btreeParseCellPtrIndex`) clamp the computed non-overflow cell size up to a 4-byte minimum
-- `if( nSize<4 ) nSize = 4;` (`btree.c:1486-1488`, `1350-1351`, `1389-1390`). This floor
is a hard on-disk-format invariant: when such a cell is later freed it is converted into an
intra-page freeblock, whose header needs at least 4 bytes (2-byte next-pointer + 2-byte
size). Go's size and parse routines (`leafCellSize` `btree.go:230-232`,
`leafCellSizeFromLengths`, `leafCellSizeWithOverflow`, and `parseLeafCellWithSize`) all
omit this minimum-cell-size clamp, returning the raw `hdr + payload` with no lower bound.
The consequence is that a degenerate tiny cell (sub-4-byte) would be sized below the
freeblock minimum, so freeing it could not store a valid freeblock header -- a latent
deviation from SQLite's free-space format guarantee, mitigated in practice only because
real key/value cells comfortably exceed 4 bytes.

<a id="drift-28-searchleafpage-missing-overflow-cell-compare-guard"></a>
### Drift: searchLeafPage Missing Overflow Cell Compare Guard
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:searchLeafPage` (`btree.go:457-507`).

SQLite's `indexCellCompare` classifies an index cell by inspecting its payload-size varint
into three cases (`btree.c:5980-5998`): a single-byte payload or a 2-byte varint whose
`nCell <= maxLocal` may be compared against the cell's local bytes, but otherwise the record
overflows the page and it returns `c=99` to skip the local-bytes fast path and force the
full-key comparison. Go's `searchLeafPage` (`btree.go:457-507`) has no equivalent on-page
vs. overflow guard in its fast path. The consequence is that for an index cell whose payload
spills into an overflow chain, Go can compare against only the locally stored prefix as if it
were the whole key, yielding a silent wrong-key comparison and a potentially incorrect search
result.

<a id="drift-29-root-interior-overflow-uses-2-way-split-not-balance-deeper-p"></a>
### Drift: Root Interior Overflow Uses 2 Way Split Not balance_deeper plus balance_nonroot
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.insertIntoParent` (`internal/btree/btree.go:2113-2166`), `btree.go:*btree.insertIntoParent` (`internal/btree/btree.go:2268-2272`), `btree.go:*btree.insertSepIntoAncestor` (`btree.go:2113`), `btree.go:*btree.insertSepIntoAncestor` (`balance.go:874`).

When a separator must be inserted into a full interior page that is the btree ROOT (no
grandparent to gather siblings from), Go does not run SQLite's `balance_deeper` +
`balance_nonroot` even-fill packing. Instead `insertSepIntoInterior` falls to a classic
2-way median split (`btree.go:2113-2166`): collect the root's interior cells, splice in the
new divider, pick a split via `interiorSplitPoint` (a ~2/3 left-fill target,
`btree.go:295-329`), rebuild into two interior pages, and grow a new root through
`splitRoot`. The same fill-factor deviation recurs in `rewriteParentAfterBalance`'s
over-full fallback (`balance.go:874-877`) and in the legacy non-path `insertIntoParent`,
which on a failed re-descent unconditionally calls `splitRoot` as a "safety net"
(`btree.go:2268-2272`) rather than reporting corruption. The consequence is that root-level
interior overflow produces a different (and looser) page-fill distribution than SQLite's
balanced redistribution.

<a id="drift-31-rebuildinteriorpage-accepts-zero-cell-pages"></a>
### Drift: rebuildInteriorPage Accepts Zero Cell Pages
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.rebuildInteriorPage` (`btree.go:1730-1762`).

C's `rebuildPage` hard-asserts `nCell>0` (`btree.c:7666-7667`) and its only callers
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
- **Affected functions:** `btree.go:*btree.splitLeafAndInsertWithPath` (`btree.go:1839-1882`).

On the non-root general-balance path, SQLite's `balance()` driver rejects a corrupt tree
before redistributing cells: if the over-full non-root page being balanced has pager
refcount > 1 it returns `SQLITE_CORRUPT_PAGE`, because the only way a non-root page can hold
more than one reference at that point is if it is one of its own ancestor pages -- a cyclic
tree (`btree.c:9173-9177`). Go's dispatcher `splitLeafAndInsertWithPath`
(`btree.go:1839-1882`) has no refcount>1 / self-ancestor corruption guard. The consequence
is that a cyclic (self-referential) tree that SQLite would detect and reject as corruption
is instead followed by Go, which can loop or corrupt state while balancing.

<a id="drift-34-splitroot-missing-anothervalidcursor-corruption-guard"></a>
### Drift: splitRoot Missing anotherValidCursor Corruption Guard
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.splitRoot` (`btree.go:2279`).

SQLite gates the `balance_deeper` (root-overflow) path on a corruption precondition: the
root-overflow branch only proceeds when `anotherValidCursor(pCur)==SQLITE_OK`
(`btree.c:9153`), where `anotherValidCursor` (`btree.c:9110-9121`) walks all other cursors on
the same `BtShared` and returns `SQLITE_CORRUPT_PAGE` if any other cursor is `CURSOR_VALID`
and positioned on the same page about to be deepened. Go's `splitRoot` (`btree.go:2279`)
deepens the root with no equivalent check. The consequence is that a state SQLite treats as
corruption -- another live cursor pinned to the page being restructured -- is allowed by Go,
risking that the second cursor is left referencing a now-stale/repurposed root page.

<a id="drift-35-legacy-superseded-insert-path-functions-undocumented"></a>
### Drift: Legacy Superseded Insert Path Functions Undocumented
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `btree.go:*btree.splitLeafAndInsert` (`btree.go:2319-2342`).

The live insert path is `Insert` -> `getWritablePage(leaf)` -> `insertIntoLeafWithPath`
(`btree.go:1155`) -> `splitLeafAndInsertWithPath` -> `balanceNonroot`. A second, closed
cluster -- `splitLeafAndInsert` (`btree.go:2170`), `insertIntoPage` (`btree.go:1162`),
`insertIntoLeaf`, `insertIntoInterior` (`btree.go:2319`), and the non-path
`insertIntoParent` (which falls back to `splitRoot`) -- forms a self-referential set with no
production (non-test, non-`WithPath`) entry point. It is dead/legacy code superseded by the
`WithPath` family. The consequence is purely a documentation/maintenance drift: unlike
`tryMergeLeaf`, which is explicitly marked superseded, this legacy cluster is not annotated
as dead code, so a reader may mistake it for a live, divergent insert path.

<a id="drift-37-delete-rebalance-underfull-trigger-counts-fragbytes-as-used"></a>
### Drift: Delete Rebalance Underfull Trigger Counts fragBytes As Used
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*WriteTx.Delete` (`btree.go:2486-2488`).

SQLite gates post-delete rebalancing on the page's *true* free space: after `dropCell`,
`balance()` is skipped iff `pCur->pPage->nFree*3 <= usableSize*2` (`btree.c:10005`), where
`nFree` (maintained by `freeSpace`, `btree.c:2022`) counts *all* bytes reclaimed from the
dropped cell as free, including non-coalescible fragmentation. Go's `WriteTx.Delete`
computes its trigger as `nFree := usable - bt.leafUsedSpace(wpg)` then
`underfull := nFree*3 > usable*2` (`btree.go:2486-2488`), but `leafUsedSpace`
(`btree.go:2653-2654`) returns `cellPtrEnd + (usable - contentOff)`, i.e. it treats every
byte in the unallocated/fragmented region as *used*. Because Go's `nFree` excludes the
fragmentation bytes that SQLite's `nFree` includes, Go's underfull test fires *less* eagerly
than SQLite's. The consequence is that some pages SQLite would rebalance after a delete are
left under-occupied by the Go port, a benign space-utilization divergence rather than a
correctness defect.

<a id="drift-41-backup-empty-source-finalization-path-missing"></a>
### Drift: Backup Empty Source Finalization Path Missing
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `backup.go:*Backup.Step` (`internal/btree/backup.go:264`),
  `backup.go:*Backup.finalize` (`internal/btree/backup.go:264-272`, `backup.go:325`,
  `pager.go:2645-2647`).

On the final (DONE) iteration SQLite special-cases a zero-page source: inside the
`rc==SQLITE_DONE` block it runs `if(nSrcPage==0){ rc=sqlite3BtreeNewDb(pDest); nSrcPage=1; }`
(`backup.c:424-427`) -- rebuilding a fresh 1-page destination -- *before* bumping the schema
cookie and truncating. Go's done-path (`b.iNext > nSrcPage`) calls `b.finalize(nSrcPage)`
directly (`backup.go:264-272`) with no equivalent empty-source handling, so for
`nSrcPage==0` it invokes `finalize(0)` whose `truncateTo(0)` returns
`btree: cannot truncate to zero pages` (`pager.go:2645-2647`). The consequence is that
backing up an empty source database -- a no-op success in SQLite -- errors out in the Go
port instead of producing a valid 1-page destination.

<a id="drift-42-backup-finalize-omits-setversion-for-wal-destination"></a>
### Drift: Backup Finalize Omits SetVersion For WAL Destination
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `backup.go:*Backup.finalize` (`internal/btree/backup.go:306-326`).

When the destination is in WAL mode, SQLite's `finalize` calls
`sqlite3BtreeSetVersion(pDest,2)` to force page-1 file-format read/write version bytes 18/19
to 2 (`backup.c:435-437`; `btree.c:11527-11528` writes `aData[18]=aData[19]=2`). Go's
`finalize` (`backup.go:306-326`) performs only the schema-cookie bump and `DatabaseSize`
re-application and omits this `SetVersion` step. For any-store this is benign by invariant:
the engine is always WAL, so the source bytes copied into the destination are already 2 and
the destination's in-memory header carries the WAL version regardless. The consequence is a
latent divergence that would only matter if a non-WAL source were ever backed up to a WAL
destination, which any-store's always-WAL design precludes.

<a id="drift-43-backup-commit-point-moved-from-step-to-finish"></a>
### Drift: Backup Commit Point Moved From Step To Finish
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `backup.go:*Backup.Finish` (`internal/btree/backup.go:264-272`,
  `backup.go:384-391`).

In SQLite the destination write transaction is committed *inside* `sqlite3_backup_step` on
the final DONE iteration via `sqlite3BtreeCommitPhaseTwo(pDest,0)` (`backup.c:542`), after
which `p->rc` becomes `SQLITE_DONE`; by the time `sqlite3_backup_finish` runs the destination
is already committed and durable, and finish's only transaction action is a no-op rollback
(`backup.c:606`). The Go port defers the destination commit to `Finish`
(`backup.go:384-391`): `Step`'s done-path only finalizes in-memory state (`backup.go:264-272`)
and the actual commit happens later. The consequence is that the durability boundary and the
point at which a commit error surfaces both move from `Step` to `Finish`, so a caller who
treats a successful final `Step` as "backup committed" can be wrong, and a commit failure is
reported from a different call than in SQLite.

<a id="drift-44-backup-finish-double-call-returns-error-not-no-op"></a>
### Drift: Backup Finish Double Call Returns Error Not No Op
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `backup.go:*Backup.Finish` (`internal/btree/backup.go:372-375`).

SQLite's `sqlite3_backup_finish(p)` is NULL-tolerant and idempotent: it returns `SQLITE_OK`
immediately when `p==0` (`backup.c:583`) and treats a second finish on a freed handle as a
benign no-op. The Go port adds an explicit `finished` flag (struct field, `backup.go:74-76`):
`Finish` returns `ErrBackupFinished` on a second call (`backup.go:372-375`) and `Step`
returns `ErrBackupFinished` after `Finish` (`backup.go:193-195`). The consequence is a
stricter, non-SQLite contract -- repeated finalization is surfaced as an error rather than
silently absorbed -- which can break callers (or wrappers) that rely on SQLite's idempotent
finish semantics.

<a id="drift-45-beginreadfast-skips-page-1-staleness-counter-reads"></a>
### Drift: BeginReadFast Skips Page 1 Staleness Counter Reads
- **Category:** new-feature  -  **Severity:** medium
- **Affected functions:** `db.go:*DB.BeginRead` (`db.go:741`),
  `db.go:*DB.BeginReadFast` (`internal/btree/db.go:741`, `db.go:669-682`, `db.go:727-730`,
  `db.go:1577-1596`), `db.go` (`internal/btree/db.go:639-682`, `db.go:739-743`).

Go parameterizes its read-transaction opener as `beginRead(readCounters bool)` (`db.go:642`)
and adds a public `BeginReadFast()` that passes `readCounters=false` (`db.go:741-743`); on
that fast path `pager.readHeaderCounters` is skipped and the transaction's
`diskFileChangeCounter`/`diskSchemaCookie` are seeded from the process-local cached counters
(`db.go:669-682`, `db.go:727-730`) rather than from on-disk page-1 metadata. Snapshot
isolation for actual data reads is preserved -- the path still fixes the WAL `maxFrame`/reader
slot and clears the per-connection reader cache on change-counter mismatch, matching SQLite's
`pagerBeginReadTransaction` reset behavior -- so the divergence is purely in the staleness
reporting layer, which has no SQLite analogue to begin with. The consequence is that on a
fast read `IsDataStale`/`IsSchemaStale` always return false and
`DiskFileChangeCounter`/`DiskSchemaCookie` return possibly-stale local values
(`db.go:1577-1596`); this is an undocumented new API whose semantics a caller must understand
to avoid mistaking a fast read's "not stale" for a verified cross-process check.

<a id="drift-46-public-multi-process-staleness-api-diverges-from-sqlite-auto"></a>
### Drift: Public Multi Process Staleness API Diverges From SQLite Auto Tracking
- **Category:** new-feature  -  **Severity:** medium
- **Affected functions:** `db.go` (`internal/btree/db.go:1577-1597`, `db.go:1646-1654`,
  `db.go:913-916`, `db.go:1684-1690`).

any-store exposes a caller-driven multi-process staleness protocol with no analogue in stock
SQLite. SQLite *automatically* increments the page-1 File Change Counter (offset 24) and
Schema Cookie (offset 40) inside the pager on every commit / schema change. any-store instead
makes counter bumping opt-in: `WriteTx.MarkDataChanged()`/`MarkSchemaChanged()`
(`db.go:1646-1654`) merely set `tx.dataChanged`/`tx.schemaChanged` flags, and `Commit` only
increments the on-disk counters when those flags are set (`db.go:1684-1690`), with
`UpdateLocalCounters` (`db.go:913-916`) and the `IsDataStale`/`IsSchemaStale`/
`DiskFileChangeCounter`/`DiskSchemaCookie` accessors (`db.go:1577-1597`) forming the rest of
the surface. The consequence is a fundamentally different contract from SQLite's automatic
tracking: a caller who forgets to call `MarkDataChanged`/`MarkSchemaChanged` will leave the
cross-process change counters unbumped, so other connections' staleness checks silently fail
to observe the change.

<a id="drift-47-checkpoint-omits-open-transaction-guard"></a>
### Drift: Checkpoint Omits Open Transaction Guard
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `db.go:*DB.Checkpoint` (`db.go:897`).

SQLite's `sqlite3BtreeCheckpoint` refuses to run if the *calling connection* has any
transaction open: `if( pBt->inTransaction!=TRANS_NONE ){ rc = SQLITE_LOCKED; }`
(`btree.c:11343-11344`), invoking `sqlite3PagerCheckpoint` only when `TRANS_NONE`. Go's
`DB.Checkpoint` (`db.go:897-907`) has no equivalent guard -- it only checks `db.closing` and
takes a `db.mu.RLock()` -- so it can proceed to checkpoint while the same handle holds an
open read or write transaction. The consequence is that a self-deadlock/inconsistency case
SQLite explicitly rejects with `SQLITE_LOCKED` is instead allowed by the Go port, letting a
connection attempt to checkpoint against its own in-flight transaction state.

<a id="drift-48-checkpoint-drops-frame-count-out-parameters"></a>
### Drift: Checkpoint Drops Frame Count Out Parameters
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*DB.Checkpoint` (`db.go:897`).

SQLite's checkpoint API returns, via out-parameters, the final number of frames in the WAL
(`pnLog`) and the number of frames checkpointed/backfilled (`pnCkpt`):
`sqlite3PagerCheckpoint` forwards `pnLog`/`pnCkpt` into `sqlite3WalCheckpoint`
(`pager.c:7510-7536`), where `*pnLog = pWal->hdr.mxFrame` and `*pnCkpt` is the backfill
count; these are surfaced by `sqlite3_wal_checkpoint_v2` and let callers monitor checkpoint
progress. Go's `DB.Checkpoint` signature (`db.go:897`) drops both out-parameters entirely.
The consequence is a reduced observability surface: callers cannot inspect how much of the
WAL existed or was backfilled by a checkpoint, a benign API-completeness divergence rather
than a correctness defect.

<a id="drift-49-non-passive-checkpoint-returns-success-instead-of-busy-on-in"></a>
### Drift: Non Passive Checkpoint Returns Success Instead Of BUSY On Incomplete Backfill
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `pager.go:*pager.checkpointWithMode` (`pager.go:2321-2329` (wrapper);
  `wal.go:3008-3012` and `wal.go:3340-3341` (suppression points)),
  `wal.go:*wal.checkpointPost` (`internal/btree/wal.go:3305-3307`),
  `wal.go:*wal.checkpointWithMode` (`wal.go:3295-3319` (checkpointPost;
  backfill<authoritativeMxFrame -> return nil)).

SQLite's `walCheckpoint` enforces the documented `sqlite3_wal_checkpoint_v2` contract for
non-PASSIVE modes (FULL/RESTART/TRUNCATE): after the backfill phase it runs
`if( pInfo->nBackfill < pWal->hdr.mxFrame ){ rc = SQLITE_BUSY; }` (`wal.c:2352-2356`), and its
final return `return (rc==SQLITE_OK && eMode!=eMode2 ? SQLITE_BUSY : rc)` (`wal.c:4425`) also
surfaces `SQLITE_BUSY` whenever the requested mode was silently downgraded to PASSIVE because
the writer lock could not be obtained (`eMode2 = SQLITE_CHECKPOINT_PASSIVE`, `wal.c:4356`).
Both signals tell the caller that active readers prevented the full WAL from being copied into
the DB, so the requested mode did not fully succeed. The Go port suppresses both: a single
incomplete-case gate in `checkpointPost` (`wal.go:3305-3307`) returns `nil` (success)
regardless of mode when `backfill < w.authoritativeMxFrame()`, and the write-lock busy
downgrade is likewise swallowed (`wal.go:3008-3012`, `wal.go:3340-3341`). The consequence is
an error-signaling / API-contract drift: a caller requesting a FULL/RESTART/TRUNCATE
checkpoint gets a false success and cannot tell that readers blocked the operation -- the data
path stays consistent, so this is not corruption, but the BUSY-means-retry semantics SQLite
guarantees are lost.

<a id="drift-52-checkpoint-missing-page-size-mismatch-and-over-grow-corrupti"></a>
### Drift: Checkpoint Missing Page Size Mismatch And Over Grow Corruption Guards
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.checkpoint` (`internal/btree/wal.go:3040-3043`).

SQLite's `sqlite3WalCheckpoint` carries two corruption guards the Go port omits: (1) a
page-size sanity check `if( pWal->hdr.mxFrame && walPagesize(pWal)!=nBuf ) rc =
SQLITE_CORRUPT_BKPT;` rejecting a checkpoint when the WAL's recorded page size disagrees with
the configured page/buffer size (`wal.c:4386-4387`); and (2) inside `walCheckpoint`, an
over-grow check `if( (nSize+65536+mxFrame*szPage)<nReq ) rc = SQLITE_CORRUPT_BKPT;` that flags
corruption when the DB would need to grow implausibly far. The Go `checkpoint` path
(`wal.go:3040-3043`) performs neither test. The consequence is that a corrupt WAL header (wrong
page size) or an implausibly over-grown checkpoint that SQLite would refuse with
`SQLITE_CORRUPT` is instead processed silently; the practical exposure is low because such
states are themselves rare, but the defensive corruption detection is absent.

<a id="drift-53-auto-checkpoint-escalates-to-wal-restart-beyond-passive"></a>
### Drift: Auto Checkpoint Escalates To WAL Restart Beyond Passive
- **Category:** new-feature  -  **Severity:** medium
- **Affected functions:** `pager.go:*pager.tryCheckpoint` (`internal/btree/pager.go:2333-2356`).

SQLite's auto-checkpoint is strictly PASSIVE: the default WAL hook
(`sqlite3WalDefaultHook`, `main.c:2471-2483`) fires when `nFrame >= nAutoCheckpoint` and calls
`sqlite3_wal_checkpoint(db, zDb)`, which uses `SQLITE_CHECKPOINT_PASSIVE` and flows through
`sqlite3PagerCheckpoint` (`pager.c:7510-7539`) with no post-PASSIVE escalation. Go's
`tryCheckpoint` (`pager.go:2333-2356`) first performs a pure PASSIVE backfill via
`checkpointPassive` and then, when that backfill completed, escalates to a best-effort
WAL-RESTART (`pager.go:2348-2354`) to reset the WAL. The consequence is a behavioral extension
beyond SQLite: an automatic checkpoint can reset/restart the WAL rather than leaving it for a
later explicit checkpoint, changing when the WAL is recycled relative to stock SQLite -- a new
feature that callers tuning checkpoint behavior should be aware of.

<a id="drift-54-close-time-checkpoint-runs-unconditionally-without-guards"></a>
### Drift: Close Time Checkpoint Runs Unconditionally Without Guards
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `db.go:*DB.Close` (`internal/btree/pager.go:2758-2789`).

SQLite's `sqlite3PagerClose` enables the close-time checkpoint (passing the non-NULL buffer
`a=pTmp` to `sqlite3WalClose`) only when `db && 0==(db->flags & SQLITE_NoCkptOnClose) &&
SQLITE_OK==databaseIsUnmoved(pPager)` (`pager.c:4189-4191`). `databaseIsUnmoved`
(`pager.c:4142-4161`) issues `SQLITE_FCNTL_HAS_MOVED` and, if the DB file has been
renamed/relinked out from under the open fd, returns `SQLITE_READONLY_DBMOVED` so the
checkpoint is skipped -- avoiding checkpointing into a file that is no longer the real
database, and honoring the `NoCkptOnClose` opt-out. The Go close-time checkpoint
(`pager.c:2758-2789` in the port) runs unconditionally with no `databaseIsUnmoved` /
`NoCkptOnClose` guard. The consequence is that closing a connection whose DB file was moved or
unlinked underneath it will still attempt to checkpoint, and callers have no way to suppress
the close-time checkpoint -- a behavior SQLite explicitly guards against.

<a id="drift-55-wal-file-truncated-but-never-unlinked-on-last-client-close"></a>
### Drift: WAL File Truncated But Never Unlinked On Last Client Close
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*DB.Close` (`internal/btree/wal.go:3396-3402`),
  `pager.go:*pager.close` (`pager.go:2785-2787` (truncate call) and `wal.go:3396-3402`
  (truncateFile)).

SQLite's `sqlite3WalClose`, after a successful checkpoint under the EXCLUSIVE DB-file lock,
deletes the `-wal` file in its default non-persistent-WAL mode: it queries
`SQLITE_FCNTL_PERSIST_WAL` and, if the result is not `1`, sets `isDelete = 1`
(`wal.c:2536-2540`) and unlinks the file via `sqlite3OsDelete(pWal->pVfs, pWal->zWalName, 0)`
(`wal.c:2553-2558`); only the persistent-WAL branch (`bPersist==1 && mxWalSize>=0`) instead
truncates the WAL to zero via `walLimitSize`. The Go `pager.close` path
(`pager.go:2785-2787`) calls `truncateFile` (`wal.go:3396-3402`), truncating the WAL to zero
length but never unlinking it. The consequence is that after the last client closes, a
zero-length `-wal` file is left behind on disk rather than being removed as stock SQLite would
do; this is benign in operation but diverges from SQLite's default file-lifecycle cleanup.

<a id="drift-58-wal-read-begin-backoff-off-by-one"></a>
### Drift: WAL Read Begin Backoff Off By One
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*DB.beginRead` (`internal/btree/wal.go:2421`).

SQLite's `walTryBeginRead` increments its back-off counter `*pCnt` at the *top* of the function
(`wal.c:3043` `(*pCnt)++;`), making it a 1-based invocation count, and gates its retry sleeps
on that 1-based value (first sleep at `if( *pCnt>5 )`, quadratic ramp at `if( *pCnt>=10 )`).
Go's `wal.beginReadHdr` -- driven by `db.beginRead` through `pager.beginReadHdr` -- increments
or tests the counter at a different point, so the quadratic back-off ramp starts one retry
later than SQLite (`wal.go:2421`). The consequence is a minor timing divergence in the
read-transaction retry path: under contention the Go reader sleeps one iteration behind
SQLite's schedule, which affects retry pacing only and not correctness.

<a id="drift-61-out-of-range-savepoint-release-errors-instead-of-no-op"></a>
### Drift: Out Of Range Savepoint Release Errors Instead Of No Op
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `db.go:*WriteTx.ReleaseSavepoint` (`internal/btree/pager.go:2288`),
  `pager.go:*pager.releaseSavepoint` (`pager.go:2288`).

SQLite's `sqlite3PagerSavepoint` guards its entire body with
`if( rc==SQLITE_OK && iSavepoint<pPager->nSavepoint )` (`pager.c:7017`), and its header comment
makes the contract explicit: "If iSavepoint is greater than (Pager.nSavepoint-1), then this
function is a no-op." (`pager.c:6990-6991`) -- so a RELEASE with an index `>= nSavepoint` silently
succeeds (returns `SQLITE_OK`), destroying nothing. Go's `releaseSavepoint`
(`pager.go:2288-2290`) instead does `if id < 0 || id >= len(p.savepoints) { return
ErrInvalidSavepoint }`, returning an error for an out-of-range savepoint id. The consequence is
that any higher layer relying on the documented no-op behavior of releasing an already-gone or
never-opened savepoint will hit a hard error in Go where SQLite would have quietly succeeded.

<a id="drift-62-full-in-transaction-rollback-isavepoint-minus-one-unsupporte"></a>
### Drift: Full In Transaction Rollback iSavepoint Minus One Unsupported
- **Category:** new-feature  -  **Severity:** low
- **Affected functions:** `db.go:*WriteTx.RollbackToSavepoint` (`internal/btree/pager.go:2183`).

SQLite's `sqlite3BtreeSavepoint`/`sqlite3PagerSavepoint` explicitly accept `iSavepoint == -1`
(`SAVEPOINT_ROLLBACK`) to mean "roll back the entire transaction contents but keep the transaction
open and the locks held" -- `btree.c:4618-4622` documents that "no locks are released and the
transaction remains open", `pager.c:7015` asserts `iSavepoint>=0 || op==ROLLBACK`, and the
playback path at `pager.c:3426` uses `pPager->dbOrigSize` / `pagerRollbackWal` for this whole-txn
case. Go's `RollbackToSavepoint` (`internal/btree/pager.go:2183`) has no support for this
in-transaction full rollback: there is no `-1`/`SAVEPOINT_ROLLBACK` sentinel and no path that
rewinds the transaction to its origin while keeping it open. The consequence is a missing SQLite
capability -- callers cannot perform a full in-transaction rollback that preserves the open
transaction and its locks, and must instead abort and re-begin.

<a id="drift-63-new-db-page-1-written-directly-to-file-bypassing-wal"></a>
### Drift: New DB Page 1 Written Directly To File Bypassing WAL
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `db.go:Open`
  (`internal/btree/pager.go:606-612 (also initNewDB entry pager.go:416-418, 542)`),
  `pager.go:*pager.initNewDB` (`internal/btree/pager.go:593-613`).

SQLite defers page-1 creation to the first write transaction: `newDatabase` dirties page 1 via
`sqlite3PagerWrite(pP1->pDbPage)` (`btree.c:3528`) and never touches the file directly, so the
actual disk write of the new page 1 happens later through the normal commit path (a WAL frame or
rollback-journal transaction) when the enclosing write transaction commits. Go's
`Open` -> `p.open()` -> `initNewDB()` instead eagerly builds the page-1 image (DB header + empty
leaf-index page) into a local buffer and writes it straight to the main DB file via
`p.file.WriteAt(writeBuf, 0)` followed by `fdatasync(p.file)` (`pager.go:606-612`), all inside
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
- **Affected functions:** `pager.go:*pager.freeOverflowChain` (`pager.go:2622-2631`),
  `pager.go:*pager.freeOverflowChain` (`pager.go:2600-2632`).

C's `clearCellOverflow` computes the exact expected overflow-page count from parsed payload
metadata -- `nOvfl = (nPayload - nLocal + ovflPageSize - 1)/ovflPageSize` (`btree.c:7004-7005`) --
and frees exactly that many via a `while(nOvfl--)` loop, skipping `getOverflowPage` on the final
iteration (guarded by `if( nOvfl )`, `btree.c:7018`) so it never reads the next-pointer of the last
page; additionally, for each page already in the cache it checks
`sqlite3PagerPageRefcount(pOvfl->pDbPage)!=1` and returns `SQLITE_CORRUPT_BKPT` instead of freeing
a page with more than one outstanding reference (`btree.c:7023-7039`), detecting a mis-typed or
shared "overflow" page still in use by a cursor. Go's `freeOverflowChain` (`pager.go:2600-2632`)
instead walks next-pointers to a zero terminator and omits both the fixed expected-count derivation
and the `refcount==1` outstanding-reference corruption check. The consequence is weaker corruption
detection on the overflow-free path: Go will follow a corrupt next-pointer chain and free a page
that is actually still referenced, where SQLite would have stopped with a corruption error.

<a id="drift-73-freepage2-trunk-decision-and-page-invalidation-drifts"></a>
### Drift: freePage2 Trunk Decision And Page Invalidation Drifts
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.freePage` (`internal/btree/pager.go:1221-1264`),
  `pager.go:*pager.freePage` (`internal/btree/pager.go:1242-1262`).

C's `freePage2` decides whether to append a freed page as a leaf of the existing first trunk or to
start a new trunk by testing the page-1 free-page COUNT -- `if( nFree!=0 )` where
`nFree = get4byte(&pPage1->aData[36])` (the total-freelist-pages counter at offset 36,
`btree.c:6864,6884`). Go's `freePage` instead branches on the FIRST-TRUNK POINTER at offset 32,
`trunkPgno := p.header.FirstFreelistPg` with `if trunkPgno != 0` (`pager.go:1221-1264`); the two
predicates only coincide in a header that is consistent between offsets 32 and 36. Additionally, C
always reaches `freepage_out:` and sets `pPage->isInit = 0` on the freed page for both the leaf and
new-trunk cases (`btree.c:6965-6968`), invalidating any cached parse of that page, whereas Go's leaf
path (`pager.go:1242-1262`) never fetches or touches the freed page object -- it only updates the
trunk's leaf array and records `dontWrite`/`setHasContent` by page number. The consequence is that
on an inconsistent header Go can pick the wrong free-list shape, and a stale cached header parse for
a freed leaf page can survive where SQLite would have discarded it.

<a id="drift-74-secure-delete-page-zeroing-on-free-unsupported"></a>
### Drift: secure_delete Page Zeroing On Free Unsupported
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.freePage` (`internal/btree/pager.go:1209-1301`).

C's `freePage2` honors `BTS_SECURE_DELETE`: when the pragma is enabled it fetches the freed page and
`memset(pPage->aData, 0, pPage->pBt->pageSize)` to scrub the deleted data (`btree.c:6867-6877`), and
it also suppresses the `PagerDontWrite` optimization in that mode so the zeroed page is actually
persisted (`btree.c:6937`). Go's `freePage` has no secure-delete concept at all
(`pager.go:1209-1301`): freed leaf-page contents are never zeroed and `dontWrite` is applied
unconditionally. The consequence is that deleted row/cell data physically remains in freed pages
on disk in Go where SQLite's secure_delete mode would have scrubbed it, and this gap is currently
undocumented.

<a id="drift-76-beginwrite-re-reads-page-1-header-on-state-change"></a>
### Drift: beginWrite Re Reads Page 1 Header On State Change
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `pager.go:*pager.beginWrite`
  (`internal/btree/pager.go:712` and `refreshHeaderFromPage1` at `internal/btree/pager.go:1623-1666`).

SQLite refreshes the page-1 header and `dbSize` on the READ path, not inside `sqlite3PagerBegin`. Go
relocates that refresh into the write path: `pager.beginWrite` calls both `p.writerCache.clear()`
and `p.refreshHeaderFromPage1()` when `wal.beginWriteWithSnapshot` reports `stateChanged=true`
(`pager.go:710-713`). `refreshHeaderFromPage1` (`pager.go:1623-1666`) re-reads page 1 from WAL or
disk and overwrites BOTH `p.header` and `p.dbSize` (`p.dbSize.Store(p.header.DatabaseSize)` at
`pager.go:1641,1657`). The consequence is a structural divergence in when and where the cached
header/size are reconciled with the on-disk image: Go performs this reconciliation lazily at the
start of a write when the underlying WAL state changed, rather than during reads as SQLite does.

<a id="drift-77-filechangecount-bumped-conditionally-not-unconditionally"></a>
### Drift: FileChangeCount Bumped Conditionally Not Unconditionally
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `pager.go:*pager.commit` (`internal/btree/pager.go:1902-1912`).

SQLite always advances the page-1 change counter (header offset 24, mirrored at offset 92) on a
committing transaction that writes pages: `pager_write_changecounter` (`pager.c:3084`) is an
unconditional update invoked from the commit machinery itself (`pagerWalFrames` at `pager.c:3218`
via `if( pList->pgno==1 ) pager_write_changecounter(pList);`, and `pager_incr_changecounter` at
`pager.c:6363`). Go's `commit` increments `p.header.FileChangeCount++` only `if dataChanged` (and
`p.header.SchemaCookie++` only `if schemaChanged`), gating the bump on the caller-supplied flag
rather than on the fact that data pages were written (`pager.go:1902-1907`). The consequence is that
a committing transaction that writes pages without the caller setting `dataChanged=true` leaves the
change counter unadvanced, so other connections relying on the counter to detect that the file
changed may miss the modification, whereas SQLite would always have bumped it.

<a id="drift-78-commit-does-not-prune-dirty-pages-above-dbsize-before-wal-wr"></a>
### Drift: Commit Does Not Prune Dirty Pages Above dbSize Before WAL Write
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pager.go:*pager.commit`
  (`internal/btree/pager.go:1931-1942`, and `wal.go:1944-2040` with no `pgno<=dbSize` filter).

SQLite's `pagerWalFrames` explicitly removes pages whose `pgno > nTruncate` (the post-commit
database size) from the dirty list before logging them, so no page beyond the database image is ever
written to the WAL and the commit frame is guaranteed to belong to a page within the image
(`pager.c:3199-3212`). Go's `commit` collects the entire dirty list unfiltered (`pager.go:1931`
`appendDirtyPages`) and `writeFrames` (`wal.go:1944-2040`) applies no `pgno <= dbSize` filter before
emitting frames, so there is no `nTruncate`-style pruning. The consequence is that a dirty page with
`pgno > dbSize` -- e.g. one left over from a since-truncated region -- can still be logged to the
WAL, and the commit (size-bearing) frame may end up attached to an over-size page rather than one
within the database image, which SQLite structurally prevents.

<a id="drift-79-truncateto-eager-dirty-page-drop-and-extra-guards"></a>
### Drift: truncateTo Eager Dirty Page Drop And Extra Guards
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `pager.go:*pager.truncateTo` (`pager.go:2652`),
  `pager.go:*pager.truncateTo` (`pager.go:2645`).

C's `sqlite3PagerTruncateImage` is a one-line operation -- its entire body is
`pPager->dbSize = nPage;` -- and it does NOT touch the page cache at all; the documentation states
it is "only called right before committing a transaction" and that "it is not safe to call this
function and then continue writing" (`pager.c:4018-4030`). Leaving above-size pages in the cache is
deliberate: a rollback to a savepoint taken BEFORE the truncate (which restores the larger `dbSize`)
still finds those pages intact. Go's `truncateTo` instead calls `p.writerCache.truncate(newDbSize)`
(`pager.go:2652`), which eagerly removes ALL cached pages above `newDbSize` -- including DIRTY ones,
unlinking them from the dirty list and decrementing `nDirty` (`pcache.go:702-715`) -- and adds two
non-C guards: it rejects truncate-to-zero with an error (`pager.go:2645-2647`) and silently no-ops
when `newDbSize >= cur` (`pager.go:2649-2651`), where C instead asserts
`pPager->dbSize >= nPage || CORRUPT_DB` (`pager.c:4018`). The consequence is a savepoint-rollback
hazard SQLite avoids: eagerly discarding dirty above-size pages means a later rollback to a
pre-truncate savepoint can no longer restore them, plus the added guards diverge from C's documented
shrink-only contract.

<a id="drift-80-inprocessshm-close-non-terminal-teardown"></a>
### Drift: inProcessShm close Non Terminal Teardown
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `shm.go:*inProcessShm.close` (`shm.go:114`).

C's `unixShmUnmap` performs a true teardown: it removes the connection from `pShmNode->pFirst`, frees
it, decrements `pShmNode->nRef`, and at `nRef==0` calls `unixShmPurge` which munmaps/frees every
region, closes the shm fd, and nulls the node -- after which the shm structure is gone and cannot be
used. Go's heap-fallback `inProcessShm.close` (`shm.go:114-120`) ignores the `isLastClient`
argument (`_ = isLastClient`), has no refcount equivalent, simply sets `s.regions = nil` under
`regMu`, and does NOT reset the per-slot lock state. The consequence is that close is a non-terminal,
partial teardown: the object remains reusable with its mapped regions cleared but its lock state
stale, diverging from SQLite's terminal connection-free/munmap-at-zero-refs semantics.

<a id="drift-81-in-process-shm-lock-collapses-per-connection-masks-to-single"></a>
### Drift: In Process Shm Lock Collapses Per Connection Masks To Single Refcount
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `shm.go:*inProcessShm.lock`
  (`shm.go:72-92`, esp. `l.state++` at `shm.go:84`; counter defined `shm.go:50-53`),
  `shm_mmap.go:*mmapShm.lock` (`internal/btree/shm_mmap.go:156`).

SQLite's `unixShmLock` distinguishes two layers of state: per-connection bitmasks
`p->sharedMask`/`p->exclMask` and a process-wide counter array `pShmNode->aLock[]`
(`os_unix.c:5291-5301`). The "is there work to do" guard
(`flags==(SQLITE_SHM_SHARED|SQLITE_SHM_LOCK) && 0==(p->sharedMask & mask)`, `os_unix.c:5351-5352`)
uses the per-connection mask so that a SHARED lock re-requested by the SAME connection that already
holds it is a NO-OP returning `SQLITE_OK` without touching `aLock[ofst]`. Go collapses both layers
into a single refcount: `inProcessShm.lock` does `l.state++` (`shm.go:84`, counter at
`shm.go:50-53`) and `mmapShm.lock` likewise increments without a per-connection sharedMask dedup
check (`shm_mmap.go:156`). The consequence is non-idempotent re-locking: a same-connection repeat
SHARED lock that SQLite treats as a free no-op instead bumps the shared refcount in Go, so the
lock/unlock accounting can drift from SQLite's behavior when a connection re-requests a lock it
already holds.

<a id="drift-88-wal-recovery-does-not-pre-seed-read-mark-slot-1"></a>
### Drift: WAL Recovery Does Not Pre Seed Read Mark Slot 1
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.recoverLocked`
  (`/Users/roma/anytype/any-store/internal/btree/wal.go:1856-1859`).

After recovery SQLite seeds read-mark slot 1 with the recovered `mxFrame` so the first reader can immediately
reuse it: `for(i=1;i<WAL_NREADER;i++){ ... if(i==1 && pWal->hdr.mxFrame){ pInfo->aReadMark[i]=pWal->hdr.mxFrame; } else { pInfo->aReadMark[i]=READMARK_NOT_USED; } ... }` (`wal.c:1576-1583`). Go's `recoverLocked`
instead stores `readMarkNotUsed` (`0xFFFFFFFF`) into every slot 1..4 and only sets `aReadMark[0]=0`
(`wal.go:1856-1859`). The consequence is a missed optimization rather than a correctness bug: the first
reader after recovery cannot reuse a pre-seeded slot at the recovered `mxFrame` and must instead carve out
a fresh read-mark, diverging from SQLite's seeded fast path.

<a id="drift-90-wal-index-szpage-field-not-encoded-or-decoded"></a>
### Drift: WAL Index szPage Field Not Encoded Or Decoded
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*walIndex.readHeader`
  (`internal/btree/wal.go:819-842` writeHeader never sets szPage; `wal.go:444` / `wal.go:881-931`
  readHeader returns szPage undecoded).

C's `walIndexTryHdr` decodes the page size from the SHM header on every successful read —
`pWal->szPage = (pWal->hdr.szPage&0xfe00) + ((pWal->hdr.szPage&0x0001)<<16);` (`wal.c:2627`) — mapping the
on-wire encoding (where 1 means 65536) back to the real page size. The raw 16-bit `szPage` field IS
round-tripped by Go's serialize/deserialize/computeCksum (`wal.go:426,444,465`), but `writeHeader`
(`wal.go:819-842`) never populates `hdr.szPage` (it stays zero) and `readHeader` never applies the decode
transform. The consequence is that Go neither encodes a meaningful page size into the wal-index header on
write nor reconstructs it on read; the field carries no usable page-size information, diverging from
SQLite's per-read `szPage` decode (low impact because Go derives page size elsewhere).

<a id="drift-92-shmhashget-skips-segment-on-map-io-error"></a>
### Drift: shmHashGet Skips Segment On Map IO Error
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*walIndex.shmHashGet` (`internal/btree/wal.go:1072-1075`).

C's `walFindFrame` aborts the entire frame lookup on any hash-segment map failure:
`rc = walHashGet(pWal, iHash, &sLoc); if(rc!=SQLITE_OK){ return rc; }` (`wal.c:3576-3579`), so an SHM
map/extend IO error (`walHashGet` -> `walIndexPage` -> `sqlite3OsShmMap`) propagates up to the reader. Go's
`shmHashGet` instead does `region, err := wi.shm.region(seg, true); if err != nil { continue }`
(`wal.go:1072-1075`), silently skipping that hash segment and continuing the scan. The consequence is that a
genuine SHM mapping/IO error is swallowed rather than reported: the lookup proceeds over the remaining
segments and may return a stale or missing-frame result where SQLite would have failed the read with the
underlying error.

<a id="drift-93-wal-hash-probe-full-chain-corruption-signal-dropped"></a>
### Drift: WAL Hash Probe Full Chain Corruption Signal Dropped
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
  (`internal/btree/wal.go:997-1002` definition; `wal.go:1088` use in shmHashGet).

C's `walHashGet` (`wal.c:1167`) returns only `aHash`, `aPgno` (base) and `iZero` — no entry count — and
`walFindFrame`'s lookup indexes `sLoc.aPgno[(iH-1)&(HASHTABLE_NPAGE-1)]==pgno` (`wal.c:3589`) using a bitmask
with no explicit upper bound, relying on the frame-range invariants for safety. Go's `htSegmentInfo`
(`wal.go:997`) additionally returns `nEntry` (`htNPageOne=4062` for segment 0, `htNPage=4096` otherwise), and
`shmHashGet` uses it as a defensive index bound: `idx := int(entry)-1; if idx < nEntry { storedPgno = region[pgnoBase+idx*4] }`
(`wal.go:1088`). The consequence is a behavioral divergence in how the `aPgno` index is constrained: Go
replaces SQLite's `(iH-1)&(HASHTABLE_NPAGE-1)` wraparound mask with a hard `nEntry` upper-bound check, so an
index that C would wrap-and-read Go instead rejects/skips — a different (and stricter) handling of
out-of-range hash entries.

<a id="drift-95-shmcleanupfromframe-zeros-all-segments-above-target"></a>
### Drift: shmCleanupFromFrame Zeros All Segments Above Target
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `wal.go:*walIndex.rollbackToFrame`
  (`internal/btree/wal.go:679-715` shmCleanupFromFrame, called from rollbackToFrame `wal.go:651`;
  missing zero-init in shmHashWrite `wal.go:1010-1040`).

SQLite's `walCleanupHash` (`wal.c:1233-1288`) deliberately cleans up ONLY the single hash table that contains
`pWal->hdr.mxFrame`: it calls `walHashGet(walFramePage(mxFrame))` (`wal.c:1252`) to obtain that one segment,
then zeroes `aHash[i] > iLimit` and memsets `aPgno[iLimit..]` within just that segment — the header comment
is explicit that "At most only the hash table containing `pWal->hdr.mxFrame` ..." needs cleaning, because the
`idx==1` zero-init in `walIndexAppend` lazily clears higher segments on reuse. Go's `shmCleanupFromFrame`
(`wal.go:679-715`) instead zeros ALL segments above the target frame, and (per drift 91) lacks the
`walIndexAppend` first-entry zero-init that C relies on. The consequence is a compensating-but-divergent
strategy: Go eagerly scrubs every higher segment on rollback to make up for the missing lazy zero-init,
diverging from SQLite's single-segment cleanup contract and doing strictly more work, with correctness
contingent on that eager scrub fully covering what C's `idx==1` reset would have handled.

<a id="drift-96-wal-index-change-counter-ichange-never-incremented"></a>
### Drift: WAL Index Change Counter iChange Never Incremented
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.writeFrames`
  (`internal/btree/wal.go:833` writeHeader writes wi.iChange, never incremented; commit path
  `wal.go:2106-2110`),
  `wal.go:*walIndex.writeHeader` (`internal/btree/wal.go:833`; decl `wal.go:541`, read-only).

SQLite's `walFrames` increments the wal-index header change counter once per committed transaction — inside
`if(isCommit)` it executes `pWal->hdr.iChange++;` (`wal.c:4248`) just before `walIndexWriteHdr(pWal)`
(`wal.c:4253`), so each commit publishes an incremented `iChange` into the SHM header. any-store reserves the
same field (`WalIndexHdr.iChange uint32` at `wal.go:407,541`, serialized at bytes 8..11) and `writeHeader`
copies it via `wi.hdr.iChange = wi.iChange` (`wal.go:833`), but `wi.iChange` is NEVER incremented anywhere in
the package despite the declaration comment claiming it is "incremented on each write transaction"
(`wal.go:540`). The consequence is that the wal-index change counter is always published as 0: the
per-transaction monotonic counter SQLite uses (e.g. to let other connections cheaply detect that the schema
or content changed) is effectively dead in the Go port, diverging from SQLite's per-commit increment.

<a id="drift-97-wal-restart-randomizes-both-salts-instead-of-incrementing-sa"></a>
### Drift: WAL Restart Randomizes Both Salts Instead Of Incrementing Salt0
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.writeHeader` (`internal/btree/wal.go:1670`).

SQLite's `walRestartHdr` deterministically advances salt-1 by `aSalt[0] = 1 + aSalt[0]` and only randomizes
salt-2, so each new WAL generation's salt-1 is a monotonic successor of the previous header's value. Go's
`writeHeader` (reached from `doResetWAL`, the analog of `walRestartHdr`) instead regenerates BOTH salts as
fresh independent random 32-bit values — `w.header.salt1 = rand.Uint32()` and `w.header.salt2 = rand.Uint32()`
(`wal.go:1670-1671`) — with no relation to the prior generation's salt-1. The consequence is that the
deterministic monotonic relationship SQLite maintains across WAL restarts is lost: any reasoning or tooling
that relies on salt-1 incrementing by one per restart no longer holds, though correctness still rests on the
salt pair simply differing from the previous generation.

<a id="drift-98-wal-checkpoint-sequence-number-nckpt-never-incremented"></a>
### Drift: WAL Checkpoint Sequence Number nCkpt Never Incremented
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.doResetWAL`
  (`internal/btree/wal.go:1669` writeHeader sets `checkpoint:0`; doResetWAL `wal.go:3360-3392` calls
  writeHeader; initHeaderStateLocked `wal.go:1624`),
  `wal.go:*wal.writeHeader` (`internal/btree/wal.go:1669`).

SQLite's `walRestartHdr` does `pWal->nCkpt++` on every WAL restart (`wal.c:2150`), making the on-disk WAL
header "Checkpoint sequence number" field (offset 12) a monotonically increasing counter that advances on each
RESTART/TRUNCATE checkpoint and each writer-initiated log wrap. The Go reset path always constructs the header
with `checkpoint: 0` at both sites — `writeHeader` (`wal.go:1669`) and `initHeaderStateLocked` (`wal.go:1624`)
— and `doResetWAL` (`wal.go:3360-3392`) invokes one of those on every reset, so the field is serialized at
offset 12 (`wal.go:244`), deserialized (`wal.go:262`), but never fed back to re-increment. The consequence is
that the checkpoint-sequence counter is always 0: SQLite's monotonic per-restart sequence number is dead in
the port, so any cross-process logic keyed off an advancing nCkpt cannot distinguish WAL generations.

<a id="drift-99-wal-restart-read-mark-reset-diverges-from-walrestarthdr"></a>
### Drift: WAL Restart Read Mark Reset Diverges From walRestartHdr
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*walIndex.reset`
  (`internal/btree/wal.go:803-805` read-mark clobber; `internal/btree/wal.go:1669-1671` missing
  nCkpt/salt increments).

C's `walRestartHdr` treats slot-0's read-mark as a permanent invariant fixed at 0 (it never touches
`aReadMark[0]` and asserts `aReadMark[0]==0`, `wal.c:2159`), explicitly sets `aReadMark[1]=0` (`wal.c:2157`),
sets only `aReadMark[2..4]=READMARK_NOT_USED` (`wal.c:2158`), and additionally performs `pWal->nCkpt++`
(`wal.c:2150`), `aSalt[0] = 1 + aSalt[0]` (`wal.c:2152`), and a fresh random `aSalt[1]` (`wal.c:2153`). Go's
`reset()` instead clobbers ALL five slots — including slot 0 and slot 1 — to `readMarkNotUsed` (0xFFFFFFFF)
via `for i := range wi.aReadMark { wi.aReadMark[i].Store(readMarkNotUsed) }` (`wal.go:803-805`), and the
`doResetWAL`/`writeHeader` pair drops the nCkpt and salt-1 increments (`wal.go:1669-1671`). This is the same
root as the restart counter/salt drifts (97, 98) surfacing on the read-mark/reset path: the consequence is
that the `aReadMark[0]==0` invariant is broken on restart (a slot-0 reader normally views only the db file)
and the deterministic restart sequence/salt advances are lost.

<a id="drift-102-reader-slot-tie-break-selects-lowest-not-highest"></a>
### Drift: Reader Slot Tie Break Selects Lowest Not Highest
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.tryBeginReadMultiProcess` (`internal/btree/wal.go:2583-2591`).

SQLite's slot-selection loop initializes `mxReadMark=0, mxI=0` and uses a non-strict comparison `if(
mxReadMark<=thisMark && thisMark<=mxFrame )` (`wal.c:3178`), so when two slots hold the SAME read-mark value the
later (higher-index) slot overwrites `mxI` and SQLite ends up locking the highest-numbered tied slot. Go's port
initializes `bestSlot=-1, bestMark=0` and selects with a strict comparison, so on equal marks it keeps the
first (lowest-index) slot it found (`wal.go:2583-2591`). The consequence is a behavioral tie-break divergence:
Go pins the lowest-numbered slot among equals where SQLite pins the highest. This does not affect read
correctness but changes which physical slot is held, altering slot-occupancy patterns that other processes
observe.

<a id="drift-104-padtosectorboundary-sector-padding-of-commit-frames-not-port"></a>
### Drift: padToSectorBoundary Sector Padding Of Commit Frames Not Ported
- **Category:** changed-logic  -  **Severity:** medium
- **Affected functions:** `wal.go:*wal.open` (`wal.go:1312`),
  `wal.go:*wal.writeFrames` (`internal/btree/wal.go:2086-2090`, single fdatasync on commit; no padding loop).

SQLite's `sqlite3WalOpen` sets `pRet->padToSectorBoundary = 1` (cleared only on POWERSAFE_OVERWRITE devices),
and `walFrames` (`wal.c:4189-4208`) uses it on a synchronous commit to compute a sync point at the next disk
sector boundary and repeatedly re-write the last frame (with its commit mark) up to that boundary before
fsyncing only that region — so the durably-synced region never ends mid-sector / mid-frame. Go's `wal.open`
has no `padToSectorBoundary` concept (`wal.go:1312`) and `writeFrames` performs a single `fdatasync(w.file)` on
commit with no padding loop (`wal.go:2086-2090`); a package-wide search finds no sector / iSyncPoint / nExtra /
powersafe logic anywhere. The consequence is that on devices where a torn write at the synced offset can
corrupt an adjacent partially-written sector, the Go WAL lacks SQLite's defensive sector alignment, so a
power-loss mid-write could leave the committed region straddling a sector that hardware partially overwrote.

<a id="drift-105-journal-size-limit-wal-truncation-feature-unimplemented"></a>
### Drift: journal_size_limit WAL Truncation Feature Unimplemented
- **Category:** new-feature  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.endWrite` (`wal.go:2776`),
  `wal.go:*wal.truncateFile` (`internal/btree/wal.go:3396`),
  `wal.go:*wal.writeFrames`
  (`internal/btree/wal.go:2072-2116`, commit block has no size-limit truncation).

SQLite implements `PRAGMA journal_size_limit` via the `truncateOnCommit`/`mxWalSize` machinery: after the
first transaction completing a WAL, `walFrames` does `if(isCommit && pWal->truncateOnCommit &&
pWal->mxWalSize>=0)` to shrink the WAL toward the configured limit and then clears the flag (`wal.c:4214-4221`),
and `sqlite3WalEndWriteTransaction` resets `truncateOnCommit=0` alongside `writeLock`/`iReCksum`
(`wal.c:3747-3752`). The Go port omits the entire feature: `endWrite` resets only `w.iReCksum` and unlocks
`lockWrite` with no `truncateOnCommit` field or reset (`wal.go:2776`); `writeFrames`' commit block does
`rewriteChecksums` + `fdatasync` + header publish with no size-limit truncation (`wal.go:2072-2116`); and
`truncateFile` ports only the close-time truncate, not the commit-time `walLimitSize` call site
(`wal.go:3396`). The consequence is that any-store WAL files grow without the periodic commit-time shrink-back
SQLite provides under `journal_size_limit`; the bound is enforced only by full checkpoint/reset rather than
incremental truncation.

<a id="drift-106-wal-read-only-fallback-not-ported"></a>
### Drift: WAL Read Only Fallback Not Ported
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.open` (`wal.go:1328`).

SQLite's `sqlite3WalOpen` opens the WAL with `SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE` and, if the VFS
downgrades the open to `SQLITE_OPEN_READONLY`, records `pRet->readOnly = WAL_RDONLY` (`wal.c:1719-1723`) so the
connection can still attach to a database sitting on read-only media. Go's `wal.open` always opens with
`os.O_RDWR|os.O_CREATE` (`wal.go:1328`) and returns the OS error directly on failure with no read-only retry;
the `wal` struct has no `readOnly`/RDONLY field anywhere. The consequence is that any-store cannot open a
WAL-mode database on read-only storage at all, whereas SQLite degrades gracefully to a read-only attachment —
a platform-support gap rather than a correctness bug on writable media.

<a id="drift-107-syncheader-device-characteristic-tuning-not-ported"></a>
### Drift: syncHeader Device Characteristic Tuning Not Ported
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `wal.go:*wal.open` (`wal.go:1646-1660`).

SQLite's `sqlite3WalOpen` sets `syncHeader = 1` and then clears it when the device reports
`SQLITE_IOCAP_SEQUENTIAL` (`wal.c:1714`, `wal.c:1731`); `syncHeader` gates the explicit header fsync before the
first frame (`wal.c:4115`), so on sequential-write devices that header sync is safely skipped. Go's `wal.open`
has no `syncHeader` field and unconditionally calls `fdatasync(w.file)` for the header in both `flushHeader`
(`wal.go:1656`) and `writeHeader` (`wal.go:1680`). The consequence is a strictly-more-conservative behavior:
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
atomic OS shm-lock operation; the only `n>1` call site is `wal.c:2361`
`walBusyLock(pWal, xBusy, pBusyArg, WAL_READ_LOCK(1), WAL_NREADER-1)`, which locks reader slots 1..4 as a single
range. Go's `walBusyLock` signature is instead `(wi, xBusy, slot, lockType)` and locks a single slot
(`wal.go:211`). The consequence is that any-store cannot express SQLite's atomic multi-slot range lock; where
SQLite grabs reader slots 1..4 in one operation, Go must lock them individually, which changes the lock-granularity
contract but is acceptable because Go does not exercise the multi-slot reader range the way C does.

<a id="drift-113-integrity-freeblock-walk-and-coverage-diagnostics-diverge"></a>
### Drift: Integrity Freeblock Walk And Coverage Diagnostics Diverge
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `integrity.go:*integrityChecker.checkPageCoverage`
  (`internal/btree/integrity.go:181-223`, `:198-201`, `:183-196`).

Go's `checkPageCoverage` freeblock handling diverges from SQLite in three ways. First, it reports a diagnostic and
`break`s on the FIRST malformed freeblock (out-of-range, size<4, extends-off-page, or unordered chain,
`integrity.go:181-203`) yet then falls through unconditionally into the overlap/fragmentation coverage analysis
(`L205-223`) using a heap that is missing every freeblock at or beyond the break point — producing a spurious
extra diagnostic SQLite never emits. Second, its ordering check is weaker: it flags an unordered chain only when
`nextFb != 0 && nextFb <= fb` (`integrity.go:198`), requiring the next link to merely start after the current
freeblock's START, whereas SQLite's invariant (`btree.c:11069`: `j==0 || j>i+size`) requires it to start after the
current freeblock's END. Third, it adds runtime range/size validation (`fb < contentOffset` lower-bound,
`fbSize < 4`, `fb+fbSize > usableSize`, `integrity.go:183-196`) where SQLite relies on prior
`btreeComputeFreeSpace` asserts and only upper-bound-checks. The consequence is purely diagnostic divergence:
different, sometimes spurious or differently-ordered integrity messages, with no impact on data correctness.

<a id="drift-114-integrity-report-inverts-zero-error-budget-and-omits-progres"></a>
### Drift: Integrity report Inverts Zero Error Budget And Omits Progress Hook
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `integrity.go:*integrityChecker.report`
  (`internal/btree/integrity.go:52`, `:56`).

C's `checkAppendMsg` gates on `if(!pCheck->mxErr) return;` (`btree.c:10626`) and the integrity walk loops are
guarded `&& pCheck->mxErr`, so an initial `mxErr==0` means STOP IMMEDIATELY / report nothing — and helpers like
`checkOom` deliberately set `mxErr=0` to halt the entire check. Go's equivalent `report()` -> `tooManyErrors()`
evaluates `ic.maxErrors > 0 && len(ic.errors) >= ic.maxErrors` (`integrity.go:52`), which inverts the meaning of a
zero budget: with `maxErrors==0` Go treats it as "unlimited" and keeps reporting rather than halting. Separately,
C's `checkAppendMsg` unconditionally calls `checkProgress(pCheck)` as its first action on every message-append
(`btree.c:10625`), reading the interrupt flag and optional `xProgress` callback and aborting the check on
interrupt/cancel; Go's `report()` omits this per-message progress/interrupt hook entirely (`integrity.go:56`). The
consequence is two behavioral divergences: a zero error budget no longer aborts as SQLite intends, and a long
integrity check cannot be interrupted or report progress mid-walk.

<a id="drift-115-cursor-skip-and-appendvaluebykey-new-apis-beyond-sqlite-surf"></a>
### Drift: Cursor Skip And AppendValueByKey New APIs Beyond SQLite Surface
- **Category:** new-feature  -  **Severity:** low
- **Affected functions:** `btree.go:Cursor.Skip` (`internal/btree/btree.go:3839`),
  `btree.go:Cursor.SkipBackward` (`btree.go:3866`),
  `btree.go:Cursor.AppendValueByKey` (`internal/btree/btree.go:3424`).

any-store adds cursor APIs with no SQLite `BtCursor` counterpart. `Skip(n)`/`SkipBackward(n)` advance or rewind the
cursor by `n` positions using an O(1) in-page `cellIdx` bump and only cross leaf boundaries via `Next()`/
`Previous()`, giving a batched O(N/entries_per_page) skip used in production by the query planner's offset/limit
path (`internal/qplanner/fullscan_iter.go:59-61`). `AppendValueByKey` (`btree.go:3424`) composes `SeekNear` + an
exact-key check + value extraction, appending the value bytes directly into a caller buffer to avoid extra
parse/copy work — for non-overflow cells it appends `cell.value` (a slice into the pinned page buffer) directly and
falls back to `Cursor.Value()` reconstruction only for overflow payloads. The consequence is added public surface
beyond the documented cursor API (NOTES.md section 15 lists only `SeekNear` as an any-store extension), so the
cursor contract a maintainer infers from NOTES is incomplete.

<a id="drift-116-open-forces-inprocess-on-non-mmap-shm-platforms-and-build-ta"></a>
### Drift: Open Forces InProcess On Non Mmap SHM Platforms And Build Tag Scope Drift
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `db.go` (`internal/btree/db.go:364-367` force InProcess when `!hasMmapShm`),
  `dbfile_lock_other.go` (`internal/btree/dbfile_lock_other.go:1,6,10-12` `//go:build !unix` no-op stubs),
  `dbfile_lock_unix.go` (`internal/btree/dbfile_lock_unix.go:1,34-73` `//go:build unix` real flock),
  with `shm_mmap.go` (`//go:build (linux||darwin)&&(amd64||arm64)`) / `shm_other.go`
  (`//go:build !((linux||darwin)&&(amd64||arm64))`) and the `isLastClient` gate at `pager.go:2773-2787`.

`Open` silently coerces `opts.InProcess = true` whenever `!hasMmapShm` (`db.go:364-367`), forcing single-process
heap-SHM mode on any platform lacking mmap SHM (e.g. Windows) with no error. The build-tag matrix diverges and is
mis-documented: `dbfile_lock_*.go` split on `unix` vs `!unix`, while `shm_*.go` split on
`(linux||darwin)&&(amd64||arm64)` vs its negation — so a unix platform that is NOT linux/darwin-on-amd64/arm64
(linux/386, linux/riscv64, freebsd, darwin on an unsupported arch) compiles the REAL flock implementation yet still
gets `hasMmapShm=false` and is forced into single-process mode. On non-unix targets the lock stubs are pure no-ops
where `tryUpgradeDBLockExclusive` returns `(true, nil)` unconditionally (`dbfile_lock_other.go:10-12`), and since
`pager.close` uses that result as `isLastClient` to gate `wal.truncateFile()` and `shm.close(isLastClient)`
(`pager.go:2773-2787`), every non-unix closer believes it is the last client. NOTES.md compounds this by
referencing an obsolete single `dbfile_lock.go`, citing stale source locations (the stub comment points at
`db.go:201-204`, now `buildCodec` encryption code; the real forcing is `db.go:364-366`; NOTES line 415 points at a
moved file), and never documenting the no-op lock stubs at all. The consequence is undocumented platform behavior
that could mislead a maintainer auditing the multi-process lock protocol on non-mainstream platforms.

<a id="drift-117-heap-and-inprocess-shm-implement-real-locks-contradicting-no"></a>
### Drift: Heap And InProcess SHM Implement Real Locks Contradicting NOTES
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `shm.go` (`internal/btree/shm.go:34-40` newHeapShm/inProcessShm,
  `:36-112` lock at `72-92`), `shm_other.go` (`internal/btree/shm_other.go:1,29-34,55-99`
  heapShm.lock/unlock), with selection paths `wal.go:552-562` and `db.go:360-367`.

NOTES.md describes a single undifferentiated "Heap SHM fallback" with no-op locks (lines 373-374, 393), but the
code ships two distinct heap-backed shm types, both implementing REAL per-slot shared/exclusive lock semantics.
`newHeapShm()` returns `*inProcessShm` (`shm.go:34-40`), whose `lock`/`unlock` (`shm.go:72-112`) maintain genuine
per-slot lock counts backed by `sync.Mutex` + an int state (0=unlocked, >0=shared count, -1=exclusive) and return
`ErrBusy` on conflict; these are actually exercised by the WAL code (`lockWrite`/`lockCheckpoint`/`lockRecover`
exclusive, `lockRead0`... shared) in InProcess/InMemory mode. The separate `heapShm` (`shm_other.go:55-99`) on
non-mmap platforms implements the same real per-slot counter semantics. Crucially `inProcessShm` is selected on ALL
platforms — including mmap-capable linux/darwin amd64/arm64 — whenever `inProcess==true` (forced for InMemory and
InProcess, `db.go:360-367`, `wal.go:552-562`). The consequence is that NOTES is wrong on both counts: the
"no-op locks" claim and the platform matrix both misrepresent the real, always-on heap lock implementation.

<a id="drift-121-fdatasync-durability-primitive-platform-split-undocumented"></a>
### Drift: fdatasync Durability Primitive Platform Split Undocumented
- **Category:** platform-support  -  **Severity:** medium
- **Affected functions:** `osfuncs_sync_linux.go` (`internal/btree/osfuncs_sync_linux.go:10` ; `internal/btree/osfuncs_sync_other.go:7`),
  `osfuncs_vfs_js.go` (`internal/btree/osfuncs_vfs_sync_linux.go:7`; `internal/btree/osfuncs_vfs_sync_other.go:5`;
  `internal/btree/osfuncs_sync_linux.go:10`; `internal/btree/osfuncs_sync_other.go:7`).

The `fdatasync` durability primitive invoked on every WAL commit (`wal.go:1656/1680/2087`) and checkpoint
(`wal.go:3141/3277`, `pager.go:610`) is selected by a build-tag matrix that silently changes its sync semantics
per platform, and NOTES.md documents none of it. On Linux the non-vfs build calls `syscall.Fdatasync(int(f.Fd()))`
(`osfuncs_sync_linux.go:10`, tag `!vfs && linux`), a true data-only sync matching SQLite's `HAVE_FDATASYNC`; on
every other platform (darwin, windows, the BSDs) it falls back to `f.Sync()` (`osfuncs_sync_other.go:7`, tag
`!vfs && !(js && wasm) && !linux`), i.e. a full fsync that also flushes inode metadata. The vfs / wasm build mirrors
this split for `defaultFdatasync` (`osfuncs_vfs_sync_linux.go:7` `syscall.Fdatasync` vs `osfuncs_vfs_sync_other.go:5`
`f.Sync()`). The consequence is that the cost and exact durability guarantee of the commit/checkpoint hot path
differs by OS — Linux gets the cheaper data-only flush SQLite assumes, while all other platforms pay for full
metadata fsync — a semantic platform divergence a maintainer reasoning about commit performance or crash durability
cannot discover from NOTES.

<a id="drift-122-inmemory-masterstore-disk-emulation-undocumented"></a>
### Drift: InMemory masterStore Disk Emulation Undocumented
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `pager.go` (`internal/btree/pager.go:43` masterStore type, `:49` readPageInto,
  `:60` writePage, `:1058` readRawPage fallback, `:1725` readHeaderCounters InMemory branch).

For InMemory databases there is no file on disk, so `pager.open` creates a `masterStore` — an `RWMutex`-protected
`map[uint32][]byte` (`pager.go:43`) — that REPLACES the database file as the "disk" backing, holding checkpointed
page data flushed out of the WAL. Its `readPageInto`/`writePage` (`pager.go:49,60`) form a VFS-disk emulation:
`checkpointPassive` writes pages into the map, and the page-read paths (`readRawPage` at `:1058`,
`readHeaderCounters` at `:1725`) fall back to it whenever `p.file == nil`. This is an in-process stand-in for the
real file VFS with no SQLite analogue, and it is entirely undocumented in NOTES.md. The consequence is that a
maintainer tracing where checkpointed pages physically land for an InMemory DB has no documentation pointing at the
map-backed "disk", and any reasoning about durability or post-checkpoint page state must reverse-engineer this layer.

<a id="drift-123-process-global-page-buffer-pool-single-page-size-constraint"></a>
### Drift: Process Global Page Buffer Pool Single Page Size Constraint
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `page_slab.go` (`internal/btree/page_slab.go:53-66` initPageBufferPool,
  `:45-47` pageBufferPoolSize, `errors.go:81-84` ErrPageBufferPoolSizeMismatch, `db.go:373` enforcement at Open),
  `page_slab.go` (`internal/btree/page_slab.go:41-47` pageBufferPool/pageBufferPoolSize,
  `:78-107` allocPageBuffer/freePageBuffer dispatch, `:161-164` + `:185-189` slab overflow recycles via default pool).

In the default (non-slab) mode, page-buffer allocation routes through a single process-global `sync.Pool`
(`pageBufferPool`, `page_slab.go:41-47`) shared across every DB in the process, keyed to one global page size held in
the atomic `pageBufferPoolSize`. `initPageBufferPool` (`page_slab.go:53-66`) CAS-sets that size on first init and
thereafter returns `ErrPageBufferPoolSizeMismatch` (`errors.go:81-84`) for any DB opened with a different `PageSize`;
`db.Open` enforces this unconditionally on every open (`db.go:373`). Because `useSlab` defaults false
(`pcache.go:36,133`, `pager.go:202-204,270`), all page buffers for all DBs come from and return to this one shared
pool, and even slab-mode OVERFLOW buffers recycle through the same default pool (`page_slab.go:161-164,185-189`). The
consequence is an undocumented process-wide constraint with no SQLite counterpart: two DBs in the same process cannot
use different page sizes in default mode, and the shared-pool/overflow-recycling design is invisible to a maintainer
relying on NOTES.

<a id="drift-124-debug-tracing-subsystem-undocumented"></a>
### Drift: Debug Tracing Subsystem Undocumented
- **Category:** platform-support  -  **Severity:** low
- **Affected functions:** `debug_trace.go` (`internal/btree/debug_trace.go:1-7`;
  `internal/btree/debug_trace_on.go:1-31` trace at `:29`, BTREE_TRACE env handling at `:14-27`),
  `debug_trace_on.go` (`internal/btree/debug_trace_on.go:12` init; build-tag pair
  `debug_trace_on.go:1` `//go:build debugtrace` vs `debug_trace.go:1` `//go:build !debugtrace`).

The btree package ships a Go-only, build-tag-gated debug tracing facility with no SQLite C counterpart and no NOTES.md
documentation, split across two files. The default build (`debug_trace.go:1`, tag `!debugtrace`) defines
`const debugTrace = false` and a no-op `func trace(format string, args ...any) {}` so tracing compiles away entirely.
Under `-tags debugtrace` (`debug_trace_on.go:1`) `debugTrace = true` and a package-init `init()` (`debug_trace_on.go:12`)
reads the `BTREE_TRACE` environment variable at process startup, routing log output: empty / `"1"` / `"stderr"` go to
stderr, while any other value is treated as a file path opened with `os.OpenFile(v, O_CREATE|O_WRONLY|O_APPEND, 0644)`
(`debug_trace_on.go:14-27`), creating the file if absent. The consequence is an undocumented diagnostic subsystem with
an environment-driven side effect (file creation) that a maintainer cannot discover from NOTES, and which has no place
in the C-to-Go mapping because it is purely a Go-side addition.

<a id="drift-125-dead-or-non-protocol-crc32-checksum-helpers"></a>
### Drift: Dead Or Non Protocol CRC32 Checksum Helpers
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `page.go` (`internal/btree/page.go:556-559`), `wal.go` (`wal.go:3432-3435`).

Two CRC32-IEEE helpers exist that are unrelated to the actual WAL checksum, which NOTES.md §7 documents as a
paired-word Fletcher-style additive recurrence (`s1 += x[i]+s2; s2 += x[i+1]+s1`), and both are misleading. `page.go:557`
defines `func checksum(data []byte) uint32 { return crc32.ChecksumIEEE(data) }` carrying the doc comment "checksum
computes a CRC32 checksum for data (used in WAL frames)", which is false — the function has ZERO production callers
(only `page_test.go` references it) and the real WAL framing uses the custom paired-word algorithm, not CRC32. Separately
`walPageChecksum` (`wal.go:3432-3435`) also computes `crc32.ChecksumIEEE`, a distinct algorithm from the documented
`walChecksum`/`walFrameChecksum` recurrence in NOTES.md §7 (lines 344-359), and is itself undocumented. The consequence
is documentation/code confusion: a maintainer reading the `checksum` comment would wrongly believe CRC32 protects WAL
frames, and `walPageChecksum`'s separate CRC32 usage is invisible in the §7 checksum spec.

<a id="drift-126-pcache-truncate-and-clear-omit-page-1-zero-and-preserve-spec"></a>
### Drift: pcache Truncate And Clear Omit Page 1 Zero And Preserve Special Case
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pcache.go:*pcache.clear` (`internal/btree/pcache.go:571`),
  `pcache.go:*pcache.truncate` (`pcache.go:690-725`).

C's `sqlite3PcacheTruncate` (and `sqlite3PcacheClear`, which delegates to `Truncate(pCache, 0)`) carries a `pgno==0`
special case (`pcache.c:713-721`): when an outstanding reference to page 1 still exists (`pCache->nRefSum>0`), it does
NOT drop page 1 — instead it fetches it, zeroes its buffer in place (`memset(pPage1->pBuf, 0, szPage)`), and bumps
`pgno` to 1 so the final `xTruncate(pgno+1) == xTruncate(2)` RETAINS page 1 in cache with zeroed content. Go's
`clear()` (`pcache.go:571`) and `truncate()` (`pcache.go:690-725`) omit this page-1 zero-and-preserve branch for
referenced caches. The consequence is a behavioral divergence in the rare path where page 1 is still referenced during
a cache clear/truncate: SQLite keeps a live, zeroed page-1 header while Go does not, which could surface as a different
cache state for a still-pinned root page.

<a id="drift-127-pcache-recycle-and-spill-thresholds-off-by-one"></a>
### Drift: pcache Recycle And Spill Thresholds Off By One
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pcache.go:*pcache.create` (`internal/btree/pcache.go:321`),
  `pcache.go:*pcache.create` (`pcache.go:347`).

Go's merged `pcache.create()` diverges from SQLite on two cache-pressure thresholds, both shifting the trigger by one
page. For LRU recycle/buffer reuse, C's `pcache1FetchStage2` step 4 enters at `(pCache->nPage+1 >= pCache->nMax)`
(`pcache1.c:898-901`) and recycles AT MOST ONE page per create (net `nPage` unchanged), whereas Go uses
`for pc.nPage >= pc.maxPages && ...` (`pcache.go:321`) — both an off-by-one threshold (`nPage>=maxPages` vs
`nPage+1>=nMax`) and a loop instead of a single recycle. For dirty-page spill, C's `sqlite3PcacheFetchStress` gates on
strict `sqlite3PcachePagecount(pCache) > pCache->szSpill` (`pcache.c:453`) while Go gates the inline stress branch on
`pc.nPage >= spill` (`pcache.go:347`), firing the `xStress` spill callback one page earlier. The consequence is that
Go's cache begins recycling and spilling slightly sooner than SQLite, a subtle eviction-timing difference that could
affect cache occupancy and spill frequency under memory pressure.

<a id="drift-129-resetpage-zeroes-buffer-on-every-page-creation"></a>
### Drift: resetPage Zeroes Buffer On Every Page Creation
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pcache.go:*pcache.resetPage` (`pcache.go:408`).

SQLite's pcache layer never zeroes the page data buffer at fetch/recycle time: `pcacheFetchFinishWithInit`
(`pcache.c:501-520`) does `memset(&pPgHdr->pDirty, 0, sizeof(PgHdr)-offsetof(PgHdr,pDirty))`, clearing only the `PgHdr`
bookkeeping fields and leaving the page content buffer as-is (the buffer is overwritten by the subsequent read). Go's
`resetPage` (`pcache.go:408`) begins with `clear(p.data)`, unconditionally zeroing the full page content buffer on
every page-struct init — heap-alloc, `pFree` reuse, `initBulk`, and recycled-victim reuse alike. The consequence is an
extra full-buffer wipe on every page creation that SQLite does not perform; functionally safe but an added per-page
cost and a behavioral divergence (stale buffer contents are always cleared in Go) that NOTES.md does not record.

<a id="drift-130-newpcache-hash-table-pre-sized-to-capacity"></a>
### Drift: newPcache Hash Table Pre Sized To Capacity
- **Category:** changed-logic  -  **Severity:** low
- **Affected functions:** `pcache.go:newPcache` (`internal/btree/pcache.go:129`; hashSizeFor at `pcache.go:144-150`;
  minHashSize at `pcache.go:140`).

SQLite always seeds the pcache hash table to exactly 256 buckets — `pcache1Create` (`pcache1.c:789`) calls
`pcache1ResizeHash` once and grows the table on demand thereafter. Go's `newPcache` (`pcache.go:129`) instead pre-sizes
`apHash := make([]*page, hashSizeFor(maxPages))`, where `hashSizeFor` (`pcache.go:144-150`) returns the smallest power
of two `>= maxPages` and `>= minHashSize` (256, `pcache.go:140`). With the default `defaultCacheSize=5000` this
allocates 8192 buckets up front, and a larger configured cache allocates proportionally more. The consequence is a
larger eager hash-table allocation at cache creation than SQLite's fixed 256-bucket seed — a memory-vs-rehash tradeoff
divergence that NOTES.md does not document.
