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
| 72 | 20 | Reserved (zero) | Reserved (zero) |
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

**Classification: Intentional** -- The magic string is intentionally different to
prevent SQLite from opening Go databases and vice versa. The auto-vacuum fields are
unused because auto-vacuum is not implemented.

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
| SHM format | mmap'd file, native byte order | mmap'd file (linux/amd64) or heap (other platforms) |
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
| First-opener `ftruncate(shm, 3)` marker | **Missing** -- see §SHM open/close protocol below |
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
| Multi-process support | Full (via file locks + SHM) | Limited (mmap on linux/amd64, heap elsewhere) |
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
- **Immediate eviction on unpin**: when cache is overfull (`len(pages) > maxPages`),
  clean pages are discarded on `release()` instead of entering LRU. Matches
  SQLite `pcache1Unpin` (`pcache1.c:1094`): `pGroup->nPurgeable > pGroup->nMaxPage`
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
| Slab allocator | Contiguous `void*` buffer, pointer arithmetic (`pcache1.c:283-288`) | `[][]byte` slice, Go-idiomatic (drift #7) |
| Slab buffer return | Range check `SQLITE_WITHIN` (`pcache1.c:381`) | Caps free list at `nSlab`; overflow buffers are GC'd (drift #8) |
| Slab init | Library init `pcache1Init` (`pcache1.c:695-741`) | Lazy init on first `Open()` or explicit `ConfigPageCache()` (drift #9) |
| Bulk alloc | Contiguous `pBulk` carved into slots (`pcache1.c:312-327`) | Individual page structs with slab buffers (drift #10) |
| Page flags | Bitmask on each page | Separate maps (`dontWritePages`, `hasContent`) |
| Cache ownership | Per-connection (private) | Per-connection (private) — matches SQLite |
| Thread safety | Per-connection (no mutex needed) | Per-connection (no mutex needed) — matches SQLite |
| PGroup cross-cache stealing | Enabled in single-thread mode (`pcache1.c:718-719`) | No PGroup; each cache isolated (drift #1) |
| Hash table | `apHash[]` with chaining (`pcache1.c:199-200`) | Go `map[uint32]*page` (drift #2) |
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
- Max leaves per trunk: `(usableSize - 8) / 4`
- Pages freed via `freePage2()` which handles auto-vacuum pointer-map updates
- `btreeSetHasContent()` / `btreeGetHasContent()` with `BtShared.pHasContent` bitvec
- `PGHDR_DONT_WRITE` flag on freed leaf pages

### Go

The Go implementation uses the same trunk/leaf format (pager.go):
- Same layout: `[4B next_trunk] [4B leaf_count] [4B leaf_pgno * N]`
- Same max leaves formula: `(usableSize - 8) / 4`
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

**Classification: Intentional** -- The format is identical. Implementation details differ
(map vs bitvec) but behavior matches. The auto-vacuum pointer-map integration is absent
because auto-vacuum is not implemented.

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
- No partial read support (always reads from beginning)

### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| Overflow page cache | `aOverflow[]` on cursor | None |
| Partial reads | Offset-based reads supported | Always reads from start |
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
- `savepoint()`: Captures `dbSize`, `walFrame`, `header` snapshot, creates empty page map
- `getWritablePage()`: Lazily copies page data before modification (copy-on-write)
- `rollbackToSavepoint()`: Restores pages from newest to oldest savepoint
- `releaseSavepoint()`: Merges page copies to parent savepoint
- All page copies stored **in memory** (no sub-journal file)
- `hasContent` map prevents NOCONTENT optimization for freed-then-reallocated pages

### Drift

| Aspect | SQLite | Go |
|--------|--------|-----|
| Storage | On-disk sub-journal | In-memory page copies |
| WAL integration | `sqlite3WalSavepoint()` captures 4 u32 values | Captures `walFrame` count |
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
`readOverflowChainInternal()` for bounds-checking overflow page numbers. Without
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
reader goroutines read it in `readOverflowChainInternal()` for bounds checking.
This is a genuine concurrent access to a shared field.

Go's memory model (defined by the Go specification, not C11/POSIX) requires that
concurrent access to a shared variable be synchronized via `sync/atomic` operations,
`sync.Mutex`, or channels. A plain `uint32` read/write from different goroutines is
a data race -- even if "logically" only one goroutine writes at any given time --
unless the Go race detector can see a happens-before relationship.

The options were:
- **Mutex on every read** -- too expensive; readers would contend with the writer
  on every overflow bounds check
- **Per-reader `dbSize` snapshot** (matching SQLite's per-connection model) -- would
  require threading a snapshot value through every reader call path
- **`atomic.Uint32`** -- minimal overhead (~1 ns per Load on x86), zero contention,
  zero API changes

We chose `atomic.Uint32` as the simplest correct solution. The writer uses
`dbSize.Add(1)` in `allocatePage()` and `dbSize.Store()` for rollback/init paths.
Readers use `dbSize.Load()` for bounds checking. This is safe under Go's memory
model and introduces no contention.

### Drift

| Aspect | SQLite C | Go |
|--------|----------|-----|
| `dbSize` ownership | Per-connection (`Pager.dbSize`) -- no sharing | Single shared `pager.dbSize` |
| Writer/reader isolation | Separate `Pager` instances per thread | Shared struct, goroutine concurrency |
| Synchronization | None needed (no sharing) or `pBt->mutex` | `atomic.Uint32` (Load/Store/Add) |
| WAL snapshot | `pWal->hdr.nPage` local copy at read-lock time | `walMaxFrame` per reader, atomic `dbSize` for bounds |
| Performance cost | Zero (no sharing = no synchronization) | ~1 ns per `atomic.Load` on x86 |

**Classification: Divergent** -- This drift stems from a fundamental architectural
difference: SQLite uses per-connection state isolation (each connection has its own
`Pager`), while the Go implementation shares a single `pager` across goroutines. The
Go memory model mandates explicit synchronization for any cross-goroutine field access,
even for benign races that C compilers and POSIX threads would handle correctly via
hardware cache coherence. The `atomic.Uint32` is the minimal correct fix; an
alternative would be to refactor toward per-reader `dbSize` snapshots (matching
SQLite's architecture), but that would be a much larger change for the same correctness
guarantee.

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

4. **Deferred SHM hash writes** (`wal.go:setBatch`): SQLite writes SHM hash entries
   immediately in `walFrames()` via `walIndexAppend()`, then cleans them up with
   `walCleanupHash()` on rollback. We defer SHM writes for spill frames into
   `pendingShmFrames` and flush on commit, avoiding the need for post-rollback
   cleanup of cross-process-visible state.

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

**Checkpoint Copies All Frame Versions** -- Severity: Minor

If a page was modified 10 times in the WAL, all 10 versions are copied during
checkpoint (last write wins). SQLite's `WalIterator` writes only the latest
version per page. Correct but wasteful -- O(WAL frames) instead of O(unique pages).

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

### Pager / Cache

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

**Known Drifts in Page Cache:**
- Buffer reuse on eviction: SQLite step 4 (`pcache1.c:897-914`) reuses the
  evicted victim's buffer in-place. Writer caches drop evicted buffers because
  evicted page structs may still be referenced elsewhere after spill; those
  buffers are returned to slab in `clear()`/`discard()`/`truncate()`. Reader
  caches (no `xStress`) return evicted buffers to the slab immediately in
  `create()`.
- No `reuseUnlikely` on unpin: SQLite's `pcache1Unpin` accepts a
  `reuseUnlikely` flag (`pcache1.c:1079`); when true, pages are immediately
  freed. Our `release()` does not have this hint. Overfull eviction
  (`len(pages) > maxPages`) matches SQLite's `pGroup->nPurgeable > nMaxPage`
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

**No Full 3-Sibling Redistribution on Split** -- Severity: Important (partially addressed)

SQLite's `balance_nonroot()` collects cells from up to 3 siblings and
redistributes them targeting ~67% fill across all siblings. Our implementation
uses a 2-way split targeting 2/3 fill on the left page. This captures most of
the benefit without the complexity of multi-sibling redistribution.

**No Full Freeblock Chain** -- Severity: Important (partially addressed)

SQLite maintains a sorted linked list of free blocks within each page for
fine-grained space reuse. We implemented in-place update (when new cell <=
old cell size), in-place delete (with fragmentation tracking), and
defragmentation-before-split. Our approach tracks fragmentation in `fragBytes`
and triggers a full rebuild when it exceeds 60 bytes.

**Path Tracking Stores Only Page Numbers** -- Severity: Minor

The cursor path stores only page numbers, requiring re-fetching pages and
re-scanning for insertion points on splits. SQLite caches page pointers + cell
indices in the cursor stack (`apPage[]`/`aiIdx[]`).

**No "Nearby" Allocation Hint for Overflow Pages** -- Severity: Minor

SQLite passes the previous overflow page number as a locality hint to
`allocateBtreePage()`. Our pages come from wherever the freelist yields them,
potentially scattering overflow chains across the file.

### Freelist

**Freelist Formula Respects Reserved Space** -- Resolved (stale drift note)

`freelistMaxLeaves()` uses `(p.usableSize() - 8) / 4` where `usableSize`
is `pageSize - ReservedSpace`. Correct regardless of ReservedSpace
value. See `pager.go:868-870`.

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
   compares the saved SHM header snapshot (from `saveReadSnapshot` during
   `pager.beginWrite`) against the current SHM header. If they differ, another
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

2. **Separate readSnapshot vs checksum state**: SQLite uses a unified `pWal->hdr`
   for both the snapshot comparison in `beginWriteTransaction` and the checksum
   chaining in `walEncodeFrame`. We use `wal.readSnapshot` for the snapshot and
   `wal.cksum1/cksum2` for chaining because our checksum fields are separate.

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

5. **readSnapshot saved in pager.beginWrite**: SQLite saves `pWal->hdr` in
   `walTryBeginRead` (called from any connection). We save `readSnapshot` in
   `pager.beginWrite` (writer-only context) to avoid data races with concurrent
   reader goroutines calling `tryBeginRead`.

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
