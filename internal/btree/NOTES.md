# Known Limitations vs SQLite

This document lists known divergences from SQLite's C implementation that were
intentionally skipped or accepted. Organized by subsystem.

---

## WAL

### No Cache Spill (pagerStress)
**Review: 2.1, 2.4 | Severity: Critical (skipped)**

SQLite's `pagerStress()` writes dirty pages to the WAL mid-transaction when the
cache is full, then marks them clean for eviction. Our cache grows unbounded
during large transactions. A transaction modifying 100K 4KB pages consumes ~400MB
regardless of cache size setting.

Why skipped: Requires mid-transaction non-commit WAL frames, a pcache stress
callback, and pager sub-states (WRITER_LOCKED vs WRITER_CACHEMOD). High
complexity with risk of WAL corruption if implemented incorrectly. The current
unbounded growth is a memory issue, not a correctness issue.

### Collapsed Pager States
**Review: 2.5 | Severity: Important (skipped)**

We have 4 states (Open, Reader, Writer, Error) vs SQLite's 7. The missing
sub-states (WRITER_LOCKED, WRITER_CACHEMOD, WRITER_DBMOD, WRITER_FINISHED) are
only meaningful for cache spill gating (which we don't implement) and rollback
journal mode (which we don't use). In WAL mode, SQLite itself never enters
WRITER_DBMOD or WRITER_FINISHED.

### In-Memory Savepoint Page Copies
**Review: 2.6, 9.6 | Severity: Important (accepted)**

Savepoints store full page copies in heap memory (`map[uint32][]byte`). SQLite
uses a bitvec (1 bit/page) + disk-backed sub-journal. For 10K modified pages
with 2 savepoint levels, we use ~80MB heap vs SQLite's ~2KB bitvecs + disk I/O.

Acceptable for any-store's usage pattern: shallow nesting, small transactions.
Would become problematic for large transactions with multiple savepoint levels.

### Same-Process Reads Bypass SHM Hash Tables
**Review: 7.9 | Severity: Important (accepted)**

`walIndex.get()` uses an in-process Go map instead of reading from SHM hash
tables. The hash tables ARE correctly populated on every `set()` call — they
exist for cross-process readers. SQLite itself uses an in-memory `aSegment`
array (not hash lookups) for same-process reads. Our Go map serves the same
role with better performance (O(1) vs linear probing).

### Auto-Checkpoint Uses FULL Mode and Higher Threshold
**Review: 1.10 | Severity: Minor**

Default threshold is 10,000 frames vs SQLite's 1,000. Auto-checkpoint currently
runs as FULL (blocking writers). PASSIVE mode was implemented and is available
via `checkpointPassive()`, but auto-checkpoint in `tryCheckpoint()` has not yet
been wired to use it. A one-line change in pager.go would fix this.

### Checkpoint Copies All Frame Versions
**Review: 6.5 | Severity: Minor**

If a page was modified 10 times in the WAL, all 10 versions are copied during
checkpoint (last write wins). SQLite's `WalIterator` writes only the latest
version per page. Correct but wasteful — O(WAL frames) instead of O(unique pages).

### Checkpoint Per-Frame Buffer Allocation
**Review: 6.6, 1.11 | Severity: Minor**

`make([]byte, pageSize)` allocated per frame during checkpoint. Causes GC
pressure for large checkpoints. Should reuse a single buffer.

### WAL Header Version Not Validated
**Review: 1.12 | Severity: Minor**

The version field in the WAL header is read but never checked against a maximum
supported version.

### Page Size Not Validated During WAL Recovery
**Review: 1.13 | Severity: Minor**

Recovery trusts the WAL header's page size without bounds or power-of-2
validation. The database header validation (added in run1, issue 4.5) covers
the main open path, but a corrupted WAL header could cause issues during
recovery.

### In-Memory WAL Mode Skips Checksums
**Review: 1.15 | Severity: Minor (accepted)**

Intentional design choice for the `InProcess + NoSync` fast path. No disk
persistence means checksums add overhead without value.

---

## Pager / Cache

### Shared Page Cache Requires MVCC-Safe Reads
**Severity: Critical (fixed)**

SQLite C uses a separate page cache per connection and clears it entirely
(`pager_reset`) when a new read transaction detects the WAL has advanced
(`pagerBeginReadTransaction` → `sqlite3WalBeginReadTransaction(&changed)` →
`pager_reset` on changed). This ensures the cache is always consistent with the
current snapshot.

Our implementation uses a SINGLE shared page cache across all concurrent readers
and the writer. We cannot clear the cache per-transaction without breaking other
concurrent readers. This means `getPageAt`'s cache-hit check
(`getLatest(pgno) <= walMaxFrame`) is insufficient: it verifies the latest WAL
frame is within the caller's snapshot, but not that the cached data is from that
latest frame. A reader with an older snapshot can populate the cache with old WAL
data, and subsequent callers get stale data.

**Invariant**: Readers MUST NOT use the shared cache (`getPageAt`) for data
reads. Overflow pages use `readOverflowChainMVCC` (→ `readPageUncached`).
B-tree node pages use `readPageMVCC` (→ `readPageUncached`). Only the single
writer may use `getPageAt`, because its snapshot is always the latest and it
needs to see its own dirty pages via the cache.

### No mmap for Database File Reads
**Review: 2.9 | Severity: Minor**

All reads use `ReadAt` syscalls. SQLite supports `PRAGMA mmap_size` for
mmap-based reads on the database file. SHM mmap is correctly implemented.

### Missing Salt Cross-Check
**Review: 2.10 | Severity: Minor**

No validation that the WAL file's salt matches the database header's salt.
Would detect stale or mismatched WAL files.

### Missing VersionValidFor Usage
**Review: 2.11 | Severity: Minor**

The `VersionValidFor` field is stored in the database header but never used for
integrity validation. SQLite uses it to detect when the version-valid-for
counter doesn't match the change counter.

### Auto-Checkpoint Errors Silently Swallowed
**Review: 6.7 | Severity: Minor**

`tryCheckpoint()` errors are discarded. Acceptable for PASSIVE-like semantics
but inconsistent with FULL checkpoint behavior.

---

## B-tree Operations

### No Full 3-Sibling Redistribution on Split
**Review: 3.6 | Severity: Important (partially addressed)**

SQLite's `balance_nonroot()` collects cells from up to 3 siblings and
redistributes them targeting ~67% fill across all siblings. Our implementation
uses a 2-way split targeting 2/3 fill on the left page. This captures most of
the benefit (better fill factor, fewer splits) without the complexity of
multi-sibling redistribution, page number reassignment, and pointer-map updates.

### No Full Freeblock Chain
**Review: 3.8 | Severity: Important (partially addressed)**

SQLite maintains a sorted linked list of free blocks within each page for
fine-grained space reuse. We implemented in-place update (when new cell <=
old cell size), in-place delete (with fragmentation tracking), and
defragmentation-before-split. The full freeblock chain was skipped due to high
complexity and corruption risk. Our approach tracks fragmentation in `fragBytes`
and triggers a full rebuild when it exceeds 60 bytes.

### Custom Cell Format
**Review: 3.11 | Severity: Important (intentional)**

Uses `varint(keyLen) | key | varint(valLen) | value` instead of SQLite's
single-varint payload format. This is intentional — our B-tree is a key-value
store, not a SQL table. The format separates key and value explicitly, which
simplifies overflow handling (only values overflow, keys are validated to fit
on-page). Not compatible with SQLite tools by design.

### Path Tracking Stores Only Page Numbers
**Review: 3.12 | Severity: Minor**

The cursor path stores only page numbers, requiring re-fetching pages and
re-scanning for insertion points on splits. SQLite caches page pointers + cell
indices in the cursor stack (`apPage[]`/`aiIdx[]`). Our approach adds some
overhead on splits but keeps cursor memory usage lower.

### Keys Cannot Overflow
**Review: 4.2, 8.1, 8.5 | Severity: Critical/Important (mitigated)**

SQLite's interior index cells support overflow for large keys. Our interior
cells carry full keys with no overflow. Instead of implementing overflow for
interior cells, we enforce a key size limit at the `Put()` entry point:
`maxLocalPayload(usableSize)` (~999 bytes for 4KB pages). Keys exceeding this
return `ErrKeyTooLarge`. This prevents any key that couldn't fit in an interior
cell from entering the tree.

### No "Nearby" Allocation Hint for Overflow Pages
**Review: 8.8 | Severity: Minor**

SQLite passes the previous overflow page number as a locality hint to
`allocateBtreePage()`. Our pages come from wherever the freelist yields them,
potentially scattering overflow chains across the file.

---

## Page Format

### Custom Magic String
**Review: 4.7 | Severity: Minor (intentional)**

Uses `"BTree format 1\000"` instead of `"SQLite format 3\000"`. Intentional to
prevent confusion with SQLite files and to signal incompatibility.

### CRC32-IEEE Instead of Cumulative Checksum
**Review: 4.8 | Severity: Minor (intentional)**

Database pages use CRC32-IEEE for integrity checking. SQLite uses a cumulative
checksum for WAL frames. CRC32-IEEE is a standard algorithm but weaker against
certain multi-bit corruption patterns compared to SQLite's paired-word
cumulative checksum.

### Single Overflow Threshold for Leaf Cells
**Review: 4.3 | Severity: Important (fixed with distinction)**

Fixed in run1 by adding separate `maxLeafPayload` (U-35, ~4061 bytes) and
`maxIndexPayload` ((U-12)*64/255-23, ~1001 bytes) functions. Leaf cells use the
generous table-leaf formula; interior cells use the conservative index formula.
However, our leaf cells still use a combined key+value total for the overflow
decision, unlike SQLite where table cells use rowid+payload and index cells use
the full key as payload.

---

## Freelist

### Freelist Formula Ignores Reserved Space
**Review: 5.7 | Severity: Minor**

Uses `(pageSize - 8) / 4` instead of `(usableSize - 8) / 4` for max leaves
per trunk. Correct while `ReservedSpace == 0`. Would cause corruption if
reserved space were ever enabled.

### No BTALLOC_EXACT / BTALLOC_LE Modes
**Review: 5.8 | Severity: Minor**

Only `BTALLOC_ANY` allocation mode. `BTALLOC_EXACT` and `BTALLOC_LE` are only
needed for auto-vacuum (which we don't implement) and for locality hints.

### Reserved Space Not Used in Overflow Computations
**Review: 8.9 | Severity: Minor**

Overflow page usable size uses raw `pageSize - 4` instead of `usableSize - 4`.
Dormant while `ReservedSpace == 0`. Would cause corruption if reserved space
were enabled.

---

## Multi-Process / Concurrency

### No Busy Handler Wired by Default
**Review: 1.7 | Severity: Important (implemented, not wired)**

`BusyHandler` type and `DefaultBusyTimeout()` are implemented in wal.go, but
the pager/db layer does not yet set `wal.busyHandler`. Lock failures still
return `ErrBusy` immediately unless the caller explicitly configures a handler.

### Auto-Checkpoint Not Yet PASSIVE
**Review: 6.2 | Severity: Important (implemented, not wired)**

`checkpointPassive()` is available but `tryCheckpoint()` in pager.go still
calls the FULL-mode `checkpoint()`. Switching is a one-line change.

---

## Not Implemented (by design)

These SQLite features are intentionally absent because they are not needed for
any-store's use case as a single-process embedded key-value store:

- **Rollback journal mode** — WAL-only
- **Auto-vacuum / incremental vacuum** — no PTRMAP pages
- **Shared cache mode** — single connection per database
- **SQL layer** — no schema, no rowid tables, no SQL parsing
- **Integer-key (table) B-trees** — index B-trees only
- **Multi-database transactions** — single database per connection
- **WAL2 mode** — standard WAL only
- **Database file locking (RESERVED/PENDING/EXCLUSIVE)** — WAL mode uses SHM locks
