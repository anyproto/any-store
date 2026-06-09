# Comparison: this spike vs pgvector's HNSW

Branch `btree-vector-search`. Companion to [README.md](./README.md) and
[DELETE_UPDATE.md](./DELETE_UPDATE.md).

pgvector is the most relevant point of comparison because the relationship
**pgvector → Postgres** is the same as **this index → any-store**: a vector
index riding on a general-purpose, page-based, WAL'd storage engine. So the
comparison is less "who's faster" and more "two opposite answers to the same
question: *where does the graph live at query time?*"

pgvector facts below are sourced from `src/hnsw.h`, `hnswbuild.c`,
`hnswvacuum.c`, the README, and the engineering write-ups linked at the bottom.

---

## The one fundamental difference

| | **This spike** | **pgvector** |
|--|----------------|--------------|
| Where the graph lives at query time | **Fully in RAM** — one contiguous arena; a hop is an array index + pointer-free distance | **On disk**, demand-paged through Postgres' shared-buffer cache; **every hop is a buffer-manager page access** |
| The btree / Postgres role | Durability only — graph is *persisted* to btree namespaces and *reloaded into RAM* on open | The index *is* the on-disk pages — Postgres buffer manager, WAL, MVCC, VACUUM all apply directly |

Everything else flows from that. pgvector scales past RAM and is crash-safe per
transaction, but pays page-fault + lock overhead per hop and MVCC costs on
write. This spike has pointer-speed traversal and no VACUUM machinery, but is
RAM-bound and rebuilds its working set on open.

Crucially: **any-store already has the primitives pgvector relies on** — a page
cache (`pcache`), MVCC read snapshots, and a WAL. So pgvector's disk-resident
design is *available* to any-store; this spike simply chose the in-memory point
first. That choice is the main thing to revisit (see "The real fork" below).

---

## Side by side

| Dimension | This spike (`FlatHNSW`/`BtreeHNSW`) | pgvector HNSW |
|-----------|-------------------------------------|---------------|
| Graph residency | In-memory arena (SoA) | Disk pages, demand-paged via buffer cache |
| Vector storage | Contiguous `[]float32` slab, dense `uint32` id | Inline in each `HnswElementTuple` |
| Adjacency storage | Flat `[]uint32` arena, block per node | Separate `HnswNeighborTuple` (`(level+2)*m` slots, all layers concatenated), kept on the **same page** as the element when possible |
| Entry point | `entryID`/`topLayer` fields (+ meta record) | `entryBlkno`/`entryOffno`/`entryLevel` on the metapage |
| ID model | dense `uint32` arena id; `IDDict` maps doc `[]byte` → `uint32` label | heap `ItemPointer` (block/offset); element holds up to 10 heap TIDs |
| Distance / SIMD | `vek` AVX2/AVX-512, pure-Go asm, **no CGO** (~20 ns @768d) | compiler auto-vectorization + `target_clones` runtime dispatch (AVX/AVX-512/FMA); explicit AVX-512 FP16 for `halfvec` |
| Defaults | M=16, M0=32, efConstruction=200, efSearch=20–64 | m=16, ef_construction=64, ef_search=40 |
| Build | In-RAM only; 20k×128d in **2.9 s**; alloc-light | In-RAM **if it fits `maintenance_work_mem`**, else one-by-one **on-disk build ~10–50× slower**; parallel workers |
| Delete | Tombstone bit, O(1); node kept as waypoint | MVCC dead tuple; filtered at scan |
| Delete repair | Cheap `Compact` (drop dead edges) or full `Rebuild` | 3-pass `VACUUM` (`RepairGraph` recomputes neighbor lists in place) + entry-point promotion |
| Update | delete+reinsert (`Update`) | MVCC new tuple + graph re-insert |
| Space reclaim | `Compact`/`Rebuild` shrink the arena | VACUUM marks slots **reusable**, does **not** shrink the index file; `REINDEX` for a clean graph |
| Durability | btree WAL on `Flush`; **graph reconstructed into RAM on open** | Full WAL logging → crash-safe, **replication**, PITR |
| Max dimensions | unbounded (tested to 1536) | **2000** (`vector`), 4000 (`halfvec`) for an HNSW index |
| Filtering | none yet (returns ids; caller filters) | SQL `WHERE` + **iterative scans** (0.8.0) to avoid under-filling under filters/LIMIT |
| Concurrency | `RWMutex`: concurrent search, single writer | per-page buffer locks + shared `HNSW_UPDATE_LOCK` (write-write concurrency), `HNSW_SCAN_LOCK` |
| Deployment | embedded pure-Go library, no CGO, no server | Postgres extension (C), needs a Postgres server |

---

## Where pgvector is ahead

- **Scales beyond RAM.** The graph is paged from disk, so a 100 GB index works on
  a box with 16 GB RAM (slowly if cold, but it works). This spike must hold the
  whole graph in RAM and **rebuilds the in-memory arena on every open** — fine
  for millions of vectors, a non-starter for sets that exceed RAM.
- **Transactional durability for free.** A committed insert is WAL'd, crash-safe,
  and replicated, atomically with the row it indexes. Here, durability only
  reaches disk on `Flush`, the index is a separate structure from the data, and
  crash mid-session loses un-flushed mutations.
- **Maturity:** SQL filtering with iterative scans, parallel builds, halfvec/bit/
  sparsevec, years of production hardening, point-in-time recovery, replicas.
- **In-place graph repair on delete** (VACUUM pass 2) keeps recall without a full
  rebuild — more sophisticated than this spike's cheap `Compact` (which thins
  edges and loses recall; we fall back to `Rebuild`).

## Where this spike is ahead

- **Query latency.** In-memory pointer-free traversal over a contiguous arena
  (~44 µs for 20k×128d, ~1 alloc/query) avoids pgvector's per-hop buffer-manager
  lookup + page lock, and never touches disk. pgvector's own operational advice
  is "keep the vector pages in `shared_buffers`" precisely because a cold hop is
  a real I/O — i.e. its fast path is trying to *become* in-memory.
- **Build speed when it fits in RAM**, with far fewer allocations, and **no
  `maintenance_work_mem` cliff** (pgvector falls off a 10–50× slowdown the moment
  the graph exceeds the budget; we have no on-disk build path to be slow).
- **No MVCC tax on writes.** pgvector's issue #875 — an UPDATE to a *non-vector*
  column still re-inserts into the graph because MVCC makes a new tuple — simply
  doesn't exist here; our `Update` only touches the graph when the vector
  changes.
- **No dimension ceiling**, no Postgres dependency, embedded, pure-Go, no CGO.
- **Compact/Rebuild physically shrink memory**; pgvector's VACUUM only marks
  slots reusable and never shrinks the index file (REINDEX required).

---

## Where we independently converged

- **Tombstone-style delete + filter, then repair/rebuild.** pgvector marks dead
  tuples and filters at scan, then repairs via VACUUM; we tombstone and filter,
  then Compact/Rebuild. Same shape (matches the whole-industry pattern).
- **Vector and adjacency are stored separately.** pgvector splits the element
  tuple (vector inline) from the neighbor tuple (adjacency), preferring to keep
  them on the same page. That is exactly the [DELETE_UPDATE.md §4](./DELETE_UPDATE.md)
  recommendation — split the churning adjacency from the immutable vector — which
  we measured would cut write volume ~4×. pgvector validates it; we should adopt
  it on the btree side.
- **Entry point persisted in a small head record**; M=16 default; same K-NN layer
  search.
- **Build wants the graph in RAM.** pgvector's whole `maintenance_work_mem` story
  is "build in memory or suffer"; our build *is* in memory. Agreement that
  HNSW construction is an in-RAM operation.

---

## The real fork for any-store

Because any-store is a btree DB with a page cache + WAL + MVCC snapshots, it can
go either way — and pgvector is the proof the disk-resident way works:

- **Option A — in-memory arena (this spike).** Load the whole graph into RAM,
  persist to btree, search in RAM. Fastest queries, simplest, but RAM-bound and
  pays a reconstruct-on-open cost.
- **Option B — pgvector-style disk-resident.** Store element + neighbor records
  in btree namespaces and **traverse the graph by paging those records through
  the btree's `pcache`** (the same trick pgvector plays with `shared_buffers`).
  Scales past RAM, durable incrementally (no reload, no big in-RAM working set),
  crash-safe per `Flush` — at the cost of a btree lookup per hop and dependence
  on a warm page cache.

This spike deliberately built Option A to get the fast path and the measurements
first. The persistence layer (`BtreeHNSW`) is already record-per-node in btree
namespaces, so it's **one step from Option B**: traverse straight off the
cursor/page cache instead of bulk-loading into the arena. A likely sweet spot is
a hybrid — hot upper layers + entry point pinned in RAM, cold layer-0 pages
demand-paged — which is roughly what pgvector approximates via buffer-cache LRU.

---

## Verdict

pgvector is the right choice when you already run Postgres and want durable,
replicated, SQL-filtered vector search that scales past RAM, and can accept
buffer-cache-paged latency and VACUUM/REINDEX maintenance. This spike is the
right shape when you want **embedded, pure-Go, in-memory-speed** ANN co-located
with an any-store dataset, and the working set fits in RAM. The most valuable
takeaway from pgvector for any-store is twofold: **(1) adopt the vector/adjacency
storage split** (pgvector already does; we measured the ~4× write-amp win), and
**(2) seriously evaluate Option B** — traversing the graph through the btree
page cache — as the path to scaling beyond RAM, since any-store already has every
primitive it requires.

## Sources

pgvector internals (storage, search model, VACUUM, limits) verified against:
`src/hnsw.h`, `hnswbuild.c`, `hnswvacuum.c`, the pgvector README, DeepWiki's
pgvector pages, kernelmaker "Exploring the Internals of pgvector", Lantern's
storage write-up, and issues #875 (slow non-vector UPDATE), #763/#244 (recall
decay over churn), #807 (slow large build), #721 (iterative scans), #461 (dim
limit). Full URLs in the research notes for this branch.
