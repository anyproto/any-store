# Option B research: paging vectors through the btree (disk-resident HNSW)

Branch `btree-vector-search`. Follows from
[COMPARISON_pgvector.md](./COMPARISON_pgvector.md), which framed the central fork:

- **Option A** (the [README](./README.md) spike): the whole graph + vectors live
  in a contiguous RAM arena; persisted to the btree only for durability.
- **Option B**: keep the graph on disk and **traverse it by paging records
  through the btree's page cache** (`pcache`) — pgvector's residency model — so
  RAM scales with the *graph*, not the *vectors*.

This note prototypes and **measures** Option B instead of theorising. Prototype:
`paged.go` (`PagedHNSW`); measurements: `paged_test.go`.

```
go test ./vector -run TestPagedMatchesMemory -v      # correctness parity with Option A
go test ./vector -run TestPagedVsMemory -v           # latency / RAM / gets-per-query (warm)
go test ./vector -run TestPagedCachePressure -v      # cache < data (the regime B is for)
```

## What was built

`PagedHNSW` keeps only the **graph topology** in RAM (adjacency arena, levels,
entry point, tombstones — ~`M0*4` ≈ 128 B/node) and stores each node's vector in
a `<name>:vec` btree namespace. During search every visited node's vector is read
from the btree (`ReadTx.AppendValue`) into a reusable, zero-copy `[]float32`
scratch (so the measurement reflects the *paging* cost, not a codec). A read
transaction is opened per query (the realistic unit) and the number of btree
lookups is counted. It reuses Option A's exact HNSW traversal, so results are
**byte-identical** to the in-memory index (verified by `TestPagedMatchesMemory`).

## The cost model (why it matters)

A node lookup is a btree **descent** — root → interior → leaf — through the LRU
page cache (4 KiB pages, default 5000-page cache). Two facts make vectors
hostile to this:

- **HNSW is random-access and visits a lot of nodes.** At `efSearch=64` on 20 000
  nodes, a query touches **~1900 nodes** — and Option B turns each into a btree
  point lookup.
- **Vectors are large relative to a page.** A 512 B record (dim 128) packs ~7 per
  4 KiB leaf page; at dim 1536 a record (~6 KiB) exceeds a page and spills to an
  **overflow chain**, so one lookup becomes several page reads. (This is why
  co-locating vector+adjacency in one record is right for *paged reads* even
  though splitting them is right for *writes* — a real tension.)

## Measured — warm cache (in-memory db, no I/O), 20 000 × 128d

| variant | latency | RAM | btree gets/query |
|---------|--------:|-----|-----------------:|
| **A** in-memory arena | **45 541 ns** | 13.9 MiB (full) | 0 |
| **B** paged vectors | 483 501 ns | **3.9 MiB** (topology only) | 1897 |
| **B′** hybrid: route in RAM + page rerank | 67 838 ns | routing-slab + topology | **64** |

- **Pure paging (B) is ~10.6× slower even with zero I/O** — purely the btree
  descent + per-hop overhead over ~1900 lookups. It holds **72% less RAM** (and
  the saving grows with dimension: ~95% at dim 1536, where the vector dwarfs the
  128 B of topology). This is the floor; real disk makes it worse (below).
- **The hybrid (B′) is only 1.5× slower** because it routes the graph against an
  in-RAM vector copy (zero btree reads) and pages **only the ef rerank
  candidates** — 64 gets/query instead of 1897.

## Measured — cache pressure (file-backed), 60 000 × 128d = 29.3 MiB vectors

| page cache | latency | gets/query |
|-----------:|--------:|-----------:|
| 8 MiB (≪ data) | 1 509 631 ns | 2145 |
| 31 MiB (≈ data) | 793 112 ns | 2145 |
| 156 MiB (≫ data) | 773 970 ns | 2145 |

When the cache can't hold the vectors, real reads roughly **double** latency
(1.5 ms vs 0.77 ms). Once the working set fits, latency flattens — i.e. Option B's
performance is **page-cache-bound**, exactly as pgvector's docs ("keep vectors in
`shared_buffers`") and Lucene's mmap model (see
[COMPARISON_mongodb.md](./COMPARISON_mongodb.md)) describe.

## Conclusions

1. **Pure Option B is not the answer for the hot path.** ~1900 btree descents per
   query cost ~10× even warm, ~20× when the cache is smaller than the data. It is
   only worth it when vectors genuinely cannot fit in RAM *and* the latency is
   acceptable (batch / cold-ish workloads).
2. **The hybrid is the answer, and it's the same answer everyone else reached.**
   Keep a **compact routing copy in RAM** and **page full vectors only to rerank**
   the final candidates. Measured at 1.5× in-memory with 64 gets/query. This is
   DiskANN's design and pgvector/Lucene's quantize-then-rescore.
3. **Quantization is the missing piece that makes the hybrid's RAM win real.** In
   B′ above the routing slab is still full float32 (no RAM saving on vectors). Swap
   it for an **int8 (¼) or binary (1/32) quantized** slab — the
   [MongoDB finding](./COMPARISON_mongodb.md) — and the hybrid becomes:
   *routing-slab (≈25% / ≈3% of the vectors) + topology in RAM, full vectors on
   disk for rerank, ~1.5× latency.* That is the production sweet spot and it
   **unifies all three prior findings**: the arena (A) for routing, paging (B) for
   the cold full vectors, quantization for the routing slab.

## Recommended target design

```
in RAM:   topology arena  (~128 B/node)
          quantized routing vectors  (int8 ≈ dim B/node, or binary ≈ dim/8 B/node)
on btree: full-fidelity vectors  (<name>:vec, paged for rerank only)
          + the persisted graph records (durability / reload)
search:   greedy-descent + layer-0 search on the in-RAM quantized vectors
          → page the ef best candidates' full vectors → exact rerank → top-k
```

This keeps query latency near Option A (1.5× measured, before quantization
shrinks the working set further), cuts vector RAM by 4–32×, scales the full
vectors past RAM via the btree page cache, and reuses the persistence layer
already built. The next concrete step is the **int8 quantized routing slab +
exact rerank**, which turns B′ from "same RAM as A" into "¼ the RAM of A at ~1.5×
the latency".

## Prototype limitations

- `PagedHNSW` reuses an Option-A `FlatHNSW` for topology and reads host-endian
  float32 bytes (a real build needs a portable on-disk encode and to load topology
  straight from the btree on open, not from a RAM-built graph).
- One read tx per query; a server could pool readers / pin the entry-point and
  upper layers. Single-threaded scratch (one query at a time per `PagedHNSW`).
- The hybrid (`SearchHybrid`) routes on full float32 (it measures the *latency/Get*
  profile, not the RAM win); the RAM win needs the quantized slab, not yet built.
