# any-store vector engine — design, approaches & benchmarks

## TL;DR (short description)

any-store is an **embedded, btree-resident document store** (MVCC, WAL, crash-safe,
multi-process-safe) with **built-in approximate-nearest-neighbour (ANN) vector
search**. Embeddings live in ordinary documents and are queried through the normal
`Find()` pipeline — a vector clause selects candidates, ordinary filters/sorts apply
on top, and each result carries its `_distance`. There is no separate vector server
or in-memory index to manage: the index **lives in the same btree as the data**, so
it inherits the store's durability, snapshot isolation, and cross-process semantics.

Two index families, same btree substrate:

- **HNSW** (graph) — `VectorModeBTree` / `VectorModeHybrid` (+ optional RAM vector
  tier) / `VectorModeBruteForce`. Best **search latency/throughput**.
- **IVF** (inverted file) — partition into coarse cells, probe a few:
  - `VectorModeIVFSQ` — int8 vectors per cell, scanned directly. **Best write
    throughput** (~6–9× HNSW insert), smallest index, lowest RAM of the IVF family.
  - `VectorModeIVFPQ` — product-quantized codes + int8 re-rank store. Same niche,
    but **IVF-SQ dominates it at this scale** (simpler, faster, smaller).

Best **search** = HNSW (esp. `hybrid + HybridCacheVectors`); best **writes** = IVF-SQ.
Both HNSW and IVF support `int8` (~4–4.5× smaller) at ~0.5 pt recall cost.

---

## How it lands on the btree

Everything is keyed bytes in namespaces inside one MVCC btree. No separate files, no
shadow in-memory structure that can diverge from disk.

### Shared substrate
- **Namespaces** per index, keyed by a big-endian `uint32` *label* (a dense internal
  id). Document id ↔ label is itself a btree map, so deletes/replaces allocate fresh
  labels and the id space stays dense.
- **MVCC copy-on-write + WAL.** Index writes ride the same write transaction as the
  document write — an insert and its indexing commit atomically. Readers see a
  consistent snapshot; a crash leaves either the whole change or none.
- **Multi-process.** Because the index is just btree pages, a second process opening
  the file sees the committed index. After a peer commits index DDL or a compaction
  (which moves the root), the reader **reconciles from the on-disk infos** rather than
  trusting a cached root — verified by cross-process consistency tests (build in a
  subprocess, query/compact in another).

### HNSW layout
- The graph is serialized into the btree: per-node **adjacency** (`:adj`), stored
  **vectors** (`:vec`, f32 or int8), id/label maps (`:lbl`), and meta (`:meta`).
- **`VectorModeHybrid`** adds a RAM **layer-0 mirror** of the graph *adjacency* — a
  pointer-free **CSR base + incremental overlay + dirty ring** (no Go maps, no GC
  scan). A write patches O(changed) labels; large bursts / cross-process gaps fall
  back to a full rebuild; a thrash valve bypasses the mirror under sustained churn.
  RAM ≈ a few MiB, independent of embedding dimension.
- **`HybridCacheVectors`** adds a RAM **vector tier**: layer-0 vector reads come from a
  RAM slab instead of the btree, making each hop pure-RAM. This is the search win
  (sub-0.3 ms p50), at the cost of ≈ the stored vector size in RAM.

### IVF layout (shared)
- A **coarse quantizer** (k-means centroids, `NList` cells) partitions the space;
  each vector is assigned to its cell(s).
- **Inverted lists** are contiguous btree key ranges (cell-major), so a probe is a
  handful of **sequential range scans** — the btree-friendly access pattern.
- **Closure (SPANN-style multi-assignment):** each vector is placed in its `Closure`
  nearest cells so boundary vectors are found at lower `NProbe`.
- Codebooks are held as **flat arenas** (one slice + offset accessors) — fewer slice
  headers, better locality on the encode/assign (insert) path.
- Search = probe `NProbe` cells → scan candidates → exact-ish top-`ef`; results stream
  back distance-ordered (the planner then skips the sort).

**`VectorModeIVFSQ` (scalar quantization — recommended IVF):** each cell stores the
**int8 full vectors**, scanned directly. No PQ codebook to train, no encode step, no
separate re-rank store — so build/insert are fast, the index is the smallest of any
mode, and RAM is the lowest of the IVF family.

**`VectorModeIVFPQ` (product quantization):** stores **PQ codes** (M subquantizers)
plus an **int8 re-rank store** for exact-ish re-ranking, with **IVFADC** precomputed
distance tables for the candidate scan. More moving parts; at 768-dim / tens of
thousands of vectors **IVF-SQ dominates it** on build, insert, size and RAM at equal
recall — PQ pays off mainly at much larger N / tighter byte budgets.

---

## Approaches & techniques (the full list)

| Technique | What it is | Lever |
|---|---|---|
| **HNSW btree-resident graph** | the whole ANN graph in the btree | crash-safe, multi-proc, no rebuild on open |
| **Hybrid layer-0 mirror (CSR)** | pointer-free RAM adjacency mirror | search ≈ btree→1.4–1.8× faster, few MiB, GC-free |
| **Vector tier (`HybridCacheVectors`)** | RAM slab of layer-0 vectors | the search win: sub-0.3 ms p50, several× QPS |
| **int8 scalar quantization** | per-vector scale + int8 components | ~4–4.5× smaller (disk & tier), ~0.5 pt recall |
| **Packed `TypeVectorF32`** | embedding stored as one f32 blob | ~2× smaller docs, zero-copy decode, 1.5–2.7× pipeline |
| **Parallel in-RAM build** | build the graph across all cores, flush once | ~17k vec/s vs ~770 per-insert (~22×) |
| **Parallel compaction** | rebuild live set with the parallel builder | 10k live in 1.4 s; reclaims tombstones |
| **Per-batch insert cache** | RAM arena of hot vectors during a write batch | 2.4× (default 16384) → 3.8× (sized to set) batch insert |
| **`CompactRatio` auto-compaction** | rebuild when tombstones reach ratio×live | caps tombstone growth; ~0.5 default |
| **IVF-SQ (`VectorModeIVFSQ`)** | coarse cells + int8 vectors scanned directly | **~6–9× insert/update vs HNSW**, smallest index, lowest IVF RAM |
| **IVF-PQ (`VectorModeIVFPQ`)** | coarse cells + PQ codes + int8 re-rank | same niche; IVF-SQ dominates it at this scale |
| **IVF closure / IVFADC / int8 re-rank** | recall-at-low-probe / fast PQ scan / small PQ index | best IVF-PQ point: `closure=4, np16` |
| **Process-global knobs** | `SetVectorBuildConcurrency`, `InitPageBuffer`+`UseGlobalPageBuffer` | bound build CPU / pre-allocated page slab |
| **`CacheSize` sizing** | btree page cache in pages | non-tiered f32 search ↑ up to ~48% when cache ≈ index |

### Configuration model (process-global vs per-DB)
Two genuinely process-wide settings are package functions (called once at startup),
mirroring SQLite's global config; only per-DB choices live on `Config`:

| Concern | Process-global setup | Per-DB |
|---|---|---|
| Page buffers | `InitPageBuffer(pageSize, nPages)` | `Config.UseGlobalPageBuffer` |
| Build/compaction parallelism | `SetVectorBuildConcurrency(n)` | — (always shared) |

---

## Benchmark results

Dataset: **38,463 × 768 real text embeddings**, cosine, 500 leave-one-out queries,
k=10, file-backed. `recall@10` vs exact top-10. Primary host: **32-core amd64,
AVX2**. RAM = live heap (forced GC).

### HNSW index matrix (default 5000-page cache)

| mode | build/s | insert/s | update/s | search/s | p50 ms | recall@10 | index MiB | heap MiB |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| btree int8 | 16.9k | 1099 | 915 | 1109 | 0.93 | 0.953 | **36** | **178** |
| hyb+vc f32 | 16.7k | 964 | 839 | **3909** | **0.25** | 0.959 | 175 | 468 |
| hyb+vc int8 | 17.0k | 1113 | 921 | 1980 | 0.52 | 0.954 | 36 | 217 |

`build/s` is batched insert into a live index. The **parallel bulk build** (insert
docs first, then `CreateIndex`) is the fast path: **16.9k vec/s, 2.27 s for 38k×768**,
recall 0.952 — vs ~770/s building incrementally (~22×). **Compaction**: 10k live
vectors rebuilt in **1.38 s** (48 MiB reclaimed).

### IVF-SQ vs IVF-PQ vs HNSW (all optimizations, local 32c)

| mode | recall | search/s | insert/s | index MiB | heap MiB |
|---|--:|--:|--:|--:|--:|
| HNSW btree int8 | 0.953 | 1144 | 1114 | 36 | 178 |
| HNSW hyb+vc f32 | 0.961 | **3520** | 941 | 175 | 468 |
| IVF-PQ i8 np64 | 0.951 | 485 | 5536 | 39 | 264 |
| **IVF-SQ np64** | 0.951 | 487 | **8640** | **35** | **183** |
| IVF-SQ np32 | 0.920 | 885 | 8780 | 35 | 183 |

- **IVF-SQ is the write champion: ~8,600 insert/s ≈ 7.8× HNSW btree int8**, at the
  **same RAM (183 vs 178)** and the **smallest index (35 MiB)**, recall parity.
- **IVF-SQ dominates IVF-PQ** at equal recall: build ~2.3×, insert ~1.5×, RAM 1.4×
  lower, smaller index, equal search — it drops PQ codes and the separate re-rank
  store, scanning int8 vectors directly. (Flat-codebook arenas added a further
  +4–11% on the IVF write paths.)
- **Search stays HNSW's domain**: IVF modes are ~2× below btree int8 and ~7× below
  `hyb+vc f32` at parity recall; tune via `NProbe` (np32 → 885 q/s @ 0.920 recall).
- IVF **build still under-parallelizes** (IVF-SQ ~7k/s local but ~850/s on 48c) — an
  open optimization vs HNSW's parallel build.

### Cross-hardware (insert/s · search/s · index MiB · heap MiB)

| mode | 32c amd64 AVX2 | 16c amd64 laptop | 48c amd64 (weak cores) | 8c M2 arm64 scalar |
|---|---|---|---|---|
| HNSW btree int8 | 1114·1144·36·178 | 521·538·36·178 | 134·265·36·179 | 181·393·36·— |
| HNSW hyb+vc f32 | 941·3520·175·468 | 412·1982·175·468 | 113·788·175·469 | — |
| IVF-PQ i8 np64 | 5536·485·39·264 | 3187·227·39·262 | 646·112·39·259 | — |
| **IVF-SQ np64** | **8640·487·35·183** | **4593·262·35·184** | **786·86·35·184** | — |

Recall, index size and heap are **arch-invariant**. IVF-SQ's write win holds on every
machine — insert is **~6–9× HNSW** (7.8× local, 8.8× p14, 5.9× hp) — as does its
RAM parity with HNSW int8 (~183 vs ~178 MiB) and the lowest IVF index size (35 MiB).
(M2/arm64 IVF numbers pending; the ~2–2.5× scalar-distance penalty below applies.)

### SIMD isolation (same machine, AVX2 on vs off)

vek provides AVX2 only for amd64; **arm64 (Apple Silicon, iOS/Android) runs a scalar
Go fallback**. Forcing AVX2 off on one host isolates the SIMD factor:

| path | AVX2 / scalar |
|---|:--:|
| build / compaction | ~2.5× |
| single insert | ~2.5× |
| update | ~2.0× |
| search (btree, memory-bound) | ~1.3× |
| search (hyb+vc f32, pure-distance) | ~2.25× |

So distance-bound paths pay ~2–2.5× on arm64 today; NEON kernels (128-bit, half of
AVX2's width) would recover most of it — biggest on build/compaction and insert.

### Page cache / slab sizing (perf vs RAM)

- **Slab (`UseGlobalPageBuffer`) is throughput-neutral** — its value is a bounded,
  GC-quiet page-cache footprint, not speed.
- **`CacheSize` is the page-level dial, for non-tiered modes only.** Search climbs as
  the cache approaches the index size, then plateaus: large f32 btree/hybrid gains up
  to ~48%; int8 saturates early; tiered `hyb+vc` is cache-insensitive (serve RAM from
  the tier, not the page cache). Sweet spot ≈ `CacheSize ≈ index size`.

### Insert cache (`ASV_VCACHE` / `SetInsertCacheSize`)

Batch insert (768 btree int8): **572/s disabled → 1366/s @16384 (2.4×) → 2164/s @65536
(3.8×)**. On by default (16384); size toward the working set for bulk loads.

---

## Choosing

- **Default / search-latency:** HNSW `hybrid + HybridCacheVectors + int8` (sub-0.3 ms,
  several k q/s; RAM ≈ vector size).
- **Smallest / lowest RAM:** HNSW `btree + int8` (36 MiB, 178 MiB heap) or **IVF-SQ**
  (35 MiB, ~183 MiB heap).
- **Write-heavy ingest:** **IVF-SQ** (~6–9× insert/update vs HNSW, smallest index,
  RAM on par with HNSW int8, recall parity), accepting ~2× lower search throughput.
  Prefer it over IVF-PQ at this scale.
- **Exact / tiny sets:** `BruteForce`.
- **arm64 / on-device:** expect ~2–2.5× slower distance-bound ops until NEON kernels
  land; favour the tiered mode (fewer distance ops per query) and int8.

## Reproduce

The standalone `cmd/vbench` binary (in the `any-store-vector` harness) runs this
matrix on any OS/arch, no Ollama, dataset embeddable for a single all-in-one binary:

```bash
vbench -data <dir> -q 500                       # full HNSW+IVF-PQ matrix incl. int8
vbench -modes "ivfsq np64" --cpuprofile p.out --memprofile m.out
```
