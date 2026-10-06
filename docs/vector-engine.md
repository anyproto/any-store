# any-store vector engine — design, approaches & benchmarks

## TL;DR (short description)

any-store is an **embedded, btree-resident document store** (MVCC, WAL, crash-safe,
multi-process-safe) with **built-in approximate-nearest-neighbour (ANN) vector
search**. Embeddings live in ordinary documents and are queried through the normal
`Find()` pipeline — a `$knn` clause selects the k nearest documents, ordinary
filters/sorts apply on top, and each result carries its `_distance`. There is no separate vector server
or in-memory index to manage: the index **lives in the same btree as the data**, so
it inherits the store's durability, snapshot isolation, and cross-process semantics.

Two index families, same btree substrate:

- **HNSW** (graph) — `VectorModeBTree` / `VectorModeHybrid` (+ optional RAM vector
  tier) / `VectorModeBruteForce`. Best **search latency/throughput**.
- **IVF-SQ** (inverted file) — `VectorModeIVFSQ`: partition into coarse cells,
  probe a few, scan the int8 vectors stored under them directly. **Best write
  throughput** (~8× HNSW insert at 38k, ~15× at 1M), smallest index, RAM on par
  with HNSW int8 (only the coarse centroids are hot).

Best **search** = HNSW (esp. `hybrid + HybridCacheVectors`); best **writes** = IVF-SQ.
HNSW supports `int8` (~4–4.5× smaller) at ~0.5 pt recall cost; IVF-SQ is int8 by
construction. (A product-quantization flavour, IVF-PQ, was removed: at equal recall
IVF-SQ built, wrote and searched faster, was smaller and used less RAM — its rows are
kept in the tables below for the record.)

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

### IVF-SQ layout
- A **coarse quantizer** (k-means centroids, `NList` cells) partitions the space;
  each vector is assigned to its cell(s).
- **Inverted lists** are contiguous btree key ranges (cell-major), so a probe is a
  handful of **sequential range scans** — the btree-friendly access pattern.
- **Closure (SPANN-style multi-assignment):** each vector is placed in its `Closure`
  nearest cells so boundary vectors are found at lower `NProbe`.
- The centroid table is held as a **flat arena** (one slice + offset accessors) —
  fewer slice headers, better locality on the assign (insert) path.
- Each cell stores the **int8 full vectors** (per-vector scale, offset-binary
  components; L2 keeps a per-vector squared norm), scanned directly with exact int8
  distance via the float×byte kernel. No codebook beyond the centroids, no encode
  step, no re-rank store — so build/insert are fast and the index is the smallest of
  any mode.
- Search = probe `NProbe` cells → score every member → keep the `ef` best → resolve
  docIDs; results stream back distance-ordered (the planner then skips the sort).

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
| **IVF-SQ (`VectorModeIVFSQ`)** | coarse cells + int8 vectors scanned directly | **~6–9× insert/update vs HNSW**, smallest index, RAM ≈ HNSW int8 |
| **IVF closure** | each vector in its `Closure` nearest cells | recall at lower `NProbe`, at ~Closure× the cell bytes; a higher `NProbe` is usually cheaper |
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

### IVF-SQ vs HNSW (all optimizations, local 32c)

Local 32c, with the int8 byte kernel (§ Distance backend); the IVF-PQ row is the
removed flavour, kept for the record:

| mode | recall | search/s | insert/s | index MiB | heap MiB |
|---|--:|--:|--:|--:|--:|
| HNSW btree int8 | 0.953 | 1582 | 1044 | 36 | 178 |
| HNSW hyb+vc f32 | 0.959 | **3519** | 892 | 175 | 468 |
| **IVF-SQ np64** | 0.951 | 1041 | **8583** | **35** | **180** |
| IVF-SQ np32 | 0.920 | 1794 | 8504 | 35 | 189 |
| IVF-PQ i8 np64 (removed) | 0.951 | 506 | 5295 | 39 | 257 |

- **IVF-SQ is the write champion: ~8,500 insert/s ≈ 8× HNSW btree int8**, at the
  **same RAM (180 vs 178)** and the **smallest index (35 MiB)**, recall parity.
- **IVF-SQ search ~2.2–2.3× faster** since the byte kernel (np64 487→1041, np32
  885→1794): the cell scan scores every probed-cell member by exact int8
  distance, so removing dequant lands the full win.
- **IVF-SQ beat IVF-PQ** at equal recall: build ~2.3×, insert ~1.5×, RAM lower,
  smaller index, faster search — no PQ codes, no separate re-rank store, no
  precomputed table; that is why PQ was removed.
- **Search still favours HNSW** (`hyb+vc f32` 3519 / btree int8 1582), but IVF-SQ
  np32 (1794) beats btree int8 at slightly lower recall; tune via `NProbe`.
- IVF **build under-parallelizes** (training runs across cores; the encode-and-put
  pass over every vector is serial: ~7k/s local but ~850/s on 48c) — an open
  optimization vs HNSW's parallel build.

### At 1M vectors (ann-benchmarks protocol, SIFT-1M / GloVe-100, 9950X)

| mode | build | index (MB) | queries/s @ ≥0.90 / ≥0.95 recall | max recall | insert/s |
|---|--:|--:|--:|--:|--:|
| SIFT HNSW hyb+vc f32 | 60 s | 758 | 6,385 / 3,856 | 0.999 | 205 |
| SIFT HNSW hyb+vc int8 | 60 s | 318 | 4,624 / 2,731 | 0.985 | 217 |
| SIFT IVF-SQ | 316 s | 214 | 781 / 239 | 0.980 | 3,389 |
| SIFT IVF-PQ f32 (removed) | 411 s | 718 | 473 / 134 | 0.993 | 3,071 |
| GloVe HNSW hyb+vc f32 | 88 s | 764 | 836 / 445 | 0.956 | not measured |
| GloVe IVF-SQ | 394 s | 207 | below 0.90 | 0.882 | not measured |
| GloVe IVF-PQ f32 (removed) | 409 s | 611 | below 0.90 | 0.748 | not measured |

Sizes as the run reported them (MB); insert/s from the SIFT incremental-write run
at `NProbe` 32. IVF-SQ's recall on clustered angular data (GloVe) caps at 0.88 with
`NProbe` 128, below RAM HNSW's 0.956; on L2 data (SIFT) it reaches 0.98. Its writes
run ~15× HNSW's; its queries 6–16× slower than RAM HNSW at matched recall. The
k-means build is the widest gap (5–7 min against 1–1.5 for HNSW).

### Cross-hardware (insert/s · search/s · index MiB · heap MiB)

Four machines, with the int8 byte kernel (after migration):

| mode | 32c AVX512 | 16c AVX2 | 48c (no-AVX2) | 8c M-series arm64 NEON |
|---|---|---|---|---|
| HNSW btree int8 | 1044·1582·36·178 | 509·744·36·178 | 154·271·36·179 | 525·713·36·178 |
| HNSW hyb+vc f32 | 892·3519·175·468 | 421·1920·175·468 | 123·856·175·469 | 500·1984·175·467 |
| **IVF-SQ np64** | **8583·1041·35·180** | **4166·489·35·182** | **933·122·35·181** | **4883·691·35·183** |
| IVF-PQ i8 np64 (removed) | 5295·506·39·257 | 2711·239·39·265 | 700·123·39·262 | 2708·326·39·263 |

Recall, index size and heap are **arch-invariant**. IVF-SQ's write win holds on every
machine — insert is **~6–9× HNSW** — as does its RAM parity with HNSW int8 and the
smallest index (35 MiB). The **48c box has no AVX2**, so `internal/simd`
dispatches to the pure-Go unrolled kernel (correct, recall-identical, just slower).

**Apple-Silicon arm64 now runs NEON** (was a scalar fallback under vek). On an 8-core
M-series Mac the same `cmd/vbench` all-in-one binary shows the scalar → NEON lift:
HNSW btree-int8 **build 806 → 2991/s (~3.7×)**, p50 **4.64 → 1.37 ms**; hyb+vc int8
build ~700 → **3719/s**; recall unchanged. The lift spans **every** mode (not just
int8) because vek had no ARM SIMD at all — even f32 was scalar before.

### Distance backend & SIMD (`internal/simd`)

Distance kernels run through **`internal/simd`** — vendored hand-written assembly
(weaviate, BSD-3-Clause): **dot/L2 for amd64 (AVX2/AVX512) and arm64 (NEON/SVE)**,
plus a **float×int8 kernel**, with a pure-Go fallback for wasm and non-AVX2 x86.
This replaced the former `viterin/vek` (amd64-only, which left arm64 on a scalar
fallback), so **`vector.SIMD()` is now true on Apple Silicon** — ARM gets real NEON,
not scalar.

**int8 byte distance.** Stored int8 vectors are scored straight from their bytes
via the unsigned float×byte kernel (offset-binary format, `byte(q)^0x80`), skipping
the per-element dequantization loop. Isolated int8 distance at dim768 drops
**308 → 19.8 ns (15.6×) — f32 speed at 4× smaller storage**. End-to-end on the real
dataset (recall unchanged):

| path | int8 search before → after |
|---|---|
| HNSW btree int8 | 1021 → 1582 q/s (+52%) |
| HNSW hyb+vc int8 | 1930 → 4925 q/s (~2.5×) |
| IVF-SQ np64 (cosine) | 437 → 1041 q/s (~2.3×) |
| IVF-SQ (isolated, cosine/L2) | ~2.9× / ~3.1× |

IVF-SQ accelerates **both cosine and L2** (L2 via `‖q−x‖²=‖q‖²+‖x‖²−2·dot`, storing a
per-vector `‖x‖²`); HNSW int8 covers cosine/dot (L2 keeps the float path). The change
is read-path only — builds are unchanged. f32 search is unchanged within noise.

The **no-AVX2 x86** path uses the pure-Go unrolled kernel, which *beats* vek's old
fallback (682 vs 870 ns at dim768).

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
- **Write-heavy ingest / large collections:** **IVF-SQ** (~6–9× insert/update vs
  HNSW here, ~15× at 1M; smallest index; RAM on par with HNSW int8; recall parity at
  this scale), accepting ~2× lower search throughput here and 6–16× at 1M. What
  large deployments run; the API default stays `VectorModeBTree`.
- **Exact / tiny sets / high-selectivity pre-filters:** `BruteForce` — zero build
  cost, 100% recall; the planner drives a selective range index and scores the few
  survivors exactly.
- **arm64 / on-device:** distance kernels now run **NEON** via `internal/simd`
  (Apple Silicon, Graviton, mobile) — no longer a scalar fallback. int8 is scored
  by the NEON byte kernel (no dequant). Still favour the tiered mode and int8.

## Reproduce

The standalone `cmd/vbench` binary (in the `any-store-vector` harness) runs this
matrix on any OS/arch, no Ollama, dataset embeddable for a single all-in-one binary:

```bash
vbench -data <dir> -q 500                       # full HNSW+IVF matrix incl. int8
vbench -modes "ivfsq np64" --cpuprofile p.out --memprofile m.out
```
