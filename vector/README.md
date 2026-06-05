# Experimental: vector (ANN) search for any-store

Status: **experimental spike** on branch `btree-vector-search`. Not wired into
the public `anystore` API yet — this package is a self-contained exploration of
how to land approximate-nearest-neighbour (ANN) search on top of the embedded
btree engine, using [`github.com/coder/hnsw`](https://github.com/coder/hnsw) as
the algorithmic reference.

The goal was to answer three questions:

1. Can we keep SIMD (vector CPU instructions) for the distance kernels, like the
   reference, without breaking any-store's "pure Go, no CGO" rule?
2. How much do any-store's memory practices (arenas / struct-of-arrays / pooled
   scratch) buy us over an idiomatic map-and-pointers HNSW?
3. What does it cost to make the index **durable** by persisting it into the
   btree, versus keeping it purely in memory?

## What's here

| File | Approach |
|------|----------|
| `distance.go` | `L2` / `Cosine` / `Dot` distance. SIMD via [`viterin/vek`](https://github.com/viterin/vek) (AVX2/AVX-512, pure-Go assembly, **no CGO** — the same library coder/hnsw uses) plus scalar & hand-unrolled fallbacks kept only for the benchmark. |
| `brute.go` | Exact flat scan. Ground truth for recall, O(n·dim) per query. |
| `hnsw.go` | **Map-based in-memory HNSW** — a faithful adaptation of coder/hnsw (`map[id]*node` adjacency, per-node pointers). The "idiomatic Go" baseline. |
| `hnsw_flat.go` | **Arena / SoA HNSW** — every vector in one contiguous `[]float32` slab, every adjacency list in one flat `[]uint32` arena, dense `uint32` ids, pooled per-query heaps + epoch-stamped visited set. Allocation-free steady-state search. |
| `heap.go` | Two-heap search frontier + epoch-stamped `visitedList` (arena trick: bump a generation counter instead of clearing N bytes per query). |
| `hnsw_btree.go` | **btree-backed persistent HNSW** — wraps the flat index, persists each node (vector + per-layer adjacency) into btree namespaces, and rebuilds the arenas on reopen straight from the persisted records (no re-construction). |
| `hnsw_flat_delete.go` | **deletes/updates** — tombstone delete, update (delete+reinsert), Compact, Rebuild, and a hard-delete-with-repair variant for cost comparison. See [DELETE_UPDATE.md](./DELETE_UPDATE.md). |
| `idmap.go` + `docindex.go` | **doc-id mapping** — `IDDict` turns any-store's `[]byte` document ids into dense `uint32` labels via a flat arena; `DocFlatHNSW` shows the composed `[]byte`-keyed index. |

### Design notes & comparisons

- [DELETE_UPDATE.md](./DELETE_UPDATE.md) — deletes/updates research + measurements (tombstones, compaction, write amplification, RAM, doc-id mapping).
- [COMPARISON_pgvector.md](./COMPARISON_pgvector.md) — vs pgvector (disk-resident buffer-cache-paged graph vs in-memory arena; the "real fork" for any-store).
- [COMPARISON_mongodb.md](./COMPARISON_mongodb.md) — vs MongoDB Atlas Vector Search (closest external/API analog; Lucene segment model; quantization; read-your-writes advantage).
- [OPTION_B.md](./OPTION_B.md) — prototype + measurements of paging vectors through the btree page cache (disk-resident graph): pure paging is ~6–10× slower, but a quantized-routing + paged-rerank **hybrid** lands at ~1.7× with a fraction of the RAM (`paged.go`). Includes a realistic **75k markdown-doc / dim-768** capstone where paging holds **94% less RAM**.

`mddata_test.go` (`TestMDDataset`) is the realistic-scale dataset: 75k synthetic topic-clustered markdown documents embedded with the feature-hashing trick (dim 768, cosine) — representative geometry, not uniform-random. Run with `go test ./vector -run TestMDDataset -v` (skipped in `-short`).

- [CROSS_HARDWARE.md](./CROSS_HARDWARE.md) — `cmd/vectorbench` run on three linux/amd64 machines (disk-backed, twice each). Headline: a no-AVX2 box makes distance ~25× slower (vek's fallback is even slower than the unrolled loop) — SIMD presence dominates everything; the hybrid stays the sweet spot (1.3–2.4×) and cold reload-from-disk is 8–37 ms loading only topology.

A self-contained, static, no-CGO benchmark binary lives at [`cmd/vectorbench`](../cmd/vectorbench) — build once, copy to any linux/amd64 box (no Go needed on the target), and run `-db <path>` twice for build-then-cold-reload measurements.

## Three ways to land it — and the one that wins

The arena/flat layout (`FlatHNSW`) is the centrepiece. It differs from the
map-based port in exactly the ways any-store already optimises elsewhere:

- **Vectors in an arena.** One `[]float32`, vector `i` at `[i*dim:(i+1)*dim]`.
  No per-vector slice header, no `N` pointers for the GC to scan, sequential
  reads feeding the SIMD kernels.
- **Adjacency in an arena.** One flat `[]uint32` grown append-only as dense ids
  are assigned — no `map[id]*node` per node. A node with top layer `L` occupies
  `M0 + L*M` slots; the dense+monotonic id stream is exactly the access pattern
  an arena is good at.
- **Pooled scratch.** The two search heaps and the visited set are pooled and
  reused, so a steady-state query allocates nothing.

`BtreeHNSW` reuses that same arena layout as its on-disk format: a forward
cursor over the (big-endian-keyed) nodes namespace yields nodes in id order,
which is precisely the order `appendRaw` needs to rebuild the arenas.

## Benchmarks

Machine: 32-core x86-64, Go 1.26, `vek` reporting AVX acceleration active.
Reproduce with:

```
go test ./vector -run TestMemoryFootprint -v          # memory
go test ./vector -bench BenchmarkDistance -benchmem    # SIMD vs scalar
go test ./vector -bench 'BenchmarkSearch' -benchmem     # query latency
go test ./vector -bench 'BenchmarkBuild' -benchmem -benchtime=1x   # build
```

### 1. SIMD distance — keeping vector CPU instructions pays off

`L2` distance, ns/op, 0 allocs:

| dim | scalar | 4-way unrolled | **SIMD (vek/AVX)** | SIMD vs scalar |
|----:|-------:|---------------:|-------------------:|---------------:|
| 128  | 35.96 | 24.86 |  **5.56** |  6.5× |
| 768  | 271.3 | 148.9 | **20.32** | 13.4× |
| 1536 | 549.2 | 288.4 | **38.28** | 14.3× |

For typical embedding sizes (768/1536) SIMD is **13–14× faster** than naive
scalar and ~7× faster than what the compiler manages from unrolled Go. Distance
dominates ANN work, so this is the single biggest lever — and it stays CGO-free.

### 2. Search latency — 20 000 × 128-dim, k=10, efSearch=64

| index | ns/op | B/op | allocs/op | vs brute |
|-------|------:|-----:|----------:|---------:|
| brute (exact)   | 2 217 256 | 327 768 | 4 | 1× |
| map HNSW        |   446 836 | 240 777 | 518 | 5.0× |
| **flat HNSW**   |    **43 942** | **160** | **1** | **50×** |

The arena index is **~10× faster than the map HNSW** and **~50× faster than
brute**, while doing essentially zero allocation per query (the one alloc is the
returned result slice). The map version pays 518 allocs/query chasing pointers
and building temporary neighbour-key slices.

### 3. Build — 20 000 × 128-dim

| index | time | alloc bytes | allocs |
|-------|-----:|------------:|-------:|
| map HNSW (ef=64) | 6.63 s | 3.73 GB | 9.0 M |
| **flat HNSW** (efConstruction=200) | **2.88 s** | **930 MB** | **20 441** |
| btree HNSW (efConstruction=200 + durable persist) | 3.08 s | 2.15 GB | 2.74 M |

The flat index builds **2.3× faster** than the map index and allocates **440×
fewer times** — despite using a *heavier* construction (efConstruction=200 vs
64). Cache locality and the absence of per-node maps more than pay for the
larger candidate list.

Persisting to the btree adds only **~7 %** wall-time over the pure in-memory
build (writes are batched into one transaction via `Flush`). The extra
allocations are serialization buffers + btree page writes.

### 4. Memory footprint — 20 000 × 128-dim (raw vectors = 9.8 MiB)

| index | resident | × raw vectors |
|-------|---------:|--------------:|
| map HNSW  | 28.6 MiB | 2.93× |
| **flat HNSW** | **14.5 MiB** | **1.48×** |

The arena index is **~2× smaller**. Both own a private copy of every vector, so
the difference is pure structural overhead: maps + node structs + per-node slice
headers vs. two flat arenas. `FlatHNSW.MemBytes()` (13.9 MiB, cap-based)
corroborates the measured 14.5 MiB.

### 5. Recall (quality is not sacrificed)

Recall@10 vs exact brute force (random vectors):

| index | L2 | Cosine |
|-------|---:|-------:|
| map HNSW (ef=64)  | 0.90 | 0.89 |
| flat HNSW (ef=64, efC=200) | 0.98 | 0.99 |
| btree HNSW, **after close + reload from disk** | 0.99 | — |

The btree variant returns **byte-identical results before and after a reload**
(deterministic graph reconstruction), confirming the persistence format is
lossless.

## Takeaways / recommendation

- **Keep SIMD via `vek`.** It's the dominant speedup, it's no-CGO, and it's the
  reference's own choice. (Go 1.26 ships an experimental `simd` package too, but
  it needs `GOEXPERIMENT=simd` — not worth the build friction yet.)
- **Land the arena/SoA layout, not the map port.** ~10× faster search, ~2×
  faster build, ~2× less memory, ~zero query allocations, same recall.
- **Persist with the arena layout as the on-disk format.** Durability via the
  btree costs only ~7 % build time and reloads losslessly. The natural next step
  for real integration is to drive `BtreeHNSW` from collection write
  transactions and expose it as a vector index type.

## Known simplifications (this is a spike)

- Neighbour pruning uses coder/hnsw's simple "drop the farthest" rule; it does
  not remove the reverse backlink on prune (slightly asymmetric graph, standard
  in lightweight HNSWs, negligible recall impact at these sizes).
- `FlatHNSW.Add` is append-only (no per-key update/delete yet); deletes would
  need tombstones or a compaction pass.
- `BtreeHNSW` serialises whole node records on change; a production version
  would split the rarely-changing vector blob from the frequently-changing
  adjacency list to cut write amplification.
- Search over the working set is in-memory; for datasets that exceed RAM the
  vectors could stay in the btree and be paged in, trading latency for footprint
  (not explored here).
