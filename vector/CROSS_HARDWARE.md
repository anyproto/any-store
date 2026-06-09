# Cross-hardware results (vectorbench)

Branch `btree-vector-search`. Produced by the self-contained `cmd/vectorbench`
binary (static, no-CGO) copied to three linux/amd64 machines and run with the
**disk-backed** index, **twice** per machine (build+persist, then cold
reopen-from-disk). All runs: `n=20000`, `dim=768`, cosine, ef=64, k=10.

```
go build -o vectorbench ./cmd/vectorbench
./vectorbench -n 20000 -dim 768 -db ~/vb.db      # run 1: build + persist
./vectorbench -n 20000 -dim 768 -db ~/vb.db      # run 2: cold reload from disk
```

## Machines

| host | cores | RAM | **SIMD (AVX2+FMA)** |
|------|------:|----:|:-------------------:|
| local | 32 | — | **yes** |
| p14 | 16 | 28 GiB | **yes** |
| hp | 48 | 121 GiB | **NO** |

## Distance kernel (L2, dim 768, ns per comparison)

| host | scalar | unrolled | SIMD (vek) | SIMD speedup |
|------|-------:|---------:|-----------:|-------------:|
| local | 277 | 152 | **20** | 13.6× |
| p14 | 547 | 291 | **36** | 15.2× |
| hp | 866 | 608 | **685** | **1.3×** |

**The headline finding.** `hp` is a bare-metal **dual-socket Intel Xeon E5-2697
v2 (2013 Ivy Bridge-EP, 24c/48t)**. Its flags include `avx` and `f16c` but
**not `avx2` and not `fma`** (those arrived with Haswell). vek requires AVX2+FMA,
so it falls back to portable Go — and its fallback (685 ns) is **slower than the
hand-unrolled scalar loop (608 ns)**. A 48-thread server thus comes *last* of the
three for vector search: ANN query latency is single-thread-distance-bound, so a
one-generation microarchitecture gap beats raw core count.

Actionable for any-store:
- **Prefer the unrolled scalar kernel over vek's fallback when AVX2 is absent**
  (detect via `vector.SIMD()` and dispatch).
- Better still, an **AVX-only** float32 distance kernel would accelerate this box
  too — it *has* AVX, vek just has no AVX-only path (all-or-nothing on AVX2+FMA),
  so the fallback leaves real speedup on the floor on Sandy/Ivy Bridge.
- Treat SIMD presence as a deployment fact to check, not assume.

## Build + search (n=20000, dim=768)

| host | build docs/s | recall@10 | A in-mem µs/q | B paged µs/q | B′ hybrid µs/q | cold reload |
|------|------:|------:|------:|------:|------:|------:|
| local (SIMD) | 7063 | 0.758 | 69 | 488 (7.1×) | **164 (2.4×)** | 8 ms |
| p14 (SIMD) | 3351 | 0.758 | 150 | 1145 (7.7×) | **321 (2.1×)** | 16 ms |
| hp (no SIMD) | 569 | 0.758 | 738 | 2839 (3.8×) | **986 (1.3×)** | 37 ms |

(recall is identical across machines — same deterministic graph.)

## What the cross-hardware run teaches

1. **SIMD presence dominates everything.** No-AVX2 `hp` is ~10× slower on
   in-memory search than `local` despite having more cores — single-thread
   distance throughput, not core count, drives query latency. Check it per box.

2. **The paged slowdown ratio is hardware-dependent and *smaller* on slow CPUs.**
   Pure paging is 7.7× on p14 but only 3.8× on hp: when the distance compute is
   huge (no SIMD), the fixed btree-lookup overhead is a smaller fraction of each
   hop. So the cost of Option B *relative to in-memory* shrinks exactly where you
   might deploy it (constrained hardware).

3. **The hybrid is the consistent sweet spot — 1.3–2.4× across all three.** It
   keeps that ratio low everywhere by paging only the ef rerank set.

4. **Cold reload-from-disk is cheap and hardware-robust: 8–37 ms**, loading only
   **3.9 MiB of topology** (vs 64 MiB for the full in-memory arena) regardless of
   machine — because the split persistence (`:topo`/`:vec`) reconstructs the
   navigable graph without the vectors. The Option-B memory win survives a
   restart on every box.

5. **On-disk footprint** was 183.6 MiB (incl. WAL) for 20k×768d — the split
   `:topo` + `:vec` records, dominated by the vectors. Identical across machines.

## Caveats

- `/tmp` is tmpfs on p14/hp, so the db lived under `$HOME`; without root to drop
  the OS page cache, "cold" means the **btree's own page cache** started empty
  (fresh process), not a cold OS file cache. The in-process reload/traversal path
  is what's exercised.
- `hp` being a no-AVX2 2013 Ivy Bridge Xeon (real hardware, not a VM) is the most useful accident here: it is the realistic
  "slow/constrained target" the binary was built to test.
