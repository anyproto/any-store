# IVF-SQ: Production Assessment & TODOs

IVF-SQ (`VectorModeIVFSQ`) is the leading candidate mode for Anytype-style
local-first workloads. Assessment from the any-store-tests cross-machine runs
on f414ff6 (r9950x, r3900x Win+Linux dual-boot, p14, hp; real embeddinggemma
corpus, 20k × 768 unless noted). Result files:
any-store-tests `results/dist-runs/`, findings in PERF-1..3.

## Why it is the favourite (measured)

- Fastest indexed build: ~9.7k docs/s bulk at 20k×768 (11.9k/s at dim 128);
  incremental inserts don't pay HNSW graph maintenance.
- Smallest index: ~3.5x smaller than f32 HNSW (int8 vectors per cell,
  no graph).
- Plain kNN 1.2–1.5 ms/q at 20k×768 on every machine, on par with HNSW-int8 —
  and OS-neutral: no Windows penalty (f32 HNSW is ~2.5x slower on Windows on
  identical hardware; see any-store-tests PERF-2).
- Stability: cross-process build and compact/retrain-against-live-handle tests
  pass everywhere; structurally immune to the BUG-18 class (no entry point) —
  probed explicitly: update-after-train, same-tx insert+upsert, delete+search
  all clean.

## Concerns / TODOs (impact order)

1. **Residual filters are the weak spot — 7x degradation.**
   `{vec, "a": selective}` runs ~8.5 ms vs ~1.2 ms plain on a 3900X (HNSW-i8
   degrades 3.7x, IVF-PQ 2x). Sort-by-other-field similar (~8.7 ms).
   TODO: investigate filtered over-fetch strategy for IVF-SQ (adaptive probe
   widening? push the residual predicate into the cell scan? brute-force
   fallback below a selectivity threshold?). This is the main blocker for
   filter-heavy query shapes.

2. **Recall trade is real but tunable.** recall@10 = 0.944 vs 0.990 (HNSW-i8)
   on real 768-dim embeddings at NProbe=32; 0.81 on adversarial uniform data.
   TODO: document an NProbe-vs-recall curve on a real corpus so apps can pick;
   consider a higher default NProbe when nlist is small.

3. **Auto-rebuild is OFF by default** (`CompactRatio: 0` disables it) while
   centroids are frozen at build. DriftScore/Rebuild machinery is good —
   TODO: decide a recommended production default (~0.5?) or document the
   manual `CompactVectorIndex` maintenance-window pattern prominently;
   surfacing DriftScore in Stats would let apps schedule rebuilds themselves.

4. **Cold start**: EnsureIndex on an empty collection fails (k-means needs
   data). TODO: document the lazy-index-creation pattern (also the fast path:
   bulk seed-then-EnsureIndex is 7.7x faster than index-first for HNSW too,
   see any-store-tests PERF-3).

5. **Quantization slack**: int8 self-distance ≈ 0.05, not 0. TODO: document
   that `_distance` thresholds need headroom in IVF-SQ/int8 modes.

6. **Crash coverage**: the any-store-tests SIGKILL workload now has a
   `vector-fts-ivfsq` variant (seed → train → mixed churn with auto-rebuild
   under kill, index-vs-collection verification on recovery). Keep it in the
   rotation alongside the btree-mode workload.
