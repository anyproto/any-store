# Research: IVF-PQ / IVF-OPQ / residual-PQ as a btree-native vector index

Status: **research / design** (branch `feat/vector-ivfpq-btree`).
Question posed: *instead of HNSW, evaluate the IVF-PQ family (IVFPQ, IVF-OPQ, residual/additive-PQ
hybrids) — the hypothesis being that these algorithms fit any-store's btree far better than a graph.*

**Verdict up front: the hypothesis is correct.** HNSW search is a chain of *dependent random
point-lookups* (one btree root-to-leaf descent per graph neighbour). IVF search is *a handful of
independent contiguous range scans* — exactly one `Cursor.Seek` + sequential `Cursor.Next()` per
probed cell. A SQLite-derived pager is built for the latter and actively hostile to the former. The
read path is the easy win; the genuinely hard part of going IVF is **index maintenance under churn**
(centroid drift), and the bulk of this doc's design effort goes there.

> Terminology note: the request listed "VFPQ". There is no published method by that name — it is a
> transposition of **IVFPQ** (FAISS `IndexIVFPQ`: a coarse IVF quantizer + product quantization of
> the per-cell residual). "IVFOPQ" = IVF + OPQ rotation + PQ. "Residual PQ hybrids" = the
> additive/residual-quantizer family (RQ/AQ/LSQ). All three are the same FAISS IVF+(O)PQ/AQ line and
> are treated as one family here.

---

## 1. Why HNSW fights the btree (the problem we are escaping)

This is not speculation — it is already documented in our own tree:

- `docs/vector-search.md` and `internal/vindex/search.go`: layer-0 beam search expands `ef`
  candidates, and **each expansion does one `:adj` read for the neighbour list, then one `:vec` read
  *per neighbour* for the distance, then (when tombstones exist) one more `:adj` read *per neighbour***.
- `vector/PLAN_HYBRID_L0.md` §0 records the measurement verbatim: *"search is btree-read-bound
  (~79% of CPU in btree tree-descent for `AppendValue`; distance only ~4.6%)."* The entire Hybrid-L0
  plan — an in-RAM mirror of layer-0 adjacency, a `:l0log` delta journal, a cross-process version
  ring, COW publish — **exists solely to claw back btree reads that HNSW's access pattern forces.**

The root cause is structural. HNSW assumes *O(1) random neighbour access*, which is true in RAM and
false through a pager. Each hop is:

```
Get(:adj[node])  -> decode neighbours -> for each nbr: Get(:vec[nbr]) ; Get(:adj[nbr])
```

Every `Get` is a fresh root-to-leaf btree descent to a **scattered** key, and the *next* descent
depends on the *result* of the previous one — so readahead, page prefetch, and range locality all
miss. Coleman et al. (NeurIPS '22) wrote a whole paper on reordering HNSW nodes for cache locality
*because* this access pattern "frequently evicts useful data from cache." The Hybrid-L0 mirror is a
real, careful fix — but it is fundamentally *re-adding an in-RAM graph that the all-btree design
deliberately removed*, with all the cross-process-consistency machinery that re-entails.

IVF sidesteps the problem instead of mitigating it.

---

## 2. The algorithm family, concretely

### 2.1 Product Quantization (PQ) — the compression primitive

Split a `D`-dim vector into `M` contiguous sub-vectors of dim `D/M`; quantize each against its own
codebook of `K=256` centroids (1 byte each). Code = **M bytes**. Codebook = `M·256·(D/M) = 256·D`
floats (independent of M). For `D=768, M=96`: 96-byte codes (32× smaller than 3072-byte f32), 768 KB
of codebook.

**Asymmetric Distance Computation (ADC)** is the property that makes it fast and btree-friendly. The
query is *not* quantized. Per query, precompute one lookup table:

```
LUT[m][j] = distance(query_sub_m, centroid[m][j])     // M × 256 floats, built once per query
```

Then every candidate's approximate distance is `Σ_{m<M} LUT[m][code[m]]` — **M table lookups + M-1
adds, no multiplies, and no access to the candidate's full vector.** This is the key: scoring a cell
member touches *only its M-byte code*, never the 3 KB vector. So an IVF cell can be a dense run of
96-byte codes that we stream sequentially and score in L1 cache.

### 2.2 IVF — the partition that becomes a range scan

A coarse k-means quantizer with `nlist` centroids partitions space into Voronoi cells. Each vector is
assigned to its nearest centroid; the cell's *inverted list* holds its members. A query finds the
`nprobe` nearest centroids and scans **only those lists**. FAISS sizing: `nlist ≈ 4–16·√N`; `nprobe`
is the single recall/speed dial (SIFT1M `IVF4096,PQ32`: nprobe 1 → ~30% recall in ~136 µs; nprobe 8 →
~74% in ~729 µs). For our 10K–10M range that's `nlist` from a few hundred to ~65536.

### 2.3 IVFPQ — PQ on the *residual* (the standard combination)

Encode `r = x − centroid(x)`, not `x`. Residuals are small and centred, so PQ distortion drops sharply
for the same M-byte budget. At query time the per-cell LUT is built from the query *residual*
`q − centroid_i` for each probed cell `i`. Memory ≈ `M bytes + id`. FAISS `IVF256,PQ32` on 1M vecs ≈
40 MB total vs 520 MB flat.

### 2.4 The recall ladder (what to store in each code byte)

From the residual-quantization research, ranked by **recall-per-byte** at a fixed code size:

| Method | Recall/byte | Per-query scoring | Encode cost | Notes |
|---|---|---|---|---|
| plain **PQ** | baseline | O(M) clean LUT | cheapest (M closed-form argmins) | the stepping stone |
| **OPQ**+PQ | +4–5% recall@10 @64-bit | O(M) LUT + 1 query rotation | + learn one `D×D` rotation | rotation balances subspaces |
| **ScaNN** anisotropic PQ | best *for cosine/IP*, ~free at query | O(M) LUT (identical to PQ) | PQ-class (anisotropic codebook update) | only changes codebook *training* |
| **RQ** (residual/additive) | better than OPQ, esp. small codes | O(M) LUT for IP; +stored norm for L2 | beam search (much costlier) | sum of M full-D codewords |
| **AQ / LSQ** | best | O(M)/O(M²) | simulated annealing (costliest) | rarely worth the encoder complexity |

**Recommendation for a pure-Go, no-heavy-SIMD implementation:** ship **plain PQ first**, then upgrade
the *codebook-training* step to **ScaNN-style anisotropic weighting** — because our vectors are
cosine/dot embeddings (the exact regime where the anisotropic score-aware loss pays off), and it keeps
PQ's clean `O(M)` LUT and 1-byte-per-subspace argmin encode (no beam search). **OPQ** (one extra matrix
multiply at encode and query) is a cheap optional recall bump. **RQ/AQ/LSQ** are a *later* phase only if
a tiny fixed byte budget makes their recall edge worth the iterative encoder — and even then, RQ before
LSQ/AQ, with an 8-bit stored norm for the L2 distance term (avoid the O(M²) cross-term path).

### 2.5 Re-ranking — how IVFPQ reaches high recall (and why we're well-positioned)

PQ distance is approximate, so the standard pattern is two-stage: IVFPQ returns the top `k_factor·k`
candidates by ADC distance, then **re-rank the survivors by *exact* distance using full-precision (or
int8) vectors.** This recovers most of the quantization recall loss; the residual loss is then
dominated by IVF's `nprobe`, not by PQ.

**We already store the exact vectors.** The `:vec` namespace holds the full f32 (or int8) vector per
label, and `internal/vindex/distance.go` already has the SIMD L2/cosine/dot kernels and the int8
codec. Re-rank is "fetch `:vec[label]` for the ~`k_factor·k` survivors and run the existing kernel" —
a tiny, bounded number of point lookups (not the per-neighbour-per-hop storm HNSW pays).

---

## 3. Why IVF *is* a btree (the core fit)

The btree already exposes the exact primitive (`internal/btree/btree.go`): `Cursor.Seek(key)`,
`Cursor.Next()`, `Cursor.Value()`, `Cursor.Valid()`. Key an inverted list so that one cell is a
**contiguous lexicographic key range**:

```
key   = listID (uint32 big-endian) || label (uint32 big-endian)
value = pq_code[M]   (+ optional norm byte for RQ-L2)
```

Big-endian `listID` makes all members of a cell adjacent; appending `label` gives a stable secondary
order *inside* the cell (useful for merge-intersecting against a pre-filter's label set). A query is
then literally:

```go
for _, listID := range nprobeNearestCells(q) {        // few; from the in-RAM coarse centroids
    c.Seek(u32be(listID))                              // one descent
    for c.Valid() {
        k, _ := c.Key(); if listPrefix(k) != listID { break }
        code, _ := c.Value()
        dist := adc(lut[listID], code)                 // M lookups + adds, code-only
        heap.push(label(k), dist)
        c.Next()                                       // sequential, page-local
    }
}
// then re-rank heap's top k_factor·k by exact :vec distance
```

`nprobe` cells = `nprobe` independent `Seek`+sequential-`Next` scans. No dependent-read chain; the
cells to scan are known up front from the coarse centroids, so the pager can prefetch and every code
in a leaf page is consumed before moving on. This is the access pattern the pager was built for, and
it is *exactly* what HNSW cannot offer.

**Hot RAM set collapses too.** HNSW wants the whole graph adjacency resident for speed (the Hybrid-L0
mirror). IVFPQ needs only the coarse centroids hot: `nlist·D·4` bytes — e.g. `nlist=4096, D=768` →
~12 MB, **independent of N**. The PQ codes (96 B/vec) are small enough that even 10M vectors is ~960 MB
on disk and streams through the page cache a leaf at a time. Optionally mirror the codes in RAM later
(a flat `N·M`-byte slab, pointer-free) for a pure-RAM scan — but that's an optimization, not a
correctness dependency, and it's *vastly* simpler than mirroring a mutable graph (no adjacency, no
`:l0log`, no version ring — just an append-mostly byte slab keyed by label).

This is what real disk-first systems converge on: pgvector IVFFlat walks a per-list page chain
sequentially ("basically sequential" vs HNSW's "more random"); **Lance/LanceDB IVF_PQ** stores exactly
`{_rowid: uint64, __pq_code: list<uint8>}` per row with one centroid per partition; SPANN keeps
centroids in RAM and posting lists on SSD and beats DiskANN ~2× *precisely* on the sequential-vs-random
gap. Our `key = listID||label, value = pq_code` is a faithful btree projection of the Lance model.

---

## 4. Proposed on-disk design for any-store

Mirrors the existing `internal/vindex` conventions (namespace = `prefix + suffix`, each a
`*btree.Namespace`; little-endian scalars; `meta` is one record; see `codec.go`/`hnsw.go`).

### 4.1 Namespaces

| Namespace | Key | Value | Notes |
|---|---|---|---|
| `:meta` | fixed `"m"` | index params + IVFPQ params + maintenance counters | extend the existing `meta` record |
| `:cb`   | small fixed keys | coarse centroids, PQ codebooks, OPQ rotation | the trained model; read once, cached in RAM |
| `:cell` | `listID(be32) ‖ label(be32)` | `pq_code[M]` (+ norm byte for RQ-L2) | **the inverted lists — the range-scan namespace** |
| `:vec`  | `label(be32)` | full f32 / int8 vector | **reused as-is** — the re-rank store |
| `:lbl` / `:doc` | as today | label ↔ docid mapping | **reused as-is** |

`:cb` holds: `nlist·D` floats of coarse centroids, `256·D` floats of PQ codebook, optional `D·D` OPQ
rotation. Total for `D=768, nlist=4096`: ~12 MB + ~0.8 MB + ~2.4 MB ≈ 15 MB — loaded once at
`Open`, cached as a plain RAM struct on the index handle (it only changes on a retrain, which bumps a
generation counter so peers reload it — reusing the *exact* staleness mechanism described next).

### 4.2 Meta additions (extends `codec.go`'s `meta`, append-only for back-compat)

```
algo        uint8     // PQ | OPQ+PQ | anisotropicPQ | RQ
nlist       uint32
M, nbits    uint16,uint8
cbGen       uint64    // bumped on retrain; peers reload :cb when their cached gen != this
// maintenance accounting (Ada-IVF, §6)
listSizes   -> kept in :cb or a side record: per-list count + running centroid + reconstruction-err accum
```

`cbGen` plays the same role for the codebook that the existing `l0Gen` plays for the HNSW mirror, and
the same role `SchemaCookie`/`FileChangeCount` already play cross-process (see `PLAN_HYBRID_L0.md` §1):
a reader that finds `meta.cbGen` ahead of its cached generation reloads `:cb` at its snapshot. Because
`:cb` and the codes are all in the btree and read at the tx snapshot, **there is no load-bearing RAM
state to keep consistent across processes** — the property the all-btree HNSW design prizes is
*preserved*, and unlike the Hybrid-L0 mirror we don't have to give it up for speed (the codes are
already small and sequential).

---

## 5. The three data paths

### 5.1 Build (index creation on a populated collection) — cheap and parallel

1. **Train** (once): sample `30·nlist … 256·nlist` vectors (FAISS guidance; warn/clamp below the
   floor), run k-means → `nlist` coarse centroids; compute residuals on the sample → train the `M`
   PQ sub-codebooks (k-means per subspace, or the anisotropic-weighted update); optionally fit OPQ.
   Write all to `:cb`, set `cbGen`.
2. **Assign + encode** (single pass, embarrassingly parallel — reuse `bulk_parallel.go`'s worker
   pool): for each vector, find nearest coarse centroid (one `nlist·D` scan, SIMD), compute residual,
   PQ-encode to M bytes, emit `:cell[listID‖label] = code`, and keep `:vec[label]` as today for
   re-rank.
3. Bulk-write the sorted `:cell` records. Because keys are `listID‖label`, the build can sort by key
   and stream them in — turning index build into a sequential btree load.

This is *cheaper than HNSW build* (no `efConstruction` search per insert; it's train-once +
single-pass assign) and parallelizes perfectly. The existing parallel-build + one-shot-write machinery
(`docs/vector-search.md` §1) maps straight onto it.

### 5.2 Search — coarse → LUT → range scans → re-rank

As in §3's snippet: compute query residual per probed cell, build the `M·256` LUT (reuse SIMD for the
`256·D` table build), scan the `nprobe` cells via cursor range scans scoring code-only, keep a top
`k_factor·k` heap (reuse `heap.go`), then re-rank survivors with exact `:vec` distances (reuse
`distance.go`). `nprobe` and `k_factor` are the two query dials (analogous to today's `EfSearch` /
`VectorEf`), and they auto-size from `Offset+Limit` exactly like the current candidate-list sizing in
`docs/vector-search.md` §3.

### 5.3 Insert / delete / update — cheap, with deferred maintenance

- **Insert**: assign to nearest *existing* coarse centroid (no retrain), PQ-encode residual, write
  `:cell` + `:vec`. O(nlist·D) for the assignment, one btree insert. Bump per-list count and the
  running reconstruction-error accumulator in meta.
- **Delete**: tombstone. Either delete the `:cell[listID‖label]` record outright (we *know* its
  listID, unlike pgvector which scans) or flip a validity bit — outright delete is cleaner here since
  the key is known. Decrement the list count. No graph repair (HNSW's costly 3-pass delete is gone).
- **Update**: delete + re-insert (may land in a different cell). Same as today's reindex-on-replace.

Inserts never move centroids, so the partition slowly drifts — which is the one real cost of IVF and
the subject of §6.

---

## 6. The hard part: maintenance under churn (centroid drift)

IVF's read-side simplicity is paid for here. As inserts/deletes accumulate, two measurable quantities
rise: **partition imbalance** (σ of list sizes → longer, uneven scans) and **reconstruction error**
(mean vector-to-assigned-centroid distance → must probe more cells for the same recall). The naive fix
is pgvector's "REINDEX the world," which at scale is prohibitive. We should adopt the **Ada-IVF**
(Mohoney et al., 2024) *local* maintenance model, which fits an embedded write-then-read DB well:

- Keep per-list metadata in `:cb`/meta: `{size, running centroid μ, reconstruction-error accum, read
  "temperature"}`.
- After a write batch, compute a **local indicator** per list `f(imbalance, error)` scaled by
  temperature; lists over a threshold are *violators*.
- **Local reindex** a violator: split it with balanced k-means to target size, optionally merge with
  its nearest neighbour lists to cut error — touching only a local region, **not** the whole index.
  This is just another write tx by the single writer, so it serializes and snapshots like everything
  else (same property the Hybrid-L0 compaction relies on).
- **Temperature** (heat on read, cool otherwise) means cold partitions that are never queried don't
  get reindexed — Ada-IVF found 80% of updates hit never-searched partitions, so skipping them is most
  of the win (~2–5× update throughput over rebuild-style schemes).
- A **global fail-safe** indicator (overall σ / error past a hard threshold, or `cbGen` age) triggers a
  full retrain + reassign — the IVFPQ analogue of today's `CompactVectorIndex`, runnable in a
  maintenance window or auto-fired like `CompactRatio`.

This is strictly more design than HNSW tombstone-compaction, and it is the main risk/effort of the
whole approach — but it is well-trodden (LIRE, DeDrift, Ada-IVF) and degrades gracefully (a drifted
index loses *recall*, recoverable by raising `nprobe`, never *correctness*).

---

## 7. What we reuse vs build new

**Reuse (large):** `:vec`/`:lbl`/`:doc` namespaces and codecs; int8 quant (`quant.go`) for both the
re-rank store and an optional RAM code-mirror; SIMD distance kernels (`distance.go`) for re-rank,
coarse assignment, and LUT build; the parallel bulk builder (`bulk_parallel.go`); the top-k heap
(`heap.go`); the `meta` record + generation-counter + cross-process staleness machinery
(`PLAN_HYBRID_L0.md` §1, `cbGen` ↔ `l0Gen`/`FileChangeCount`); the `Find()` integration, `_distance`
decoration, and auto-sizing of the candidate list (`docs/vector-search.md`).

**Build new (focused):** k-means + PQ/OPQ/anisotropic codebook training (the only genuinely new math,
pure Go, no exotic SIMD needed — closed-form argmin encode if we stay in the PQ/anisotropic family);
the `:cb` and `:cell` namespaces + codecs; the LUT/ADC scan loop; the Ada-IVF maintenance controller.

**Crucially, no btree-engine change is needed** — the cursor range-scan API already exists.

---

## 8. Head-to-head with the current HNSW index

| Axis | HNSW (current) | IVFPQ (proposed) |
|---|---|---|
| Read access | random point-lookup per neighbour per hop; *dependent* reads | `nprobe` contiguous range scans; *independent*, known up front |
| Btree behaviour | ~79% CPU in tree descent (measured); needs Hybrid-L0 mirror to fight it | sequential, page-local; pager-friendly by construction |
| Hot RAM for speed | whole graph adjacency (`~M·N·4`) | coarse centroids only (`nlist·D·4`, *independent of N*) |
| Per-vector storage | full `:vec` + `:adj` (degree·4) | `:vec` (re-rank) + M-byte code (e.g. 96 B) |
| Build | incremental, ~`O(N·logN·efC)`, search-per-insert | train-once + single-pass parallel assign |
| Insert | natural incremental | assign-to-nearest + append (cheap) |
| Delete | tombstone **+ graph repair** | tombstone / direct delete (key known); **no repair** |
| Recall (in-RAM) | higher (graph wins when resident) | needs re-rank to close the gap |
| Recall (disk / RAM-tight) | degrades as graph spills | designed for it — compact codes + sequential reads |
| Filtered search | greedy traversal fights predicates | list-then-code structure interleaves with filters / label sets |
| Main weakness | btree-hostile reads; mirror complexity for speed | centroid drift → needs Ada-IVF maintenance |
| Cross-process consistency | hard (Hybrid-L0 plan) — load-bearing RAM mirror | easy — codes live in btree, only tiny advisory `:cb` cache |

The honest summary: **in a pure-RAM index HNSW wins recall/QPS** (ann-benchmarks reflects this). But
any-store's index is *btree-resident by design*, which is HNSW's worst case and IVFPQ's best case. The
trade we're making is **"give up a few points of raw recall (recoverable via re-rank + `nprobe`) in
exchange for a read pattern the storage engine likes, a hot set independent of N, and the elimination
of the load-bearing cross-process RAM mirror."**

---

## 9. Recommended path & phasing

Algorithm choice: **IVFPQ with residual encoding**, codebooks trained plain-PQ first then upgraded to
**ScaNN-style anisotropic** (our metric is cosine/dot); **OPQ** as an optional rotation; **RQ** parked
as a later recall-per-byte upgrade. Always with an **exact re-rank** stage over `:vec`.

- **Phase 0 — kernels, no storage. ✅ DONE.** Pure-Go k-means, PQ encode/decode, ADC LUT + scan, exact
  re-rank, all in `internal/vivf`, tested against brute force / ground truth on the real `/tmp/vbench`
  embedding set. See results below.
- **Phase 1 — btree-resident build + search. ✅ DONE.** `internal/vivf` now has a btree-resident
  `StoreIndex`: `:cb` (codebooks) + `:cell` (`listID‖label → pq code`) namespaces, `:vec`/`:lbl`/`:doc`
  for re-rank and label maps. `BulkBuild` trains + writes in one pass; `SearchCandidates` does the
  cursor range-scan + ADC + bounded exact re-rank; `Insert`/`Delete` maintain it incrementally
  (physical delete, no tombstones). Wired into the collection as **`VectorModeIVFPQ`** (`index.go`),
  dispatched in `vector_index.go` (build/open/insert/delete/update/search/stats), and search plugs into
  the **existing `qplanner.VectorQuerySpec.Search` closure** so the normal FilterIter→SortIter→LimitIter
  pipeline finishes the query unchanged. Validated end-to-end through `Find()` (`vector_ivfpq_test.go`):
  recall@10 = 1.000 on realistic queries, residual metadata filters, `_distance` ordering, update/delete,
  and reopen-persistence; race-clean. Store-level recall on the real export = **0.963 @ nprobe=16**
  (`internal/vivf/store_test.go`), matching the prototype. Remaining: benchmark btree-reads/query vs HNSW
  on the `vector_rps_test.go` harness, and an int8 re-rank store.
- **Phase 2 — incremental insert/delete + drift accounting. ✅ DONE.** Assign-on-insert, *physical*
  delete (no tombstones — so no HNSW-style compaction needed), and **cheap drift detection**: the
  reconstruction error `‖x−nearestCentroid‖²` is computed for free during `Insert`, so a running mean
  vs the build-time baseline (`reconBase`) tells us how well the frozen codebooks still fit the data;
  a churn counter backstops the delete-heavy case. `DriftScore = max(reconRatio−1, churnRatio)` (O(1),
  one meta read; the recon signal gated behind ≥10% inserted so outliers can't trip it). The global
  fail-safe full-retrain is `vivf.Rebuild` (re-train from live `:vec`, rescale nlist), wired to the
  **existing auto-maintenance machinery**: IVF reuses `VectorParams.CompactRatio` as the drift
  threshold, `vectorIndex.overThreshold` = `DriftScore ≥ CompactRatio`, `vectorIndex.compact` =
  `Rebuild`, and `Collection.CompactVectorIndex` triggers it manually. (Required one fix: `Insert` now
  also calls `maybeAutoCompactVectors`, since for IVF — unlike HNSW — inserts are what cause drift.)
  Tested: drift score rises on a distribution shift, auto-rebuild recovers recall a frozen index loses
  (0.67 → 1.00, `vector_ivfpq_test.go`), `Rebuild` clears drift and restores store-level recall
  0.58 → 1.00 (`internal/vivf/drift_test.go`).
- **Search-path allocation pass. ✅ DONE.** `SearchCandidates` now allocates ~3 (result + cursor)
  instead of ~339/query: a pooled `searcher` (sync.Pool, mirroring `internal/vindex`) holds all
  scratch (normBuf/qr/lut/cell buffers); the closure-dedup `map[uint32]float32` is replaced by a
  map-free flat open-addressing min-map with O(1) generation reset (`u32fmap`, the value-carrying
  sibling of vindex's `u32set`); per-candidate `:vec`/`:lbl` reads use `rtx.AppendValue` into reusable
  buffers instead of allocating `Get`; result docIDs pack into one backing slice; cell-boundary check
  is `bytes.Equal`; `sqNorm` is `vek32.Dot`. Microbench: **339 → ~6 allocs/op, 136 KB → 7 KB/op**.
  The final closest-first sort is kept *inside* `SearchCandidates` (with `spec.Ordered`) rather than
  delegated to the pipeline `SortIter` — measured ~19% faster end-to-end, since the planner then skips
  the SortIter for the default distance order and streams to `LimitIter`.

- **Search-path latency pass (pprof-driven). ✅ DONE.** A CPU profile of the search loop found three
  costs: full-sorting the candidate set, the per-cell ADC LUT rebuild, and the btree cell scan.
  Fixes: (1) **quickselect** (Hoare, median-of-three) to partition out the ef best in O(n) instead of
  sorting thousands to take ~100; (2) **IVFADC precomputed tables** — the exact decomposition
  `‖q−c−r̂‖² = ‖q−c‖² + Σ_m[‖cb_mj‖²+2c_m·cb_mj] − 2Σ_m q_m·cb_mj` precomputes the cell term
  (`precomp[cell][m][j]`, RAM-gated at 64 MiB; large indexes fall back to per-cell sqL2), so a query
  builds one `−2·q_m·cb` table and each cell's LUT is a cheap add instead of m·256 sqL2s (a test
  asserts both paths return identical results). Net **~540 → ~320 µs/query (−40%)**, recall bit-identical.
  The btree cell scan (the remaining ~⅓) is the engine's cost and the inherent IVF range-scan work —
  left to the btree, unchanged.

- **Phase 3 — Ada-IVF local maintenance.** Local split/merge of hot violator lists; bound drift
  without full rebuilds; the IVFPQ analogue of `CompactRatio`.
- **Phase 4 (optional) — recall/perf upgrades.** Anisotropic codebooks; OPQ rotation; an in-RAM
  pointer-free code slab for pure-RAM scans; RQ for tiny code budgets.

Each phase is independently shippable and falls back cleanly (a missing/over-drifted IVF index can
always brute-force or defer to the existing HNSW mode).

### Phase 0 results (validated, `internal/vivf`)

Measured on the same real export the current HNSW index is benchmarked on
(`/tmp/vbench`: 38463 base × 768d, 500 queries, cosine top-10 ground truth where **HNSW scores
0.970**, `results_anystore.csv`). Self-excluded, scored against `gt.i32`.

| Config | recall@10 |
|---|---|
| brute-force cosine (float32 ceiling) | 0.989 |
| **IVF-PQ M=96, nlist=256, nprobe=64, kFactor=10** | **0.966** |
| IVF-PQ M=96, nlist=512, nprobe=64, kFactor=10 | 0.955 |
| IVF-PQ M=48 (48-byte codes), best | 0.901 |
| IVF-PQ M=96, nprobe=64, **kFactor=1 (no re-rank)** | 0.707 |

Conclusions:

1. **Recall parity with HNSW is achievable** — 0.966 vs 0.970, i.e. ~97.7% of the 0.989 float32
   ceiling, at 32× compression (96-byte codes vs 3072-byte f32).
2. **Re-rank is load-bearing** — without it (kFactor=1) recall tops out at 0.71; the exact re-rank over
   `:vec` survivors is what closes the gap. Confirms §2.5.
3. **M=96 (dsub=8) is the knee** — M=48 caps ~0.90; M=192 doesn't beat M=96.
4. **The nprobe knee, and how it was closed.** Plain PQ + random init needed nprobe=64/256 (~25% of
   cells) for parity. Adding **k-means++ init** and, decisively, **closure / multi-assignment** (SPANN
   §2: place each vector in its `Assign` nearest cells with a per-cell residual code) pushes parity to
   **nprobe=16** (closure=4 → 0.965@16, 0.947@8):

   | init / closure | nprobe=8 | nprobe=16 | nprobe=32 | nprobe=64 | entries/vec |
   |---|---|---|---|---|---|
   | random, assign=1 | 0.854 | 0.913 | 0.945 | 0.967 | 1.0 |
   | k-means++, assign=1 | 0.853 | 0.910 | 0.951 | 0.971 | 1.0 |
   | k-means++, assign=2 | 0.914 | 0.944 | 0.965 | 0.976 | 2.0 |
   | **k-means++, assign=4** | **0.947** | **0.965** | 0.976 | 0.982 | 4.0 |

   Closure trades disk (4× the M-byte codes — still tiny vs the f32 re-rank store) for **4× fewer
   cursor seeks and 4× fewer per-cell LUG builds at equal recall** — i.e. it makes the btree read
   pattern *even better*, not just the recall. This is a strong argument to make closure a first-class
   build parameter in the btree index.

5. **OPQ / ScaNN deferred deliberately.** With exact re-rank neutralizing most PQ-quality loss, the
   residual gap to the 0.989 ceiling is dominated by IVF *coverage* (nprobe), which closure addresses
   directly — not by PQ *fidelity*, which is what OPQ/anisotropic improve. They remain future levers
   (a pure-Go Jacobi eigensolver for OPQ) but are low-value until coverage is exhausted.

---

## 10. Open questions / risks

- **Recall parity.** Will IVFPQ + re-rank match the current HNSW recall at acceptable `nprobe`? Phase 0
  must answer this on real data before any storage work. The literature says yes in the disk regime;
  verify on our embeddings.
- **Drift management cost.** Ada-IVF is the riskiest sub-system. Start with the simple global-retrain
  fail-safe (Phase 2) and only build local reindex (Phase 3) if churn workloads need it.
- **Training data on small collections.** `nlist` must scale down gracefully for 10K-vector indexes
  (few hundred lists, clamp to the points-per-centroid floor); below some N, just use the existing
  `BruteForce` mode.
- **Coexistence.** Ship IVFPQ as a new `VectorMode` alongside HNSW rather than replacing it, so the two
  can be A/B'd on the same data and HNSW remains for RAM-resident / highest-recall use.

---

## Sources

Synthesized from FAISS docs/wiki and primary papers, validated against real disk-first systems:

- Jégou, Douze, Schmid — *Product Quantization for Nearest Neighbor Search* (TPAMI 2011) — PQ/ADC/IVFADC.
- Ge, He, Ke, Sun — *Optimized Product Quantization* (CVPR/PAMI 2013) — OPQ rotation, +4–5% recall@10.
- Guo et al. — *Accelerating Large-Scale Inference with Anisotropic Vector Quantization* (ICML 2020) — ScaNN.
- Babenko/Lempitsky (AQ), Martinez et al. (LSQ++ ECCV 2018) — additive/residual quantization.
- Chen et al. — *SPANN* (NeurIPS 2021) — centroids-in-RAM / posting-lists-on-SSD; balancing, closure, pruning.
- Mohoney et al. — *Ada-IVF: Incremental IVF Index Maintenance for Streaming Vector Search* (2024) — drift/maintenance.
- Subramanya et al. — *DiskANN/Vamana* (NeurIPS 2019) — the graph-on-SSD contrast case.
- FAISS wiki: *Guidelines to choose an index*, *The index factory*, *Additive quantizers*, *Faiss building blocks*.
- Real systems surveyed for on-disk layout: **pgvector IVFFlat** (per-list page chains), **Lance/LanceDB
  IVF_PQ** (`{_rowid, __pq_code}` + one centroid/partition), Milvus segments, sqlite-vec chunks.
- FAISS source mirrored at `/tmp/faiss-ref` for implementation reference (`IndexIVFPQ`, `ProductQuantizer`,
  `ResidualQuantizer`, OPQ).

(Per-agent source URL lists are in the research transcripts that produced this synthesis.)
