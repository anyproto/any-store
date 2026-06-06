# Plan: pluggable vector index modes — btree / hybrid / brute-force

Status: **design**, branch `feat/vector-hybrid-l0`.
Supersedes nothing; the hybrid internals are specified in `PLAN_HYBRID_L0.md` and are
executed as **Phase 2** here.

## Goal

Make the vector index strategy a first-class, per-index **mode**, so a user picks the
storage/latency/RAM trade-off that fits their collection:

| Mode | Index storage on disk | Write cost | Search cost | Extra RAM | Recall | Best for |
|---|---|---|---|---|---|---|
| **a. btree** (current) | full HNSW graph + vectors in btree namespaces | high (graph maintenance, read-heavy insert) | ~ANN, btree-read-bound | ~0 (btree page cache only) | approx | large sets, low RAM, multiprocess writes |
| **b. hybrid** | same as btree (btree = source of truth) | same writes + RAM mirror update | ~ANN, layer-0 reads served from RAM | layer-0 mirror (≈128 MB/1M @ M0=32) + optional int8 vec tier | approx (== btree) | read-heavy, RAM available, want max search RPS |
| **c. brute-force** | **none** (index is metadata-only: a declaration that a field is a vector) | **~0** (nothing to maintain) | O(N): scan docs, exact top-k | ~0 (optional int8 vec cache, see §4) | **exact (100%)** | small/medium sets, max write throughput, min storage, or as the recall ground-truth |

Invariants across all modes (unchanged from today): **no btree-engine changes**;
multiprocess-safe; `-race` clean; default mode = btree (full back-compat for existing
indexes whose metadata has no `mode` field).

## The four phases (executed in order; commit per phase)

- **Phase 1 — modes scaffolding & backend abstraction.** The `VectorMode` enum, public
  API, persistence, validation, Stats, and a `vectorBackend` interface so the three modes
  are pluggable behind the existing query-pipeline integration. Only btree is real;
  hybrid routes to btree (no mirror yet), brute is implemented minimally (it's small).
- **Phase 2 — hybrid.** Implement the RAM layer-0 mirror per `PLAN_HYBRID_L0.md`
  (its sub-phases 0→4). The hybrid backend = btree backend + mirror.
- **Phase 3 — brute-force.** Flesh out the metadata-only backend + scan search (filter
  pushdown, exact top-k), incl. the optional int8 RAM vector cache.
- **Phase 4 — unified bench & comparison incl. RAM.** One harness sweeping
  mode × quantization × per-tx/batched, reporting RPS (insert/update/delete/search),
  recall (vs brute ground truth), on-disk size, and **heap RAM delta**, on local + p14 + hp.

---

## Phase 1 — modes scaffolding & backend abstraction

**Public API** (`index.go`):
```go
type VectorMode uint8
const (
    VectorModeBTree      VectorMode = iota // default; full HNSW in btree
    VectorModeHybrid                       // HNSW + RAM-resident layer 0
    VectorModeBruteForce                   // metadata-only; scan documents, exact
)
func (m VectorMode) String() string
// VectorParams gains:
//   Mode VectorMode `json:"mode,omitempty"`
```

**Persistence** (`db.go`): write/read `mode` int alongside `quant` in the index metadata
(`registerIndex` ~`:994-1003`, `getIndexInfos` ~`:915-925`). Absent ⇒ `VectorModeBTree`.

**Backend abstraction** (`vector_index.go`): replace the bare `ix *vindex.Index` field
with a backend:
```go
type vectorBackend interface {
    Insert(wtx *btree.WriteTx, docID []byte, vec []float32) error
    Update(wtx *btree.WriteTx, docID []byte, oldVec, newVec []float32) error
    Delete(wtx *btree.WriteTx, docID []byte) (bool, error)
    // Search returns docID+distance hits. ef is advisory (ignored by brute).
    // filter (may be nil) lets a backend push residual filtering down (brute uses it).
    Search(rtx *btree.ReadTx, query []float32, k, ef int, filter ...) ([]vindex.Hit, error)
    Stats(rtx *btree.ReadTx) (vindex.Stats, error)
}
```
- `btreeBackend` — thin wrapper over `*vindex.Index` (today's behaviour, zero functional
  change).
- `hybridBackend` — Phase 2; in Phase 1 it is constructed identically to btree (alias) so
  the mode is selectable and persists, but behaves exactly like btree until the mirror
  lands. This lets Phase 1 ship and be tested without the mirror.
- `bruteBackend` — holds the collection's doc namespace handle + field path + dim/metric;
  Insert/Update/Delete are no-ops; Search scans (Phase 3 fleshes it out; Phase 1 may land a
  correct-but-simple scan to make the mode end-to-end testable immediately).

Dispatch: `createVectorIndex`/`loadVectorIndex` (`vector_index.go:145-215`) choose the
backend from `info.Vector.Mode`. The brute backend needs the collection's `c.ns` and field
path, so its constructor takes them (the others ignore them).

**Validation** (`validateVectorParams`, `:50`): brute ignores M/EfC/EfS/Quantization
(reject or warn if set, TBD); hybrid accepts all; reject unknown mode values.

**Stats** (`stats.go`): `VectorIndexStats` gains `Mode string`. Brute reports zero
Vector/Graph/Mapping/Meta bytes (metadata-only) — making the storage win measurable.

**Exit criteria:** all three modes selectable, persist across reopen, reported by Stats;
btree behaviour byte-identical to before; brute returns exact top-k on a small set; full
suite green with and without `-race`. Commit.

---

## Phase 2 — hybrid (see `PLAN_HYBRID_L0.md`)

Implement the RAM layer-0 mirror and cross-process sync exactly as specified there
(sub-phases 0→4): `l0Gen` in `:meta`; `:l0log` delta log; COW atomic-pointer publish;
snapshot-alignment contract; compaction; optional int8 vector tier. The `hybridBackend`
embeds `btreeBackend` (writes go to btree first = source of truth) and adds the mirror for
layer-0 search. Truth-equivalence test: hybrid hits == btree hits, exactly, at every
snapshot. Multiprocess convergence test. `-race`. Commit per hybrid sub-phase.

---

## Phase 3 — brute-force

- **Storage:** none. The index "record" is purely the metadata declaration (kind=vector,
  mode=brute, field, dim, metric) persisted in `_system`. No vindex namespaces created.
- **Writes:** Insert/Update/Delete are no-ops (the vector already lives in the document).
  So brute has the *cheapest possible* write path — effectively free index maintenance,
  the headline number for the comparison.
- **Search:** scan the collection's document namespace; for each doc, extract the vector
  field (`extractVector`), compute distance, maintain a top-k min-heap. Apply the residual
  **filter inline during the scan** (brute is exact, so no over-fetch needed — this avoids
  the `chooseEf` over-fetch dance entirely). Inject `_distance` as today.
- **Pipeline integration:** when the planner sees a vector query on a brute index, it emits
  a scan-distance iterator instead of an HNSW `VectorIter`. Keep the same `iter.Distance()`
  / `_distance` surface so callers don't change.
- **Optional int8 RAM vector cache:** brute is read-bound on *document* reads (full docs,
  which include text + metadata, not just the vector). An opt-in RAM cache of int8 vectors
  (keyed by docId→vec) turns the scan into a tight in-RAM distance loop — bridging toward
  hybrid's vector tier without any graph. Gated by a memory cap; off by default to honour
  "no data to save". Decide in Phase 3 whether to land this now or defer.
- **Tests:** brute recall == 100% by construction; cross-check that btree/hybrid recall is
  measured *against brute as ground truth*; filter pushdown correctness; empty/边 cases.
  Commit.

---

## Phase 4 — unified bench & comparison (incl. RAM)

Extend `vector_rps_test.go` into a **mode × quantization** matrix (keep per-tx/batched and
the real-disk `ASV_RPS_DIR` / `ASV_RPS_*` controls):

Axes: mode ∈ {btree, hybrid, brute} × quant ∈ {none, int8} (quant N/A for brute unless the
int8 cache lands).

Report per cell:
- **RPS:** insert / update / delete (per-tx and batched) / search.
- **Recall@k:** vs brute-force exact ground truth (brute itself = 1.0).
- **On-disk size:** docs + index namespaces (brute index ≈ 0).
- **RAM delta:** `runtime.GC(); runtime.ReadMemStats` before/after building the index,
  reporting HeapInuse delta — isolates the resident index structures (hybrid mirror,
  optional brute cache) from the shared btree page cache. Also log the analytic mirror size
  for hybrid as a cross-check.
- WAL flushed (CheckpointTruncate) before the search measurement (already in place).

Run on **local + any@p14 + any@hp** (real-disk `$HOME`), as before. Produce a comparison
table + a short written conclusion (which mode to pick when).

---

## Cross-cutting rules (all phases)

1. **No btree-engine changes.** Verified: namespaces via `CreateNamespace`, staleness via
   the already-public page-1 counters, cursors/Delete for log+compaction.
2. **Back-compat.** Missing `mode` ⇒ btree. Existing indexes keep working untouched.
3. **Multiprocess + consistency.** btree unchanged; hybrid mirror is snapshot-aligned and
   derived-from-btree (see `PLAN_HYBRID_L0.md` §3); brute reads live docs at the tx snapshot
   so it is trivially consistent and multiprocess-safe.
4. **`-race` clean** every phase; **commit per phase** (and per hybrid sub-phase).
5. **Brute and hybrid never become the source of truth.** btree (or, for brute, the
   documents themselves) is always authoritative; RAM structures are caches/derivations.
