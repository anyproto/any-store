# Plan: landing a btree-resident vector index in any-store

Branch `btree-vector-search`. This is the integration plan for turning the
experimental `vector` package into a real any-store index type, using the
**full-btree (Option B)** design: the HNSW graph lives entirely in btree
namespaces and is traversed/mutated through transactions. No per-process
in-memory graph.

## Why full-btree (the decision)

any-store is multi-process: multiple OS processes open the same DB and read/write
through the SQLite-derived btree (WAL + MVCC snapshots + cross-process locks).

- An **in-memory HNSW** (Option A) would be a per-process cache of the graph.
  When another process commits an insert, every other process's in-memory
  layer-0 + adjacency is silently stale — the same class of bug the index
  **sketch staleness** machinery already exists to paper over, but far worse
  (a stale graph returns wrong neighbours, not just wrong cost estimates).
- A **btree-resident graph** has no such layer: the graph *is* committed btree
  data. A reader's MVCC snapshot sees a consistent committed graph; a writer's
  HNSW mutations commit **atomically with the document** in the same write tx.
  Cross-process correctness, crash-safety, and read-your-writes come for free
  from the engine — nothing to invalidate or sync.

Measured cost (OPTION_B.md / CROSS_HARDWARE.md): paged search is ~1.7–6× the
in-memory arena warm, less on slow CPUs; the hybrid (quantized routing) recovers
most of it later. Fast enough, and the reliability win is decisive.

## Where it plugs in (existing machinery)

The current indexes already do exactly what we need — they live in btree
namespaces and mutate inside the collection write tx. We mirror that:

| existing (`index`) | vector index |
|---|---|
| namespace `ix:<coll>:<name>` | namespaces `v*:<coll>:<name>` (below) |
| `Index` interface (`Info`, `Len`) | same interface, `IndexInfo.Kind = Vector` |
| `insertKeys(tx, it)` @ `collection.go:331` | `vectorInsert(tx, it)` — HNSW insert |
| `deleteKeys(tx, it)` @ `collection.go:541` | `vectorDelete(tx, it)` — tombstone |
| update = delete+insert @ `collection.go:460` | same: tombstone old + insert new |
| `CreateIndex` builds from existing docs | same: scan docs, HNSW-insert each |
| `c.loadIndexes()` CoW snapshot | unchanged (vector index is just another entry) |

No new transaction plumbing: the write hooks already run inside `*btree.WriteTx`,
and search runs inside `*btree.ReadTx` via `db.doReadTx`.

## On-disk layout (namespaces)

Keyed by a dense `uint32` **label** (HNSW needs dense ids; documents are keyed by
arbitrary `[]byte`). Vector and adjacency are **split** (pgvector/Lucene do the
same) so the write-heavy adjacency updates don't rewrite the immutable vector
(DELETE_UPDATE.md measured ~4× write-amp saving):

```
vmeta:<coll>:<idx>   1 record  → dim, metric, M, M0, Ml, efConstruction,
                                 efSearch, entryLabel, topLayer, count,
                                 nextLabel, deletedCount, version
vvec:<coll>:<idx>    label → raw vector (dim×f32, LE)            [immutable]
vadj:<coll>:<idx>    label → level + deleted-flag + per-layer neighbour labels  [churns]
vdoc:<coll>:<idx>    docId(bytes) → label                        [for delete/update by id]
```

- Label is big-endian so a cursor over `vadj`/`vvec` is in label order (bulk
  ops, compaction).
- Tombstone = a flag bit in the small `vadj` record (cheap rewrite; the `vvec`
  record stays).
- `vdoc` removes the need for any in-memory id dictionary — the docId→label map
  is in the btree, hot entries cached by the page cache.

## Write path (inside the collection write tx)

`vectorInsert(tx, it)`:
1. Extract the vector field from `it` (the `IndexInfo` field path); skip if
   absent (sparse) or wrong dim.
2. Allocate `label = meta.nextLabel++` (transactional, monotonic).
3. Write `vvec[label] = vector`, `vdoc[docId] = label`.
4. HNSW insert operating **directly on the btree**: read `vmeta` for the entry
   point; greedy-descend the upper layers and run the efConstruction layer
   search, reading neighbour vectors via `tx.Get(vvec, …)` and adjacency via
   `tx.Get(vadj, …)`; select M neighbours; write this node's `vadj[label]` and
   `tx.Put` the updated adjacency of each touched neighbour (≈ M0 records).
5. If the node's level raised the top, update `meta.entryLabel/topLayer`. Always
   bump `meta.count`. `tx.Put(vmeta)`.

`vectorDelete(tx, it)`: look up `vdoc[docId]` → label; set the deleted flag in
`vadj[label]`; `meta.deletedCount++`. O(1) (tombstone). The node stays as a
navigation waypoint until compaction.

`vectorUpdate`: if the vector field changed, tombstone the old label + insert a
new one (HNSW-standard); otherwise no-op.

All of this is plain `tx.Get`/`tx.Put` on namespaces — the btree **writer cache**
absorbs the repeated reads of hot upper-layer nodes within the tx, so a single
insert's ~tens of record touches commit as one transaction.

## Read path (search)

```go
func (c Collection) VectorSearch(ctx, indexName string, query []float32,
        k int, opts ...) ([]VectorHit, error)   // VectorHit{ DocId []byte; Distance float32 }
```

Runs in `db.doReadTx`: read `vmeta` (entry point), traverse the graph reading
`vvec`/`vadj` records through the read tx (MVCC snapshot + page cache), skipping
tombstoned nodes, return the top-k labels → resolve to docIds via the node record
→ optionally hydrate full docs via the existing `FindId` path. `efSearch` and
`exact` (brute fallback = the `Brute` index, for tiny/over-filtered sets) are
options, mirroring MongoDB's `numCandidates` / `exact`.

Pre-filtering (combine a Mongo filter with the ANN walk) is a later enhancement;
v1 returns ANN hits and the caller filters, or uses `exact` for hard filters.

## Public API

Extend `IndexInfo` (additive, back-compatible):

```go
type IndexInfo struct {
    Name   string
    Fields []string
    Unique bool
    Sparse bool
    // new:
    Kind   IndexKind   // Range (default) | Vector
    Vector *VectorParams
}
type VectorParams struct {
    Field    string   // path to the embedding array field
    Dim      int
    Metric   Metric   // cosine | l2 | dot
    M, EfConstruction, EfSearch int   // sensible defaults
    Quantization Quantization          // none (v1) | scalar | binary (later)
}
```

`CreateIndex`/`EnsureIndex` dispatch on `Kind`; `VectorSearch` is a new
`Collection` method. (A `$vectorSearch` aggregation-style entry point can wrap it
later — see COMPARISON_mongodb.md for the target API shape.)

## Concurrency / MVCC (the reliability argument, concretely)

- **Writers**: any-store is single-writer (WAL write lock, cross-process). HNSW
  insert/delete run under that lock, serialized, committed atomically with the
  document. No torn graph.
- **Readers**: each `VectorSearch` opens a read tx → a frozen MVCC snapshot of
  the graph at a committed point. Concurrent readers across processes each see a
  consistent graph; a concurrent writer's changes are invisible until they
  commit and the reader takes a new snapshot. **Nothing to invalidate** — the
  Option-A in-memory-layer-0 sync problem simply does not exist.
- **Entry point / meta** live in `vmeta` and are read from the snapshot, so a
  reader never chases an entry the writer just moved.

## Build on an existing collection

`CreateIndex` with `Kind=Vector`: open a write tx, create the namespaces, scan
the collection's documents (cursor over the main namespace), and `vectorInsert`
each. This is the slow part (HNSW construction over paged storage — thousands/s);
for large collections it can be chunked into several write txs or run as a
background build that flips the index "ready" flag in `vmeta` on completion.

## Deletes, compaction, maintenance

- Deletes are tombstones (O(1)); they cost query latency, not correctness
  (DELETE_UPDATE.md: ~1.8× at 50% deleted).
- A **compaction** maintenance op (analogous to VACUUM / the Qdrant vacuum at
  `deletedCount/count > 0.2`) rebuilds the graph keeping live nodes, reassigning
  labels, in a write tx. Triggered manually first; auto-threshold later.
- Reuses `Compact`/`Rebuild` logic already prototyped in the package.

## Distance / SIMD

- amd64: `vek` (AVX2/AVX-512). Non-AVX2 / arm64: the unrolled scalar kernel
  (`DistanceFor()` already auto-dispatches via `vector.SIMD()`).
- **Quantization is the priority follow-up** (COMPARISON_mongodb / ARM): a
  scalar-int8 or binary routing slab cuts RAM/bandwidth 4–32× and makes the
  hybrid (route on quantized, page full vectors to rerank) viable — and on ARM,
  binary-Hamming maps to the native NEON `CNT`. Stored as an extra
  `vqvec:<coll>:<idx>` namespace; full vectors stay in `vvec` for rerank.

## Performance knobs & expectations

- `M`/`efConstruction` (build quality vs cost), `efSearch`/`numCandidates`
  (recall vs latency), page-cache size (the dominant factor for paged search —
  keep the hot graph in `pcache`).
- From the measurements: single inserts are a few ms; queries tens-to-hundreds of
  µs warm at dim 768; reload/open is instant (no graph to load). On a no-SIMD box
  everything scales by the distance-kernel gap — check `vector.SIMD()` per target.

## Phasing

1. **MVP**: `IndexInfo.Kind=Vector`, the four namespaces, write hooks, paged
   `VectorSearch`, build-on-create, tombstone delete, manual compaction. L2/cosine
   via vek+unrolled. No quantization, no planner integration.
2. **Quantization**: int8/binary routing slab + paged rerank (the hybrid); the
   big RAM/ARM win.
3. **Query integration**: a `$vectorSearch`-style stage and ANN+filter
   pre-filtering through the planner.
4. **Maintenance**: auto-compaction at a deleted-fraction threshold; background
   build.

## Open questions / risks

- **Insert latency on cold cache**: HNSW insert does many random reads; on a cold
  page cache the first inserts after open are slower. Mitigation: warm the upper
  layers, or keep the (tiny) upper-layer records pinned.
- **Write amplification** on insert (~M0 adjacency Puts) inflates WAL per commit;
  the vvec/vadj split keeps each Put small. Batch bulk inserts per tx.
- **Vector-record overflow**: dim≥~1000 vectors exceed a 4 KiB page → overflow
  chains (more page reads per `vvec` Get). Quantization or a larger page size
  helps; measure on real embeddings.
- **Endianness**: define `vvec` as little-endian explicitly (the prototype reads
  host-endian).
- **Planner**: vector search is not a range scan; keep it a distinct access path
  (separate API) rather than forcing it into the CBO, as MongoDB does with a
  separate `$vectorSearch` stage.
