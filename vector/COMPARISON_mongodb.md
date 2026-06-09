# Comparison: this spike vs MongoDB Atlas Vector Search

Branch `btree-vector-search`. Companion to [README.md](./README.md),
[DELETE_UPDATE.md](./DELETE_UPDATE.md), and
[COMPARISON_pgvector.md](./COMPARISON_pgvector.md).

MongoDB is the most relevant comparison for any-store's **external design**:
both are **document stores with a MongoDB-style query language** (any-store ships
`$in`/`$inc`/comparison/logical operators). So MongoDB is the natural template
for *how a vector index is exposed and queried* on a collection — even though,
as it turns out, its *internal* architecture is **further** from any-store than
pgvector's is.

Facts below are sourced from MongoDB's docs (`$vectorSearch`, vector-search-type,
automated-embedding, vector-quantization), the "now source-available" mongot
engineering post, Apache Lucene's `Lucene99HnswVectorsFormat`, and Elastic/Lucene
HNSW write-ups (see Sources).

---

## The headline: closest in API, furthest in architecture

| | This spike (in any-store) | MongoDB Atlas Vector Search |
|--|---------------------------|-----------------------------|
| Process model | **Single process, embedded** — index lives in the same engine as the data | **Two processes**: `mongod` (docs, WiredTiger) + **`mongot`** (vectors, Apache Lucene) |
| Index ↔ data sync | **Synchronous, same transaction** | **Asynchronous** — `mongot` tails the change stream / oplog |
| Consistency | **Read-your-writes** possible | **Eventually consistent** — a just-written doc isn't immediately searchable |
| Query | one engine, one hop | two-hop: `mongot` returns ids+scores → `mongod` fetches docs |

This is the crux. Atlas deliberately **disaggregates** vector search into a
separate Lucene process to keep indexing off the transaction commit path — at the
cost of eventual consistency. any-store, like pgvector-in-Postgres, is
**single-process and embedded**, so it can offer **synchronous, read-your-writes
vector indexing in the same write transaction** — a guarantee Atlas trades away.
For an embedded store this is a real, marketable advantage.

So: borrow MongoDB's **external API**, but keep any-store's **single-process,
transactional internals** (our in-memory arena, or the pgvector-style
btree-paged Option B from the pgvector comparison).

---

## External design — what any-store should borrow from MongoDB

This is the part the API should mirror, because users already think in this
shape:

- **Index definition on a field** — MongoDB: `{ type: "vector", path, numDimensions,
  similarity: euclidean|cosine|dotProduct, quantization: none|scalar|binary }`.
  any-store equivalent: a vector index type alongside its existing compound/unique
  indexes, declaring the vector field, dim, metric (we already have L2/Cosine/Dot).
- **A query stage, not a magic operator** — MongoDB's `$vectorSearch` aggregation
  stage: `{ queryVector, path, numCandidates, limit, filter, exact }`.
  - `numCandidates` is MongoDB's name for the HNSW `efSearch` knob (recommended
    ~10–20× `limit`). We expose `EfSearch` directly — same concept.
  - `exact: true` falls back to **ENN** (brute force) for guaranteed-correct
    results on small/filtered sets. **We already have `Brute` for exactly this** —
    it should be the `exact` path.
  - `filter` is a **pre-filter** (MQL predicate narrows candidates *before/within*
    the ANN walk, not after). any-store has the query/filter engine to do this;
    wiring pre-filtering into the graph walk is the interesting integration work.
- **`numDimensions ≤ 4096`** for an indexable vector (pgvector caps at 2000). We
  have no hard cap; matching ~4096 is reasonable.
- **Auto-embedding (preview/GA)** — MongoDB's `type: "autoEmbed"` indexes a *text*
  field and generates the embedding for you (via Voyage AI models), at both index
  and query time, storing vectors in an internal collection. any-store could offer
  an optional pluggable embedder hook so callers store text and query with text —
  a product decision, but the API shape (a `query` text field instead of
  `queryVector`) is worth keeping in mind so it can be added later without
  breaking the interface.

## Internal/storage — what's instructive (and what differs)

MongoDB's vectors run on **Apache Lucene's HNSW**, which has a notably different
internal model from our single mutable graph:

- **Many immutable per-segment graphs, not one mutable graph.** Each Lucene
  segment has its own HNSW graph (`.vec` raw vectors, `.vex` graph with
  delta-encoded neighbor ordinals, `.vem` metadata). New writes go to **new
  segments**; a query searches **every** segment's graph and merges the per-segment
  top-k. Background **merges** consolidate segments and reclaim deletes.
  - vs **this spike:** one mutable arena graph, in-place inserts, periodic
    `Compact`/`Rebuild`.
  - vs **pgvector:** one mutable on-disk graph, in-place inserts + VACUUM repair.
- **Vectors decoupled from the graph** (`.vec` vs `.vex`, via Lucene's
  `FlatVectorsFormat`). This is the **third independent vote** for the
  vector/adjacency storage split — pgvector does it (element vs neighbor tuple),
  Lucene does it (`.vec` vs `.vex`), and we [measured the ~4× write-amp win](./DELETE_UPDATE.md).
  any-store should split too.
- **Memory-mapped, OS-page-cache-resident (off-heap).** Lucene serves vector
  files via mmap and the OS page cache, not JVM heap — performance is
  **page-cache-bound** with graceful I/O degradation when the hot set exceeds
  cache, rather than OOM. This is the same residency model as pgvector's buffer
  cache and exactly the **Option B** any-store path (page the graph through the
  btree `pcache`).
- **Deletes = tombstones + filter-at-query + reclaim-on-merge** — the *same*
  philosophy as ours. Lucene flips a `liveDocs` bit; the deleted node **stays in
  the graph and is still traversed**, just not returned. Elastic measured **18–46%
  throughput loss at 50% deleted** — independent confirmation of our own
  ["latency, not recall" tombstone finding](./DELETE_UPDATE.md) (we measured
  ~1.8× slower at 50% tombstones). Merges that touch a segment with deletes must
  re-insert its vectors (can't seed the merge from a graph with holes) — the same
  cost we pay in `Rebuild`.

### Segment model vs single mutable graph — a real choice for any-store

The Lucene segment design has an interesting fit with any-store's **copy-on-write
btree**: append-only new segments avoid in-place mutation of a large structure,
which is friendlier to a CoW/WAL engine and gives instant write-visibility
(freshness via new segments) without rewriting existing pages. The costs are (a)
searching N graphs per query until merge, and (b) merge complexity. For a
single-node embedded store, our **single mutable graph + Compact** is simpler and
gives synchronous read-your-writes; the segment model is worth considering only
if write-amplification on the CoW btree (rewriting neighbor pages in place)
proves to be the bottleneck — at which point "append a small new segment, merge
in the background" is the Lucene-proven escape hatch.

## Quantization — the biggest lever we haven't pulled

MongoDB (via Lucene) keeps **quantized** vectors in the hot index and **full-
fidelity vectors on disk for rescoring**:

- **scalar int8** → ~3.75× less RAM; **binary (1-bit)** → ~24× less RAM, with a
  second-pass rescore recovering precision (binary+rescore: ~96% less memory, ~95%
  recall retained).
- The HNSW graph topology is unchanged; only the stored vectors shrink (which is
  why RAM drops less than the raw 4×/32× value shrink).

Our arena stores raw `float32`. Adding an int8/binary quantized vector slab (with
the full-fidelity vectors kept in the btree for an exact rescore of the top
candidates) is the **highest-leverage memory optimization available** — it would
cut the 9.8 MiB-per-20k-vectors arena ~4× (scalar) and stacks on top of the
arena/SoA savings we already measured. This is a clearer win than anything in the
graph structure itself.

---

## Takeaways for any-store

1. **Adopt MongoDB's external API shape** — a vector index type on the collection
   (`path`, `numDimensions`, `similarity`, `quantization`) and a `$vectorSearch`-
   style query stage (`queryVector`/`query`, `numCandidates`=efSearch, `limit`,
   `filter` pre-filter, `exact`→our `Brute`). It's what users already expect.
2. **But keep single-process, read-your-writes internals** — the one place to
   *beat* Atlas. Don't disaggregate; the index lives in the btree engine, updated
   in the same transaction as the document.
3. **Split vector from adjacency on disk** — pgvector, Lucene, and our own
   measurement all agree.
4. **Tombstone + filter + compact/merge** — we, Lucene, and pgvector already
   agree; Elastic's 18–46%-at-50%-deleted numbers match ours.
5. **Add quantization (scalar/binary) with full-fidelity rescore** — the biggest
   unclaimed memory win, and the feature users now expect from a vector store.
6. **Consider the segment model only if CoW write-amplification bites** — it's the
   Lucene-proven alternative to in-place graph mutation, and it suits a CoW btree,
   at the price of multi-graph queries.

## Sources

mongot/Lucene architecture: MongoDB "now source-available" engine blog; Atlas
vector-search-overview; `Lucene99HnswVectorsFormat` Javadoc; Lucene
`FlatVectorsFormat` PR #12729. API: `$vectorSearch` aggregation reference,
vector-search-stage, vector-search-type, automated-embedding docs. Deletes/merges:
LUCENE-10040, Elastic "Lucene's handling of deleted documents" and "HNSW graphs
speed up merging", LUCENE-10318. Quantization: Atlas vector-quantization docs,
MongoDB binary-quantization-rescoring blog. Memory model: OpenSearch "Lucene HNSW
& the OS page cache". Full URLs in this branch's research notes.
