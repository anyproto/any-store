# Deletes, updates & the doc-id mapping — research + measurements

Branch: `btree-vector-search`. Companion to [README.md](./README.md). This note
answers: **how do deletes and updates affect HNSW performance, the arena layout,
and the btree storage — and how should a real any-store index link graph nodes
to documents?**

Everything below is backed by tests in this package (cited by name) so the
numbers are reproducible:

```
go test ./vector -run TestTombstoneLatencyReport -v   # latency/recall vs deleted fraction
go test ./vector -run TestHardDeleteCost -v            # tombstone vs O(N) hard delete
go test ./vector -run TestBtreeWriteAmplification -v   # bytes/records per insert/delete/update
go test ./vector -run TestDeleteCompactRAM -v          # RAM across delete -> compact
go test ./vector -run TestIDMapMemory -v               # doc-id mapping RAM
go test ./vector -run TestCompactVsRebuildRecall -v    # cheap compact vs full rebuild
```

---

## 1. What the production systems do (and why)

The original HNSW paper defines only **insert** and **search** — no deletion.
Every production engine bolts it on, and they nearly all converge on the same
design because of three structural facts:

- **HNSW edges are directed and there is no reverse index.** A node's adjacency
  list says who it points *at*, not who points *at it*. Truly removing a node
  means finding its in-neighbours, which needs a full-graph scan or a separate
  reverse structure. This one fact is why hard-delete is avoided.
- **The entry point is special** — delete it and search has no start; something
  must promote a replacement.
- **Removing a node can fragment the graph**, stranding nodes (out-edges but no
  in-edges) and dropping recall.

| System | Delete | Update | Repair / reclaim |
|--------|--------|--------|------------------|
| hnswlib | soft tombstone, no repair | delete+reinsert (`replace_deleted`) | periodic full rebuild |
| FAISS | **unsupported** for HNSW | — | rebuild |
| Qdrant | soft + filter | delete+reinsert | **Vacuum rebuilds a segment at `deleted_threshold=0.2`** (≥1000 vecs), online via proxy |
| Weaviate | tombstone | delete+reinsert | async cleanup that **scans all nodes** to find in-neighbours and reconnect |
| Milvus | soft bitset, skip at query | delete+reinsert | compaction copies live → new segment, **rebuilds HNSW** |
| pgvector | MVCC dead tuple | delete+insert | 3-pass VACUUM repairs in place; REINDEX past ~20% |
| Vespa | **real-time in-place mutation** | in-place | none — pays insert-cost per mutation |

**Common patterns:** soft-delete is the default everywhere but Vespa; tombstoned
nodes are still *traversed* during search (excluded only from results) until
cleanup; updates are delete+reinsert; a **~20% deleted fraction** is the usual
rebuild trigger; repeated updates erode the graph (hnswlib: **3–4% unreachable
points and ~3% recall loss after 3000 update iterations on SIFT**), so periodic
rebuild is the universal cure; write amplification scales with **M** (≈ M…M²
adjacency edits per delete/update).

Sources: HNSW paper (arxiv 1603.09320); hnswlib issues #4/#275/#321; "Enhancing
HNSW for Real-Time Updates" (arxiv 2407.07871); Qdrant optimizer docs; Weaviate
`hnsw/delete.go`; Milvus delete/compaction blog; pgvector VACUUM docs; Vespa
approximate-nn docs.

This package implements the consensus design: **tombstone + filtered search +
threshold-triggered Compact/Rebuild**, plus a hard-delete variant kept only to
measure what the tombstone path avoids.

---

## 2. What deletes cost *this* arena layout

`FlatHNSW` stores vectors in one contiguous slab and adjacency in one
append-only `[]uint32` arena with **variable-length per-node blocks**
(`M0 + L*M`, L = node level). Two consequences specific to that layout:

- **No id reuse without Compact.** A freed dense id can't be handed to a new node
  of a different level — its old adjacency block is the wrong size. So a delete
  cannot be a simple free-list slot; reclamation means rewriting the arenas.
- **Hard delete must scan the whole adjacency arena** to find in-edges (no
  reverse index), exactly the production problem.

Both push toward **tombstones** (`deleted` bit + drop the key from the lookup
map; O(1)), with periodic **Compact** (rebuild arenas keeping live nodes, remap
ids) or **Rebuild** (full re-insertion).

### Measured: tombstone delete vs hard delete — `TestHardDeleteCost` (20 000 × 128d)

| operation | ns/op |
|-----------|------:|
| tombstone `Delete` | **99** |
| `DeleteHardRepair` (full-arena scan) | 250 612 |

**~2530× slower**, and the gap *grows with index size* (the scan is O(N·degree)).
This is the empirical reason production HNSW never hard-deletes inline.

### Measured: search cost vs deleted fraction — `TestTombstoneLatencyReport` (20 000 × 64d, ef=64)

| state | search ns/op | recall@10 (live set) |
|-------|------:|------:|
| 0% deleted | 37 509 | 0.819 |
| 10% tombstoned | 41 226 | 0.824 |
| 20% tombstoned | 44 704 | 0.839 |
| 30% tombstoned | 49 798 | 0.855 |
| 50% tombstoned | 66 968 | 0.886 |
| **after Compact** | **24 992** | 0.674 |

The headline cost of soft-delete is **latency, not recall**: tombstoned nodes
still route the search, so a query at 50% deleted is **~1.8× slower**. Recall
over the *live* set even drifts up slightly — the extra traversal inflates the
effective `ef`. Compact then removes tombstones from navigation, so search gets
*faster than the original* (fewer nodes), but cheap compaction thins edges that
pointed at deleted nodes, costing recall (see §3).

---

## 3. Reclaiming: Compact vs Rebuild

- **Compact** — rebuild the arenas keeping only live nodes, remap every
  neighbour id, drop edges to deleted nodes. O(nodes + links), ~2.3 ms for 20k.
  Cheap, reclaims all tombstone RAM, but leaves thinner neighbourhoods.
- **Rebuild** — re-insert every live vector from scratch. Full HNSW quality,
  much more expensive.

### Measured: recall after 50% deletes — `TestCompactVsRebuildRecall` (3 000 × 48d)

| | recall@10 |
|--|------:|
| Compact (drop dead edges) | 0.932 |
| Rebuild (full reconstruction) | 0.982 |

So the recall cost of cheap compaction is real and grows with the deleted
fraction (it's a ~0.05 hit here at 50% deleted; larger in the 64-d run above).
This matches the production split: Qdrant/Milvus **rebuild** the segment, while
Weaviate/pgvector do in-place edge repair. **Recommendation:** tombstone for the
common case; trigger **Rebuild around the ~20% deleted mark** (the industry rule
of thumb); use cheap Compact only when RAM reclaim matters more than the last
few points of recall.

### Measured: RAM across the lifecycle — `TestDeleteCompactRAM` (50 000 × 128d)

| stage | live | physical | arena RAM |
|-------|-----:|---------:|----------:|
| after build | 50 000 | 50 000 | 34.0 MiB |
| after 50% delete | 25 000 | 50 000 | **34.0 MiB** |
| after Compact | 25 000 | 25 000 | **17.0 MiB** |

**Tombstones do not free memory — Compact does.** RAM tracks *physical* nodes, so
a delete-heavy workload must compact/rebuild to release memory. This is the RAM
analogue of the latency curve above.

---

## 4. btree storage impact & write amplification

Persistence (`BtreeHNSW`) keeps node records in `<name>:nodes` (keyed by dense
id) and now adds `<name>:tomb` for tombstones. Deleting a node **keeps its node
record** (it's still a waypoint) and writes a 1-byte tombstone; reload re-applies
tombstones after loading the graph.

### Measured: write amplification — `TestBtreeWriteAmplification` (4 000 × 128d, flush per op)

| operation | btree records / op | bytes / op | breakdown |
|-----------|------:|------:|-----------|
| **insert** | 36.6 | 24 288 | vector 18 213 + adjacency 5 603 + overhead |
| **delete** (tombstone) | 2.0 | 46 | tombstone + meta only |
| **update** (delete+reinsert) | 38.5 | 25 020 | tombstone + new node + neighbours |

Three findings that should shape the on-disk format:

1. **Fan-out ≈ M.** An insert/update rewrites ~36 records — itself plus its
   touched neighbours — confirming the "M…M² adjacency edits" rule. Batching many
   inserts per `Flush` amortises this (a neighbour touched by several inserts is
   written once); the per-op numbers above are the worst case (flush-per-op).

2. **~75% of insert write volume is re-writing *unchanged* vectors.** Of the
   24 KB/insert, 18 KB is vector bytes belonging to neighbour records whose
   vector didn't change — only their adjacency did. **Splitting the record into a
   `<name>:vec` namespace (large, immutable: written once) and a `<name>:adj`
   namespace (small, churning: ~`M0*4` = 128 B) would cut a re-link from ~664 B
   to ~153 B per touched node — roughly a 4× reduction in write volume**, and a
   corresponding cut in WAL/checkpoint pressure. This is the single highest-value
   storage change.

3. **Tombstone delete is almost free** (46 B), but neighbour ids are scattered
   across the keyspace, so each insert/update dirties ~M *different* btree leaf
   pages → ~M pages copied into the WAL per op. The vector/adjacency split helps
   here too (smaller adj pages pack more neighbours per page).

**Compaction on disk** rewrites every record (ids change) — a full namespace
rewrite. Keep it rare (threshold-triggered), and consider keying records by the
*stable label* (next section) instead of the volatile arena id so that Compact
doesn't have to rewrite untouched nodes.

---

## 5. Linking graph nodes to documents (the doc-id mapping)

any-store keys every document by the **marshaled bytes of its `id` field**
(`idVal.MarshalTo`) — an arbitrary variable-length `[]byte` (string, int,
objectid…). **There is no numeric rowid.** An HNSW arena needs a dense,
fixed-width `uint32` to index its slabs. So the index must map `docID []byte ↔
uint32`.

`IDDict` does this with **dictionary encoding** (the "uint32 counter" option):
each distinct doc id gets a monotonic `uint32` *label*; the graph stores labels,
and the doc id is recovered only when returning results. The label→id reverse
map is a **flat arena** (all id bytes concatenated + one `[]uint32` of offsets),
*not* a `[][]byte`.

### Measured: id-mapping RAM — `TestIDMapMemory` (500 000 × 12-byte ids)

| mapping | RAM | per id |
|---------|----:|-------:|
| flat-arena `IDDict` | 43.1 MiB | **90.5 B** |
| naive `[][]byte` + map ("store ptr to id") | 53.4 MiB | 111.9 B |

The flat arena saves ~19% overall and, more importantly, turns the reverse map
from `N` heap objects + a 24-byte slice header each into **two** heap objects
(one `[]byte`, one `[]uint32`) — 4 B/id of overhead instead of 28 B/id, and far
less GC scanning.

**But note where the cost actually is:** in *both* designs the forward
`map[string]uint32` (docID → label) dominates at ~80 B/id. If id-mapping RAM
becomes a concern at scale, the lever is the forward map, not the reverse one —
options: (a) don't keep it resident at all and resolve docID→label from a btree
namespace on the write path; (b) a compact open-addressed `[]byte`-keyed hash;
(c) store the label *in the document* so inserts already know it. The reverse
arena should stay flat regardless.

### Why labels beat raw arena ids on disk

Labels are **stable for the life of a document** — they survive `Compact` (which
only reshuffles the graph's *internal* arena ids). Keying the persisted node
records by **label** rather than arena id means Compact doesn't invalidate every
on-disk key, and the doc↔label dictionary never needs rewriting. `DocFlatHNSW`
demonstrates the composition (dict + arena graph, returning `[]byte` ids).

---

## 6. Recommendation

1. **Tombstone deletes; filter at search.** O(1), ~free on disk (46 B). Accept
   that queries slow down with the deleted fraction (latency, not recall).
2. **Updates = delete + reinsert.** Expect graph erosion over many updates;
   schedule maintenance.
3. **Rebuild around ~20% deleted** (industry default); cheap `Compact` only when
   RAM reclaim outweighs the recall hit. Tombstones don't free RAM — maintenance
   does.
4. **Never hard-delete inline** (2500× slower here); if in-place repair is ever
   wanted, it requires adding a reverse-edge index (≈ doubles adjacency memory).
5. **Split the on-disk record into immutable vector + churning adjacency
   namespaces** — ~4× less write volume per insert/update, the highest-value
   storage change.
6. **Map doc ids with a uint32-label dictionary + flat reverse arena**, key
   persisted records by the stable label, and treat the forward `map` as the
   real RAM cost to optimise later.

## Known simplifications (still a spike)

- Cheap `Compact` drops edges to deleted nodes without replenishing; a smarter
  compaction would re-link 2-hop survivors to recover some of the lost recall.
- `BtreeHNSW` still stores vector+adjacency in one record (the split in §4 is
  measured but not yet implemented) and keys by arena id, so on-disk Compact
  would rewrite everything — both noted as the next storage steps.
- The forward id map is an in-memory `map[string]uint32`; the resident-RAM
  alternatives in §5 are not yet built.
