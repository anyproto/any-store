# Plan: Hybrid HNSW — RAM-resident layer 0, btree-backed, multiprocess-consistent

Status: **design / experimental** (branch `feat/vector-hybrid-l0`, off `feat/vector-index`).
Goal: keep the hot part of the HNSW graph — **layer 0 adjacency** (and, optionally, a
tier of vectors) — in RAM, while keeping the on-disk btree as the source of truth, and
keep the RAM mirror **consistent across processes** by reusing the machinery any-store
already uses to detect that another process changed the data/schema.

This is the "hybrid" we discussed: the all-btree index (current `internal/vindex`) is
reliable for multiprocess writes precisely because it has **no in-memory graph to keep
in sync**. This plan adds an in-memory graph *back* — but only layer 0, and only as a
**cache derived from the btree**, synchronized with the same page-1 counter + COW-publish
pattern as the index sketch. The btree remains the source of truth; the RAM mirror is
always reconstructible from it and is never the authority.

---

## 0. Why layer 0, and why this is worth it

Per the read-path map (`internal/vindex/search.go`):

- `Search` descends layers `topLayer..1` via `greedyClosest` (few nodes touched), then
  runs the **beam search at layer 0** (`searchLayer(ep, ef, 0)`), which is where the vast
  majority of work happens — `ef` (e.g. 64–256) candidates expanded, each expansion doing:
  1. one `:adj` read for the neighbour list (`adjBytesOf` → `adjNeighbors`),
  2. one `:vec` read **per neighbour** for the distance (`vecOf`),
  3. one more `:adj` read **per neighbour** for the tombstone check (`isDeleted`), *only
     when* `deletedCount > 0`.
- Measured earlier: search is **btree-read-bound** (~79% of CPU in btree tree-descent for
  `AppendValue`; distance only ~4.6%). So the win is in **eliminating btree reads at
  layer 0**, not in faster math.

Caching layer-0 **adjacency** removes the per-expansion `:adj` read and the per-neighbour
tombstone `:adj` read — roughly **2 of every 3 btree reads** in the layer-0 loop. Caching
**vectors** too (optional Tier-2, int8) removes the last one and makes layer-0 search
almost entirely RAM-resident.

Memory cost (the deliberate trade):
- adjacency mirror ≈ `N · M0 · 4 bytes` + per-node overhead. At `M0 = 32`: ~128 B/node →
  **~128 MB per 1M nodes** (plus map/slab overhead).
- optional int8 vector tier ≈ `N · dim` bytes → **~768 MB per 1M nodes @ dim 768** (vs
  ~3 GB float32). This is why int8 (already landed) is a prerequisite for the vector tier.

---

## 1. The mechanism we are reusing (and where it is NOT enough)

any-store already keeps a per-index **sketch** in RAM and synchronizes it across processes.
The relevant machinery (verified, with file:line):

- **Page-1 counters** (`internal/btree/db.go`): every committing write bumps
  `FileChangeCount`; DDL bumps `SchemaCookie`. A `ReadTx` reads both *at snapshot open*
  and exposes `IsDataStale()` / `IsSchemaStale()` (`db.go:1764`, `:1770`) by comparing the
  disk values to per-process cached `localFileChangeCounter` / `localSchemaCookie`.
- **`checkStale(tx)`** runs at the start of *every* top-level read and write tx
  (`db.go:288-326`, `:348-356`):
  - `IsSchemaStale` → `reconcileIndexSet` (rebuild index handles: add/drop/re-root).
  - `IsSchemaStale || IsDataStale` → `reloadSketches` + `UpdateLocalCounters(...)`.
- **Writer vs reader publish** (`db.go:381-397`, `collection.go:963-985`):
  - write tx (sole mutator under the btree write lock) reloads **in place** into the live
    sketch, then `storePubSketch(live)`;
  - read tx swaps a **fresh copy-on-write** snapshot via an `atomic.Pointer`
    (`idx.sketchPub`), so it can never clobber a concurrent in-process writer.
- **Rollback safety** (`db.go:412-432`): `resetUncommittedSketches` at write-tx begin
  rebases any dirty-but-uncommitted sketch back to the committed bytes.

**Why this is exactly the right trigger but an insufficient contract:**

The sketch is **advisory** — a stale or slightly-wrong sketch only changes *which index the
planner picks*, never query *results*. So "eventually consistent, atomically published" is
fine.

A layer-0 mirror is **load-bearing** — it is consulted to produce search results. Two new
hazards the sketch never has:

1. **Mirror ahead of the snapshot.** Labels are allocated monotonically; `:vec` records are
   written when a node is inserted. If a reader at btree snapshot `V` consults a mirror that
   already contains an edge to a label inserted at `V+1`, then `vecOf(thatLabel)` does a
   `:vec` read **that misses in snapshot `V`** → error / wrong result. A long-lived read tx
   in a process that is also writing is the canonical trigger.
2. **Mirror behind the snapshot.** Missing recent edges → silent recall loss (no error, just
   worse results). Less dangerous than (1) but still a correctness-of-quality bug.

So we keep the sketch's *trigger and publish* mechanics, but add a **strict snapshot-alignment
contract**: a transaction may only use a mirror whose logical version equals (or is replayable
to) its own btree snapshot version, and the system must always be able to fall back to a
pure-btree layer-0 search.

---

## 2. Core design

### 2.1 Source of truth stays in the btree

No change to the on-disk format's authority. `:vec`, `:adj`, `:doc`, `:lbl`, `:meta` remain
the truth. The mirror is a **derived cache**; deleting it and rebuilding from the btree must
always yield identical search behaviour. This is the single most important invariant and the
basis of every test below.

### 2.2 A logical layer-0 generation, carried in the btree

Add an `l0Gen uint64` to the index `meta` record (`internal/vindex/codec.go`), bumped on
every write tx that changes any layer-0 structure (insert, the back-link `addNeighbor` at
layer 0, tombstone). Because it lives in `:meta`, **every reader reads `l0Gen` at exactly its
own snapshot** — this is the version we align the mirror to. (`FileChangeCount` is the
cross-process *wake-up*; `l0Gen` is the *precise* mirror version. We need both: the former is
free and global, the latter is snapshot-exact and per-index.)

> **Why a dedicated `l0Gen` and not the index sketch's doc count?** The sketch is the
> obvious candidate to reuse for staleness detection, but it *cannot* detect the changes
> we care about: (a) vector indexes are not wired into the sketch system at all today
> (the sketch lives only on range indexes); (b) `docCount` is **net** — an update is
> tombstone-old + insert-new = `-1 + 1 = 0`, so the count never moves even though layer-0
> edges changed; (c) the buckets sketch *field values*, but a 768-dim vector has no
> meaningful bucket; (d) pure back-link churn (`addNeighbor` rewriting an existing node's
> neighbour list during another node's insert) changes layer 0 with no doc-count or bucket
> change at all. So we reuse the sketch's *mechanism* — persist-inside-commit,
> reload-at-exact-snapshot, COW atomic-pointer publish, rollback-rebase
> (`resetUncommittedSketches`) — but carry a **monotonic `l0Gen`** as the value, kept in
> the load-bearing `:meta` record (already read at the tx snapshot via `readMeta(rtx)`)
> rather than in the advisory sketch, so a correctness-critical version is never coupled to
> a structure other code may treat as throwaway.

### 2.3 A delta log in the btree (so sync is incremental and snapshot-consistent)

New namespace `:l0log`, keyed by `l0Gen` (big-endian, cursor-ordered). Each write tx appends
**one record** describing the layer-0 changes it made:

```
l0log[gen] = { entryLabel, topLayer,            // meta deltas (cheap, always included)
               adds:   [ (label, level, nbrs0[]) ... ],   // new nodes' layer-0 lists
               edits:  [ (label, nbrs0[]) ... ],          // existing nodes whose nbrs0 changed
               tombs:  [ label ... ] }                    // newly-tombstoned labels
```

Two consequences:
- **Cross-process sync is automatic and snapshot-exact.** A process that wakes on a bumped
  `FileChangeCount` reads `:l0log(mirrorGen, snapshotGen]` *from its own btree snapshot* and
  replays it into a fresh mirror. Because the log is read at the snapshot, the resulting
  mirror is exactly snapshot `snapshotGen` — never ahead, never behind. This is the crux that
  makes it safe.
- **In-process writing is O(changes), not O(N).** The writer also applies the same delta to
  its live mirror as it commits.

Log growth is bounded by periodic **compaction** (§2.6).

### 2.4 Mirror data structure & MVCC publish

Per vector index, in RAM:

```
type l0Mirror struct {        // immutable snapshot, shared by readers
    gen      uint64
    adj      ...               // label -> []uint32 layer-0 neighbours
    deleted  ...               // label -> bool (tombstone)
    entry    uint32
    topLayer int32
    // optional Tier-2: vecs (int8) label -> quantized vector + scale
}
```

Published via `atomic.Pointer[l0Mirror]` on the index handle — identical pattern to
`idx.sketchPub`. Readers `Load()` it once at search start and use that immutable snapshot for
the whole search.

To bound memory and support long-lived old readers, keep a **small ring of recent versions**
(`gen → *l0Mirror`, e.g. last 4). A read tx looks up the version matching its `snapshotGen`:
- exact hit → use it;
- live mirror is *newer* and the needed gen was evicted, or live is *older* than snapshot
  (foreign write we haven't replayed yet) → **replay from `:l0log` to build the exact gen**,
  publish it into the ring;
- replay impossible (log compacted past `snapshotGen` — only for very old readers) →
  **fall back to pure-btree layer-0 search** for that tx. Always correct, just slow.

### 2.5 Writer path (single writer at a time, holds the btree write lock)

Within a write tx (mirrors `addNeighbor`/insert/tombstone in `hnsw.go`):
1. Do the existing btree writes (`:vec`, `:adj`, …) **unchanged** — truth first.
2. Stage the layer-0 delta in a per-tx buffer (adds/edits/tombs + entry/topLayer).
3. On **commit**: bump `meta.l0Gen`, append the delta to `:l0log`, write `:meta`; then apply
   the staged delta to a new `l0Mirror` (COW from current live) and `storePubMirror(new)`.
   Ordering matches the sketch: persist inside the btree commit, publish after.
4. On **rollback**: discard the staged buffer; do **not** advance the mirror. (Analog of
   `resetUncommittedSketches`; simpler because we stage out-of-line rather than mutate live.)

The writing process is therefore always self-consistent without reading its own log.

### 2.6 Compaction (the genuinely tricky part)

`:l0log` must not grow forever. Periodically (every K gens, or when log bytes exceed a
threshold) the **writer** snapshots the full layer-0 mirror into a compact `:l0base` record
(or a set of cursor-ranged records) tagged with `baseGen`, then deletes `:l0log[<= baseGen]`.

A process that is far behind (`mirrorGen < baseGen`) syncs by: load `:l0base@baseGen` →
replay `:l0log(baseGen, snapshotGen]`. A process behind even the base (older snapshot than any
retained base) falls back to btree-only for that tx. Compaction parameters (K, retained
bases) trade log size vs how-old-a-reader-can-be-fast. Start conservative; measure.

Because compaction is just another write tx by the single writer, it is naturally serialized
and snapshot-versioned like everything else.

### 2.7 Schema changes (index create/drop) — tie into `reconcileIndexSet`

When `IsSchemaStale` fires and a vector index is **created** by a peer → allocate an empty
mirror (lazy; first search builds it). **Dropped** by a peer → drop the mirror. **Re-rooted**
→ discard and rebuild. This hooks into the existing `reconcileIndexes` path so we never keep a
mirror for a namespace that moved. (Directly answers "see to the machinery for how we detect
an index changed in another process": that machinery is the `SchemaCookie` →
`reconcileIndexSet`; we extend its per-index reconcile to own the mirror lifecycle.)

---

## 3. The consistency contract (must hold, will be tested)

1. **Truth-equivalence.** For any snapshot, search using the mirror returns the *same hits*
   as search using only the btree at that snapshot. (Recall is identical, not merely close.)
2. **Snapshot alignment.** A tx only ever uses a mirror at `gen == its meta.l0Gen` (or one
   replay-derived to it). Never a mirror ahead of the snapshot (hazard §1.1), never silently
   behind (hazard §1.2).
3. **Fallback always available.** Any tx can run layer-0 search purely from the btree; the
   mirror is an optimization, never a dependency. A disabled/oversized/too-old mirror degrades
   to today's behaviour.
4. **Rollback isolation.** A rolled-back write tx leaves `l0Gen`, `:l0log`, and the published
   mirror unchanged.
5. **Single-writer.** All mirror/log mutation happens under the btree write lock; readers only
   ever COW-swap published snapshots. No new cross-process locks.
6. **Memory-bounded.** A configurable cap disables the mirror (fallback) rather than OOM.

---

## 4. Phasing (each phase independently shippable & testable)

**Phase 0 — scaffolding & invariant harness.** Add `l0Gen` to meta (unused), and a test-only
`verifyMirror(rtx)` that rebuilds the mirror by scanning `:adj` and asserts it equals the live
mirror. This harness backs every later phase.

**Phase 1 — in-process mirror (no log, no cross-process).** Build the mirror by scanning
`:adj` on first use; writer updates it in place under the write lock; reader COW-swaps.
`Search` consults the mirror for layer-0 adjacency + tombstones; vectors still from btree.
Prove: recall identical to btree-only (contract §1), search latency drops, race-clean
(`-race`), `verifyMirror` passes after randomized insert/delete/update sequences.
**No multiprocess claim yet** — single process only.

**Phase 2 — delta log + cross-process sync.** Add `:l0log`, bump/append on commit, replay on
`checkStale` wake-up, COW publish, version ring + btree fallback. This is where the
multiprocess guarantee returns. Test with **two processes** over one db file (extend the
existing multiprocess test harness): writer mutates; reader must converge to identical recall
after each commit; assert no "mirror ahead of snapshot" `:vec` misses under a long-lived
reader + concurrent writer.

**Phase 3 — compaction.** `:l0base` + log truncation + far-behind reader fallback. Test: log
bytes stay bounded under a long write stream; a reader resumed after compaction still
converges (replay-from-base or fallback).

**Phase 4 (optional) — vector tier (int8 in RAM).** Add quantized vectors to the mirror so
the layer-0 distance loop is fully RAM-resident. Gated by the memory cap. Reuses the existing
int8 codec. Measure the additional latency win vs the additional RAM.

---

## 5. Test matrix (correctness first, then speed)

- **Truth-equivalence** (Phase 1+): for random graphs + random query sets, `mirror hits ==
  btree-only hits`, exact, with and without deletes.
- **`verifyMirror` fuzz**: randomized interleavings of insert/update/delete/search; assert
  mirror == rebuilt-from-`:adj` after every op and after rollback.
- **Multiprocess convergence** (Phase 2+): process A writes, process B reads; after each A
  commit, B's recall == A's; include the long-lived-reader-vs-writer "ahead" hazard and the
  schema-change (create/drop) cases.
- **Crash/rollback**: kill mid-write; reopen; mirror rebuilds from btree truth; `l0Gen` and
  `:l0log` consistent with `:adj`.
- **Race**: every test under `-race`.
- **Memory cap**: oversized mirror disables cleanly and falls back.
- **RPS/size**: extend `vector_rps_test.go` with a third axis (btree-only vs hybrid-adj vs
  hybrid-adj+int8vec); report search RPS uplift and the RAM delta on local + p14 + hp.

---

## 6. Risks / open questions (resolve during Phase 1–2)

- **`addNeighbor` churn.** Back-links rewrite existing nodes' layer-0 lists frequently; the
  log "edits" set can be large per insert. Mitigation: log the *final* layer-0 list per
  touched label once per tx (dedupe within the tx), not per edge.
- **Mirror memory at scale.** Decide the default cap and the adjacency representation (slab of
  fixed `M0`-wide rows vs map of slices). Fixed-width slab is far more compact and cache-
  friendly; favour it.
- **Version-ring depth.** How many old gens to retain for long-lived readers before forcing
  fallback. Start at 4; measure real reader lifetimes.
- **Do we even need the log for v1?** Alternative cross-process sync = rebuild-by-scanning
  `:adj` on staleness (O(N) per foreign-write wake-up). Acceptable only if foreign writes are
  rare. The log is strictly better for write-heavy multiprocess; keep the scan path as the
  fallback (it is the Phase-1 builder anyway).
- **Interaction with int8 search.** Tier-2 vectors in the mirror should store the same int8
  encoding as `:vec` so distances match bit-for-bit and recall is unchanged.

---

## 7. Non-goals

- No change to the btree engine.
- No change to on-disk authority or to single-process semantics when the mirror is disabled.
- Not trying to keep *upper* layers in RAM (they are cheap; layer 0 is the cost).
