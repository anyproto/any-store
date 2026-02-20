# BTree branch — Performance anomalies & TODO

Based on benchmark comparison with SQLite v0.4.6 on 500k docs.

---

## P0: BatchUpdate persistSketches per-doc (3.27x slower, 48k allocs)

**Symptom:** BatchUpdate 1,589ms vs 486ms. 48,701 allocs/op vs 48.

**Root cause:** `collection.update()` calls `persistSketches(tx)` after every single document (collection.go:369). With 100 matched docs and 6 indexes, that's 600 sketch serializations + btree puts per batch.

Compare with `Insert()` which correctly calls `persistSketches` once at the end of the batch (collection.go:207).

**Fix:** Move `persistSketches` out of `update()` / `deleteItem()`. Call it once at the end of `collQuery.Update()` (query.go:255) and `collQuery.Delete()`. The per-doc methods should only set `sketchModified = true`.

**Files:**
- `collection.go:369` — remove persistSketches from update()
- `collection.go:440` — remove persistSketches from deleteItem()
- `query.go:255` — add persistSketches after the update loop
- `query.go:295` — add persistSketches after the delete loop
- `collection.go:248-280` (UpdateId) — keep persistSketches since it's single-doc

---

## P1: SortIter with index 2x slower than SQLite

**Symptom:** Sort/WithIdx 383us vs 182us. FilterSort/SimpleIdx 393us vs 224us. All indexed-sort scenarios ~2x slower.

**Root cause:** `SortIter.Next()` (sort_iter.go:50-62) re-fetches and parses every document from the data namespace even after sorting, so the iterator can set `Plan.DocParsed`. This is a full btree lookup + parse per document on the output path.

The `collectAndSort()` phase already fetched+parsed every doc (sort_iter.go:97-110) to compute sort keys, but discards the parsed result. So every doc is fetched twice.

Additionally, the arena offset arithmetic in the sort comparator (sort_iter.go:122-125) adds overhead vs direct slice references, though this is secondary to the double-fetch.

**Fix options:**
1. Cache parsed docs during collect phase — store doc bytes in the arena alongside sort key + docId, avoid second fetch in Next()
2. If only Count is needed (not iteration), skip the re-fetch entirely
3. For small result sets (limit <= N), store doc bytes directly

**Files:**
- `internal/qplanner/sort_iter.go:34-64` — Next() re-fetch
- `internal/qplanner/sort_iter.go:87-129` — collectAndSort()

---

## P1: Compound/Unique index point lookups 2x slower

**Symptom:** CompoundIndex/FullMatch 435us vs 194us. UniqueIndex/Eq 19us vs 11.5us. CBO/ThreeIdx 434us vs 194us.

**Root cause:** Needs profiling to pinpoint. Likely candidates:
- Extra alloc overhead per-key in the btree cursor path (sortEntry packing, bounds checking)
- CoverIter (cover_iter.go) loops through bounds even for single-point unique lookups
- FetchIter (fetch_iter.go) parses every doc even for count-only queries

**Investigation:** Profile `CompoundIndex/FullMatch` to see where time is spent. Compare with `SimpleIndex/Eq` which is only 25% slower (not 2x) despite similar result count (5000 vs 100 docs). The per-doc overhead is much higher for compound lookups.

**Files:**
- `internal/qplanner/cover_iter.go`
- `internal/qplanner/fetch_iter.go`
- `internal/qplanner/planner.go` — plan construction

---

## P1: LowSelectivity index scan 2.5x slower (170ms vs 67ms)

**Symptom:** `{c:0}` returning 50k docs via index: 170ms btree vs 67ms sqlite.

**Root cause:** Scanning 50,000 index entries + fetching 50,000 documents from the data namespace. Each fetch is a btree lookup. SQLite benefits from its row-oriented storage where index scan + data fetch is a single B-tree traversal into the clustered rowid.

The btree branch does: index cursor scan -> get docId -> separate data namespace lookup per doc. That's 2 btree traversals per document.

**Fix options:**
1. CBO should prefer full scan over index when selectivity > ~20-30%
2. Optimize the fetch path to batch lookups or use a cursor on the data namespace

**Files:**
- `internal/qplanner/planner.go` — cost model / selectivity threshold
- `internal/qplanner/fetch_iter.go` — per-doc fetch

---

## P2: Sort/NoIdx allocates 72MB vs 8MB

**Symptom:** Sorting 500k docs without index: btree 72MB / 97 allocs, SQLite 8MB / 500k allocs. BTree uses ~9x more memory but similar time.

**Root cause:** `SortIter.collectAndSort()` stores sort key + docId for all 500k docs in a contiguous arena (sort_iter.go:112-119). Each entry is ~144 bytes (sort key + docId). SQLite stores only 16-byte sort keys and uses rowid references.

The arena growth strategy (sort_iter.go:70-85) uses tiered steps (1KB, 10KB, 100KB) which causes ~50% overallocation in the final buffer.

**Fix:** Pre-estimate arena size from plan's estimated row count. Or use a more efficient encoding (e.g., fixed-size entries when sort key length is uniform).

**Files:**
- `internal/qplanner/sort_iter.go:70-85` — growArena()
- `internal/qplanner/sort_iter.go:112-119` — entry packing

---

## P2: BatchInsert 37% more memory (159KB vs 116KB per 100 docs)

**Symptom:** 159,438 B/op vs 116,170 B/op for 100-doc batch insert.

**Root cause:** Index key construction allocates fresh buffers per entry. In `insertKeys()` each non-unique index creates `append(anyenc.Tuple(nil), key...)` which allocates a new slice. With 6 indexes * 100 docs = 600 allocations for key buffers alone.

**Fix:** Pool or reuse key buffers across insertions within the same transaction.

**Files:**
- `index.go` — insertKeys() key buffer allocation

---

## P3: FindId 29% slower (8.9us vs 6.9us)

**Symptom:** Single document lookup by ID: btree slightly slower.

**Root cause:** Likely the overhead of anyenc parsing on the btree side vs SQLite's direct blob return. Low priority since absolute numbers are small.

---

## Investigation needed

- Profile compound index FullMatch to understand the 2x gap — the result set is only 100 docs, so per-doc overhead dominates
- Check if Count() queries skip unnecessary doc fetching or if they still materialize through SortIter
- Verify whether the CBO cost model weights correctly for small vs large result sets
