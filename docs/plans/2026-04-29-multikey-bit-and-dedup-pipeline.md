# Multi-key bit on index entries + per-iterator multiKey propagation

> **Goal:** Restore `SimpleIndex/In` to alpha.2 perf (~50 µs at 500K) and bring
> `ArrayIndex/In` to ~5–10 ms while keeping correctness on multi-key
> indexes. Fixes the regression introduced by 84917852 ("disable
> covering-count fast-path when multi-range bounds (multi-key safety)").

## Background

`84917852` correctly identified that multi-range `$in` over a multi-key
(array-valued) index over-counts when the planner uses the entry-count
fast path: a doc with `tags=["a","b"]` queried by `$in:["a","b"]`
contributes 2 entries but is 1 doc. The fix disabled the fast path
whenever `len(idx.Bounds) > 1`, which is too coarse — it also disables
the fast path for **scalar** indexes (e.g. `$in:[1,2,3]` on field `a`),
where each doc has exactly one value of the field and entry-count =
doc-count is still safe.

Result: `SimpleIndex/In` regressed ~1500× (56 µs → 83 ms / 75K allocs)
and `ArrayIndex/In` regressed ~3000× (76 µs → 230 ms / 75K allocs) at
500K docs, both routed through `Fetch → Filter → Dedup` with one
parsed-doc allocation per result row.

## Design summary

Two interlocking pieces:

1. **Per-entry bit on the index value.** Today every index entry stores
   `value=nil`. We change it to a 1-byte bitmask. Bit 0 = "this doc has
   more than one entry in this index" (multi-key from the producer's
   perspective). Reversible per-doc: when an array shrinks to one
   element, the next `deleteKeys+insertKeys` cycle rewrites the entries
   with the new bit.

2. **`multiKey bool` propagated through the iterator chain.** Each
   iterator's `Next()` returns `(key, docId []byte, multiKey bool, err
   error)`. Source iterators set it (IndexIter from the value byte;
   CoverIter and FullScanIter hard-code `false`). Transform iterators
   pass it through. Dedup iterators (CanonicalKeyDedupIter) return
   `false` because their output stream is unique. Consumers use a
   tiny `DocDedup` helper that lazily allocates a hash-set only when a
   `multiKey=true` entry arrives, so fully-scalar streams pay zero
   dedup cost.

Together: scalar queries skip dedup entirely (alpha.2 speed),
multi-key queries dedup at the docId level without fetching documents,
and legacy entries (no value byte) are conservatively treated as
multi-key for safety.

## Encoding spec

Index entry value layout (1 byte):

```
bit 0    : multi-key — this doc has additional entries in this index
bits 1-7 : reserved (must be 0 in this version)
```

Read interpretation:

| Stored value     | Source                                        | `multiKey` |
|------------------|-----------------------------------------------|------------|
| empty / nil      | legacy: written before this PR, unknown shape | **true**   |
| `[]byte{0x00}`   | confirmed scalar (insert produced 1 key)      | false      |
| `[]byte{0x01}`   | confirmed multi-key (insert produced ≥2 keys) | true       |
| len > 1          | future format; bit 0 of byte 0 governs        | val[0]&1   |

Sentinels exposed from `index.go` (zero per-call alloc):

```go
var (
    indexValueScalar   = []byte{0x00}
    indexValueMultiKey = []byte{0x01}
)
const indexValueBitMultiKey byte = 0x01
```

`tx.Put(idx.ns, fullKey, indexValueScalar | indexValueMultiKey)` — the
two slices are package-level so no allocation per insert.

Distinguishing nil/empty (legacy) from `[]byte{0x00}` (new scalar)
relies on `tx.Put` round-tripping a 1-byte zero value as a 1-byte read,
not collapsing to empty. Verified: `btree.Cursor.Value` returns the
exact bytes from the cell, length-preserving.

## API change

`internal/qplanner/iterator.go`:

```go
type Iterator interface {
    // Next returns the next entry. multiKey signals that another entry
    // with the same docId may appear later in this iteration; consumers
    // counting/yielding by docId must dedup multiKey=true entries.
    // multiKey=false guarantees uniqueness across the entire iteration.
    //
    // (nil, nil, false, nil) signals end of iteration.
    Next() (key, docId []byte, multiKey bool, err error)
    Close()
    String() string
}
```

## Per-iterator behaviour (all 11)

| Iterator                  | multiKey rule                                                           | Why                                                                                          |
|---------------------------|-------------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| `IndexIter`               | Source. Reads value byte: `len(v)==0 → true; v[0]&1`.                   | Authoritative source of dup-eligibility.                                                     |
| `IndexFilterIter`         | Passthrough.                                                            | Filtering can only drop entries, not change dup status of survivors.                         |
| `FetchIter`               | Passthrough.                                                            | Doc fetch doesn't change dup status.                                                         |
| `FilterIter`              | Passthrough.                                                            | Same.                                                                                        |
| `LimitIter`               | Passthrough.                                                            | Same.                                                                                        |
| `SortIter`                | Passthrough — flag is per-row, propagated alongside the sort key.       | Re-ordering doesn't change dup-eligibility.                                                  |
| `VerifyIter`              | Passthrough.                                                            | Same.                                                                                        |
| `CoverIter`               | Hard `false`.                                                           | Unique-index point lookup ⇒ at most one entry per doc.                                       |
| `FullScanIter`            | Hard `false`.                                                           | Walks data namespace where docId is the primary key.                                         |
| `CanonicalKeyDedupIter`   | Hard `false`.                                                           | Output is unique by construction — that's the iterator's purpose.                            |
| `SeenSetDedupIter`        | **REMOVED**                                                             | Replaced by consumer-side `DocDedup` helper. Compound multi-key fetch path no longer wraps. |

## DocDedup helper

`internal/qplanner/dedup.go` (new):

```go
// DocDedup is a zero-cost dedup helper for the multi-bound / multi-key
// covering-count + iterator paths. Stack-allocate one per query loop;
// the seen-set map is created lazily on the first multiKey=true entry.
type DocDedup struct {
    seen map[string]struct{}
}

// Accept reports whether docId should be counted/yielded. multiKey=false
// is a guarantee from the iterator that this docId won't reappear, so
// Accept always returns true and never touches the map. multiKey=true
// triggers lazy map allocation and a dedup check.
func (d *DocDedup) Accept(docId []byte, multiKey bool) bool {
    if !multiKey {
        return true
    }
    if d.seen == nil {
        d.seen = make(map[string]struct{}, 64)
    }
    if _, dup := d.seen[string(docId)]; dup {
        return false
    }
    d.seen[string(docId)] = struct{}{}
    return true
}
```

Properties:
- Zero allocation when every entry is `multiKey=false`.
- Single map allocation (initial cap 64) on first `multiKey=true` entry.
- `string(docId)` is a Go-recognised idiom: when used as a map key the
  compiler avoids the byte-to-string copy in lookup paths.

## Source: `insertKeys`

`index.go`:

```go
func (idx *index) insertKeys(tx *btree.WriteTx, it item) error {
    idx.fillKeysBuf(it)
    idKey := it.appendId(nil)

    // 1-byte index value: bit 0 set iff this doc produced >1 keys
    // (multi-key insertion). Sentinels are package-level so no
    // allocation per call.
    indexValue := indexValueScalar
    if len(idx.keysBuf) > 1 {
        indexValue = indexValueMultiKey
    }

    for _, key := range idx.keysBuf {
        idx.fullKeyBuf = append(idx.fullKeyBuf[:0], key...)
        idx.fullKeyBuf = append(idx.fullKeyBuf, idKey...)
        // unique-check unchanged
        if err := tx.Put(idx.ns, idx.fullKeyBuf, indexValue); err != nil {
            return err
        }
        // sketch update unchanged
    }
    ...
}
```

`deleteKeys` is unchanged — the bit lives on the entry, deletion drops
it implicitly.

## Source: `IndexIter`

`internal/qplanner/index_iter.go`:

```go
func (it *IndexIter) Next() (key, docId []byte, multiKey bool, err error) {
    // existing seek/advance
    ...
    val, verr := it.cursor.Value()
    if verr != nil {
        return nil, nil, false, verr
    }
    multiKey = len(val) == 0 || val[0]&indexValueBitMultiKey != 0
    key = k
    docId = extractDocId(k, len(it.IdxInfo.FieldNames))
    return
}
```

## `IndexIter.CountEntries`

Single-bound or no-bound case: page-batch `cursor.CountUntil` as today —
within-doc dedup in `insertKeys` guarantees uniqueness, no per-entry
inspection needed. Fast.

Multi-bound case: walk per entry, read value byte, dedup multi-key
internally:

```go
func (it *IndexIter) CountEntries() (int, error) {
    if len(it.Bounds) <= 1 {
        // existing fast path: cursor.CountUntil per bound
        ...
    }

    // Multi-bound: a doc's array entries can match multiple bounds.
    // Stream-count scalars; lazy seen-set for multi-key + legacy.
    var seen map[string]struct{}
    total := 0
    for _, b := range it.Bounds {
        // seek + walk
        for it.cursor.Valid() {
            if past b.End { break }
            val, _ := it.cursor.Value()
            multiKey := len(val) == 0 || val[0]&indexValueBitMultiKey != 0
            if multiKey {
                if seen == nil {
                    seen = make(map[string]struct{}, 64)
                }
                k, _ := it.cursor.Key()
                docId := extractDocId(k, len(it.IdxInfo.FieldNames))
                if _, dup := seen[string(docId)]; dup {
                    it.cursor.Next()
                    continue
                }
                seen[string(docId)] = struct{}{}
            }
            total++
            it.cursor.Next()
        }
    }
    return total, nil
}
```

## Consumers

### `iterator.go::planIterator.Next` (Find().Iter() boundary)

Embed `qplanner.DocDedup`; loop until a non-dup `multiKey=true` entry or
a `multiKey=false` entry:

```go
func (pi *planIterator) Next() bool {
    if pi.err != nil || pi.closed { return false }
    for {
        pi.plan.DocParsed = nil
        _, docId, mk, err := pi.plan.Root.Next()
        if err != nil { pi.err = err; return false }
        if docId == nil { return false }
        if !pi.dedup.Accept(docId, mk) { continue }
        if pi.plan.DocParsed == nil {
            pi.docId = append(pi.docId[:0], docId...)
        }
        return true
    }
}
```

### `query.go` count loop

```go
var dedup qplanner.DocDedup
for {
    _, docId, mk, iterErr := plan.Root.Next()
    if iterErr != nil { return iterErr }
    if docId == nil { break }
    if dedup.Accept(docId, mk) { count++ }
}
```

### `query.go::Iter` and other Next-loops at lines 322 / 431

Same DocDedup pattern.

## Planner changes

Revert the `SeenSetDedupIter` wrap I added at `planner.go:917` (it
becomes redundant once `IndexIter.CountEntries` handles multi-bound
internally). Restore the simple form:

```go
if params.CountOnly && idx.PointLookup && indexCoversFilter(idx, params.Filter) {
    return root  // IndexIter — CountEntries deduplicates per-entry
}
```

Drop the `SeenSetDedupIter` wrap at lines 962 and 1041 (compound
multi-key fetch paths). The compound multi-key case is now handled by
`planIterator.Next`'s `DocDedup`. `setPlanRef`'s
`case *SeenSetDedupIter` arm is removed.

## Backward compatibility

- **Read of legacy index entries**: any entry with `value=nil` (or
  empty) is treated as `multiKey=true`. Conservative — the planner
  wraps with the dedup path. Slow but correct.
- **Re-write of legacy entries**: any update on a doc removes its old
  entries (legacy nil values) and writes new entries with explicit
  byte values. Migration is per-doc, lazy, transparent.
- **No on-disk schema bump**: same key layout, same namespace. Just a
  new (1-byte) value where there was nil.
- **Downgrade**: old code reads 1-byte values where it expected nil.
  All read sites either ignore the value (existence check) or fetch it
  via `cursor.Value()` and discard. Safe.

## Test plan

### Unit tests (in same package as the code)

1. `internal/qplanner/dedup_test.go` (new): DocDedup behaviour matrix —
   accept-all-on-false, lazy-allocate-on-first-true, dedup-on-second,
   passthrough-after-allocation.

2. `internal/qplanner/index_iter_test.go` extension: `IndexIter.Next`
   reports correct `multiKey` for scalar entries, multi-key entries,
   and legacy nil entries. `CountEntries` correct on:
   - single-bound scalar
   - single-bound multi-key (within-doc dedup)
   - multi-bound scalar (stream count, no map)
   - multi-bound multi-key with overlapping bounds (deduped)
   - multi-bound mixed (some scalar, some multi-key)
   - multi-bound legacy (all nil values, treated multi-key)
   - multi-bound after a doc transitions multi-key → scalar via update.

3. Iterator passthrough tests: each transform iterator (Filter, Fetch,
   Sort, Limit, Verify, IndexFilter) propagates upstream's multiKey
   correctly. CoverIter and FullScanIter hard-code false. CanonicalKey
   returns false.

4. `planner_test.go`: existing `TestBuildPlan_Coverage_CountOnly_*`
   assertions updated. New assertion: multi-bound covering count plan
   is just `IndexScan(...)` with no Dedup wrap (dedup is internal).

5. **Sort + dedup ordering test**: scan a multi-key index with
   $in over overlapping array values, sort by an external key, assert
   the emitted docs are deduped AND in correct sort order. Pins that
   passing multiKey through SortIter doesn't reorder relative to the
   sort key.

### Integration / end-to-end

6. `collection_test.go`: count + iter on `tags` field with
   `$in:["a","b","c"]` returns correct distinct doc count, no dupes
   in iterated docs. Same query after deleting a doc returns
   doc-count-1.

7. **Migration test**: open a fresh DB, manually write nil-value
   entries (simulating legacy), open with new code, run multi-bound
   count → correct (treats as multi-key, deduped). Update a doc → its
   entries get explicit bytes → confirm `IndexIter.Next` reports
   correct flag for those.

### Bench verification

8. Re-run `SimpleIndex/In` and `ArrayIndex/In` at 500K, 3 iters.
   Targets:
   - `SimpleIndex/In`: ≤ 60 µs / ≤ 100 allocs (alpha.2 was 56 µs / 66 allocs)
   - `ArrayIndex/In`: ≤ 15 ms / O(distinct results) allocs

## Commit strategy

One branch, sequential commits, each green and reviewable:

1. `qplanner: DocDedup helper + tests`
2. `qplanner: Iterator.Next gains multiKey bool (passthrough/conservative)`
   - Mechanical signature change. Every iterator returns `multiKey=true`
     conservatively except CoverIter, FullScanIter, CanonicalKey,
     SeenSet which return `false`. All tests updated. No behaviour
     change.
3. `anystore: per-entry index value byte + IndexIter source flag`
   - `insertKeys` writes scalar/multi-key sentinel.
   - `IndexIter.Next` reads value byte to set multiKey.
   - Tests for insertKeys encoding + IndexIter source.
4. `anystore: consumers use DocDedup`
   - `planIterator.Next` and the three `query.go` Next-loops switch
     to DocDedup. SortIter dedup-after-sort test added.
5. `qplanner: drop SeenSetDedupIter; IndexIter.CountEntries handles multi-bound`
   - Remove `SeenSetDedupIter` type, planner wraps, `setPlanRef` arm.
   - Multi-bound `CountEntries` does internal dedup.
   - Update / remove related tests.
   - Restore `planner.go:900` to the simple `len(Bounds)<=1` fast path.
6. `bench: capture SimpleIndex/In + ArrayIndex/In recovery`
   - Add the result file under
     `any-store-tests/results/<commit>_vs_alpha2/`.

Each commit's `go test ./...` and `go vet ./...` must pass before
moving to the next. Commit only when the whole package set is green.
