# Query filter contract

The `query` package guarantees, for every `Filter` produced by `ParseCondition`
or the exported constructors (`NewComp`, `NewCompValue`, `NewInValue`):

1. **Build once, reuse concurrently.** A built filter is immutable. It may be
   shared by any number of queries running in parallel; `Ok` and `IndexBounds`
   take no locks and never mutate filter state. Corollary for hand-constructed
   filters: do not mutate exported fields (`Comp.EqValue`, `In.Values`) after
   the filter has been handed to a query.

2. **Alloc-free evaluation.** `Ok` performs zero heap allocations once the
   per-query `*syncpool.DocBuffer` is warm. Per-document scratch lives in that
   buffer (one per executing query), never in the filter.

3. **Read-only bound bytes.** `IndexBounds` may return bounds whose
   `Start`/`End` alias filter-owned memory (`Comp.EqValue`, `In`'s interned
   keys). These slices are clipped to `cap == len`, so planner bound-extension
   appends (`AdjustBoundsForNonUnique`, `padReverseBounds`) reallocate instead
   of writing into memory shared across queries. Code consuming bounds must
   never write `Start`/`End` in place.

Pinned by `query/filter_contract_test.go`: `TestFilterConcurrentReuse` (run
under `-race`), `TestFilterOkAllocFree`, and
`TestIndexBounds_FilterOwnedBytesAreCapped`.

Note: the contract covers filters only. `Sort`/modifier values are cheap to
build per query and carry no such guarantee.
