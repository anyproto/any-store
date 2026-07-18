# Index snapshot visibility grooming: generation-stamped index sets, snapshot-cookie gate

**Repo:** `github.com/anyproto/any-store/v2`, branch `btree` @ `f8cbf6f` (post PR #140/#141 merge)
**Companion test repo:** `../any-store-tests` (module `any-store-tests`, go.work → `../any-store`)
**Bug doc:** `BUG-54-index-visibility-not-snapshot-based.md` in the test repo
**Reference source:** sqlitec pinned `version-3.52.0` (verified this groom); vdbe.c cites against that tag

**Goal (acceptance):** the bug doc flips to FIXED with stale-reader regression tests green across all three index kinds — tests the flag design structurally cannot pass: a read tx opened before a CreateIndex/Compact commit that queries after it gets snapshot-correct results (range: scan, correct Count/Find; fts: ErrNoFulltextIndex; vector create: ErrIndexNotFound; vector compact: correct candidates from its own snapshot). Plus a cross-process flavor in the test repo. benchstat neutral-or-better on hot query paths.

**Scope decisions this groom (2026-07-18):**
- The cross-process reconcile flavor (window 3, found this groom — not in the bug doc's original two windows) is **in scope**: same mechanism, stamped reconciled handles close it for free. The current flag design cannot see it at all (flags cover local DDL only).
- Vector post-commit stale readers are served by **transient rebuild from the reader's own snapshot** (user-ratified): `prev` clearing at commit stays exactly as today — no retention machinery, no codebook/cache pinning, hot paths untouched. Rebuild cost falls only on the rare stale reader.
- Drop-side staleness (reader whose snapshot still contains a dropped index) stays a **non-goal**: planner pessimism → scan → correct; fts/vector fail noisy. Same posture as the bug doc.

**Semantics oracle:** SQLite `OP_Transaction` P5!=0 (vdbe.c:4091-4192) — schema validity decided per execution against the statement's own read snapshot via the schema cookie, which commits atomically with the DDL in page 1. The wall-clock flags are the drift; this change removes it (no new DRIFT entry; commit message cites OP_Transaction).

Every empirical claim below was verified on branch btree @ f8cbf6f.

---

## Decision summary

| # | question | decision |
|---|---|---|
| 1 | Mechanism | Per-handle `validFromCookie uint32` — plain field, set before publish, immutable after. Visibility predicate: `tx.IsWriteTx() \|\| tx.DiskSchemaCookie() >= h.validFromCookie` → fast path (visible); else per-snapshot resolution (slow path, decision 4). Deletes: `index.uncommitted` + `isUncommitted` (index.go:341-356, 386-389), `ftsIndex.uncommitted` (fulltext_index.go:73), `vectorIndex.uncommitted` (vector_index.go:43), the flag-set + flag-clearing pubs (collection.go:948-968, vector_index.go:1283, 1297), `visibleTo`'s flag test (fulltext_index.go:192-194), and `forTx`'s flag/prev cross-load race dance (vector_index.go:731-747). |
| 2 | Stamp values | Local DDL publish: `wtx.DiskSchemaCookie() + 1` — exact: all four DDL paths call `MarkSchemaChanged` (collection.go:1027, 1090; vector_index.go:425, 1264), `pager.commit` bumps `SchemaCookie` exactly once per schema-changing commit (pager.go:2338-2339), and the single writer holds the cross-process write lock, so no other commit interleaves. Reconcile/open/adopt sites: the building tx's `DiskSchemaCookie()` — the earliest cookie at which the handle is *known* visible; readers below it go through the slow path, which is exact. `cloneWithNs` copies the value (replaces the shared-pointer comment at index.go:418-421 — a plain immutable field needs no sharing). |
| 3 | Fast path granularity | Per-handle compare inside the existing loops (visibleIndexes already iterates; a plain uint32 load+compare is cheaper than today's per-handle atomic loads). No collection-level watermark — nothing to maintain on rollback, and benchstat gets the final say. |
| 4 | Slow path per kind | Reader cookie < validFrom → prove the reader's OWN snapshot still carries this exact index. Root-page equality alone is NOT identity — freelist reuse can land a recreated tree on its freed predecessor's page number (review-confirmed, all three kinds) — so admission requires the snapshot's catalog row to match the handle's full definition (`indexDefMatches` against the raw row at the handle's captured `catalogKey`) AND namespace-root match: range = the one `ix:` root; fts = ALL FIVE roots (a partial match would mix generations). Both together are exact: the tree at a name-matched root is the reader's own generation of that definition. Vector: decision 5 (no root proxy at all). Anything less degrades to the pre-DDL behavior — never a wrong answer. Handles capture `nsName`/`catalogKey`/`collName` at construction (immutable) so the lock-free slow paths never read `c.name`, which Rename mutates under c.mu. |
| 5 | Vector slow path + prev policy | `forTx` serves readers by generation INTERVAL, never by root identity: each handle in the prev chain was the published handle for cookies `[h.validFromCookie, next)`, so the first `h` with `cookie >= h.validFromCookie` is exactly the reader's generation (the mid-compaction reader lands on prev this way). A reader older than every held generation gets a **transient rebuild** `loadVectorIndexAs(readerTx, capturedCollName, info)` — but only when the snapshot's catalog row matches the handle's exact definition, which also pins the backend class so the spec branch chosen at detect time stays valid; else ErrIndexNotFound (the SQLITE_SCHEMA posture: never serve old data under a new definition). `prev` lifecycle unchanged: committed-tail target at compact time stays; commit pub still clears it (no pinning of codebooks/caches). The transient handle is per-query and read-only; a per-tx memo is a later optimization if it ever shows on a profile. |
| 6 | Window 2 (commit→runPubs gap) | Structurally closed: the cookie commits atomically with the DDL in page 1, and no pubs govern visibility anymore. `ddlUnwindGate` (tx.go:255-258) stays — it protects the failed-COMMIT unwind for new *writers*, a different job. |
| 7 | Window 3 (cross-process, new) | Peer commits DDL → local `reconcileIndexes` (collection.go:1672) swaps freshly built handles into the shared CoW set → a still-open local reader on an older snapshot plans with them. Closed by decision 2's reconcile stamping: old reader → slow path → namespaces unresolvable in its snapshot → invisible. Added to the bug doc this groom. |
| 8 | Rollback | `registerIndexSetRestore` (collection.go:1004-1024) unchanged — the set restore discards stamped handles wholesale; stamps need no undo work. The `indexSetDDLTxs` commit pub stays (it is set hygiene, not visibility). |
| 9 | Exec-time gates | query.go:886-897 ($knn/$text executed-path checks), fulltext_search.go:961-963, stats.go:193-205 swap mechanically to the same predicate. Plan-time and exec-time use the same tx, so the two checks always agree. |
| 10 | Wraparound | uint32 `>=`; wraps after 2^32 schema-changing commits. Accepted, SQLite parity (the C cookie is a plain equality-checked u32). |
| 11 | NOTES/DRIFT | No new DRIFT — this removes the wall-clock-flag divergence. No existing NOTES entry describes the flag gate (verified by grep). Commit message cites `OP_Transaction` vdbe.c:4091-4192. |
| 12 | Increments | 1 = stamps + predicate swap + flag deletion, all kinds (core). 2 = vector slow path (chain probe + transient rebuild). 3 = regression tests (+ cross-process e2e in the test repo). One PR, separable commits per scope discipline; no local bug numbers in code or test names. |

---

## Facts (verified @ f8cbf6f)

- Cookie plumbing: `ReadTx.DiskSchemaCookie()` returns the page-1 cookie captured at tx begin (internal/btree/db.go:1959-1963; capture at :896 for readers, :1030 for the write tx's embedded read view). `pager.commit(dataChanged, schemaChanged)` does `SchemaCookie++` iff schemaChanged (pager.go:2338-2339), once per commit regardless of DDL-op count; the new value lands in `db.localSchemaCookie` post-commit (internal/btree/db.go:2104-2108) and in the app layer via `UpdateLocalCounters` (db.go:485).
- All handle-publishing DDL marks schema changed: `createIndex` collection.go:1027, `createFtsIndex` collection.go:1090, `createVectorIndex` vector_index.go:425, `CompactVectorIndex` vector_index.go:1264. So begin-cookie+1 is the exact committed cookie for every stamped publish.
- Flag machinery to delete: publish-side flag set + clearing pub collection.go:948-968; compact-side vector_index.go:1283, 1295-1299 (flag-before-prev ordering exists only to serve the `forTx` cross-load race, vector_index.go:738-745 — both go); `visibleIndexes` pending-scan query.go:137-155; `visibleTo` fulltext_index.go:192-194; `isUncommitted` index.go:386-389; clone sharing index.go:418-421.
- Gate call sites: plan-time `visibleIndexes` at query.go:164, :319, reportCandidates :164; exec-time query.go:886-897; fts search fulltext_search.go:961-963; stats.go:193-205.
- Slow-path building blocks exist: `ReadTx.GetNamespace` resolves at the reader's WAL snapshot (internal/btree/db.go:1923-1928); root compare precedent collection.go:1732-1734 (reconcile reuse check); fts `nsMeta` root check fulltext_index.go:202-209; `loadVectorIndex(tx *btree.ReadTx, info)` opens brute/IVF/HNSW from any snapshot (vector_index.go:393-417).
- Reconcile adopt/build sites to stamp: range collection.go:1741 (`newIndex` in `reconcileIndexes`), vector `reconcileVectorIndexesLocked` vector_index.go:518, fts `reconcileFtsIndexesLocked` fulltext_index.go:220 (bind at :248). Open-path build sites: `c.init`'s `load` closure builds handles from a tx (collection.go:313, :325).
- Window 3 is real: `reconcileIndexes` guards only against *local* in-flight DDL (`indexSetDDLTxs`, collection.go:1681-1691); a reconcile triggered by a peer's cookie bump swaps the CoW set while older local readers are live, and those readers immediately plan with handles their snapshots do not contain. IVF handles pin RAM-resident codebooks (internal/vivf/store.go:69-73), which is why unbounded prev retention was rejected.
- No live-reader tracking exists anywhere (neither any-store `db` nor an exposed btree read-mark API) — the watermark-trim alternative would be new hot-path machinery; rejected in favor of transient rebuild.

---

## Design

### 1. Stamps (increment 1)

`validFromCookie uint32` on `index`, `ftsIndex`, `vectorIndex`. Sites:

- **createIndexes publish block** (collection.go:940-993): replace the three flag loops + flag-clearing pub with one stamp loop over all new handles: `validFromCookie = tx.DiskSchemaCookie() + 1`. CoW publish and `registerIndexSetRestore` unchanged.
- **CompactVectorIndex** (vector_index.go:1278-1304): stamp `nvi` the same way; keep `prevTarget` selection and the prev-clearing commit pub; drop the flag store and the ordering comment.
- **Reconcile adopt/build** (three sites above): stamp with the reconciling tx's `DiskSchemaCookie()`. Reused (unchanged-root) handles keep their existing stamp — monotonicity holds because a reused handle was already visible at its old stamp.
- **init/open** (collection.go:313, :325): stamp with the loading tx's `DiskSchemaCookie()`.
- **cloneWithNs** (index.go:398-423): copy the value; delete the shared-pointer rationale comment.

### 2. Predicate swap (increment 1)

One helper per kind, common shape:

```
visible := tx.IsWriteTx() || tx.DiskSchemaCookie() >= h.validFromCookie
if !visible { visible = h.resolvesIn(tx) }   // slow path, decision 4
```

- `visibleIndexes` (query.go:137): keep the zero-alloc common case — first pass finds any handle failing the fast path; only then allocate and filter with the full predicate.
- `ftsIndex.visibleTo` (fulltext_index.go:192): fast path, else `nsMeta` root match against `tx.GetNamespace`.
- `stats.go:193-205` and the exec-time gates swap mechanically.

### 3. Vector forTx (increment 2)

```
fast path → vi
slow path:
  rootsResolve(tx, vi)        → vi          // vmeta root match
  prev := vi.prev.Load(); prev != nil && rootsResolve(tx, prev) → prev
  c.loadVectorIndex(tx, vi.info) → transient handle (per-query, read-only)
  open error → ErrIndexNotFound (pre-DDL behavior)
```

Brute-force mode short-circuits (no namespaces; metadata-only handle is snapshot-independent — visibility is the fast path alone, matching today's behavior where brute handles carry no backfilled state).

### 4. Tests (increment 3)

In-process (any-store repo; names descriptive, no bug numbers):

1. Stale reader × CreateIndex (range): open read tx → CreateIndex commits on another goroutine → query through the old tx: Count/Find correct (scan), Explain shows the new index unused; a fresh tx uses it (fast path).
2. Stale reader × fts create: `$text` through the old tx → ErrNoFulltextIndex; fresh tx matches.
3. Stale reader × vector create: `$knn` through the old tx → ErrIndexNotFound; fresh tx searches.
4. Stale reader × CompactVectorIndex: old tx gets correct candidates post-commit (transient rebuild path — assert result correctness, not mechanism); fresh tx gets the compacted handle. Mid-window reader still served via prev.
5. Rollback: failed DDL commit / explicit rollback discards stamped handles (existing registerIndexSetRestore tests extended to assert no phantom visibility).
6. Chained same-tx create+compact keeps passing (existing coverage from the compact committed-tail work).

Cross-process (test repo, e2e): process A holds a long read tx; process B creates an index and commits; process A's *next* tx reconciles (cookie bump) while the old tx is still open; the old tx's query is snapshot-correct, the next tx uses the index. Per the multiprocess contract this is a first-class deployment shape, not an edge case.

### Review amendments (2026-07-18, post high-effort review)

- **Stamp under an ambient write tx**: `init`'s load stamps `begin+1` when
  `wtx.SchemaChanged()` (new btree accessor) — a mid-tx collection reopen
  after a same-tx CreateIndex would otherwise publish an uncommitted handle
  stamped one too low (phantom visible at the begin cookie, surviving
  rollback). Over-stamping a committed handle by one is safe: the slow path
  admits its readers exactly.
- **init bind source**: range/fts namespaces bind through the load tx's
  snapshot (`resolve`), not the db-level latest-committed lookup — the bound
  roots and the stamp must describe the same state.
- **Brute-force slow path**: served via the catalog-gated transient rebuild
  (`loadVectorIndexAs` returns the metadata-only handle), fixing the spurious
  ErrIndexNotFound a restamped brute handle produced for older readers.
- **Deleted**: `vectorIndex.resolvesIn` (root-proxy admission) — the interval
  walk plus catalog-gated rebuild replaces it, which also resolves the
  rootUnchanged-duplication cleanup.

### 5. Verification

- `go test ./...` both repos; race detector on the new concurrency tests.
- benchstat before/after on the query hot-path benches (Find/Count with indexes) — the predicate change swaps atomic loads for plain compares, expect neutral-or-better.
- Bug doc flips to FIXED citing the regression test names.
