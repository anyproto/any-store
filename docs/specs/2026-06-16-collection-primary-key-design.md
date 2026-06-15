# Design: Configurable collection primary key

**Date:** 2026-06-16
**Branch:** `btree` (or fresh branch off it)
**Scope:** `any-store` core (collection / query / qplanner / db config / CLI). No on-disk format change. No data migration.
**Status:** Draft — awaiting user review

---

## 1. Goal

Today every collection's primary key is the hardcoded document field `"id"`.
Make the primary-key **field name** configurable per collection, defaulting to
`"id"` so all existing databases and call sites keep working unchanged.

The key's **value** may be any anyenc type (string, int, bytes, …) — this is
already true today and is preserved, not extended.

After this change there is no bare literal `"id"` standing in for "the primary
key" anywhere in the data, write, query, or planning paths; each such site reads
the collection's configured primary-key field instead.

## 2. Scope

**In scope.**
- `CollectionOptions.PrimaryKey string` — choose the field at creation time.
- `Collection.PrimaryKey() string` accessor on the public interface.
- Persist the chosen field in the existing per-collection config record.
- Thread the field name through key derivation, indexing, the query layer, and
  the cost-based planner's "sort-is-natural-order" optimization.
- Update the CLI's hardcoded `"id"` projection.
- Tests covering round-trip, indexing, planner correctness, back-compat, and
  immutability.

**Out of scope** (confirmed with requester):
- Composite / multi-field primary keys.
- Nested / dotted-path primary keys (single top-level field only).
- Changing the primary key of an existing collection (it is immutable once set;
  there is no in-place re-key or migration).
- Decoupling the key from the document body (the key is always read from a field
  *inside* each document).

## 3. Background — how `"id"` is wired today

The data-namespace btree key for a document is the anyenc-marshaled **value** of
its `"id"` field — the field *name* never appears on disk. That single fact is
why this change needs no migration: a collection configured with `PrimaryKey:
"id"` is byte-for-byte identical on disk to today, and an old collection opened
after this change resolves its primary key to `"id"` by default.

Hardcoded `"id"` sites (non-test, current source):

| Layer | Site | Role |
|---|---|---|
| Key derivation | `item.go:13` (`newItem`), `item.go:26-27` (`appendId`) | require + marshal the id field |
| Write path | `collection.go:314` (`insertItem`), `collection.go:447` (`update`) | derive data-ns key |
| Write path | `collection.go:408` (`UpsertId`) | `modValue.Set("id", idVal)` on insert-by-id |
| Index | `index.go:179` (`insertKeys`), `index.go:220` (`deleteKeys`) | append id suffix to every index entry |
| Query | `query.go:419` (`Count`), `query.go:564` (`makeQuery`) | `cond.IndexBounds("id", …)` for id-bounds seeks |
| Query | `query.go:645` (`isIDOnlyFilterNode`) | id-only fast-path detection |
| Planner | `planner.go:261`, `planner.go:780` | "sort by `id` == natural btree order, skip sort" |
| CLI | `cmd/any-store-cli2/db.go:938` | always include `"id"` in projection |

`FindId` / `UpdateId` / `UpsertId` / `DeleteId` take `id any` and build the key
directly via `anyenc.AppendAnyValue` — they never read the field name and need
**no change**. Sample docs in `example/example.go` and `wasm/main.go` use the
default `"id"` and need no change.

The planner sites are the only ones where a wrong field name causes **incorrect
results** rather than merely a missed optimization:
- With pk `"uuid"`, sorting by `"uuid"` must be recognized as natural order.
- With pk `"uuid"`, sorting by a field literally named `"id"` (now an ordinary
  field) must **not** be treated as natural order.

Both are fixed by replacing the literal with the configured field.

## 4. Architecture / approach

**Collection-owned primary key, centralized key derivation.** The resolved field
name lives on the `collection` struct. Key derivation becomes a collection
method (the "`collectionPrimaryKey` func" from the request). `item` stays a thin
value wrapper. The planner learns the field via a new `PlanParams` field.

Rejected alternatives:
- **PK on each `item`** (`item{val, pk}`): forces every `item{val: …}` literal
  to thread the name — churn and easy to miss a site.
- **Global/package-level pk**: cannot be per-collection, which is the requirement.

## 5. Public API

In `config.go`:

```go
type CollectionOptions struct {
    Compression Compression
    // PrimaryKey is the document field whose value is the primary key.
    // Empty means "id". Honored only at CreateCollection; immutable after.
    PrimaryKey string
}
```

In `collection.go`, add to the `Collection` interface:

```go
// PrimaryKey returns the document field used as this collection's primary key.
PrimaryKey() string
```

Semantics:
- `CreateCollection(name, CollectionOptions{PrimaryKey: "uuid"})` sets and
  persists the field. Empty ⇒ `"id"`.
- `OpenCollection` resolves the field from persisted config (no opts argument).
- `Collection(name, opts)` (open-or-create convenience): if it **creates**, uses
  `opts`; if it **opens** an existing collection and `opts.PrimaryKey` is
  non-empty and differs from the stored value, returns `ErrPrimaryKeyMismatch`
  (never silently ignores a caller that believes it is changing the key).
- Validation (at create): non-empty after defaulting; no `$` prefix; no `.`
  (single top-level field). Reuse the spirit of `validateIndexField`.

## 6. Persistence (`db.go`)

Extend the existing per-collection config object stored under `collcfg:<name>`:

```go
type collConfig struct {
    Compression Compression
    PrimaryKey  string // "" on load ⇒ resolve to "id"
}
```

- `loadCollConfig` (`db.go:1102`): read `primaryKey` from the anyenc object;
  if absent or empty, leave `""` (caller resolves to `"id"`). Absent key on an
  old record is the back-compat path — no special handling needed.
- `CreateCollection` (`db.go:445`): persist `primaryKey` in the `collcfg`
  object. Write the `primaryKey` field only when it is non-empty and ≠ `"id"`,
  and write the config record at all when **either** compression ≠ 0 **or** the
  primary key is non-default (today it is written only when compression ≠ 0). A
  default-pk collection therefore stores no `primaryKey` and loads as `"id"`.
- `mergeCollOpts` (`db.go:435`): carry `PrimaryKey` (last non-empty wins).
- `renameCollection` already copies the whole `collcfg` blob, so the primary key
  travels with a rename automatically.

`collection.init` (`collection.go:166`) resolves `c.primaryKey = cfg.PrimaryKey`
with a fallback to `"id"` when empty.

## 7. Internal key derivation (`item.go`, `collection.go`, `index.go`)

- Add field `primaryKey string` to `collection`.
- Replace `item.appendId` with a collection method:

  ```go
  // appendId appends the marshaled primary-key value of val to dst.
  func (c *collection) appendId(dst []byte, val *anyenc.Value) []byte
  ```

  reading `val.Get(c.primaryKey)`. Preserve the current invariant (the field is
  always present here because items are validated on construction).
- Replace the free function `newItem(val)` with a collection method
  `c.newItem(val)` that validates `val.Get(c.primaryKey) != nil`, returning
  `ErrDocWithoutId` otherwise. `item` is reduced to `{ val }` + `Value()`.
- All `newItem` callers already hold a `*collection` (directly, via `q.c`, or via
  `buildIndex`), so the conversion is mechanical.
- `index.insertKeys` / `index.deleteKeys` derive the suffix via `idx.c.appendId(
  nil, it.Value())` (the index already holds its collection back-reference).
- `collection.go:408`: `modValue.Set(c.primaryKey, idVal)`.

The `AppendAnyValue(go-value)` vs `Value.MarshalTo()` encoding equivalence that
`FindId` relies on today is unchanged and continues to hold for any pk field.

## 8. Query & planner

**`query.go`:**
- `makeQuery` (`:564`) and `Count` (`:419`): `cond.IndexBounds(q.c.primaryKey, nil)`.
- `isIDOnlyFilterNode` (`:642`): add a `pk string` parameter and compare
  `ft.Path[0] == pk`; `isIDOnlyFilter` passes `q.c.primaryKey`.
- Pass the pk into the planner via a new param (below).

**`internal/qplanner`:**
- Add `PrimaryKey string` to `PlanParams`. Empty ⇒ treat as `"id"` (keeps every
  existing test and internal caller correct without edits).
- Introduce a local resolve at the two sites:
  `pk := params.PrimaryKey; if pk == "" { pk = "id" }`, then compare
  `fields[0].Field == pk` at `planner.go:261` and `planner.go:780`.
- `query.go` sets `PrimaryKey: q.c.primaryKey` in every `qplanner.BuildPlan(
  &qplanner.PlanParams{…})` call (Iter / Update / Delete / Count / Explain).
- Update the now-stale comments at `planner.go:257` and `fullscan_iter.go:75`
  ("id" → "primary key").

No change to `FullScanIter`, `IDBounds` plumbing, or the index range-scan
suffix logic — those already operate on opaque key bytes.

## 9. CLI (`cmd/any-store-cli2/db.go`)

At `:938`, replace `key == "id"` with the collection's primary key
(`coll.PrimaryKey()`), so projections keep the key column for custom-pk
collections.

## 10. Errors (`errors.go`)

- Reuse `ErrDocWithoutId` for "document is missing its primary-key field".
- Add `ErrPrimaryKeyMismatch` for the `Collection()` open-with-conflicting-opts
  case (§5).

## 11. Backward compatibility & immutability

- **Existing collections**: `collcfg` has no `primaryKey` ⇒ resolves to `"id"`
  ⇒ identical behavior. No migration, no format bump.
- **Default for new collections**: `"id"` unless overridden.
- **Immutability**: the field is read from persisted config on every open and is
  never rewritten after creation. There is no API to change it. Attempting to
  "change" it via `Collection(name, opts)` on an existing collection is a
  diagnostic error, not a silent no-op (§5).
- A document may contain both the pk field and an unrelated field named `"id"`;
  the latter is then an ordinary indexable/queryable field with no special status.

## 12. Testing strategy

New `collection_primary_key_test.go` (plus targeted planner cases):

1. **Round-trip, custom pk** (`PrimaryKey: "uuid"`): Insert / FindId / UpdateOne
   / UpdateId / UpsertOne / UpsertId / DeleteId / Count all key off `uuid`.
2. **Non-string pk value** (e.g. integer `key`): confirms any-type values work
   end-to-end, including ordered range scans.
3. **Missing pk field** on Insert ⇒ `ErrDocWithoutId`.
4. **Indexing under custom pk**: secondary index build over existing docs,
   unique-constraint enforcement, and array/multikey entries all carry the
   correct pk suffix; delete removes the right entries.
5. **Planner correctness** (the sharp edge):
   - pk `"uuid"`, `Sort("uuid")` ⇒ uses natural-order full scan (no SortIter);
     assert via `Explain`.
   - pk `"uuid"`, doc also has field `"id"`, `Sort("id")` ⇒ results sorted by the
     `id` field's values, **not** by storage order; assert exact ordering.
   - pk `"uuid"`, filter `{"uuid": {"$in":[…]}}` ⇒ id-bounds seek fast path.
6. **Back-compat**: create a collection with default pk, reopen, assert
   `PrimaryKey() == "id"` and all ops work. (Optionally: open a fixture DB
   written before this change.)
7. **Immutability**: `Collection(name, {PrimaryKey:"a"})` then
   `Collection(name, {PrimaryKey:"b"})` ⇒ `ErrPrimaryKeyMismatch`; same field ⇒
   ok.
8. **Validation**: empty-after-default, `$`-prefixed, and dotted field names
   rejected at create.

## 13. File-by-file change list

| File | Change |
|---|---|
| `config.go` | `CollectionOptions.PrimaryKey` |
| `collection.go` | `collection.primaryKey`; `c.newItem`/`c.appendId`; resolve in `init`; `PrimaryKey()` accessor; `Set(c.primaryKey,…)` at upsert-by-id; interface method |
| `item.go` | reduce `item` to `{val}`; drop `appendId`/`newItem` (moved to collection) |
| `index.go` | suffix via `idx.c.appendId(...)` |
| `query.go` | `IndexBounds(c.primaryKey,…)`, parametrize `isIDOnlyFilter`, set `PlanParams.PrimaryKey` in all `BuildPlan` calls |
| `db.go` | `collConfig.PrimaryKey`; persist in `CreateCollection`; read in `loadCollConfig`; `mergeCollOpts` |
| `errors.go` | `ErrPrimaryKeyMismatch` |
| `internal/qplanner/planner.go` | `PlanParams.PrimaryKey`; resolve+compare at `:261`,`:780`; comment at `:257` |
| `internal/qplanner/fullscan_iter.go` | comment at `:75` |
| `cmd/any-store-cli2/db.go` | projection uses `coll.PrimaryKey()` |
| `collection_primary_key_test.go` (new) | §12 |

## 14. Risks & edge cases

- **Planner default**: forgetting `if pk == "" { pk = "id" }` would break the
  natural-order optimization for the many internal/test `BuildPlan` callers that
  don't set the field. The empty-string fallback is mandatory and tested.
- **`isIDOnlyFilter` parametrization**: must compare against the live pk, not a
  captured default, or the id-only fast path silently misfires on custom-pk
  collections.
- **Encoding equivalence**: any-type pk values depend on `AppendAnyValue` and
  `MarshalTo` agreeing; this is already an invariant for `"id"` and is covered by
  the round-trip tests for non-string keys.
- **No new on-disk version**: because the key format is unchanged, there is no
  reader/writer compatibility gate to add.
