# Configurable Collection Primary Key — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let each collection choose which top-level document field is its primary key (default `"id"`), set once at creation and immutable, with no on-disk format change and no migration.

**Architecture:** The resolved primary-key field name lives on the `collection` struct (`c.primaryKey`), persisted in the existing `collcfg:<name>` config record. Key derivation is centralized in two collection methods (`c.newItem` / `c.appendId`); `item` becomes a thin value wrapper. The query layer and cost-based planner read the field instead of the literal `"id"`. A read-your-own-writes fix in `collection.init` lets a freshly created collection observe its own just-written config.

**Tech Stack:** Go 1.25, `anyenc` (custom order-preserving binary encoding), the in-tree `internal/btree` engine, `internal/qplanner` cost-based planner, `testify` for tests.

**Spec:** `docs/specs/2026-06-16-collection-primary-key-design.md`

---

## Conventions

- **Package:** library code and tests are package `anystore` at the repo root. The global test `ctx` and `newFixture(t)` / `newFixturePath(t, dir)` helpers live in `db_test.go`.
- **Run root tests:** `go test .` (single test: `go test -run TestName -v .`).
- **Run CLI tests:** the CLI is a separate module joined via `go.work`; run with `(cd cmd/any-store-cli2 && go test ./...)`.
- **A failing-to-compile test counts as the red step** in Go — when a step adds a test that references a not-yet-existing symbol, "Expected: FAIL" means a build error naming that symbol.
- **Commits:** conventional commits, one per task.
- All new library tests go in a single new file `collection_primary_key_test.go` (created in Task 1, appended to in later tasks).

---

## Task 1: Per-collection primary-key config (plumbing, accessor, persistence, immutability)

Adds `CollectionOptions.PrimaryKey`, persists it in `collcfg:<name>`, resolves it onto `c.primaryKey` (default `"id"`), exposes `Collection.PrimaryKey()`, validates it, enforces immutability via `ErrPrimaryKeyMismatch`, and fixes `init()` to read config through the write tx so a just-created collection sees its own config (read-your-writes). The data path still keys off the literal `"id"` after this task — that is wired in Task 2.

**Files:**
- Modify: `config.go` (`CollectionOptions`)
- Modify: `errors.go` (new error)
- Modify: `db.go` (`collConfig`, `mergeCollOpts`, `CreateCollection`, `Collection`, `loadCollConfig`)
- Modify: `collection.go` (imports, struct field, `init`, `PrimaryKey()` + interface, `validatePrimaryKey`)
- Test: `collection_primary_key_test.go` (new)

- [ ] **Step 1: Write the failing tests**

Create `collection_primary_key_test.go`:

```go
package anystore

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollection_PrimaryKey_DefaultId(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, "id", coll.PrimaryKey())
}

func TestCollection_PrimaryKey_Custom(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	assert.Equal(t, "uuid", coll.PrimaryKey())
}

func TestCollection_PrimaryKey_PersistedAcrossReopen(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.db")

	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	_, err = db.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, db.Close())

	db2, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer db2.Close()
	coll, err := db2.OpenCollection(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, "uuid", coll.PrimaryKey())
}

func TestCollection_PrimaryKey_Validation(t *testing.T) {
	fx := newFixture(t)
	_, err := fx.CreateCollection(ctx, "c1", CollectionOptions{PrimaryKey: "$bad"})
	require.Error(t, err)
	_, err = fx.CreateCollection(ctx, "c2", CollectionOptions{PrimaryKey: "a.b"})
	require.Error(t, err)
}

func TestCollection_PrimaryKey_ImmutableMismatch(t *testing.T) {
	fx := newFixture(t)
	_, err := fx.Collection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	_, err = fx.Collection(ctx, "c", CollectionOptions{PrimaryKey: "other"})
	assert.ErrorIs(t, err, ErrPrimaryKeyMismatch)
	// Re-opening with the same key (or none) is fine.
	_, err = fx.Collection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestCollection_PrimaryKey -v .`
Expected: FAIL — build error `coll.PrimaryKey undefined`, `unknown field PrimaryKey in struct literal of type CollectionOptions`, `undefined: ErrPrimaryKeyMismatch`.

- [ ] **Step 3: Add `CollectionOptions.PrimaryKey`**

In `config.go`, replace the `CollectionOptions` struct:

```go
// CollectionOptions configures per-collection settings at creation time.
type CollectionOptions struct {
	// Compression overrides the database-wide compression setting for this collection.
	// Zero value inherits the database default.
	Compression Compression

	// PrimaryKey is the document field whose value is the collection's primary
	// key. Empty defaults to "id". Honored only at CreateCollection; the primary
	// key is immutable once the collection exists.
	PrimaryKey string
}
```

- [ ] **Step 4: Add `ErrPrimaryKeyMismatch`**

In `errors.go`, add inside the `var (...)` block, right after the `ErrDocWithoutId` declaration:

```go
	// ErrPrimaryKeyMismatch is returned by Collection when the requested
	// PrimaryKey option conflicts with an existing collection's stored primary
	// key. The primary key is immutable after creation.
	ErrPrimaryKeyMismatch = errors.New("any-store: primary key mismatch")
```

- [ ] **Step 5: Extend `collConfig`, `mergeCollOpts`, and `loadCollConfig`**

In `db.go`, replace the `collConfig` struct:

```go
type collConfig struct {
	Compression Compression
	PrimaryKey  string
}
```

Replace `mergeCollOpts`:

```go
func mergeCollOpts(opts []CollectionOptions) CollectionOptions {
	var merged CollectionOptions
	for _, o := range opts {
		if o.Compression != 0 {
			merged.Compression = o.Compression
		}
		if o.PrimaryKey != "" {
			merged.PrimaryKey = o.PrimaryKey
		}
	}
	return merged
}
```

In `loadCollConfig`, add the primary-key read right after the compression line:

```go
	cfg.Compression = Compression(val.GetInt("compression"))
	cfg.PrimaryKey = val.GetString("primaryKey")
	return cfg, nil
```

(`GetString` returns `""` when the key is absent — the back-compat default.)

- [ ] **Step 6: Validate + persist the primary key in `CreateCollection`**

In `db.go` `CreateCollection`, immediately after `merged := mergeCollOpts(opts)` add validation (note this function returns `(Collection, error)`):

```go
	merged := mergeCollOpts(opts)
	pk := merged.PrimaryKey
	if pk == "" {
		pk = "id"
	}
	if err := validatePrimaryKey(pk); err != nil {
		return nil, err
	}
```

Then replace the existing persist block (the `if merged.Compression != 0 { ... }` that writes `collConfigKey`) with:

```go
		// Persist per-collection config when any setting is non-default.
		if merged.Compression != 0 || pk != "id" {
			var a anyenc.Arena
			obj := a.NewObject()
			if merged.Compression != 0 {
				obj.Set("compression", a.NewNumberInt(int(merged.Compression)))
			}
			if pk != "id" {
				obj.Set("primaryKey", a.NewString(pk))
			}
			if err = tx.Put(db.systemNS, collConfigKey(collectionName), obj.MarshalTo(nil)); err != nil {
				return err
			}
		}
```

- [ ] **Step 7: Enforce immutability in `Collection`**

In `db.go`, replace the success arm at the top of `Collection`:

```go
func (db *db) Collection(ctx context.Context, collectionName string, opts ...CollectionOptions) (Collection, error) {
	coll, err := db.OpenCollection(ctx, collectionName)
	if err == nil {
		// Existing collection: a conflicting PrimaryKey option is a misuse, not a
		// silent no-op — the primary key is immutable after creation.
		if merged := mergeCollOpts(opts); merged.PrimaryKey != "" && merged.PrimaryKey != coll.PrimaryKey() {
			return nil, ErrPrimaryKeyMismatch
		}
		return coll, nil
	}
	if !errors.Is(err, ErrCollectionNotFound) {
		return nil, err
	}
	coll, err = db.CreateCollection(ctx, collectionName, opts...)
	if err == nil {
		return coll, nil
	}
	if !errors.Is(err, ErrCollectionExists) {
		return nil, err
	}
	return db.OpenCollection(ctx, collectionName)
}
```

- [ ] **Step 8: Add the `primaryKey` field, imports, validator, accessor, and interface method in `collection.go`**

Add `"fmt"` and `"strings"` to the import block:

```go
import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)
```

In the `Collection` interface, add the accessor right after the `Name() string` method:

```go
	// Name returns the name of the collection.
	Name() string

	// PrimaryKey returns the document field used as this collection's primary key.
	PrimaryKey() string
```

In the `collection` struct, add the field next to `compression`:

```go
	compression Compression // 0 = use db default

	// primaryKey is the document field whose value is the btree key. Resolved
	// once in init (default "id") and never mutated after, so the data and query
	// paths read it lock-free.
	primaryKey string
```

Add the validator and accessor (place the accessor right after the `Name()` method, and the validator near it):

```go
func (c *collection) PrimaryKey() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.primaryKey
}

// validatePrimaryKey checks a primary-key field name: non-empty, no "$" prefix,
// and a single top-level field (no "." path separators).
func validatePrimaryKey(s string) error {
	if s == "" {
		return fmt.Errorf("any-store: primary key field is empty")
	}
	if strings.HasPrefix(s, "$") {
		return fmt.Errorf("any-store: invalid primary key field name: %s", s)
	}
	if strings.Contains(s, ".") {
		return fmt.Errorf("any-store: primary key must be a single top-level field: %s", s)
	}
	return nil
}
```

- [ ] **Step 9: Make `init` read config through the write tx (read-your-writes) and resolve the primary key**

This is the fix for the create-handle visibility gap: during `CreateCollection` the config is written in an uncommitted write tx, so a fresh read tx cannot see it. Read through `wtx` when present (mirroring how `getNamespace` already uses `wtx`); keep the fresh-read path for the open case.

This aligns any-store with SQLite's read-your-writes model. In SQLite a connection has exactly one transaction state per database (`sqlite3BtreeBeginTrans`, btree.c:3811-3831) and every read during a write transaction goes through the single shared page cache (`getPageNormal`, pager.c:5552-5573), so reads always observe the writer's uncommitted dirty pages — including catalog/schema writes (a freshly written `sqlite_schema` row is read back during the in-transaction reparse, build.c → vdbe.c OP_ParseSchema). SQLite has no equivalent of opening a fresh, independent, committed-snapshot read inside an open write transaction; any-store's sub-read that misses the writer's config was effectively behaving like a separate connection — the bug.

In `collection.go`, replace the whole body of `init` (from the `getNamespace` setup through the end) with:

```go
func (c *collection) init(ctx context.Context, wtx *btree.WriteTx) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// getNamespace resolves a namespace, using the writer path when available.
	getNamespace := func(name string) (*btree.Namespace, error) {
		if wtx != nil {
			return wtx.GetNamespace(name)
		}
		return c.db.btreeDB.GetNamespace(name)
	}

	// Get the namespace for this collection
	ns, err := getNamespace(c.name)
	if err != nil {
		return err
	}
	c.ns = ns

	// load reads per-collection config + index metadata. When invoked within a
	// write tx (CreateCollection) it MUST read through the writer's own view so
	// it observes config written earlier in this same uncommitted tx — a fresh
	// read tx would see only committed state and miss it (read-your-writes).
	load := func(tx *btree.ReadTx) (err error) {
		cfg, err := c.db.loadCollConfig(tx, c.name)
		if err != nil {
			return err
		}
		c.compression = cfg.Compression
		c.primaryKey = cfg.PrimaryKey
		if c.primaryKey == "" {
			c.primaryKey = "id"
		}

		idxInfos, err := c.db.getIndexInfos(tx, c.name)
		if err != nil {
			return err
		}
		var idxs []*index
		for _, info := range idxInfos {
			nsName := indexNsName(c.name, info.Name)
			ns, nsErr := getNamespace(nsName)
			if nsErr != nil {
				return nsErr
			}
			idx, idxErr := newIndex(c, info, ns)
			if idxErr != nil {
				return idxErr
			}
			c.loadSketchAtOpen(tx, idx)
			idxs = append(idxs, idx)
		}
		c.storeIndexes(idxs)
		return nil
	}

	if wtx != nil {
		return load(&wtx.ReadTx)
	}
	return c.db.doReadTx(ctx, load)
}
```

- [ ] **Step 10: Run the new tests to verify they pass**

Run: `go test -run TestCollection_PrimaryKey -v .`
Expected: PASS (all five tests).

- [ ] **Step 11: Run the full package suite (regression — incl. per-collection compression now applying on the create handle)**

Run: `go test .`
Expected: PASS (`ok  github.com/anyproto/any-store/v2`).

- [ ] **Step 12: Commit**

```bash
git add config.go errors.go db.go collection.go collection_primary_key_test.go
git commit -m "feat(collection): persist + expose configurable primary key (default id)"
```

---

## Task 2: Centralize key derivation on the collection (consume `c.primaryKey`)

Replaces the free functions `newItem` / `item.appendId` (which hardcode `"id"`) with collection methods that use `c.primaryKey`, and routes the data + index + update paths through them. After this task, inserts/finds/updates/deletes and indexing all key off the configured field.

**Files:**
- Modify: `item.go` (reduce `item` to a value wrapper)
- Modify: `collection.go` (`newItem`/`appendId` methods + call sites)
- Modify: `index.go` (`insertKeys`, `deleteKeys`)
- Modify: `query.go` (`Update` item construction)
- Test: `collection_primary_key_test.go` (append)

- [ ] **Step 1: Write the failing tests**

Append to `collection_primary_key_test.go` (and ensure `anyenc` is imported — add `"github.com/anyproto/any-store/v2/anyenc"` to the file's import block):

```go
func TestCollection_PrimaryKey_RoundTrip(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","n":1}`),
		anyenc.MustParseJson(`{"uuid":"b","n":2}`),
	))
	assertCollCount(t, coll, 2)

	doc, err := coll.FindId(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, 1, doc.Value().GetInt("n"))

	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"uuid":"a","n":11}`)))
	doc, err = coll.FindId(ctx, "a")
	require.NoError(t, err)
	assert.Equal(t, 11, doc.Value().GetInt("n"))

	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"uuid":"b","n":22}`)))
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"uuid":"c","n":3}`)))
	assertCollCount(t, coll, 3)

	require.NoError(t, coll.DeleteId(ctx, "c"))
	assertCollCount(t, coll, 2)

	// A document missing the primary-key field is rejected.
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"x","n":9}`))
	assert.ErrorIs(t, err, ErrDocWithoutId)
}

func TestCollection_PrimaryKey_IntValue(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "key"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"key":2,"n":20}`),
		anyenc.MustParseJson(`{"key":1,"n":10}`),
	))
	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, 10, doc.Value().GetInt("n"))
}

func TestCollection_PrimaryKey_Index(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","color":"red"}`),
		anyenc.MustParseJson(`{"uuid":"b","color":"red"}`),
		anyenc.MustParseJson(`{"uuid":"c","color":"blue"}`),
	))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"color"}}))

	n, err := coll.Find(`{"color":"red"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	// Deleting a doc must remove its index entry (entry suffix is the uuid key).
	require.NoError(t, coll.DeleteId(ctx, "a"))
	n, err = coll.Find(`{"color":"red"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run 'TestCollection_PrimaryKey_RoundTrip|TestCollection_PrimaryKey_IntValue|TestCollection_PrimaryKey_Index' -v .`
Expected: FAIL — `Insert` returns `ErrDocWithoutId` because the still-hardcoded `newItem` looks for `"id"` and the docs only have `"uuid"`/`"key"`.

- [ ] **Step 3: Reduce `item` to a value wrapper**

Replace the entire contents of `item.go`:

```go
package anystore

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

// item wraps a document value. Key derivation and validation are collection
// responsibilities (see collection.newItem / collection.appendId), because the
// primary-key field name is per-collection.
type item struct {
	val *anyenc.Value
}

func (i item) Value() *anyenc.Value {
	return i.val
}
```

- [ ] **Step 3b: Add test-only key-derivation shims (keep existing low-level tests compiling)**

Several existing internal tests build an `item` directly and derive its id: `item_test.go`, `index_test.go` (lines 17, 369, 454), `index_unique_sparse_test.go` (line 1073), and `iterator_test.go` (lines 197, 199, 304, 306). They call the free `newItem(val)` and the `item.appendId(dst)` method that Step 3 removed from production. Re-provide them as **test-only** helpers defaulting to `"id"` — production derives keys via `collection.newItem`/`collection.appendId`, which honor the per-collection primary key.

Create `item_compat_test.go`:

```go
package anystore

import "github.com/anyproto/any-store/v2/anyenc"

// newItem is a TEST-ONLY helper that wraps a value as an item keyed by the
// default "id" field (the pre-configurable-primary-key contract). It lets
// low-level index/iterator tests build items without a collection. Production
// derives keys via collection.newItem / collection.appendId.
func newItem(val *anyenc.Value) (item, error) {
	objVal, err := val.Object()
	if err != nil {
		return item{}, err
	}
	if objVal.Get("id") == nil {
		return item{}, ErrDocWithoutId
	}
	return item{val: val}, nil
}

// appendId is a TEST-ONLY mirror of collection.appendId for the default "id" key.
func (i item) appendId(dst []byte) []byte {
	return i.val.Get("id").MarshalTo(dst)
}
```

Because production no longer references the free `newItem` or `item.appendId`, the non-test `go build ./...` is the guard that every production call site was converted.

- [ ] **Step 4: Add `newItem` / `appendId` methods on `collection`**

In `collection.go`, add these methods (e.g. right after `compressionDisabled`):

```go
// newItem wraps a document value, validating that it carries this collection's
// primary-key field. Returns ErrDocWithoutId when the field is absent.
func (c *collection) newItem(val *anyenc.Value) (item, error) {
	objVal, err := val.Object()
	if err != nil {
		return item{}, err
	}
	if objVal.Get(c.primaryKey) == nil {
		return item{}, ErrDocWithoutId
	}
	return item{val: val}, nil
}

// appendId appends the marshaled primary-key value of val to dst. The field is
// always present here because items are validated on construction.
func (c *collection) appendId(dst []byte, val *anyenc.Value) []byte {
	idVal := val.Get(c.primaryKey)
	if idVal == nil {
		panic("document without primary key")
	}
	return idVal.MarshalTo(dst)
}
```

- [ ] **Step 5: Route `collection.go` call sites through the new methods**

Make these exact replacements in `collection.go`:

In `Insert`: `if it, txErr = newItem(doc); txErr != nil {` → `if it, txErr = c.newItem(doc); txErr != nil {`

In `insertItem`: `buf.SmallBuf = it.appendId(buf.SmallBuf[:0])` → `buf.SmallBuf = c.appendId(buf.SmallBuf[:0], it.Value())`

In `UpdateOne`: `if it, err = newItem(doc); err != nil {` → `if it, err = c.newItem(doc); err != nil {`

In `UpsertId`: `modValue.Set("id", idVal)` → `modValue.Set(c.primaryKey, idVal)`

In `update`: `buf.SmallBuf = it.appendId(buf.SmallBuf[:0])` → `buf.SmallBuf = c.appendId(buf.SmallBuf[:0], it.Value())`

In `loadById`: `return newItem(doc)` → `return c.newItem(doc)`

In `UpsertOne`: `if it, err = newItem(doc); err != nil {` → `if it, err = c.newItem(doc); err != nil {`

In `buildIndex`: `it, err := newItem(doc)` → `it, err := c.newItem(doc)`

In `loadByIdRead`: `return newItem(doc)` → `return c.newItem(doc)`

(Leave the two `item{val: data}` literals in `FindIdWithParser` unchanged — those are read-path wrappers that need no validation.)

- [ ] **Step 6: Route `index.go` call sites through `idx.c.appendId`**

In `index.go`, in both `insertKeys` and `deleteKeys`:

`idKey := it.appendId(nil)` → `idKey := idx.c.appendId(nil, it.Value())`

- [ ] **Step 7: Route `query.go` `Update` through `q.c.newItem`**

In `query.go` `Update`:

`oldItem, itemErr := newItem(doc)` → `oldItem, itemErr := q.c.newItem(doc)`

`if it, err = newItem(modifiedVal); err != nil {` → `if it, err = q.c.newItem(modifiedVal); err != nil {`

- [ ] **Step 8: Run the new tests to verify they pass**

Run: `go test -run 'TestCollection_PrimaryKey_RoundTrip|TestCollection_PrimaryKey_IntValue|TestCollection_PrimaryKey_Index' -v .`
Expected: PASS.

- [ ] **Step 9: Run the full package suite (regression for default-"id" collections)**

Run: `go test .`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add item.go collection.go index.go query.go collection_primary_key_test.go
git commit -m "feat(collection): derive document key from configured primary key"
```

---

## Task 3: Query id-bounds + id-only fast path use the primary key

`query.go` still asks the filter for bounds on the literal `"id"`. On a custom-pk collection this both misses the fast path for the real key and — worse — wrongly treats a filter on a field literally named `"id"` as a primary-key seek. Route both through `c.primaryKey`.

**Files:**
- Modify: `query.go` (`makeQuery`, `Count`, `isIDOnlyFilter` / `isIDOnlyFilterNode`)
- Test: `collection_primary_key_test.go` (append)

- [ ] **Step 1: Write the failing test**

Append to `collection_primary_key_test.go`:

```go
func TestCollection_PrimaryKey_FilterByNonPkId(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","id":5}`),
		anyenc.MustParseJson(`{"uuid":"b","id":6}`),
	))

	// "id" is an ordinary field here (NOT the primary key). It must match by
	// value, not be treated as a data-namespace key seek.
	n, err := coll.Find(`{"id":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	iter, err := coll.Find(`{"id":5}`).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var got []string
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		got = append(got, d.Value().GetString("uuid"))
	}
	require.NoError(t, iter.Err())
	assert.Equal(t, []string{"a"}, got)

	// Filtering by the real primary key still works.
	n, err = coll.Find(`{"uuid":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -run TestCollection_PrimaryKey_FilterByNonPkId -v .`
Expected: FAIL — `Count` is `0` and the iterator yields nothing, because `{"id":5}` is taken as a primary-key seek into the uuid-keyed data namespace.

- [ ] **Step 3: Use `c.primaryKey` for id bounds in `makeQuery` and `Count`**

In `query.go` `makeQuery`, replace:

```go
	// handle the primary-key field
	if idBounds := q.cond.IndexBounds(q.c.primaryKey, nil); len(idBounds) != 0 {
		qb.idBounds = idBounds
	}
```

In `query.go` `Count`, replace the `idBounds` computation:

```go
	// Compute idBounds only if filter references the primary-key field
	var idBounds query.Bounds
	if ib := q.cond.IndexBounds(q.c.primaryKey, nil); len(ib) != 0 {
		idBounds = ib
	}
```

- [ ] **Step 4: Parametrize the id-only fast-path detection**

In `query.go`, replace `isIDOnlyFilter` and `isIDOnlyFilterNode`:

```go
// isIDOnlyFilter returns true if the filter only references the primary-key
// field with equality or $in conditions (all fixed bounds). This enables a fast
// path that skips CBO planning entirely for simple primary-key lookups.
func (q *collQuery) isIDOnlyFilter() bool {
	return isIDOnlyFilterNode(q.cond, q.c.primaryKey)
}

func isIDOnlyFilterNode(f query.Filter, pk string) bool {
	switch ft := f.(type) {
	case query.Key:
		return len(ft.Path) == 1 && ft.Path[0] == pk
	case query.And:
		// All children must be primary-key-only
		for _, child := range ft {
			if !isIDOnlyFilterNode(child, pk) {
				return false
			}
		}
		return len(ft) > 0
	case *query.And:
		return isIDOnlyFilterNode(*ft, pk)
	default:
		return false
	}
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `go test -run TestCollection_PrimaryKey_FilterByNonPkId -v .`
Expected: PASS.

- [ ] **Step 6: Run the full package suite**

Run: `go test .`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add query.go collection_primary_key_test.go
git commit -m "feat(query): resolve id bounds against the configured primary key"
```

---

## Task 4: Planner natural-order optimization uses the primary key

The planner treats `Sort` by the literal `"id"` as "already in natural btree order, skip the sort". On a custom-pk collection this is a correctness bug: sorting by a non-pk field named `"id"` would skip the real sort and return storage order. Thread the field through `PlanParams.PrimaryKey` (empty ⇒ `"id"`, so existing callers/tests are unaffected).

**Files:**
- Modify: `internal/qplanner/planner.go` (`PlanParams`, two sort checks, comment)
- Modify: `internal/qplanner/fullscan_iter.go` (comment)
- Modify: `query.go` (set `PrimaryKey` in all five `BuildPlan` calls)
- Test: `collection_primary_key_test.go` (append)

- [ ] **Step 1: Write the failing test**

Append to `collection_primary_key_test.go`:

```go
// sortedInts runs Find(nil).Sort(sortField) and collects intField from each doc.
func sortedInts(t *testing.T, coll Collection, sortField, intField string) []int {
	t.Helper()
	iter, err := coll.Find(nil).Sort(sortField).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []int
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		out = append(out, d.Value().GetInt(intField))
	}
	require.NoError(t, iter.Err())
	return out
}

// sortedStrings runs Find(nil).Sort(sortField) and collects strField from each doc.
func sortedStrings(t *testing.T, coll Collection, sortField, strField string) []string {
	t.Helper()
	iter, err := coll.Find(nil).Sort(sortField).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []string
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		out = append(out, d.Value().GetString(strField))
	}
	require.NoError(t, iter.Err())
	return out
}

func TestCollection_PrimaryKey_SortNonPkId(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c", CollectionOptions{PrimaryKey: "uuid"})
	require.NoError(t, err)
	// Storage order is by uuid (a,b,c); their ids 3,1,2 are deliberately NOT in
	// storage order, so a wrong "natural order" optimization is observable.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"uuid":"a","id":3}`),
		anyenc.MustParseJson(`{"uuid":"b","id":1}`),
		anyenc.MustParseJson(`{"uuid":"c","id":2}`),
	))

	// Sorting by the non-pk field "id" must sort by value, not storage order.
	assert.Equal(t, []int{1, 2, 3}, sortedInts(t, coll, "id", "id"))

	// Sorting by the real primary key yields natural (storage) order.
	assert.Equal(t, []string{"a", "b", "c"}, sortedStrings(t, coll, "uuid", "uuid"))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -run TestCollection_PrimaryKey_SortNonPkId -v .`
Expected: FAIL — `sortedInts(... "id" ...)` returns `[3 1 2]` (storage order) instead of `[1 2 3]`, because the planner treats `Sort("id")` as natural order.

- [ ] **Step 3: Add `PrimaryKey` to `PlanParams`**

In `internal/qplanner/planner.go`, add the field to the `PlanParams` struct (next to `IDBounds`):

```go
	IDBounds query.Bounds

	// PrimaryKey is the collection's primary-key field. Empty ⇒ "id". A single
	// Sort on this field needs no SortIter because a full scan already yields
	// primary-key order.
	PrimaryKey string
```

- [ ] **Step 4: Use it in the cost estimate (around the first sort check)**

In `internal/qplanner/planner.go`, replace the block that begins with the `// FullScan naturally reads in ID order` comment:

```go
	// FullScan naturally reads in primary-key order, so sorting by the primary
	// key is free.
	fullScanNeedSort := needSort
	if needSort {
		fields := params.Sorter.Fields()
		pk := params.PrimaryKey
		if pk == "" {
			pk = "id"
		}
		if len(fields) == 1 && fields[0].Field == pk {
			fullScanNeedSort = false
		}
	}
```

- [ ] **Step 5: Use it in the iterator build (`buildFullScanChain`)**

In `internal/qplanner/planner.go` `buildFullScanChain`, replace the sort check:

```go
	idSorted := false
	if needSort {
		fields := params.Sorter.Fields()
		pk := params.PrimaryKey
		if pk == "" {
			pk = "id"
		}
		if len(fields) == 1 && fields[0].Field == pk {
			idSorted = true
			fsi := &FullScanIter{
				Source: &CursorSource{
					Tx: params.Tx,
					Ns: params.DataNs,
				},
				Filter:   params.Filter,
				IDBounds: params.IDBounds,
				Buf:      params.Buf,
				Reverse:  fields[0].Reverse,
			}
```

(Only the addition of the `pk := …; if pk == "" …` lines and the `fields[0].Field == pk` comparison changes; the `fsi := &FullScanIter{...}` body is unchanged.)

- [ ] **Step 6: Update the stale comment in `fullscan_iter.go`**

In `internal/qplanner/fullscan_iter.go`, change the comment at the noted line:

`// FullScan walks the data namespace; docId is the primary key — unique by construction.`

(no code change — keep the wording accurate; it already says "primary key", leave as-is if so).

- [ ] **Step 7: Pass `PrimaryKey` from `query.go` into every `BuildPlan`**

In `query.go`, add `PrimaryKey: q.c.primaryKey,` to each `qplanner.PlanParams{...}` literal — there are five, in `Iter`, `Update`, `Delete`, `Count`, and `Explain`. Add it next to the existing `IDBounds:` line in each, e.g.:

```go
			IDBounds:    qb.idBounds,
			PrimaryKey:  q.c.primaryKey,
```

and in `Count` (which uses the local `idBounds`):

```go
			IDBounds:    idBounds,
			PrimaryKey:  q.c.primaryKey,
```

- [ ] **Step 8: Run the test to verify it passes**

Run: `go test -run TestCollection_PrimaryKey_SortNonPkId -v .`
Expected: PASS.

- [ ] **Step 9: Run the full package suite (regression for default-"id" sort optimization)**

Run: `go test .`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add internal/qplanner/planner.go internal/qplanner/fullscan_iter.go query.go collection_primary_key_test.go
git commit -m "feat(qplanner): natural-order sort optimization honors the primary key"
```

---

## Task 5: CLI projection preserves the primary-key field

The CLI's `applyProjection` always re-includes `"id"` on inclusion projections. Thread the collection's primary key through so a custom-pk collection keeps its key column.

**Files:**
- Modify: `cmd/any-store-cli2/db.go` (`applyProjection`, `printDoc`, three call sites)
- Test: `cmd/any-store-cli2/projection_test.go` (new)

- [ ] **Step 1: Write the failing test**

Create `cmd/any-store-cli2/projection_test.go`:

```go
package main

import (
	"encoding/json"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestApplyProjection_PrimaryKey(t *testing.T) {
	val := anyenc.MustParseJson(`{"uuid":"x","name":"n","extra":"e"}`)
	out, err := applyProjection(val, json.RawMessage(`{"name":1}`), "uuid")
	if err != nil {
		t.Fatal(err)
	}
	if out.Get("uuid") == nil {
		t.Fatalf("primary key field 'uuid' must be retained by an inclusion projection")
	}
	if out.Get("name") == nil {
		t.Fatalf("'name' must be included")
	}
	if out.Get("extra") != nil {
		t.Fatalf("'extra' must be excluded")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `(cd cmd/any-store-cli2 && go test -run TestApplyProjection_PrimaryKey ./...)`
Expected: FAIL — build error: too many arguments to `applyProjection` (it currently takes two args).

- [ ] **Step 3: Add the `pkField` parameter to `applyProjection`**

In `cmd/any-store-cli2/db.go`, change the signature and the inclusion check:

```go
func applyProjection(val *anyenc.Value, projection json.RawMessage, pkField string) (*anyenc.Value, error) {
```

and inside the inclusion branch:

```go
			if projMap[key] > 0 || key == pkField {
				newVal.Set(key, v)
			}
```

- [ ] **Step 4: Thread `pkField` through `printDoc` and the call sites**

Change `printDoc`'s signature and its internal call:

```go
func (c *Conn) printDoc(doc anystore.Doc, query Query, pkField string) error {
```
```go
		if val, err = applyProjection(val, query.Project, pkField); err != nil {
```

Update the three call sites:
- In `FindId`: `if val, err = applyProjection(val, cmd.Query.Project); err != nil {` → `if val, err = applyProjection(val, cmd.Query.Project, coll.PrimaryKey()); err != nil {`
- In `FindOne`: `err = c.printDoc(doc, cmd.Query)` → `err = c.printDoc(doc, cmd.Query, coll.PrimaryKey())`
- In `Find` (both `c.printDoc(doc, query)` occurrences): → `c.printDoc(doc, query, coll.PrimaryKey())`

- [ ] **Step 5: Run the CLI test to verify it passes**

Run: `(cd cmd/any-store-cli2 && go test -run TestApplyProjection_PrimaryKey ./...)`
Expected: PASS.

- [ ] **Step 6: Build the CLI module (workspace, against in-tree library)**

Run: `(cd cmd/any-store-cli2 && go build ./...)`
Expected: no output (success).

- [ ] **Step 7: Commit**

```bash
git add cmd/any-store-cli2/db.go cmd/any-store-cli2/projection_test.go
git commit -m "feat(cli): keep the primary-key field in projections"
```

---

## Task 6: Final verification

- [ ] **Step 1: Vet and full library suite**

Run: `go vet ./... && go test ./...`
Expected: PASS for the root module (`ok  github.com/anyproto/any-store/v2`).

- [ ] **Step 2: CLI module vet + tests**

Run: `(cd cmd/any-store-cli2 && go vet ./... && go test ./...)`
Expected: PASS.

- [ ] **Step 3: Confirm no stray hardcoded primary-key literals remain**

Run: `grep -rn '"id"' --include=*.go . | grep -v _test.go | grep -v '/.claude/' | grep -v anytype-desktop-suite`
Expected: only legitimate occurrences remain — `example/example.go` and `wasm/main.go` sample docs (which intentionally use the default `"id"`). There must be no remaining `"id"` in `item.go`, `collection.go`, `index.go`, `query.go`, `internal/qplanner/planner.go`, or the CLI projection.

- [ ] **Step 4 (optional): tidy spec/plan status**

If desired, update the spec `Status:` line to `Implemented` and commit.

---

## Self-review notes

- **Spec coverage:** §5 API → Task 1 (Steps 3,7,8) + Task 2 (`UpsertId` set). §6 persistence → Task 1 (Steps 5,6,9). §7 key derivation → Task 2. §8 query+planner → Tasks 3,4. §9 CLI → Task 5. §10 errors → Task 1 (Step 4) + reused `ErrDocWithoutId` in Task 2. §11 back-compat/immutability → Task 1 (reopen + mismatch tests) + the `init`-via-`wtx` read-your-writes fix (Step 9). §12 tests → Tasks 1–5. §13 file list → all tasks. §14 risks: the empty-`PrimaryKey` ⇒ `"id"` fallback is implemented in both `init` (Task 1) and `PlanParams` (Task 4) and explicitly tested.
- **Type consistency:** `c.newItem(*anyenc.Value) (item, error)` and `c.appendId([]byte, *anyenc.Value) []byte` are used identically across `collection.go`, `index.go`, `query.go`; `isIDOnlyFilterNode(query.Filter, string)`; `applyProjection(*anyenc.Value, json.RawMessage, string)`; `printDoc(anystore.Doc, Query, string)`.
- **No placeholders:** every code step shows complete code; every run step shows the command and expected result.
