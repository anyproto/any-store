# Migrating from v1 to v2

v2 uses a new storage engine and a new on-disk format. It **cannot open a v1
database file**. Moving to v2 means copying the data into a new file.

## Requirements

Migrate with **v1 ≥ v1.0.1**. Releases up to v0.4.7 read index metadata with
`Sparse` and `Unique` swapped, so a migration driven by `GetIndexes()` builds
unique indexes over fields that were never unique — failing on the first
duplicate — and silently drops the constraints that genuinely were unique.

## What changed

|             | v1                                   | v2                                 |
| ----------- | ------------------------------------ | ---------------------------------- |
| Import path | `github.com/anyproto/any-store`      | `github.com/anyproto/any-store/v2` |
| Engine      | SQLite, via `anyproto/go-sqlite`     | in-tree btree/pager/WAL, pure Go   |
| File header | `SQLite format 3\0`                  | `any-store v2\0`, zero-padded to 16 bytes |
| Minimum Go  | 1.24                                 | 1.25                               |

The two are separate Go modules, so one binary may import both. That is what
makes an in-process migration possible.

## Detecting an already-migrated file

v1 reports a v2 file explicitly instead of failing with SQLite's opaque
"file is not a database". Both the current magic and the `BTree format 1\0` one
written by earlier v2 builds are recognised:

```go
_, err := v1.Open(ctx, path, nil)
if errors.Is(err, v1.ErrV2Database) {
	// already v2 — nothing to migrate
}
```

## Migrating

Documents are carried across as raw `anyenc` bytes. v2's `anyenc` is a superset
of v1's — the type tags and layouts of types 1–8 are identical, and v1 never
emitted anything else — so v1-encoded values parse unchanged in v2 with no
re-encoding.

```go
import (
	"context"
	"errors"
	"fmt"
	"os"

	v1 "github.com/anyproto/any-store"
	v2 "github.com/anyproto/any-store/v2"
	v2enc "github.com/anyproto/any-store/v2/anyenc"
)

const batchSize = 1000

// Report summarises a migration.
type Report struct {
	Collections int
	Documents   int
	// SkippedIndexes lists indexes that could not be rebuilt. Documents are
	// migrated regardless. With v1 >= v1.0.1 this should be empty; a non-empty
	// list means the source data genuinely violates the index.
	SkippedIndexes []SkippedIndex
}

type SkippedIndex struct {
	Collection string
	Index      v2.IndexInfo
	Err        error
}

// Migrate copies every collection, document and index from a v1 database into a
// new v2 database at dstPath. srcPath is left untouched.
func Migrate(ctx context.Context, srcPath, dstPath string) (Report, error) {
	var rep Report
	if srcPath == dstPath {
		return rep, errors.New("migrate: srcPath and dstPath must differ")
	}
	// v1.Open creates the database when the file is missing, so a mistyped
	// srcPath would otherwise "migrate" a fresh empty database over dstPath.
	if _, err := os.Stat(srcPath); err != nil {
		return rep, err
	}

	src, err := v1.Open(ctx, srcPath, nil)
	if err != nil {
		return rep, err // ErrV2Database if srcPath is already a v2 file
	}
	defer src.Close()

	// Build into a temp file and swap at the end, so an interrupted migration
	// never leaves a half-written database at dstPath.
	tmpPath := dstPath + ".tmp"
	removeDB(tmpPath)

	if err = build(ctx, src, tmpPath, &rep); err != nil {
		removeDB(tmpPath)
		return rep, err
	}

	// Rename replaces only the main file. A WAL left beside dstPath by an
	// earlier database would be replayed into the migrated file on first open.
	_ = os.Remove(dstPath + "-wal")
	_ = os.Remove(dstPath + "-wal-shm")
	if err = os.Rename(tmpPath, dstPath); err != nil {
		removeDB(tmpPath)
		return rep, err
	}
	removeDB(tmpPath)
	return rep, nil
}

func build(ctx context.Context, src v1.DB, path string, rep *Report) error {
	dst, err := v2.Open(ctx, path, nil)
	if err != nil {
		return err
	}
	defer dst.Close()

	names, err := src.GetCollectionNames(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err = copyCollection(ctx, src, dst, name, rep); err != nil {
			return fmt.Errorf("collection %q: %w", name, err)
		}
		rep.Collections++
	}
	// Checkpoint into the main file so the WAL is empty at rename time.
	return dst.Flush(ctx, 0, v2.FlushModeCheckpointTruncate)
}

func copyCollection(ctx context.Context, src v1.DB, dst v2.DB, name string, rep *Report) error {
	srcColl, err := src.OpenCollection(ctx, name)
	if err != nil {
		return err
	}
	defer srcColl.Close()

	dstColl, err := dst.CreateCollection(ctx, name)
	if err != nil {
		return err
	}
	defer dstColl.Close()

	iter, err := srcColl.Find(nil).Iter(ctx)
	if err != nil {
		return err
	}
	defer iter.Close()

	var (
		p   v2enc.Parser
		buf []byte
		n   int
	)
	tx, err := dst.WriteTx(ctx)
	if err != nil {
		return err
	}
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			tx.Rollback()
			return err
		}
		// v1-encoded anyenc bytes parse unchanged in v2. ParseOwned is
		// zero-copy: val is consumed by Insert before buf is reused.
		buf = doc.Value().MarshalTo(buf[:0])
		val, err := p.ParseOwned(buf)
		if err != nil {
			tx.Rollback()
			return err
		}
		if err = dstColl.Insert(tx.Context(), val); err != nil {
			tx.Rollback()
			return err
		}
		if n++; n%batchSize == 0 {
			if err = tx.Commit(); err != nil {
				return err
			}
			if tx, err = dst.WriteTx(ctx); err != nil {
				return err
			}
		}
	}
	if err = iter.Err(); err != nil {
		tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	rep.Documents += n

	// Indexes last: building them once over a loaded collection is far cheaper
	// than maintaining them on every insert.
	for _, idx := range srcColl.GetIndexes() {
		i := idx.Info()
		info := v2.IndexInfo{Name: i.Name, Fields: i.Fields, Unique: i.Unique, Sparse: i.Sparse}
		if err = dstColl.CreateIndex(ctx, info); err != nil {
			rep.SkippedIndexes = append(rep.SkippedIndexes,
				SkippedIndex{Collection: name, Index: info, Err: err})
		}
	}
	return nil
}

// removeDB deletes a v2 database and its sidecars. Removing the main file alone
// leaves a WAL that the engine would replay into whatever takes its place.
func removeDB(path string) {
	for _, suffix := range []string{"", "-wal", "-wal-shm"} {
		_ = os.Remove(path + suffix)
	}
}
```

Adapt freely — the loop is the natural place to drop dead collections, rename
fields, or change the index set. Keep four properties:

* **Create indexes after the bulk load**, not before.
* **One write transaction per batch.** One transaction per document is slow; a
  single transaction over a large collection grows the WAL without bound and
  loses everything on abort.
* **Write to a temp path and rename**, and move the `-wal`/`-wal-shm` sidecars
  with their main file. For durability of the swap itself, `fsync` the parent
  directory after the rename.
* **Keep the v1 file** until the migrated database has been verified.

## Verify before deleting the v1 file

Compare index **definitions**, not just how many there are — a dropped `Unique`
flag changes no count and raises no error.

```go
func Verify(ctx context.Context, src v1.DB, dst v2.DB) error {
	names, err := src.GetCollectionNames(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		sc, err := src.OpenCollection(ctx, name)
		if err != nil {
			return err
		}
		dc, err := dst.OpenCollection(ctx, name)
		if err != nil {
			return fmt.Errorf("collection %q: %w", name, err)
		}

		want, err := sc.Count(ctx)
		if err != nil {
			return err
		}
		got, err := dc.Count(ctx)
		if err != nil {
			return err
		}
		if want != got {
			return fmt.Errorf("collection %q: %d documents, want %d", name, got, want)
		}

		flags := make(map[string][2]bool, len(dc.GetIndexes()))
		for _, idx := range dc.GetIndexes() {
			i := idx.Info()
			flags[i.Name] = [2]bool{i.Sparse, i.Unique}
		}
		for _, idx := range sc.GetIndexes() {
			i := idx.Info()
			if flags[i.Name] != [2]bool{i.Sparse, i.Unique} {
				return fmt.Errorf("collection %q: index %q changed definition", name, i.Name)
			}
		}
		sc.Close()
		dc.Close()
	}
	return dst.QuickCheck(ctx)
}
```

## What v2 rejects that v1 accepted

Each of these aborts the migration; fix the source data or rename before
retrying.

* Collection and index names longer than 255 bytes, or equal to `_system`, or
  prefixed `ix:` / `ftx:` / `vix:` — these namespaces are reserved in v2.
* Documents whose `id` is an array — `ErrArrayPrimaryKey`.
* Documents nested deeper than 512 containers.

## Do not migrate through JSON

v1 encodes binary values as base64 **strings** in its JSON representation
(`anyenc.Value.FastJson`). A JSON round-trip silently turns every `TypeBinary`
value into a string, and the original type cannot be recovered without
out-of-band schema knowledge. Use the raw `anyenc` bytes as shown above.

## Sparse indexes: same contents, stricter planning

Index contents migrate one to one. Both versions exclude a document from a
sparse index when an indexed field is missing **or explicitly null** — the rule
is unchanged.

What changed is plan selection. v1's planner did not consider sparse
completeness at all, so it could serve a query from a sparse index that did not
contain every matching document. v2 uses a sparse index only when the filter
guarantees each indexed field is present and non-null, which `$exists: true`
does not (an explicit null is "present" but unindexed). Such queries now fall
back to a complete index or a full scan: correct results, sometimes slower.

## Encoding edge cases

The byte-level pass-through is lossless except for three shapes documented in
v2's `anyenc/escape.go`: `-0.0` re-encodes as `+0.0`; an object key that is
exactly `"\x1f"` decodes as `""`; and two legacy key shapes whose first byte is
invalid UTF-8 are unsupported. All are unreachable through JSON ingestion.

## v1 stays available

v1 remains resolvable indefinitely at the unchanged module path:

```
go get github.com/anyproto/any-store@v1
```

`v1.0.1` is the v0.4.x API plus the index-metadata and v2-detection fixes above.
Only bug fixes land there.
