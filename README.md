# Any Store

[![Go Reference](https://pkg.go.dev/badge/github.com/anyproto/any-store/v2.svg)](https://pkg.go.dev/github.com/anyproto/any-store/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/anyproto/any-store/v2)](https://goreportcard.com/report/github.com/anyproto/any-store/v2)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE.md)

A **document-oriented embedded database** for Go with a MongoDB-like query language, built on an in-tree btree/pager/WAL storage engine derived from SQLite (aligned with its design, not a literal port — the on-disk format is our own).
Schema-less documents, rich indexes, full-text and vector search, ACID transactions, multi-process access, page-level integrity and optional encryption — pure Go, no CGO.

## Features

* **Mongo-style queries** — `$eq/$in/$gt/...`, logical operators, dot-path fields, modifiers (`$set`, `$inc`, ...). Formal semantics in [docs/query-filter-contract.md](docs/query-filter-contract.md).
* **Indexes** — compound, unique, sparse, multikey (arrays), asc/desc per field; created and dropped at runtime.
* **Cost-based planner** — index selection driven by btree page statistics and per-prefix selectivity sketches; `Explain()` shows the chosen plan and its candidates.
* **Full-text search** — btree-resident inverted index with BM25 ranking via `$text`: phrases, prefixes, anchored and per-field matching for search-as-you-type. See [docs/full-text-search.md](docs/full-text-search.md).
* **Vector search** — btree-resident ANN indexes (IVF-SQ, HNSW, brute-force) via `$knn`. See [docs/vector-search.md](docs/vector-search.md) and [docs/vector-engine.md](docs/vector-engine.md).
* **Aggregation** — MongoDB-style pipelines: shaping (`$match`, `$group`, `$sort`, `$unwind`, `$facet`, ...), computed expressions (arithmetic, conditionals, comparisons, date math), field-to-field `$expr` predicates, primary-key `$lookup` joins, and materialization via `$merge`/`$out` — with planner pushdown. See [docs/aggregation.md](docs/aggregation.md).
* **ACID transactions** — snapshot-isolated read transactions, single-writer write transactions. On an operating system's file system, a commit survives a process crash by default and reaches the disk with a later checkpoint, as in SQLite's WAL mode at `synchronous=NORMAL`, and `CommitSync` syncs every commit, the per-commit guarantee; the wasm example's IndexedDB file system gives neither (see [Durability](#durability)).
* **Multi-process** — SQLite-like contract on Linux and Darwin, amd64 and arm64: any number of OS processes may open, read and write the same database file at any time (WAL + shared-memory index, busy handling, cross-process DDL reconciliation). Other targets run one process at a time (see [Multi-process](#multi-process)).
* **Durability** — SQLite's WAL durability levels, crash-safe WAL recovery, explicit checkpointing, opt-in idle auto-flush, sentinel-triggered quick check after unclean shutdown.
* **Integrity & encryption** — per-page XXH3-128 checksums by default; optional AES-GCM or (X)ChaCha20-Poly1305 (AEAD doubles as integrity).
* **Streaming iterators** — low-memory scans with a cursor API; [AnyEnc](anyenc) arena encoding minimizes GC churn.
* **Cross-platform** — pure Go, no CGO; builds for every Go target but AIX and Solaris, `js/wasm` and `wasip1` included (see [wasm/](wasm)), with multi-process access on Linux and Darwin, amd64 and arm64.
* **CLI** — inspection, import/export and interactive shell.

## Quick start

### Install

```bash
go get github.com/anyproto/any-store/v2
```

CLI (optional):

```bash
go install github.com/anyproto/any-store/cmd/any-store-cli2/v2@latest
```

### Hello, Any Store

```go
package main

import (
    "context"
    "fmt"
    "log"

    anystore "github.com/anyproto/any-store/v2"
    "github.com/anyproto/any-store/v2/anyenc"
)

func main() {
    ctx := context.Background()

    db, err := anystore.Open(ctx, "/tmp/demo.db", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    users, _ := db.Collection(ctx, "users")

    _ = users.Insert(ctx,
        anyenc.MustParseJson(`{"id": 1, "name": "John"}`),
        anyenc.MustParseJson(`{"id": 2, "name": "Jane"}`),
    )

    res, err := users.Find(`{"id": {"$in": [1,2]}}`).Sort("-name").Iter(ctx)
    if err != nil {
        log.Fatal(err)
    }
    // An iterator holds a read transaction until its Close, and db.Close
    // waits for every read transaction: close it before the DB.
    defer res.Close()
    for res.Next() {
        doc, _ := res.Doc()
        fmt.Println(doc.Value().String())
    }

    // Storage footprint: doc count, sizes, compression and per-index stats.
    st, _ := users.Stats(ctx)
    fmt.Printf("docs=%d total=%d bytes ratio=%.2fx\n",
        st.DocCount, st.TotalSizeBytes, st.CompressionRatio)
}
```

The full end-to-end example lives in [`example/`](example) and the [API docs](https://pkg.go.dev/github.com/anyproto/any-store/v2).

> **Document size.** Each key/value pair a write stores is limited to 1 GiB (`1<<30` bytes): a document's encoded id with its stored body, each range-index entry's key with its one-byte value, and each record a full-text or vector index stores. A write over the limit stores nothing and fails with `ErrValueTooLarge`, or with `ErrKeyTooLarge` when a key alone is over it; the two are separate errors, check for both.

> **Choosing document ids.** Unlike SQLite there is no hidden rowid — the document id is the btree key. Increment-like ids (`anyenc.NewObjectID()`, timestamps, sequences) keep inserts append-ordered and reads cache-friendly; fully random ids (e.g. UUIDv4) scatter them. In practice this only matters for very hot write/read paths or huge collections.

> **Numbers are float64.** Every number is stored as a float64, as in JSON: integers are exact up to 2^53 (9,007,199,254,740,992) in magnitude, and a larger one, written as a plain integer in JSON or given as a Go integer, becomes the nearest float64, in a document, a filter and an id alike (a JSON number with an exponent can round to another one: [query contract](docs/query-filter-contract.md), item 18). A filter given as a Go value is encoded as JSON, which takes every integer type, `uintptr` and named types included. An integer id given to `FindId` and the other id calls, or an integer operand of `query.NewComp`, is an `int`, `uint` or sized integer (`int64`, `uint64`, …): a `uintptr`, or a value of a named type such as `type myInt int64`, panics. Two such integers can then be one value, and as ids one document: with a document of id 2^53 in the collection, inserting id 2^53+1 fails with `ErrDocExists`, `UpsertOne` with it replaces that document, and `FindId(2^53+1)` returns it. Store integers that can go past 2^53, such as 64-bit hashes, as strings.

## Transactions

```go
tx, _ := db.WriteTx(ctx)
_ = users.Insert(tx.Context(), anyenc.MustParseJson(`{"id": 3}`))
_, _ = users.Find(`{"id": 3}`).Update(tx.Context(), `{"$set": {"seen": true}}`)
if err := tx.Commit(); err != nil { // or tx.Rollback()
    log.Fatal(err)
}
```

Any operation run with `tx.Context()` joins the transaction. Read transactions (`db.ReadTx`) pin a consistent snapshot; writers never block readers. One write transaction is active at a time — across all processes.

A transaction begins with a context, as does an operation run without one, which opens its own. The wait for this process's writer lock, or for a reader slot once `Config.ReadConcurrency` of them are held, ends with the context's error when the context ends first; a context that has ended already begins nothing; a context that never ends waits until the database closes (`ErrDBIsClosed`). The wait behind another process's writer is the busy handler's: it ends with the busy error (`ErrBusy`) after the busy timeout, not with the context. `Commit` and `Rollback` do not consult the context. `TxFromContext` returns the transaction a context carries.

`db.Close` waits for every read transaction of the database to end; it has no timeout and takes no context. Commit each `ReadTx`, and close each `Iterator`: one opened without a transaction can hold a read transaction of its own until its `Close`. (An iterator opened inside a transaction holds none; it ends with the transaction.) A read transaction that is never ended blocks `Close` forever, and `Close` called by the goroutine that still holds one deadlocks on itself, as when a deferred `it.Close()` runs after `db.Close()`. While `Close` waits, an operation on a collection already fails with `ErrDBIsClosed`, and an open iterator's `Next` and `Doc` go on until it is closed; a call on the `DB` itself through an open read transaction may go on too (`GetCollectionNames` does) or fail with `ErrDBIsClosed`. A write transaction does not hold `Close` back. `Close` waits for the one step of storage work the transaction is in, which is as long as that step (a count walks its whole tree), and rolls the transaction back: its writes are discarded, an operation that was running ends with `ErrDBIsClosed` unless it was already in or past its last step, later ones fail with `ErrDBIsClosed`, as do the transaction's iterators that read through it and its `Commit`, and its `Rollback` returns nil (the `Rollback` of a `WriteTx` opened inside it returns `ErrDBIsClosed`). A `Commit` that runs while `Close` does may commit the transaction or fail with `ErrDBIsClosed`. Do not call `Close` from code that runs inside a storage step: `Config.OnIntegrityError`, the methods of a custom VFS, or the methods of a context passed to an operation.

`db.Backup` is the exception to the waits: it does not wait for this process's writer lock or for a reader slot. The copy advances between this process's write transactions; if one stays open, or no reader slot is free, for the busy timeout, `Backup` returns an error that matches `ErrBusy` (`errors.Is`) and can be called again. With a context that carries a write transaction of the same database it returns that error at once: it would be waiting for its caller. It looks at its context between the batches of the copy, not inside one. The destination file is never removed. If `Backup` fails before the copy is committed, a destination that existed keeps its content and a new one is an empty database; the copy is committed in one step at the end, and an error after that leaves the completed copy. A batch of the copy begins its transactions with the ordinary waits behind another process, so `Backup` can return later than the busy timeout after it last copied. Two backups in opposite directions between the same two databases are busy for each other until one has ended.

An operation run with a context that has ended begins nothing, inside a transaction as well; the transaction stays open for `Commit` and `Rollback`. A find, count, update or delete that is running polls its context once per page of its scan and ends with the context's error: an iterator's `Next` returns false and `Err` reports it. An `Update` or `Delete` rolls its writes back: its own transaction, or its savepoint inside the caller's transaction, which stays open (SQLite rolls back the whole transaction for an interrupted write statement). When the scan is an index's, the polled pages are the index's: the document fetches for one index leaf are the longest it runs past its context. The in-memory sort step of a sorted query and the search of a `$text` or `$knn` plan run to completion before the first `Next` and do not poll the context.

An operation that fails because the log could not be written while the transaction made room in its page cache (a full disk, an I/O error) fails the transaction, not only itself. A read can be that operation as well as a write: a read in a write transaction goes through the transaction's page cache. The savepoint of a write cannot restore what came before it. From then on every read or write of the database through the transaction returns that error. An operation run with the transaction's context returns it where it needs such a read or write; one that can do without, such as `Explain`, which falls back to default estimates, can still succeed. An iterator open on the transaction returns the error at its next read (the next step of a scan). `Commit` returns the error as well and rolls the transaction back, as `Rollback` does. Nothing the transaction wrote is committed.

A transaction belongs to one goroutine at a time: the calls made on it — operations run with its context, the methods of its savepoints and iterators, `Commit` and `Rollback` — must not run at once from two goroutines. Calls nest: from inside the operation that runs it, a `query.Modifier` may read through the transaction's context, open collections, and write other collections (a write made with the context opens a savepoint on the transaction; one made with a context that carries no transaction waits for the writer lock the transaction holds until that context ends or the database closes). What that operation holds, the modifier must not pull away or layer over: a write made with the context to, or a schema change of, the collection whose modifier is running fails with `ErrWriteInModifier`, a `Commit` or `Rollback` of the transaction or of a savepoint fails with `ErrTxEndInModifier`, and a savepoint opened with the context (`WriteTx`) fails with `ErrSavepointInModifier` — a modifier that needs its work undone returns an error, which fails the operation and rolls back everything the operation and the modifier wrote. A call made while another goroutine's call is in progress panics with `ErrTxConcurrentCalls`, detected the way the runtime detects concurrent map writes: best effort — the overlap is seen, the goroutine is not, so a call that lands inside a running modifier passes for a nested one — and the transaction stays usable by the call that was in progress; `Commit` and `Rollback` wait that call out instead of refusing, so a deferred `Rollback` still ends the transaction. A `query.Filter` or `query.Sort` implementation and `Config.OnIntegrityError` run inside the call that evaluates them and must not use the transaction. Once the transaction has ended, an operation run with its context fails with `ErrTxIsUsed`, and so do the `Next` and `Doc` of an iterator still open on it; a second `Commit` or `Rollback` returns nil, as does the iterator's `Close`. The `Rollback` of a savepoint ends the iterators opened or moved while it was open — inside it, or inside a savepoint nested in it: their `Next` and `Doc` fail with `ErrIterRolledBack`, their `Close` returns nil, and the transaction goes on; an iterator that last moved before the savepoint opened continues. A write that fails rolls back a savepoint of its own and ends the iterators moved inside it the same way: one a `query.Modifier` advanced while the write ran. A write to the collection an iterator of the same transaction is scanning never corrupts the store. A scan the plan drives with a cursor — a full scan, an index scan — goes on from the key it stood on, as a cursor of the same SQLite connection does: the current document deleted, the scan goes on with its successor; a document inserted ahead of the position is visited, one inserted behind it is not; an update that moves a document within the scan's order — an indexed field under an index scan — can make the scan visit it again or miss it. A plan that collects its result before yielding it — a sort run in memory, a `$knn` probe (`KnnProbe` in `Explain`) — yields what it collected: a document inserted since is not visited, one deleted since fails its `Doc` with `ErrDocNotFound`, and an aggregation over such a plan ends there with that error. The `$knn` search over the vector index (`KnnSearch`) collects its candidates the same way and skips one deleted since: it yields the first k that remain, fewer once they run out. `Doc` returns the document as `Next` read it. To change every matched document once, use `Query.Update` and `Query.Delete`, which collect the ids before they write. `Drop`, `DropIndex` and `CompactVectorIndex` fail with `ErrIterOpen` while an iterator of the transaction is open on the collection, advanced or not, until its `Close`: they free the trees the iterator reads. SQLite refuses a `DROP` while any other statement of the connection is running; here an iterator on another collection does not count.

## Indexes

```go
_ = users.EnsureIndex(ctx,
    anystore.IndexInfo{Fields: []string{"name", "-createdDate"}},          // compound, mixed order
    anystore.IndexInfo{Fields: []string{"email"}, Unique: true},           // unique
    anystore.IndexInfo{Fields: []string{"nick"}, Sparse: true},            // skips docs missing the field
)

exp, _ := users.Find(`{"name": "Jane"}`).Explain(ctx)
fmt.Println(exp.Plan) // chosen index, cost breakdown, rejected candidates
```

Array fields index every element (multikey), and a dotted path through an array of objects (`"items.sku"` over `{"items":[{"sku":1},{"sku":2}]}`) indexes one entry per element — the same MongoDB field-path semantics filters and sorts use; `$elemMatch` binds several predicates to one element. The cost-based planner picks the cheapest index per query; `IndexHint` overrides it. Every index key ends with the document's primary key, ascending, so a sort on an index's fields followed by the primary key is served by the index when the whole sort runs the way the scan does: the keyset-pagination shape `Sort("-modifiedAt", "-id")` reads an index on `modifiedAt` backwards, and `Sort("-modifiedAt", "id")` reads an index on `-modifiedAt` forwards. A sort that starts with the primary key is a scan in storage order.

Every range index this release creates records the format it was built under. When a release changes how an index derives its entries, opening the database with that release rebuilds the indexes the change affects, one write transaction per index, before `Open` returns; unaffected indexes are left as they are. The rebuild takes as long as creating those indexes would and blocks `Open` for that time; a crash in between keeps the indexes already rebuilt. Once nothing is left to rebuild the file records it, and later opens skip the catalog check; a schema change committed by a release without this record makes the next open check again. An index the rebuild cannot produce from the data (a unique index whose documents the new format finds duplicate: explicit nulls under a sparse index, elements of an array under a dotted path; or a document this release rejects) is left as it was: `Stats` and `Explain` report it outdated, the planner never uses it, and writes keep maintaining it with entries of the new format, so a unique one checks new writes against a mixed set and can reject an insert or update it should accept. Drop and recreate it once the data allows; `EnsureIndex` with the same definition leaves it alone.

Once a release has opened a file, older releases must not write documents to it: they do not stamp the entries they write, so entries in the older format would sit under a current stamp that nothing re-checks. An index an older release creates is rebuilt at the next open. An older release opening the file reads it (the stamp is ignored) and, if its own format differs, rebuilds every range index in its own format at open.

## Full-text search

```go
_ = docs.EnsureIndex(ctx, anystore.IndexInfo{
    Kind:   anystore.IndexKindFulltext,
    Fields: []string{"title", "body"},
})

res, err := docs.Find(`{"$text": {"$search": "wal checkpoint"}}`).Limit(10).Iter(ctx)
if err != nil {
    log.Fatal(err)
}
defer res.Close()
```

BM25-ranked, transactional (a `$text` write is visible to queries in the same tx), combinable with any other filter, sort and limit.

## Vector search

```go
// IVF-SQ trains its cells on the vectors already stored: insert first.
err := docs.Insert(ctx,
    anyenc.MustParseJson(`{"id": 1, "embedding": [0.9, 0.1, 0.0, 0.2]}`),
    anyenc.MustParseJson(`{"id": 2, "embedding": [0.0, 0.1, 0.9, 0.7]}`),
)
if err != nil {
    log.Fatal(err)
}
err = docs.EnsureIndex(ctx, anystore.IndexInfo{
    Kind: anystore.IndexKindVector,
    Vector: &anystore.VectorParams{
        Field:  "embedding",
        Dim:    4, // the length of every vector: 768, 1536, … for a real embedding model
        Metric: anystore.VectorCosine, // or VectorL2 (VectorDot: HNSW/brute-force modes only)
        Mode:   anystore.VectorModeIVFSQ, // or BTree/Hybrid (HNSW), BruteForce
    },
})
if err != nil {
    log.Fatal(err)
}

res, err := docs.Find(`{"embedding": {"$knn": {"$query": [0.85, 0.15, 0.05, 0.15], "$k": 10}}}`).Iter(ctx)
if err != nil {
    log.Fatal(err)
}
defer res.Close()
```

ANN results stream in distance order and compose with filters; `$ef` tunes recall per query, int8 quantization and IVF cell counts are per-index options. Large deployments run IVF-SQ; the API default is `VectorModeBTree`.

## Aggregation

```go
res, err := orders.Aggregate(`[
    {"$match": {"status": "paid"}},
    {"$group": {"_id": "$region", "total": {"$sum": "$amount"}}},
    {"$sort": {"total": -1}}
]`).Iter(ctx)
if err != nil {
    log.Fatal(err)
}
defer res.Close()
```

`$match`/`$sort`/`$limit` prefixes are pushed down into the index planner; the rest streams through the pipeline. Expressions compute derived values in `$project`/`$addFields`/`$group` (arithmetic, `$cond`/`$switch`, comparisons, `$dateDiff`-family date math over the first-class dateTime type); `$expr` brings field-to-field predicates into `$match`; `$lookup` resolves object-id relations as primary-key point lookups; `$facet` runs N sub-pipelines over one scan; `$merge`/`$out` materialize results into a collection where they are indexable like any other data. Operator reference and Mongo divergences: [docs/aggregation.md](docs/aggregation.md).

## Multi-process

On Linux (Android included) and Darwin (macOS, iOS), on amd64 and arm64, the database file behaves like SQLite in WAL mode: any process may open it at any moment and read or write concurrently. Coordination runs through file locks and a shared-memory WAL index; a snapshot read is not blocked by a writer or a checkpoint, and writers serialize. A lock another process holds, where a transaction's begin, `Open`, or a `Full`, `Restart` or `Truncate` checkpoint waits for one, is retried by the busy handler, with SQLite's backoff, for a fixed 5 seconds before it returns a busy error; there is no option to change it. `Open`, `Backup` and `IntegrityCheck` return every busy error as one that matches `ErrBusy`. A transaction's begin (`ReadTx`, `WriteTx`, and the transaction an operation begins for itself) returns `ErrBusy` when another process's writer or checkpoint held the lock, and the storage layer's other busy errors as they are: that of a begin that waited out another process's rebuild of the shared-memory index after a crash, and that of a write begin whose snapshot other processes' commits kept overtaking. Not every lock goes through the busy handler, as in SQLite: a passive checkpoint waits for none, a checkpoint (`Flush`) that finds another process checkpointing returns `ErrBusy` at once, and an `Open` that finds another process setting up the shared-memory index retries for about a second. A read waits too, as in SQLite, when it begins while another process rebuilds the shared-memory index after a crash: through the busy handler, and it fails with that rebuild's busy error once the 5 seconds pass. DDL from a peer process (created/dropped collections and indexes) is reconciled automatically on the next transaction.

Every other target runs a database in one process at a time, with its WAL index in that process's memory. On Windows and the other Unix systems — FreeBSD and the other BSDs, illumos, and Linux on other architectures, 32-bit ones included — the process that opens the file first holds an exclusive lock on it, and another process's `Open` fails. On `js/wasm`, `wasip1` and Plan 9 any-store takes no lock: it does not stop a second process, or a second browser tab, from opening the file. The file system can: the OPFS file system of the [wasm](wasm) example opens a file through an exclusive sync access handle, which the browser refuses to a second opener. Where the file system does not, the application has to prevent it. AIX and Solaris are not supported: their `syscall` package has no `flock`, and the library does not build there.

One process holds one handle on a database file: a second `Open` of a file the process has open returns `ErrDBAlreadyOpen` (on Windows, a second `Open` through another path to the file, such as a link, returns `ErrDBOpenInOtherProcess`). Where multi-process access is not available (anything but linux, android, darwin and ios on amd64 and arm64), `Open` of a file that another process has open returns `ErrDBOpenInOtherProcess` at once; js/wasm, wasip1 and plan9 take no file lock and do not detect the other process.

## Durability

By default a commit is written to the WAL and not synced, as in SQLite's WAL mode at `synchronous=NORMAL`. A checkpoint that copies frames from the WAL into the database file syncs the whole WAL first, and one is attempted automatically once the WAL holds `Config.AutoCheckpointAfter` frames (10,000 by default), on `Flush`, and on the last `Close` of the database. A passive checkpoint — the automatic one, and a flush in `FlushModeCheckpointPassive`, auto-flush's default — copies frames only up to the oldest snapshot a reader still holds: with frames to copy below it, it syncs the whole WAL, the frames past the snapshot included; with none, it returns before the sync, and the commits since the last sync stay unsynced until a later checkpoint. (A write that starts the WAL over also syncs the new WAL header.) On the file system of an operating system, a process crash loses no committed transaction: the operating system has the write. A power loss or an OS crash leaves the database consistent, but the commits made since the last sync can be lost although they returned without error. `Config.CommitSync` syncs the WAL on every commit, as `synchronous=FULL` does: a commit that returned survives a power loss, at the cost of a sync per commit. It is the only per-commit guarantee.

The IndexedDB file system of the [wasm](wasm) example gives no durability guarantee, with `CommitSync` or without: it keeps each file in memory and stores it in IndexedDB only when the file is synced or closed, and its sync returns before IndexedDB has committed what it stores. A closed tab or a killed worker can lose commits that returned without error, as a power loss can elsewhere.

Auto-flush (`Durability.AutoFlush`, off by default) attempts a checkpoint, in its `FlushMode`, once the database has been idle for `IdleAfter`:

```go
db, _ := anystore.Open(ctx, "data.db", &anystore.Config{
    Durability: anystore.DurabilityConfig{
        AutoFlush: true,
        IdleAfter: 20 * time.Second,  // flush after 20s of inactivity
        FlushMode: anystore.FlushModeCheckpointPassive, // ...Full, ...Restart, ...Truncate
        Sentinel:  true,  // track dirty state, quick-check on next open
    },
})

// Manual flush, e.g. before app suspension: a checkpoint once this handle has
// seen no write for 100ms (another process's writes do not count). Flush
// retries until such a pause, and the context bounds that wait. A checkpoint
// that has started does not take the context and runs to its end: the
// deadline bounds when the checkpoint starts, not when Flush returns.
fctx, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()
if err := db.Flush(fctx, 100*time.Millisecond, anystore.FlushModeCheckpointPassive); err != nil {
    log.Printf("flush: %v", err)
}
```

**Sentinel:** when enabled, a `.lock` file marks not-explicitly-persisted writes so an unclean shutdown triggers an integrity quick check on open. A flush removes the file, and so does a `Close` that checkpointed as the last handle on the database; a `Close` while another process still has the database open leaves it. An in-memory database has no sentinel file.

**Lifetime:** the idle flush runs until `Close`. It is not bound to the context passed to `Open`, which only bounds the open itself; a DB that is never closed keeps the flush goroutine alive.

## Integrity

Every non-encrypted database carries an XXH3-128 page-trailer checksum (16 bytes/page) by default — corruption is caught on read. There is no opt-out; the cost is <1% on writes and effectively zero on reads. Encrypted databases derive integrity from the cipher's AEAD tag instead. File state is authoritative on reopen — existing plain databases stay plain, existing checksum databases keep their checksums, whatever the caller's config. An in-memory database has no checksums: its `Backup` is a plain file, or an encrypted one when the database was opened with `Encryption`. `Backup` copies a database as the kind of file it is, and returns an error, leaving the destination as it was, when the file at the destination path is a database of another kind (plain, checksummed, encrypted).

Conceptually mirrors SQLite's [`cksumvfs`](https://sqlite.org/cksumvfs.html), generalized to also surface AEAD failures via the same API.

```go
db, _ := anystore.Open(ctx, "data.db", &anystore.Config{
    // Wire monitoring at Open time so failures during the first page-1
    // read (inside Open) are observable.
    OnIntegrityError: func(e anystore.IntegrityError) {
        log.Printf("integrity: page %d %v: %v", e.PageNo, e.Kind, e.Inner)
    },
    // Default false: corrupt pages fail reads. True is for forensic dumps.
    ContinueOnIntegrityError: false,
})

// Walk every page and report mismatches (works in encrypted mode too).
rep, _ := db.VerifyIntegrity(ctx)
fmt.Printf("scanned %d pages, %d errors\n", rep.Pages, len(rep.Errors))
```

The page-1 DB header (first 100 bytes) is outside the per-page hash; its invariants are validated separately at open. Full design: [docs/btree/specs/integrity.md](docs/btree/specs/integrity.md).

### Corruption errors

`ErrCorrupt` means damage that is in the files and will still be there at the next `Open`; a caller may discard and rebuild the database on it. Match it with `errors.Is`. The damage is in the database the call read: `Backup` also reads the file at its path, and can return `ErrCorrupt` or `ErrHeaderCorrupt` for a damaged destination while the source is sound, so rule that out before discarding the source. The kinds of damage match it too:

| Error | When |
|---|---|
| `ErrHeaderCorrupt` | `Open`: the file header is unusable. |
| `ErrPageIntegrity` | A page fails its checksum or its AEAD authentication, on the read that meets it. |
| `ErrQuickCheckFailed` | `Open` after an unclean shutdown (`Durability.Sentinel`): the quick check, or the open's read of the catalog before it, found damage. |

A read of a page that the database file ends inside (a file cut short) matches `ErrCorrupt` too; a read that came up short of bytes the file holds is that read's error. `IntegrityCheck` returns an error that matches `ErrCorrupt` when it found damage. A damaged write-ahead log index (`-wal-shm`, shared memory) matches `ErrCorrupt` too, as in SQLite; closing every handle on the file rebuilds it.

Errors that are not damage do not match `ErrCorrupt` and are returned as themselves: a cancelled context, an I/O error or a full disk (also when they stop a commit, the quick check of a dirty `Open`, or an `IntegrityCheck`), `ErrBusy`, `ErrDBIsClosed`, `ErrV1Database`, `ErrIncompatibleVersion`, `ErrDBAlreadyOpen`, `ErrDBOpenInOtherProcess`, `ErrCopyVerifyFailed` (`CopyToLatestFormat`'s copy failed its verification, not the source), and the encryption errors below. So does `ErrWALShortRead`: the write-ahead log's index points past the end of the log; close every handle on the file and open it again. It keeps the short read's own error (`io.EOF`, or `io.ErrUnexpectedEOF`).

## Encryption

```go
db, _ := anystore.Open(ctx, "data.db", &anystore.Config{
    Encryption: anystore.EncryptionConfig{
        Passphrase: []byte("correct horse battery staple"),
        CipherType: anystore.CipherXChaCha20Poly1305, // or CipherAES256GCM, CipherChaCha20Poly1305
    },
})
```

Whole-file page-level AEAD with a built-in cipher. A nil `Passphrase` means no encryption, and an empty non-nil one fails `Open`. A `Passphrase` of exactly 32 bytes is used as the key itself; any other nonempty length is stretched with PBKDF2-HMAC-SHA256 against a salt stored in the file, at `KDFIterations` rounds (256,000 by default, at most 2,147,483,647, or 2,147,483,646 on a 32-bit target). The cipher and the count are not stored in the file: a database opens only with the `CipherType` it was created with and, when its passphrase goes through PBKDF2, the same effective `KDFIterations` (0 and 256,000 are one count; a 32-byte key ignores it).

`Open` refuses an `Encryption` config that does not fit the file, with an error to match with `errors.Is`. None of them matches `ErrCorrupt`, and the file is left unchanged:

| Error | When |
|---|---|
| `ErrEncryptionKeyRequired` | The file is encrypted and `Config.Encryption` is empty. |
| `ErrNotEncrypted` | The file is not encrypted and `Config.Encryption` is not empty. |
| `ErrWrongKey` | `Config.Encryption` does not open the file: another passphrase or key than the file's, or another cipher. |

A damaged first page under the right key is not `ErrWrongKey`: `Open` proves the key on other pages and reports `ErrPageIntegrity`, which matches `ErrCorrupt`. See `ErrWrongKey` for the cases it cannot tell apart, which it reports as a wrong key.

`Backup` of an encrypted database writes an encrypted copy that opens with the source's `EncryptionConfig`: the same passphrase or key and `CipherType`, and with a passphrase the same effective `KDFIterations`. A copy that `Backup` creates has a salt of its own, so with a passphrase its derived key differs from the source's. A file that already exists at the path keeps the salt it has, which is the source's when that file is a copy of the source made outside `Backup`.

An independent Go implementation following design conventions from [SQLCipher](https://www.zetetic.net/sqlcipher/) (BSD-3-Clause) — reserved-tail page overhead, the page-1 plaintext-prefix rule, PBKDF2-HMAC-SHA256 key derivation. No SQLCipher code is included; the cryptography is Go-native and uses AEAD rather than SQLCipher's encrypt-then-MAC construction. Not SQLCipher-compatible on disk. See [NOTICE.md](NOTICE.md).

## File format versions

A database file keeps the on-disk format it was created with.

| Format | Created by | Opened by |
| ------ | ---------- | --------- |
| 5 | v2.0.0 – v2.4.x | every v2 release |
| 6 | v2.5.0 and later | v2.5.0 and later |

Format 6 keeps a document or index entry of up to about one page (4,045 bytes on the default page size) on its b-tree leaf; format 5 moves anything over 997 bytes to a page of its own. Format 6 also stores the planner's statistics compressed: a row per index that takes about 100 bytes for a small collection, where format 5 keeps 8 KB per indexed field. A format 5 file opened by v2.5.0 or later stays format 5 and is read and written as before.

Opening a format 6 file with v2.4.4 fails with `ErrIncompatibleVersion` and leaves the file unchanged. Releases before v2.4.4 refuse it as a corrupt file and do not read it either: do not go back past v2.4.4 once a release that creates format 6 files has run.

`Backup` writes a copy in the source's format: a backup of a format 5 database is a format 5 file.

`SchemaFormat` reports the format of an open database, and `CopyToLatestFormat` copies a database into a new file in the latest format. Moving a format 5 database onto format 6 is optional; [docs/migration-format-5-to-6.md](docs/migration-format-5-to-6.md) describes when to do it, what can differ in the copy, and how to put the copy in the old file's place.

## Documentation

* **API reference** — [pkg.go.dev/github.com/anyproto/any-store/v2](https://pkg.go.dev/github.com/anyproto/any-store/v2)
* **Query semantics** — [docs/query-filter-contract.md](docs/query-filter-contract.md)
* **Full-text search** — [docs/full-text-search.md](docs/full-text-search.md)
* **Vector search** — [docs/vector-search.md](docs/vector-search.md), [docs/vector-engine.md](docs/vector-engine.md)
* **Aggregation** — [docs/aggregation.md](docs/aggregation.md)
* **CLI manual** — `any-store-cli2 --help`

## Contributing

1. Fork & clone
2. `make test` — the whole suite; `make cover` for library coverage
3. Create your feature branch
4. Open a PR and sign the CLA

Unit tests live beside the code they cover (`db.go` + `db_test.go`). The
behaviour suites that drive the library through its exported API — query
semantics, planner, index lifecycle, vector and full-text — live in `test/`.
Long-running crash, multiprocess and fuzz harnesses run in a separate internal
repo; a pull request does not need those green locally.

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.

## ⚖️ License

Any Store is released under the MIT License — see [LICENSE.md](LICENSE.md).

The storage engine derives from SQLite (public domain) and follows page-encryption design conventions from SQLCipher (BSD-3-Clause). Neither project's source is included. Attribution and scope: [NOTICE.md](NOTICE.md).
