# Any Store

[![Go Reference](https://pkg.go.dev/badge/github.com/anyproto/any-store.svg)](https://pkg.go.dev/github.com/anyproto/any-store)
[![Go Report Card](https://goreportcard.com/badge/github.com/anyproto/any-store)](https://goreportcard.com/report/github.com/anyproto/any-store)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A **document‑oriented database** with a MongoDB‑like query language, running on top of a single SQLite file.
Any Store brings schema‑less flexibility, rich indexes and ACID transactions to embedded Go applications.

> ⚠️ **Status:** pre‑1.0 – APIs may change. We actively dog‑food the library in production and welcome early adopters & contributors.


## Features

* **Mongo‑style queries** – `$in`, `$inc`, comparison & logical operators out of the box.
* **Automatic indexes** – create, ensure or drop compound & unique indexes at runtime.
* **ACID transactions** – explicit read / write transactions plus convenience helpers.
* **Streaming iterators** – low‑memory scans with cursor API.
* **Durability** – db flush and protections mechanisms in case of power-loss.
* **CLI** – quick inspection, import/export and interactive shell.
* **Cross‑platform** – pure Go, no CGO, runs anywhere Go runs.


## Quick start

### Install library

```bash
go get github.com/anyproto/any-store
```

### Install CLI (optional)

```bash
go install github.com/anyproto/any-store/cmd/any-store-cli@latest
```

### Hello, Any Store

```go
package main

import (
    "context"
    "fmt"
    "log"

    anystore "github.com/anyproto/any-store"
    "github.com/anyproto/any-store/anyenc"
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

    res, _ := users.Find(`{"id": {"$in": [1,2]}}`).Sort("-name").Iter(ctx)
    for res.Next() {
        doc, _ := res.Doc()
        fmt.Println(doc.Value().String())
    }
}
```

The full end‑to‑end example lives in [`example/`](example) and in the [API docs](https://pkg.go.dev/github.com/anyproto/any-store).


## Documentation

* **API reference** – [https://pkg.go.dev/github.com/anyproto/any-store](https://pkg.go.dev/github.com/anyproto/any-store)
* **CLI manual** – `any-store-cli --help`


## Design highlights

| Layer               | Responsibility                                                             |
| ------------------- | -------------------------------------------------------------------------- |
| **Query builder**   | Parses Mongo‑like JSON filters and modifiers                               |
| **Index engine**    | Generates composite SQLite indexes, picks optimal index via cost estimator |
| **Encoding arena**  | Efficient [AnyEnc](anyenc) value arena to minimise GC churn                |
| **Connection pool** | Separate read / write SQLite connections for concurrent workloads          |


## Durability

Any Store automatically performs WAL checkpoints and fsync after idle periods to ensure data durability.

```go
db, _ := anystore.Open(ctx, "data.db", &anystore.Config{
    Durability: anystore.DurabilityConfig{
        AutoFlush: true,
        IdleAfter: 20 * time.Second,  // Flush after 20s of inactivity
        FlushMode: anystore.FlushModeCheckpointPassive, // other options are FlushModeCheckpointFull, FlushModeCheckpointRestart
        Sentinel:  true,  // Track dirty db state for automatic quickCheck on start
    },
})

// Manual flush, e.g. before app suspension (ensure we have at least 100ms of idle, to ensure we finished pending writes)
db.Flush(ctx, 100*time.Millisecond, anystore.FlushModeCheckpointPassive)
```

**Sentinel:** When enabled, creates a `.lock` file to detect not explicitly persisted writes and run integrity check on open.


## Integrity

Any Store can stamp a per-page hash trailer on every page (XXH3-128, 16 bytes) and verify it on read — analogous to SQLite's
[`cksumvfs`](https://sqlite.org/cksumvfs.html). Mutually exclusive with `Encryption` (AEAD modes already authenticate every page; the
same `OnError` callback fires for both).

```go
db, _ := anystore.Open(ctx, "data.db", &anystore.Config{
    Integrity: anystore.IntegrityConfig{
        PageChecksums: true,                // stamp + verify XXH3-128 per page
        OnError: func(e anystore.IntegrityError) {
            log.Printf("integrity: page %d %v: %v", e.PageNo, e.Kind, e.Inner)
        },
    },
})

// Walk every page and report mismatches (works in encrypted mode too).
rep, _ := db.VerifyIntegrity(ctx)
fmt.Printf("scanned %d pages, %d errors\n", rep.Pages, len(rep.Errors))

// Forensic mode: stop erroring on read mismatches but keep firing OnError.
_ = db.SetVerifyOnRead(false) // returns ErrAEADIntegrityVerifyMandatory in AEAD mode
```

Page-1 DB header (first 100 bytes) is not covered by the per-page hash; SQLite-format invariants there are validated separately at open. See [internal/btree/integrity.md](internal/btree/integrity.md) for the full design.


## Contributing

1. Fork & clone
2. `make test` – run unit tests
3. Create your feature branch
4. Open a PR and sign the CLA

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.


## ⚖️ License

Any Store is released under the MIT License – see [LICENSE](LICENSE) for details.
