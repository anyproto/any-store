# Any Store

[![Go Reference](https://pkg.go.dev/badge/github.com/anyproto/any-store/v2.svg)](https://pkg.go.dev/github.com/anyproto/any-store/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/anyproto/any-store/v2)](https://goreportcard.com/report/github.com/anyproto/any-store/v2)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A **document‑oriented database** with a MongoDB‑like query language, built on an embedded Go btree/pager/wal storage engine derived from SQLite (with intentional drifts — not a literal port, and the on-disk format is our own).
Any Store brings schema‑less flexibility, rich indexes, ACID transactions, page-level integrity and optional encryption to embedded Go applications — pure Go, no CGO.

> ⚠️ **Status:** pre‑1.0 – APIs may change. We actively dog‑food the library in production and welcome early adopters & contributors.


## Features

* **Mongo‑style queries** – `$in`, `$inc`, comparison & logical operators out of the box.
* **Automatic indexes** – create, ensure or drop compound & unique indexes at runtime.
* **ACID transactions** – explicit read / write transactions plus convenience helpers.
* **Streaming iterators** – low‑memory scans with cursor API.
* **Durability** – db flush and protections mechanisms in case of power-loss.
* **Integrity & encryption** – per-page XXH3-128 checksums by default; optional AES-GCM or ChaCha20-Poly1305 (AEAD doubles as integrity).
* **CLI** – quick inspection, import/export and interactive shell.
* **Cross‑platform** – pure Go, no CGO, runs anywhere Go runs.


## Quick start

### Install library

```bash
go get github.com/anyproto/any-store/v2
```

### Install CLI (optional)

```bash
go install github.com/anyproto/any-store/cmd/any-store-cli2/v2@latest
```

### Hello, Any Store

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

    res, _ := users.Find(`{"id": {"$in": [1,2]}}`).Sort("-name").Iter(ctx)
    for res.Next() {
        doc, _ := res.Doc()
        fmt.Println(doc.Value().String())
    }

    // Inspect storage footprint: doc count, sizes, compression and per-index stats.
    st, _ := users.Stats(ctx)
    fmt.Printf("docs=%d total=%d bytes ratio=%.2fx\n",
        st.DocCount, st.TotalSizeBytes, st.CompressionRatio)
}
```

The full end‑to‑end example lives in [`example/`](example) and in the [API docs](https://pkg.go.dev/github.com/anyproto/any-store/v2).


## Documentation

* **API reference** – [https://pkg.go.dev/github.com/anyproto/any-store/v2](https://pkg.go.dev/github.com/anyproto/any-store/v2)
* **CLI manual** – `any-store-cli2 --help`


## Design highlights

| Layer               | Responsibility                                                             |
| ------------------- | -------------------------------------------------------------------------- |
| **Query builder**   | Parses Mongo‑like JSON filters and modifiers                               |
| **Index engine**    | Generates composite indexes, picks optimal index via cost estimator        |
| **Encoding arena**  | Efficient [AnyEnc](anyenc) value arena to minimise GC churn                |
| **Connection pool** | Separate read / write engine handles for concurrent workloads              |


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

Every non-encrypted Any Store database carries an XXH3-128 page-trailer
checksum (16 bytes/page) by default — corruption is caught on read.
There is no opt-out; the cost is <1% on writes and effectively zero on
reads. Encrypted databases derive integrity from the cipher's AEAD
authentication tag instead. File state is authoritative on reopen —
existing plain databases stay plain, existing checksum databases
auto-install the codec regardless of caller config.

Conceptually mirrors SQLite's [`cksumvfs`](https://sqlite.org/cksumvfs.html),
generalized to also surface AEAD failures via the same API.

```go
db, _ := anystore.Open(ctx, "data.db", &anystore.Config{
    // Wire monitoring at Open time so failures during the first page-1
    // read (which happens inside Open) are observable.
    OnIntegrityError: func(e anystore.IntegrityError) {
        log.Printf("integrity: page %d %v: %v", e.PageNo, e.Kind, e.Inner)
    },
    // Default false: corrupt pages cause reads to fail. Flip to true
    // for forensic dumps where you'd rather read garbage than lose
    // access to the rest of the data.
    ContinueOnIntegrityError: false,
})
// db now has IntegrityChecksum mode automatically.

// Walk every page and report mismatches (works in encrypted mode too).
rep, _ := db.VerifyIntegrity(ctx)
fmt.Printf("scanned %d pages, %d errors\n", rep.Pages, len(rep.Errors))
```

Page-1 DB header (first 100 bytes) is not covered by the per-page hash;
header invariants there are validated separately at open.
See [docs/btree/specs/integrity.md](docs/btree/specs/integrity.md) for the full design.


## Contributing

1. Fork & clone
2. `make test` – run unit tests
3. Create your feature branch
4. Open a PR and sign the CLA

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.


## ⚖️ License

Any Store is released under the MIT License – see [LICENSE](LICENSE) for details.
