# Any Store

[![Go Reference](https://pkg.go.dev/badge/github.com/anyproto/any-store/v2.svg)](https://pkg.go.dev/github.com/anyproto/any-store/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/anyproto/any-store/v2)](https://goreportcard.com/report/github.com/anyproto/any-store/v2)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A **document-oriented embedded database** for Go with a MongoDB-like query language, built on an in-tree btree/pager/WAL storage engine derived from SQLite (aligned with its design, not a literal port — the on-disk format is our own).
Schema-less documents, rich indexes, full-text and vector search, ACID transactions, multi-process access, page-level integrity and optional encryption — pure Go, no CGO.

> **Status:** v2 is in beta (`v2.0.0-beta.x`) — the API is stabilizing; remaining changes before GA are expected to be minor. We actively dog-food the library in production and welcome early adopters & contributors.

## Features

* **Mongo-style queries** — `$eq/$in/$gt/...`, logical operators, dot-path fields, modifiers (`$set`, `$inc`, ...). Formal semantics in [docs/query-filter-contract.md](docs/query-filter-contract.md).
* **Indexes** — compound, unique, sparse, multikey (arrays), asc/desc per field; created and dropped at runtime.
* **Cost-based planner** — index selection driven by btree page statistics and per-prefix selectivity sketches; `Explain()` shows the chosen plan and its candidates.
* **Full-text search** — btree-resident inverted index with BM25 ranking via `$text`. See [docs/full-text-search.md](docs/full-text-search.md).
* **Vector search** — btree-resident ANN indexes (IVF-PQ / IVF-SQ, HNSW, brute-force) via `$knn`. See [docs/vector-search.md](docs/vector-search.md) and [docs/vector-engine.md](docs/vector-engine.md).
* **Aggregation** — MongoDB-style pipelines: shaping (`$match`, `$group`, `$sort`, `$unwind`, `$facet`, ...), computed expressions (arithmetic, conditionals, comparisons, date math), field-to-field `$expr` predicates, primary-key `$lookup` joins, and materialization via `$merge`/`$out` — with planner pushdown. See [docs/aggregation.md](docs/aggregation.md).
* **ACID transactions** — snapshot-isolated read transactions, single-writer write transactions.
* **Multi-process** — SQLite-like contract: any number of OS processes may open, read and write the same database file at any time (WAL + shared-memory index, busy handling, cross-process DDL reconciliation).
* **Durability** — idle auto-flush, explicit checkpointing, crash-safe WAL recovery, sentinel-triggered quick check after unclean shutdown.
* **Integrity & encryption** — per-page XXH3-128 checksums by default; optional AES-GCM or (X)ChaCha20-Poly1305 (AEAD doubles as integrity).
* **Streaming iterators** — low-memory scans with a cursor API; [AnyEnc](anyenc) arena encoding minimizes GC churn.
* **Cross-platform** — pure Go, no CGO; runs anywhere Go runs, including `js/wasm` (see [wasm/](wasm)).
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

    res, _ := users.Find(`{"id": {"$in": [1,2]}}`).Sort("-name").Iter(ctx)
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

> **Choosing document ids.** Unlike SQLite there is no hidden rowid — the document id is the btree key. Increment-like ids (`anyenc.NewObjectID()`, timestamps, sequences) keep inserts append-ordered and reads cache-friendly; fully random ids (e.g. UUIDv4) scatter them. In practice this only matters for very hot write/read paths or huge collections.

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

## Indexes

```go
_ = users.EnsureIndex(ctx,
    anystore.IndexInfo{Fields: []string{"name", "-createdDate"}},          // compound, mixed order
    anystore.IndexInfo{Fields: []string{"email"}, Unique: true},           // unique
    anystore.IndexInfo{Fields: []string{"nick"}, Sparse: true},            // skips missing/null
)

exp, _ := users.Find(`{"name": "Jane"}`).Explain(ctx)
fmt.Println(exp.Plan) // chosen index, cost breakdown, rejected candidates
```

Array fields index every element (multikey). The cost-based planner picks the cheapest index per query; `IndexHint` overrides it.

## Full-text search

```go
_ = docs.EnsureIndex(ctx, anystore.IndexInfo{
    Kind:   anystore.IndexKindFulltext,
    Fields: []string{"title", "body"},
})

res, _ := docs.Find(`{"$text": {"$search": "wal checkpoint"}}`).Limit(10).Iter(ctx)
```

BM25-ranked, transactional (a `$text` write is visible to queries in the same tx), combinable with any other filter, sort and limit.

## Vector search

```go
_ = docs.EnsureIndex(ctx, anystore.IndexInfo{
    Kind: anystore.IndexKindVector,
    Vector: &anystore.VectorParams{
        Field:  "embedding",
        Dim:    768,
        Metric: anystore.VectorCosine, // or VectorL2, VectorDot
        Mode:   anystore.VectorModeIVFSQ, // or IVFPQ, BTree/Hybrid (HNSW), BruteForce
    },
})

res, _ := docs.Find(`{"embedding": {"$knn": {"$query": [0.1, 0.2, ...], "$k": 10}}}`).Iter(ctx)
```

ANN results stream in distance order and compose with filters; `$ef` tunes recall per query, int8 quantization and IVF cell counts are per-index options. IVF modes are the production default for large collections.

## Aggregation

```go
res, _ := orders.Aggregate(`[
    {"$match": {"status": "paid"}},
    {"$group": {"_id": "$region", "total": {"$sum": "$amount"}}},
    {"$sort": {"total": -1}}
]`).Iter(ctx)
```

`$match`/`$sort`/`$limit` prefixes are pushed down into the index planner; the rest streams through the pipeline. Expressions compute derived values in `$project`/`$addFields`/`$group` (arithmetic, `$cond`/`$switch`, comparisons, `$dateDiff`-family date math over the first-class dateTime type); `$expr` brings field-to-field predicates into `$match`; `$lookup` resolves object-id relations as primary-key point lookups; `$facet` runs N sub-pipelines over one scan; `$merge`/`$out` materialize results into a collection where they are indexable like any other data. Operator reference and Mongo divergences: [docs/aggregation.md](docs/aggregation.md).

## Multi-process

The database file behaves like SQLite in WAL mode: any process may open it at any moment and read or write concurrently. Coordination runs through file locks and a shared-memory WAL index; snapshot reads are never blocked, writers serialize, and busy situations back off through a configurable handler. DDL from a peer process (created/dropped collections and indexes) is reconciled automatically on the next transaction.

## Durability

Any Store performs WAL checkpoints and fsync after idle periods.

```go
db, _ := anystore.Open(ctx, "data.db", &anystore.Config{
    Durability: anystore.DurabilityConfig{
        AutoFlush: true,
        IdleAfter: 20 * time.Second,  // flush after 20s of inactivity
        FlushMode: anystore.FlushModeCheckpointPassive, // ...Full, ...Restart, ...Truncate
        Sentinel:  true,  // track dirty state, quick-check on next open
    },
})

// Manual flush, e.g. before app suspension (waits up to 100ms for pending writes to settle).
db.Flush(ctx, 100*time.Millisecond, anystore.FlushModeCheckpointPassive)
```

**Sentinel:** when enabled, a `.lock` file marks not-explicitly-persisted writes so an unclean shutdown triggers an integrity quick check on open.

## Integrity

Every non-encrypted database carries an XXH3-128 page-trailer checksum (16 bytes/page) by default — corruption is caught on read. There is no opt-out; the cost is <1% on writes and effectively zero on reads. Encrypted databases derive integrity from the cipher's AEAD tag instead. File state is authoritative on reopen — existing plain databases stay plain, existing checksum databases auto-install the codec regardless of caller config.

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

## Encryption

```go
db, _ := anystore.Open(ctx, "data.db", &anystore.Config{
    Encryption: anystore.EncryptionConfig{
        Passphrase: []byte("correct horse battery staple"),
        CipherType: anystore.CipherXChaCha20Poly1305, // or CipherAES256GCM, CipherChaCha20Poly1305
    },
})
```

Whole-file page-level AEAD; bring-your-own key management via `EncryptionConfig.Codec`.

## Documentation

* **API reference** — [pkg.go.dev/github.com/anyproto/any-store/v2](https://pkg.go.dev/github.com/anyproto/any-store/v2)
* **Query semantics** — [docs/query-filter-contract.md](docs/query-filter-contract.md)
* **Full-text search** — [docs/full-text-search.md](docs/full-text-search.md)
* **Vector search** — [docs/vector-search.md](docs/vector-search.md), [docs/vector-engine.md](docs/vector-engine.md)
* **Aggregation** — [docs/aggregation.md](docs/aggregation.md)
* **CLI manual** — `any-store-cli2 --help`

## Contributing

1. Fork & clone
2. `make test` — run unit tests
3. Create your feature branch
4. Open a PR and sign the CLA

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.

## ⚖️ License

Any Store is released under the MIT License — see [LICENSE](LICENSE) for details.
