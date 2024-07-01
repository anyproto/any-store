
# Any Store

Any Store is a document-oriented database with a MongoDB-like query language but uses JSON instead of BSON. It is built on top of SQLite and fastjson. The database supports transactions and indexes.

**Warning:** This library is not well tested and the API is still unstable. However, it is under active development.

## Installation

To install Any Store, run:
```sh
go get github.com/anyproto/any-store
```

For the CLI interface, run:
```sh
go install github.com/anyproto/any-store/cmd/any-store-cli@latest
```

## Usage Example

Here is an all-in-one example demonstrating various operations with Any Store:

```go
package main

import (
    "context"
    "fmt"
    "log"

    anystore "github.com/anyproto/any-store"
)

var ctx = context.Background()

func main() {
    // Open database
    db, err := anystore.Open(ctx, "/tmp/file.db", nil)
    if err != nil {
        log.Fatalf("unable to open db: %v", err)
    }
    defer func() {
        if err = db.Close(); err != nil {
            log.Fatalf("close db error: %v", err)
        }
    }()

    coll, err := db.Collection(ctx, "users")
    if err != nil {
        log.Fatalf("unable to open collection: %v", err)
    }

    // Cleanup
    if _, err = coll.Find(nil).Delete(ctx); err != nil {
        log.Fatalf("unable to delete collection documents: %v", err)
    }

    // Insert documents
    docId, err := coll.InsertOne(ctx, `{"name": "John"}`)
    if err != nil {
        log.Fatalf("unable to insert document: %v", err)
    }
    fmt.Println("user created", docId)

    docId, err = coll.InsertOne(ctx, map[string]any{"name": "Jane"})
    if err != nil {
        log.Fatalf("unable to insert document: %v", err)
    }
    fmt.Println("user created", docId)

    if err = coll.Insert(ctx, `{"id":1, "name": "Alex"}`, `{"id":2, "name": "rob"}`, `{"id":3, "name": "Paul"}`); err != nil {
        log.Fatalf("unable to insert documents: %v", err)
    }

    // Upsert
    docId, err = coll.UpsertOne(ctx, map[string]any{"name": "Mike"})
    if err != nil {
        log.Fatalf("unable to insert document: %v", err)
    }
    fmt.Println("user created", docId)

    // Update one
    if err = coll.UpdateOne(ctx, `{"id":2, "name": "Rob"}`); err != nil {
        log.Fatalf("unable to update document: %v", err)
    }

    // Find by ID
    doc, err := coll.FindId(ctx, 2)
    if err != nil {
        log.Fatalf("unable to find document: %v", err)
    }
    fmt.Println("document found:", doc.Value().String())

    // Count documents
    count, err := coll.Count(ctx)
    if err != nil {
        log.Fatalf("unable to count documents: %v", err)
    }
    fmt.Println("document count:", count)

    // Find many with condition
    iter, err := coll.Find(`{"id":{"$in":[1,2,3]}}`).Sort("-name").Limit(2).Offset(1).Iter(ctx)
    if err != nil {
        log.Fatalf("query failed: %v", err)
    }
    defer func() {
        if err = iter.Close(); err != nil {
            log.Fatalf("unable to close iterator: %v", err)
        }
    }()
    for iter.Next() {
        doc, err = iter.Doc()
        if err != nil {
            log.Fatalf("load document error: %v", err)
        }
        fmt.Println("findMany:", doc.Value().String())
    }

    // Create index
    if err = coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"name"}}); err != nil {
        fmt.Println("unable to ensure index:", err)
    }

    // Update many
    result, err := coll.Find(`{"name": {"$in": ["Rob","Alex"]}}`).Update(ctx, `{"$inc":{"rating":0.1}}`)
    if err != nil {
        log.Fatalf("cannot update documents: %v", err)
    }
    fmt.Printf("updated documents count: %d\n", result.Modified)

    // Transaction
    tx, err := db.WriteTx(ctx)
    if err != nil {
        log.Fatalf("cannot create tx: %v", err)
    }

    // It's important to pass tx.Context() to any operations within transaction
    result, err = coll.Find(`{"name": "Mike"}`).Delete(tx.Context())
    if err != nil {
        log.Fatalf("cannot delete document: %v", err)
    }
    fmt.Println("deleted count:", result.Modified)

    // Document is deleted inside transaction
    count, err = coll.Find(`{"name": "Mike"}`).Count(tx.Context())
    if err != nil {
        log.Fatalf("cannot count documents: %v", err)
    }
    fmt.Println("count within transaction:", count)

    // By passing other ctx we can find Mike in other transaction
    count, err = coll.Find(`{"name": "Mike"}`).Count(ctx)
    if err != nil {
        log.Fatalf("cannot count documents: %v", err)
    }
    fmt.Println("count outside transaction:", count)

    if err = tx.Commit(); err != nil {
        log.Fatalf("cannot commit transaction: %v", err)
    }
}
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE.md) file for details.