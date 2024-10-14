package main

import (
	"context"
	"fmt"
	"log"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
)

var ctx = context.Background()

func main() {
	// open database
	db, err := anystore.Open(ctx, "/tmp/file.db", nil)
	if err != nil {
		log.Fatalf("unable to open db: %v", err)
	}

	defer func() {
		if err = db.Close(); err != nil {
			log.Fatalf("close db eroor: %v", err)
		}
	}()

	coll, err := db.Collection(ctx, "users")
	if err != nil {
		log.Fatalf("unable to open collection: %v", err)
	}

	// insert document, convert from json
	doc := anyenc.MustParseJson(`{"id":1, "name": "John"}`)
	err = coll.Insert(ctx, doc)
	if err != nil {
		log.Fatalf("unable to insert document: %v", err)
	}

	// create document
	a := &anyenc.Arena{}
	doc = a.NewObject()
	doc.Set("id", a.NewNumberInt(2))
	doc.Set("name", a.NewString("Jane"))
	err = coll.Insert(ctx, doc)
	if err != nil {
		log.Fatalf("unable to insert document: %v", err)
	}

	// batch insert
	if err = coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":3, "name": "Alex"}`),
		anyenc.MustParseJson(`{"id":4, "name": "rob"}`),
		anyenc.MustParseJson(`{"id":5, "name": "Paul"}`),
	); err != nil {
		log.Fatalf("unable to insert document: %v", err)
	}

	// upsert
	err = coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":6, "name": "Mike"}`))
	if err != nil {
		log.Fatalf("unable to insert document: %v", err)
	}

	// update one
	if err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":4, "name": "Rob"}`)); err != nil {
		log.Fatalf("unable to update document: %v", err)
	}

	// find by id
	res, err := coll.FindId(ctx, 2)
	if err != nil {
		log.Fatalf("unable to find document: %v", err)
	}
	fmt.Println("document found:", res.Value().String())

	// collection count
	count, err := coll.Count(ctx)
	if err != nil {
		log.Fatalf("unable to count documents: %v", err)
	}
	fmt.Println("document count:", count)

	// find many with condition
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
		res, err = iter.Doc()
		if err != nil {
			log.Fatalf("load document error: %v", err)
		}
		fmt.Println("findMany:", res.Value().String())
	}

	// create index
	if err = coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"name"}}); err != nil {
		fmt.Println("unable to ensure index:", err)
	}

	// update many
	result, err := coll.Find(`{"name": {"$in": ["Rob","Alex"]}}`).Update(ctx, `{"$inc":{"rating":0.1}}`)
	if err != nil {
		log.Fatalf("cannot update document: %v", err)
	}
	fmt.Printf("updated documents count: %d\n", result.Modified)

	// transaction
	tx, err := db.WriteTx(ctx)
	if err != nil {
		log.Fatalf("cannot create tx: %v", err)
	}

	// it's important to pass tx.Context() to any operations within transaction
	// because sqlite can handle only one transaction in time - passing ctx instead tx.Context() will cause possibly deadlock
	result, err = coll.Find(`{"name": "Mike"}`).Delete(tx.Context())
	if err != nil {
		log.Fatalf("cannot delete document: %v", err)
	}
	fmt.Println("deleted count:", result.Modified)

	// document is deleted inside transaction
	count, err = coll.Find(`{"name": "Mike"}`).Count(tx.Context())
	if err != nil {
		log.Fatalf("cannot count documents: %v", err)
	}
	fmt.Println("count within transaction:", count)

	// by passing other ctx we can find Mike in other transaction
	count, err = coll.Find(`{"name": "Mike"}`).Count(ctx)
	if err != nil {
		log.Fatalf("cannot count documents: %v", err)
	}
	fmt.Println("count outside transaction:", count)

	if err = tx.Commit(); err != nil {
		log.Fatalf("cannot commit transaction: %v", err)
	}

}
