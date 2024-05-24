package main

import (
	"context"
	"fmt"

	anystore "github.com/anyproto/any-store"
)

var ctx = context.Background()

func main() {
	db, err := anystore.Open(ctx, "file.db", nil)
	if err != nil {
		return
	}

	coll, err := db.OpenCollection(ctx, "myColl")
	if err != nil {
		return
	}

	tx, err := db.WriteTx(ctx)
	if err != nil {
		return
	}

	docId, err := coll.UpsertOne(tx.Context(), `{"k":"v"}`)
	if err != nil {
		return
	}
	fmt.Println(docId)

	if err = tx.Commit(); err != nil {
		return
	}

	_ = db.Close()
}
