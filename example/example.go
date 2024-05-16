package example

import (
	"context"

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

	tx := db.WriteTx(ctx)

	if err = coll.UpsertId(tx.Context(), "1", `{"k":"v"}`); err != nil {
		return
	}

	if err = tx.Commit(ctx); err != nil {
		return
	}
}
