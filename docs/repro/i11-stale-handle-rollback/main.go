// Dynamic repro for I-11: create collection inside ambient tx, roll back the
// outer tx, then keep using the stale handle. Watch for silent corruption.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	ctx := context.Background()
	// Fresh dir per run: leftover state (collections x/y already on disk)
	// changes the page-reuse timing the repro depends on. Same pattern as the
	// i10 repro.
	dir := "/var/tmp/anystore-repro/i11"
	must(os.RemoveAll(dir))
	must(os.MkdirAll(dir, 0755))
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "i11.db")

	db, err := anystore.Open(ctx, path, nil)
	must(err)

	// 1. Open a top-level write tx, create collection "x" via the ambient ctx.
	tx, err := db.WriteTx(ctx)
	must(err)
	collX, err := db.Collection(tx.Context(), "x")
	must(err)
	fmt.Println("created collection x inside tx, handle:", collX.Name())

	// 2. Roll back the outer tx — namespace creation should be undone on disk.
	must(tx.Rollback())
	fmt.Println("outer tx rolled back")

	// 3. Get "x" again outside any tx — per the claim, returns the stale handle.
	collX2, err := db.Collection(ctx, "x")
	if err != nil {
		fmt.Println("db.Collection after rollback returned error (SAFE):", err)
		return
	}
	fmt.Println("got collection x after rollback (stale handle?)")

	// 4. Create a fresh collection "y" first (reuses the freed root page) and fill it.
	collY, err := db.Collection(ctx, "y")
	must(err)
	for i := 0; i < 50; i++ {
		must(collY.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":"y-%d"}`, i))))
	}
	cntY, err := collY.Count(ctx)
	fmt.Println("y count:", cntY, "err:", err)

	// 5. NOW write through the stale x handle — its cached root page is y's root.
	doc := anyenc.MustParseJson(`{"id":"doc-in-x","v":1}`)
	if err := collX2.Insert(ctx, doc); err != nil {
		fmt.Println("insert into stale x failed (LOUD):", err)
	} else {
		fmt.Println("insert into stale x SUCCEEDED — checking where it landed...")
		found, ferr := collY.FindId(ctx, "doc-in-x")
		fmt.Println("y.FindId(doc-in-x):", found, "err:", ferr)
		cntY2, _ := collY.Count(ctx)
		fmt.Println("y count after x-insert:", cntY2)
	}

	// 6. Read x back through the same handle and via integrity check.
	cntX, err := collX2.Count(ctx)
	fmt.Println("x count:", cntX, "err:", err)

	if err := db.IntegrityCheck(ctx); err != nil {
		fmt.Println("IntegrityCheck FAILED:", err)
	} else {
		fmt.Println("IntegrityCheck ok")
	}
	must(db.Close())

	// 7. Reopen and check what's on disk.
	db2, err := anystore.Open(ctx, path, nil)
	must(err)
	defer db2.Close()
	names, _ := db2.GetCollectionNames(ctx)
	fmt.Println("collections on disk after reopen:", names)
	if err := db2.IntegrityCheck(ctx); err != nil {
		fmt.Println("IntegrityCheck after reopen FAILED:", err)
	} else {
		fmt.Println("IntegrityCheck after reopen ok")
	}
	for _, n := range names {
		c, err := db2.OpenCollection(ctx, n)
		if err != nil {
			fmt.Println("open", n, "failed:", err)
			continue
		}
		cnt, err := c.Count(ctx)
		fmt.Println("collection", n, "count:", cnt, "err:", err)
	}
}
