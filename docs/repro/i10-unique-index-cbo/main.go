// Repro sweep: unique-secondary-index point lookups on a v2 collection with a
// custom primary key. Isolates the trigger: id randomness / doc count / payload size.
package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"regexp"
	"runtime/pprof"
	"time"

	anystore2 "github.com/anyproto/any-store/v2"
	anyenc2 "github.com/anyproto/any-store/v2/anyenc"
	query2 "github.com/anyproto/any-store/v2/query"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

const cidChars = "abcdefghijklmnopqrstuvwxyz234567"

func newCid(r *rand.Rand) string {
	b := make([]byte, 59)
	copy(b, "bafyrei")
	for i := 7; i < 59; i++ {
		b[i] = cidChars[r.IntN(len(cidChars))]
	}
	return string(b)
}

var planRe = regexp.MustCompile(`(?m)^Plan: \S+.*$|est_rows=\d+\s+\[chosen\]`)

func cell(nDocs, payloadSize int, randomIds bool) {
	ctx := context.Background()
	r := rand.New(rand.NewChaCha8([32]byte{5}))
	dir := "/var/tmp/anystore-layoutbench/repro"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)
	db, err := anystore2.Open(ctx, dir+"/store.db", &anystore2.Config{})
	check(err)
	coll, err := db.Collection(ctx, "changes", anystore2.CollectionOptions{PrimaryKey: "k"})
	check(err)
	check(coll.EnsureIndex(ctx, anystore2.IndexInfo{Fields: []string{"id"}, Unique: true}))

	a := &anyenc2.Arena{}
	rng := rand.NewChaCha8([32]byte{6})
	ids := make([]string, 0, nDocs)
	for b := 0; b < nDocs/10; b++ {
		tx, err := db.WriteTx(ctx)
		check(err)
		a.Reset()
		var docs []*anyenc2.Value
		for i := 0; i < 10; i++ {
			n := b*10 + i
			var id string
			if randomIds {
				id = newCid(r)
			} else {
				id = fmt.Sprintf("bafyreichange%046d", n)
			}
			ids = append(ids, id)
			v := a.NewObject()
			v.Set("k", a.NewString(fmt.Sprintf("%06d:%04d", n/50, n%50)))
			v.Set("id", a.NewString(id))
			payload := make([]byte, payloadSize)
			rng.Read(payload)
			v.Set("r", a.NewBinary(payload))
			docs = append(docs, v)
		}
		check(coll.Insert(tx.Context(), docs...))
		check(tx.Commit())
	}

	filter := query2.Key{Path: []string{"id"}, Filter: query2.NewComp(query2.CompOpEq, ids[nDocs/2+7])}
	exp, err := coll.Find(filter).Limit(1).Explain(ctx)
	check(err)
	plan := planRe.FindAllString(exp.Plan, -1)

	if os.Getenv("PROFILE") != "" {
		pf, _ := os.Create("cpu.prof")
		pprof.StartCPUProfile(pf)
		defer pprof.StopCPUProfile()
	}
	t0 := time.Now()
	gets := 20
	if os.Getenv("PROFILE") != "" {
		gets = 300
	}
	for i := 0; i < gets; i++ {
		f := query2.Key{Path: []string{"id"}, Filter: query2.NewComp(query2.CompOpEq, ids[(i*2571)%len(ids)])}
		iter, err := coll.Find(f).Limit(1).Iter(ctx)
		check(err)
		found := iter.Next()
		check(iter.Err())
		iter.Close()
		if !found {
			panic("not found")
		}
	}
	d := time.Since(t0)

	// plan-only timing: is the 8ms in planning or execution?
	t1 := time.Now()
	for i := 0; i < gets; i++ {
		f := query2.Key{Path: []string{"id"}, Filter: query2.NewComp(query2.CompOpEq, ids[(i*2571)%len(ids)])}
		_, err := coll.Find(f).Limit(1).Explain(ctx)
		check(err)
	}
	dPlan := time.Since(t1)

	// count per-value plan choices across the get ids
	fullScans, idxSeeks := 0, 0
	for i := 0; i < 20; i++ {
		f := query2.Key{Path: []string{"id"}, Filter: query2.NewComp(query2.CompOpEq, ids[(i*2571)%len(ids)])}
		e, err := coll.Find(f).Limit(1).Explain(ctx)
		check(err)
		if len(e.Plan) > 6 && e.Plan[6:14] == "FullScan" {
			fullScans++
		} else {
			idxSeeks++
		}
	}
	fmt.Printf("docs=%6d payload=%4dB randomIds=%-5v -> %.3fms/get (plan-only %.3fms) planFlip: %d FullScan / %d IndexSeek  plan=%v\n",
		nDocs, payloadSize, randomIds, d.Seconds()*1000/float64(gets), dPlan.Seconds()*1000/float64(gets), fullScans, idxSeeks, plan)
	check(db.Close())
	os.RemoveAll(dir)
}

func main() {
	if os.Getenv("PROFILE") != "" {
		cell(175000, 2800, false) // IndexSeek plan, still 8ms/get
		return
	}
	cell(50000, 1000, false)  // baseline (was fast)
	cell(50000, 1000, true)   // random cids
	cell(50000, 2800, true)   // random cids + big payload
	cell(175000, 1000, true)  // big count, small payload
	cell(175000, 2800, false) // big count, big payload, sequential ids
	cell(175000, 2800, true)  // full bench profile
}
