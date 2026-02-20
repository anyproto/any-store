package main

import (
	"context"
	"fmt"
	"sync/atomic"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
)

func registerAll(ctx context.Context, golden, writeColl, batchWriteColl, noIdxColl, bulkColl anystore.Collection, db anystore.DB, n int) []Scenario {
	var all []Scenario
	all = append(all, registerCRUD(ctx, golden, writeColl, batchWriteColl, db, n)...)
	all = append(all, registerIdQueries(ctx, golden, n)...)
	all = append(all, registerFullscan(ctx, noIdxColl)...)
	all = append(all, registerSimpleIndex(ctx, golden, n)...)
	all = append(all, registerUniqueIndex(ctx, golden, n)...)
	all = append(all, registerCompoundIndex(ctx, golden, n)...)
	all = append(all, registerCompoundRev(ctx, golden, n)...)
	all = append(all, registerCBO(ctx, golden, n)...)
	all = append(all, registerSort(ctx, golden)...)
	all = append(all, registerFilterSort(ctx, golden)...)
	all = append(all, registerBulk(ctx, bulkColl, db)...)
	return all
}

// --- crud group ---

func registerCRUD(ctx context.Context, golden, writeColl, batchWriteColl anystore.Collection, db anystore.DB, n int) []Scenario {
	incMod := query.MustParseModifier(`{"$inc":{"val":1}}`)
	var insertCounter atomic.Int64
	insertCounter.Store(100000) // start beyond write coll's 1000 docs

	arena := &anyenc.Arena{}

	return []Scenario{
		{
			Group: "crud",
			Name:  "FindId",
			Run: func() int {
				_, err := golden.FindId(ctx, 500)
				if err != nil {
					panic(err)
				}
				return 1
			},
			Check: func() error {
				doc, err := golden.FindId(ctx, 500)
				if err != nil {
					return err
				}
				if doc == nil {
					return fmt.Errorf("doc is nil")
				}
				return nil
			},
		},
		{
			Group: "crud",
			Name:  "Insert",
			Run: func() int {
				id := insertCounter.Add(1)
				arena := &anyenc.Arena{}
				doc := arena.NewObject()
				doc.Set("id", arena.NewNumberInt(int(id)))
				doc.Set("a", arena.NewNumberInt(int(id)%100))
				doc.Set("val", arena.NewNumberInt(42))
				if err := writeColl.Insert(ctx, doc); err != nil {
					panic(err)
				}
				return 1
			},
			Check: func() error {
				id := insertCounter.Add(1)
				a := &anyenc.Arena{}
				doc := a.NewObject()
				doc.Set("id", a.NewNumberInt(int(id)))
				doc.Set("val", a.NewNumberInt(1))
				return writeColl.Insert(ctx, doc)
			},
		},
		{
			Group: "crud",
			Name:  "UpdateId",
			Run: func() int {
				_, err := writeColl.UpdateId(ctx, 500, incMod)
				if err != nil {
					panic(err)
				}
				return 1
			},
			Check: func() error {
				res, err := writeColl.UpdateId(ctx, 500, incMod)
				if err != nil {
					return err
				}
				if res.Modified != 1 {
					return fmt.Errorf("expected modified=1, got %d", res.Modified)
				}
				return nil
			},
		},
		{
			Group: "crud",
			Name:  "DeleteReinsert",
			Run: func() int {
				if err := writeColl.DeleteId(ctx, 999); err != nil {
					panic(err)
				}
				doc := arena.NewObject()
				doc.Set("id", arena.NewNumberInt(999))
				doc.Set("a", arena.NewNumberInt(99))
				doc.Set("val", arena.NewNumberInt(42))
				if err := writeColl.Insert(ctx, doc); err != nil {
					panic(err)
				}
				arena.Reset()
				return 1
			},
			Check: func() error {
				if err := writeColl.DeleteId(ctx, 999); err != nil {
					return err
				}
				a := &anyenc.Arena{}
				doc := a.NewObject()
				doc.Set("id", a.NewNumberInt(999))
				doc.Set("val", a.NewNumberInt(1))
				return writeColl.Insert(ctx, doc)
			},
		},
		{
			Group: "crud",
			Name:  "BatchInsert100",
			Run: func() int {
				a := &anyenc.Arena{}
				base := insertCounter.Add(100)
				docs := make([]*anyenc.Value, 100)
				for i := range docs {
					doc := a.NewObject()
					doc.Set("id", a.NewNumberInt(int(base)+i))
					doc.Set("a", a.NewNumberInt(i%100))
					doc.Set("val", a.NewNumberInt(42))
					docs[i] = doc
				}
				if err := batchWriteColl.Insert(ctx, docs...); err != nil {
					panic(err)
				}
				return 100
			},
			Check: func() error {
				a := &anyenc.Arena{}
				base := insertCounter.Add(100)
				docs := make([]*anyenc.Value, 100)
				for i := range docs {
					doc := a.NewObject()
					doc.Set("id", a.NewNumberInt(int(base)+i))
					doc.Set("val", a.NewNumberInt(1))
					docs[i] = doc
				}
				return batchWriteColl.Insert(ctx, docs...)
			},
		},
		{
			Group: "crud",
			Name:  "BatchInsert1000",
			Run: func() int {
				a := &anyenc.Arena{}
				base := insertCounter.Add(1000)
				docs := make([]*anyenc.Value, 1000)
				for i := range docs {
					doc := a.NewObject()
					doc.Set("id", a.NewNumberInt(int(base)+i))
					doc.Set("a", a.NewNumberInt(i%100))
					doc.Set("val", a.NewNumberInt(42))
					docs[i] = doc
				}
				if err := batchWriteColl.Insert(ctx, docs...); err != nil {
					panic(err)
				}
				return 1000
			},
			Check: func() error {
				a := &anyenc.Arena{}
				base := insertCounter.Add(1000)
				docs := make([]*anyenc.Value, 1000)
				for i := range docs {
					doc := a.NewObject()
					doc.Set("id", a.NewNumberInt(int(base)+i))
					doc.Set("val", a.NewNumberInt(1))
					docs[i] = doc
				}
				return batchWriteColl.Insert(ctx, docs...)
			},
		},
		{
			Group: "crud",
			Name:  "BatchUpdate",
			Run: func() int {
				res, err := batchWriteColl.Find(`{"a":50}`).Update(ctx, `{"$inc":{"val":1}}`)
				if err != nil {
					panic(err)
				}
				return res.Matched
			},
			Check: func() error {
				res, err := batchWriteColl.Find(`{"a":50}`).Update(ctx, `{"$inc":{"val":1}}`)
				if err != nil {
					return err
				}
				if res.Matched < 100 {
					return fmt.Errorf("expected matched>=100, got %d", res.Matched)
				}
				return nil
			},
		},
	}
}

// --- id_queries group ---

func registerIdQueries(ctx context.Context, golden anystore.Collection, n int) []Scenario {
	mid := n / 2
	last := n - 1
	return []Scenario{
		countScenario(ctx, "id_queries", "IdEq", golden,
			fmt.Sprintf(`{"id":%d}`, mid), 1),
		countScenario(ctx, "id_queries", "IdIn3", golden,
			fmt.Sprintf(`{"id":{"$in":[1,%d,%d]}}`, mid, last), 3),
		countScenario(ctx, "id_queries", "IdIn10", golden,
			fmt.Sprintf(`{"id":{"$in":%s}}`, spreadIds(n, 10)), 10),
		countScenario(ctx, "id_queries", "IdIn100", golden,
			fmt.Sprintf(`{"id":{"$in":%s}}`, spreadIds(n, 100)), 100),
		countScenario(ctx, "id_queries", "IdIn1000", golden,
			fmt.Sprintf(`{"id":{"$in":%s}}`, spreadIds(n, 1000)), 1000),
	}
}

// --- fullscan group ---

func registerFullscan(ctx context.Context, noIdxColl anystore.Collection) []Scenario {
	return []Scenario{
		countScenario(ctx, "fullscan", "Count", noIdxColl, nil, 10000),
		countScenario(ctx, "fullscan", "EqFilter", noIdxColl, `{"a":50}`, 100),
		countScenario(ctx, "fullscan", "RangeFilter", noIdxColl, `{"a":{"$gte":40,"$lte":60}}`, 2100),
		countScenarioGt(ctx, "fullscan", "ComplexAndOr", noIdxColl,
			`{"$and":[{"a":{"$gt":50}},{"$or":[{"b":10},{"c":1}]}]}`, 0),
		countScenario(ctx, "fullscan", "NeFilter", noIdxColl, `{"a":{"$ne":50}}`, 9900),
	}
}

// --- simple_index group ---

func registerSimpleIndex(ctx context.Context, golden anystore.Collection, n int) []Scenario {
	return []Scenario{
		countScenario(ctx, "simple_index", "Eq", golden, `{"a":50}`, n/100),
		countScenario(ctx, "simple_index", "Range", golden, `{"a":{"$gte":40,"$lte":60}}`, n/100*21),
		countScenario(ctx, "simple_index", "In", golden, `{"a":{"$in":[10,30,50,70,90]}}`, n/100*5),
		countScenario(ctx, "simple_index", "HighSelectivity", golden, `{"a":50}`, n/100),
		countScenario(ctx, "simple_index", "LowSelectivity", golden, `{"c":0}`, countC(n, 0)),
	}
}

// --- unique_index group ---

func registerUniqueIndex(ctx context.Context, golden anystore.Collection, n int) []Scenario {
	mid := n / 2
	last := n - 1
	return []Scenario{
		countScenario(ctx, "unique_index", "Eq", golden,
			fmt.Sprintf(`{"email":"user%d@test.com"}`, mid), 1),
		countScenario(ctx, "unique_index", "In3", golden,
			fmt.Sprintf(`{"email":{"$in":["user1@test.com","user%d@test.com","user%d@test.com"]}}`, mid, last), 3),
	}
}

// --- compound_index group ---

func registerCompoundIndex(ctx context.Context, golden anystore.Collection, n int) []Scenario {
	return []Scenario{
		countScenario(ctx, "compound_index", "FullMatch", golden, `{"a":50,"b":25}`, n/100/50),
		countScenario(ctx, "compound_index", "PrefixOnly", golden, `{"a":50}`, n/100),
		countScenarioGt(ctx, "compound_index", "PrefixRange", golden,
			`{"a":50,"b":{"$gte":10,"$lte":30}}`, 0),
	}
}

// --- compound_rev group ---

func registerCompoundRev(ctx context.Context, golden anystore.Collection, n int) []Scenario {
	return []Scenario{
		countScenario(ctx, "compound_rev", "FullMatch", golden, `{"a":50,"b":25}`, n/100/50),
		iterScenario(ctx, "compound_rev", "SortAscDesc", golden, nil, "a,-b", 100),
		iterScenario(ctx, "compound_rev", "FilterSort", golden,
			`{"a":{"$gte":40,"$lte":60}}`, "a,-b", 100),
	}
}

// --- cbo group ---

func registerCBO(ctx context.Context, golden anystore.Collection, n int) []Scenario {
	return []Scenario{
		countScenario(ctx, "cbo", "TwoIdx", golden, `{"a":50,"c":5}`, countAC(n, 50, 5)),
		countScenario(ctx, "cbo", "CompoundVsSimple", golden, `{"a":50,"b":25}`, n/100/50),
		countScenario(ctx, "cbo", "ThreeIdx", golden, `{"a":50,"b":25,"c":5}`, countABC(n, 50, 25, 5)),
	}
}

// --- sort group ---

func registerSort(ctx context.Context, golden anystore.Collection) []Scenario {
	return []Scenario{
		iterScenario(ctx, "sort", "NoIdx", golden, nil, "val", 100),
		iterScenario(ctx, "sort", "WithIdx", golden, nil, "a", 100),
		iterScenario(ctx, "sort", "DescNoIdx", golden, nil, "-val", 100),
		iterScenario(ctx, "sort", "DescWithIdx", golden, nil, "-a", 100),
		iterScenario(ctx, "sort", "LimitNoIdx", golden, nil, "val", 10),
		iterScenario(ctx, "sort", "LimitWithIdx", golden, nil, "a", 10),
	}
}

// --- filter_sort group ---

func registerFilterSort(ctx context.Context, golden anystore.Collection) []Scenario {
	return []Scenario{
		iterScenario(ctx, "filter_sort", "NoIdx", golden,
			`{"a":{"$gte":40,"$lte":60}}`, "val", 100),
		iterScenario(ctx, "filter_sort", "SimpleIdx", golden,
			`{"a":{"$gte":40,"$lte":60}}`, "a", 100),
		iterScenario(ctx, "filter_sort", "CompoundIdx", golden,
			`{"a":50}`, "b", 100),
		iterScenario(ctx, "filter_sort", "CompoundRevIdx", golden,
			`{"a":50}`, "-b", 100),
		iterScenario(ctx, "filter_sort", "WithLimit10", golden,
			`{"a":{"$gte":40,"$lte":60}}`, "a", 10),
	}
}

// --- bulk group ---

func registerBulk(ctx context.Context, bulkColl anystore.Collection, db anystore.DB) []Scenario {
	return []Scenario{
		{
			Group: "bulk",
			Name:  "Update",
			Run: func() int {
				res, err := bulkColl.Find(`{"a":50}`).Update(ctx, `{"$inc":{"val":1}}`)
				if err != nil {
					panic(err)
				}
				return res.Matched
			},
			Check: func() error {
				res, err := bulkColl.Find(`{"a":50}`).Update(ctx, `{"$inc":{"val":1}}`)
				if err != nil {
					return err
				}
				if res.Matched != 100 {
					return fmt.Errorf("expected matched=100, got %d", res.Matched)
				}
				return nil
			},
		},
		{
			Group: "bulk",
			Name:  "Delete",
			Run: func() int {
				res, err := bulkColl.Find(`{"a":99}`).Delete(ctx)
				if err != nil {
					panic(err)
				}
				// Re-insert deleted docs
				reinsert(ctx, bulkColl, 99, res.Matched)
				return res.Matched
			},
			Check: func() error {
				res, err := bulkColl.Find(`{"a":99}`).Delete(ctx)
				if err != nil {
					return err
				}
				if res.Matched != 100 {
					return fmt.Errorf("expected matched=100, got %d", res.Matched)
				}
				reinsert(ctx, bulkColl, 99, res.Matched)
				return nil
			},
		},
	}
}

// --- helpers ---

func countScenario(ctx context.Context, group, name string, coll anystore.Collection, filter any, expected int) Scenario {
	return Scenario{
		Group: group,
		Name:  name,
		Run: func() int {
			count, err := coll.Find(filter).Count(ctx)
			if err != nil {
				panic(err)
			}
			return count
		},
		Check: func() error {
			count, err := coll.Find(filter).Count(ctx)
			if err != nil {
				return err
			}
			if count != expected {
				return fmt.Errorf("expected %d, got %d", expected, count)
			}
			return nil
		},
	}
}

func countScenarioGt(ctx context.Context, group, name string, coll anystore.Collection, filter any, minExpected int) Scenario {
	return Scenario{
		Group: group,
		Name:  name,
		Run: func() int {
			count, err := coll.Find(filter).Count(ctx)
			if err != nil {
				panic(err)
			}
			return count
		},
		Check: func() error {
			count, err := coll.Find(filter).Count(ctx)
			if err != nil {
				return err
			}
			if count <= minExpected {
				return fmt.Errorf("expected > %d, got %d", minExpected, count)
			}
			return nil
		},
	}
}

func iterScenario(ctx context.Context, group, name string, coll anystore.Collection, filter any, sort string, limit int) Scenario {
	return Scenario{
		Group: group,
		Name:  name,
		Run: func() int {
			q := coll.Find(filter).Sort(sort).Limit(uint(limit))
			iter, err := q.Iter(ctx)
			if err != nil {
				panic(err)
			}
			count := 0
			for iter.Next() {
				_, _ = iter.Doc()
				count++
			}
			if err := iter.Close(); err != nil {
				panic(err)
			}
			return count
		},
		Check: func() error {
			q := coll.Find(filter).Sort(sort).Limit(uint(limit))
			iter, err := q.Iter(ctx)
			if err != nil {
				return err
			}
			count := 0
			for iter.Next() {
				_, _ = iter.Doc()
				count++
			}
			if err := iter.Close(); err != nil {
				return err
			}
			if count != limit {
				return fmt.Errorf("expected %d docs, got %d", limit, count)
			}
			return nil
		},
	}
}

func spreadIds(n, count int) string {
	step := n / count
	ids := make([]int, count)
	for i := range ids {
		ids[i] = i * step
	}
	b := "["
	for i, id := range ids {
		if i > 0 {
			b += ","
		}
		b += fmt.Sprintf("%d", id)
	}
	b += "]"
	return b
}

// countC returns how many docs in [0,n) have (i/5000)%10 == cVal.
func countC(n, cVal int) int {
	cycle := 50000 // 5000 * 10
	full := n / cycle
	rem := n % cycle
	count := full * 5000
	start := cVal * 5000
	if rem > start {
		extra := rem - start
		if extra > 5000 {
			extra = 5000
		}
		count += extra
	}
	return count
}

// countAC returns how many docs in [0,n) have i%100==aVal AND (i/5000)%10==cVal.
// With mixed-radix: i = a + 100*b + 5000*c + 50000*...
// For a=aVal, c=cVal: i = aVal + 100*b + 5000*cVal + 50000*k, b in [0,50), k >= 0
func countAC(n, aVal, cVal int) int {
	count := 0
	base := aVal + 5000*cVal // smallest i with this a,c
	for i := base; i < n; i += 50000 {
		// each starting i has 50 docs (b=0..49) if they fit
		for b := 0; b < 50; b++ {
			if i+b*100 < n {
				count++
			}
		}
	}
	return count
}

// countABC returns how many docs in [0,n) have i%100==aVal AND (i/100)%50==bVal AND (i/5000)%10==cVal.
func countABC(n, aVal, bVal, cVal int) int {
	// i = aVal + 100*bVal + 5000*cVal + 50000*k
	count := 0
	base := aVal + 100*bVal + 5000*cVal
	for i := base; i < n; i += 50000 {
		count++
	}
	return count
}

func reinsert(ctx context.Context, coll anystore.Collection, aVal, count int) {
	arena := &anyenc.Arena{}
	// The docs with a=99 had ids: 99, 199, 299, ..., 9999
	docs := make([]*anyenc.Value, 0, count)
	for i := 0; i < count; i++ {
		id := aVal + i*100
		doc := arena.NewObject()
		doc.Set("id", arena.NewNumberInt(id))
		doc.Set("a", arena.NewNumberInt(id%100))
		doc.Set("b", arena.NewNumberInt((id/100)%50))
		doc.Set("c", arena.NewNumberInt((id/5000)%10))
		doc.Set("val", arena.NewNumberInt(id*7%1000))
		doc.Set("email", arena.NewString(fmt.Sprintf("user%d@test.com", id)))
		docs = append(docs, doc)
	}
	if err := coll.Insert(ctx, docs...); err != nil {
		panic(fmt.Sprintf("reinsert: %v", err))
	}
}
