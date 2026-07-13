package query

import (
	"sync"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// These tests pin the package-level filter contract (see docs/query-filter-contract.md):
//
//  1. A filter is built once and may be shared by any number of queries running
//     concurrently. Ok and IndexBounds take no locks, so they must not mutate
//     filter state, and bound bytes returned by IndexBounds must tolerate the
//     planner's bound-extension appends (AdjustBoundsForNonUnique,
//     padReverseBounds) without writing into shared filter memory.
//  2. Ok with a warm per-query DocBuffer is alloc-free.

// filterZoo covers every Filter implementation reachable from ParseCondition.
const filterZoo = `{
	"a": {"$in": [1, 2, 3]},
	"b": {"$gte": 10, "$lte": 20},
	"c": "test",
	"d": {"$ne": 5},
	"e": {"$nin": [7, 8]},
	"g": {"$all": [1, 2]},
	"h": {"$exists": true},
	"i": {"$type": "number"},
	"j": {"$regex": "^pre"},
	"k": {"$size": 2},
	"$or": [{"l": 1}, {"l": {"$gt": 100}}],
	"$nor": [{"m": "x"}],
	"n": {"$not": {"$eq": 3}},
	"vf": {"$eq": {"$vector": [1, 2]}},
	"vt": {"$type": "vectorF32"},
	"$text": {"$search": "hello"}
}`

// TestFilterConcurrentReuse shares one built filter between goroutines that
// concurrently evaluate Ok (each with its own DocBuffer, as the executor does)
// and compute IndexBounds with planner-style bound extension. Run under -race.
func TestFilterConcurrentReuse(t *testing.T) {
	f := MustParseCondition(filterZoo)
	docs := []*anyenc.Value{
		anyenc.MustParseJson(`{"a":1,"b":15,"c":"test","d":1,"e":1,"g":[1,2],"h":1,"i":1,"j":"prefix","k":[1,2],"l":1,"m":"y","n":1}`),
		anyenc.MustParseJson(`{"a":9,"b":[11,19],"d":5,"j":["x","pre"],"k":[1],"l":101}`),
		anyenc.MustParseJson(`{}`),
	}
	fields := []string{"a", "b", "c", "d", "e", "g", "h", "i", "j", "k", "l", "m", "n"}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := &syncpool.DocBuffer{}
			for range 200 {
				for _, d := range docs {
					f.Ok(d, buf)
				}
				for _, field := range fields {
					for _, b := range f.IndexBounds(field, nil) {
						// Planner-style bound extension: must reallocate, never
						// write into shared filter bytes (EqValue, In keys).
						if len(b.Start) > 0 {
							_ = append(b.Start, 0x01)
						}
						if len(b.End) > 0 {
							_ = append(b.End, 0xff)
						}
					}
				}
			}
		}()
	}
	wg.Wait()
}

// TestIndexBounds_FilterOwnedBytesAreCapped pins the mechanism behind the
// concurrent-reuse guarantee: bound bytes that alias filter-owned memory
// (Comp.EqValue, In's interned keys) must be returned with cap == len, so the
// planner's appends are forced to reallocate instead of scribbling over memory
// shared by concurrent queries.
func TestIndexBounds_FilterOwnedBytesAreCapped(t *testing.T) {
	conds := []string{
		`{"a": 1}`,
		`{"a": {"$eq": "str"}}`,
		`{"a": {"$ne": 1}}`,
		`{"a": {"$gt": 1}}`,
		`{"a": {"$gte": 1}}`,
		`{"a": {"$lt": 1}}`,
		`{"a": {"$lte": 1}}`,
		`{"a": {"$in": [1, 2, 3]}}`,
		`{"a": {"$all": [1, 2]}}`,
	}
	for _, cond := range conds {
		f := MustParseCondition(cond)
		for i, b := range f.IndexBounds("a", nil) {
			if cap(b.Start) != len(b.Start) {
				t.Errorf("%s: bound %d Start len=%d cap=%d — spare capacity over filter-owned bytes", cond, i, len(b.Start), cap(b.Start))
			}
			if cap(b.End) != len(b.End) {
				t.Errorf("%s: bound %d End len=%d cap=%d — spare capacity over filter-owned bytes", cond, i, len(b.End), cap(b.End))
			}
		}
	}
	// The exported constructors must hold the same guarantee.
	for _, f := range []Filter{
		NewComp(CompOpEq, 42),
		NewCompValue(CompOpLte, anyenc.MustParseJson(`"x"`)),
	} {
		for i, b := range f.IndexBounds("a", nil) {
			if cap(b.Start) != len(b.Start) || cap(b.End) != len(b.End) {
				t.Errorf("%s: bound %d has spare capacity over filter-owned bytes", f, i)
			}
		}
	}
}

// TestFilterOkAllocFree pins that Ok is alloc-free for every filter type once
// the per-query DocBuffer is warm (testing.AllocsPerRun does one warm-up call).
func TestFilterOkAllocFree(t *testing.T) {
	cases := []struct {
		name string
		cond string
	}{
		{"eq", `{"a": 2}`},
		{"eq array doc", `{"b": 3}`},
		{"ne", `{"a": {"$ne": 5}}`},
		{"range", `{"a": {"$gt": 1, "$lt": 10}}`},
		{"in", `{"a": {"$in": [1, 2, 3]}}`},
		{"in array doc", `{"b": {"$in": [9, 3]}}`},
		{"nin", `{"a": {"$nin": [7, 8]}}`},
		{"all", `{"b": {"$all": [1, 2]}}`},
		{"or", `{"$or": [{"a": 2}, {"c": "test"}]}`},
		{"nor", `{"$nor": [{"a": 5}]}`},
		{"not", `{"a": {"$not": {"$eq": 5}}}`},
		{"exists", `{"a": {"$exists": true}}`},
		{"type", `{"a": {"$type": "number"}}`},
		{"size", `{"b": {"$size": 3}}`},
		{"regexp", `{"c": {"$regex": "^te"}}`},
		{"regexp array doc", `{"d": {"$regex": "^x"}}`},
		{"nested path", `{"e.f": 1}`},
		{"text", `{"$text": {"$search": "hello"}}`},
		{"zoo", filterZoo},
	}
	docs := []*anyenc.Value{
		anyenc.MustParseJson(`{"a":2,"b":[3,2,1],"c":"test","d":["x","y"],"e":{"f":1}}`),
		anyenc.MustParseJson(`{"a":5,"b":[9],"c":"other","d":["z"],"e":{"f":2}}`),
		anyenc.MustParseJson(`{}`),
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := MustParseCondition(tc.cond)
			buf := &syncpool.DocBuffer{}
			allocs := testing.AllocsPerRun(100, func() {
				for _, d := range docs {
					f.Ok(d, buf)
				}
			})
			if allocs != 0 {
				t.Fatalf("Ok must be alloc-free with a warm DocBuffer, got %v allocs/run", allocs)
			}
		})
	}
}
