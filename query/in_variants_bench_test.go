package query

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"unsafe"

	"github.com/anyproto/any-store/v2/anyenc"
)

// Benchmarks comparing three In-set representations (PERF-1 case 3 design
// probe; variant 2 is the live implementation):
//
//  1. mapString — the original: one heap string per key, map set, IndexBounds
//     sorts per call.
//  2. arenaMap — arena-interned unsafe.String keys + map set + pre-sorted
//     bounds (current NewInValue).
//  3. arenaSorted — no map at all: sorted interned keys only; Ok is a binary
//     search. Would be an API break (drops the public Values map).

// --- variant 1: original ---

type inV1 struct{ values map[string]struct{} }

func newInV1(values ...*anyenc.Value) inV1 {
	m := make(map[string]struct{}, len(values))
	for _, v := range values {
		m[string(v.MarshalTo(nil))] = struct{}{}
	}
	return inV1{values: m}
}

func (e inV1) ok(probe []byte) bool {
	_, ok := e.values[string(probe)]
	return ok
}

func (e inV1) indexBounds() Bounds {
	result := make(Bounds, 0, len(e.values))
	for val := range e.values {
		b := []byte(val)
		result = append(result, Bound{Start: b, End: b, StartInclude: true, EndInclude: true})
	}
	return result.SortAndMerge()
}

// --- variant 3: sorted arena, binary search, no map ---

type inV3 struct{ sorted []string }

func newInV3(values ...*anyenc.Value) inV3 {
	var arena []byte
	sorted := make([]string, 0, len(values))
	for _, v := range values {
		start := len(arena)
		arena = v.MarshalTo(arena)
		span := arena[start:]
		sorted = append(sorted, unsafe.String(unsafe.SliceData(span), len(span)))
	}
	slices.Sort(sorted)
	return inV3{sorted: slices.Compact(sorted)}
}

func (e inV3) ok(probe []byte) bool {
	_, found := slices.BinarySearchFunc(e.sorted, probe, func(s string, b []byte) int {
		return bytes.Compare(unsafe.Slice(unsafe.StringData(s), len(s)), b)
	})
	return found
}

func (e inV3) indexBounds() Bounds {
	result := make(Bounds, 0, len(e.sorted))
	for _, val := range e.sorted {
		b := unsafe.Slice(unsafe.StringData(val), len(val))
		result = append(result, Bound{Start: b, End: b, StartInclude: true, EndInclude: true})
	}
	return result
}

// --- harness ---

func makeInValues(n int) []*anyenc.Value {
	a := &anyenc.Arena{}
	vals := make([]*anyenc.Value, n)
	for i := range n {
		vals[i] = a.NewNumberInt(i * 3) // members: multiples of 3
	}
	return vals
}

// probes: half hits (multiples of 3), half misses
func makeProbes(n int) [][]byte {
	a := &anyenc.Arena{}
	probes := make([][]byte, 0, 2*n)
	for i := range n {
		probes = append(probes, a.NewNumberInt(i*3).MarshalTo(nil))   // hit
		probes = append(probes, a.NewNumberInt(i*3+1).MarshalTo(nil)) // miss
	}
	return probes
}

func BenchmarkInVariants(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 5000} {
		vals := makeInValues(n)
		probes := makeProbes(n)

		b.Run(fmt.Sprintf("Construct/n%d/v1_mapString", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = newInV1(vals...)
			}
		})
		b.Run(fmt.Sprintf("Construct/n%d/v2_arenaMap", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = NewInValue(vals...)
			}
		})
		b.Run(fmt.Sprintf("Construct/n%d/v3_arenaSorted", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = newInV3(vals...)
			}
		})

		v1 := newInV1(vals...)
		v2 := NewInValue(vals...)
		v3 := newInV3(vals...)
		var sink bool

		b.Run(fmt.Sprintf("Ok/n%d/v1_mapString", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, p := range probes {
					sink = v1.ok(p)
				}
			}
		})
		b.Run(fmt.Sprintf("Ok/n%d/v2_arenaMap", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, p := range probes {
					_, sink = v2.Values[string(p)]
				}
			}
		})
		b.Run(fmt.Sprintf("Ok/n%d/v3_arenaSorted", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, p := range probes {
					sink = v3.ok(p)
				}
			}
		})

		b.Run(fmt.Sprintf("IndexBounds/n%d/v1_mapString", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = v1.indexBounds()
			}
		})
		b.Run(fmt.Sprintf("IndexBounds/n%d/v2_arenaMap", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = v2.IndexBounds("id", nil)
			}
		})
		b.Run(fmt.Sprintf("IndexBounds/n%d/v3_arenaSorted", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = v3.indexBounds()
			}
		})
		_ = sink
	}
}

// Sanity: the three variants agree on membership.
func TestInVariantsAgree(t *testing.T) {
	vals := makeInValues(200)
	v1, v2, v3 := newInV1(vals...), NewInValue(vals...), newInV3(vals...)
	for _, p := range makeProbes(200) {
		_, m2 := v2.Values[string(p)]
		if v1.ok(p) != m2 || m2 != v3.ok(p) {
			t.Fatalf("variant disagreement on probe %x", p)
		}
	}
	b2 := v2.IndexBounds("x", nil)
	b3 := v3.indexBounds()
	b1 := v1.indexBounds()
	if len(b1) != len(b2) || len(b2) != len(b3) {
		t.Fatalf("bounds length mismatch: %d %d %d", len(b1), len(b2), len(b3))
	}
	for i := range b2 {
		if !bytes.Equal(b1[i].Start, b2[i].Start) || !bytes.Equal(b2[i].Start, b3[i].Start) {
			t.Fatalf("bounds order mismatch at %d", i)
		}
	}
}
