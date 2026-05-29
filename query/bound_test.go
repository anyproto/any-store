package query

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-store/v2/anyenc"
)

func newBoundKey(v any) (k anyenc.Tuple) {
	return anyenc.AppendAnyValue(nil, v)
}

type boundTestCase struct {
	unmerged Bounds
	expected string
}

var boundTestData = []boundTestCase{
	{
		unmerged: Bounds{
			{Start: nil, End: newBoundKey(10)},
			{Start: newBoundKey(5), End: newBoundKey(15)},
		},
		expected: `Bounds{[-inf,'15')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(2)},
			{Start: newBoundKey(2), End: newBoundKey(3)},
		},
		expected: `Bounds{('1','2'),('2','3')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(2), End: newBoundKey(3)},
			{Start: newBoundKey(1), End: newBoundKey(2)},
		},
		expected: `Bounds{('1','2'),('2','3')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(2), EndInclude: true},
			{Start: newBoundKey(2), End: newBoundKey(3)},
		},
		expected: `Bounds{('1','3')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(2), End: newBoundKey(3), StartInclude: true},
			{Start: newBoundKey(1), End: newBoundKey(2)},
		},
		expected: `Bounds{('1','3')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(10)},
			{Start: newBoundKey(1), End: newBoundKey(2)},
		},
		expected: `Bounds{('1','10')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(10)},
			{Start: newBoundKey(8), End: nil},
		},
		expected: `Bounds{('1',inf]}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(10), StartInclude: true},
			{Start: newBoundKey(8), End: nil},
		},
		expected: `Bounds{['1',inf]}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(10), StartInclude: true},
			{Start: newBoundKey(8), End: newBoundKey(11), EndInclude: true},
		},
		expected: `Bounds{['1','11']}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(2), StartInclude: true},
			{Start: newBoundKey(4), End: newBoundKey(5), EndInclude: true},
		},
		expected: `Bounds{['1','2'),('4','5']}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: nil},
			{Start: nil, End: newBoundKey(2)},
		},
		expected: `Bounds{[-inf,inf]}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(3)},
			{Start: newBoundKey(5), End: newBoundKey(7)},
			{Start: newBoundKey(2), End: newBoundKey(6)},
		},
		expected: `Bounds{('1','7')}`,
	},
}

func TestBounds_Append(t *testing.T) {
	for _, tc := range boundTestData {
		t.Run(tc.expected, func(t *testing.T) {
			var bs Bounds
			for _, b := range tc.unmerged {
				bs = bs.Append(b)
			}
			assert.Equal(t, tc.expected, bs.String())
		})
	}
}

func TestBounds_SortAndMerge(t *testing.T) {
	for _, tc := range boundTestData {
		t.Run(tc.expected, func(t *testing.T) {
			bs := make(Bounds, len(tc.unmerged))
			copy(bs, tc.unmerged)
			bs = bs.SortAndMerge()
			assert.Equal(t, tc.expected, bs.String())
		})
	}
}

func TestBounds_Append_NoAliasing(t *testing.T) {
	// Verify Append doesn't corrupt prior data through shared backing array
	original := Bounds{
		{Start: newBoundKey(1), End: newBoundKey(2), StartInclude: true, EndInclude: true},
		{Start: newBoundKey(5), End: newBoundKey(6), StartInclude: true, EndInclude: true},
	}
	// Save a copy of original[0] for later comparison
	origStart := make([]byte, len(original[0].Start))
	copy(origStart, original[0].Start)

	// Append a non-overlapping bound
	result := original.Append(Bound{
		Start: newBoundKey(10), End: newBoundKey(11),
		StartInclude: true, EndInclude: true,
	})

	assert.Len(t, result, 3)
	// Original should be untouched
	assert.Equal(t, origStart, []byte(original[0].Start))
	assert.Len(t, original, 2)
}

func BenchmarkBounds_Append(b *testing.B) {
	benchAppend := func(b *testing.B, n int) {
		points := make(Bounds, n)
		for i := range points {
			k := newBoundKey(i * 2) // non-overlapping
			points[i] = Bound{Start: k, End: k, StartInclude: true, EndInclude: true}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var bs Bounds
			for _, p := range points {
				bs = bs.Append(p)
			}
		}
	}
	b.Run("10_points", func(b *testing.B) { benchAppend(b, 10) })
	b.Run("100_points", func(b *testing.B) { benchAppend(b, 100) })
	b.Run("500_points", func(b *testing.B) { benchAppend(b, 500) })
}

func BenchmarkBounds_SortAndMerge(b *testing.B) {
	benchSAM := func(b *testing.B, n int) {
		points := make(Bounds, n)
		for i := range points {
			k := newBoundKey(i * 2) // non-overlapping
			points[i] = Bound{Start: k, End: k, StartInclude: true, EndInclude: true}
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bs := make(Bounds, n)
			copy(bs, points)
			bs.SortAndMerge()
		}
	}
	b.Run("10_points", func(b *testing.B) { benchSAM(b, 10) })
	b.Run("100_points", func(b *testing.B) { benchSAM(b, 100) })
	b.Run("500_points", func(b *testing.B) { benchSAM(b, 500) })
}

func TestBounds_Contains(t *testing.T) {
	mk := func(start, end string, si, ei bool) Bound {
		return Bound{Start: []byte(start), End: []byte(end), StartInclude: si, EndInclude: ei}
	}

	t.Run("point bound", func(t *testing.T) {
		bs := Bounds{mk("a", "a", true, true)}
		assert.True(t, bs.Contains([]byte("a")))
		assert.False(t, bs.Contains([]byte("b")))
	})

	t.Run("disjoint ranges", func(t *testing.T) {
		bs := Bounds{mk("a", "a", true, true), mk("c", "c", true, true)}
		assert.True(t, bs.Contains([]byte("a")))
		assert.True(t, bs.Contains([]byte("c")))
		assert.False(t, bs.Contains([]byte("b")))
	})

	t.Run("exclusive open range", func(t *testing.T) {
		bs := Bounds{mk("a", "c", false, false)}
		assert.False(t, bs.Contains([]byte("a")))
		assert.True(t, bs.Contains([]byte("b")))
		assert.False(t, bs.Contains([]byte("c")))
	})

	t.Run("unbounded end", func(t *testing.T) {
		bs := Bounds{{Start: []byte("a"), StartInclude: true}}
		assert.True(t, bs.Contains([]byte("z")))
		assert.False(t, bs.Contains([]byte{}))
	})

	t.Run("unbounded start", func(t *testing.T) {
		bs := Bounds{{End: []byte("m"), EndInclude: true}}
		assert.True(t, bs.Contains([]byte("a")))
		assert.True(t, bs.Contains([]byte("m")))
		assert.False(t, bs.Contains([]byte("z")))
	})
}

func TestBounds_Intersect(t *testing.T) {
	// pt builds an inclusive point bound [v,v].
	pt := func(v any) Bound {
		k := newBoundKey(v)
		return Bound{Start: k, End: k, StartInclude: true, EndInclude: true}
	}
	// rng builds a range bound; nil lo/hi means -inf/+inf.
	rng := func(lo, hi any, si, ei bool) Bound {
		var s, e anyenc.Tuple
		if lo != nil {
			s = newBoundKey(lo)
		}
		if hi != nil {
			e = newBoundKey(hi)
		}
		return Bound{Start: s, End: e, StartInclude: si, EndInclude: ei}
	}

	assertBounds := func(t *testing.T, want, got Bounds) {
		t.Helper()
		if !assert.Equal(t, len(want), len(got),
			"bound count mismatch: got %s, want %s", got.String(), want.String()) {
			return
		}
		for i := range want {
			assert.Equalf(t, []byte(want[i].Start), []byte(got[i].Start),
				"bound %d Start (got %s want %s)", i, got.String(), want.String())
			assert.Equalf(t, []byte(want[i].End), []byte(got[i].End),
				"bound %d End (got %s want %s)", i, got.String(), want.String())
			assert.Equalf(t, want[i].StartInclude, got[i].StartInclude,
				"bound %d StartInclude (got %s want %s)", i, got.String(), want.String())
			assert.Equalf(t, want[i].EndInclude, got[i].EndInclude,
				"bound %d EndInclude (got %s want %s)", i, got.String(), want.String())
		}
	}

	cases := []struct {
		name string
		a, b Bounds
		want Bounds
	}{
		{"point∩point equal → point", Bounds{pt(5)}, Bounds{pt(5)}, Bounds{pt(5)}},
		{"point∩point disjoint → empty", Bounds{pt(5)}, Bounds{pt(7)}, Bounds{}},
		{"point∩range inside → point", Bounds{pt(5)}, Bounds{rng(1, 10, true, true)}, Bounds{pt(5)}},
		{"point∩range outside → empty", Bounds{pt(5)}, Bounds{rng(10, 20, true, true)}, Bounds{}},
		{"range∩range overlap → intersection", Bounds{rng(1, 10, true, true)}, Bounds{rng(5, 15, true, true)}, Bounds{rng(5, 10, true, true)}},
		{"range∩range disjoint → empty", Bounds{rng(1, 5, true, true)}, Bounds{rng(10, 15, true, true)}, Bounds{}},
		{"multi-bound cross-product", Bounds{pt(1), pt(5), pt(10)}, Bounds{rng(5, nil, true, false)}, Bounds{pt(5), pt(10)}},
		{"open-ended -inf/+inf", Bounds{rng(nil, 10, false, true)}, Bounds{rng(5, nil, true, false)}, Bounds{rng(5, 10, true, true)}},
		{"exclusivity equal endpoints, both include → point", Bounds{rng(1, 5, true, true)}, Bounds{rng(5, 10, true, true)}, Bounds{pt(5)}},
		{"exclusivity equal endpoints, one excludes → empty", Bounds{rng(1, 5, true, false)}, Bounds{rng(5, 10, true, true)}, Bounds{}},
		{"empty left → empty", Bounds{}, Bounds{pt(5)}, Bounds{}},
		{"empty right → empty", Bounds{pt(5)}, Bounds{}, Bounds{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertBounds(t, tc.want, tc.a.Intersect(tc.b))
			// Intersection is commutative.
			assertBounds(t, tc.want, tc.b.Intersect(tc.a))
		})
	}
}

// --- Coverage tests from bound_coverage_test.go ---

// TestBounds_Contains_Coverage_EmptyBounds asserts that an empty Bounds{}
// reports Contains=false for any value. Covers query/bound.go:192-198.
// The loop body never executes, so the method returns false.
func TestBounds_Contains_Coverage_EmptyBounds(t *testing.T) {
	var empty Bounds
	assert.False(t, empty.Contains([]byte("a")),
		"empty Bounds{} must not contain any value")
	assert.False(t, empty.Contains(nil),
		"empty Bounds{} must not contain nil")
	assert.False(t, empty.Contains([]byte{}),
		"empty Bounds{} must not contain empty byte slice")

	// Explicit zero-length literal behaves identically.
	assert.False(t, Bounds{}.Contains([]byte("a")),
		"Bounds{} literal must not contain any value")
}
