package query

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-store/anyenc"
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
