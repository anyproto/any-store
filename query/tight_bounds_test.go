package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tb runs TightIndexBounds over a parsed condition.
func tb(t *testing.T, cond, field string) (Bounds, bool) {
	t.Helper()
	f, err := ParseCondition(cond)
	require.NoError(t, err)
	return TightIndexBounds(f, field)
}

func TestTightIndexBounds_TwoSidedRange(t *testing.T) {
	// The headline two-sided-range shape: both ends must survive.
	bs, empty := tb(t, `{"a":{"$gt":10,"$lt":20}}`, "a")
	require.False(t, empty)
	require.Len(t, bs, 1)
	assert.Equal(t, []byte(newBoundKey(10)), []byte(bs[0].Start))
	assert.Equal(t, []byte(newBoundKey(20)), []byte(bs[0].End))
	assert.False(t, bs[0].StartInclude)
	assert.False(t, bs[0].EndInclude)

	// The $and-array spelling produces the same interval.
	bs2, empty2 := tb(t, `{"$and":[{"a":{"$gt":10}},{"a":{"$lt":20}}]}`, "a")
	require.False(t, empty2)
	require.Len(t, bs2, 1)
	assert.Equal(t, bs.String(), bs2.String())

	// ...and so does a nested $and.
	bs3, empty3 := tb(t, `{"a":{"$gt":10},"$and":[{"a":{"$lt":20}}]}`, "a")
	require.False(t, empty3)
	assert.Equal(t, bs.String(), bs3.String())
}

func TestTightIndexBounds_InIntersectsRange(t *testing.T) {
	// $in point set filtered by a range: {5,10} survive from {1,5,10}.
	bs, empty := tb(t, `{"a":{"$in":[1,5,10],"$gte":5}}`, "a")
	require.False(t, empty)
	require.Len(t, bs, 2)
	assert.Equal(t, []byte(newBoundKey(5)), []byte(bs[0].Start))
	assert.Equal(t, []byte(newBoundKey(10)), []byte(bs[1].Start))
}

func TestTightIndexBounds_NeSplitsInterval(t *testing.T) {
	// $ne carves a hole: (1,9) minus {5} = (1,5) U (5,9).
	bs, empty := tb(t, `{"a":{"$gt":1,"$lt":9,"$ne":5}}`, "a")
	require.False(t, empty)
	require.Len(t, bs, 2)
	assert.Equal(t, []byte(newBoundKey(1)), []byte(bs[0].Start))
	assert.Equal(t, []byte(newBoundKey(5)), []byte(bs[0].End))
	assert.False(t, bs[0].EndInclude)
	assert.Equal(t, []byte(newBoundKey(5)), []byte(bs[1].Start))
	assert.False(t, bs[1].StartInclude)
	assert.Equal(t, []byte(newBoundKey(9)), []byte(bs[1].End))
}

func TestTightIndexBounds_ContradictionIsEmptyNotUnsat(t *testing.T) {
	// Provably-empty VALUE set — but per array semantics the FILTER can still
	// match ({a:[6,1]}: 6>5 via one element, 1<3 via another), so empty must
	// be an explicit flag with nil bounds, never a len==0 "unconstrained".
	bs, empty := tb(t, `{"a":{"$gt":5,"$lt":3}}`, "a")
	assert.True(t, empty)
	assert.Nil(t, bs)

	bs, empty = tb(t, `{"a":{"$in":[1,2],"$gte":5}}`, "a")
	assert.True(t, empty)
	assert.Nil(t, bs)

	// The wide channel keeps its over-approximation for the same filters.
	f := MustParseCondition(`{"a":{"$gt":5,"$lt":3}}`)
	assert.NotEmpty(t, f.IndexBounds("a", nil),
		"wide IndexBounds must stay a sound over-approximation")
}

func TestTightIndexBounds_UnrelatedFieldAndOtherNodes(t *testing.T) {
	// Field not referenced: no contribution, NOT empty.
	bs, empty := tb(t, `{"b":{"$gt":5,"$lt":3}}`, "a")
	assert.False(t, empty)
	assert.Empty(t, bs)

	// Non-contributing conjuncts ($exists adds no bounds) are unconstrained —
	// they must not narrow, and must not block intersection of the others.
	bs, empty = tb(t, `{"a":{"$gt":10,"$lt":20,"$exists":true}}`, "a")
	require.False(t, empty)
	require.Len(t, bs, 1)
	assert.Equal(t, []byte(newBoundKey(10)), []byte(bs[0].Start))
	assert.Equal(t, []byte(newBoundKey(20)), []byte(bs[0].End))
}

func TestTightIndexBounds_OrDelegatesWide(t *testing.T) {
	// Or nodes keep the wide union — no tightening inside branches, and a
	// contradictory branch cannot leak empty=true.
	for _, cond := range []string{
		`{"$or":[{"a":{"$gt":1,"$lt":5}},{"a":{"$gt":10,"$lt":20}}]}`,
		`{"$or":[{"a":{"$gt":5,"$lt":3}},{"a":1}]}`,
	} {
		f := MustParseCondition(cond)
		tight, empty := TightIndexBounds(f, "a")
		assert.False(t, empty, cond)
		wide := f.IndexBounds("a", nil)
		assert.Equal(t, wide.String(), tight.String(),
			"Or must delegate to the wide channel: %s", cond)
	}
}

func TestTightIndexBounds_SubsetOfWide(t *testing.T) {
	// For every non-empty result, tight ⊆ wide must hold (wide is the sound
	// over-approximation; tight only ever narrows).
	for _, cond := range []string{
		`{"a":{"$gt":10,"$lt":20}}`,
		`{"a":{"$in":[1,5,10],"$gte":5}}`,
		`{"a":{"$gt":1,"$lt":9,"$ne":5}}`,
		`{"a":{"$gte":5,"$lte":5}}`,
		`{"a":5}`,
		`{"a":{"$in":[1,2,3]}}`,
		`{"a":{"$gt":3}}`,
	} {
		f := MustParseCondition(cond)
		tight, empty := TightIndexBounds(f, "a")
		require.False(t, empty, cond)
		wide := f.IndexBounds("a", nil)
		for _, tbound := range tight {
			// Every tight interval's endpoints must lie within some wide
			// interval — probe via representative keys.
			if len(tbound.Start) > 0 && tbound.StartInclude {
				assert.True(t, wide.Contains(tbound.Start), "tight ⊄ wide for %s", cond)
			}
			if len(tbound.End) > 0 && tbound.EndInclude {
				assert.True(t, wide.Contains(tbound.End), "tight ⊄ wide for %s", cond)
			}
		}
	}
}

func TestTightIndexBounds_EqCollapsesToPoint(t *testing.T) {
	// {$gte:5,$lte:5} intersects to the inclusive point [5,5].
	bs, empty := tb(t, `{"a":{"$gte":5,"$lte":5}}`, "a")
	require.False(t, empty)
	require.Len(t, bs, 1)
	assert.Equal(t, []byte(bs[0].Start), []byte(bs[0].End))
	assert.True(t, bs[0].StartInclude)
	assert.True(t, bs[0].EndInclude)
}

func TestTightIndexBounds_CapClipped(t *testing.T) {
	// Endpoints alias filter-owned memory and must stay cap==len so later
	// planner appends reallocate (docs/query-filter-contract.md item 3).
	for _, cond := range []string{
		`{"a":{"$gt":10,"$lt":20}}`,
		`{"a":{"$in":[1,5,10],"$gte":5}}`,
		`{"a":{"$gt":1,"$lt":9,"$ne":5}}`,
	} {
		bs, empty := tb(t, cond, "a")
		require.False(t, empty)
		for i, b := range bs {
			if len(b.Start) > 0 {
				assert.Equal(t, len(b.Start), cap(b.Start), "%s bound %d Start", cond, i)
			}
			if len(b.End) > 0 {
				assert.Equal(t, len(b.End), cap(b.End), "%s bound %d End", cond, i)
			}
		}
	}
}

// cb collects every set ConjunctBounds yields for a parsed condition.
func cb(t *testing.T, cond, field string) (sets []Bounds) {
	t.Helper()
	f, err := ParseCondition(cond)
	require.NoError(t, err)
	ConjunctBounds(f, field, func(bs Bounds) bool {
		sets = append(sets, bs)
		return true
	})
	return sets
}

func TestConjunctBounds_EnumeratesEachConjunct(t *testing.T) {
	// $all desugars to And(Eq, Eq): one point set per value, in order.
	sets := cb(t, `{"tags":{"$all":["common","rare"]}}`, "tags")
	require.Len(t, sets, 2)
	assert.Equal(t, []byte(newBoundKey("common")), []byte(sets[0][0].Start))
	assert.Equal(t, []byte(newBoundKey("rare")), []byte(sets[1][0].Start))

	// The $and-array spelling, a nested $and and an inline multi-op all
	// flatten to the same enumeration.
	for _, cond := range []string{
		`{"$and":[{"tags":"common"},{"tags":"rare"}]}`,
		`{"tags":"common","$and":[{"$and":[{"tags":"rare"}]}]}`,
		`{"tags":{"$eq":"common","$in":["rare"]}}`,
	} {
		got := cb(t, cond, "tags")
		require.Len(t, got, 2, cond)
		assert.Equal(t, sets[0].String(), got[0].String(), cond)
		assert.Equal(t, sets[1].String(), got[1].String(), cond)
	}

	// A two-sided range yields each side on its own.
	sets = cb(t, `{"a":{"$gt":10,"$lt":20}}`, "a")
	require.Len(t, sets, 2)
	assert.Equal(t, []byte(newBoundKey(10)), []byte(sets[0][0].Start))
	assert.True(t, sets[0][0].EndIsTypeEdge(), "the $gt side ends at its type bracket")
	assert.Equal(t, []byte(newBoundKey(20)), []byte(sets[1][0].End))
}

func TestConjunctBounds_FirstSetIsTheWideChannel(t *testing.T) {
	// Whatever is enumerated first is exactly And.IndexBounds' pick.
	for _, cond := range []string{
		`{"a":{"$all":["common","rare"]}}`,
		`{"a":{"$in":[1,5,10],"$gte":5}}`,
		`{"a":{"$gt":1,"$lt":9,"$ne":5}}`,
		`{"a":{"$exists":true,"$gt":1,"$lt":9}}`,
		`{"a":5}`,
		`{"$or":[{"a":1},{"a":2}],"a":{"$gt":0}}`,
	} {
		sets := cb(t, cond, "a")
		require.NotEmpty(t, sets, cond)
		assert.Equal(t, MustParseCondition(cond).IndexBounds("a", nil).String(), sets[0].String(), cond)
	}
}

func TestConjunctBounds_LeavesAndRouting(t *testing.T) {
	// Non-contributing conjuncts ($exists, a prefix-less $regex) are skipped,
	// not yielded as empty sets.
	sets := cb(t, `{"a":{"$exists":true,"$regex":"x$","$gt":1}}`, "a")
	require.Len(t, sets, 1)
	assert.Equal(t, []byte(newBoundKey(1)), []byte(sets[0][0].Start))

	// Or is a leaf: its union is yielded once, never its branches.
	sets = cb(t, `{"$or":[{"a":1},{"a":2}],"a":{"$gt":0}}`, "a")
	require.Len(t, sets, 2)
	assert.Len(t, sets[0], 2, "the $or union")
	assert.Len(t, sets[1], 1)

	// A value-form $elemMatch intersects its own conjuncts (one element) and
	// is a single leaf.
	sets = cb(t, `{"a":{"$elemMatch":{"$gt":1,"$lt":5}}}`, "a")
	require.Len(t, sets, 1)
	assert.Equal(t, []byte(newBoundKey(1)), []byte(sets[0][0].Start))
	assert.Equal(t, []byte(newBoundKey(5)), []byte(sets[0][0].End))

	// An object-form $elemMatch is re-keyed under its sub-field, alongside a
	// dotted-path conjunct on the same field.
	sets = cb(t, `{"a":{"$elemMatch":{"b":1}},"a.b":2}`, "a.b")
	require.Len(t, sets, 2)
	assert.Equal(t, []byte(newBoundKey(1)), []byte(sets[0][0].Start))
	assert.Equal(t, []byte(newBoundKey(2)), []byte(sets[1][0].Start))

	// Another field's conjuncts yield nothing.
	assert.Empty(t, cb(t, `{"b":{"$all":[1,2]}}`, "a"))

	// yield returning false stops the walk.
	f := MustParseCondition(`{"tags":{"$all":["x","y","z"]}}`)
	n := 0
	ConjunctBounds(f, "tags", func(Bounds) bool { n++; return false })
	assert.Equal(t, 1, n)
}

func TestConjunctBounds_CapClipped(t *testing.T) {
	// Every yielded bound aliases filter-owned memory clipped to cap == len,
	// so a planner append can never write into the filter.
	for _, cond := range []string{
		`{"a":{"$all":["common","rare"]}}`,
		`{"a":{"$gt":10,"$lt":20}}`,
		`{"a":{"$in":[1,5,10],"$gte":5}}`,
	} {
		sets := cb(t, cond, "a")
		require.NotEmpty(t, sets, cond)
		for _, bs := range sets {
			for _, b := range bs {
				assert.Equal(t, len(b.Start), cap(b.Start), cond)
				assert.Equal(t, len(b.End), cap(b.End), cond)
			}
		}
	}
}

func TestKey_PathIs(t *testing.T) {
	k := Key{Path: []string{"a", "b"}}
	assert.True(t, k.PathIs("a.b"))
	assert.False(t, k.PathIs("a"))
	assert.False(t, k.PathIs("a.bc"))
	assert.False(t, k.PathIs("a.b.c"))
	assert.False(t, k.PathIs("ab"))
	assert.False(t, k.PathIs(""))
	assert.True(t, Key{Path: []string{"a"}}.PathIs("a"))
	assert.False(t, Key{Path: []string{"a"}}.PathIs("a."))
	assert.Zero(t, testing.AllocsPerRun(10, func() { k.PathIs("a.b") }))
}

func TestKey_ElemMatchSubField_NoJoin(t *testing.T) {
	f := MustParseCondition(`{"a.x":{"$elemMatch":{"b":1}},"c.d":2}`)
	k := f.(And)[0].(Key)
	cond, sub, ok := k.ElemMatchSubField("a.x.b")
	require.True(t, ok)
	assert.Equal(t, "b", sub)
	assert.Equal(t, `{"b": {"$eq": 1}}`, cond.String())
	_, _, ok = k.ElemMatchSubField("a.xb")
	assert.False(t, ok)
	_, _, ok = k.ElemMatchSubField("a.x")
	assert.False(t, ok)
	_, _, ok = k.ElemMatchSubField("c.d.e")
	assert.False(t, ok)
	// Asking for an unrelated dotted field — what IndexBounds does for every
	// index field — allocates nothing.
	assert.Zero(t, testing.AllocsPerRun(10, func() { k.ElemMatchSubField("c.d.e") }))
	assert.Zero(t, testing.AllocsPerRun(10, func() { k.ElemMatchSubField("a.x.b") }))
}
