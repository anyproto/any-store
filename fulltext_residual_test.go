package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// TestFtsResidual_NoTextSurvives mirrors TestKnnResidual_NoKnnSurvives: the
// residual handed to the FilterIter must never contain the Text node, in any
// $and nesting — detection (findTextInAnd) recurses, so the strip must too.
// A leaked Text fails open (Text.Ok returns true), so the leak is invisible
// in results; this test pins the structural post-condition instead.
func TestFtsResidual_NoTextSurvives(t *testing.T) {
	td := `{"$text":{"$search":"x"}}`
	cases := []string{
		td,
		fmt.Sprintf(`{"$and":[%s,{"a":1}]}`, td),
		fmt.Sprintf(`{"$and":[{"$and":[%s,{"a":1}]},{"b":2}]}`, td),
		fmt.Sprintf(`{"$and":[{"$and":[%s]},{"$and":[{"a":1},{"b":2}]}]}`, td),
		fmt.Sprintf(`{"$and":[{"$and":[{"$and":[%s]}]},{"a":1}]}`, td),
	}
	for _, c := range cases {
		f := query.MustParseCondition(c)
		res := ftsResidualFilter(f)
		require.NotNil(t, res, c)
		assert.False(t, query.ContainsText(res), "text survived: %s", c)
	}

	// The surviving conjuncts keep their semantics: for the nested shape the
	// residual must still require a==1 AND b==2.
	res := ftsResidualFilter(query.MustParseCondition(cases[2]))
	buf := &syncpool.DocBuffer{}
	assert.True(t, res.Ok(anyenc.MustParseJson(`{"a":1,"b":2}`), buf))
	assert.False(t, res.Ok(anyenc.MustParseJson(`{"a":1,"b":3}`), buf))
	assert.False(t, res.Ok(anyenc.MustParseJson(`{"a":0,"b":2}`), buf))

	// A text-only query leaves the match-all residual.
	assert.Equal(t, query.All{}, ftsResidualFilter(query.MustParseCondition(td)))

	// Pointer forms: *And nodes and *Text leaves must be stripped too.
	ptr := &query.And{&query.Text{}, query.MustParseCondition(`{"a":1}`)}
	res = ftsResidualFilter(query.And{ptr, query.MustParseCondition(`{"b":2}`)})
	require.NotNil(t, res)
	assert.False(t, query.ContainsText(res))
	assert.True(t, res.Ok(anyenc.MustParseJson(`{"a":1,"b":2}`), buf))
	assert.False(t, res.Ok(anyenc.MustParseJson(`{"a":2,"b":2}`), buf))
}

// TestFtsPipeline_NestedAndResidual drives the leak shape end-to-end: the
// nested conjuncts must actually FILTER the BM25 candidates (guards against
// over-stripping, the opposite failure of the leak).
func TestFtsPipeline_NestedAndResidual(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	cond := `{"$and":[{"$and":[{"$text":{"$search":"london"}},{"status":"open"}]},{"year":{"$lt":1940}}]}`

	// a: open/1920 matches; b: closed; c: 1950; d: no "london".
	ids, _ := collectIter(t, coll.Find(cond))
	assert.Equal(t, []string{"a"}, ids)

	cnt, err := coll.Find(cond).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
}

// TestFtsPlacementRejections pins the placement errors around the residual
// path: $text under $or/$nor/$not and duplicate $text fail with the named
// errors on every entry point, they don't silently degrade.
func TestFtsPlacementRejections(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	for _, c := range []string{
		`{"$or":[{"$text":{"$search":"london"}},{"status":"open"}]}`,
		`{"$nor":[{"$text":{"$search":"london"}}]}`,
	} {
		_, err := coll.Find(c).Count(ctx)
		assert.ErrorIs(t, err, errFtsBadPlacement, c)
	}
	_, err := coll.Find(`{"$and":[{"$text":{"$search":"london"}},{"$and":[{"$text":{"$search":"fog"}}]}]}`).Count(ctx)
	assert.ErrorIs(t, err, errFtsMultiple)
}
