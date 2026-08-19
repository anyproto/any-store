package anystore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// The tests below differentially verify the two sort fast paths against
// equivalent queries that cannot take them:
//   - a filterless full scan + sort hands decoded bytes to SortIter
//     (Plan.DocRaw + RawSort) instead of parsing every row;
//   - an ordered index scan whose IndexFilterIter equality checks fully cover
//     the filter elides the post-fetch FilterIter.
// The shadow query carries an extra $exists conjunct on the pk, which every
// document satisfies but no index covers — forcing the parsed/filtered path
// while selecting the same documents.

// pad pushes every document above the 256-byte compression threshold so the
// raw path exercises s2-decoded bytes, matching production-shaped documents.
var sortPad = strings.Repeat("x", 300)

func sortFastpathFixture(t *testing.T, arrayVal bool) (*fixture, Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	// 7i mod 101 permutes 0..100: unique scalar sort keys, insertion order
	// decorrelated from key order.
	var docs []*anyenc.Value
	for i := 0; i < 101; i++ {
		v := (7 * i) % 101
		var doc string
		if arrayVal {
			doc = fmt.Sprintf(`{"id":%d,"val":[%d,%d],"pad":"%s"}`, i, v, (v+13)%101, sortPad)
		} else {
			doc = fmt.Sprintf(`{"id":%d,"val":%d,"pad":"%s"}`, i, v, sortPad)
		}
		docs = append(docs, anyenc.MustParseJson(doc))
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	return fx, coll
}

func collectIds(t *testing.T, q Query) []int {
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var ids []int
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		ids = append(ids, doc.Value().GetInt("id"))
	}
	require.NoError(t, iter.Err())
	return ids
}

func TestSortRawScan_MatchesParsedPath(t *testing.T) {
	for _, arrayVal := range []bool{false, true} {
		name := "scalar"
		if arrayVal {
			name = "array" // array sort field: raw path must fall back to parse
		}
		t.Run(name, func(t *testing.T) {
			_, coll := sortFastpathFixture(t, arrayVal)

			cases := []struct {
				sort          string
				limit, offset uint
			}{
				{"val", 10, 0},
				{"-val", 10, 0},
				{"val", 10, 25},
				{"val", 0, 0},  // full sort (TopK disabled)
				{"-val", 0, 0}, // full sort desc
			}
			for _, tc := range cases {
				t.Run(fmt.Sprintf("sort=%s limit=%d offset=%d", tc.sort, tc.limit, tc.offset), func(t *testing.T) {
					raw := coll.Find(nil).Sort(tc.sort)
					shadow := coll.Find(`{"id":{"$exists":true}}`).Sort(tc.sort)
					if tc.limit > 0 {
						raw, shadow = raw.Limit(tc.limit), shadow.Limit(tc.limit)
					}
					if tc.offset > 0 {
						raw, shadow = raw.Offset(tc.offset), shadow.Offset(tc.offset)
					}
					got := collectIds(t, raw)
					want := collectIds(t, shadow)
					require.NotEmpty(t, want)
					assert.Equal(t, want, got)
				})
			}

			// The fast path must actually be planned: filterless scan + sort.
			ex, err := coll.Find(nil).Sort("val").Limit(10).Explain(ctx)
			require.NoError(t, err)
			assert.Equal(t, "FullScan -> TopK(10) -> Limit(10)", ex.Sql)
		})
	}
}
