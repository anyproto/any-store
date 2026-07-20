package anystore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestIndexScanCoveringFilterElision(t *testing.T) {
	newColl := func(t *testing.T, fx *fixture, arrayB bool) Collection {
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
		var docs []*anyenc.Value
		for i := 0; i < 500; i++ {
			var doc string
			if arrayB {
				doc = fmt.Sprintf(`{"id":%d,"a":%d,"b":[%d,%d],"pad":"%s"}`, i, (7*i)%500, i%10, (i+3)%10, sortPad)
			} else {
				doc = fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"pad":"%s"}`, i, (7*i)%500, i%10, sortPad)
			}
			docs = append(docs, anyenc.MustParseJson(doc))
		}
		require.NoError(t, coll.Insert(ctx, docs...))
		return coll
	}

	for _, arrayB := range []bool{false, true} {
		name := "scalar-b"
		if arrayB {
			name = "multikey-b" // per-entry equality + DocDedup must stand in for the residual
		}
		t.Run(name, func(t *testing.T) {
			fx := newFixture(t)
			coll := newColl(t, fx, arrayB)

			elided := coll.Find(`{"b":5}`).Sort("a").Limit(100)
			shadow := coll.Find(`{"$and":[{"b":5},{"id":{"$exists":true}}]}`).Sort("a").Limit(100)
			got := collectIds(t, elided)
			want := collectIds(t, shadow)
			require.NotEmpty(t, want)
			assert.Equal(t, want, got)

			// No duplicate ids (multikey entries must still dedup without the
			// residual filter).
			seen := map[int]bool{}
			for _, id := range got {
				assert.False(t, seen[id], "duplicate id %d", id)
				seen[id] = true
			}

			// The covered scalar plan must have IndexFilter and no residual
			// Filter. A multikey (a,b) index gets no sort-service scan
			// candidate at all (array order is plan-dependent), so the elided
			// chain is unreachable there — only the differential equality
			// above applies.
			ex, err := elided.Explain(ctx)
			require.NoError(t, err)
			if !arrayB && assert.Contains(t, ex.Sql, "IndexFilter") {
				assert.NotContains(t, ex.Sql, "-> Filter ", "residual filter must be elided: %s", ex.Sql)
				assert.False(t, strings.HasSuffix(ex.Sql, "-> Filter") || strings.Contains(ex.Sql, "-> Filter -"),
					"residual filter must be elided: %s", ex.Sql)
			}
		})
	}

	t.Run("negative gates keep the residual", func(t *testing.T) {
		fx := newFixture(t)
		coll := newColl(t, fx, false)

		for _, tc := range []struct{ name, filter string }{
			{"uncovered extra field", `{"b":5,"pad":{"$exists":true}}`},
			{"two predicates on b", `{"b":{"$gte":5,"$lte":5}}`},
			{"in with two values", `{"b":{"$in":[5,6]}}`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ex, err := coll.Find(tc.filter).Sort("a").Limit(100).Explain(ctx)
				require.NoError(t, err)
				if strings.Contains(ex.Sql, "IndexFilter") {
					assert.Contains(t, ex.Sql, "-> Filter", "residual must stay for %s: %s", tc.filter, ex.Sql)
				}
			})
		}
	})
}
