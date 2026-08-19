package anystore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// The unique-index CoverIter branch elides the residual FilterIter for
// covering counts (bounds fully represent the filter). These tests pin the
// Count == Iter parity on that branch: the elision must never change the
// counted set — including the gate-rejection shapes that must keep the
// residual, and the multikey dedup a unique array index needs.

func uniqueInColl(t *testing.T, n int) (fixture *fixture, coll Collection) {
	fx := newFixture(t)
	c, err := fx.CreateCollection(ctx, "users")
	require.NoError(t, err)
	require.NoError(t, c.CreateIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))
	for i := 0; i < n; i++ {
		require.NoError(t, c.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"email":"user%d@test.com","grp":%d}`, i, i, i%2))))
	}
	return fx, c
}

func countAndIterLen(t *testing.T, coll Collection, filter string) (int, int) {
	t.Helper()
	cnt, err := coll.Find(filter).Count(ctx)
	require.NoError(t, err)
	iterN := 0
	iter, err := coll.Find(filter).Iter(ctx)
	require.NoError(t, err)
	for iter.Next() {
		iterN++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return cnt, iterN
}

func TestUniqueInCountCovering(t *testing.T) {
	_, coll := uniqueInColl(t, 5000)

	var vals []string
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf(`"user%d@test.com"`, i*7))
	}
	// Half the values miss (beyond the population): count = hits only.
	for i := 0; i < 50; i++ {
		vals = append(vals, fmt.Sprintf(`"missing%d@test.com"`, i))
	}
	in := fmt.Sprintf(`{"email":{"$in":[%s]}}`, strings.Join(vals, ","))

	cnt, iterN := countAndIterLen(t, coll, in)
	assert.Equal(t, 100, cnt)
	assert.Equal(t, cnt, iterN, "covering count must equal Iter cardinality")

	// Single-value Eq takes the same branch.
	cnt, iterN = countAndIterLen(t, coll, `{"email":"user42@test.com"}`)
	assert.Equal(t, 1, cnt)
	assert.Equal(t, cnt, iterN)

	// Duplicate $in values must not double-count the doc.
	cnt, _ = countAndIterLen(t, coll,
		`{"email":{"$in":["user7@test.com","user7@test.com"]}}`)
	assert.Equal(t, 1, cnt)
}

func TestUniqueInCountKeepsResidualWhenNotCovering(t *testing.T) {
	_, coll := uniqueInColl(t, 2000)

	// Second predicate on the bounded field: bounds over-approximate, the
	// residual must stay — $nin removes one of the $in hits.
	f := `{"email":{"$in":["user1@test.com","user2@test.com","user3@test.com"],"$nin":["user2@test.com"]}}`
	cnt, iterN := countAndIterLen(t, coll, f)
	assert.Equal(t, 2, cnt)
	assert.Equal(t, cnt, iterN)

	// Uncovered second field: the filter reaches beyond the index, residual
	// must stay — grp halves the hits.
	f = `{"email":{"$in":["user1@test.com","user2@test.com","user3@test.com","user4@test.com"]},"grp":0}`
	cnt, iterN = countAndIterLen(t, coll, f)
	assert.Equal(t, 2, cnt)
	assert.Equal(t, cnt, iterN)
}

// A unique index over an array field is still multikey: one doc fans out to
// one entry per element, uniqueness enforced per entry. A $in matching two
// elements of the SAME doc must count it once — the elision relies on the
// CoverIter multiKey tagging + DocDedupIter for this.
func TestUniqueInCountMultikeyDedup(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"aliases"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"aliases":["a","b","c"]}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"aliases":["d"]}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"aliases":["e","f"]}`)))

	// "a" and "b" hit doc 1 through two of its entries; "d" hits doc 2.
	cnt, iterN := countAndIterLen(t, coll, `{"aliases":{"$in":["a","b","d"]}}`)
	assert.Equal(t, 2, cnt, "same-doc multi-element hits must dedup")
	assert.Equal(t, cnt, iterN)
}
