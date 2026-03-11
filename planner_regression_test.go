package anystore

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestPlannerRegression_ReverseIndexSortOrder(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"-a"}})

	explain, err := coll.Find(nil).Sort("-a").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan(-a)")
	assert.NotContains(t, explain.Sql, "-> Sort")

	vals := collectField(t, coll.Find(nil).Sort("-a"), "a")
	require.Len(t, vals, 100)
	for i := 1; i < len(vals); i++ {
		prev, perr := strconv.Atoi(vals[i-1])
		cur, cerr := strconv.Atoi(vals[i])
		require.NoError(t, perr)
		require.NoError(t, cerr)
		if prev < cur {
			t.Fatalf("expected descending order at %d: %d < %d", i, prev, cur)
		}
	}
}

func TestPlannerRegression_CompoundMixedDirectionSortOrder(t *testing.T) {
	// Create explicit x/y distribution used by this test.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x", "y"}}))
	for i := range 200 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"x":%d,"y":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(nil).Sort("x", "-y").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "Sort")

	iter, err := coll.Find(nil).Sort("x", "-y").Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	prevX := -1
	prevY := int(^uint(0) >> 1) // max int
	first := true
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		xv := doc.Value().Get("x")
		yv := doc.Value().Get("y")
		require.NotNil(t, xv)
		require.NotNil(t, yv)
		x, xerr := strconv.Atoi(xv.String())
		y, yerr := strconv.Atoi(yv.String())
		require.NoError(t, xerr)
		require.NoError(t, yerr)

		if first {
			first = false
			prevX, prevY = x, y
			continue
		}

		if x == prevX {
			if prevY < y {
				t.Fatalf("expected y descending within same x: prev=(%d,%d) cur=(%d,%d)", prevX, prevY, x, y)
			}
		} else {
			if prevX > x {
				t.Fatalf("expected x ascending: prev=(%d,%d) cur=(%d,%d)", prevX, prevY, x, y)
			}
		}
		prevX, prevY = x, y
	}
	require.NoError(t, iter.Err())
}

func TestPlannerRegression_BoundsBuildMoreThanEightIndexes(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	for i := 0; i < 9; i++ {
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{fmt.Sprintf("k%d", i)}}))
	}

	for i := range 200 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k0":%d,"k1":%d,"k2":%d,"k3":%d,"k4":%d,"k5":%d,"k6":%d,"k7":%d,"k8":%d}`,
			i, i%3, i%4, i%5, i%6, i%7, i%8, i%9, i%10, i%11))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(`{"k8": 3}`).Explain(ctx)
	require.NoError(t, err)

	// Expected: planner should use the k8 index. Current behavior (bug) falls back
	// to full scan because buildBoundsResult only preloads up to 8 indexes.
	assert.Contains(t, explain.Sql, "IndexScan(k8)")
	assert.NotContains(t, explain.Sql, "FullScan")
}
