package qplanner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// vectorTestDB builds a data namespace with n docs {"id":i,"p":i%3} keyed by
// the encoded id, plus a Search stub returning the given candidates.
func vectorTestDB(t *testing.T, n int) (*btree.ReadTx, *btree.Namespace, func(int) []byte) {
	t.Helper()
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("data")
	require.NoError(t, err)
	idOf := func(i int) []byte { return anyenc.AppendAnyValue(nil, i) }
	a := &anyenc.Arena{}
	for i := 0; i < n; i++ {
		a.Reset()
		obj := a.NewObject()
		obj.Set("id", a.NewNumberInt(i))
		obj.Set("p", a.NewNumberInt(i%3))
		require.NoError(t, wtx.Put(ns, idOf(i), obj.MarshalTo(nil)))
	}
	require.NoError(t, wtx.Commit())
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })
	return rtx, ns, idOf
}

func knnSpec(idOf func(int) []byte, k int, cands ...int) *VectorQuerySpec {
	return &VectorQuerySpec{
		Query:          []float32{1},
		K:              k,
		Ef:             len(cands),
		IndexName:      "emb",
		TotallyOrdered: true,
		Search: func(tx *btree.ReadTx, q []float32, ef int) ([]VectorCandidate, error) {
			out := make([]VectorCandidate, 0, len(cands))
			for rank, id := range cands {
				out = append(out, VectorCandidate{DocId: idOf(id), Distance: float32(rank) / 10})
			}
			return out, nil
		},
	}
}

// TestBuildVectorPlan_KCutBeforeSort pins the plan shape: the k-cut LimitIter
// sits AFTER the residual filter and BEFORE the user sort, with pagination
// outermost — "k selects, Sort orders, Limit paginates".
func TestBuildVectorPlan_KCutBeforeSort(t *testing.T) {
	rtx, ns, idOf := vectorTestDB(t, 10)
	buf := syncpool.NewSyncPool(0).GetDocBuf()
	sorter, err := query.ParseSort("-p")
	require.NoError(t, err)

	plan := BuildPlan(&PlanParams{
		Tx:     rtx,
		DataNs: ns,
		Filter: query.MustParseCondition(`{"p":{"$gte":0}}`),
		Sorter: sorter,
		Limit:  3,
		Offset: 1,
		Buf:    buf,
		Vector: knnSpec(idOf, 5, 0, 1, 2, 3, 4, 5, 6, 7),
	})
	assert.Equal(t, "KnnSearch", plan.Name)
	assert.Equal(t, "emb", plan.IndexName)
	assert.Equal(t,
		"KnnSearch(k=5,ef=8) -> Filter -> Limit(5) -> TopK(4) -> Limit(offset=1,limit=3)",
		plan.String(), "filter → cut-to-k → sort (bounded to offset+limit) → page")
	plan.Close()

	// Without residual/sort/paging the k-cut is still present.
	plan = BuildPlan(&PlanParams{
		Tx: rtx, DataNs: ns, Buf: buf,
		Vector: knnSpec(idOf, 5, 0, 1, 2),
	})
	assert.Equal(t, "KnnSearch(k=5,ef=3) -> Limit(5)", plan.String())
	plan.Close()
}

// TestVectorIter_KCutAndOrder: the source order (TotallyOrdered at the
// backend) streams through, the k-cut truncates AFTER the residual filter, and
// the pagination window slices the sorted k-set.
func TestVectorIter_KCutAndOrder(t *testing.T) {
	rtx, ns, idOf := vectorTestDB(t, 12)
	buf := syncpool.NewSyncPool(0).GetDocBuf()

	collect := func(plan *Plan) []string {
		defer plan.Close()
		var out []string
		for {
			_, docId, _, err := plan.Root.Next()
			require.NoError(t, err)
			if docId == nil {
				return out
			}
			out = append(out, string(docId))
		}
	}

	// k=3 cut of an 8-candidate stream: first 3 in source (distance) order.
	plan := BuildPlan(&PlanParams{
		Tx: rtx, DataNs: ns, Buf: buf,
		Vector: knnSpec(idOf, 3, 7, 5, 3, 1, 0, 2, 4, 6),
	})
	want := []string{string(idOf(7)), string(idOf(5)), string(idOf(3))}
	assert.Equal(t, want, collect(plan), "k-cut keeps the k nearest in source order")

	// Residual filter (p==1 → ids 1,4,7,10) runs BEFORE the cut: the k=2 set
	// is the 2 nearest SURVIVORS, not survivors of the 2 nearest.
	plan = BuildPlan(&PlanParams{
		Tx: rtx, DataNs: ns, Buf: buf,
		Filter: query.MustParseCondition(`{"p":1}`),
		Vector: knnSpec(idOf, 2, 0, 1, 2, 3, 4, 5, 6, 7),
	})
	want = []string{string(idOf(1)), string(idOf(4))}
	assert.Equal(t, want, collect(plan), "filter, THEN cut — hybrid search would be destroyed otherwise")

	// Offset beyond k → empty.
	plan = BuildPlan(&PlanParams{
		Tx: rtx, DataNs: ns, Buf: buf,
		Offset: 3,
		Vector: knnSpec(idOf, 3, 0, 1, 2, 3, 4),
	})
	assert.Empty(t, collect(plan), "Offset >= k pages past the denoted set")
}

// TestVectorIter_InjectDistanceUnconditional pins the T2.6 gating rule:
// NeedDistances gates ONLY the sidecar; the _distance injection into the
// in-flight document is unconditional, because the residual filter reads it
// from Plan.DocParsed on every verb. If injection were gated, a
// {"_distance":{"$lt":x}} residual would evaluate Comp.Ok(nil) == true
// (number tag > null tag) — match-all — and a bounded Delete would remove all
// k instead of the thresholded subset.
func TestVectorIter_InjectDistanceUnconditional(t *testing.T) {
	rtx, ns, idOf := vectorTestDB(t, 6)
	buf := syncpool.NewSyncPool(0).GetDocBuf()

	// Distances are rank/10 = 0.0, 0.1, 0.2, ... — threshold 0.15 keeps 2.
	mk := func(needDistances bool) *Plan {
		spec := knnSpec(idOf, 5, 0, 1, 2, 3, 4)
		spec.NeedDistances = needDistances
		return BuildPlan(&PlanParams{
			Tx: rtx, DataNs: ns, Buf: buf,
			Filter: query.MustParseCondition(`{"_distance":{"$lt":0.15}}`),
			Vector: spec,
		})
	}

	for _, needDistances := range []bool{false, true} {
		plan := mk(needDistances)
		var n int
		for {
			_, docId, _, err := plan.Root.Next()
			require.NoError(t, err)
			if docId == nil {
				break
			}
			n++
		}
		assert.Equal(t, 2, n,
			"needDistances=%v: the _distance residual must threshold the stream", needDistances)
		if needDistances {
			assert.NotNil(t, plan.Distances, "sidecar kept for the read verb")
		} else {
			assert.Nil(t, plan.Distances, "sidecar skipped when nothing reads Distance()")
		}
		plan.Close()
	}
}

// TestVectorIter_String pins the explain fragment (k and ef are the two
// numbers a human needs to reason about an ANN result).
func TestVectorIter_String(t *testing.T) {
	it := &VectorIter{Spec: &VectorQuerySpec{K: 10, Ef: 100}}
	assert.Equal(t, "KnnSearch(k=10,ef=100)", it.String())
	assert.Equal(t, "KnnSearch(k=10,ef=100)", fmt.Sprintf("%s", it))
}
