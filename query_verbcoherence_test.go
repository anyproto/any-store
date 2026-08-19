package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// TestVerbCoherence_Knn pins the central $knn invariant: for a fixed query Q on
// a fixed snapshot, every verb denotes the same document sequence Rows(Q) —
// Count(Q) == len(Rows(Q)), Delete(Q) removes exactly set(Rows(Q)), Update(Q)
// visits exactly that set, Explain(Q) describes the ANN plan, and two identical
// runs return identical output (source-level determinism).
//
// Matrix: all five index modes × {no residual, selective residual, _distance
// residual} × {no sort, real-field sort, _distance sort} × {no paging,
// Limit(3), Offset(2).Limit(3)}. The _distance residual on the non-Iter verbs
// is the axis that catches the injectDistance gating trap: if injection were
// gated on the sidecar, Comp.Ok(nil) would be TRUE for $lt against a missing
// _distance and a bounded Delete would remove all k instead of the thresholded
// subset.
func TestVerbCoherence_Knn(t *testing.T) {
	const (
		dim = 8
		n   = 400
		k   = 6
	)

	// v = [i/10, fixed…] → L2 distance to query [q0, fixed…] is |i-q|/10:
	// distinct per doc, exact under PQ re-rank, and the _distance threshold
	// below has a predictable membership. "r" is a sort field uncorrelated
	// with id; "a" is a ~50% selective residual field.
	docJSON := func(i int) string {
		return fmt.Sprintf(`{"id":%d,"v":[%g,1,2,3,4,5,6,7],"a":%d,"r":%d}`,
			i, float32(i)/10, i%2, (i*7919)%n)
	}
	queryVec := `[20.05,1,2,3,4,5,6,7]` // between docs 200 and 201: no exact ties, no self-doc

	modes := []struct {
		name string
		mode VectorMode
	}{
		{"btree", VectorModeBTree},
		{"hybrid", VectorModeHybrid},
		{"bruteforce", VectorModeBruteForce},
		{"ivfpq", VectorModeIVFPQ},
		{"ivfsq", VectorModeIVFSQ},
	}
	residuals := []struct {
		name string
		cond string // appended after the $knn clause
	}{
		{"none", ""},
		{"selective", `,"a":1`},
		// |i-200.5|/10 < 0.35 → docs 198..203 (6 docs) are within the
		// threshold BEFORE the k-cut; survivors depend on the ANN ranking.
		{"distance", `,"_distance":{"$lt":0.35}`},
	}
	sorts := []struct {
		name string
		sort []any
	}{
		{"none", nil},
		{"real-field", []any{"-r"}},
		{"distance", []any{"_distance"}},
	}
	pagings := []struct {
		name          string
		limit, offset uint
	}{
		{"all", 0, 0},
		{"limit3", 3, 0},
		{"offset2limit3", 3, 2},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "vc_"+m.name)
			require.NoError(t, err)
			// Docs first: IVF trains its codebooks from existing documents.
			docs := make(map[int]string, n)
			for i := 0; i < n; i++ {
				docs[i] = docJSON(i)
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(docs[i])))
			}
			require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
				Name: "emb", Kind: IndexKindVector,
				Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64, Mode: m.mode},
			}))

			if m.mode == VectorModeHybrid {
				// Warm the RAM l0 mirror and dirty its overlay: read verbs
				// (Iter/Count) traverse the mirror while write verbs
				// (Delete/Update) traverse raw btree adjacency inside the
				// write tx — the coherence below asserts the two are
				// candidate-identical, an equivalence nothing exercised while
				// the write verbs never reached the vector path at all.
				for w := 0; w < 3; w++ {
					_, werr := coll.Find(fmt.Sprintf(`{"v":{"$knn":{"$query":%s,"$k":%d}}}`, queryVec, k)).Count(ctx)
					require.NoError(t, werr)
				}
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(docJSON(n))))
				require.NoError(t, coll.DeleteId(ctx, n))
			}

			for _, res := range residuals {
				for _, srt := range sorts {
					for _, pg := range pagings {
						name := fmt.Sprintf("res=%s/sort=%s/page=%s", res.name, srt.name, pg.name)
						t.Run(name, func(t *testing.T) {
							cond := fmt.Sprintf(`{"v":{"$knn":{"$query":%s,"$k":%d}}%s}`, queryVec, k, res.cond)
							mkQ := func() Query {
								q := coll.Find(cond)
								if srt.sort != nil {
									q = q.Sort(srt.sort...)
								}
								if pg.limit > 0 {
									q = q.Limit(pg.limit)
								}
								if pg.offset > 0 {
									q = q.Offset(pg.offset)
								}
								return q
							}

							ids := writeOrderIterIds(t, mkQ())
							assert.LessOrEqual(t, len(ids), k, "the k-cut bounds every page")
							assert.Equal(t, ids, writeOrderIterIds(t, mkQ()),
								"two identical runs must return identical sequences")

							count, err := mkQ().Count(ctx)
							require.NoError(t, err)
							assert.Equal(t, len(ids), count, "Count(Q) == len(Rows(Q))")

							exp, err := mkQ().Explain(ctx)
							require.NoError(t, err)
							assert.Contains(t, exp.Sql, "KnnSearch(", "Explain describes the ANN plan: %s", exp.Sql)

							upd, err := mkQ().Update(ctx, anyenc.MustParseJson(`{"$inc":{"touched":1}}`))
							require.NoError(t, err)
							assert.Equal(t, len(ids), upd.Matched, "Update(Q) visits exactly Rows(Q)")

							before, err := coll.Count(ctx)
							require.NoError(t, err)
							del, err := mkQ().Delete(ctx)
							require.NoError(t, err)
							assert.Equal(t, len(ids), del.Modified, "Delete(Q) removes exactly len(Rows(Q))")
							after, err := coll.Count(ctx)
							require.NoError(t, err)
							assert.Equal(t, before-len(ids), after)
							for _, id := range ids {
								doc, gerr := coll.FindId(ctx, id)
								assert.Error(t, gerr, "doc %d was in Rows(Q) and must be deleted (got %v)", id, doc)
							}

							// Restore the deleted docs so the next cell sees
							// the same document set (the index graph churns,
							// which only adds realism — each cell is
							// self-consistent on its own snapshot).
							for _, id := range ids {
								require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(docs[id])))
							}
						})
					}
				}
			}
		})
	}
}
