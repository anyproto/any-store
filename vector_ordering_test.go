package anystore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// Rule V: no ordering comparison against a vector may ever match
// everything. Syntax-independent — this must hold no matter what the ANN query
// syntax becomes.
//
// The mass delete needed NO vector index and no vector-valued document: anyenc
// resolves cross-type comparisons on the type tag, and TypeVectorF32 (10) sorts
// above every scalar tag, so an ordering op with a vector operand was true for
// every document — including ones where the field is absent.
func TestVectorOrderingNeverMatchesEverything(t *testing.T) {
	t.Run("no vector index anywhere", func(t *testing.T) {
		ctx := context.Background()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "plain")
		require.NoError(t, err)

		docs := []string{
			`{"id":1,"n":5,"s":"abc","tags":[1,2]}`,
			`{"id":2,"n":-1,"s":"zzz"}`,
			`{"id":3,"b":true}`,
			`{"id":4,"o":{"k":"v"}}`,
			`{"id":5}`,
		}
		for _, d := range docs {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
		}

		// Parse-time: an ordering op against a $vector literal is rejected outright,
		// on every field — present, absent, scalar, array.
		for _, cond := range []string{
			`{"n":{"$lt":{"$vector":[0]}}}`,
			`{"s":{"$lt":{"$vector":[0]}}}`,
			`{"tags":{"$lt":{"$vector":[0]}}}`,
			`{"nosuchfield":{"$lt":{"$vector":[0]}}}`,
			`{"nosuchfield":{"$gt":{"$vector":[0]}}}`,
		} {
			_, err := coll.Find(cond).Count(ctx)
			assert.ErrorIs(t, err, query.ErrVectorNotOrderable, "cond=%s", cond)
		}

		// Eval-time: a hand-built filter never sees the parser. It must still match
		// nothing — and, above all, Delete must remove nothing.
		a := &anyenc.Arena{}
		vec := a.NewVectorF32([]float32{0})
		for _, op := range []query.CompOp{query.CompOpGt, query.CompOpGte, query.CompOpLt, query.CompOpLte} {
			for _, path := range []string{"n", "s", "tags", "nosuchfield"} {
				f := query.Key{Path: []string{path}, Filter: query.NewCompValue(op, vec)}

				count, err := coll.Find(f).Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, 0, count, "op=%d path=%s must match no documents", op, path)

				res, err := coll.Find(f).Delete(ctx)
				require.NoError(t, err)
				assert.Equal(t, 0, res.Modified, "op=%d path=%s must delete nothing", op, path)
			}
		}

		remaining, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(docs), remaining, "the collection must be intact")
	})

	t.Run("vector-indexed field", func(t *testing.T) {
		ctx := context.Background()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "vec")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Name:   "v",
			Vector: &VectorParams{Field: "v", Dim: 3, Metric: VectorCosine},
		}))

		const n = 20
		for i := 0; i < n; i++ {
			doc := fmt.Sprintf(`{"id":%d,"v":{"$vector":[%d,1,2]}}`, i, i)
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(doc)))
		}

		// The clause as originally filed: a scalar ordering op on a vector field.
		// It used to Count 20 of 20 and Delete the entire collection, err=nil.
		for _, cond := range []string{
			`{"v":{"$gt":1}}`,
			`{"v":{"$gte":1}}`,
			`{"v":{"$lt":1}}`,
			`{"v":{"$lte":1}}`,
		} {
			count, err := coll.Find(cond).Count(ctx)
			require.NoError(t, err, cond)
			assert.Equal(t, 0, count, "cond=%s must match no documents", cond)

			res, err := coll.Find(cond).Delete(ctx)
			require.NoError(t, err, cond)
			assert.Equal(t, 0, res.Modified, "cond=%s must delete nothing", cond)
		}

		remaining, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, n, remaining, "the collection must be intact")
	})
}

// Count is the only verb that does not go through makeQuery, so it used to skip
// the q.err check entirely: a query that failed to parse left q.cond nil, which
// the no-filter fast path read as "count everything". Every other verb errored.
// Found while wiring Rule V's parse rejection, but it is general — any invalid
// operator hit it.
func TestCount_ReportsParseError(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))))
	}

	_, err = coll.Find(`{"a":{"$bogusOp":1}}`).Count(ctx)
	assert.Error(t, err, "Count must not swallow a parse error and count the whole collection")

	_, err = coll.Find(`{"a":{"$lt":{"$vector":[0]}}}`).Count(ctx)
	assert.ErrorIs(t, err, query.ErrVectorNotOrderable)

	// a valid no-filter Count still takes the fast path
	n, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
}

// $type:"vectorF32" is the supported way to select vector-valued documents now
// that ordering ops against a vector are always false.
//
// The axis that matters is the DESCENDING index combined with a sort that
// selects it: a reverse index key is bitwise-inverted, so a vector in one lands
// under the tag ^Type(10) = 0xF5. anyenc used to reject that as an unknown type,
// which made extractDocId hand back the whole key as the docId — the Fetch then
// missed and the row vanished, with err=nil. A forward-only or sort-free test
// passes while every one of those rows is being dropped, so both axes are here.
func TestType_VectorF32_AcrossIndexes(t *testing.T) {
	for _, idx := range []string{"", "v", "-v"} {
		for _, sort := range []string{"", "v", "-v"} {
			name := fmt.Sprintf("index=%q/sort=%q", idx, sort)
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				fx := newFixture(t)
				coll, err := fx.CreateCollection(ctx, "c")
				require.NoError(t, err)
				if idx != "" {
					require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{idx}}))
				}

				for _, d := range []string{
					`{"id":1,"v":{"$vector":[1,2]}}`,
					`{"id":2,"v":{"$vector":[3,4]}}`,
					`{"id":3,"v":{"$vector":[5,6]}}`,
					`{"id":4,"v":1}`,
					`{"id":5,"v":"s"}`,
					`{"id":6,"v":[1,2]}`,
				} {
					require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
				}

				count, err := coll.Find(`{"v":{"$type":"vectorF32"}}`).Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, 3, count, "Count must find the 3 vector-valued docs")

				q := coll.Find(`{"v":{"$type":"vectorF32"}}`)
				if sort != "" {
					q = q.Sort(sort)
				}
				iter, err := q.Iter(ctx)
				require.NoError(t, err)
				var iterated int
				for iter.Next() {
					iterated++
				}
				require.NoError(t, iter.Close())
				assert.Equal(t, 3, iterated, "Iter must agree with Count")
			})
		}
	}
}

// A vector-valued document must survive a scan of a DESCENDING index even when
// there is no filter at all — the reverse key holds an inverted vector tag, and
// failing to parse it silently dropped the row.
func TestReverseIndex_KeepsVectorValuedDocs(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"-v"}}))

	for _, d := range []string{
		`{"id":1,"v":{"$vector":[1,2]}}`,
		`{"id":2,"v":{"$vector":[3,4]}}`,
		`{"id":3,"v":1}`,
		`{"id":4,"v":"s"}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}

	iter, err := coll.Find(nil).Sort("-v").Iter(ctx)
	require.NoError(t, err)
	var ids []string
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		ids = append(ids, d.Value().Get("id").String())
	}
	require.NoError(t, iter.Close())
	assert.Len(t, ids, 4, "no document may be dropped by a reverse-index scan, got ids=%v", ids)
}
