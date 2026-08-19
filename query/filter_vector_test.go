package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// Rule V: a vector is not a point on the scalar order.
//
// anyenc resolves cross-type comparisons on the type tag, and TypeVectorF32 (10)
// sorts above every scalar tag (null=1 .. compressedObjectS2=9). That made every
// ordering op against a vector — on either side — true for every document, so
// Find({"anyField":{"$lt":{"$vector":[0]}}}).Delete(ctx) emptied any collection
// with err=nil, no vector index required anywhere.
func TestComp_VectorIsNotOrdered(t *testing.T) {
	doc := anyenc.MustParseJson(`{"n":5,"s":"abc","tags":[1,2],"v":{"$vector":[1,2,3]}}`)
	buf := &syncpool.DocBuffer{}

	for _, tc := range []struct {
		name string
		cond string
		want bool
	}{
		// --- probe side: the stored value is the vector (the clause as filed) ---
		{"gt scalar vs vector field", `{"v":{"$gt":1}}`, false},
		{"gte scalar vs vector field", `{"v":{"$gte":1}}`, false},
		{"lt scalar vs vector field", `{"v":{"$lt":1}}`, false},
		{"lte scalar vs vector field", `{"v":{"$lte":1}}`, false},
		{"gt string vs vector field", `{"v":{"$gt":"zzz"}}`, false},
		{"lt bool vs vector field", `{"v":{"$lt":true}}`, false},

		// NOTE: the operand-side family ({"n":{"$lt":{"$vector":[..]}}}) is not here
		// because it no longer PARSES — see TestParse_VectorOperandNotOrderable. Its
		// eval-time guard, which is what protects hand-built filters, is exercised by
		// TestComp_VectorIsNotOrdered_HandBuilt.

		// --- equality stays byte equality: a vector IS comparable for $eq/$ne ---
		{"eq same vector", `{"v":{"$eq":{"$vector":[1,2,3]}}}`, true},
		{"eq different vector", `{"v":{"$eq":{"$vector":[9,9,9]}}}`, false},
		{"eq plain array vs packed vector", `{"v":{"$eq":[1,2,3]}}`, false},
		{"eq scalar vs vector", `{"v":{"$eq":1}}`, false},
		{"ne scalar vs vector", `{"v":{"$ne":1}}`, true},
		{"ne different vector", `{"v":{"$ne":{"$vector":[9,9,9]}}}`, true},
		{"ne same vector", `{"v":{"$ne":{"$vector":[1,2,3]}}}`, false},
		{"ne vector vs absent field", `{"nosuchfield":{"$ne":{"$vector":[1,2,3]}}}`, true},

		// --- $exists is unaffected ---
		{"exists on vector field", `{"v":{"$exists":true}}`, true},

		// --- the one behaviour Rule V WIDENS, pinned deliberately ---
		//
		// $not of a comparison the value cannot satisfy is true, so {"$not":{"$gt":1}}
		// on a vector field now matches (it did not before, when $gt itself matched).
		// This is MongoDB's type-bracketing semantics and it is correct: a vector is
		// not greater than 1, so it is not-greater-than-1. It requires the caller to
		// deliberately type $not, unlike the bug, which fired on a plain $gt.
		// Pinned so nobody "fixes" it back.
		{"not gt on vector field", `{"v":{"$not":{"$gt":1}}}`, true},
		{"not lt on vector field", `{"v":{"$not":{"$lt":1}}}`, true},
		{"not gt on a scalar field is unchanged", `{"n":{"$not":{"$gt":1}}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := ParseCondition(tc.cond)
			require.NoError(t, err)
			assert.Equal(t, tc.want, f.Ok(doc, buf))
		})
	}
}

// The parser cannot see hand-built filters, so the eval-time guard is the
// backstop. NewCompValue leaves notArray=false, which routes an array probe
// through Comp.Ok's whole-array shortcut — a second path into the same bug.
func TestComp_VectorIsNotOrdered_HandBuilt(t *testing.T) {
	a := &anyenc.Arena{}
	vec := a.NewVectorF32([]float32{1, 2, 3})
	doc := anyenc.MustParseJson(`{"n":5,"s":"abc","tags":[1,2],"v":{"$vector":[1,2,3]}}`)
	buf := &syncpool.DocBuffer{Parser: &anyenc.Parser{}}

	for _, op := range []CompOp{CompOpGt, CompOpGte, CompOpLt, CompOpLte} {
		cmp := NewCompValue(op, vec)
		for _, path := range []string{"n", "s", "tags", "v", "nosuchfield"} {
			k := Key{Path: []string{path}, Filter: cmp}
			assert.False(t, k.Ok(doc, buf), "op=%d path=%s: ordering against a vector operand must be false", op, path)
			// the raw fast path must reach the same verdict
			if ok, handled := k.OkRaw(doc.MarshalTo(nil), buf); handled {
				assert.False(t, ok, "op=%d path=%s: raw fast path must agree", op, path)
			}
		}
	}
}

// An ordering op whose operand is a $vector literal is rejected at parse time:
// it is decidable syntactically, and killing it at the door also prevents its
// reflection through $not (where a false inner predicate becomes match-all).
func TestParse_VectorOperandNotOrderable(t *testing.T) {
	for _, cond := range []string{
		`{"n":{"$lt":{"$vector":[0]}}}`,
		`{"n":{"$lte":{"$vector":[0]}}}`,
		`{"n":{"$gt":{"$vector":[0]}}}`,
		`{"n":{"$gte":{"$vector":[0]}}}`,
		`{"n":{"$not":{"$lt":{"$vector":[0]}}}}`,
	} {
		_, err := ParseCondition(cond)
		assert.ErrorIs(t, err, ErrVectorNotOrderable, "cond=%s", cond)
	}

	// $eq / $ne against a vector stay legal — they are byte equality.
	for _, cond := range []string{
		`{"v":{"$eq":{"$vector":[1,2]}}}`,
		`{"v":{"$ne":{"$vector":[1,2]}}}`,
	} {
		_, err := ParseCondition(cond)
		assert.NoError(t, err, "cond=%s", cond)
	}
}

// Rule V removes the (accidental) way to select vector-valued documents, so
// $type gives the correct one.
func TestType_VectorF32(t *testing.T) {
	vecDoc := anyenc.MustParseJson(`{"v":{"$vector":[1,2,3]}}`)
	numDoc := anyenc.MustParseJson(`{"v":1}`)
	arrDoc := anyenc.MustParseJson(`{"v":[1,2,3]}`)
	buf := &syncpool.DocBuffer{}

	for _, cond := range []string{`{"v":{"$type":"vectorF32"}}`, `{"v":{"$type":10}}`} {
		f, err := ParseCondition(cond)
		require.NoError(t, err, cond)
		assert.True(t, f.Ok(vecDoc, buf), cond)
		assert.False(t, f.Ok(numDoc, buf), cond)
		assert.False(t, f.Ok(arrDoc, buf), cond)
	}
}

// A vector is not a range: TypeFilter must contribute NO index bounds for it.
// The generic [tag, tag+1) bound is unsound on a reverse index — reverse keys are
// bitwise-inverted, so ^Type(10) = 0xF5 is (by anyenc's own invariant) an unknown
// type, and the transformed bound lands strictly below every real entry, silently
// dropping every row with err=nil.
func TestTypeFilter_VectorContributesNoBounds(t *testing.T) {
	vec := TypeFilter{Type: anyenc.TypeVectorF32}
	assert.Nil(t, vec.IndexBounds("v", nil))

	// every other type still contributes its tag range
	num := TypeFilter{Type: anyenc.TypeNumber}
	assert.Len(t, num.IndexBounds("v", nil), 1)
}
