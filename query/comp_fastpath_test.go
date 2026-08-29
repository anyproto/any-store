package query

import (
	"bytes"
	"math"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// refBracket is the oracle's own copy of the bracket rule (MongoDB type
// bracketing): the type tag, with false/true folded into one bracket.
func refBracket(tag byte) byte {
	if tag == byte(anyenc.TypeTrue) {
		return byte(anyenc.TypeFalse)
	}
	return tag
}

// refComp is the pre-fast-path comparison: bytes.Compare of the marshaled
// operands, with ordering ops restricted to one bracket — a probe outside the
// operand's bracket (or a degenerate empty side) is never less or greater.
// Rule V is the one divergence from pure bracketing: a vector is not
// orderable even against itself (docs/query-filter-contract.md item 6).
func refComp(e *Comp, probe []byte) bool {
	switch e.CompOp {
	case CompOpGt, CompOpGte, CompOpLt, CompOpLte:
		if len(e.EqValue) == 0 || len(probe) == 0 || refBracket(e.EqValue[0]) != refBracket(probe[0]) {
			return false
		}
		if e.EqValue[0] == byte(anyenc.TypeVectorF32) {
			return false
		}
	}
	return e.compResult(bytes.Compare(e.EqValue, probe))
}

// compReference marshals the probe and compares it with refComp. okScalar must
// agree with it bit-for-bit for every operator and every value pairing.
//
// The vector values stay in the matrix below so Rule V stays pinned — a vector
// is its own bracket, so an ordering op against one is false on either side.
func compReference(e *Comp, v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	buf.SmallBuf = v.MarshalTo(buf.SmallBuf[:0])
	return refComp(e, buf.SmallBuf)
}

func TestCompOkScalar_MatchesMarshalReference(t *testing.T) {
	a := &anyenc.Arena{}
	values := []*anyenc.Value{
		a.NewNumberFloat64(0),
		a.NewNumberFloat64(math.Copysign(0, -1)), // -0
		a.NewNumberFloat64(1),
		a.NewNumberFloat64(-1),
		a.NewNumberFloat64(1.5),
		a.NewNumberFloat64(-1.5),
		a.NewNumberFloat64(math.MaxFloat64),
		a.NewNumberFloat64(-math.MaxFloat64),
		a.NewNumberFloat64(math.SmallestNonzeroFloat64),
		a.NewNumberFloat64(math.Inf(1)),
		a.NewNumberFloat64(math.Inf(-1)),
		a.NewNumberFloat64(math.NaN()),
		a.NewString(""),
		a.NewString("a"),
		a.NewString("ab"),
		a.NewString("b"),
		a.NewString("\x00"),
		a.NewString("a\x00b"),
		a.NewTrue(),
		a.NewFalse(),
		a.NewNull(),
		a.NewBinary([]byte{0, 1, 2}),
		a.NewVectorF32([]float32{1, 2}),
		a.NewObjectID(anyenc.ObjectID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		a.NewObjectID(anyenc.ObjectID{0xff, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		a.NewDateTimeMillis(0),
		a.NewDateTimeMillis(1756058400000),
		a.NewDateTimeMillis(-1),
		anyenc.MustParseJson(`{"x":1}`),
		anyenc.MustParseJson(`[1,2]`),
	}
	ops := []CompOp{CompOpEq, CompOpNe, CompOpGt, CompOpGte, CompOpLt, CompOpLte}
	buf := &syncpool.DocBuffer{}
	refBuf := &syncpool.DocBuffer{}
	for _, eqV := range values {
		for _, op := range ops {
			cmp := &Comp{CompOp: op, EqValue: eqV.MarshalTo(nil)}
			for _, probe := range values {
				want := compReference(cmp, probe, refBuf)
				if got := cmp.okScalar(probe, buf); got != want {
					t.Errorf("op=%d eq=%s probe=%s: okScalar=%v, reference=%v", op, eqV, probe, got, want)
				}
			}
			// nil probe (absent field) must equal comparing against encoded null:
			// null is its own bracket, so an ordering op with any non-null operand
			// is false for an absent field, and {"$gte":null}/{"$lte":null} match it.
			wantNil := refComp(cmp, valueNull.MarshalTo(nil))
			if got := cmp.Ok(nil, buf); got != wantNil {
				t.Errorf("op=%d eq=%s probe=nil: Ok=%v, reference=%v", op, eqV, got, wantNil)
			}
		}
	}
}

// okReference replicates the pre-fast-path Comp.Ok (marshal every element)
// for differential testing of the array paths.
func okReference(e *Comp, v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	if v == nil {
		v = valueNull
	}
	if v.Type() == anyenc.TypeArray {
		vals, _ := v.Array()
		if e.CompOp == CompOpNe {
			if !e.notArray {
				buf.SmallBuf = v.MarshalTo(buf.SmallBuf[:0])
				if !refComp(e, buf.SmallBuf) {
					return false
				}
			}
			for _, val := range vals {
				if !compReference(e, val, buf) {
					return false
				}
			}
			return true
		}
		if !e.notArray {
			buf.SmallBuf = v.MarshalTo(buf.SmallBuf[:0])
			if refComp(e, buf.SmallBuf) {
				return true
			}
		}
		for _, val := range vals {
			if compReference(e, val, buf) {
				return true
			}
		}
		return false
	}
	return compReference(e, v, buf)
}

// TestCompOk_MixedTypeArrays_MatchesReference pins Ok's array paths on
// documents whose arrays mix value types — every element comparison must agree
// with the marshal-based reference for every operator and both notArray modes.
func TestCompOk_MixedTypeArrays_MatchesReference(t *testing.T) {
	a := &anyenc.Arena{}
	eqValues := []*anyenc.Value{
		a.NewNumberFloat64(2.5),
		a.NewNumberFloat64(0),
		a.NewString("x"),
		a.NewString(""),
		a.NewNull(),
		a.NewTrue(),
		anyenc.MustParseJson(`[1,"x"]`),
		anyenc.MustParseJson(`[]`),
	}
	probes := []*anyenc.Value{
		anyenc.MustParseJson(`[1,"x",null]`),
		anyenc.MustParseJson(`[true,2.5,"a"]`),
		anyenc.MustParseJson(`["a",[1,"x"],{"o":1}]`),
		anyenc.MustParseJson(`[]`),
		anyenc.MustParseJson(`[[1,"x"]]`),
		anyenc.MustParseJson(`[null]`),
		anyenc.MustParseJson(`2.5`),
		nil,
	}
	ops := []CompOp{CompOpEq, CompOpNe, CompOpGt, CompOpGte, CompOpLt, CompOpLte}
	buf := &syncpool.DocBuffer{}
	refBuf := &syncpool.DocBuffer{}
	for _, eqV := range eqValues {
		for _, op := range ops {
			for _, notArray := range []bool{false, true} {
				cmp := &Comp{CompOp: op, EqValue: eqV.MarshalTo(nil), notArray: notArray}
				for _, probe := range probes {
					want := okReference(cmp, probe, refBuf)
					if got := cmp.Ok(probe, buf); got != want {
						t.Errorf("op=%d eq=%s notArray=%v probe=%s: Ok=%v, reference=%v",
							op, eqV, notArray, probe, got, want)
					}
				}
			}
		}
	}
}

// TestInOk_NumericFastPathMatchesMap pins that In.Ok's numBits binary-search
// path (number probes on a NewInValue-built In) agrees with the marshal+map
// reference, which a hand-built In (no sorted/numBits) still takes.
func TestInOk_NumericFastPathMatchesMap(t *testing.T) {
	a := &anyenc.Arena{}
	members := []*anyenc.Value{
		a.NewNumberFloat64(0),
		a.NewNumberFloat64(math.Copysign(0, -1)), // dedups with +0
		a.NewNumberFloat64(3),
		a.NewNumberFloat64(-7.5),
		a.NewNumberFloat64(math.Inf(1)),
		a.NewNumberFloat64(math.NaN()),
		a.NewString("3"),
		a.NewNull(),
	}
	built := NewInValue(members...)
	mapOnly := In{Values: built.Values}
	probes := []*anyenc.Value{
		a.NewNumberFloat64(0),
		a.NewNumberFloat64(math.Copysign(0, -1)),
		a.NewNumberFloat64(3),
		a.NewNumberFloat64(4),
		a.NewNumberFloat64(-7.5),
		a.NewNumberFloat64(math.Inf(1)),
		a.NewNumberFloat64(math.Inf(-1)),
		a.NewNumberFloat64(math.NaN()),
		a.NewString("3"),
		a.NewNull(),
		anyenc.MustParseJson(`[3, "x"]`),
		anyenc.MustParseJson(`[4]`),
		nil,
	}
	buf := &syncpool.DocBuffer{}
	refBuf := &syncpool.DocBuffer{}
	for _, p := range probes {
		if got, want := built.Ok(p, buf), mapOnly.Ok(p, refBuf); got != want {
			t.Errorf("probe %s: fast path=%v, map reference=%v", p, got, want)
		}
	}
}

// Degenerate EqValue shapes (hand-built filters) must not panic and must keep
// raw bytes.Compare semantics: empty, bare tag, truncated number, oversized.
func TestCompOkScalar_DegenerateEqValue(t *testing.T) {
	a := &anyenc.Arena{}
	probes := []*anyenc.Value{
		a.NewNumberFloat64(1), a.NewString("a"), a.NewTrue(), a.NewNull(),
	}
	eqValues := [][]byte{
		nil,
		{},
		{byte(anyenc.TypeNumber)},          // truncated number
		{byte(anyenc.TypeNumber), 1, 2, 3}, // short number
		append(a.NewNumberFloat64(1).MarshalTo(nil), 0xff), // oversized number
		{byte(anyenc.TypeTrue), 0xff},                      // oversized bool
		{byte(anyenc.TypeNull), 0x01},                      // oversized null
	}
	ops := []CompOp{CompOpEq, CompOpNe, CompOpGt, CompOpGte, CompOpLt, CompOpLte}
	buf := &syncpool.DocBuffer{}
	refBuf := &syncpool.DocBuffer{}
	for _, ev := range eqValues {
		for _, op := range ops {
			cmp := &Comp{CompOp: op, EqValue: ev}
			for _, probe := range probes {
				want := compReference(cmp, probe, refBuf)
				if got := cmp.okScalar(probe, buf); got != want {
					t.Errorf("op=%d eq=%x probe=%s: okScalar=%v, reference=%v", op, ev, probe, got, want)
				}
			}
		}
	}
}
