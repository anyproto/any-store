package query

import (
	"bytes"
	"math"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// compReference is the pre-fast-path implementation: always marshal the probe
// and bytes.Compare against EqValue. okScalar must agree with it bit-for-bit
// for every operator and every value pairing.
//
// It carries one deliberate divergence from raw bytes.Compare: Rule V.
// The vector values stay in the matrix below precisely so the divergence stays
// pinned — a vector is not a point on the scalar order, so an ordering op
// against one is false on either side rather than resolving on the type tag.
func compReference(e *Comp, v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	if e.isOrderingOp() && (v.Type() == anyenc.TypeVectorF32 || e.eqIsVector()) {
		return false
	}
	buf.SmallBuf = v.MarshalTo(buf.SmallBuf[:0])
	return e.compResult(bytes.Compare(e.EqValue, buf.SmallBuf))
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
			// nil probe (absent field) must equal comparing against encoded null —
			// except under Rule V, where an ordering op against a vector operand is
			// false. This row IS the bug: null's tag (1) sorts below a vector's (10),
			// so {"absentField":{"$lt":{"$vector":[..]}}} used to match every document.
			wantNil := cmp.compResult(bytes.Compare(cmp.EqValue, valueNull.MarshalTo(nil)))
			if cmp.isOrderingOp() && cmp.eqIsVector() {
				wantNil = false
			}
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
				if !e.comp(buf.SmallBuf) {
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
			if e.comp(buf.SmallBuf) {
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
