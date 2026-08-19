package aggregate

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
)

// Accumulator folds the per-row values of one $group output field.
//
// Step receives the already-evaluated argument expression (nil = missing);
// the value is transient — valid only during the call — so implementations
// retain data by marshaling into self-owned buffers. Finalize is called once,
// after the upstream drain; results may be allocated on out or on an
// accumulator-owned parser (the stage consumes the result before the next
// emit either way).
type Accumulator interface {
	Step(val *anyenc.Value) error
	Finalize(out *anyenc.Arena) (*anyenc.Value, error)
}

// accFactory creates one accumulator instance for a new group; mem is the
// pipeline run's retained-bytes account.
type accFactory func(mem *MemAccount) Accumulator

// makeAccFactories compiles accumulator specs into per-group instance factories.
func makeAccFactories(specs []AccumSpec, limits Limits) []accFactory {
	res := make([]accFactory, len(specs))
	for i, spec := range specs {
		switch spec.Op {
		case AccumSum:
			res[i] = func(*MemAccount) Accumulator { return &sumAcc{} }
		case AccumAvg:
			res[i] = func(*MemAccount) Accumulator { return &avgAcc{} }
		case AccumCount:
			res[i] = func(*MemAccount) Accumulator { return &countAcc{} }
		case AccumMin:
			res[i] = func(*MemAccount) Accumulator { return &minMaxAcc{} }
		case AccumMax:
			res[i] = func(*MemAccount) Accumulator { return &minMaxAcc{isMax: true} }
		case AccumFirst:
			res[i] = func(*MemAccount) Accumulator { return &firstLastAcc{first: true} }
		case AccumLast:
			res[i] = func(*MemAccount) Accumulator { return &firstLastAcc{} }
		case AccumPush:
			res[i] = func(mem *MemAccount) Accumulator {
				return &pushAcc{maxLen: limits.MaxAccumArrayLen, mem: mem}
			}
		case AccumAddToSet:
			res[i] = func(mem *MemAccount) Accumulator {
				return &pushAcc{maxLen: limits.MaxAccumArrayLen, mem: mem, seen: make(map[string]struct{})}
			}
		default:
			panic(fmt.Errorf("aggregate: unknown accumulator op: %d", spec.Op))
		}
	}
	return res
}

// sumAcc implements $sum: IEEE 754 float64 addition over numeric inputs;
// non-numbers and missing are ignored; an empty sum is 0 (Mongo semantics).
type sumAcc struct {
	sum float64
}

func (a *sumAcc) Step(val *anyenc.Value) error {
	if val != nil && val.Type() == anyenc.TypeNumber {
		a.sum += val.GetFloat64()
	}
	return nil
}

func (a *sumAcc) Finalize(out *anyenc.Arena) (*anyenc.Value, error) {
	return out.NewNumberFloat64(a.sum), nil
}

// avgAcc implements $avg over numeric inputs; null when no numbers were seen.
type avgAcc struct {
	sum float64
	n   int
}

func (a *avgAcc) Step(val *anyenc.Value) error {
	if val != nil && val.Type() == anyenc.TypeNumber {
		a.sum += val.GetFloat64()
		a.n++
	}
	return nil
}

func (a *avgAcc) Finalize(out *anyenc.Arena) (*anyenc.Value, error) {
	if a.n == 0 {
		return out.NewNull(), nil
	}
	return out.NewNumberFloat64(a.sum / float64(a.n)), nil
}

// countAcc implements the $count accumulator: the number of rows in the group.
type countAcc struct {
	n int
}

func (a *countAcc) Step(*anyenc.Value) error {
	a.n++
	return nil
}

func (a *countAcc) Finalize(out *anyenc.Arena) (*anyenc.Value, error) {
	return out.NewNumberInt(a.n), nil
}

// minMaxAcc implements $min / $max in anyenc value order (byte order of the
// marshaled encoding). Null and missing inputs are ignored (Mongo); if no
// comparable value was seen the result is null. Retention is two swapping
// buffers: a candidate is marshaled into scratch and the buffers swap only
// when the best value changes — no copy on the keep path.
type minMaxAcc struct {
	isMax   bool
	cur     []byte // marshaled best value; nil until the first comparable input
	scratch []byte
	p       anyenc.Parser
}

func (a *minMaxAcc) Step(val *anyenc.Value) error {
	if val == nil || val.Type() == anyenc.TypeNull {
		return nil
	}
	a.scratch = val.MarshalTo(a.scratch[:0])
	if a.cur == nil {
		a.cur, a.scratch = a.scratch, nil
		return nil
	}
	c := bytes.Compare(a.scratch, a.cur)
	if (a.isMax && c > 0) || (!a.isMax && c < 0) {
		a.cur, a.scratch = a.scratch, a.cur
	}
	return nil
}

func (a *minMaxAcc) Finalize(out *anyenc.Arena) (*anyenc.Value, error) {
	if a.cur == nil {
		return out.NewNull(), nil
	}
	// The parser is owned by this accumulator and used exactly once, so the
	// parsed value can be returned without a copy.
	return a.p.Parse(a.cur)
}

// firstLastAcc implements $first / $last. Missing values count: $first of a
// row without the field yields missing (the output field is omitted), and
// $last reflects the final row whatever it held. $last rewrites one buffer
// per row (FillCopy pattern, no growth).
type firstLastAcc struct {
	first   bool
	set     bool
	missing bool
	raw     []byte
	p       anyenc.Parser
}

func (a *firstLastAcc) Step(val *anyenc.Value) error {
	if a.first && a.set {
		return nil
	}
	a.set = true
	if val == nil {
		a.missing = true
		return nil
	}
	a.missing = false
	a.raw = val.MarshalTo(a.raw[:0])
	return nil
}

func (a *firstLastAcc) Finalize(out *anyenc.Arena) (*anyenc.Value, error) {
	if !a.set || a.missing {
		return nil, nil
	}
	return a.p.Parse(a.raw)
}

// pushAcc implements $push and, when seen != nil, $addToSet. Values are
// retained as marshaled spans in one packed grow-only buffer; $addToSet
// dedups via unsafe.String keys over the spans (the query.In pattern — safe
// because spans are written once and old backing arrays stay alive under
// their keys when the buffer grows).
type pushAcc struct {
	buf    []byte
	offs   []uint32
	maxLen int
	mem    *MemAccount
	seen   map[string]struct{} // nil for $push
	p      anyenc.Parser
}

func (a *pushAcc) Step(val *anyenc.Value) error {
	if val == nil {
		return nil // missing values are skipped; null is kept
	}
	start := len(a.buf)
	a.buf = val.MarshalTo(a.buf)
	if a.seen != nil {
		span := a.buf[start:]
		key := unsafe.String(unsafe.SliceData(span), len(span))
		if _, dup := a.seen[key]; dup {
			a.buf = a.buf[:start]
			return nil
		}
		a.seen[key] = struct{}{}
	}
	if a.maxLen > 0 && len(a.offs) >= a.maxLen {
		return ErrAccumArrayLimitExceeded
	}
	a.offs = append(a.offs, uint32(start))
	return a.mem.Add(len(a.buf) - start)
}

func (a *pushAcc) Finalize(out *anyenc.Arena) (*anyenc.Value, error) {
	arr := out.NewArray()
	for i, off := range a.offs {
		end := len(a.buf)
		if i+1 < len(a.offs) {
			end = int(a.offs[i+1])
		}
		v, err := a.p.Parse(a.buf[off:end])
		if err != nil {
			return nil, err
		}
		// The parser is reused per element, so each parsed value is copied
		// onto the output arena to keep all elements alive together.
		arr.SetArrayItem(i, anyencutil.Copy(out, v))
	}
	return arr, nil
}
