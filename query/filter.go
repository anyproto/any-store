package query

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
	"unsafe"
)

// Filter is built once (ParseCondition or a constructor) and is immutable
// afterwards: it may be shared by any number of queries running concurrently.
// Ok and IndexBounds never mutate filter state and need no synchronization;
// bound bytes returned by IndexBounds that alias filter memory are clipped to
// cap == len so downstream appends reallocate instead of writing into the
// shared filter. Pinned by TestFilterConcurrentReuse and
// TestIndexBounds_FilterOwnedBytesAreCapped; see docs/query-filter-contract.md.
type Filter interface {
	// Ok evaluates whether the given value satisfies the filter condition.
	// docBuf is optional and can be nil, it's used to reuse memory between
	// calls: with a warm per-query DocBuffer, Ok is alloc-free
	// (TestFilterOkAllocFree).
	Ok(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool

	// IndexBounds appends bounds to the bs for the given field if it is applicable
	IndexBounds(fieldName string, bs Bounds) (bounds Bounds)

	fmt.Stringer
}

type CompOp uint8

var orExpressionLimit = 10000

const (
	CompOpEq CompOp = iota
	CompOpGt
	CompOpGte
	CompOpLt
	CompOpLte
	CompOpNe
)

func NewComp(op CompOp, value any) *Comp {
	return &Comp{
		EqValue: anyenc.AppendAnyValue(nil, value),
		CompOp:  op,
	}
}

func NewCompValue(op CompOp, value *anyenc.Value) *Comp {
	return &Comp{
		EqValue: value.MarshalTo(nil),
		CompOp:  op,
	}
}

type Comp struct {
	EqValue  []byte
	CompOp   CompOp
	notArray bool
}

var valueNull = anyenc.MustParseJson("null")

var encodedNull = []byte{byte(anyenc.TypeNull)}

// isOrderingOp reports whether the op places its operands on the scalar order.
// $eq/$ne do not: they are byte equality, which is well-defined for a vector.
func (e *Comp) isOrderingOp() bool {
	switch e.CompOp {
	case CompOpGt, CompOpGte, CompOpLt, CompOpLte:
		return true
	}
	return false
}

// eqIsVector reports whether the filter operand is a packed TypeVectorF32 value.
func (e *Comp) eqIsVector() bool {
	return len(e.EqValue) > 0 && e.EqValue[0] == byte(anyenc.TypeVectorF32)
}

// Rule V (BUG-32): a vector is not a point on the scalar order.
//
// anyenc resolves a cross-type comparison on the type tag, and TypeVectorF32 (10)
// sorts above every scalar tag (1..9). So an ordering op with a vector on either
// side used to be true for EVERY document — {"v":{"$gt":1}} on a vector field, and
// symmetrically {"anyField":{"$lt":{"$vector":[..]}}} on any field at all, even an
// absent one. On Query.Delete that emptied the collection with err=nil, no vector
// index required. $eq/$ne are unaffected: byte equality is well-defined for a vector.
//
// The parser rejects the operand-side spelling outright (ErrVectorNotOrderable);
// these guards are what protect hand-built filters, which never see the parser.
func (e *Comp) Ok(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	// Operand side. Must precede both the nil check and the array branch below:
	// e.comp(encodedNull) would make $lt true for an ABSENT field (null's tag 1 <
	// vector's tag 10), and the whole-array shortcut (notArray is false on a
	// hand-built Comp) would make it true for an array-valued one.
	if e.isOrderingOp() && e.eqIsVector() {
		return false
	}
	if v == nil {
		return e.comp(encodedNull)
	}

	if v.Type() == anyenc.TypeArray {
		vals, _ := v.Array()
		if e.CompOp == CompOpNe {
			if !e.notArray {
				if docBuf == nil {
					docBuf = &syncpool.DocBuffer{}
				}
				docBuf.SmallBuf = v.MarshalTo(docBuf.SmallBuf[:0])
				if !e.comp(docBuf.SmallBuf) {
					return false
				}
			}
			for _, val := range vals {
				if !e.okScalar(val, docBuf) {
					return false
				}
			}

			return true
		} else {
			if !e.notArray {
				if docBuf == nil {
					docBuf = &syncpool.DocBuffer{}
				}
				docBuf.SmallBuf = v.MarshalTo(docBuf.SmallBuf[:0])
				if e.comp(docBuf.SmallBuf) {
					return true
				}
			}
			for _, val := range vals {
				if e.okScalar(val, docBuf) {
					return true
				}
			}
			return false
		}
	}
	return e.okScalar(v, docBuf)
}

// okScalar compares one non-array value against EqValue, equivalent to
// e.comp(v.MarshalTo(...)) but skipping the marshal whenever the comparison is
// decidable from the value alone: the encoding starts with the type tag, so
// mismatched tags resolve on the first byte; same-type numbers compare as the
// monotonic uint64 the encoding is built from (see encodeFloatBits);
// null/true/false encode as the bare tag. Strings, binary, and nested
// containers fall back to marshal + bytes.Compare.
func (e *Comp) okScalar(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	// Rule V (BUG-32), probe side: the stored value is the vector, as in
	// {"v":{"$gt":1}}. See Comp.Ok for the rationale. Reached for a top-level
	// value and for each element of an array — a vector nested deeper (inside an
	// object, or inside an array compared whole) still resolves on the tag, the
	// same as any other higher-tag value in a container.
	//
	// The eqIsVector disjunct is defense-in-depth: every production caller enters
	// through Comp.Ok, which already rejected the operand side. Keep it — okScalar
	// is unexported and must not become true for a vector if a new caller appears.
	if e.isOrderingOp() && (v.Type() == anyenc.TypeVectorF32 || e.eqIsVector()) {
		return false
	}
	if len(e.EqValue) > 0 {
		switch t := v.Type(); t {
		case anyenc.TypeArray, anyenc.TypeObject:
			// containers need their full encoding — no tag short-circuit:
			// e.g. an array EqValue and an array probe share the tag byte
		case anyenc.TypeNumber:
			if tag := byte(t); e.EqValue[0] != tag {
				return e.compResult(compareBytes(e.EqValue[0], tag))
			}
			if len(e.EqValue) == 1+8 {
				f, _ := v.Float64()
				return e.compResult(compareUint64(
					binary.BigEndian.Uint64(e.EqValue[1:]),
					encodeFloatBits(f),
				))
			}
		case anyenc.TypeNull, anyenc.TypeTrue, anyenc.TypeFalse:
			if tag := byte(t); e.EqValue[0] != tag {
				return e.compResult(compareBytes(e.EqValue[0], tag))
			}
			// single-byte encoding: equal unless EqValue carries extra bytes
			if len(e.EqValue) == 1 {
				return e.compResult(0)
			}
			return e.compResult(1)
		case anyenc.TypeString:
			if tag := byte(t); e.EqValue[0] != tag {
				return e.compResult(compareBytes(e.EqValue[0], tag))
			}
			// Plain (NUL-free) probe strings encode as tag + raw bytes + EOS
			// (0x00 inside a string is escaped as the pair (0x00, 0xFF)), so
			// the comparison can run on raw bytes without marshaling.
			sb, _ := v.StringBytes()
			if bytes.IndexByte(sb, 0) < 0 {
				n := len(e.EqValue)
				if e.CompOp == CompOpEq || e.CompOp == CompOpNe {
					// only equality matters: EqValue must be byte-identical to
					// tag + sb + EOS (any malformed EqValue is simply unequal)
					if n == len(sb)+2 && e.EqValue[n-1] == 0 && bytes.Equal(e.EqValue[1:n-1], sb) {
						return e.compResult(0)
					}
					return e.compResult(1)
				}
				// ordering ops additionally need the EqValue payload to be a
				// plain string (an interior 0x00 means an escape pair or
				// trailing junk): then both payloads are raw strings and the
				// shared EOS terminator preserves order.
				if n >= 2 && e.EqValue[n-1] == 0 {
					payload := e.EqValue[1 : n-1]
					if bytes.IndexByte(payload, 0) < 0 {
						return e.compResult(bytes.Compare(payload, sb))
					}
				}
			}
		default:
			// binary/vector: the tag still decides cross-type comparisons
			if tag := byte(t); e.EqValue[0] != tag {
				return e.compResult(compareBytes(e.EqValue[0], tag))
			}
		}
	}
	if docBuf == nil {
		docBuf = &syncpool.DocBuffer{}
	}
	docBuf.SmallBuf = v.MarshalTo(docBuf.SmallBuf[:0])
	return e.comp(docBuf.SmallBuf)
}

// encodeFloatBits returns the uint64 whose big-endian bytes are exactly
// anyenc.AppendFloat64's encoding of f (sign-flip scheme, -0 normalized to +0),
// so uint64 order == encoded byte order.
func encodeFloatBits(f float64) uint64 {
	bits := math.Float64bits(f)
	if bits&(1<<63) != 0 {
		if bits == 1<<63 { // -0 normalizes to +0's encoding
			return bits
		}
		return ^bits
	}
	return bits | 1<<63
}

func compareBytes(a, b byte) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

func compareUint64(a, b uint64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

// IndexBounds stays a sound over-approximation under Rule V: it may select keys
// that Ok now rejects (an ordering op against a vector selects a tag range and
// then matches nothing), never the reverse. Every plan that uses these bounds
// still runs the residual filter — the covering CountOnly shortcuts require a
// PointLookup with FIXED bounds, and an ordering op is neither — so a stricter
// Ok cannot make a plan return rows it should not.
func (e *Comp) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	// Clip spare capacity: a built Comp is shared by concurrent queries, and
	// the planner extends bound bytes with appends (AdjustBoundsForNonUnique,
	// padReverseBounds). cap == len forces those appends to reallocate instead
	// of writing into EqValue's backing array shared across queries (In gives
	// the same guarantee via inBoundBytes).
	ev := e.EqValue[:len(e.EqValue):len(e.EqValue)]
	switch e.CompOp {
	case CompOpEq:
		return bs.Append(Bound{
			Start:        ev,
			End:          ev,
			StartInclude: true,
			EndInclude:   true,
		})
	case CompOpGt:
		return bs.Append(Bound{
			Start: ev,
		})
	case CompOpGte:
		return bs.Append(Bound{
			Start:        ev,
			StartInclude: true,
		})
	case CompOpLt:
		return bs.Append(Bound{
			End: ev,
		})
	case CompOpLte:
		return bs.Append(Bound{
			End:        ev,
			EndInclude: true,
		})
	case CompOpNe:
		return bs.Append(Bound{
			End: ev,
		}).Append(Bound{
			Start: ev,
		})
	default:
		panic(fmt.Errorf("unexpected comp op: %v", e.CompOp))
	}
}

func (e *Comp) comp(b []byte) bool {
	return e.compResult(bytes.Compare(e.EqValue, b))
}

// compResult maps a bytes.Compare(EqValue, probe) result to the operator's verdict.
func (e *Comp) compResult(comp int) bool {
	switch e.CompOp {
	case CompOpEq:
		return comp == 0
	case CompOpGt:
		return comp < 0
	case CompOpGte:
		return comp <= 0
	case CompOpLt:
		return comp > 0
	case CompOpLte:
		return comp >= 0
	case CompOpNe:
		return comp != 0
	default:
		panic(fmt.Errorf("unexpected comp op: %v", e.CompOp))
	}
}

func (e *Comp) String() string {
	var op string
	switch e.CompOp {
	case CompOpEq:
		op = string(opBytesEq)
	case CompOpGt:
		op = string(opBytesGt)
	case CompOpGte:
		op = string(opBytesGte)
	case CompOpLt:
		op = string(opBytesLt)
	case CompOpLte:
		op = string(opBytesLte)
	case CompOpNe:
		op = string(opBytesNe)
	}
	a := &fastjson.Arena{}
	p := parserPool.Get()
	defer parserPool.Put(p)
	av, _ := p.Parse(e.EqValue)
	val := av.FastJson(a)
	return fmt.Sprintf(`{"%s": %s}`, op, val.String())
}

type Key struct {
	Path []string
	Filter
}

func (e Key) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	return e.Filter.Ok(v.Get(e.Path...), buf)
}

func (e Key) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	if strings.Join(e.Path, ".") == fieldName {
		return e.Filter.IndexBounds(fieldName, bs)
	}
	return bs
}

func (e Key) String() string {
	return fmt.Sprintf(`{"%s": %s}`, strings.Join(e.Path, "."), e.Filter.String())
}

type And []Filter

func (e And) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if !f.Ok(v, buf) {
			return false
		}
	}
	return true
}

// IndexBounds returns a SOUND OVER-APPROXIMATION of the index bounds for this
// conjunction: the bounds of the first conjunct that constrains fieldName. The
// result must be a SUPERSET of the matching set so the index seek (and Iter)
// never miss a doc.
//
// Intersecting conjunct bounds (the original I-04 fix) is UNSOUND for
// ARRAY/multi-key fields: array filter semantics match each conjunct against
// the whole array independently, so a doc matches {$gte:2,$lte:3} when one
// element is >=2 and a DIFFERENT element is <=3 — it need not have any element
// in [2,3]. Narrowing the seek to the intersection drops such docs from both
// Count and Iter (and a FilterIter cannot re-add what the seek skipped). The
// over-approximation here is correct for scalar AND array fields because every
// matching doc has an element satisfying the first conjunct, hence in its
// bounds; a FilterIter re-checks the full conjunction.
//
// The CountOnly fast path skips that FilterIter, so it would over-count when
// these over-approx bounds are a strict superset of the matches. It is gated
// separately: indexCoversFilter rejects a covered field carrying more than one
// predicate, so the fast path is only taken when bounds exactly equal the
// matches (a single In/Eq/range per field). See docs/known-issues.md (I-04).
//
// Consumers that can prove fan-out entries are absent (pk namespace, indexes
// with a scalar-proven multikey flag) — or that only ESTIMATE — get the
// intersected variant from TightIndexBounds (query/tight_bounds.go); this
// method's contract stays over-approximating regardless.
func (e And) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	for _, f := range e {
		if bounds = f.IndexBounds(fieldName, bs); len(bounds) != len(bs) {
			return
		}
	}
	return bs
}

func (e And) String() string {
	var subS []string
	for _, f := range e {
		if f != nil {
			subS = append(subS, f.String())
		}
	}
	return fmt.Sprintf(`{"$and":[%s]}`, strings.Join(subS, ", "))
}

type In struct {
	// Values is the membership set, keyed by anyenc-marshaled value. A
	// built filter is immutable: once handed to a query it may be used by
	// many queries concurrently, so Values must not be mutated (IndexBounds
	// and Ok read it and the pre-sorted view without synchronization).
	// Build In via NewInValue.
	Values map[string]struct{}

	// sorted holds the marshaled values in ascending byte order (== anyenc
	// value order == btree key order), built once by NewInValue. IndexBounds
	// emits bounds in this order so the executor walks the btree
	// monotonically (hot interior pages, leaves touched in sequence) and the
	// per-call sort+merge is skipped — unique point bounds are already
	// disjoint. nil for a hand-constructed In, which falls back to sorting.
	sorted []string

	// numBits holds the numeric members as their encoded uint64 forms (see
	// encodeFloatBits), ascending. Number probes binary-search here instead
	// of marshaling + hashing into Values. Authoritative only when sorted is
	// non-nil (built by NewInValue); a hand-constructed In takes the map path.
	numBits []uint64
}

func NewInValue(values ...*anyenc.Value) In {
	inValues := make(map[string]struct{}, len(values))
	// All values are marshaled into one grow-only arena and the map keys are
	// unsafe.Strings over their arena spans — no per-element key allocation.
	// This is safe because the arena bytes are written once and never
	// mutated: when append grows the arena, keys created earlier keep the
	// previous backing array alive with their bytes intact.
	var arena []byte
	var numBits []uint64
	sorted := make([]string, 0, len(values))
	for _, v := range values {
		start := len(arena)
		arena = v.MarshalTo(arena)
		span := arena[start:]
		key := unsafe.String(unsafe.SliceData(span), len(span))
		if _, dup := inValues[key]; dup {
			arena = arena[:start] // duplicate input value — reuse the span
			continue
		}
		inValues[key] = struct{}{}
		sorted = append(sorted, key)
		if len(span) == 1+8 && span[0] == byte(anyenc.TypeNumber) {
			if numBits == nil {
				numBits = make([]uint64, 0, len(values))
			}
			numBits = append(numBits, binary.BigEndian.Uint64(span[1:]))
		}
	}
	slices.Sort(sorted)
	slices.Sort(numBits)
	return In{
		Values:  inValues,
		sorted:  sorted,
		numBits: numBits,
	}
}

func (e In) Ok(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	if v == nil {
		// $in is an OR of equalities and {"$eq":null} matches a MISSING field
		// (Comp.Ok probes encodedNull for a nil value), so a null member must
		// match here too — otherwise {"$in":[null]} disagrees with {"$eq":null},
		// and Iter drops missing-field docs that IndexBounds' null point bound
		// (a missing field is indexed under TypeNull) lets Count include.
		_, ok := e.Values[string(encodedNull)]
		return ok
	}
	if docBuf == nil {
		docBuf = &syncpool.DocBuffer{}
	}
	if v.Type() == anyenc.TypeArray {
		// Whole-array membership first: $in is an OR of equalities, and the
		// $eq path (Comp.Ok) matches a whole array before falling back to its
		// elements. An empty array can ONLY match this way (it has no
		// elements), and whole arrays are indexed alongside their elements,
		// so IndexBounds/Count already include these matches — without this
		// probe Iter disagreed with Count for $in sets containing an array.
		if e.containsValue(v, docBuf) {
			return true
		}
		arr, _ := v.Array()
		for _, item := range arr {
			if e.containsValue(item, docBuf) {
				return true
			}
		}
		return false
	}
	return e.containsValue(v, docBuf)
}

// containsValue reports membership of one non-array value. Number probes skip
// the marshal + map hash via a binary search over numBits when the In was
// built by NewInValue (sorted non-nil ⇒ numBits is authoritative).
func (e In) containsValue(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	if e.sorted != nil && v.Type() == anyenc.TypeNumber {
		f, _ := v.Float64()
		_, found := slices.BinarySearch(e.numBits, encodeFloatBits(f))
		return found
	}
	// Store the grown buffer back so the marshal is alloc-free from the
	// second document on (Comp.Ok does the same).
	docBuf.SmallBuf = v.MarshalTo(docBuf.SmallBuf[:0])
	_, ok := e.Values[string(docBuf.SmallBuf)]
	return ok
}

func (e In) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	if len(e.Values) >= orExpressionLimit {
		return bs
	}
	// Fast path: no incoming bounds and the pre-sorted key list is intact.
	// The unique point bounds are emitted ascending and are pairwise
	// disjoint, so the result is already in SortAndMerge's canonical form —
	// and the executor gets a monotonic btree access pattern for free.
	if len(bs) == 0 && len(e.sorted) == len(e.Values) {
		result := make(Bounds, 0, len(e.sorted))
		for _, val := range e.sorted {
			b := inBoundBytes(val)
			result = append(result, Bound{
				Start:        b,
				End:          b,
				StartInclude: true,
				EndInclude:   true,
			})
		}
		return result
	}
	result := make(Bounds, len(bs), len(bs)+len(e.Values))
	copy(result, bs)
	for val := range e.Values {
		b := inBoundBytes(val)
		result = append(result, Bound{
			Start:        b,
			End:          b,
			StartInclude: true,
			EndInclude:   true,
		})
	}
	return result.SortAndMerge()
}

// inBoundBytes aliases a map key string's bytes for use as a read-only bound.
// Bound bytes are never written in place downstream, and unsafe.Slice yields
// cap == len, so even the planner's bound-extension appends are forced to
// reallocate rather than scribble over the string.
func inBoundBytes(val string) []byte {
	return unsafe.Slice(unsafe.StringData(val), len(val))
}

func (e In) String() string {
	subS := make([]string, len(e.Values))
	i := 0
	a := &fastjson.Arena{}
	p := parserPool.Get()
	defer parserPool.Put(p)
	for k := range e.Values {
		v, _ := p.Parse([]byte(k))
		subS[i] = v.FastJson(a).String()
		i++
	}
	return fmt.Sprintf(`{"$in":[%s]}`, strings.Join(subS, ", "))
}

type Or []Filter

func (e Or) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if f.Ok(v, buf) {
			return true
		}
	}
	return false
}

func (e Or) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	if len(e) > orExpressionLimit {
		return bs
	}
	result := make(Bounds, len(bs), len(bs)+len(e))
	copy(result, bs)
	for _, f := range e {
		beforeLen := len(result)
		result = f.IndexBounds(fieldName, result)
		if len(result) == beforeLen {
			return bs // branch produced no bounds → can't narrow
		}
	}
	return result.SortAndMerge()
}

func (e Or) String() string {
	var subS []string
	for _, f := range e {
		subS = append(subS, f.String())
	}
	return fmt.Sprintf(`{"$or":[%s]}`, strings.Join(subS, ", "))
}

type Nor []Filter

func (e Nor) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	for _, f := range e {
		if f.Ok(v, buf) {
			return false
		}
	}
	return true
}

func (e Nor) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	return bs
}

func (e Nor) String() string {
	var subS []string
	for _, f := range e {
		subS = append(subS, f.String())
	}
	return fmt.Sprintf(`{"$nor":[%s]}`, strings.Join(subS, ", "))
}

type Not struct {
	Filter
}

func (e Not) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	return !e.Filter.Ok(v, buf)
}

func (e Not) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	return bs
}

func (e Not) String() string {
	return fmt.Sprintf(`{"$not": %s}`, e.Filter.String())
}

type All struct{}

func (a All) Ok(_ *anyenc.Value, buf *syncpool.DocBuffer) bool {
	return true
}

func (a All) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	return bs
}

func (a All) String() string {
	return "null"
}

// Text is the {"$text":{"$search":"..."}} full-text predicate. Matching and
// ranking are performed by the full-text index (the query is driven by an FTS
// scan), so as a residual predicate Ok always passes: any document the FTS scan
// yields has already matched. It contributes no index bounds.
//
// CAUTION: this makes Text meaningful only inside an FTS-driven query. Used
// standalone — calling Ok directly to post-filter documents, as
// subscription-style consumers of this package do — a $text condition silently
// matches EVERY document; Ok never evaluates the search text. Placement rules
// ($text only at the top level or inside $and, at most one per query) are also
// enforced by the executor, not here. See docs/fts/DESIGN.md.
type Text struct {
	// Search is the raw $search string, preserved verbatim for String() and as
	// the fallback source when Clauses is nil (a directly-constructed Text).
	Search string
	// DefaultAnd is set by {"$defaultOperator":"and"}: bare (should) terms then
	// become required (AND) rather than the default OR.
	DefaultAnd bool
	// Clauses are the parsed clauses of the whole $text predicate: the should
	// clauses from $search (phrases, prefix*, plain terms) plus the required and
	// excluded clauses from the $require / $exclude sub-fields. Populated by
	// ParseCondition; a Text built directly with only Search set has nil Clauses
	// and the executor re-parses Search on demand (yielding should clauses only).
	Clauses []TextClause
}

// TextOp classifies a $text clause's boolean role within the query.
type TextOp uint8

const (
	// TextShould is a default clause: OR'd with the other should-clauses, unless
	// the query sets DefaultAnd, in which case it is required.
	TextShould TextOp = iota
	// TextMust is a +term: always required (AND), regardless of DefaultAnd.
	TextMust
	// TextMustNot is a -term: documents containing it are excluded.
	TextMustNot
)

// TextClause is one parsed unit of a $text $search string: a single term, a
// quoted phrase, or a prefix term, together with its boolean role. The raw text
// is NOT analyzed here — the query package stays free of the fts analyzer; the
// FTS executor analyzes each clause with the index's own pipeline.
type TextClause struct {
	// Raw is the unanalyzed clause text: a single word, or the inner text of a
	// "quoted phrase".
	Raw string
	// Phrase marks a "..." clause: Raw analyzes to an ordered term list matched
	// by positional adjacency.
	Phrase bool
	// Prefix marks a trailing-* clause (foo*): Raw's single analyzed term is
	// expanded against the vocabulary. Mutually exclusive with Phrase.
	Prefix bool
	// Op is the clause's boolean role.
	Op TextOp
}

// ParsedClauses returns the parsed clauses, parsing Search on demand for a Text
// that was constructed directly (Clauses nil) rather than via ParseCondition.
func (t Text) ParsedClauses() []TextClause {
	if t.Clauses != nil {
		return t.Clauses
	}
	return ParseTextSearch(t.Search)
}

func (t Text) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	return true
}

func (t Text) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	return bs
}

func (t Text) String() string {
	return fmt.Sprintf(`{"$text":{"$search":%q}}`, t.Search)
}

// Knn is the {"<field>":{"$knn":{"$query":[…],"$k":N}}} approximate-nearest-neighbour
// clause. It is NOT a predicate: it selects the K documents whose vector at this
// field the field's vector index ranks closest to Query. "Is this document one of
// the k nearest?" is a property of the CANDIDATE SET, not of the document, so no
// per-document truth value exists.
//
// Ok therefore returns FALSE, unconditionally, and is unreachable in a correct
// plan: the executor detects the clause on EVERY verb, drives the query from the
// ANN source, strips the clause from the residual, and hard-errors on any
// placement where it could not be stripped. The false FAILS CLOSED — a Knn that
// somehow reaches a document-by-document evaluator matches nothing rather than
// everything. Contrast query.Text, whose Ok returns TRUE: fail-open on
// Query.Delete costs the collection. Never do that here.
//
// FAIL-CLOSED IS LOAD-BEARING IN TWO PLACES, both of which must be kept in sync:
//   - ContainsKnn must walk Not/Nor: Not{Knn}.Ok == !false == MATCH-ALL. A $knn
//     that reaches a Not is a mass delete through the front door, reachable from
//     Query.Delete.
//   - GuaranteesPresence must special-case Knn: it calls the inner filter's Ok
//     DIRECTLY and reads !Ok(nil) && !Ok(null) as "guarantees presence" — so a
//     fail-closed Ok yields TRUE, the AGGRESSIVE answer.
type Knn struct {
	Query []float32 // non-empty, finite; len checked against the index dim at detection
	K     int       // required, 1..KnnMaxK
	Ef    int       // 0 = auto; candidate/beam depth (numCandidates); >= K when set
	Index string    // optional: vector index name (disambiguates 2 indexes on one field)
}

const (
	// KnnMaxK bounds $k. Policy, not semantics: raising it later is
	// non-breaking, lowering it is not — start conservative.
	KnnMaxK = 10_000
	// KnnMaxEf bounds $ef, the ANN candidate/beam depth.
	KnnMaxEf = 65_536
)

// Ok fails closed: a Knn is not a predicate (see the type comment).
func (k Knn) Ok(*anyenc.Value, *syncpool.DocBuffer) bool { return false }

// IndexBounds returns bs verbatim: a Knn clause contributes no range-index
// bounds. Verbatim matters — Or.IndexBounds detects "this branch contributed
// nothing" by comparing lengths, and a shorter return silently discards
// accumulated sibling bounds.
func (k Knn) IndexBounds(_ string, bs Bounds) Bounds { return bs }

// String renders the clause losslessly: MustParseCondition round-trips it.
func (k Knn) String() string {
	var b strings.Builder
	b.WriteString(`{"$knn":{"$query":[`)
	for i, f := range k.Query {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(f), 'g', -1, 32))
	}
	b.WriteString(`],"$k":`)
	b.WriteString(strconv.Itoa(k.K))
	if k.Ef != 0 {
		b.WriteString(`,"$ef":`)
		b.WriteString(strconv.Itoa(k.Ef))
	}
	if k.Index != "" {
		b.WriteString(`,"$index":`)
		b.WriteString(strconv.Quote(k.Index))
	}
	b.WriteString(`}}`)
	return b.String()
}

// Validate checks the $knn argument rules — the ONE copy shared by the parser
// (parseKnn, after its shape/presence checks) and the executor's detection
// walk (which re-validates because programmatic NewKnn filters never see the
// parser). Message texts are the normative parse-error strings.
func (k Knn) Validate() error {
	if len(k.Query) == 0 {
		return errors.New("$knn: $query must be non-empty")
	}
	for _, f := range k.Query {
		if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
			return errors.New("$knn: $query must contain finite numbers")
		}
	}
	if k.K < 1 || k.K > KnnMaxK {
		return fmt.Errorf("$knn: $k must be an integer in [1, %d], got %d", KnnMaxK, k.K)
	}
	if k.Ef != 0 && (k.Ef < k.K || k.Ef > KnnMaxEf) {
		return fmt.Errorf("$knn: $ef must be an integer in [$k, %d], got %d", KnnMaxEf, k.Ef)
	}
	return nil
}

// KnnOpt is an option for NewKnn.
type KnnOpt func(*Knn)

// KnnEf sets the explicit ANN candidate/beam depth ($ef).
func KnnEf(ef int) KnnOpt { return func(k *Knn) { k.Ef = ef } }

// KnnIndex names the vector index to search ($index) — required only when the
// field has more than one vector index.
func KnnIndex(name string) KnnOpt { return func(k *Knn) { k.Index = name } }

// NewKnn is the programmatic constructor for the $knn clause: wrap it in a
// query.Key naming the vector field. It exists because programmatic consumers
// build their ANN filter as a Go value and never touch JSON; every validation
// the parser applies is re-checked at query build time (detection), which is
// the only validation a hand-built filter ever sees.
func NewKnn(vec []float32, k int, opts ...KnnOpt) Knn {
	kn := Knn{Query: vec, K: k}
	for _, o := range opts {
		o(&kn)
	}
	return kn
}

// FilterTreeAny walks the standard filter tree and reports whether pred is
// true for any node. It descends And/Or/Nor/Not/Key — in BOTH their value and
// pointer forms: every composite here has value-receiver methods, so a
// pointer-built &Not{…}/&Key{…}/&Or{…} satisfies Filter too, and a walker that
// switches only on value types silently skips such nodes. That is not a
// stylistic gap — ContainsKnn is a mass-delete guard (see Knn), and a missed
// node is a guard bypass.
func FilterTreeAny(f Filter, pred func(Filter) bool) bool {
	if f == nil {
		return false
	}
	if pred(f) {
		return true
	}
	switch ft := f.(type) {
	case And:
		return anyFilterTree(ft, pred)
	case *And:
		return anyFilterTree(*ft, pred)
	case Or:
		return anyFilterTree(ft, pred)
	case *Or:
		return anyFilterTree(*ft, pred)
	case Nor:
		return anyFilterTree(ft, pred)
	case *Nor:
		return anyFilterTree(*ft, pred)
	case Not:
		return FilterTreeAny(ft.Filter, pred)
	case *Not:
		return FilterTreeAny(ft.Filter, pred)
	case Key:
		return FilterTreeAny(ft.Filter, pred)
	case *Key:
		return FilterTreeAny(ft.Filter, pred)
	}
	return false
}

func anyFilterTree(fs []Filter, pred func(Filter) bool) bool {
	for _, f := range fs {
		if FilterTreeAny(f, pred) {
			return true
		}
	}
	return false
}

func isKnnLeaf(f Filter) bool {
	switch f.(type) {
	case Knn, *Knn:
		return true
	}
	return false
}

func isSourceLeaf(f Filter) bool {
	switch f.(type) {
	case Knn, *Knn, Text, *Text:
		return true
	}
	return false
}

// ContainsKnn reports whether the tree contains a Knn ANYWHERE. It MUST walk
// the whole tree, pointer nodes included: a Knn under Not would evaluate
// !false == match-all (see the Knn type comment), so every consumer that
// rejects bad placements needs the full walk.
func ContainsKnn(f Filter) bool {
	return FilterTreeAny(f, isKnnLeaf)
}

// ContainsSourceFilter reports whether the tree contains a SOURCE filter — one
// matched by an index (Text, Knn) rather than by Ok. RECURSIVE, not a shallow
// type check: the only legal shapes are Key{path, Knn} and And{Text, …}, so a
// shallow test would return false for every real query and protect nobody.
// External consumers that post-filter by calling Filter.Ok directly (the
// subscription pattern) MUST reject these: Text.Ok matches everything, Knn.Ok
// matches nothing.
func ContainsSourceFilter(f Filter) bool {
	return FilterTreeAny(f, isSourceLeaf)
}

// presenceNullProbe is an explicit JSON null used by GuaranteesPresence to test
// whether a predicate rejects a present-but-null value, as distinct from a
// missing field (which probes as a nil *anyenc.Value).
var presenceNullProbe = anyenc.MustParseJson("null")

// GuaranteesPresence reports whether every document matching f must carry a
// present, non-null value at fieldName. It is true only when some top-level
// conjunct constrains fieldName with a predicate that rejects BOTH a missing
// field (a nil value) and an explicit null — exactly the two cases a sparse
// index omits (see index.writeValues: it drops a key when any indexed field is
// nil or TypeNull).
//
// The planner uses this to decide whether a SPARSE index can represent the whole
// matching set: a sparse index that omits some matching document would silently
// drop rows if seeked. Probing the field's own sub-filter with Ok keeps this in
// lockstep with match semantics, so it is exact rather than a rule-of-thumb —
// e.g. {$exists:true} yields false here because an explicit-null document
// matches $exists:true yet is absent from the sparse index. An OR, a
// $exists:false, a $ne, an equality-to-null, an $in containing null, or no
// predicate on the field at all all yield false, keeping that sparse index out
// of consideration (the planner then falls back to a complete index or scan).
func GuaranteesPresence(f Filter, fieldName string) bool {
	switch ft := f.(type) {
	case Key:
		// Source filters (Knn, Text) are matched by an index, not by Ok, so
		// their Ok says nothing about presence. Knn is the trap this guard
		// exists for: its fail-closed Ok would probe !false && !false == true —
		// the AGGRESSIVE answer, feeding sparse-index selection with a claim
		// the filter never made. (Text is "safe" only because its Ok fails
		// open; special-case it anyway rather than lean on that accident.)
		if isSourceLeaf(ft.Filter) {
			return false
		}
		if strings.Join(ft.Path, ".") == fieldName {
			var buf syncpool.DocBuffer
			return !ft.Filter.Ok(nil, &buf) && !ft.Filter.Ok(presenceNullProbe, &buf)
		}
		return false
	case And:
		for _, sub := range ft {
			if GuaranteesPresence(sub, fieldName) {
				return true
			}
		}
		return false
	case *And:
		return GuaranteesPresence(*ft, fieldName)
	default:
		return false
	}
}

type Exists struct{}

func (e Exists) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	return v != nil
}

func (e Exists) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	return bs
}

func (e Exists) String() string {
	return fmt.Sprintf(`{"$exists": true}`)
}

type TypeFilter struct {
	Type anyenc.Type
}

func (e TypeFilter) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	if v == nil {
		return false
	}
	return v.Type() == e.Type
}

func (e TypeFilter) IndexBounds(fieldName string, bs Bounds) (bounds Bounds) {
	if e.Type == anyenc.TypeVectorF32 {
		// A vector is not a range: contribute no bounds and let the filter do the
		// work. The [tag, tag+1) bound below does not survive inversion — on a
		// DESCENDING index the reverse transform of a tag-10 range selects nothing
		// at all, so Count and Iter both return 0 (measured; drop this guard and
		// TestType_VectorF32_AcrossIndexes fails on every "-v" config). Costs a
		// scan, which is the right price for a type that has no order.
		//
		// This is separate from — and was masked by — the inverted-vector-tag read
		// bug fixed in anyenc/parser.go: that one silently dropped vector-valued
		// rows from ANY reverse-index scan, filter or not.
		return bs
	}
	// Upper bound is the next type tag, exclusive: this covers every key of
	// type e.Type regardless of its payload bytes (an inclusive
	// {tag, 0xff} end under-approximates — a payload whose first byte is
	// 0xff sorts past it).
	return bs.Append(Bound{
		Start:        []byte{byte(e.Type)},
		End:          []byte{byte(e.Type) + 1},
		StartInclude: true,
		EndInclude:   false,
	})
}

func (e TypeFilter) String() string {
	return fmt.Sprintf(`{"$type": "%s"}`, Type(e.Type).String())
}

type Regexp struct {
	Regexp *regexp.Regexp
}

func (r Regexp) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	if v == nil {
		return false
	}
	if v.Type() != anyenc.TypeString && v.Type() != anyenc.TypeArray {
		return false
	}
	if v.Type() == anyenc.TypeArray {
		vals, _ := v.Array()
		for _, val := range vals {
			exp, err := val.StringBytes()
			if err != nil {
				return false
			}
			if r.Regexp.Match(exp) {
				return true
			}
		}
		return false
	}
	exp, err := v.StringBytes()
	if err != nil {
		return false
	}
	return r.Regexp.Match(exp)
}

func (r Regexp) IndexBounds(_ string, bs Bounds) (bounds Bounds) {
	prefix := extractPrefix(r.Regexp.String())
	if prefix == "" {
		return bs
	}
	var (
		prefixBuf     = make([]byte, 0, len(prefix)+2)
		prefixEncoded = anyenc.AppendAnyValue(prefixBuf, prefix)
	)
	// strip the 'eof' byte
	prefixEncoded = prefixEncoded[:len(prefixEncoded)-1]
	bound := Bound{
		Start:        prefixEncoded,
		End:          append(prefixEncoded, 255),
		StartInclude: true,
		EndInclude:   true,
	}
	return bs.Append(bound)
}

func findPrefix(pattern string) string {
	var result []rune
	specialChars := `^$|*+?(){}[]\.`
	escaped := false

	for i := range len(pattern) {
		char := pattern[i]

		if escaped {
			escaped = false
			result = append(result, rune(char))
			continue
		}

		if char == '\\' {
			escaped = true
			continue
		}

		if !isSpecialChar(char, specialChars) {
			result = append(result, rune(char))
		} else {
			break
		}
	}
	var resultString string
	for _, v := range result {
		resultString += string(v)
	}
	return resultString
}

func isSpecialChar(char byte, specialChars string) bool {
	for i := 0; i < len(specialChars); i++ {
		if char == specialChars[i] {
			return true
		}
	}
	return false
}

func extractPrefix(pattern string) string {
	if !strings.HasPrefix(pattern, "^") || strings.HasPrefix(pattern, "^(?i)") {
		return ""
	}
	pattern = strings.TrimPrefix(pattern, "^")
	return findPrefix(pattern)
}

func (r Regexp) String() string {
	return fmt.Sprintf(`{"$regex": "%s"}`, r.Regexp.String())
}

type Size struct {
	Size int64
}

func (s Size) Ok(v *anyenc.Value, buf *syncpool.DocBuffer) bool {
	if v == nil {
		return false
	}
	array, err := v.Array()
	if err != nil {
		return false
	}
	return int64(len(array)) == s.Size
}

func (s Size) IndexBounds(_ string, bs Bounds) (bounds Bounds) {
	return bs
}

func (s Size) String() string {
	return fmt.Sprintf(`{"$size": %d}`, s.Size)
}
