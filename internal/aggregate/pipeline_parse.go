package aggregate

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/parser"
	"github.com/anyproto/any-store/v2/query"
)

// Pipeline is a parsed aggregation pipeline: an ordered list of stage specs.
type Pipeline []StageSpec

// StageSpec is a parsed pipeline stage description (not yet an executable
// Stage; see Build).
type StageSpec interface {
	fmt.Stringer
	stageSpec()
}

// MatchSpec is a parsed $match stage: the ordinary query-filter part and the
// $expr predicates, AND-ed together. Filter is index-eligible when the stage
// sits in the pushdown prefix; Exprs are always residual per-document
// predicates (Mongo semantics) — they never become index bounds.
type MatchSpec struct {
	Filter query.Filter // nil when the spec is pure $expr (or empty)
	Exprs  []Expr       // nil when the spec has no $expr
}

type SortSpec struct {
	Sort   query.Sorts
	Fields []query.SortField // parsed order, for String()/pushdown checks
}

type SkipSpec struct{ N int }

type LimitSpec struct{ N int }

type CountSpec struct{ Field string }

// ProjectField is one output field of $project / $addFields.
type ProjectField struct {
	Name string
	Expr Expr
}

type ProjectSpec struct{ Fields []ProjectField }

type AddFieldsSpec struct{ Fields []ProjectField }

type UnwindSpec struct {
	Field                      string // original spelling without "$"
	Path                       []string
	PreserveNullAndEmptyArrays bool
}

// AccumOp identifies a $group accumulator operator.
type AccumOp uint8

const (
	AccumSum AccumOp = iota
	AccumAvg
	AccumMin
	AccumMax
	AccumCount
	AccumFirst
	AccumLast
	AccumPush
	AccumAddToSet
)

var accumOpNames = [...]string{"$sum", "$avg", "$min", "$max", "$count", "$first", "$last", "$push", "$addToSet"}

func (op AccumOp) String() string { return accumOpNames[op] }

// AccumSpec is one accumulator of a $group stage. Arg is nil for $count.
type AccumSpec struct {
	Name string
	Op   AccumOp
	Arg  Expr
}

type GroupSpec struct {
	Key    Expr
	Accums []AccumSpec
}

// FacetSpec is a parsed $facet stage: named sub-pipelines fanned out over one
// shared input stream. Names and Pipelines are parallel, in spec order.
type FacetSpec struct {
	Names     []string
	Pipelines []Pipeline
}

// LookupSpec is a parsed $lookup stage, scoped to a self-join point lookup on
// the primary key: From (optional) must name the aggregated collection itself
// — the parser doesn't know that name, so the root package validates it at
// execution setup — and the foreign field is always the primary key "id"
// (enforced at parse time).
type LookupSpec struct {
	From       string // "" = self-join implied
	LocalField string
	LocalPath  []string
	As         string
}

func (MatchSpec) stageSpec()     {}
func (SortSpec) stageSpec()      {}
func (SkipSpec) stageSpec()      {}
func (LimitSpec) stageSpec()     {}
func (CountSpec) stageSpec()     {}
func (ProjectSpec) stageSpec()   {}
func (AddFieldsSpec) stageSpec() {}
func (UnwindSpec) stageSpec()    {}
func (GroupSpec) stageSpec()     {}
func (LookupSpec) stageSpec()    {}
func (FacetSpec) stageSpec()     {}

func (s MatchSpec) String() string {
	parts := make([]string, 0, 1+len(s.Exprs))
	if s.Filter != nil {
		parts = append(parts, s.Filter.String())
	}
	for _, e := range s.Exprs {
		parts = append(parts, fmt.Sprintf(`{"$expr":%s}`, e))
	}
	switch len(parts) {
	case 0:
		return "$match {}"
	case 1:
		return "$match " + parts[0]
	default:
		return fmt.Sprintf(`$match {"$and":[%s]}`, strings.Join(parts, ", "))
	}
}

func (s SortSpec) String() string {
	var b strings.Builder
	b.WriteString("$sort {")
	for i, f := range s.Fields {
		if i > 0 {
			b.WriteByte(',')
		}
		dir := 1
		if f.Reverse {
			dir = -1
		}
		fmt.Fprintf(&b, "%q:%d", f.Field, dir)
	}
	b.WriteByte('}')
	return b.String()
}

func (s SkipSpec) String() string  { return fmt.Sprintf("$skip %d", s.N) }
func (s LimitSpec) String() string { return fmt.Sprintf("$limit %d", s.N) }
func (s CountSpec) String() string { return fmt.Sprintf("$count %q", s.Field) }

func projectFieldsString(name string, fields []ProjectField) string {
	var b strings.Builder
	b.WriteString(name)
	b.WriteString(" {")
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%q:%s", f.Name, f.Expr)
	}
	b.WriteByte('}')
	return b.String()
}

func (s ProjectSpec) String() string   { return projectFieldsString("$project", s.Fields) }
func (s AddFieldsSpec) String() string { return projectFieldsString("$addFields", s.Fields) }

func (s UnwindSpec) String() string {
	if s.PreserveNullAndEmptyArrays {
		return fmt.Sprintf("$unwind {path:%q, preserveNullAndEmptyArrays:true}", "$"+s.Field)
	}
	return fmt.Sprintf("$unwind %q", "$"+s.Field)
}

func (s GroupSpec) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "$group {id:%s", s.Key)
	for _, a := range s.Accums {
		fmt.Fprintf(&b, ",%q:{%s:%s}", a.Name, a.Op, exprOrEmpty(a.Arg))
	}
	b.WriteByte('}')
	return b.String()
}

func (s LookupSpec) String() string {
	var b strings.Builder
	b.WriteString("$lookup {")
	if s.From != "" {
		fmt.Fprintf(&b, "%q:%q,", "from", s.From)
	}
	fmt.Fprintf(&b, `"localField":%q,"foreignField":"id","as":%q}`, s.LocalField, s.As)
	return b.String()
}

func (s FacetSpec) String() string {
	var b strings.Builder
	b.WriteString("$facet {")
	for i, name := range s.Names {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%q:[", name)
		for j := range s.Pipelines[i] {
			if j > 0 {
				b.WriteString(", ")
			}
			b.WriteString(stageString(s.Pipelines[i], j))
		}
		b.WriteByte(']')
	}
	b.WriteByte('}')
	return b.String()
}

func exprOrEmpty(e Expr) string {
	if e == nil {
		return "{}"
	}
	return e.String()
}

// MustParsePipeline is ParsePipeline that panics on error (tests).
func MustParsePipeline(pipeline any) Pipeline {
	p, err := ParsePipeline(pipeline)
	if err != nil {
		panic(err)
	}
	return p
}

// stageParsers is the pipeline grammar's stage vocabulary as data: the single
// source of truth for stage recognition. parseStage dispatches through it and
// Stages exports it — so the parser, its errors, and the advertised
// vocabulary cannot drift apart.
var stageParsers = map[string]func(*anyenc.Value) (StageSpec, error){
	"$match":     parseMatch,
	"$sort":      parseSortStage,
	"$skip":      parseSkip,
	"$limit":     parseLimit,
	"$count":     parseCount,
	"$project":   parseProject,
	"$addFields": parseAddFields,
	"$set":       parseAddFields, // alias
	"$unwind":    parseUnwind,
	"$group":     parseGroup,
	"$lookup":    parseLookup,
}

// $facet is registered in init: parseFacet recurses through parseStage, which
// reads stageParsers — a literal entry would be an initialization cycle.
func init() {
	stageParsers["$facet"] = parseFacet
}

// Stages returns the stage vocabulary accepted by the pipeline parser —
// every stage key ParsePipeline recognizes ($set is $addFields' alias),
// sorted and with the leading '$'. The slice is a fresh copy: callers may
// keep or mutate it. Use it to advertise the grammar (docs, error payloads)
// instead of hand-copying the list.
func Stages() []string {
	res := make([]string, 0, len(stageParsers))
	for name := range stageParsers {
		res = append(res, name)
	}
	sort.Strings(res)
	return res
}

// Accumulators returns the $group accumulator vocabulary, sorted and with the
// leading '$'. The slice is a fresh copy: callers may keep or mutate it.
func Accumulators() []string {
	res := append([]string(nil), accumOpNames[:]...)
	sort.Strings(res)
	return res
}

// ParsePipeline parses a JSON-ish aggregation pipeline (any input accepted by
// the same parser as Find conditions) into stage specs. The result is fully
// detached from the input value. Rejections are reported as *query.ParseError
// with Source "pipeline", whose Path's leading segment is the stage index
// ("1.$match.a.$gt"); see query.ParseError.
func ParsePipeline(pipeline any) (Pipeline, error) {
	if pipeline == nil {
		return nil, nil // empty pipeline: plain passthrough scan
	}
	if p, ok := pipeline.(Pipeline); ok {
		return p, nil
	}
	v, err := parser.Parse(pipeline)
	if err != nil {
		return nil, err
	}
	if v.Type() != anyenc.TypeArray {
		return nil, &query.ParseError{Source: "pipeline", Reason: "pipeline must be an array of stages"}
	}
	arr, _ := v.Array()
	res := make(Pipeline, 0, len(arr))
	for i, el := range arr {
		spec, err := parseStage(el)
		if err != nil {
			return nil, withSource(atPath(err, strconv.Itoa(i)), "pipeline")
		}
		res = append(res, spec)
	}
	return res, nil
}

func parseStage(v *anyenc.Value) (StageSpec, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, &query.ParseError{Reason: "stage must be an object"}
	}
	if obj.Len() != 1 {
		return nil, &query.ParseError{Reason: fmt.Sprintf("stage must have exactly one key, got %d", obj.Len())}
	}
	var (
		spec StageSpec
		serr error
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		parse, ok := stageParsers[string(key)]
		if !ok {
			serr = atPath(&query.ParseError{
				Op:     string(key),
				Reason: "unknown stage: " + string(key),
				Err:    query.ErrUnknownOperator,
			}, string(key))
			return
		}
		if spec, serr = parse(val); serr != nil {
			serr = atPath(serr, string(key))
		}
	})
	return spec, serr
}

// parseMatch parses a $match spec. $expr predicates (aggregation expressions
// used as per-document conditions, Mongo semantics) are intercepted here — the
// query package's filter grammar never learns about them. $expr is accepted at
// the top level of the spec and inside top-level $and arrays (a conjunction
// splits cleanly into a filter part and an expression part); under $or/$nor,
// where no such split exists, it is rejected with a dedicated parse error.
func parseMatch(v *anyenc.Value) (StageSpec, error) {
	if v.Type() == anyenc.TypeObject && filterHasExpr(v) {
		return splitMatchExpr(v)
	}
	f, err := query.ParseCondition(v)
	if err != nil {
		// Already a *query.ParseError with a filter-relative Path; parseStage
		// and ParsePipeline prefix "$match" and the stage index onto it.
		return nil, err
	}
	return MatchSpec{Filter: f}, nil
}

// filterHasExpr reports whether a filter object carries "$expr" at a filter
// position: its top level or the elements of $and/$or/$nor arrays. Field
// condition values are never descended into — a nested "$expr" key there is
// plain equality data, not an operator.
func filterHasExpr(v *anyenc.Value) bool {
	if v.Type() != anyenc.TypeObject {
		return false
	}
	obj, _ := v.Object()
	var found bool
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if found {
			return
		}
		switch string(key) {
		case "$expr":
			found = true
		case "$and", "$or", "$nor":
			found = arrayHasExpr(val)
		}
	})
	return found
}

func arrayHasExpr(v *anyenc.Value) bool {
	if v.Type() != anyenc.TypeArray {
		return false
	}
	arr, _ := v.Array()
	for _, el := range arr {
		if filterHasExpr(el) {
			return true
		}
	}
	return false
}

// splitMatchExpr splits a $match spec containing $expr into the ordinary
// filter part (delegated to the query parser) and the expression predicates.
func splitMatchExpr(v *anyenc.Value) (StageSpec, error) {
	rest, exprs, remap, err := stripExpr(&anyenc.Arena{}, v)
	if err != nil {
		return nil, err
	}
	spec := MatchSpec{Exprs: exprs}
	if rest != nil {
		if spec.Filter, err = query.ParseCondition(rest); err != nil {
			return nil, remapAndPath(err, remap)
		}
	}
	return spec, nil
}

// andRemap maps a stripped $and's kept element indices back to their original
// positions in the user's spec ($expr-only elements are dropped, shifting the
// rest). sub carries the remap of a kept element's own stripped $and, so
// nested strips compose.
type andRemap struct {
	orig []int             // kept index -> original index
	sub  map[int]*andRemap // kept index -> that element's remap, if stripped
}

// remapAndPath rewrites the leading "$and.<n>" segments of a *query.ParseError
// Path from stripped-array indices back to the original spec's, so the error
// points at the element the user wrote.
func remapAndPath(err error, rm *andRemap) error {
	if rm == nil {
		return err
	}
	var pe *query.ParseError
	if errors.As(err, &pe) {
		pe.Path = rm.rewrite(pe.Path)
	}
	return err
}

func (rm *andRemap) rewrite(path string) string {
	tail, ok := strings.CutPrefix(path, "$and.")
	if !ok {
		return path
	}
	idxStr, tail, hasTail := strings.Cut(tail, ".")
	k, aerr := strconv.Atoi(idxStr)
	if aerr != nil || k < 0 || k >= len(rm.orig) {
		return path
	}
	head := "$and." + strconv.Itoa(rm.orig[k])
	if !hasTail {
		return head
	}
	if sub := rm.sub[k]; sub != nil {
		tail = sub.rewrite(tail)
	}
	return head + "." + tail
}

// stripExpr parses out the $expr keys of a filter object (recursing into $and
// elements) and rebuilds the object without them on a. A nil rest means every
// key was $expr; remap tracks the original indices of kept $and elements for
// error-path rewriting (remapAndPath).
func stripExpr(a *anyenc.Arena, v *anyenc.Value) (rest *anyenc.Value, exprs []Expr, remap *andRemap, err error) {
	obj, _ := v.Object() // callers verified v is an object
	out := a.NewObject()
	kept := 0
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if err != nil {
			return
		}
		switch string(key) {
		case "$expr":
			var e Expr
			if e, err = ParseExpr(val); err != nil {
				err = atPath(err, "$expr")
				return
			}
			exprs = append(exprs, e)
		case "$and":
			if !arrayHasExpr(val) {
				// No $expr inside (or a malformed $and, which the filter
				// parser reports itself).
				out.Set("$and", val)
				kept++
				return
			}
			arr, _ := val.Array()
			restArr := a.NewArray()
			rm := &andRemap{}
			cnt := 0
			for i, el := range arr {
				if !filterHasExpr(el) {
					restArr.SetArrayItem(cnt, el)
					rm.orig = append(rm.orig, i)
					cnt++
					continue
				}
				subRest, subExprs, subRemap, subErr := stripExpr(a, el)
				if subErr != nil {
					err = atPath(atPath(subErr, strconv.Itoa(i)), "$and")
					return
				}
				exprs = append(exprs, subExprs...)
				if subRest != nil {
					restArr.SetArrayItem(cnt, subRest)
					rm.orig = append(rm.orig, i)
					if subRemap != nil {
						if rm.sub == nil {
							rm.sub = map[int]*andRemap{}
						}
						rm.sub[cnt] = subRemap
					}
					cnt++
				}
			}
			if cnt > 0 {
				out.Set("$and", restArr)
				kept++
				remap = rm
			}
		case "$or", "$nor":
			if arrayHasExpr(val) {
				err = atPath(&query.ParseError{
					Op:     "$expr",
					Reason: "$expr is not supported under " + string(key) + ": use it at the top level of $match or inside $and",
				}, string(key))
				return
			}
			out.Set(string(key), val)
			kept++
		default:
			out.Set(string(key), val)
			kept++
		}
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if kept == 0 {
		return nil, exprs, nil, nil
	}
	return out, exprs, remap, nil
}

func parseSortStage(v *anyenc.Value) (StageSpec, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, &query.ParseError{Op: "$sort", Reason: "$sort must be an object"}
	}
	if obj.Len() == 0 {
		return nil, &query.ParseError{Op: "$sort", Reason: "$sort requires at least one field"}
	}
	var (
		sorts  = make(query.Sorts, 0, obj.Len())
		fields = make([]query.SortField, 0, obj.Len())
		perr   error
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if perr != nil {
			return
		}
		dir, e := val.Int()
		if e != nil || (dir != 1 && dir != -1) {
			perr = atPath(&query.ParseError{
				Op:     "$sort",
				Reason: fmt.Sprintf("$sort value for %q must be 1 or -1", string(key)),
			}, string(key))
			return
		}
		field := string(key)
		sf := &query.SortField{
			Field:   field,
			Path:    splitPath(field),
			Reverse: dir == -1,
		}
		sorts = append(sorts, sf)
		fields = append(fields, *sf)
	})
	if perr != nil {
		return nil, perr
	}
	return SortSpec{Sort: sorts, Fields: fields}, nil
}

func parseSkip(v *anyenc.Value) (StageSpec, error) {
	n, err := v.Int()
	if err != nil || n < 0 {
		return nil, &query.ParseError{Op: "$skip", Reason: "$skip must be a non-negative integer"}
	}
	return SkipSpec{N: n}, nil
}

func parseLimit(v *anyenc.Value) (StageSpec, error) {
	n, err := v.Int()
	if err != nil || n <= 0 {
		return nil, &query.ParseError{Op: "$limit", Reason: "$limit must be a positive integer"}
	}
	return LimitSpec{N: n}, nil
}

func parseCount(v *anyenc.Value) (StageSpec, error) {
	sb, err := v.StringBytes()
	if err != nil {
		return nil, &query.ParseError{Op: "$count", Reason: "$count must be a string field name"}
	}
	name := string(sb)
	if err = validateOutName(name); err != nil {
		return nil, &query.ParseError{Op: "$count", Reason: "$count: " + err.Error()}
	}
	return CountSpec{Field: name}, nil
}

func parseProject(v *anyenc.Value) (StageSpec, error) {
	fields, err := parseProjectFields(v, "$project", true)
	if err != nil {
		return nil, err
	}
	return ProjectSpec{Fields: fields}, nil
}

func parseAddFields(v *anyenc.Value) (StageSpec, error) {
	fields, err := parseProjectFields(v, "$addFields", false)
	if err != nil {
		return nil, err
	}
	return AddFieldsSpec{Fields: fields}, nil
}

func parseProjectFields(v *anyenc.Value, stage string, allowInclude bool) ([]ProjectField, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, &query.ParseError{Op: stage, Reason: stage + " must be an object"}
	}
	if obj.Len() == 0 {
		return nil, &query.ParseError{Op: stage, Reason: stage + " requires at least one field"}
	}
	var (
		fields = make([]ProjectField, 0, obj.Len())
		perr   error
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if perr != nil {
			return
		}
		name := string(key)
		if e := validateOutName(name); e != nil {
			perr = &query.ParseError{Op: stage, Reason: stage + ": " + e.Error()}
			return
		}
		var expr Expr
		switch {
		case allowInclude && isIncludeFlag(val, true):
			expr = &FieldRefExpr{Field: name, Path: splitPath(name)}
		case allowInclude && isIncludeFlag(val, false):
			perr = atPath(&query.ParseError{
				Op:     stage,
				Reason: fmt.Sprintf("exclusion (%q: 0) is not supported", name),
			}, name)
			return
		default:
			var e error
			if expr, e = ParseExpr(val); e != nil {
				perr = atPath(e, name)
				return
			}
		}
		fields = append(fields, ProjectField{Name: name, Expr: expr})
	})
	if perr != nil {
		return nil, perr
	}
	return fields, nil
}

// isIncludeFlag reports whether v is the $project include (1/true) or exclude
// (0/false) flag, depending on want.
func isIncludeFlag(v *anyenc.Value, want bool) bool {
	switch v.Type() {
	case anyenc.TypeTrue:
		return want
	case anyenc.TypeFalse:
		return !want
	case anyenc.TypeNumber:
		if n := v.GetFloat64(); n != 0 {
			return want
		}
		return !want
	}
	return false
}

func parseUnwind(v *anyenc.Value) (StageSpec, error) {
	var (
		pathStr  string
		preserve bool
	)
	switch v.Type() {
	case anyenc.TypeString:
		pathStr = string(v.GetStringBytes())
	case anyenc.TypeObject:
		obj, _ := v.Object()
		var perr error
		obj.Visit(func(key []byte, val *anyenc.Value) {
			if perr != nil {
				return
			}
			switch string(key) {
			case "path":
				sb, e := val.StringBytes()
				if e != nil {
					perr = atPath(&query.ParseError{Op: "$unwind", Reason: "$unwind path must be a string"}, "path")
					return
				}
				pathStr = string(sb)
			case "preserveNullAndEmptyArrays":
				b, e := val.Bool()
				if e != nil {
					perr = atPath(&query.ParseError{Op: "$unwind", Reason: "$unwind preserveNullAndEmptyArrays must be a boolean"}, "preserveNullAndEmptyArrays")
					return
				}
				preserve = b
			default:
				// Like an unknown $text field in the filter grammar: an option
				// miss inside a known operator, deliberately not
				// ErrUnknownOperator.
				perr = atPath(&query.ParseError{
					Op:     "$unwind",
					Reason: "unknown $unwind option: " + string(key),
				}, string(key))
			}
		})
		if perr != nil {
			return nil, perr
		}
	default:
		return nil, &query.ParseError{Op: "$unwind", Reason: "$unwind must be a string or an object"}
	}
	if !strings.HasPrefix(pathStr, "$") || len(pathStr) < 2 {
		return nil, &query.ParseError{Op: "$unwind", Reason: "$unwind path must start with $ and name a field"}
	}
	field := pathStr[1:]
	return UnwindSpec{Field: field, Path: splitPath(field), PreserveNullAndEmptyArrays: preserve}, nil
}

func parseGroup(v *anyenc.Value) (StageSpec, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, &query.ParseError{Op: "$group", Reason: "$group must be an object"}
	}
	var (
		spec    GroupSpec
		hasKey  bool
		perr    error
		twoKeys bool
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if perr != nil {
			return
		}
		name := string(key)
		// The group key: Mongo spells it "_id"; any-store's document id field
		// is "id", so both spellings are accepted and the output field is "id".
		if name == "_id" || name == "id" {
			if hasKey {
				twoKeys = true
				return
			}
			hasKey = true
			if spec.Key, perr = ParseExpr(val); perr != nil {
				perr = atPath(perr, name)
			}
			return
		}
		if e := validateOutName(name); e != nil {
			perr = &query.ParseError{Op: "$group", Reason: "$group: " + e.Error()}
			return
		}
		acc, e := parseAccum(name, val)
		if e != nil {
			perr = atPath(e, name)
			return
		}
		spec.Accums = append(spec.Accums, acc)
	})
	if perr != nil {
		return nil, perr
	}
	if twoKeys {
		return nil, &query.ParseError{Op: "$group", Reason: "$group: both id and _id specified"}
	}
	if !hasKey {
		return nil, &query.ParseError{Op: "$group", Reason: "$group requires an id (or _id) key expression"}
	}
	return spec, nil
}

func parseAccum(name string, v *anyenc.Value) (AccumSpec, error) {
	obj, err := v.Object()
	if err != nil || obj.Len() != 1 {
		return AccumSpec{}, &query.ParseError{
			Op:     "$group",
			Reason: fmt.Sprintf("$group field %q must be an accumulator object like {\"$sum\": ...}", name),
		}
	}
	var (
		spec = AccumSpec{Name: name}
		perr error
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		op, ok := accumOpByName(string(key))
		if !ok {
			perr = atPath(&query.ParseError{
				Op:     string(key),
				Reason: "unknown accumulator: " + string(key),
				Err:    query.ErrUnknownOperator,
			}, string(key))
			return
		}
		spec.Op = op
		if op == AccumCount {
			// {"$count": {}} — argument must be an empty object.
			if o, e := val.Object(); e != nil || o.Len() != 0 {
				perr = atPath(&query.ParseError{Op: "$count", Reason: "$count takes an empty object {}"}, string(key))
			}
			return
		}
		if spec.Arg, perr = ParseExpr(val); perr != nil {
			perr = atPath(perr, string(key))
		}
	})
	if perr != nil {
		return AccumSpec{}, perr
	}
	return spec, nil
}

// parseLookup parses the $lookup stage, narrowed to a primary-key self-join:
// {from?, localField, foreignField: "id", as}. The pipeline/let form and any
// other foreignField are rejected — general cross-collection joins are out of
// scope. foreignField may be omitted ("id" is the only legal value); a from
// naming another collection parses fine and fails at execution setup, where
// the aggregated collection's name is known.
func parseLookup(v *anyenc.Value) (StageSpec, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, &query.ParseError{Op: "$lookup", Reason: "$lookup must be an object"}
	}
	var (
		spec     LookupSpec
		hasLocal bool
		hasAs    bool
		perr     error
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if perr != nil {
			return
		}
		switch string(key) {
		case "from":
			sb, e := val.StringBytes()
			if e != nil {
				perr = atPath(&query.ParseError{Op: "$lookup", Reason: "$lookup from must be a string collection name"}, "from")
				return
			}
			spec.From = string(sb)
		case "localField":
			sb, e := val.StringBytes()
			if e != nil || len(sb) == 0 {
				perr = atPath(&query.ParseError{Op: "$lookup", Reason: "$lookup localField must be a non-empty string field path"}, "localField")
				return
			}
			spec.LocalField = string(sb)
			hasLocal = true
		case "foreignField":
			if sb, e := val.StringBytes(); e != nil || string(sb) != "id" {
				perr = atPath(&query.ParseError{
					Op:     "$lookup",
					Reason: `only primary-key self-joins are supported: foreignField must be "id"`,
				}, "foreignField")
			}
		case "as":
			sb, e := val.StringBytes()
			if e != nil {
				perr = atPath(&query.ParseError{Op: "$lookup", Reason: "$lookup as must be a string field name"}, "as")
				return
			}
			name := string(sb)
			if ne := validateOutName(name); ne != nil {
				perr = atPath(&query.ParseError{Op: "$lookup", Reason: "$lookup as: " + ne.Error()}, "as")
				return
			}
			spec.As = name
			hasAs = true
		case "let", "pipeline":
			perr = atPath(&query.ParseError{
				Op:     "$lookup",
				Reason: `pipeline-form $lookup is not supported: use {from?, localField, foreignField: "id", as}`,
			}, string(key))
		default:
			perr = atPath(&query.ParseError{
				Op:     "$lookup",
				Reason: "unknown $lookup option: " + string(key),
			}, string(key))
		}
	})
	if perr != nil {
		return nil, perr
	}
	if !hasLocal {
		return nil, &query.ParseError{Op: "$lookup", Reason: "$lookup requires localField"}
	}
	if !hasAs {
		return nil, &query.ParseError{Op: "$lookup", Reason: "$lookup requires as"}
	}
	spec.LocalPath = splitPath(spec.LocalField)
	return spec, nil
}

// parseFacet parses {"$facet": {name: [stage...], ...}}: at least one facet,
// names following output-field naming rules, each value a non-empty pipeline
// array. $facet inside a facet is rejected (Mongo forbids nesting).
//
// SYN-129 exclusion point: when $merge/$out land, reject them inside facets
// here too — side-effect stages must not run fanned out over a shared scan.
func parseFacet(v *anyenc.Value) (StageSpec, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, &query.ParseError{Op: "$facet", Reason: "$facet must be an object of named pipelines"}
	}
	if obj.Len() == 0 {
		return nil, &query.ParseError{Op: "$facet", Reason: "$facet requires at least one facet"}
	}
	var (
		spec FacetSpec
		perr error
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if perr != nil {
			return
		}
		name := string(key)
		if e := validateOutName(name); e != nil {
			perr = &query.ParseError{Op: "$facet", Reason: "$facet: " + e.Error()}
			return
		}
		if slices.Contains(spec.Names, name) {
			// Last-wins would silently execute both pipelines and discard one
			// facet's results.
			perr = atPath(&query.ParseError{Op: "$facet", Reason: "duplicate facet name: " + name}, name)
			return
		}
		if val.Type() != anyenc.TypeArray {
			perr = atPath(&query.ParseError{Op: "$facet", Reason: "facet must be a pipeline array"}, name)
			return
		}
		arr, _ := val.Array()
		if len(arr) == 0 {
			perr = atPath(&query.ParseError{Op: "$facet", Reason: "facet pipeline must not be empty"}, name)
			return
		}
		sub := make(Pipeline, 0, len(arr))
		for i, el := range arr {
			st, e := parseStage(el)
			if e != nil {
				perr = atPath(atPath(e, strconv.Itoa(i)), name)
				return
			}
			if _, nested := st.(FacetSpec); nested {
				perr = atPath(atPath(&query.ParseError{Op: "$facet", Reason: "$facet cannot be nested"}, strconv.Itoa(i)), name)
				return
			}
			sub = append(sub, st)
		}
		spec.Names = append(spec.Names, name)
		spec.Pipelines = append(spec.Pipelines, sub)
	})
	if perr != nil {
		return nil, perr
	}
	return spec, nil
}

func accumOpByName(name string) (AccumOp, bool) {
	for op, n := range accumOpNames {
		if n == name {
			return AccumOp(op), true
		}
	}
	return 0, false
}

// validateOutName rejects output field names that would corrupt result
// documents: empty, operator-like ($ prefix), or nested (dots; not supported
// in v1). In $group, "id"/"_id" never reach here — both spellings are routed
// to the group key by parseGroup.
func validateOutName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("empty field name")
	case strings.HasPrefix(name, "$"):
		return fmt.Errorf("field name must not start with $: %q", name)
	case strings.Contains(name, "."):
		return fmt.Errorf("nested (dotted) output fields are not supported: %q", name)
	}
	return nil
}
