package aggregate

import (
	"fmt"
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

type MatchSpec struct{ Filter query.Filter }

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

func (MatchSpec) stageSpec()     {}
func (SortSpec) stageSpec()      {}
func (SkipSpec) stageSpec()      {}
func (LimitSpec) stageSpec()     {}
func (CountSpec) stageSpec()     {}
func (ProjectSpec) stageSpec()   {}
func (AddFieldsSpec) stageSpec() {}
func (UnwindSpec) stageSpec()    {}
func (GroupSpec) stageSpec()     {}

func (s MatchSpec) String() string { return fmt.Sprintf("$match %s", s.Filter) }

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

// ParsePipeline parses a JSON-ish aggregation pipeline (any input accepted by
// the same parser as Find conditions) into stage specs. The result is fully
// detached from the input value.
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
		return nil, fmt.Errorf("aggregate: pipeline must be an array of stages")
	}
	arr, _ := v.Array()
	res := make(Pipeline, 0, len(arr))
	for i, el := range arr {
		spec, err := parseStage(el)
		if err != nil {
			return nil, fmt.Errorf("aggregate: stage %d: %w", i, err)
		}
		res = append(res, spec)
	}
	return res, nil
}

func parseStage(v *anyenc.Value) (StageSpec, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, fmt.Errorf("stage must be an object")
	}
	if obj.Len() != 1 {
		return nil, fmt.Errorf("stage must have exactly one key, got %d", obj.Len())
	}
	var (
		spec StageSpec
		serr error
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		switch string(key) {
		case "$match":
			spec, serr = parseMatch(val)
		case "$sort":
			spec, serr = parseSortStage(val)
		case "$skip":
			spec, serr = parseSkip(val)
		case "$limit":
			spec, serr = parseLimit(val)
		case "$count":
			spec, serr = parseCount(val)
		case "$project":
			spec, serr = parseProject(val)
		case "$addFields", "$set":
			spec, serr = parseAddFields(val)
		case "$unwind":
			spec, serr = parseUnwind(val)
		case "$group":
			spec, serr = parseGroup(val)
		default:
			serr = fmt.Errorf("unknown stage: %s", string(key))
		}
	})
	return spec, serr
}

func parseMatch(v *anyenc.Value) (StageSpec, error) {
	f, err := query.ParseCondition(v)
	if err != nil {
		return nil, fmt.Errorf("$match: %w", err)
	}
	return MatchSpec{Filter: f}, nil
}

func parseSortStage(v *anyenc.Value) (StageSpec, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, fmt.Errorf("$sort must be an object")
	}
	if obj.Len() == 0 {
		return nil, fmt.Errorf("$sort requires at least one field")
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
			perr = fmt.Errorf("$sort value for %q must be 1 or -1", string(key))
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
		return nil, fmt.Errorf("$skip must be a non-negative integer")
	}
	return SkipSpec{N: n}, nil
}

func parseLimit(v *anyenc.Value) (StageSpec, error) {
	n, err := v.Int()
	if err != nil || n <= 0 {
		return nil, fmt.Errorf("$limit must be a positive integer")
	}
	return LimitSpec{N: n}, nil
}

func parseCount(v *anyenc.Value) (StageSpec, error) {
	sb, err := v.StringBytes()
	if err != nil {
		return nil, fmt.Errorf("$count must be a string field name")
	}
	name := string(sb)
	if err = validateOutName(name); err != nil {
		return nil, fmt.Errorf("$count: %w", err)
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
		return nil, fmt.Errorf("%s must be an object", stage)
	}
	if obj.Len() == 0 {
		return nil, fmt.Errorf("%s requires at least one field", stage)
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
			perr = fmt.Errorf("%s: %w", stage, e)
			return
		}
		var expr Expr
		switch {
		case allowInclude && isIncludeFlag(val, true):
			expr = &FieldRefExpr{Field: name, Path: splitPath(name)}
		case allowInclude && isIncludeFlag(val, false):
			perr = fmt.Errorf("%s: exclusion (%q: 0) is not supported", stage, name)
			return
		default:
			var e error
			if expr, e = ParseExpr(val); e != nil {
				perr = e
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
					perr = fmt.Errorf("$unwind path must be a string")
					return
				}
				pathStr = string(sb)
			case "preserveNullAndEmptyArrays":
				b, e := val.Bool()
				if e != nil {
					perr = fmt.Errorf("$unwind preserveNullAndEmptyArrays must be a boolean")
					return
				}
				preserve = b
			default:
				perr = fmt.Errorf("unknown $unwind option: %s", string(key))
			}
		})
		if perr != nil {
			return nil, perr
		}
	default:
		return nil, fmt.Errorf("$unwind must be a string or an object")
	}
	if !strings.HasPrefix(pathStr, "$") || len(pathStr) < 2 {
		return nil, fmt.Errorf("$unwind path must start with $ and name a field")
	}
	field := pathStr[1:]
	return UnwindSpec{Field: field, Path: splitPath(field), PreserveNullAndEmptyArrays: preserve}, nil
}

func parseGroup(v *anyenc.Value) (StageSpec, error) {
	obj, err := v.Object()
	if err != nil {
		return nil, fmt.Errorf("$group must be an object")
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
			spec.Key, perr = ParseExpr(val)
			return
		}
		if e := validateOutName(name); e != nil {
			perr = fmt.Errorf("$group: %w", e)
			return
		}
		acc, e := parseAccum(name, val)
		if e != nil {
			perr = e
			return
		}
		spec.Accums = append(spec.Accums, acc)
	})
	if perr != nil {
		return nil, perr
	}
	if twoKeys {
		return nil, fmt.Errorf("$group: both id and _id specified")
	}
	if !hasKey {
		return nil, fmt.Errorf("$group requires an id (or _id) key expression")
	}
	return spec, nil
}

func parseAccum(name string, v *anyenc.Value) (AccumSpec, error) {
	obj, err := v.Object()
	if err != nil || obj.Len() != 1 {
		return AccumSpec{}, fmt.Errorf("$group field %q must be an accumulator object like {\"$sum\": ...}", name)
	}
	var (
		spec = AccumSpec{Name: name}
		perr error
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		op, ok := accumOpByName(string(key))
		if !ok {
			perr = fmt.Errorf("$group field %q: unknown accumulator: %s", name, string(key))
			return
		}
		spec.Op = op
		if op == AccumCount {
			// {"$count": {}} — argument must be an empty object.
			if o, e := val.Object(); e != nil || o.Len() != 0 {
				perr = fmt.Errorf("$group field %q: $count takes an empty object {}", name)
			}
			return
		}
		spec.Arg, perr = ParseExpr(val)
	})
	if perr != nil {
		return AccumSpec{}, perr
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
