package aggregate

import (
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// Sink stages ($merge, $out) materialize pipeline results into a collection.
// They are specs only — no executable Stage: the root package cuts the
// trailing sink off the pipeline (CutSink), drains the read part, and applies
// the buffered results through the collection write path. Both must be the
// last pipeline stage (enforced by ParsePipeline) and are rejected inside
// $facet (side-effect stages must not run fanned out over a shared scan).

// MergeWhenMatched selects $merge's action for a result document whose id
// already exists in the target. The zero value is the default ("merge").
type MergeWhenMatched uint8

const (
	// MergeMatchedMerge overlays the result's top-level fields onto the
	// existing document (missing fields keep their existing values).
	MergeMatchedMerge MergeWhenMatched = iota
	// MergeMatchedReplace replaces the existing document entirely.
	MergeMatchedReplace
	// MergeMatchedKeepExisting leaves the existing document untouched.
	MergeMatchedKeepExisting
	// MergeMatchedFail aborts the whole write, persisting nothing.
	MergeMatchedFail
)

var mergeMatchedNames = [...]string{"merge", "replace", "keepExisting", "fail"}

func (m MergeWhenMatched) String() string { return mergeMatchedNames[m] }

// MergeWhenNotMatched selects $merge's action for a result document whose id
// does not exist in the target. The zero value is the default ("insert").
type MergeWhenNotMatched uint8

const (
	// MergeNotMatchedInsert inserts the result document.
	MergeNotMatchedInsert MergeWhenNotMatched = iota
	// MergeNotMatchedDiscard drops the result document.
	MergeNotMatchedDiscard
	// MergeNotMatchedFail aborts the whole write, persisting nothing.
	MergeNotMatchedFail
)

var mergeNotMatchedNames = [...]string{"insert", "discard", "fail"}

func (m MergeWhenNotMatched) String() string { return mergeNotMatchedNames[m] }

// MergeSpec is a parsed $merge stage: upsert pipeline results into Into,
// keyed by the primary key "id" (the only supported "on"; enforced at parse
// time, mirroring $lookup's pk-only scope).
type MergeSpec struct {
	Into           string
	WhenMatched    MergeWhenMatched
	WhenNotMatched MergeWhenNotMatched
}

// OutSpec is a parsed $out stage: replace the contents of collection Coll
// with the pipeline results.
type OutSpec struct {
	Coll string
}

func (MergeSpec) stageSpec() {}
func (OutSpec) stageSpec()   {}

func (s MergeSpec) String() string {
	return fmt.Sprintf(`$merge {"into":%q,"on":"id","whenMatched":%q,"whenNotMatched":%q}`,
		s.Into, s.WhenMatched, s.WhenNotMatched)
}

func (s OutSpec) String() string { return fmt.Sprintf("$out %q", s.Coll) }

// SinkName returns the stage name of a sink spec ("$merge"/"$out"), or ""
// for any other spec.
func SinkName(s StageSpec) string {
	switch s.(type) {
	case MergeSpec:
		return "$merge"
	case OutSpec:
		return "$out"
	}
	return ""
}

// CutSink splits a trailing sink spec off the pipeline. Returns (nil, p)
// when the pipeline does not end in one. ParsePipeline guarantees a sink can
// appear only in the last position.
func CutSink(p Pipeline) (StageSpec, Pipeline) {
	if len(p) == 0 {
		return nil, p
	}
	last := p[len(p)-1]
	if SinkName(last) != "" {
		return last, p[:len(p)-1]
	}
	return nil, p
}

// parseOut parses {"$out": "collection"}. Mongo's db-qualified object form is
// rejected: cross-database output does not exist here.
func parseOut(v *anyenc.Value) (StageSpec, error) {
	sb, err := v.StringBytes()
	if err != nil {
		return nil, &query.ParseError{Op: "$out", Reason: "$out must be a collection name string (the db-qualified form is not supported)"}
	}
	if len(sb) == 0 {
		return nil, &query.ParseError{Op: "$out", Reason: "$out collection name must not be empty"}
	}
	return OutSpec{Coll: string(sb)}, nil
}

// parseMerge parses {"$merge": "collection"} or
// {"$merge": {"into": c, "on"?: "id", "whenMatched"?: m, "whenNotMatched"?: n}}.
// "on" may only be "id" (primary-key merges, mirroring $lookup's scope) and
// the pipeline/let whenMatched form is rejected.
func parseMerge(v *anyenc.Value) (StageSpec, error) {
	if v.Type() == anyenc.TypeString {
		name := string(v.GetStringBytes())
		if name == "" {
			return nil, &query.ParseError{Op: "$merge", Reason: "$merge collection name must not be empty"}
		}
		return MergeSpec{Into: name}, nil
	}
	obj, err := v.Object()
	if err != nil {
		return nil, &query.ParseError{Op: "$merge", Reason: "$merge must be a collection name string or an options object"}
	}
	var (
		spec MergeSpec
		perr error
	)
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if perr != nil {
			return
		}
		switch string(key) {
		case "into":
			sb, e := val.StringBytes()
			if e != nil || len(sb) == 0 {
				perr = atPath(&query.ParseError{Op: "$merge", Reason: "$merge into must be a collection name string (the db-qualified form is not supported)"}, "into")
				return
			}
			spec.Into = string(sb)
		case "on":
			if sb, e := val.StringBytes(); e != nil || string(sb) != "id" {
				perr = atPath(&query.ParseError{
					Op:     "$merge",
					Reason: `only primary-key merges are supported: on must be "id"`,
				}, "on")
			}
		case "whenMatched":
			sb, e := val.StringBytes()
			if e != nil {
				if val.Type() == anyenc.TypeArray {
					perr = atPath(&query.ParseError{Op: "$merge", Reason: "pipeline-form whenMatched is not supported"}, "whenMatched")
				} else {
					perr = atPath(&query.ParseError{Op: "$merge", Reason: mergeEnumReason("whenMatched", mergeMatchedNames[:])}, "whenMatched")
				}
				return
			}
			m, ok := mergeEnumByName(string(sb), mergeMatchedNames[:])
			if !ok {
				perr = atPath(&query.ParseError{Op: "$merge", Reason: mergeEnumReason("whenMatched", mergeMatchedNames[:])}, "whenMatched")
				return
			}
			spec.WhenMatched = MergeWhenMatched(m)
		case "whenNotMatched":
			sb, e := val.StringBytes()
			if e != nil {
				perr = atPath(&query.ParseError{Op: "$merge", Reason: mergeEnumReason("whenNotMatched", mergeNotMatchedNames[:])}, "whenNotMatched")
				return
			}
			m, ok := mergeEnumByName(string(sb), mergeNotMatchedNames[:])
			if !ok {
				perr = atPath(&query.ParseError{Op: "$merge", Reason: mergeEnumReason("whenNotMatched", mergeNotMatchedNames[:])}, "whenNotMatched")
				return
			}
			spec.WhenNotMatched = MergeWhenNotMatched(m)
		case "let":
			perr = atPath(&query.ParseError{Op: "$merge", Reason: "pipeline-form whenMatched (let) is not supported"}, "let")
		default:
			perr = atPath(&query.ParseError{
				Op:     "$merge",
				Reason: "unknown $merge option: " + string(key),
			}, string(key))
		}
	})
	if perr != nil {
		return nil, perr
	}
	if spec.Into == "" {
		return nil, &query.ParseError{Op: "$merge", Reason: "$merge requires into"}
	}
	return spec, nil
}

func mergeEnumByName(name string, names []string) (int, bool) {
	for i, n := range names {
		if n == name {
			return i, true
		}
	}
	return 0, false
}

func mergeEnumReason(opt string, names []string) string {
	res := "$merge " + opt + " must be one of"
	for i, n := range names {
		if i > 0 {
			res += ","
		}
		res += " " + fmt.Sprintf("%q", n)
	}
	return res
}
