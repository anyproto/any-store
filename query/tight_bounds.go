package query

import "strings"

// TightIndexBounds computes the INTERSECTED ("tight") bounds for fieldName:
// where a conjunction carries several bound-contributing conjuncts on the same
// field ({"f":{"$gt":lo,"$lt":hi}}, {$in:[...],$ne:v}, nested $and forms), the
// contributions are interval-intersected instead of keeping only the first one
// the way And.IndexBounds does.
//
// SOUNDNESS — read before adding consumers. Tight bounds are NOT a sound seek
// range in general: array filter semantics match each conjunct against the
// whole array independently, so {tags:{$gte:2,$lte:3}} matches {tags:[5,1]}
// (5>=2 via one element, 1<=3 via another) with NO index entry in [2,3] — a
// seek narrowed to the intersection silently drops the doc from Iter AND
// Count (docs/known-issues.md I-04, revert ba92bc7). Tight bounds may drive:
//   - cost/selectivity ESTIMATION: always (estimates cannot drop docs);
//   - actual SEEKS: only when the scanned namespace provably holds no
//     fan-out (multi-key) entries — the primary-key namespace (array pks are
//     rejected on write, see ErrArrayPrimaryKey) or a secondary index whose
//     persisted multikey flag proves it scalar-only.
//
// empty=true reports that the intersection is PROVABLY EMPTY as a value set
// ({"f":{"$gt":5,"$lt":3}}). It does NOT mean the filter is unsatisfiable:
// under array semantics {f:[6,1]} MATCHES that filter (6>5, 1<3), so callers
// may treat empty as "zero results" only under the same fan-out-free proof as
// seeks; everywhere else they must fall back to the wide bounds. When
// empty=true the returned bounds are nil — never feed that to code that reads
// len==0 as "unconstrained".
//
// Node handling: Key routes the field; And/*And intersect their contributing
// children (children contributing nothing are unconstrained, not narrowing);
// every other node — including Or — delegates to its own IndexBounds, so Or
// keeps its all-or-nothing union and never produces empty. Bounds bytes alias
// filter-owned pre-clipped (cap==len) memory and are never mutated in place,
// per docs/query-filter-contract.md.
func TightIndexBounds(f Filter, fieldName string) (bounds Bounds, empty bool) {
	switch t := f.(type) {
	case Key:
		if strings.Join(t.Path, ".") == fieldName {
			return TightIndexBounds(t.Filter, fieldName)
		}
		return nil, false
	case And:
		return tightAndBounds(t, fieldName)
	case *And:
		return tightAndBounds(*t, fieldName)
	default:
		return f.IndexBounds(fieldName, nil), false
	}
}

func tightAndBounds(conj And, fieldName string) (bounds Bounds, empty bool) {
	contributed := false
	for _, child := range conj {
		cb, cEmpty := TightIndexBounds(child, fieldName)
		if cEmpty {
			return nil, true
		}
		if len(cb) == 0 {
			continue // no contribution for this field — unconstrained, skip
		}
		if !contributed {
			bounds, contributed = cb, true
			continue
		}
		if bounds = bounds.Intersect(cb); len(bounds) == 0 {
			return nil, true
		}
	}
	return bounds, false
}
