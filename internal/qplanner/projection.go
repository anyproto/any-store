package qplanner

import "github.com/anyproto/any-store/v2/query"

// Projected field decoding (Task 2 of docs/btree/plans/2026-05-23-readwrite-hotpath-opts.md):
// the v2 analogue of SQLite's OP_Column lazy single-column decode
// (sqlitec/src/vdbe.c). Instead of fully materializing every scanned row into
// an owned anyenc.Value tree before the filter looks at one field, the scan
// decodes ONLY the top-level fields a query references and structurally skips
// the rest (anyenc.Parser.ParseProjected). For a filter like {a:50} on the
// benchmark's ~920 B buildDoc, this skips the unused 80-element nums array that
// dominates decode time.
//
// Correctness rests on a single invariant, enforced by the helpers below:
//
//	The projected field set must contain EVERY top-level root key that the
//	filter and (when its result is consumed via the same cached doc) the sort
//	reference. Because the leaf comparison operators (Comp/In/Not/Exists/...)
//	are ALWAYS wrapped in a query.Key whose inner filter only ever sees
//	v.Get(Key.Path...), projecting Key.Path[0] and fully decoding that subtree
//	is sufficient for any operator, including $ne / array-membership and nested
//	paths — the entire referenced subtree is decoded, only sibling top-level
//	keys are dropped.
//
// If the field set cannot be determined statically (an unrecognized filter
// node), projection is disabled and the scan falls back to a full ParseOwned.

// filterFieldRoots walks the filter tree and appends the top-level root key
// (Path[0]) of every field-bearing leaf to dst. It returns ok=false if the
// filter contains any node whose referenced fields cannot be determined
// statically (forcing the caller to fall back to a full parse).
//
// Recognized, field-preserving nodes: Key, And/*And, Or, Nor, Not, All.
// Anything else => ok=false.
func filterFieldRoots(dst []string, f query.Filter) ([]string, bool) {
	switch ft := f.(type) {
	case nil:
		return dst, true
	case query.All:
		return dst, true
	case query.Key:
		// A Key with an empty path can't be projected (we wouldn't know which
		// root to keep); be conservative and fall back.
		if len(ft.Path) == 0 {
			return dst, false
		}
		return appendUnique(dst, ft.Path[0]), true
	case query.And:
		for _, sub := range ft {
			var ok bool
			if dst, ok = filterFieldRoots(dst, sub); !ok {
				return dst, false
			}
		}
		return dst, true
	case *query.And:
		// query.MustParseCondition produces *query.And for `{"$and":[...]}`
		// (query/cond_parse.go), so the pointer arm must be handled too.
		for _, sub := range *ft {
			var ok bool
			if dst, ok = filterFieldRoots(dst, sub); !ok {
				return dst, false
			}
		}
		return dst, true
	case query.Or:
		for _, sub := range ft {
			var ok bool
			if dst, ok = filterFieldRoots(dst, sub); !ok {
				return dst, false
			}
		}
		return dst, true
	case query.Nor:
		for _, sub := range ft {
			var ok bool
			if dst, ok = filterFieldRoots(dst, sub); !ok {
				return dst, false
			}
		}
		return dst, true
	case query.Not:
		// Not wraps an inner filter; in practice the field-bearing Key sits
		// above it, but recurse defensively in case the inner references a
		// field (e.g. Not{Exists{}} references none, which is fine).
		return filterFieldRoots(dst, ft.Filter)
	default:
		// Unknown / non-decomposable node: cannot determine fields statically.
		return dst, false
	}
}

// sortRootsFromFields appends the top-level root key (Path[0]) of every sort
// field in fields to dst. It returns ok=false if any sort field has an empty
// path (which would make its projection root indeterminate). The caller passes
// fields it already fetched via Sort.Fields() so this never re-allocates that
// slice on the hot planner path.
func sortRootsFromFields(dst []string, fields []query.SortField) ([]string, bool) {
	for _, sf := range fields {
		if len(sf.Path) == 0 {
			return dst, false
		}
		dst = appendUnique(dst, sf.Path[0])
	}
	return dst, true
}

// sortFieldRoots is the Sort-typed convenience wrapper used by tests; the
// planner uses sortRootsFromFields with a pre-fetched Fields() slice.
func sortFieldRoots(dst []string, s query.Sort) ([]string, bool) {
	if s == nil {
		return dst, true
	}
	return sortRootsFromFields(dst, s.Fields())
}

// appendUnique appends key to dst only if it is not already present. The set
// is tiny (1–3 fields), so linear membership is the right tradeoff.
func appendUnique(dst []string, key string) []string {
	for _, k := range dst {
		if k == key {
			return dst
		}
	}
	return append(dst, key)
}

// scanProjection returns the deduplicated union of the filter's and the
// supplied sort fields' top-level field roots, to be passed to ParseProjected
// on the scan path. ok is false (and fields nil) when projection cannot be
// applied safely — either the filter references fields that can't be determined
// statically, or a sort field does. The caller then performs a full ParseOwned.
//
// sortFields must be non-empty precisely when the projected doc produced for
// the filter is ALSO consumed to extract the sort key from the SAME cached
// value (a SortIter directly above a filtered scan reads Plan.DocParsed). Pass
// nil when the sort re-derives the key from its own parse. The caller supplies
// the already-fetched Sort.Fields() slice so this performs no allocation for it.
//
// dst is an in-place destination buffer (typically an iterator's inline array,
// sliced to len 0) so the result is allocation-free when the union fits dst's
// capacity; a union larger than cap(dst) simply reallocates (rare — field sets
// are 1–3 entries in practice). The returned slice may alias dst, so callers
// must keep dst's backing array alive for as long as they hold the result.
func scanProjection(dst []string, filter query.Filter, sortFields []query.SortField) (fields []string, ok bool) {
	dst = dst[:0]
	if dst, ok = filterFieldRoots(dst, filter); !ok {
		return nil, false
	}
	if len(sortFields) > 0 {
		if dst, ok = sortRootsFromFields(dst, sortFields); !ok {
			return nil, false
		}
	}
	if len(dst) == 0 {
		return nil, false
	}
	return dst, true
}
