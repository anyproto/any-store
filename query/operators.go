package query

import "sort"

// operators is the filter grammar's operator vocabulary as data: the single
// source of truth for operator recognition. isOperator consults it, error
// messages name ops through opName, and Operators exports it — so the parser,
// its errors, and the advertised vocabulary cannot drift apart.
var operators = map[string]Operator{
	"$and":  opAnd,
	"$or":   opOr,
	"$nor":  opNor,
	"$text": opText,

	"$eq":  opEq,
	"$ne":  opNe,
	"$gt":  opGt,
	"$gte": opGte,
	"$lt":  opLt,
	"$lte": opLte,

	"$in":      opIn,
	"$nin":     opNin,
	"$all":     opAll,
	"$not":     opNot,
	"$exists":  opExists,
	"$type":    opType,
	"$regex":   opRegexp,
	"$options": opOptions,
	"$size":    opSize,

	"$knn": opKnn,
}

// Operators returns the operator vocabulary accepted by the filter parser —
// every '$'-prefixed key ParseCondition recognizes, top-level and field-level
// alike, sorted and with the leading '$'. The slice is a fresh copy: callers
// may keep or mutate it. Use it to advertise the grammar (docs, error
// payloads) instead of hand-copying the list.
func Operators() []string {
	res := make([]string, 0, len(operators))
	for name := range operators {
		res = append(res, name)
	}
	sort.Strings(res)
	return res
}

// opName returns the canonical spelling of op for error messages. Reverse
// lookup over the vocabulary is fine here: it runs only on rejection paths.
func opName(op Operator) string {
	for name, o := range operators {
		if o == op {
			return name
		}
	}
	return "$<unknown>"
}
