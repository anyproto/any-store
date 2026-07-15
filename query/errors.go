package query

import "errors"

// ErrUnknownOperator is returned when a filter names an operator that is not
// part of the grammar, e.g. {"tags": {"$contains": "x"}}. It is a client fault:
// callers parsing untrusted filters can errors.Is it to answer with a bad-request
// rather than an internal error.
//
// Use errors.As with *UnknownOperatorError to recover the offending operator and
// name it back to the caller.
var ErrUnknownOperator = errors.New("unknown operator")

// UnknownOperatorError carries the operator that was not recognized. It wraps
// ErrUnknownOperator, so errors.Is(err, ErrUnknownOperator) reports true.
type UnknownOperatorError struct {
	// Op is the unrecognized operator, including the leading '$'.
	Op string
}

func (e *UnknownOperatorError) Error() string {
	return "unknown operator: " + e.Op
}

func (e *UnknownOperatorError) Unwrap() error {
	return ErrUnknownOperator
}
