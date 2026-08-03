package query

import "errors"

// ErrUnknownOperator is the class sentinel for a filter that names an operator
// outside the grammar, e.g. {"tags": {"$contains": "x"}}. It is a client
// fault: callers parsing untrusted filters can errors.Is it to answer with a
// bad-request rather than an internal error. The rejection itself is a
// *ParseError (errors.As) whose Op names the bad token.
var ErrUnknownOperator = errors.New("unknown operator")

// ParseError is the structured rejection for a filter that does not parse.
// Every parse-rejection class — unknown operator, wrong operand type,
// malformed $and/$or/$nor array, bad $regex, … — is reported as a *ParseError,
// so a caller exposing the filter grammar to untrusted input can errors.As one
// error type and answer with a precise bad-request instead of string-matching
// parser messages.
type ParseError struct {
	// Path locates the failure inside the filter document as a dotted key
	// path whose leaf is the offending key, e.g. "tags.$sizee" or
	// "$and.1.price.$gt". Array positions appear as indexes. Empty when the
	// failure concerns the top-level document itself.
	Path string
	// Op is the operator whose grammar was violated, including the leading
	// '$' (for an unknown operator: the unrecognized token itself). Empty
	// when the failure is not tied to an operator.
	Op string
	// Reason is the human-readable description of the rejection. It is
	// self-contained: rendering it without Path/Op loses location, not
	// meaning.
	Reason string

	// wrapped preserves the finer error class underneath the uniform type:
	// ErrUnknownOperator for unrecognized operators, ErrVectorNotOrderable
	// for ordering ops on vectors, the regexp.Compile error for a bad
	// $regex, … Nil when there is no finer class.
	wrapped error
}

func (e *ParseError) Error() string {
	if e.Path != "" {
		return "parse filter: " + e.Reason + " (at " + e.Path + ")"
	}
	return "parse filter: " + e.Reason
}

func (e *ParseError) Unwrap() error {
	return e.wrapped
}

// atPath prefixes seg onto the *ParseError's Path as the recursive descent
// unwinds, so the finished Path reads root-to-leaf. Errors of other types pass
// through unchanged. Mutating in place is safe: every ParseError is freshly
// allocated at its failure site and owned by the unwinding call chain.
func atPath(err error, seg string) error {
	var pe *ParseError
	if errors.As(err, &pe) {
		if pe.Path == "" {
			pe.Path = seg
		} else {
			pe.Path = seg + "." + pe.Path
		}
	}
	return err
}
