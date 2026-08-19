package query

import "errors"

// ErrUnknownOperator is the class sentinel for input that names an operator
// outside its grammar's vocabulary — a filter operator ({"tags": {"$contains":
// "x"}}), a modifier ({"$sett": …}), an aggregation stage or accumulator. It
// is a client fault: callers parsing untrusted input can errors.Is it to
// answer with a bad-request rather than an internal error. The rejection
// itself is a *ParseError (errors.As) whose Op names the bad token.
var ErrUnknownOperator = errors.New("unknown operator")

// ParseError is the structured rejection for input that does not parse:
// a query filter, an update modifier, or an aggregation pipeline (Source
// tells which). Every parse-rejection class — unknown operator, wrong operand
// type, malformed $and/$or/$nor array, bad $regex, unknown stage, … — is
// reported as a *ParseError, so a caller exposing these grammars to untrusted
// input can errors.As one error type and answer with a precise bad-request
// instead of string-matching parser messages.
type ParseError struct {
	// Source names the grammar that rejected the input: "filter" (the
	// default; empty means filter), "modifier", or "pipeline".
	Source string
	// Path locates the failure inside the input document as a dotted key
	// path whose leaf is the offending key, e.g. "tags.$sizee",
	// "$and.1.price.$gt", or — pipeline errors, where the leading segment is
	// the stage index — "1.$match.a.$gt". Array positions appear as indexes.
	// Empty when the failure concerns the top-level document itself.
	Path string
	// Op is the operator whose grammar was violated, including the leading
	// '$' (for an unknown operator: the unrecognized token itself). Empty
	// when the failure is not tied to an operator.
	Op string
	// Reason is the human-readable description of the rejection. It is
	// self-contained: rendering it without Path/Op loses location, not
	// meaning.
	Reason string

	// Err preserves the finer error class underneath the uniform type:
	// ErrUnknownOperator for unrecognized operators, ErrVectorNotOrderable
	// for ordering ops on vectors, the regexp.Compile error for a bad
	// $regex, … Nil when there is no finer class.
	Err error
}

func (e *ParseError) Error() string {
	src := e.Source
	if src == "" {
		src = "filter"
	}
	if e.Path != "" {
		return "parse " + src + ": " + e.Reason + " (at " + e.Path + ")"
	}
	return "parse " + src + ": " + e.Reason
}

func (e *ParseError) Unwrap() error {
	return e.Err
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

// withSource stamps the grammar name onto the *ParseError at the API
// boundary (ParseModifier, the pipeline parser), so failure sites deep in a
// shared descent — a bad $match filter inside a pipeline — need not know
// which grammar they were reached from.
func withSource(err error, source string) error {
	var pe *ParseError
	if errors.As(err, &pe) {
		pe.Source = source
	}
	return err
}
