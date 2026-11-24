package query

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/parser"
)

// modifierOps is the modifier grammar's operator vocabulary as data: the
// single source of truth for modifier recognition. parseModRoot dispatches
// through it and ModifierOperators exports it — so the parser, its errors,
// and the advertised vocabulary cannot drift apart.
var modifierOps = map[string]func(key []byte, val *anyenc.Value) (Modifier, error){
	"$set":      newSetModifier,
	"$unset":    newUnsetModifier,
	"$inc":      newIncModifier,
	"$rename":   newRenameModifier,
	"$pop":      newPopModifier,
	"$push":     newPushModifier,
	"$pull":     newPullModifier,
	"$pullAll":  newPullAllModifier,
	"$addToSet": newAddToSetModifier,
}

// ModifierOperators returns the operator vocabulary accepted by the modifier
// parser — every top-level '$'-key ParseModifier recognizes, sorted and with
// the leading '$'. The slice is a fresh copy: callers may keep or mutate it.
// Use it to advertise the grammar (docs, error payloads) instead of
// hand-copying the list.
func ModifierOperators() []string {
	res := make([]string, 0, len(modifierOps))
	for name := range modifierOps {
		res = append(res, name)
	}
	sort.Strings(res)
	return res
}

func MustParseModifier(modifier any) Modifier {
	res, err := ParseModifier(modifier)
	if err != nil {
		panic(err)
	}
	return res
}

// ParseModifier parses an update modifier document. Rejections are reported
// as *ParseError with Source "modifier"; see ParseError.
//
// Modify returns the modified document as its result; read that, not the
// document passed in. A modifier with a path segment that names an array
// index past the padding limit (anyencutil.MaxArrayPadding) is applied once to
// a copy of the document, and the copy is the result: the document passed in
// is left as it was, whether the modifier succeeds or Modify returns an error.
// Any other modifier changes the document passed in and returns it. $set,
// $push and $addToSet store a copy of an operand that can be changed in
// place, so applying a modifier never changes it, and every document gets a
// value of its own.
//
// A modifier given as an *anyenc.Value is copied, so the parsed modifier does
// not depend on the caller's value after the call. It may nest at most 512
// levels, the modifier object being the first, as anyenc's parser reads, so
// an operand is at most 510 levels deep; a deeper one, or a value that
// contains itself, is a *ParseError.
func ParseModifier(modifier any) (Modifier, error) {
	if m, ok := modifier.(Modifier); ok {
		return m, nil
	}

	v, err := parser.Parse(modifier)
	if err != nil {
		return nil, err
	}
	if goValue, ok := modifier.(*anyenc.Value); ok {
		// Built in Go, the value has no parser's depth bound: copied, or
		// stored into a document, a value that contains itself would recurse
		// until the stack ran out. The check stops at the bound.
		if nestsDeeper(goValue, maxModifierDepth) {
			return nil, &ParseError{Source: "modifier", Reason: fmt.Sprintf("modifier is nested deeper than %d levels", maxModifierDepth)}
		}
		v = (&anyenc.Arena{}).Copy(goValue)
	}
	m, err := parseModRoot(v)
	if err != nil {
		return nil, withSource(err, "modifier")
	}
	if chain, ok := m.(ModifierChain); ok && mayExceedPadding(chain) {
		return paddingStaged{ModifierChain: chain}, nil
	}
	return m, nil
}

// maxModifierDepth is the deepest a modifier given as an *anyenc.Value may
// nest, the modifier object being the first level: the depth anyenc's parser
// reads, to which a modifier given as JSON or bytes is held by its parse.
const maxModifierDepth = 512

// nestsDeeper reports whether v nests objects and arrays more than levels
// deep, v included. It stops at the bound, so a value that contains itself
// is reported deeper rather than walked forever.
func nestsDeeper(v *anyenc.Value, levels int) bool {
	switch v.Type() {
	case anyenc.TypeObject:
		if levels == 0 {
			return true
		}
		deeper := false
		v.GetObject().Visit(func(_ []byte, f *anyenc.Value) {
			deeper = deeper || nestsDeeper(f, levels-1)
		})
		return deeper
	case anyenc.TypeArray:
		if levels == 0 {
			return true
		}
		for _, e := range v.GetArray() {
			if nestsDeeper(e, levels-1) {
				return true
			}
		}
	}
	return false
}

func parseModRoot(v *anyenc.Value) (m Modifier, err error) {
	if v.Type() != anyenc.TypeObject {
		return nil, &ParseError{Reason: "modifier must be an object"}
	}
	obj, _ := v.Object()
	root := ModifierChain{}
	obj.Visit(func(key []byte, v *anyenc.Value) {
		if err != nil {
			return
		}
		create, ok := modifierOps[string(key)]
		if !ok {
			err = atPath(&ParseError{
				Op:     string(key),
				Reason: "unknown modifier: " + string(key),
				Err:    ErrUnknownOperator,
			}, string(key))
			return
		}
		var mods ModifierChain
		if mods, err = parseMod(string(key), v, create); err != nil {
			err = atPath(err, string(key))
			return
		}
		root = append(root, mods...)
	})
	if err != nil {
		return nil, err
	}
	if len(root) > 0 {
		return root, nil
	}
	return nil, &ParseError{Reason: "empty modifier"}
}

func parseMod(op string, v *anyenc.Value, create func(key []byte, val *anyenc.Value) (Modifier, error)) (root ModifierChain, err error) {
	if v.Type() != anyenc.TypeObject {
		return nil, &ParseError{Op: op, Reason: op + " must be an object of field paths"}
	}
	obj, _ := v.Object()
	obj.Visit(func(key []byte, v *anyenc.Value) {
		if err != nil {
			return
		}

		if bytes.HasPrefix(key, opBytesPrefix) {
			err = atPath(&ParseError{
				Op:     string(key),
				Reason: "unexpected operator " + string(key) + ", expected a field path",
			}, string(key))
			return
		}
		var mod Modifier
		if mod, err = create(key, v); err != nil {
			err = atPath(err, string(key))
			return
		}
		root = append(root, mod)
	})
	return
}

func newSetModifier(key []byte, v *anyenc.Value) (Modifier, error) {
	return modifierSet{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}, nil
}

func newUnsetModifier(key []byte, _ *anyenc.Value) (Modifier, error) {
	return modifierUnset{
		fieldPath: strings.Split(string(key), "."),
	}, nil
}

func newIncModifier(key []byte, v *anyenc.Value) (Modifier, error) {
	if v.Type() != anyenc.TypeNumber {
		return nil, &ParseError{Op: "$inc", Reason: "$inc requires a numeric value, got " + v.String()}
	}
	return modifierInc{
		fieldPath: strings.Split(string(key), "."),
		val:       v.GetFloat64(),
	}, nil
}

func newRenameModifier(key []byte, v *anyenc.Value) (Modifier, error) {
	stringBytes, err := v.StringBytes()
	if err != nil {
		return nil, &ParseError{Op: "$rename", Reason: "$rename requires a string target field path, got " + v.String(), Err: err}
	}
	return modifierRename{
		fieldPath: strings.Split(string(key), "."),
		val:       strings.Split(string(stringBytes), "."),
	}, nil
}

func newPopModifier(key []byte, v *anyenc.Value) (Modifier, error) {
	value, err := v.Int()
	if err != nil || (value != 1 && value != -1) {
		return nil, &ParseError{Op: "$pop", Reason: "$pop must be 1 (last) or -1 (first), got " + v.String(), Err: err}
	}
	return modifierPop{
		fieldPath: strings.Split(string(key), "."),
		val:       value,
	}, nil
}

// refuseEachForm refuses, for $push and $addToSet, an operand object with a
// key that starts with '$'. MongoDB reads {"$each": [...]} there as the
// operator's modifiers (mods), which any-store does not implement: taken as a
// value, the object would be stored as it is where MongoDB stores its
// elements. Refusing every such key keeps those forms free to be implemented
// later without changing what an accepted update does.
func refuseEachForm(op, mods string, v *anyenc.Value) error {
	obj, err := v.Object()
	if err != nil {
		return nil
	}
	var dollarKey string
	obj.Visit(func(k []byte, _ *anyenc.Value) {
		if dollarKey == "" && bytes.HasPrefix(k, opBytesPrefix) {
			dollarKey = string(k)
		}
	})
	if dollarKey == "" {
		return nil
	}
	return &ParseError{
		Op:     op,
		Reason: op + " takes one value: its modifiers (" + mods + ") are not supported, and an object operand cannot have a key that starts with '$', got " + dollarKey,
	}
}

func newPushModifier(key []byte, v *anyenc.Value) (Modifier, error) {
	if err := refuseEachForm("$push", "$each, $position, $slice, $sort", v); err != nil {
		return nil, err
	}
	return modifierPush{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}, nil
}

func newPullModifier(key []byte, v *anyenc.Value) (Modifier, error) {
	var err error
	pull := modifierPull{
		fieldPath: strings.Split(string(key), "."),
	}
	if v.Type() == anyenc.TypeObject {
		// An object operand is a condition, as in Mongo; a malformed one is a
		// rejection, NOT a literal-equality fallback — a swallowed error here
		// makes the same bytes mean different pulls across library versions.
		if pull.filter, err = parseCompObj(v); err != nil {
			return nil, err
		}
		return pull, nil
	}
	pull.val = v
	return pull, nil
}

func newPullAllModifier(key []byte, v *anyenc.Value) (Modifier, error) {
	removedValues, err := v.Array()
	if err != nil {
		return nil, &ParseError{Op: "$pullAll", Reason: "$pullAll must be an array, got " + v.String(), Err: err}
	}
	return modifierPullAll{
		fieldPath:     strings.Split(string(key), "."),
		removedValues: removedValues,
	}, nil
}

func newAddToSetModifier(key []byte, v *anyenc.Value) (Modifier, error) {
	if err := refuseEachForm("$addToSet", "$each", v); err != nil {
		return nil, err
	}
	return modifierAddToSet{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}, nil
}
