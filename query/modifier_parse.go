package query

import (
	"bytes"
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
func ParseModifier(modifier any) (Modifier, error) {
	if m, ok := modifier.(Modifier); ok {
		return m, nil
	}

	v, err := parser.Parse(modifier)
	if err != nil {
		return nil, err
	}
	m, err := parseModRoot(v)
	if err != nil {
		return nil, withSource(err, "modifier")
	}
	return m, nil
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

func newPushModifier(key []byte, v *anyenc.Value) (Modifier, error) {
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
	return modifierAddToSet{
		fieldPath: strings.Split(string(key), "."),
		val:       v,
	}, nil
}
