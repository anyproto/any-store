package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGuaranteesPresence(t *testing.T) {
	tests := []struct {
		name  string
		cond  any
		field string
		want  bool
	}{
		// Presence guaranteed: every matching doc carries the field.
		{"equality", `{"a":1}`, "a", true},
		{"equality string", `{"a":"x"}`, "a", true},
		{"gt", `{"a":{"$gt":5}}`, "a", true},
		{"gte", `{"a":{"$gte":5}}`, "a", true},
		{"range both sides", `{"a":{"$gt":1,"$lt":5}}`, "a", true},
		// Ordering ops are type-bracketed: a missing field (null) is outside any
		// non-null operand's bracket, so $lt/$lte guarantee presence too.
		{"lt", `{"a":{"$lt":5}}`, "a", true},
		{"lte", `{"a":{"$lte":5}}`, "a", true},
		{"lt string", `{"a":{"$lt":"x"}}`, "a", true},
		// {"$gt":null} matches nothing at all — vacuously present.
		{"gt null", `{"a":{"$gt":null}}`, "a", true},
		{"in non-null", `{"a":{"$in":[1,2,3]}}`, "a", true},
		{"guaranteed among many fields", `{"a":1,"b":2}`, "b", true},
		// An explicit null is present: predicates that admit it while rejecting
		// a missing field still guarantee presence.
		{"exists true", `{"a":{"$exists":true}}`, "a", true},
		{"exists true with range", `{"a":{"$exists":true,"$gt":1}}`, "a", true},
		{"type null", `{"a":{"$type":"null"}}`, "a", true},
		{"type string", `{"a":{"$type":"string"}}`, "a", true},
		{"ne null", `{"a":{"$ne":null}}`, "a", true},

		// NOT guaranteed: a doc missing the field can match, so a sparse index on
		// the field would drop rows.
		{"unconstrained", `{"a":1}`, "b", false},
		{"exists false", `{"a":{"$exists":false}}`, "a", false},
		{"equality to null", `{"a":null}`, "a", false},
		{"ne", `{"a":{"$ne":5}}`, "a", false},
		{"in with null", `{"a":{"$in":[1,null]}}`, "a", false},
		{"nin", `{"a":{"$nin":[5]}}`, "a", false},
		{"not", `{"a":{"$not":{"$gt":5}}}`, "a", false},
		// {"$gte":null} / {"$lte":null} match null and missing.
		{"gte null admits missing", `{"a":{"$gte":null}}`, "a", false},
		{"lte null admits missing", `{"a":{"$lte":null}}`, "a", false},

		// A sibling predicate that admits a missing leaf AND bounds the field:
		// over an array the conjuncts match leaf by leaf, so its null bound can
		// be met through a leaf the sparse index never wrote.
		{"exists true with eq null", `{"a":{"$exists":true,"$eq":null}}`, "a", false},
		{"exists true and eq null", `{"$and":[{"a":{"$exists":true}},{"a":null}]}`, "a", false},
		{"exists true with gte null", `{"a":{"$exists":true,"$gte":null}}`, "a", false},
		{"exists true with in null", `{"a":{"$exists":true,"$in":[null,1]}}`, "a", false},
		{"exists true with ne", `{"a":{"$exists":true,"$ne":5}}`, "a", false},
		{"exists true beside or", `{"a":{"$exists":true},"$or":[{"a":null},{"a":1}]}`, "a", false},
		{"exists true beside parent elemMatch", `{"x.y":{"$exists":true},"x":{"$elemMatch":{"y":null}}}`, "x.y", false},
		// ...while one that contributes no bounds is harmless.
		{"exists true with not", `{"a":{"$exists":true,"$not":{"$gt":5}}}`, "a", true},
		{"exists true beside other field", `{"a":{"$exists":true},"b":null}`, "a", true},
		{"range beside parent elemMatch", `{"x.y":{"$gt":0},"x":{"$elemMatch":{"y":{"$gt":1}}}}`, "x.y", true},
		{"parent elemMatch alone", `{"x":{"$elemMatch":{"y":{"$gt":1}}}}`, "x.y", true},
		{"parent elemMatch on null", `{"x":{"$elemMatch":{"y":null}}}`, "x.y", false},

		// OR is not a conjunction — presence cannot be guaranteed.
		{"or", `{"$or":[{"a":1},{"b":2}]}`, "a", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := MustParseCondition(tc.cond)
			assert.Equal(t, tc.want, GuaranteesPresence(f, tc.field))
		})
	}
}

func TestPathIsParent(t *testing.T) {
	for _, tc := range []struct {
		path []string
		name string
		want bool
	}{
		{[]string{"x"}, "x.y", true},
		{[]string{"x", "y"}, "x.y.z", true},
		{[]string{"x"}, "x", false},
		{[]string{"x"}, "x.", false},
		{[]string{"x"}, "xy.z", false},
		{[]string{"x", "y"}, "x.y", false},
		{nil, "x", false},
	} {
		assert.Equal(t, tc.want, pathIsParent(tc.path, tc.name), "%v vs %q", tc.path, tc.name)
	}
}

func TestPathIs(t *testing.T) {
	for _, tc := range []struct {
		path []string
		name string
		want bool
	}{
		{[]string{"a"}, "a", true},
		{[]string{"a", "b"}, "a.b", true},
		{[]string{"a", "b"}, "a.bc", false},
		{[]string{"a", "b"}, "a", false},
		{[]string{"a"}, "a.b", false},
		{[]string{"ab"}, "a.b", false},
		{[]string{"a", "b"}, "ab", false},
		{nil, "", false},
	} {
		assert.Equal(t, tc.want, pathIs(tc.path, tc.name), "%v vs %q", tc.path, tc.name)
	}
}
