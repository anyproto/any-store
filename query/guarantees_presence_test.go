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
		// Presence guaranteed: matching docs must have a present, non-null value.
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

		// NOT guaranteed: a missing/null doc can match, so a sparse index on the
		// field would drop rows.
		{"unconstrained", `{"a":1}`, "b", false},
		{"exists false", `{"a":{"$exists":false}}`, "a", false},
		{"equality to null", `{"a":null}`, "a", false},
		{"ne", `{"a":{"$ne":5}}`, "a", false},
		{"in with null", `{"a":{"$in":[1,null]}}`, "a", false},
		// $exists:true admits an explicit-null doc, which a sparse index omits, so
		// it does NOT guarantee the sparse index is complete.
		{"exists true admits explicit null", `{"a":{"$exists":true}}`, "a", false},
		// {"$gte":null} / {"$lte":null} match null and missing.
		{"gte null admits null", `{"a":{"$gte":null}}`, "a", false},
		{"lte null admits null", `{"a":{"$lte":null}}`, "a", false},

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
