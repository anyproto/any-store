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
		// $lt admits null/missing in this engine's type ordering (null sorts low).
		{"lt admits null", `{"a":{"$lt":5}}`, "a", false},
		{"lte admits null", `{"a":{"$lte":5}}`, "a", false},

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
