package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/anyproto/any-store/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helpExampleData is the shop.db the help examples describe.
var helpExampleData = []string{
	`db.createCollection("customers")`,
	`db.createCollection("orders")`,
	`db.customers.insert({"id":"c1","name":"Ada","city":"Berlin"},{"id":"c2","name":"Bo","city":"Paris"},{"id":"c3","name":"Cy","city":"Berlin"})`,
	`db.orders.insert(` +
		`{"id":"o1","customerId":"c1","status":"paid","total":42.5,"items":["pen","ink"],"placed":"2026-08-30"},` +
		`{"id":"o2","customerId":"c2","status":"open","total":12,"items":["pen"],"placed":"2026-09-02"},` +
		`{"id":"o3","customerId":"c1","status":"open","total":99.9,"items":["desk","pen"],"placed":"2026-09-05"},` +
		`{"id":"o4","customerId":"c3","status":"cancelled","total":5,"items":["ink"],"placed":"2026-09-06"},` +
		`{"id":"o5","customerId":"c2","status":"paid","total":30,"items":["paper","pen"],"placed":"2026-09-10"})`,
}

func helpOutput(forAgent bool) string {
	var b strings.Builder
	printHelp(&b, "any-store-cli", forAgent)
	return b.String()
}

// Test_HelpExamples runs the help's examples, in order, on the data they
// describe and checks the output line the help shows for each.
func Test_HelpExamples(t *testing.T) {
	require.NoError(t, openConn(filepath.Join(t.TempDir(), "shop.db")))
	defer func() {
		conn.closeLastIter()
		require.NoError(t, conn.db.Close())
		conn = nil
	}()
	for _, cmd := range helpExampleData {
		_, err := conn.Exec(cmd)
		require.NoError(t, err, cmd)
	}
	conn.pageSize = 0 // as with -e
	for _, e := range helpExamples {
		var result string
		out := captureStdout(t, func() {
			var err error
			result, err = conn.Exec(e.cmd)
			require.NoError(t, err, e.cmd)
		})
		assert.Contains(t, out+result, e.out, e.cmd)
	}
}

// Test_HelpOperatorsParse checks that every operator the help lists is one
// the parsers accept.
func Test_HelpOperatorsParse(t *testing.T) {
	operand := map[string]any{
		"$and": []any{map[string]any{"a": 1}}, "$or": []any{map[string]any{"a": 1}}, "$nor": []any{map[string]any{"a": 1}},
		"$in": []any{1}, "$nin": []any{1}, "$all": []any{1}, "$not": map[string]any{"$eq": 1},
		"$exists": true, "$type": "string", "$regex": "^a", "$size": 1,
	}
	for _, op := range helpFilterOps {
		var cond any = map[string]any{"a": map[string]any{op: valueOr(operand, op, 1)}}
		if op == "$and" || op == "$or" || op == "$nor" {
			cond = map[string]any{op: operand[op]}
		}
		_, err := query.ParseCondition(cond)
		assert.NoError(t, err, op)
	}
	modOperand := map[string]any{"$rename": "b", "$pullAll": []any{1}, "$unset": ""}
	for _, op := range helpModifierOps {
		_, err := query.ParseModifier(map[string]any{op: map[string]any{"a": valueOr(modOperand, op, 1)}})
		assert.NoError(t, err, op)
	}
}

func valueOr(m map[string]any, k string, def any) any {
	if v, ok := m[k]; ok {
		return v
	}
	return def
}

func Test_HelpScriptingOnlyForAgents(t *testing.T) {
	assert.NotContains(t, helpOutput(false), "\nScripting\n")
	agent := helpOutput(true)
	assert.Contains(t, agent, "\nScripting\n")
	assert.NotContains(t, agent, "{name}")
	assert.True(t, strings.HasPrefix(agent, helpOutput(false)), "the agent help extends the human help")
}
