package main

import (
	"path/filepath"
	"strings"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/query"
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
	printHelp(&b, "any-store-cli2", forAgent)
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

func Test_HelpListsVocabulary(t *testing.T) {
	out := helpOutput(false)
	for _, list := range [][]string{query.Operators(), query.ModifierOperators(), anystore.AggregateStages(), anystore.AggregateAccumulators()} {
		for _, op := range list {
			assert.Contains(t, out, " "+op, "help must list %s", op)
		}
	}
}

func Test_HelpScriptingOnlyForAgents(t *testing.T) {
	assert.NotContains(t, helpOutput(false), "\nScripting\n")
	agent := helpOutput(true)
	assert.Contains(t, agent, "\nScripting\n")
	assert.NotContains(t, agent, "{name}")
	assert.True(t, strings.HasPrefix(agent, helpOutput(false)), "the agent help extends the human help")
}
