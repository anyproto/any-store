package main

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test_ExtJSON_EndToEnd exercises the full MongoDB-style Extended-JSON round
// trip through the CLI: insert a document with objectID / binary / vector
// fields, look it up by its objectID id, and confirm the output renders the
// same wrappers. It also confirms that filtering by an objectID field value
// works (the filter carries a real objectID into the query engine).
func Test_ExtJSON_EndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	require.NoError(t, openConn(path))
	defer func() { conn = nil }()

	_, err := conn.db.CreateCollection(mainCtx.Ctx(), "coll")
	require.NoError(t, err)

	const oid = "0123456789abcdef01234567"
	doc := json.RawMessage(`{"id":{"$oid":"` + oid + `"},"b":{"$binary":"AQID"},"e":{"$vector":[1,2,3]}}`)
	_, err = conn.Insert(Cmd{Cmd: "insert", Collection: "coll", Documents: []json.RawMessage{doc}})
	require.NoError(t, err)

	// findId by objectID; the output round-trips all three wrappers.
	out, err := conn.FindId(Cmd{Cmd: "findId", Collection: "coll",
		Documents: []json.RawMessage{json.RawMessage(`{"$oid":"` + oid + `"}`)}})
	require.NoError(t, err)
	require.Contains(t, out, `"$oid":"`+oid+`"`)
	require.Contains(t, out, `"$binary":"AQID"`)
	require.Contains(t, out, `"$vector":[1,2,3]`)

	// filtering by the objectID field value matches the stored document.
	cnt, err := conn.Find(Cmd{Cmd: "find", Collection: "coll",
		Query: Query{Find: json.RawMessage(`{"id":{"$oid":"` + oid + `"}}`), Count: true}})
	require.NoError(t, err)
	require.Equal(t, "1", cnt)
}
