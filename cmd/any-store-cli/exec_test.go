package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureStdout returns what fn prints: query results go straight to stdout.
func captureStdout(t *testing.T, fn func()) string {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	orig := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	os.Stdout = orig
	require.NoError(t, w.Close())
	return <-done
}

// Test_ExecPrintsAllResults: the shell pages query results, a single -e
// command (pageSize 0) prints all of them.
func Test_ExecPrintsAllResults(t *testing.T) {
	require.NoError(t, openConn(filepath.Join(t.TempDir(), "test.db")))
	defer func() {
		conn.closeLastIter()
		require.NoError(t, conn.db.Close())
		conn = nil
	}()
	docs := make([]string, 45)
	for i := range docs {
		docs[i] = fmt.Sprintf(`{"id":"d%02d"}`, i)
	}
	_, err := conn.Exec(`db.createCollection("coll")`)
	require.NoError(t, err)
	_, err = conn.Exec(`db.coll.insert(` + strings.Join(docs, ",") + `)`)
	require.NoError(t, err)

	docLines := func(cmd string) (int, string) {
		out := captureStdout(t, func() {
			_, err := conn.Exec(cmd)
			require.NoError(t, err)
		})
		return strings.Count(out, `{"id"`), out
	}

	n, out := docLines(`db.coll.find({})`)
	assert.Equal(t, 30, n)
	assert.Contains(t, out, `Type "it" for more`)

	conn.pageSize = 0
	n, out = docLines(`db.coll.find({})`)
	assert.Equal(t, 45, n)
	assert.NotContains(t, out, `Type "it"`)
	n, _ = docLines(`db.coll.find({}).limit(40)`)
	assert.Equal(t, 40, n)
}

// Test_ExecTrimsCommand: a command may span lines and carry surrounding
// whitespace, as a quoted multi-line -e argument does.
func Test_ExecTrimsCommand(t *testing.T) {
	require.NoError(t, openConn(filepath.Join(t.TempDir(), "test.db")))
	defer func() {
		conn.closeLastIter()
		require.NoError(t, conn.db.Close())
		conn = nil
	}()
	_, err := conn.Exec(`db.createCollection("coll")`)
	require.NoError(t, err)
	_, err = conn.Exec("\n  db.coll.insert(\n    {\"id\":\"a\"},\n    {\"id\":\"b\"}\n  )\n")
	require.NoError(t, err)
	res, err := conn.Exec("\n  db.coll.count()\n")
	require.NoError(t, err)
	assert.Equal(t, "2", res)
}
