package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
)

// Test_Migrate: db.migrate(path) copies the open database into a new file
// in the latest format, prints what it copied, and refuses a path that
// exists. The open database stays usable.
func Test_Migrate(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, openConn(filepath.Join(dir, "test.db")))
	defer func() {
		conn.closeLastIter()
		require.NoError(t, conn.db.Close())
		conn = nil
	}()
	for _, cmd := range []string{
		`db.createCollection("coll")`,
		`db.coll.insert({"id":"a","n":1},{"id":"b","n":2},{"id":"c","n":3})`,
		`db.coll.ensureIndex({"name":"n","fields":["n"]})`,
	} {
		_, err := conn.Exec(cmd)
		require.NoError(t, err, cmd)
	}

	dst := filepath.Join(dir, "new.db")
	result, err := conn.Exec(`db.migrate(` + strconv.Quote(dst) + `)`)
	require.NoError(t, err)
	assert.Contains(t, result, "Collections:\t1\n")
	assert.Contains(t, result, "Documents:\t3\n")
	assert.Contains(t, result, "Indexes:\t1\n")

	_, err = conn.Exec(`db.migrate(` + strconv.Quote(dst) + `)`)
	require.ErrorIs(t, err, anystore.ErrDestinationExists)
	_, err = conn.Exec(`db.migrate()`)
	require.Error(t, err)
	count, err := conn.Exec(`db.coll.count()`)
	require.NoError(t, err)
	assert.Equal(t, "3", count)

	ctx := context.Background()
	copied, err := anystore.Open(ctx, dst, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, copied.Close()) }()
	format, err := anystore.SchemaFormat(copied)
	require.NoError(t, err)
	assert.Equal(t, anystore.LatestSchemaFormat, format)
	coll, err := copied.OpenCollection(ctx, "coll")
	require.NoError(t, err)
	n, err := coll.Find(`{"n":{"$gte":2}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
}

// Test_MigrateResult: a copy that is published with a failure after its
// publication prints its report and the failure as a warning; any other
// error is the command's error.
func Test_MigrateResult(t *testing.T) {
	report := anystore.CopyReport{Collections: 1, Documents: 3, Indexes: 1}
	result, err := migrateResult(5, report, nil)
	require.NoError(t, err)
	assert.Contains(t, result, "Documents:\t3\n")
	assert.NotContains(t, result, "Warning")

	failure := fmt.Errorf("%w: %w", anystore.ErrCopyPublished, errors.New("cannot sync"))
	result, err = migrateResult(5, report, failure)
	require.NoError(t, err)
	assert.Contains(t, result, "Documents:\t3\n")
	assert.Contains(t, result, "\nWarning:\t"+failure.Error())

	result, err = migrateResult(5, anystore.CopyReport{}, anystore.ErrDestinationExists)
	require.ErrorIs(t, err, anystore.ErrDestinationExists)
	assert.Empty(t, result)
}

// Test_HelpMigrate: the command has its help entry.
func Test_HelpMigrate(t *testing.T) {
	c := &Conn{}
	help, err := c.Help(Cmd{Cmd: "help", Path: "migrate"})
	require.NoError(t, err)
	assert.Contains(t, help, `db.migrate("new.db")`)
	all, err := c.Help(Cmd{Cmd: "help"})
	require.NoError(t, err)
	assert.Contains(t, all, ".migrate(path)")
	assert.Contains(t, helpOutput(false), `db.migrate("new.db")`)
}
