package sql

import (
	"context"
	"strings"

	"github.com/anyproto/any-store/internal/conn"
)

type DBSql struct {
	Namespace string
}

const dbInit = `
	CREATE TABLE IF NOT EXISTS '%ns_system_collections' (
		name TEXT NOT NULL PRIMARY KEY
	);
	CREATE TABLE IF NOT EXISTS '%ns_system_indexes' (
		name TEXT NOT NULL,
		collection TEXT NOT NULL,
		fields TEXT NOT NULL,
		isSparse BOOL NOT NULL DEFAULT FALSE,
		isUnique BOOL NOT NULL DEFAULT FALSE
	);
	CREATE UNIQUE INDEX IF NOT EXISTS '%ns_system_indexes_index' ON '%ns_system_indexes' (collection, name);
`

func (s DBSql) InitDB() string {
	return s.WithNS(dbInit)
}

func (s DBSql) Collection(name string) CollectionSql {
	return CollectionSql{
		DBSql:          s,
		CollectionName: name,
	}
}

func (s DBSql) RegisterCollectionStmt(ctx context.Context, c conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, c, s.WithNS(`INSERT INTO '%ns_system_collections' (name) VALUES (:collName)`))
}

func (s DBSql) RemoveCollectionStmt(ctx context.Context, c conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, c, s.WithNS(`DELETE FROM '%ns_system_collections' WHERE name = :collName`))
}

func (s DBSql) RenameCollectionStmt(ctx context.Context, c conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, c, s.WithNS(`UPDATE '%ns_system_collections' SET name = :newName WHERE name = :oldName`))
}

func (s DBSql) RenameCollectionIndexStmt(ctx context.Context, c conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, c, s.WithNS(`UPDATE '%ns_system_indexes' SET collection = :newName WHERE collection = :oldName`))
}

func (s DBSql) RegisterIndexStmt(ctx context.Context, c conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, c, s.WithNS(`
		INSERT INTO '%ns_system_indexes' (name, collection, fields, isSparse, isUnique) 
			VALUES(:indexName, :collName, :fields, :sparse, :unique)
	`))
}

func (s DBSql) RemoveIndexStmt(ctx context.Context, c conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, c, s.WithNS(`DELETE FROM '%ns_system_indexes' WHERE name = :indexName AND collection = :collName`))
}

func (s DBSql) FindCollection() string {
	return s.WithNS(`SELECT * FROM '%ns_system_collections' WHERE name = :collName`)
}
func (s DBSql) FindCollections() string {
	return s.WithNS(`SELECT name FROM '%ns_system_collections'`)
}

func (s DBSql) FindIndexes() string {
	return s.WithNS(`SELECT name, fields, isSparse, isUnique FROM '%ns_system_indexes' WHERE collection = :collName`)
}

func (s DBSql) CountIndexes() string {
	return s.WithNS(`SELECT COUNT(*) FROM '%ns_system_indexes'`)
}

func (s DBSql) CountCollections() string {
	return s.WithNS(`SELECT COUNT(*) FROM '%ns_system_collections'`)
}

func (s DBSql) StatsTotalSize() string {
	return `SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size();`
}

func (s DBSql) StatsDataSize() string {
	return `SELECT (page_count - freelist_count) * page_size as size FROM pragma_page_count(), pragma_freelist_count(), pragma_page_size();`
}

func (s DBSql) WithNS(sql string) string {
	return strings.ReplaceAll(sql, "%ns", s.Namespace)
}

func (s DBSql) Prepare(ctx context.Context, c conn.Conn, query string) (conn.Stmt, error) {
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.(conn.Stmt), nil
}
