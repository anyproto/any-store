package sql

import (
	"context"
	"strings"

	"github.com/anyproto/any-store/internal/conn"
)

type CollectionSql struct {
	DBSql
	CollectionName string
}

const collCreate = `
	CREATE TABLE IF NOT EXISTS '%ns_%coll_docs' (
		id BLOB NOT NULL PRIMARY KEY,
		data JSONB NOT NULL
	);
`

const collDrop = `
	DROP TABLE '%ns_%coll_docs';
`

func (s CollectionSql) TableName() string {
	return s.WithColl(`%ns_%coll_docs`)
}

func (s CollectionSql) Create() string {
	return s.WithColl(collCreate)
}

func (s CollectionSql) Drop() string {
	return s.WithColl(collDrop)
}

func (s CollectionSql) Rename(newName string) string {
	return s.With2Coll(`ALTER TABLE '%ns_%coll_docs' RENAME TO '%ns_%2coll_docs';`, newName)
}

func (s CollectionSql) DeleteStmt(ctx context.Context, c conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, c, s.WithColl(`DELETE FROM '%ns_%coll_docs' WHERE id = :id`))
}

func (s CollectionSql) InsertStmt(ctx context.Context, c conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, c, s.WithColl(`INSERT INTO '%ns_%coll_docs' (id, data) VALUES (:id, :data)`))
}

func (s CollectionSql) UpdateStmt(ctx context.Context, c conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, c, s.WithColl(`UPDATE '%ns_%coll_docs' SET data = :data WHERE id = :id`))
}

func (s CollectionSql) FindIdStmt(ctx context.Context, c conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, c, s.WithColl(`SELECT data FROM '%ns_%coll_docs' WHERE id = :id`))
}

func (s CollectionSql) WithColl(sql string) string {
	return strings.ReplaceAll(s.WithNS(sql), "%coll", s.CollectionName)
}

func (s CollectionSql) With2Coll(sql, name string) string {
	return strings.ReplaceAll(s.WithColl(sql), "%2coll", name)
}

func (s CollectionSql) Index(indexName string) IndexSql {
	return IndexSql{CollectionSql: s, IndexName: indexName}
}
