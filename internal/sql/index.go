package sql

import (
	"context"
	"strings"

	"github.com/anyproto/any-store/internal/conn"
)

type IndexSql struct {
	CollectionSql
	IndexName string
}

const indexCreate = `
	CREATE TABLE IF NOT EXISTS '%ns_%coll_%idx_idx' (
	val BLOB NOT NULL,
  	docId BLOB NOT NULL,
  	PRIMARY KEY (val, docId)
);
`

const indexUniqCreate = `
	CREATE TABLE IF NOT EXISTS '%ns_%coll_%idx_idx' (
	val BLOB NOT NULL,
  	docId BLOB NOT NULL,
  	PRIMARY KEY (val)
);
`

func (s IndexSql) TableName() string {
	return s.WithIndex(`%ns_%coll_%idx_idx`)
}

func (s IndexSql) Create(unique bool) string {
	if unique {
		return s.WithIndex(indexUniqCreate)
	}
	return s.WithIndex(indexCreate)
}

func (s IndexSql) Drop() string {
	return s.WithIndex(`DROP TABLE '%ns_%coll_%idx_idx'`)
}

func (s IndexSql) RenameColl(newCollName string) string {
	return s.With2Coll(s.WithIndex(`ALTER TABLE '%ns_%coll_%idx_idx' RENAME TO '%ns_%2coll_%idx_idx';`), newCollName)
}

func (s IndexSql) InsertStmt(ctx context.Context, cn conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, cn, s.WithIndex(`INSERT INTO '%ns_%coll_%idx_idx' (docId, val) VALUES (:docId, :val)`))
}

func (s IndexSql) UpdateStmt(ctx context.Context, cn conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, cn, s.WithIndex(`UPDATE '%ns_%coll_%idx_idx' SET val = :val WHERE docId = :docId`))
}

func (s IndexSql) DeleteStmt(ctx context.Context, cn conn.Conn) (conn.Stmt, error) {
	return s.Prepare(ctx, cn, s.WithIndex(`DELETE FROM '%ns_%coll_%idx_idx' WHERE docId = :docId`))
}

func (s IndexSql) WithIndex(sql string) string {
	return strings.ReplaceAll(s.WithColl(sql), "%idx", s.IndexName)
}
