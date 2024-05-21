package sql

import "strings"

type IndexSql struct {
	CollectionSql
	IndexName string
}

const indexCreate = `
	CREATE TABLE IF NOT EXISTS '%ns_%coll_%idx_idx' (
	val BLOB NOT NULL,
  	docId BLOB NOT NULL REFERENCES '%ns_%coll_docs' (id),
  	PRIMARY KEY (val, docId)
);
`

const indexUniqCreate = `
	CREATE TABLE IF NOT EXISTS '%ns_%coll_%idx_idx' (
	val BLOB NOT NULL,
  	docId BLOB NOT NULL REFERENCES '%ns_%coll_docs' (id),
  	PRIMARY KEY (val)
);
`

const indexDrop = `
	DROP TABLE '%ns_%coll_%idx_idx';
`

func (s IndexSql) Create(unique bool) string {
	if unique {
		return s.WithIndex(indexUniqCreate)
	}
	return s.WithIndex(indexCreate)
}

func (s IndexSql) Drop() string {
	return s.WithIndex(indexDrop)
}

func (s IndexSql) WithIndex(sql string) string {
	return strings.ReplaceAll(s.WithColl(sql), "%idx", s.IndexName)
}
