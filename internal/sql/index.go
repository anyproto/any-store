package sql

import (
	"fmt"
	"strconv"
	"strings"
)

type IndexSql struct {
	CollectionSql
	IndexName string
}

const indexCreateHeader = `CREATE TABLE IF NOT EXISTS '%ns_%coll_%idx_idx' (`

func (s IndexSql) TableName() string {
	return s.WithIndex(`%ns_%coll_%idx_idx`)
}

func (s IndexSql) Create(unique bool, fieldsIsDesc []bool) string {
	header := s.WithIndex(indexCreateHeader)
	builder := &strings.Builder{}
	builder.WriteString(header)
	for i := range fieldsIsDesc {
		builder.WriteString("\n\tval")
		builder.WriteString(strconv.Itoa(i))
		builder.WriteString(" BLOB NOT NULL,")
	}
	builder.WriteString("\n\tdocId BLOB NOT NULL,")
	builder.WriteString("\n\tPRIMARY KEY (")
	for i, isDesc := range fieldsIsDesc {
		builder.WriteString("\n\tval")
		builder.WriteString(strconv.Itoa(i))
		if isDesc {
			builder.WriteString(" DESC")
		}
		if i != len(fieldsIsDesc)-1 {
			builder.WriteString(",")
		}
	}
	if !unique {
		builder.WriteString(", docId")
	}
	builder.WriteString(")\n)")
	return builder.String()
}

func (s IndexSql) Drop() string {
	return s.WithIndex(`DROP TABLE '%ns_%coll_%idx_idx'`)
}

func (s IndexSql) RenameColl(newCollName string) string {
	return s.With2Coll(s.WithIndex(`ALTER TABLE '%ns_%coll_%idx_idx' RENAME TO '%ns_%2coll_%idx_idx';`), newCollName)
}

func (s IndexSql) InsertStmt(numFields int) string {
	var fields = make([]string, 0, numFields+1)
	var values = make([]string, 0, numFields+1)
	fields = append(fields, "docId")
	values = append(values, ":docId")
	for i := 0; i < numFields; i++ {
		fields = append(fields, fmt.Sprintf("val%d", i))
		values = append(values, fmt.Sprintf(":val%d", i))
	}
	return fmt.Sprintf(s.WithIndex(`INSERT INTO '%ns_%coll_%idx_idx' (%s) VALUES (%s)`), strings.Join(fields, ", "), strings.Join(values, ", "))
}

func (s IndexSql) DeleteStmt(numFields int) string {
	var fields = make([]string, 0, numFields+1)
	fields = append(fields, "docId = :docId")
	for i := 0; i < numFields; i++ {
		fields = append(fields, fmt.Sprintf("val%d = :val%d", i, i))
	}
	return fmt.Sprintf(s.WithIndex(`DELETE FROM '%ns_%coll_%idx_idx' WHERE %s`), strings.Join(fields, " AND "))
}

func (s IndexSql) WithIndex(sql string) string {
	return strings.ReplaceAll(s.WithColl(sql), "%idx", s.IndexName)
}
