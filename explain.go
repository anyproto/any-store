package anystore

import (
	"sort"
	"strings"

	"zombiezen.com/go/sqlite"
)

func scanExplainStmt(stmt *sqlite.Stmt) ([]string, error) {
	var es = &explainString{}
	for {
		hasRow, stepErr := stmt.Step()
		if stepErr != nil {
			return nil, stepErr
		}
		if !hasRow {
			break
		}

		es.addRow(explainRow{
			id:     stmt.ColumnInt64(0),
			parent: stmt.ColumnInt64(1),
			detail: stmt.ColumnText(3),
		})
	}
	return es.Result(), nil
}

type explainRow struct {
	id     int64
	parent int64
	detail string
}

type explainString struct {
	byParent map[int64][]explainRow
	buf      []string
}

func (es *explainString) addRow(r explainRow) {
	if es.byParent == nil {
		es.byParent = make(map[int64][]explainRow)
	}
	es.byParent[r.parent] = append(es.byParent[r.parent], r)
}

func (es *explainString) String() string {
	es.buf = es.buf[:0]
	es.string(0, 0)
	return strings.Join(es.buf, "\n")
}

func (es *explainString) Result() []string {
	es.buf = es.buf[:0]
	es.string(0, 0)
	return es.buf
}

func (es *explainString) string(parent, nest int64) {
	if _, ok := es.byParent[parent]; !ok {
		return
	}
	sort.Slice(es.byParent[parent], func(i, j int) bool {
		return es.byParent[parent][i].id < es.byParent[parent][j].id
	})

	for _, r := range es.byParent[parent] {
		var res string
		for range nest {
			res += "-"
		}
		if nest > 0 {
			res += " "
		}
		res += r.detail
		es.buf = append(es.buf, res)
		es.string(r.id, nest+1)
	}
}
