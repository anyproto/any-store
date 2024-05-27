package anystore

import (
	"database/sql/driver"
	"errors"
	"io"
	"sort"
	"strings"
)

func scanExplainRows(rows driver.Rows) (string, error) {
	var dest = make([]driver.Value, 4)
	var es = &explainString{}
	for {
		if rErr := rows.Next(dest); rErr != nil {
			if errors.Is(rErr, io.EOF) {
				return es.String(), nil
			} else {
				return "", rErr
			}
		}
		es.addRow(explainRow{
			id:     dest[0].(int64),
			parent: dest[1].(int64),
			detail: dest[3].(string),
		})
	}
}

type explainRow struct {
	id     int64
	parent int64
	detail string
}

type explainString struct {
	byParent map[int64][]explainRow
	buf      *strings.Builder
}

func (es *explainString) addRow(r explainRow) {
	if es.byParent == nil {
		es.byParent = make(map[int64][]explainRow)
	}
	es.byParent[r.parent] = append(es.byParent[r.parent], r)
}

func (es *explainString) String() string {
	es.string(0, 0)
	return es.buf.String()
}

func (es *explainString) string(parent, nest int64) {
	if _, ok := es.byParent[parent]; !ok {
		return
	}
	sort.Slice(es.byParent[parent], func(i, j int) bool {
		return es.byParent[parent][i].id < es.byParent[parent][j].id
	})
	if es.buf == nil {
		es.buf = &strings.Builder{}
	}
	for _, r := range es.byParent[parent] {
		for range nest {
			es.buf.WriteString("-")
		}
		if nest > 0 {
			es.buf.WriteString(" ")
		}
		es.buf.WriteString(r.detail)
		es.buf.WriteString("\n")
		es.string(r.id, nest+1)
	}
}
