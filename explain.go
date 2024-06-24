package anystore

import (
	"database/sql/driver"
	"errors"
	"io"
	"sort"
	"strings"
)

func scanExplainRows(rows driver.Rows) ([]string, error) {
	var dest = make([]driver.Value, 4)
	var es = &explainString{}
	for {
		if rErr := rows.Next(dest); rErr != nil {
			if errors.Is(rErr, io.EOF) {
				return es.Result(), nil
			} else {
				return nil, rErr
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
