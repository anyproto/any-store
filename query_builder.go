package anystore

import (
	"bytes"
	"strconv"
	"sync"

	"github.com/anyproto/any-store/query"
)

var qbPool = &sync.Pool{
	New: func() any {
		return &queryBuilder{
			buf: &bytes.Buffer{},
		}
	},
}

func newQueryBuilder() *queryBuilder {
	return qbPool.Get().(*queryBuilder)
}

type queryBuilder struct {
	coll      *collection
	tableName string
	joins     []qbJoin
	sorts     []qbSort
	idBounds  query.Bounds
	filterId  int
	sortId    int
	buf       *bytes.Buffer
	values    [][]byte
	limit     int
	offset    int
}

type qbJoin struct {
	idx       *index
	tableName string
	bounds    []qbBounds
}

type qbBounds struct {
	fieldNum int
	bounds   query.Bounds
}

type qbSort struct {
	tableName string
	fieldNum  int
	reverse   bool
}

func (qb *queryBuilder) build(count bool) string {
	qb.buf.WriteString("SELECT ")
	if count {
		qb.buf.WriteString("COUNT(*)")
	} else {
		qb.buf.WriteString("data")
	}
	qb.buf.WriteString(" FROM '")
	qb.buf.WriteString(qb.tableName)
	qb.buf.WriteString("' ")

	for _, join := range qb.joins {
		qb.buf.WriteString("JOIN '")
		qb.buf.WriteString(join.tableName)
		qb.buf.WriteString("' ON '")
		qb.buf.WriteString(join.tableName)
		qb.buf.WriteString("'.docId = id ")
	}

	var whereStarted, needAnd bool
	var writeWhere = func() {
		if !whereStarted {
			whereStarted = true
			qb.buf.WriteString("WHERE ")
		}
	}
	var writeAnd = func() {
		if needAnd {
			qb.buf.WriteString(" AND ")
		} else {
			needAnd = true
		}
	}

	var writePlaceholder = func(tableNum, fieldNum, boundNum int, isEnd bool, val []byte) {
		fieldName := "val_" + strconv.Itoa(tableNum) + "_" + strconv.Itoa(fieldNum) + "_" + strconv.Itoa(boundNum)
		if isEnd {
			fieldName += "_end"
		}
		qb.buf.WriteString(":")
		qb.buf.WriteString(fieldName)

		qb.values = append(qb.values, val)
	}

	var writeTableVal = func(tableName string, fieldNum int) {
		if tableName == "" {
			qb.buf.WriteString("id")
		} else {
			qb.buf.WriteString("'")
			qb.buf.WriteString(tableName)
			qb.buf.WriteString("'.val")
			qb.buf.WriteString(strconv.Itoa(fieldNum))
		}
	}

	var writeBound = func(join qbJoin, tableNum, valNum, boundNum, fieldNum int) {
		b := join.bounds[valNum].bounds[boundNum]

		// fast eq case
		if b.StartInclude && b.EndInclude && bytes.Equal(b.Start, b.End) {
			writeTableVal(join.tableName, fieldNum)
			qb.buf.WriteString(" = ")
			writePlaceholder(tableNum, valNum, boundNum, false, b.Start)
			return
		}

		if len(b.Start) != 0 {
			writeTableVal(join.tableName, valNum)
			if b.StartInclude {
				qb.buf.WriteString(" >= ")
			} else {
				qb.buf.WriteString(" > ")
			}
			writePlaceholder(tableNum, valNum, boundNum, false, b.Start)
			needAnd = true
		}
		if len(b.End) != 0 {
			if len(b.Start) != 0 {
				writeAnd()
			}
			writeTableVal(join.tableName, valNum)
			if b.EndInclude {
				qb.buf.WriteString(" <= ")
			} else {
				qb.buf.WriteString(" < ")
			}
			writePlaceholder(tableNum, valNum, boundNum, true, b.End)
			needAnd = true
		}

	}

	var writeBounds = func(join qbJoin, tableNum int) {
		if len(join.bounds) == 0 {
			return
		}

		writeWhere()
		writeAnd()
		for valNum, bounds := range join.bounds {
			if len(bounds.bounds) == 0 {
				continue
			}
			if valNum != 0 {
				qb.buf.WriteString(" AND (")
			} else {
				qb.buf.WriteString(" (")
			}
			for i := range bounds.bounds {
				if i != 0 {
					qb.buf.WriteString(" OR (")
				} else {
					qb.buf.WriteString("(")
				}
				writeBound(join, tableNum, valNum, i, bounds.fieldNum)
				qb.buf.WriteString(")")
			}
			qb.buf.WriteString(")")
		}
	}

	if len(qb.idBounds) > 0 {
		writeBounds(qbJoin{bounds: []qbBounds{{bounds: qb.idBounds}}}, 0)
	}

	for tableNum, join := range qb.joins {
		tableNum += 1
		writeBounds(join, tableNum)
	}

	if qb.filterId > 0 {
		writeWhere()
		writeAnd()
		qb.buf.WriteString("any_filter(")
		qb.buf.WriteString(strconv.Itoa(qb.filterId))
		qb.buf.WriteString(", data) ")
	}

	if count {
		return qb.buf.String()
	}

	var orderStarted bool
	var writeOrder = func() {
		if !orderStarted {
			orderStarted = true
			qb.buf.WriteString(" ORDER BY ")
		} else {
			qb.buf.WriteString(", ")
		}
	}

	for _, s := range qb.sorts {
		writeOrder()
		if s.tableName != "" {
			qb.buf.WriteString("'")
			qb.buf.WriteString(s.tableName)
			qb.buf.WriteString("'.val")
			qb.buf.WriteString(strconv.Itoa(s.fieldNum))
		} else {
			qb.buf.WriteString("id")
		}
		if s.reverse {
			qb.buf.WriteString(" DESC")
		}
	}

	if qb.sortId > 0 {
		writeOrder()
		qb.buf.WriteString("any_sort(")
		qb.buf.WriteString(strconv.Itoa(qb.sortId))
		qb.buf.WriteString(", data)")
	}

	if qb.limit > 0 {
		qb.buf.WriteString(" LIMIT ")
		qb.buf.WriteString(strconv.Itoa(qb.limit))
	}
	if qb.offset > 0 {
		if qb.limit == 0 {
			qb.buf.WriteString(" LIMIT -1")
		}
		qb.buf.WriteString(" OFFSET ")
		qb.buf.WriteString(strconv.Itoa(qb.offset))
	}

	return qb.buf.String()
}

func (qb *queryBuilder) Close() {
	if qb != nil {
		if qb.filterId > 0 {
			qb.coll.db.filterReg.Release(qb.filterId)
		}
		if qb.sortId > 0 {
			qb.coll.db.sortReg.Release(qb.sortId)
		}
		qb.coll = nil
		qb.values = qb.values[:0]
		qb.sorts = qb.sorts[:0]
		qb.idBounds = qb.idBounds[:0]
		qb.joins = qb.joins[:0]
		qb.filterId = 0
		qb.sortId = 0
		qb.buf.Reset()
		qbPool.Put(qb)
	}
}
