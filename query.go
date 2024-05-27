package anystore

import (
	"context"
	"database/sql/driver"
	"errors"
	"strconv"
	"strings"

	"github.com/anyproto/any-store/internal/conn"

	"github.com/anyproto/any-store/internal/sort"
	"github.com/anyproto/any-store/query"
)

type Query interface {
	Cond(filter any) Query
	Limit(limit uint) Query
	Offset(offset uint) Query
	Sort(sort ...any) Query
	Iter(ctx context.Context) (iter Iterator)
	Count(ctx context.Context) (count int, err error)
	Update(ctx context.Context, modifier any) error
	Delete(ctx context.Context) (err error)
	Explain(ctx context.Context) (query, explain string, err error)
}

type collQuery struct {
	c *collection

	cond query.Filter
	sort sort.Sort

	limit, offset uint

	qb *queryBuilder

	sqlRes string

	err error
}

func (q *collQuery) Cond(filter any) Query {
	var err error
	if q.cond, err = query.ParseCondition(filter); err != nil {
		q.err = errors.Join(err)
	}
	return q
}

func (q *collQuery) Limit(limit uint) Query {
	q.limit = limit
	return q
}

func (q *collQuery) Offset(offset uint) Query {
	q.offset = offset
	return q
}

func (q *collQuery) Sort(sorts ...any) Query {
	var err error
	if q.sort, err = sort.ParseSort(sorts...); err != nil {
		q.err = errors.Join(err)
	}
	return q
}

func (q *collQuery) Iter(ctx context.Context) (iter Iterator) {
	if err := q.makeQuery(false); err != nil {
		return &iterator{err: err}
	}
	q.sqlRes = q.qb.build()
	tx, err := q.c.db.ReadTx(ctx)
	if err != nil {
		return &iterator{err: err}
	}
	rows, err := tx.conn().QueryContext(ctx, q.sqlRes, q.qb.values)
	if err != nil {
		return &iterator{err: err}
	}
	return &iterator{
		tx:   tx,
		rows: rows,
		dest: make([]driver.Value, 1),
		buf:  q.c.db.syncPool.GetDocBuf(),
		q:    q,
	}
}

func (q *collQuery) Update(ctx context.Context, modifier any) error {
	//TODO implement me
	panic("implement me")
}

func (q *collQuery) Delete(ctx context.Context) (err error) {
	//TODO implement me
	panic("implement me")
}

func (q *collQuery) Count(ctx context.Context) (count int, err error) {
	if err = q.makeQuery(true); err != nil {
		return
	}
	defer q.qb.release(q.c.db)
	q.sqlRes = q.qb.build()
	err = q.c.db.doReadTx(ctx, func(cn conn.Conn) (txErr error) {
		rows, txErr := cn.QueryContext(ctx, q.sqlRes, q.qb.values)
		if txErr != nil {
			return txErr
		}
		defer func() {
			_ = rows.Close()
		}()
		count, txErr = readOneInt(rows)
		return
	})
	return
}

func (q *collQuery) Explain(ctx context.Context) (query, explain string, err error) {
	if err = q.makeQuery(false); err != nil {
		return
	}
	defer q.qb.release(q.c.db)
	q.sqlRes = q.qb.build()
	query = q.sqlRes
	err = q.c.db.doReadTx(ctx, func(cn conn.Conn) (txErr error) {
		rows, txErr := cn.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, q.qb.values)
		if txErr != nil {
			return txErr
		}
		defer func() {
			_ = rows.Close()
		}()
		explain, txErr = scanExplainRows(rows)
		return
	})
	return
}

func (q *collQuery) makeQuery(count bool) (err error) {
	if q.err != nil {
		return q.err
	}

	q.qb = &queryBuilder{
		tableName: q.c.tableName,
		count:     count,
		buf:       &strings.Builder{},
		limit:     int(q.limit),
		offset:    int(q.offset),
	}

	if q.cond != nil {
		q.qb.filterId = q.c.db.filterReg.Register(q.cond)
	} else {
		q.cond = query.All{}
	}

	var sortFields []sort.SortField
	if q.sort != nil {
		sortFields = q.sort.Fields()
	}

	// handle "id" field
	if _, idBounds := q.cond.IndexFilter("id", nil); len(idBounds) != 0 {
		q.qb.idBounds = idBounds
	}

	var checkIdSort = func() {
		if len(sortFields) > 0 && sortFields[0].Field == "id" {
			q.qb.sorts = append(q.qb.sorts, qbSort{reverse: sortFields[0].Reverse})
			sortFields = sortFields[1:]
		}
	}
	checkIdSort()

	var exactIndexSort bool
	for _, idx := range q.c.indexes {
		var (
			hasFilters bool
			hasSorts   bool
			join       qbJoin
		)
		for _, field := range idx.fieldNames {
			_, bounds := q.cond.IndexFilter(field, nil)
			join.bounds = append(join.bounds, bounds)
			if len(bounds) > 0 {
				hasFilters = true
			}
		}

		if en := equalNum(sortFields, idx.fieldNames); en > 0 {
			for i := 0; i < en; i++ {
				q.qb.sorts = append(q.qb.sorts, qbSort{
					tableName: idx.sql.TableName(),
					fieldNum:  i,
					reverse:   sortFields[i].Reverse,
				})
			}
			if en == len(sortFields) {
				exactIndexSort = true
			}
			sortFields = sortFields[en:]
			checkIdSort()
			hasSorts = true
		}

		if hasSorts || hasFilters {
			join.tableName = idx.sql.TableName()
			if !hasFilters {
				join.bounds = nil
			}
			q.qb.joins = append(q.qb.joins, join)
		}

	}

	if len(sortFields) > 0 && !exactIndexSort {
		q.qb.sortId = q.c.db.sortReg.Register(q.sort)
	}
	return
}

type queryBuilder struct {
	tableName string
	count     bool
	joins     []qbJoin
	sorts     []qbSort
	idBounds  query.Bounds
	filterId  int
	sortId    int
	buf       *strings.Builder
	values    []driver.NamedValue
	limit     int
	offset    int
}

type qbJoin struct {
	idx       *index
	tableName string
	bounds    []query.Bounds
}

type qbSort struct {
	tableName string
	fieldNum  int
	reverse   bool
}

func (qb *queryBuilder) build() string {
	qb.buf.WriteString("SELECT ")
	if qb.count {
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

		qb.values = append(qb.values, driver.NamedValue{
			Name:  fieldName,
			Value: val,
		})
	}

	for i, b := range qb.idBounds {
		if b.Start.Empty() && b.End.Empty() {
			continue
		}

		// fast equal case
		if b.StartInclude && b.EndInclude && b.Start.Equal(b.End) {
			writeWhere()
			writeAnd()
			qb.buf.WriteString("id")
			qb.buf.WriteString(" = ")
			writePlaceholder(0, 0, i, false, b.Start)
			continue
		}
		if !b.Start.Empty() {
			writeWhere()
			writeAnd()
			qb.buf.WriteString("id")
			if b.StartInclude {
				qb.buf.WriteString(" >= ")
			} else {
				qb.buf.WriteString(" > ")
			}
			writePlaceholder(0, 0, i, false, b.Start)
			needAnd = true
		}
		if !b.End.Empty() {
			writeWhere()
			writeAnd()
			qb.buf.WriteString("id")
			if b.EndInclude {
				qb.buf.WriteString(" <= ")
			} else {
				qb.buf.WriteString(" < ")
			}
			writePlaceholder(0, 0, i, true, b.End)
			needAnd = true
		}
	}

	var writeTableVal = func(tableName string, fieldNum int) {
		qb.buf.WriteString(" '")
		qb.buf.WriteString(tableName)
		qb.buf.WriteString("'.val")
		qb.buf.WriteString(strconv.Itoa(fieldNum))
	}

	for tableNum, join := range qb.joins {
		tableNum += 1
		for fieldNum, bounds := range join.bounds {
			for i, b := range bounds {
				if b.Start.Empty() && b.End.Empty() {
					continue
				}

				// fast equal case
				if b.StartInclude && b.EndInclude && b.Start.Equal(b.End) {
					writeWhere()
					writeAnd()
					writeTableVal(join.tableName, fieldNum)
					qb.buf.WriteString(" = ")
					writePlaceholder(tableNum, fieldNum, i, false, b.Start)
					continue
				}
				if !b.Start.Empty() {
					writeWhere()
					writeAnd()
					writeTableVal(join.tableName, fieldNum)
					if b.StartInclude {
						qb.buf.WriteString(" >= ")
					} else {
						qb.buf.WriteString(" > ")
					}
					writePlaceholder(tableNum, fieldNum, i, false, b.Start)
					needAnd = true
				}
				if !b.End.Empty() {
					writeWhere()
					writeAnd()
					writeTableVal(join.tableName, fieldNum)
					if b.EndInclude {
						qb.buf.WriteString(" <= ")
					} else {
						qb.buf.WriteString(" < ")
					}
					writePlaceholder(tableNum, fieldNum, i, true, b.End)
					needAnd = true
				}
			}
		}
	}

	if qb.filterId > 0 {
		writeWhere()
		writeAnd()
		qb.buf.WriteString("any_filter(")
		qb.buf.WriteString(strconv.Itoa(qb.filterId))
		qb.buf.WriteString(", data) ")
	}

	if qb.count {
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
		qb.buf.WriteString(" OFFSET ")
		qb.buf.WriteString(strconv.Itoa(qb.offset))
	}

	return qb.buf.String()
}

func (qb *queryBuilder) release(db *db) {
	if qb != nil {
		if qb.filterId > 0 {
			db.filterReg.Release(qb.filterId)
		}
		if qb.sortId > 0 {
			db.sortReg.Release(qb.sortId)
		}
	}
}

func equalNum(sortFields []sort.SortField, indexFields []string) int {
	m := min(len(sortFields), len(indexFields))
	for n, sortField := range sortFields[:m] {
		if sortField.Field != indexFields[n] {
			return n
		}
		if n+1 == m {
			return m
		}
	}
	return 0
}
