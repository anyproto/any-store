package anystore

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"strconv"
	"sync"

	"github.com/valyala/fastjson"

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
	qb, err := q.makeQuery()
	if err != nil {
		return &iterator{err: err}
	}
	sqlRes := qb.build(false)
	tx, err := q.c.db.ReadTx(ctx)
	if err != nil {
		return &iterator{err: err}
	}
	rows, err := tx.conn().QueryContext(ctx, sqlRes, qb.values)
	if err != nil {
		return &iterator{err: err}
	}
	return q.newIterator(rows, tx, qb)
}

func (q *collQuery) newIterator(rows driver.Rows, tx ReadTx, qb *queryBuilder) *iterator {
	return &iterator{
		rows: rows,
		dest: make([]driver.Value, 1),
		buf:  q.c.db.syncPool.GetDocBuf(),
		tx:   tx,
		qb:   qb,
	}
}

func (q *collQuery) Update(ctx context.Context, modifier any) (err error) {
	mod, err := query.ParseModifier(modifier)
	if err != nil {
		return
	}
	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	sqlRes := qb.build(false)

	tx, err := q.c.db.getWriteTx(ctx)
	if err != nil {
		qb.Close()
		return
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	if err = q.c.checkStmts(tx.Context(), tx.conn()); err != nil {
		qb.Close()
		return
	}

	rows, err := tx.conn().QueryContext(ctx, sqlRes, qb.values)
	if err != nil {
		qb.Close()
		return
	}
	iter := q.newIterator(rows, tx, qb)
	defer func() {
		_ = iter.Close()
	}()

	buf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(buf)

	for iter.Next() {
		var doc Doc
		if doc, err = iter.Doc(); err != nil {
			return
		}
		var (
			modifiedVal *fastjson.Value
			isModified  bool
		)
		buf.Arena.Reset()
		modifiedVal, isModified, err = mod.Modify(buf.Arena, copyItem(buf, doc.(item)).val)
		if err != nil {
			return
		}
		if !isModified {
			continue
		}
		var it item
		if it, err = newItem(modifiedVal, nil, false); err != nil {
			return
		}
		if err = q.c.update(tx.Context(), it, doc.(item)); err != nil {
			return
		}
	}
	err = iter.Err()
	return
}

func (q *collQuery) Delete(ctx context.Context) (err error) {
	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	sqlRes := qb.build(false)

	tx, err := q.c.db.getWriteTx(ctx)
	if err != nil {
		qb.Close()
		return
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	if err = q.c.checkStmts(tx.Context(), tx.conn()); err != nil {
		qb.Close()
		return
	}

	rows, err := tx.conn().QueryContext(ctx, sqlRes, qb.values)
	if err != nil {
		qb.Close()
		return
	}
	iter := q.newIterator(rows, tx, qb)
	defer func() {
		_ = iter.Close()
	}()

	buf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(buf)

	for iter.Next() {
		var doc Doc
		if doc, err = iter.Doc(); err != nil {
			return
		}
		id := doc.(item).appendId(buf.SmallBuf[:0])
		if err = q.c.deleteItem(tx.Context(), id, doc.(item)); err != nil {
			return
		}
	}
	err = iter.Err()
	return
}

func (q *collQuery) Count(ctx context.Context) (count int, err error) {
	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	defer qb.Close()
	sqlRes := qb.build(true)
	err = q.c.db.doReadTx(ctx, func(cn conn.Conn) (txErr error) {
		rows, txErr := cn.QueryContext(ctx, sqlRes, qb.values)
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
	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	defer qb.Close()
	query = qb.build(false)
	err = q.c.db.doReadTx(ctx, func(cn conn.Conn) (txErr error) {
		rows, txErr := cn.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, qb.values)
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

func (q *collQuery) makeQuery() (qb *queryBuilder, err error) {
	if q.err != nil {
		return nil, q.err
	}

	qb = newQueryBuilder()
	qb.coll = q.c
	qb.tableName = q.c.tableName
	qb.limit = int(q.limit)
	qb.offset = int(q.offset)

	if q.cond != nil {
		qb.filterId = q.c.db.filterReg.Register(q.cond)
	} else {
		q.cond = query.All{}
	}

	var sortFields []sort.SortField
	if q.sort != nil {
		sortFields = q.sort.Fields()
	}

	// handle "id" field
	if _, idBounds := q.cond.IndexFilter("id", nil); len(idBounds) != 0 {
		qb.idBounds = idBounds
	}

	var checkIdSort = func() {
		if len(sortFields) > 0 && sortFields[0].Field == "id" {
			qb.sorts = append(qb.sorts, qbSort{reverse: sortFields[0].Reverse})
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
				qb.sorts = append(qb.sorts, qbSort{
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
			qb.joins = append(qb.joins, join)
		}

	}

	if len(sortFields) > 0 && !exactIndexSort {
		qb.sortId = q.c.db.sortReg.Register(q.sort)
	}
	return
}

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

		qb.values = append(qb.values, driver.NamedValue{
			Name:  fieldName,
			Value: val,
		})
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

	var writeBound = func(join qbJoin, tableNum, fieldNum, boundNum int) {
		b := join.bounds[fieldNum][boundNum]

		// fast eq case
		if b.StartInclude && b.EndInclude && b.Start.Equal(b.End) {
			writeTableVal(join.tableName, fieldNum)
			qb.buf.WriteString(" = ")
			writePlaceholder(tableNum, fieldNum, boundNum, false, b.Start)
			return
		}

		if !b.Start.Empty() {
			writeTableVal(join.tableName, fieldNum)
			if b.StartInclude {
				qb.buf.WriteString(" >= ")
			} else {
				qb.buf.WriteString(" > ")
			}
			writePlaceholder(tableNum, fieldNum, boundNum, false, b.Start)
			needAnd = true
		}
		if !b.End.Empty() {
			if !b.Start.Empty() {
				writeAnd()
			}
			writeTableVal(join.tableName, fieldNum)
			if b.EndInclude {
				qb.buf.WriteString(" <= ")
			} else {
				qb.buf.WriteString(" < ")
			}
			writePlaceholder(tableNum, fieldNum, boundNum, true, b.End)
			needAnd = true
		}

	}

	var writeBounds = func(join qbJoin, tableNum int) {
		if len(join.bounds) == 0 {
			return
		}

		writeWhere()
		writeAnd()
		for fieldNum, bounds := range join.bounds {
			if len(bounds) == 0 {
				continue
			}
			if fieldNum != 0 {
				qb.buf.WriteString(" AND (")
			} else {
				qb.buf.WriteString(" (")
			}
			for i := range bounds {
				if i != 0 {
					qb.buf.WriteString(" OR (")
				} else {
					qb.buf.WriteString("(")
				}
				writeBound(join, tableNum, fieldNum, i)
				qb.buf.WriteString(")")
			}
			qb.buf.WriteString(")")
		}
	}

	if len(qb.idBounds) > 0 {
		writeBounds(qbJoin{bounds: []query.Bounds{qb.idBounds}}, 0)
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
