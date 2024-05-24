package anystore

import (
	"context"
	"database/sql/driver"
	"errors"
	"github.com/anyproto/any-store/internal/conn"
	"strconv"
	"strings"

	"github.com/anyproto/any-store/internal/sort"
	"github.com/anyproto/any-store/query"
)

type Query interface {
	Cond(filter any) Query
	Limit(limit uint) Query
	Offset(offset uint) Query
	Sort(sort ...any) Query
	Iter() Iterator
	Count(ctx context.Context) (count int, err error)
	Update(ctx context.Context, modifier any) error
	Delete(ctx context.Context) (err error)
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

func (q *collQuery) Iter() Iterator {
	//TODO implement me
	panic("implement me")
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

func (q *collQuery) makeQuery(count bool) (err error) {
	if q.err != nil {
		return q.err
	}

	var filterId, sortId int

	if q.cond != nil {
		filterId = q.c.db.filterReg.Register(q.cond)
	}
	if q.sort != nil {
		sortId = q.c.db.sortReg.Register(q.sort)
	}

	q.qb = &queryBuilder{
		tableName: q.c.tableName,
		count:     count,
		filterId:  filterId,
		sortId:    sortId,
		buf:       &strings.Builder{},
	}
	return
}

type queryIndex struct {
	idx       *index
	filter    query.Filter
	bounds    query.Bounds
	reverse   bool
	exactSort bool
}

type queryBuilder struct {
	tableName string
	count     bool
	joins     []qbJoin
	filterId  int
	sortId    int
	buf       *strings.Builder
	values    []driver.NamedValue
}

type qbJoin struct {
	tableName string
	bounds    query.Bounds
	sort      []sort.SortField
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

	/*
		for _, join := range qb.joins {
			qb.buf.WriteString("\nJOIN '")
			qb.buf.WriteString(qb.tableName)
			qb.buf.WriteString("'.id = '")
			qb.buf.WriteString(join.tableName)
			qb.buf.WriteString("'.docId ")
		}
	*/

	var whereStarted, needAnd bool
	var writeWhere = func() {
		if !whereStarted {
			whereStarted = true
			qb.buf.WriteString("\nWHERE ")
		}
	}
	var writeAnd = func() {
		if needAnd {
			qb.buf.WriteString("\nAND ")
		} else {
			needAnd = true
		}
	}

	/*
		for _, join := range qb.joins {
			writeWhere()
			writeAnd()
			for _, b := range join.bounds {
				qb.buf.WriteString("\n\t'")
				qb.buf.WriteString(join.tableName)
				qb.buf.WriteString("'.val ")
			}
		}
	*/

	if qb.filterId > 0 {
		writeWhere()
		writeAnd()
		qb.buf.WriteString("\n\tany_filter(")
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
			qb.buf.WriteString("\nORDER BY ")
		} else {
			qb.buf.WriteString(", ")
		}
	}

	if qb.sortId > 0 {
		writeOrder()
		qb.buf.WriteString("any_sort(")
		qb.buf.WriteString(strconv.Itoa(qb.sortId))
		qb.buf.WriteString(", data)")
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
