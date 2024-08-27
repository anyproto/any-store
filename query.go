package anystore

import (
	"context"
	"errors"
	"slices"
	"sort"

	"github.com/valyala/fastjson"
	"zombiezen.com/go/sqlite"

	"github.com/anyproto/any-store/internal/bitmap"
	"github.com/anyproto/any-store/internal/driver"

	"github.com/anyproto/any-store/query"
)

const maxIndexesInQuery = 2

// ModifyResult represents the result of a modification operation.
type ModifyResult struct {
	// Matched is the number of documents matched by the query.
	Matched int

	// Modified is the number of documents that were actually modified.
	Modified int
}

// Query represents a query on a collection.
type Query interface {

	// Limit sets the maximum number of documents to return.
	Limit(limit uint) Query

	// Offset sets the number of documents to skip before starting to return results.
	Offset(offset uint) Query

	// Sort sets the sort order for the query results.
	Sort(sort ...any) Query

	// IndexHint adds or removes boost for some indexes
	IndexHint(hints ...IndexHint) Query

	// Iter executes the query and returns an Iterator for the results.
	Iter(ctx context.Context) (Iterator, error)

	// Count returns the number of documents matching the query.
	Count(ctx context.Context) (count int, err error)

	// Update modifies documents matching the query.
	Update(ctx context.Context, modifier any) (res ModifyResult, err error)

	// Delete removes documents matching the query.
	Delete(ctx context.Context) (res ModifyResult, err error)

	// Explain provides the query execution plan.
	Explain(ctx context.Context) (explain Explain, err error)
}

type Explain struct {
	Sql           string
	SqliteExplain []string
	Indexes       []IndexExplain
}

type IndexExplain struct {
	Name   string
	Weight int
	Used   bool
}

type IndexHint struct {
	IndexName string
	Boost     int
}

type collQuery struct {
	c *collection

	cond query.Filter
	sort query.Sort

	limit, offset uint

	indexesWithWeight weightedIndexes
	sortFields        []query.SortField
	queryFields       []queryField
	indexHints        []IndexHint

	err error
}

type queryField struct {
	field  string
	bounds query.Bounds
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

func (q *collQuery) IndexHint(hints ...IndexHint) Query {
	q.indexHints = hints
	return q
}

func (q *collQuery) Sort(sorts ...any) Query {
	var err error
	if q.sort, err = query.ParseSort(sorts...); err != nil {
		q.err = errors.Join(err)
	}
	return q
}

func (q *collQuery) Iter(ctx context.Context) (iter Iterator, err error) {
	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	sqlRes := qb.build(false)
	tx, err := q.c.db.ReadTx(ctx)
	if err != nil {
		return
	}
	stmt, err := tx.conn().Query(ctx, sqlRes)
	for i, val := range qb.values {
		stmt.BindBytes(i+1, val)
	}
	if err != nil {
		return
	}
	return q.newIterator(stmt, tx, qb), nil
}

func (q *collQuery) newIterator(stmt *sqlite.Stmt, tx ReadTx, qb *queryBuilder) *iterator {
	return &iterator{
		stmt: stmt,
		buf:  q.c.db.syncPool.GetDocBuf(),
		tx:   tx,
		qb:   qb,
	}
}

func (q *collQuery) Update(ctx context.Context, modifier any) (result ModifyResult, err error) {
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

	stmt, err := tx.conn().Query(ctx, sqlRes)
	if err != nil {
		qb.Close()
		return
	}
	for i, val := range qb.values {
		stmt.BindBytes(i+1, val)
	}
	iter := q.newIterator(stmt, tx, qb)
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

		result.Matched++
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
		result.Modified++
	}
	err = iter.Err()
	return
}

func (q *collQuery) Delete(ctx context.Context) (result ModifyResult, err error) {
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

	stmt, err := tx.conn().Query(ctx, sqlRes)
	if err != nil {
		qb.Close()
		return
	}
	for i, val := range qb.values {
		stmt.BindBytes(i+1, val)
	}
	iter := q.newIterator(stmt, tx, qb)
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
		result.Matched++
		result.Modified++
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
	err = q.c.db.doReadTx(ctx, func(cn *driver.Conn) (txErr error) {
		txErr = cn.ExecCached(ctx, sqlRes, func(stmt *sqlite.Stmt) {
			for i, val := range qb.values {
				stmt.BindBytes(i+1, val)
			}
		}, func(stmt *sqlite.Stmt) error {
			hasRow, stepErr := stmt.Step()
			if stepErr != nil {
				return stepErr
			}
			if !hasRow {
				return nil
			}
			count = stmt.ColumnInt(0)
			return nil
		})
		return
	})
	return
}

func (q *collQuery) Explain(ctx context.Context) (explain Explain, err error) {
	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	defer qb.Close()

	explain.Sql = qb.build(false)
	err = q.c.db.doReadTx(ctx, func(cn *driver.Conn) (txErr error) {
		txErr = cn.Exec(ctx, "EXPLAIN QUERY PLAN "+explain.Sql, func(stmt *sqlite.Stmt) {
			for i, val := range qb.values {
				stmt.BindBytes(i+1, val)
			}
		}, func(stmt *sqlite.Stmt) error {
			if explain.SqliteExplain, txErr = scanExplainStmt(stmt); txErr != nil {
				return txErr
			}
			return nil
		})
		return
	})
	for _, idx := range q.indexesWithWeight {
		explain.Indexes = append(explain.Indexes, IndexExplain{
			Name:   idx.Info().Name,
			Weight: idx.weight,
			Used:   idx.used,
		})
	}
	return
}

type indexWithWeight struct {
	*index
	weight          int
	pos             int
	queryFieldsBits bitmap.Bitmap256
	sortFieldsBits  bitmap.Bitmap256
	bounds          query.Bounds
	exactSort       bool
	used            bool
}

type weightedIndexes []indexWithWeight

func (w weightedIndexes) Len() int           { return len(w) }
func (w weightedIndexes) Less(i, j int) bool { return w[i].weight > w[j].weight }
func (w weightedIndexes) Swap(i, j int)      { w[i], w[j] = w[j], w[i] }

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

	if q.sort != nil {
		q.sortFields = q.sort.Fields()
	}

	var addedSorts bitmap.Bitmap256

	// handle "id" field
	if idBounds := q.cond.IndexBounds("id", nil); len(idBounds) != 0 {
		qb.idBounds = idBounds
	}

	var addIdSort = func(reverse bool) {
		qb.sorts = append(qb.sorts, qbSort{
			reverse: reverse,
		})
	}

	if len(q.sortFields) != 0 && q.sortFields[0].Field == "id" {
		// if an id field is first, other sorts will be useless
		q.sortFields = q.sortFields[:1]
		addedSorts = addedSorts.Set(uint8(0))
		addIdSort(q.sortFields[0].Reverse)
	}

	// calculate weights
	q.indexesWithWeight = make(weightedIndexes, len(q.c.indexes))
	for i, idx := range q.c.indexes {
		q.indexesWithWeight[i].index = idx
		q.indexesWithWeight[i].weight,
			q.indexesWithWeight[i].queryFieldsBits = q.indexQueryWeight(idx)
		if sw, sf := q.indexSortWeight(idx); sw > 0 {
			q.indexesWithWeight[i].weight += sw
			q.indexesWithWeight[i].sortFieldsBits = sf
			q.indexesWithWeight[i].exactSort = sf.CountLeadingOnes() == len(q.sortFields)
		}
		for _, hint := range q.indexHints {
			if hint.IndexName == idx.info.Name {
				q.indexesWithWeight[i].weight += hint.Boost
			}
		}
	}
	sort.Sort(q.indexesWithWeight)

	// filter useless indexes
	var (
		usedFieldsBits  bitmap.Bitmap256
		usedSortBits    bitmap.Bitmap256
		filteredIndexes = q.indexesWithWeight[:0]
		exactSortFound  bool
		exactSortIdx    int
	)
	for i, idx := range q.indexesWithWeight {
		if usedFieldsBits.Subtract(idx.queryFieldsBits).Count() != 0 ||
			usedSortBits.Subtract(idx.sortFieldsBits).Count() != 0 ||
			(!exactSortFound && idx.exactSort) {
			usedFieldsBits = usedFieldsBits.Or(idx.queryFieldsBits)
			usedSortBits = usedSortBits.Or(idx.sortFieldsBits)
			idx.pos = i
			filteredIndexes = append(filteredIndexes, idx)
			if idx.exactSort {
				exactSortFound = true
				exactSortIdx = len(filteredIndexes) - 1
			}
		}
	}

	if len(filteredIndexes) > maxIndexesInQuery {
		filteredIndexes = filteredIndexes[:maxIndexesInQuery]
	}

	for i, idx := range filteredIndexes {
		tableName := idx.sql.TableName()
		used := false
		join := qbJoin{
			idx:       idx.index,
			tableName: tableName,
		}
		idx.queryFieldsBits.Iterate(func(j int) {
			if len(q.queryFields[j].bounds) != 0 {
				join.bounds = append(join.bounds, q.queryFields[j].bounds)
				used = true
			}
		})
		if !exactSortFound {
			for j, field := range q.sortFields {
				if !addedSorts.Get(uint8(j)) {
					if idx.sortFieldsBits.Get(uint8(j)) {
						addedSorts = addedSorts.Set(uint8(j))
						qb.sorts = append(qb.sorts, qbSort{
							tableName: tableName,
							fieldNum:  slices.Index(idx.fieldNames, field.Field),
							reverse:   field.Reverse,
						})
						used = true
					} else if field.Field == "id" {
						addedSorts = addedSorts.Set(uint8(j))
						addIdSort(field.Reverse)
					}
				}
			}
		}
		if used || (exactSortFound && i == exactSortIdx) {
			qb.joins = append(qb.joins, join)
			q.indexesWithWeight[idx.pos].used = true
		}
	}

	if exactSortFound {
		idx := filteredIndexes[exactSortIdx]
		for j, field := range q.sortFields {
			if !addedSorts.Get(uint8(j)) && idx.sortFieldsBits.Get(uint8(j)) {
				addedSorts = addedSorts.Set(uint8(j))
				qb.sorts = append(qb.sorts, qbSort{
					tableName: idx.sql.TableName(),
					fieldNum:  slices.Index(idx.fieldNames, field.Field),
					reverse:   field.Reverse,
				})
			}
		}
	}

	if len(q.sortFields) > addedSorts.CountLeadingOnes() {
		qb.sortId = q.c.db.sortReg.Register(q.sort)
	}
	return
}

func (q *collQuery) queryField(field string) (queryField, int) {
	for i, f := range q.queryFields {
		if f.field == field {
			return f, i
		}
	}
	bounds := q.cond.IndexBounds(field, nil)
	f := queryField{
		field:  field,
		bounds: bounds,
	}
	q.queryFields = append(q.queryFields, f)
	return f, len(q.queryFields) - 1
}

func (q *collQuery) indexQueryWeight(idx *index) (weight int, fieldBits bitmap.Bitmap256) {
	var isChain = true
	for i, field := range idx.fieldNames {
		qField, fi := q.queryField(field)
		if len(qField.bounds) != 0 {
			if isChain {
				if i == 0 {
					weight = 10
				} else {
					weight *= 2
				}
			} else {
				weight += 2
			}
			if i < 256 {
				fieldBits = fieldBits.Set(uint8(fi))
			}
		} else {
			if isChain {
				isChain = false
				weight -= 1
			}
		}
	}
	return
}

func (q *collQuery) indexSortWeight(idx *index) (weight int, fieldBits bitmap.Bitmap256) {
	var isChain = true
	sortFields := q.sortFields
	if len(sortFields) > 256 {
		sortFields = sortFields[:256]
	}
	for i, sf := range sortFields {
		if isChain && i < len(idx.fieldNames) {
			if idx.fieldNames[i] == sf.Field {
				if i == 0 {
					weight = 10
				} else {
					weight *= 2
					if idx.reverse[i] == sf.Reverse {
						weight += 2
					}
				}
				fieldBits = fieldBits.Set(uint8(i))
				continue
			}
		}
		isChain = false
		if slices.Contains(idx.fieldNames, sf.Field) {
			weight += 5
			fieldBits = fieldBits.Set(uint8(i))
		} else {
			break
		}
	}
	return
}
