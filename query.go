package anystore

import (
	"context"
	"errors"
	"slices"
	"sort"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/bitmap"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/query"
)

const maxIndexesInQuery = 1

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

	tx, err := q.c.db.getReadTx(ctx)
	if err != nil {
		qb.Close()
		return
	}

	cursor := tx.btreeReadTx().NewCursor(q.c.ns)

	var filter query.Filter
	if q.cond != nil {
		filter = q.cond
	}

	return &iterator{
		cursor: cursor,
		filter: filter,
		sorter: q.sort,
		buf:    q.c.db.syncPool.GetDocBuf(),
		tx:     tx,
		qb:     qb,
		limit:  int(q.limit),
		offset: int(q.offset),
	}, nil
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

	tx, err := q.c.db.WriteTx(ctx)
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

	btWtx := tx.btreeWriteTx()
	cursor := btWtx.NewCursor(q.c.ns)

	var filter query.Filter
	if q.cond != nil {
		filter = q.cond
	}

	iter := &iterator{
		cursor: cursor,
		filter: filter,
		buf:    q.c.db.syncPool.GetDocBuf(),
		tx:     tx,
		qb:     qb,
		limit:  int(q.limit),
		offset: int(q.offset),
	}
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
			modifiedVal *anyenc.Value
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
		if it, err = newItem(modifiedVal); err != nil {
			return
		}
		if _, err = q.c.update(btWtx, it, doc.(item)); err != nil {
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

	tx, err := q.c.db.WriteTx(ctx)
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

	btWtx := tx.btreeWriteTx()

	// First collect all docs to delete (can't modify while iterating)
	cursor := btWtx.NewCursor(q.c.ns)

	var filter query.Filter
	if q.cond != nil {
		filter = q.cond
	}

	iterBuf := q.c.db.syncPool.GetDocBuf()
	iter := &iterator{
		cursor: cursor,
		filter: filter,
		buf:    iterBuf,
		qb:     qb,
		limit:  int(q.limit),
		offset: int(q.offset),
	}

	buf := q.c.db.syncPool.GetDocBuf()
	defer q.c.db.syncPool.ReleaseDocBuf(buf)

	// Collect IDs to delete
	var idsToDelete [][]byte
	for iter.Next() {
		var doc Doc
		if doc, err = iter.Doc(); err != nil {
			q.c.db.syncPool.ReleaseDocBuf(iterBuf)
			qb.Close()
			return
		}
		id := doc.(item).appendId(buf.SmallBuf[:0])
		idsToDelete = append(idsToDelete, append([]byte(nil), id...))
	}
	if err = iter.Err(); err != nil {
		q.c.db.syncPool.ReleaseDocBuf(iterBuf)
		qb.Close()
		return
	}
	q.c.db.syncPool.ReleaseDocBuf(iterBuf)
	qb.Close()

	// Now delete collected docs
	for _, id := range idsToDelete {
		if err = q.c.deleteItem(btWtx, id); err != nil {
			return
		}
		result.Matched++
		result.Modified++
	}
	return
}

func (q *collQuery) Count(ctx context.Context) (count int, err error) {
	qb, err := q.makeQuery()
	if err != nil {
		return
	}
	defer qb.Close()

	err = q.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		cursor := tx.NewCursor(q.c.ns)
		buf := q.c.db.syncPool.GetDocBuf()
		defer q.c.db.syncPool.ReleaseDocBuf(buf)

		if err := cursor.First(); err != nil {
			return nil
		}
		var skipped, counted int
		for cursor.Valid() {
			if q.cond != nil {
				val, err := cursor.Value()
				if err != nil {
					return err
				}
				doc, err := buf.Parser.Parse(val)
				if err != nil {
					return err
				}
				if !q.cond.Ok(doc, buf) {
					if err := cursor.Next(); err != nil {
						return err
					}
					continue
				}
			}
			if q.offset > 0 && skipped < int(q.offset) {
				skipped++
				if err := cursor.Next(); err != nil {
					return err
				}
				continue
			}
			counted++
			if q.limit > 0 && counted >= int(q.limit) {
				break
			}
			if err := cursor.Next(); err != nil {
				return err
			}
		}
		count = counted
		return nil
	})
	return
}

func (q *collQuery) Explain(ctx context.Context) (explain Explain, err error) {
	_, err = q.makeQuery()
	if err != nil {
		return
	}

	explain.Sql = "FULL_SCAN"
	explain.SqliteExplain = []string{"btree full scan"}

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
	qb.limit = int(q.limit)
	qb.offset = int(q.offset)

	if q.cond == nil {
		q.cond = query.All{}
	}

	if q.sort != nil {
		q.sortFields = q.sort.Fields()
	}

	// handle "id" field
	if idBounds := q.cond.IndexBounds("id", nil); len(idBounds) != 0 {
		qb.idBounds = idBounds
	}

	// calculate weights (kept for index choosing logic, not used for actual queries)
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
		if q.indexesWithWeight[i].weight > 0 {
			for _, hint := range q.indexHints {
				if hint.IndexName == idx.info.Name {
					q.indexesWithWeight[i].weight += hint.Boost
				}
			}
		}
	}
	sort.Sort(q.indexesWithWeight)

	// filter useless indexes (kept for index choosing logic)
	var (
		usedFieldsBits bitmap.Bitmap256
		usedSortBits   bitmap.Bitmap256
		exactSortFound bool
	)
	for i, idx := range q.indexesWithWeight {
		if idx.weight < 1 {
			continue
		}
		if usedFieldsBits.Subtract(idx.queryFieldsBits).Count() != 0 ||
			usedSortBits.Subtract(idx.sortFieldsBits).Count() != 0 ||
			(!exactSortFound && idx.exactSort) {
			usedFieldsBits = usedFieldsBits.Or(idx.queryFieldsBits)
			usedSortBits = usedSortBits.Or(idx.sortFieldsBits)
			if idx.exactSort {
				exactSortFound = true
			}
			_ = i // index choosing logic preserved but not used
		}
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
	if weight > 0 && idx.info.Unique {
		weight++
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
					weight = 11
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
