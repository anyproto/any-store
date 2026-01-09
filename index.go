package anystore

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/anyproto/go-sqlite"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/driver"
	"github.com/anyproto/any-store/internal/sql"
)

// IndexInfo provides information about an index.
type IndexInfo struct {
	// Name is the name of the index. If empty, it will be generated
	// based on the fields (e.g., "name,-createdDate").
	Name string `json:"name"`

	// Fields are the fields included in the index. Each field can specify
	// ascending (e.g., "name") or descending (e.g., "-createdDate") order.
	Fields []string `json:"fields"`

	// Unique indicates whether the index enforces a unique constraint.
	Unique bool `json:"unique"`

	// Sparse indicates whether the index is sparse, indexing only documents
	// with the specified fields.
	Sparse bool `json:"sparse"`
}

func (i IndexInfo) createName() string {
	return strings.Join(i.Fields, ",")
}

// Index represents an index on a collection.
type Index interface {
	// Info returns the IndexInfo for this index.
	Info() IndexInfo

	// Len returns the length of the index.
	Len(ctx context.Context) (int, error)
}

func newIndex(ctx context.Context, c *collection, info IndexInfo) (idx *index, err error) {
	idx = &index{info: info, c: c}
	if err = idx.init(ctx); err != nil {
		return nil, err
	}
	return
}

type index struct {
	c    *collection
	sql  sql.IndexSql
	info IndexInfo

	fieldNames []string
	fieldPaths [][]string
	reverse    []bool

	keyBuf      anyenc.Tuple
	keysBuf     []anyenc.Tuple
	keysBufPrev []anyenc.Tuple
	uniqBuf     [][]anyenc.Tuple

	stmts struct {
		insert,
		delete *driver.Stmt
	}
	queries struct {
		count string
	}
	stmtsReady      atomic.Bool
	driverValuesBuf [][]byte
}

func validateIndexField(s string) (err error) {
	if s == "" || s == "-" {
		return fmt.Errorf("index field is empty")
	}
	if strings.HasPrefix(s, "$") {
		return fmt.Errorf("invalid index field name: %s", s)
	}
	return nil
}

func parseIndexField(s string) (fields []string, reverse bool) {
	if strings.HasPrefix(s, "-") {
		return strings.Split(s[1:], "."), true
	}
	return strings.Split(s, "."), false
}

func (idx *index) init(ctx context.Context) (err error) {
	for _, field := range idx.info.Fields {
		fields, reverse := parseIndexField(field)
		for _, f := range fields {
			if f == "" {
				return fmt.Errorf("invalid index field: '%s'", field)
			}
		}
		idx.fieldNames = append(idx.fieldNames, strings.Join(fields, "."))
		idx.fieldPaths = append(idx.fieldPaths, fields)
		idx.reverse = append(idx.reverse, reverse)
	}
	idx.uniqBuf = make([][]anyenc.Tuple, len(idx.fieldPaths))
	idx.driverValuesBuf = make([][]byte, 0, len(idx.fieldNames))
	idx.sql = idx.c.sql.Index(idx.info.Name)
	idx.makeQueries()
	return nil
}

func (idx *index) makeQueries() {
	tableName := idx.sql.TableName()
	idx.queries.count = fmt.Sprintf("SELECT COUNT(*) FROM '%s'", tableName)
}

func (idx *index) checkStmts(ctx context.Context, cn *driver.Conn) (err error) {
	if idx.stmtsReady.CompareAndSwap(false, true) {
		if idx.stmts.insert, err = cn.Prepare(idx.sql.InsertStmt(len(idx.fieldNames))); err != nil {
			return err
		}
		if idx.stmts.delete, err = cn.Prepare(idx.sql.DeleteStmt(len(idx.fieldNames))); err != nil {
			return err
		}
	}
	return nil
}

func (idx *index) Info() IndexInfo {
	return idx.info
}

func (idx *index) Len(ctx context.Context) (count int, err error) {
	err = idx.c.db.doReadTx(ctx, func(cn *driver.Conn) error {
		err = cn.ExecCached(ctx, idx.queries.count, nil, func(stmt *sqlite.Stmt) error {
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
		return err
	})
	return
}

func (idx *index) Drop(ctx context.Context, cn *driver.Conn) (err error) {
	if err = cn.ExecNoResult(ctx, idx.sql.Drop()); err != nil {
		return
	}
	if err = idx.c.db.stmt.removeIndex.Exec(ctx, func(stmt *sqlite.Stmt) {
		stmt.SetText(":indexName", idx.info.Name)
		stmt.SetText(":collName", idx.c.name)
	}, driver.StmtExecNoResults); err != nil {
		return
	}
	return
}

func (idx *index) RenameColl(ctx context.Context, cn *driver.Conn, name string) (err error) {
	if err = cn.ExecNoResult(ctx, idx.sql.RenameColl(name)); err != nil {
		return err
	}
	idx.sql = idx.c.sql.Index(idx.info.Name)
	idx.makeQueries()
	idx.closeStmts()
	return idx.checkStmts(ctx, cn)
}

func (idx *index) Insert(ctx context.Context, id anyenc.Tuple, it item) error {
	idx.fillKeysBuf(it)
	return idx.insertBuf(ctx, id)
}

func (idx *index) Update(ctx context.Context, id anyenc.Tuple, prevIt, newIt item) (err error) {
	// calc previous index keys
	idx.fillKeysBuf(prevIt)

	// copy prev keys to second buffer
	idx.keysBufPrev = slices.Grow(idx.keysBufPrev, len(idx.keysBuf))[:len(idx.keysBuf)]
	for i, k := range idx.keysBuf {
		idx.keysBufPrev[i] = append(idx.keysBufPrev[i][:0], k...)
	}

	// calc new index keys
	idx.fillKeysBuf(newIt)

	// delete equal keys from both bufs
	idx.keysBuf = slices.DeleteFunc(idx.keysBuf, func(k anyenc.Tuple) bool {
		for i, pk := range idx.keysBufPrev {
			if bytes.Equal(k, pk) {
				idx.keysBufPrev = slices.Delete(idx.keysBufPrev, i, i+1)
				return true
			}
		}
		return false
	})
	if err = idx.deleteBuf(ctx, id, idx.keysBufPrev); err != nil {
		return err
	}
	return idx.insertBuf(ctx, id)
}

func (idx *index) Delete(ctx context.Context, id anyenc.Tuple, prevIt item) error {
	idx.fillKeysBuf(prevIt)
	return idx.deleteBuf(ctx, id, idx.keysBuf)
}

func (idx *index) writeKey() {
	nl := len(idx.keysBuf) + 1
	idx.keysBuf = slices.Grow(idx.keysBuf, nl)[:nl]
	idx.keysBuf[nl-1] = append(idx.keysBuf[nl-1][:0], idx.keyBuf...)
}

func (idx *index) writeValues(d *anyenc.Value, i int) bool {
	if i == len(idx.fieldPaths) {
		idx.writeKey()
		return true
	}
	v := d.Get(idx.fieldPaths[i]...)
	if idx.info.Sparse && (v == nil || v.Type() == anyenc.TypeNull) {
		return false
	}

	k := idx.keyBuf
	if v != nil && v.Type() == anyenc.TypeArray {
		arr, _ := v.Array()
		if len(arr) != 0 {
			idx.uniqBuf[i] = idx.uniqBuf[i][:0]
			for _, av := range arr {
				idx.keyBuf = av.MarshalTo(k)
				if idx.isUnique(i, idx.keyBuf) {
					if !idx.writeValues(d, i+1) {
						return false
					}
				}
			}
		}
	}

	idx.keyBuf = v.MarshalTo(k)
	return idx.writeValues(d, i+1)
}

func (idx *index) fillKeysBuf(it item) {
	idx.keysBuf = idx.keysBuf[:0]
	idx.keyBuf = idx.keyBuf[:0]
	idx.resetUnique()
	if !idx.writeValues(it.Value(), 0) {
		// we got false in case sparse index and nil value - reset the buffer
		idx.keysBuf = idx.keysBuf[:0]
	}
}

func (idx *index) resetUnique() {
	for i := range idx.uniqBuf {
		idx.uniqBuf[i] = idx.uniqBuf[i][:0]
	}
}

func (idx *index) isUnique(i int, k anyenc.Tuple) bool {
	for _, ek := range idx.uniqBuf[i] {
		if bytes.Equal(k, ek) {
			return false
		}
	}
	nl := len(idx.uniqBuf[i]) + 1
	idx.uniqBuf[i] = slices.Grow(idx.uniqBuf[i], nl)[:nl]
	idx.uniqBuf[i][nl-1] = append(idx.uniqBuf[i][nl-1][:0], k...)
	return true
}

func (idx *index) insertBuf(ctx context.Context, id []byte) (err error) {
	for _, k := range idx.keysBuf {
		err = idx.stmts.insert.Exec(ctx, func(stmt *sqlite.Stmt) {
			stmt.BindBytes(1, id)
			var i = 2
			_ = k.ReadBytes(func(b []byte) error {
				stmt.BindBytes(i, b)
				i++
				return nil
			})
		}, driver.StmtExecNoResults)
		if err != nil {
			return replaceUniqErr(err, ErrUniqueConstraint)
		}
	}
	return
}

func (idx *index) deleteBuf(ctx context.Context, id []byte, buf []anyenc.Tuple) (err error) {
	for _, k := range buf {
		err = idx.stmts.delete.Exec(ctx, func(stmt *sqlite.Stmt) {
			stmt.BindBytes(1, id)
			var i = 2
			_ = k.ReadBytes(func(b []byte) error {
				stmt.BindBytes(i, b)
				i++
				return nil
			})
		}, driver.StmtExecNoResults)
		if err != nil {
			return
		}
	}
	return
}

func (idx *index) closeStmts() {
	if idx.stmtsReady.CompareAndSwap(true, false) {
		for _, stmt := range []*driver.Stmt{
			idx.stmts.insert, idx.stmts.delete,
		} {
			_ = stmt.Close()
		}
	}
}

func (idx *index) Close() (err error) {
	idx.closeStmts()
	return
}
