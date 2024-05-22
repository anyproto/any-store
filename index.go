package anystore

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/conn"
	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/sql"
)

type IndexInfo struct {
	Name   string
	Fields []string
	Unique bool
	Sparse bool
}

func (i IndexInfo) createName() string {
	return strings.Join(i.Fields, ",")
}

type Index interface {
	Info() IndexInfo
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

	fieldPaths [][]string
	reverse    []bool

	idBuf           key.Key
	keyBuf          key.Key
	keysBuf         []key.Key
	keysBufPrev     []key.Key
	uniqBuf         [][]key.Key
	jvalsBuf        []*fastjson.Value
	driverValuesBuf []driver.NamedValue

	stmts struct {
		insert,
		delete,
		update conn.Stmt
	}
	queries struct {
		count string
	}
	stmtsReady atomic.Bool
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
		idx.fieldPaths = append(idx.fieldPaths, fields)
		idx.reverse = append(idx.reverse, reverse)
	}
	idx.driverValuesBuf = []driver.NamedValue{
		{Name: "docId"},
		{Name: "val"},
	}
	idx.sql = idx.c.sql.Index(idx.info.Name)
	idx.makeQueries()
	return nil
}

func (idx *index) makeQueries() {
	tableName := idx.sql.TableName()
	idx.queries.count = fmt.Sprintf("SELECT COUNT(*) FROM '%s'", tableName)
}

func (idx *index) checkStmts(ctx context.Context, cn conn.Conn) (err error) {
	if idx.stmtsReady.CompareAndSwap(false, true) {
		if idx.stmts.insert, err = idx.sql.InsertStmt(ctx, cn); err != nil {
			return err
		}
		if idx.stmts.update, err = idx.sql.UpdateStmt(ctx, cn); err != nil {
			return err
		}
		if idx.stmts.delete, err = idx.sql.DeleteStmt(ctx, cn); err != nil {
			return err
		}
	}
	return nil
}

func (idx *index) Info() IndexInfo {
	return idx.info
}

func (idx *index) Len(ctx context.Context) (count int, err error) {
	err = idx.c.db.doReadTx(ctx, func(cn conn.Conn) error {
		rows, err := cn.QueryContext(ctx, idx.queries.count, nil)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()
		count, err = readOneInt(rows)
		return err
	})
	return
}

func (idx *index) Drop(ctx context.Context, cn conn.Conn) (err error) {
	_, err = cn.ExecContext(ctx, idx.sql.Drop(), nil)
	return nil
}

func (idx *index) RenameColl(ctx context.Context, cn conn.Conn, name string) (err error) {
	if _, err = cn.ExecContext(ctx, idx.sql.RenameColl(name), nil); err != nil {
		return err
	}
	idx.sql = idx.c.sql.Index(idx.info.Name)
	idx.makeQueries()
	idx.closeStmts()
	return idx.checkStmts(ctx, cn)
}

func (idx *index) Insert(ctx context.Context, id key.Key, it item) error {
	idx.fillKeysBuf(it)
	return idx.insertBuf(ctx, id)
}

func (idx *index) writeKey() {
	nl := len(idx.keysBuf) + 1
	idx.keysBuf = slices.Grow(idx.keysBuf, nl)[:nl]
	idx.keysBuf[nl-1] = idx.keyBuf.CopyTo(idx.keysBuf[nl-1][:0])
}

func (idx *index) writeValues(d *fastjson.Value, i int) bool {
	if i == len(idx.fieldPaths) {
		idx.writeKey()
		return true
	}
	v := d.Get(idx.fieldPaths[i]...)
	if v == nil && idx.info.Sparse {
		return false
	}
	reverse := idx.reverse[i]

	k := idx.keyBuf[:0]
	if v != nil && v.Type() == fastjson.TypeArray {
		arr, _ := v.Array()
		if len(arr) != 0 {
			idx.uniqBuf[i] = idx.uniqBuf[i][:0]
			for _, av := range arr {
				if reverse {
					idx.keyBuf = k.AppendInvertedJSON(av)
				} else {
					idx.keyBuf = k.AppendJSON(av)
				}
				if idx.isUnique(i, idx.keyBuf[len(k):]) {
					if !idx.writeValues(d, i+1) {
						return false
					}
				}
			}
			return true
		}
	}
	if reverse {
		idx.keyBuf = k.AppendInvertedJSON(v)
	} else {
		idx.keyBuf = k.AppendJSON(v)
	}
	return idx.writeValues(d, i+1)
}

func (idx *index) fillKeysBuf(it item) {
	idx.keysBuf = idx.keysBuf[:0]
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

func (idx *index) isUnique(i int, k key.Key) bool {
	for _, ek := range idx.uniqBuf[i] {
		if bytes.Equal(k, ek) {
			return false
		}
	}
	nl := len(idx.uniqBuf[i]) + 1
	idx.uniqBuf[i] = slices.Grow(idx.uniqBuf[i], nl)[:nl]
	idx.uniqBuf[i][nl-1] = k.CopyTo(idx.uniqBuf[i][nl-1][:0])
	return true
}

func (idx *index) insertBuf(ctx context.Context, id []byte) (err error) {
	for _, k := range idx.keysBuf {
		_, err = idx.stmts.insert.ExecContext(ctx, idx.driverValues(id, k))
		if err != nil {
			return
		}
	}
	return
}

func (idx *index) driverValues(docId, val []byte) []driver.NamedValue {
	idx.driverValuesBuf[0].Value = docId
	idx.driverValuesBuf[1].Value = val
	return idx.driverValuesBuf
}

func (idx *index) closeStmts() {
	if idx.stmtsReady.CompareAndSwap(true, false) {
		for _, stmt := range []conn.Stmt{
			idx.stmts.insert, idx.stmts.update, idx.stmts.delete,
		} {
			_ = stmt.Close()
		}
	}
}

func (idx *index) Close() (err error) {
	idx.closeStmts()
	return
}
