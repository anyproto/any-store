package anystore

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/conn"
	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/sql"
)

type Collection interface {
	Name() string

	FindId(ctx context.Context, id any) (Doc, error)
	Query() Query

	Insert(ctx context.Context, doc ...any) (err error)

	UpdateId(ctx context.Context, id, doc any) (err error)
	UpsertId(ctx context.Context, id, doc any) (err error)

	EnsureIndex(ctx context.Context, info ...IndexInfo) (err error)
	DropIndex(ctx context.Context, indexName string) (err error)
	GetIndexes(ctx context.Context) (indexes []Index, err error)

	Rename(ctx context.Context, newName string) (err error)
	Drop(ctx context.Context) (err error)

	Close() error
}

func newCollection(ctx context.Context, db *db, name string) (Collection, error) {
	coll := &collection{
		name: name,
		sql:  db.sql.Collection(name),
		db:   db,
	}
	coll.tableName = coll.sql.TableName()
	if err := coll.init(ctx); err != nil {
		return nil, err
	}
	return coll, nil
}

type collection struct {
	name      string
	tableName string
	sql       sql.CollectionSql
	indexes   []*index
	db        *db

	stmts struct {
		insert,
		update,
		delete,
		findId conn.Stmt
	}

	stmtsReady atomic.Bool
	closed     atomic.Bool

	mu sync.Mutex
}

func (c *collection) init(ctx context.Context) error {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db.doReadTx(ctx, func(cn conn.Conn) (err error) {
		rows, err := cn.QueryContext(ctx, c.sql.FindIndexes(), []driver.NamedValue{
			{
				Name:    "collName",
				Ordinal: 1,
				Value:   c.name,
			},
		})
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()
		idxInfo, err := readIndexInfo(buf, rows)
		if err != nil {
			return err
		}
		for _, info := range idxInfo {
			idx, err := newIndex(ctx, c, info)
			if err != nil {
				return err
			}
			c.indexes = append(c.indexes, idx)
		}
		return nil
	})
}

func (c *collection) checkStmts(ctx context.Context, cn conn.Conn) (err error) {
	if !c.stmtsReady.CompareAndSwap(false, true) {
		return nil
	}
	if c.stmts.delete, err = c.sql.DeleteStmt(ctx, cn); err != nil {
		return
	}
	if c.stmts.insert, err = c.sql.InsertStmt(ctx, cn); err != nil {
		return
	}
	if c.stmts.update, err = c.sql.UpdateStmt(ctx, cn); err != nil {
		return
	}
	if c.stmts.findId, err = c.sql.FindIdStmt(ctx, cn); err != nil {
		return
	}
	return
}

func (c *collection) Name() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.name
}

func (c *collection) FindId(ctx context.Context, docId any) (doc Doc, err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	id := encoding.AppendAnyValue(nil, docId)
	err = c.db.doReadTx(ctx, func(cn conn.Conn) (err error) {
		rows, err := cn.QueryContext(ctx, fmt.Sprintf(`SELECT data FROM '%s' WHERE id = ?`, c.tableName), []driver.NamedValue{
			{Value: id, Ordinal: 1},
		})
		if err != nil {
			return err
		}
		var result = make([]driver.Value, 1)
		if err = rows.Next(result); err != nil {
			_ = rows.Close()
			if errors.Is(err, io.EOF) {
				return ErrDocNotFound
			}
			return err
		}
		data, err := fastjson.ParseBytes(result[0].([]byte))
		if err != nil {
			_ = rows.Close()
			return
		}
		if err = rows.Close(); err != nil {
			return err
		}
		doc = &item{val: data}
		return
	})
	return
}

func (c *collection) Query() Query {
	//TODO implement me
	panic("implement me")
}

func (c *collection) Insert(ctx context.Context, doc ...any) (err error) {
	//TODO implement me
	panic("implement me")
}

func (c *collection) UpdateId(ctx context.Context, id, doc any) (err error) {
	//TODO implement me
	panic("implement me")
}

func (c *collection) UpsertId(ctx context.Context, id, doc any) (err error) {
	//TODO implement me
	panic("implement me")
}

func (c *collection) EnsureIndex(ctx context.Context, info ...IndexInfo) (err error) {
	//TODO implement me
	panic("implement me")
}

func (c *collection) DropIndex(ctx context.Context, indexName string) (err error) {
	//TODO implement me
	panic("implement me")
}

func (c *collection) GetIndexes(ctx context.Context) (indexes []Index, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *collection) Rename(ctx context.Context, newName string) (err error) {
	//TODO implement me
	panic("implement me")
}

func (c *collection) Drop(ctx context.Context) (err error) {
	//TODO implement me
	panic("implement me")
}

func (c *collection) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	if c.stmtsReady.Load() {
		for _, stmt := range []conn.Stmt{
			c.stmts.insert, c.stmts.update, c.stmts.findId, c.stmts.delete,
		} {
			_ = stmt.Close()
		}
	}
	c.db.onCollectionClose(c.name)
	return nil
}
