package anystore

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/internal/key"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/conn"
	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/sql"
	"github.com/anyproto/any-store/internal/syncpool"
)

type Collection interface {
	Name() string

	FindId(ctx context.Context, id any) (Doc, error)
	Query() Query

	InsertOne(ctx context.Context, doc any) (id any, err error)
	Insert(ctx context.Context, docs ...any) (err error)

	UpdateOne(ctx context.Context, doc any) (err error)
	UpsertOne(ctx context.Context, doc any) (id any, err error)

	DeleteOne(ctx context.Context, id any) (err error)

	Count(ctx context.Context) (count int, err error)

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

	queries struct {
		findId,
		count string
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
	c.makeQueries()
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

func (c *collection) makeQueries() {
	c.queries.findId = fmt.Sprintf("SELECT data FROM '%s' WHERE id = :id", c.tableName)
	c.queries.count = fmt.Sprintf("SELECT COUNT(*) FROM '%s'", c.tableName)
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

	id := encoding.AppendAnyValue(buf.SmallBuf[:0], docId)
	err = c.db.doReadTx(ctx, func(cn conn.Conn) (err error) {
		rows, err := cn.QueryContext(ctx, c.queries.findId, []driver.NamedValue{
			{Name: "id", Value: id},
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

func (c *collection) InsertOne(ctx context.Context, doc any) (id any, err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	var idBytes key.Key
	err = c.db.doWriteTx(ctx, func(cn conn.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		var it item
		if it, txErr = parseItem(buf.Parser, buf.Arena, doc, true); txErr != nil {
			return txErr
		}
		if idBytes, txErr = c.insertItem(ctx, cn, buf, it); txErr != nil {
			return txErr
		}
		return
	})
	if err != nil {
		return nil, replaceUniqErr(err, ErrDocExists)
	}
	if err = idBytes.ReadAnyValue(func(v any) error {
		id = v
		return nil
	}); err != nil {
		return
	}
	return id, nil
}

func (c *collection) Insert(ctx context.Context, docs ...any) (err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	err = c.db.doWriteTx(ctx, func(cn conn.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		var it item
		for _, doc := range docs {
			if it, txErr = parseItem(buf.Parser, buf.Arena, doc, true); txErr != nil {
				return txErr
			}
			if _, txErr = c.insertItem(ctx, cn, buf, it); txErr != nil {
				return txErr
			}
		}
		return
	})
	return replaceUniqErr(err, ErrDocExists)
}

func (c *collection) insertItem(ctx context.Context, cn conn.Conn, buf *syncpool.DocBuffer, it item) (id []byte, err error) {
	id = it.appendId(buf.SmallBuf[:0])
	if _, err = c.stmts.insert.ExecContext(ctx, []driver.NamedValue{
		{Name: "id", Value: id},
		{Name: "data", Value: it.Value().MarshalTo(buf.DocBuf[:0])},
	}); err != nil {
		return nil, replaceUniqErr(err, ErrDocExists)
	}
	if err = c.indexesHandleInsert(ctx, cn, it); err != nil {
		return nil, err
	}
	return
}

func (c *collection) UpdateOne(ctx context.Context, doc any) (err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	var it item
	if it, err = parseItem(buf.Parser, buf.Arena, doc, false); err != nil {
		return
	}

	return c.db.doWriteTx(ctx, func(cn conn.Conn) (txErr error) {
		return c.update(ctx, cn, it)
	})
}

func (c *collection) update(ctx context.Context, cn conn.Conn, it item) (err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	if err = c.checkStmts(ctx, cn); err != nil {
		return
	}

	idKey := it.appendId(buf.SmallBuf[:0])
	prevIt, err := c.loadById(ctx, buf, idKey)
	if err != nil {
		return
	}
	if _, err = c.stmts.update.ExecContext(ctx, []driver.NamedValue{
		{Name: "id", Value: idKey},
		{Name: "data", Value: it.Value().MarshalTo(buf.DocBuf[:0])},
	}); err != nil {
		return
	}

	return c.indexesHandleUpdate(ctx, cn, prevIt, it)
}

func (c *collection) loadById(ctx context.Context, buf *syncpool.DocBuffer, id key.Key) (it item, err error) {
	rows, err := c.stmts.findId.QueryContext(ctx, []driver.NamedValue{{Name: "id", Value: []byte(id)}})
	if err != nil {
		return
	}
	defer func() {
		_ = rows.Close()
	}()
	var dest = make([]driver.Value, 1)
	rErr := rows.Next(dest)
	if rErr != nil {
		if errors.Is(rErr, io.EOF) {
			return item{}, ErrDocNotFound
		}
		return item{}, rErr
	}
	return parseItem(buf.Parser, buf.Arena, dest[0], false)
}

func (c *collection) UpsertOne(ctx context.Context, doc any) (id any, err error) {
	//TODO implement me
	panic("implement me")
}

func (c *collection) DeleteOne(ctx context.Context, id any) (err error) {
	//TODO implement me
	panic("implement me")
}

func (c *collection) Count(ctx context.Context) (count int, err error) {
	err = c.db.doReadTx(ctx, func(cn conn.Conn) error {
		rows, txErr := cn.QueryContext(ctx, c.queries.count, nil)
		if txErr != nil {
			return txErr
		}
		defer func() {
			_ = rows.Close()
		}()
		if count, txErr = readOneInt(rows); txErr != nil {
			return txErr
		}
		return nil
	})
	return
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

func (c *collection) indexesHandleInsert(ctx context.Context, cn conn.Conn, it item) (err error) {
	return
}

func (c *collection) indexesHandleUpdate(ctx context.Context, cn conn.Conn, prevIt, newIt item) (err error) {
	return
}

func (c *collection) indexesHandleDelete(ctx context.Context, cn conn.Conn, prevIt item) (err error) {
	return
}

func (c *collection) Rename(ctx context.Context, newName string) error {
	return c.db.doWriteTx(ctx, func(cn conn.Conn) (err error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if _, err = c.db.stmt.renameCollection.ExecContext(ctx, []driver.NamedValue{
			{
				Name:    "oldName",
				Ordinal: 1,
				Value:   c.name,
			},
			{
				Name:    "newName",
				Ordinal: 2,
				Value:   newName,
			},
		}); err != nil {
			return
		}
		for _, idx := range c.indexes {
			if err = idx.renameColl(ctx, cn, newName); err != nil {
				return
			}
		}
		if _, err = cn.ExecContext(ctx, c.sql.Rename(newName), nil); err != nil {
			return err
		}
		c.name = newName
		c.sql = c.db.sql.Collection(newName)
		c.tableName = c.sql.TableName()
		c.makeQueries()
		c.closeStmts()
		return nil
	})
}

func (c *collection) Drop(ctx context.Context) error {
	return c.db.doWriteTx(ctx, func(cn conn.Conn) (err error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if err = c.Close(); err != nil {
			return err
		}
		for _, idx := range c.indexes {
			if err = idx.drop(ctx, cn); err != nil {
				return
			}
		}
		if _, err = c.db.stmt.renameCollection.ExecContext(ctx, []driver.NamedValue{
			{
				Name:    "collName",
				Ordinal: 1,
				Value:   c.name,
			},
		}); err != nil {
			return
		}
		if _, err = cn.ExecContext(ctx, c.sql.Drop(), nil); err != nil {
			return
		}
		return nil
	})
}

func (c *collection) closeStmts() {
	if c.stmtsReady.Load() {
		for _, stmt := range []conn.Stmt{
			c.stmts.insert, c.stmts.update, c.stmts.findId, c.stmts.delete,
		} {
			_ = stmt.Close()
		}
	}
}

func (c *collection) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.closeStmts()
	c.db.onCollectionClose(c.name)
	return nil
}
