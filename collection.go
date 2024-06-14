package anystore

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/internal/key"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/conn"
	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/sql"
	"github.com/anyproto/any-store/internal/syncpool"
)

// Collection represents a collection of documents.
type Collection interface {
	// Name returns the name of the collection.
	Name() string

	// FindId finds a document by its ID.
	// Returns the document or an error if the document is not found.
	FindId(ctx context.Context, id any) (Doc, error)

	// Find returns a new Query object with given filter
	Find(filter any) Query

	// InsertOne inserts a single document into the collection.
	// Returns the ID of the inserted document or an error if the insertion fails.
	InsertOne(ctx context.Context, doc any) (id any, err error)

	// Insert inserts multiple documents into the collection.
	// Returns an error if the insertion fails.
	Insert(ctx context.Context, docs ...any) (err error)

	// UpdateOne updates a single document in the collection.
	// Returns an error if the update fails.
	UpdateOne(ctx context.Context, doc any) (err error)

	// UpsertOne inserts a document if it does not exist, or updates it if it does.
	// Returns the ID of the upserted document or an error if the operation fails.
	UpsertOne(ctx context.Context, doc any) (id any, err error)

	// DeleteId deletes a single document by its ID.
	// Returns an error if the deletion fails.
	DeleteId(ctx context.Context, id any) (err error)

	// Count returns the number of documents in the collection.
	// Returns the count of documents or an error if the operation fails.
	Count(ctx context.Context) (count int, err error)

	// EnsureIndex ensures an index exists on the specified fields.
	// Returns an error if the operation fails.
	EnsureIndex(ctx context.Context, info ...IndexInfo) (err error)

	// DropIndex drops an index by its name.
	// Returns an error if the operation fails.
	DropIndex(ctx context.Context, indexName string) (err error)

	// GetIndexes returns a list of indexes on the collection.
	GetIndexes() (indexes []Index)

	// Rename renames the collection.
	// Returns an error if the operation fails.
	Rename(ctx context.Context, newName string) (err error)

	// Drop drops the collection.
	// Returns an error if the operation fails.
	Drop(ctx context.Context) (err error)

	// ReadTx starts a new read-only transaction. It's just a proxy to db object.
	// Returns a ReadTx or an error if there is an issue starting the transaction.
	ReadTx(ctx context.Context) (ReadTx, error)

	// WriteTx starts a new read-write transaction. It's just a proxy to db object.
	// Returns a WriteTx or an error if there is an issue starting the transaction.
	WriteTx(ctx context.Context) (WriteTx, error)

	// Close closes the collection.
	// Returns an error if the operation fails.
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
		findAll,
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
	c.queries.findAll = fmt.Sprintf("SELECT data FROM '%s'", c.tableName)
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
	for _, idx := range c.indexes {
		if err = idx.checkStmts(ctx, cn); err != nil {
			return err
		}
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

func (c *collection) Find(filter any) Query {
	q := &collQuery{c: c}
	if filter != nil {
		return q.Cond(filter)
	} else {
		return q
	}
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
		buf.Arena.Reset()
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
			buf.Arena.Reset()
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
	if _, err = c.stmts.insert.ExecContext(ctx, buf.DriverValues(id, it.Value().MarshalTo(buf.DocBuf[:0]))); err != nil {
		return nil, replaceUniqErr(err, ErrDocExists)
	}
	if err = c.indexesHandleInsert(ctx, id, it); err != nil {
		return nil, err
	}
	return
}

func (c *collection) UpdateOne(ctx context.Context, doc any) (err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	var it item
	if it, err = parseItem(buf.Parser, nil, doc, false); err != nil {
		return
	}

	return c.db.doWriteTx(ctx, func(cn conn.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		return c.update(ctx, it, item{})
	})
}

func (c *collection) update(ctx context.Context, it, prevIt item) (err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	idKey := it.appendId(buf.SmallBuf[:0])
	if prevIt.val == nil {
		prevIt, err = c.loadById(ctx, buf, idKey)
		if err != nil {
			return
		}
	}
	if _, err = c.stmts.update.ExecContext(ctx, buf.DriverValues(idKey, it.Value().MarshalTo(buf.DocBuf[:0]))); err != nil {
		return
	}

	return c.indexesHandleUpdate(ctx, idKey, prevIt, it)
}

func (c *collection) loadById(ctx context.Context, buf *syncpool.DocBuffer, id key.Key) (it item, err error) {
	rows, err := c.stmts.findId.QueryContext(ctx, buf.DriverValuesId(id))
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

	return parseItem(buf.Parser, nil, dest[0], false)
}

func (c *collection) UpsertOne(ctx context.Context, doc any) (id any, err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	var it item
	buf.Arena.Reset()
	if it, err = parseItem(buf.Parser, buf.Arena, doc, true); err != nil {
		return
	}

	err = c.db.doWriteTx(ctx, func(cn conn.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		_, insErr := c.insertItem(ctx, cn, buf, it)
		if errors.Is(insErr, ErrDocExists) {
			return c.update(ctx, it, item{})
		}
		return insErr
	})
	if err != nil {
		return nil, err
	}

	if err = key.Key(it.appendId(buf.SmallBuf[:0])).ReadAnyValue(func(v any) error {
		id = v
		return nil
	}); err != nil {
		return
	}
	return id, nil
}

func (c *collection) DeleteId(ctx context.Context, id any) (err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	return c.db.doWriteTx(ctx, func(cn conn.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		idKey := encoding.AppendAnyValue(buf.SmallBuf[:0], id)
		it, txErr := c.loadById(ctx, buf, idKey)
		if txErr != nil {
			return
		}
		return c.deleteItem(ctx, idKey, it)
	})
}

func (c *collection) deleteItem(ctx context.Context, id []byte, prevItem item) (err error) {
	if _, err = c.stmts.delete.ExecContext(ctx, []driver.NamedValue{{Name: "id", Value: id}}); err != nil {
		return
	}
	return c.indexesHandleDelete(ctx, id, prevItem)
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
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)
	// TODO: validate fields
	return c.db.doWriteTx(ctx, func(cn conn.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		var (
			idx        *index
			newIndexes []*index
		)
		for _, idxInfo := range info {
			if idx, txErr = c.createIndex(ctx, cn, idxInfo); txErr != nil {
				return
			}
			if txErr = idx.checkStmts(ctx, cn); txErr != nil {
				return
			}
			newIndexes = append(newIndexes, idx)
		}

		rows, txErr := cn.QueryContext(ctx, c.queries.findAll, nil)
		if txErr != nil {
			return
		}
		defer func() {
			_ = rows.Close()
		}()

		var dest = make([]driver.Value, 1)
		for {
			rErr := rows.Next(dest)
			if rErr != nil {
				if errors.Is(rErr, io.EOF) {
					break
				}
				return rErr
			}
			var it item
			if it, txErr = parseItem(buf.Parser, nil, dest[0], false); txErr != nil {
				return
			}
			id := it.appendId(buf.SmallBuf[:0])
			for _, idx = range newIndexes {
				if txErr = idx.Insert(ctx, id, it); txErr != nil {
					return
				}
			}
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.indexes = append(c.indexes, newIndexes...)
		return nil
	})
}

func (c *collection) createIndex(ctx context.Context, cn conn.Conn, info IndexInfo) (idx *index, err error) {
	if info.Name == "" {
		info.Name = info.createName()
	}
	var fieldsIsDesc = make([]bool, len(info.Fields))
	for i, field := range info.Fields {
		if err = validateIndexField(field); err != nil {
			return nil, err
		}
		if _, isDesc := parseIndexField(field); isDesc {
			fieldsIsDesc[i] = isDesc
		}
	}
	if _, err = c.db.stmt.registerIndex.ExecContext(ctx, []driver.NamedValue{
		{Name: "indexName", Value: info.Name},
		{Name: "collName", Value: c.name},
		{Name: "fields", Value: stringArrayToJson(&fastjson.Arena{}, info.Fields)},
		{Name: "sparse", Value: info.Sparse},
		{Name: "unique", Value: info.Unique},
	}); err != nil {
		return nil, replaceUniqErr(err, ErrIndexExists)
	}

	if _, err = cn.ExecContext(ctx, c.sql.Index(info.Name).Create(info.Unique, fieldsIsDesc), nil); err != nil {
		return
	}
	return newIndex(ctx, c, info)
}

func (c *collection) DropIndex(ctx context.Context, indexName string) (err error) {
	return c.db.doWriteTx(ctx, func(cn conn.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}

		res, txErr := c.db.stmt.removeIndex.ExecContext(ctx, []driver.NamedValue{
			{Name: "indexName", Value: indexName},
			{Name: "collName", Value: c.name},
		})
		if txErr != nil {
			return
		}
		affected, txErr := res.RowsAffected()
		if txErr != nil {
			return
		}
		if affected == 0 {
			return ErrIndexNotFound
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		for _, idx := range c.indexes {
			if idx.Info().Name == indexName {
				if txErr = idx.Drop(ctx, cn); txErr != nil {
					return
				}
			}
		}
		c.indexes = slices.DeleteFunc(c.indexes, func(i *index) bool {
			return i.Info().Name == indexName
		})
		return
	})
}

func (c *collection) GetIndexes() (indexes []Index) {
	c.mu.Lock()
	defer c.mu.Unlock()
	indexes = make([]Index, len(c.indexes))
	for i, idx := range c.indexes {
		indexes[i] = idx
	}
	return
}

func (c *collection) indexesHandleInsert(ctx context.Context, id key.Key, it item) (err error) {
	for _, idx := range c.indexes {
		if err = idx.Insert(ctx, id, it); err != nil {
			return
		}
	}
	return
}

func (c *collection) indexesHandleUpdate(ctx context.Context, id key.Key, prevIt, newIt item) (err error) {
	for _, idx := range c.indexes {
		if err = idx.Update(ctx, id, prevIt, newIt); err != nil {
			return
		}
	}
	return
}

func (c *collection) indexesHandleDelete(ctx context.Context, id key.Key, prevIt item) (err error) {
	for _, idx := range c.indexes {
		if err = idx.Delete(ctx, id, prevIt); err != nil {
			return
		}
	}
	return
}

func (c *collection) Rename(ctx context.Context, newName string) error {
	return c.db.doWriteTx(ctx, func(cn conn.Conn) (err error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, stmt := range []conn.Stmt{c.db.stmt.renameCollection, c.db.stmt.renameCollectionIndex} {
			if _, err = stmt.ExecContext(ctx, []driver.NamedValue{
				{
					Name:  "oldName",
					Value: c.name,
				},
				{
					Name:  "newName",
					Value: newName,
				},
			}); err != nil {
				return
			}
		}

		if _, err = cn.ExecContext(ctx, c.sql.Rename(newName), nil); err != nil {
			return err
		}
		c.name = newName
		c.sql = c.db.sql.Collection(newName)
		c.tableName = c.sql.TableName()
		for _, idx := range c.indexes {
			if err = idx.RenameColl(ctx, cn, newName); err != nil {
				return
			}
		}
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
			if err = idx.Drop(ctx, cn); err != nil {
				return
			}
		}
		if _, err = c.db.stmt.removeCollection.ExecContext(ctx, []driver.NamedValue{
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
	if c.stmtsReady.CompareAndSwap(true, false) {
		for _, stmt := range []conn.Stmt{
			c.stmts.insert, c.stmts.update, c.stmts.findId, c.stmts.delete,
		} {
			_ = stmt.Close()
		}
	}
}

func (c *collection) WriteTx(ctx context.Context) (WriteTx, error) {
	return c.db.WriteTx(ctx)
}

func (c *collection) ReadTx(ctx context.Context) (ReadTx, error) {
	return c.db.ReadTx(ctx)
}

func (c *collection) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.closeStmts()
	for _, idx := range c.indexes {
		_ = idx.Close()
	}
	c.db.onCollectionClose(c.name)
	return nil
}
