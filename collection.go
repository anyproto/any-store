package anystore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/valyala/fastjson"
	"zombiezen.com/go/sqlite"

	"github.com/anyproto/any-store/encoding"
	"github.com/anyproto/any-store/internal/driver"
	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/sql"
	"github.com/anyproto/any-store/internal/syncpool"
	"github.com/anyproto/any-store/query"
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
	// Provided document must contain an id field
	// Returns an error if the update fails.
	UpdateOne(ctx context.Context, doc any) (err error)

	// UpdateId updates a single document in the collection with provided modifier
	// Returns a modify result or error.
	UpdateId(ctx context.Context, id any, mod query.Modifier) (res ModifyResult, err error)

	// UpsertOne inserts a document if it does not exist, or updates it if it does.
	// Returns the ID of the upserted document or an error if the operation fails.
	UpsertOne(ctx context.Context, doc any) (id any, err error)

	// UpsertId updates a single document or creates new one
	// Returns a modify result or error.
	UpsertId(ctx context.Context, id any, mod query.Modifier) (res ModifyResult, err error)

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
		findId driver.Stmt
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
	return c.db.doReadTx(ctx, func(cn *driver.Conn) (err error) {
		var idxInfo []IndexInfo
		err = cn.Exec(ctx, c.sql.FindIndexes(), nil, func(stmt *sqlite.Stmt) error {
			idxInfo, err = readIndexInfo(buf, stmt)
			if err != nil {
				return err
			}
			return nil
		})
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

func (c *collection) checkStmts(ctx context.Context, cn *driver.Conn) (err error) {
	if !c.stmtsReady.CompareAndSwap(false, true) {
		return nil
	}
	if c.stmts.delete, err = cn.Prepare(c.sql.DeleteStmt()); err != nil {
		return
	}
	if c.stmts.insert, err = cn.Prepare(c.sql.InsertStmt()); err != nil {
		return
	}
	if c.stmts.update, err = cn.Prepare(c.sql.UpdateStmt()); err != nil {
		return
	}
	if c.stmts.findId, err = cn.Prepare(c.sql.FindIdStmt()); err != nil {
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

	buf.SmallBuf = encoding.AppendAnyValue(buf.SmallBuf[:0], docId)
	err = c.db.doReadTx(ctx, func(cn *driver.Conn) (err error) {
		err = cn.Exec(ctx, c.queries.findId, func(stmt *sqlite.Stmt) {
			stmt.BindBytes(1, buf.SmallBuf)
		}, func(stmt *sqlite.Stmt) error {
			hasRow, stepErr := stmt.Step()
			if stepErr != nil {
				return stepErr
			}
			if !hasRow {
				return ErrDocNotFound
			}
			buf.DocBuf = readBytes(stmt, buf.DocBuf)
			return nil
		})
		if err != nil {
			return err
		}
		data, err := fastjson.ParseBytes(buf.DocBuf)
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
	err = c.db.doWriteTx(ctx, func(cn *driver.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		var it item
		buf.Arena.Reset()
		if it, txErr = parseItem(buf.Parser, buf.Arena, doc, true); txErr != nil {
			return txErr
		}
		if idBytes, txErr = c.insertItem(ctx, buf, it); txErr != nil {
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

	err = c.db.doWriteTx(ctx, func(cn *driver.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		var it item
		for _, doc := range docs {
			buf.Arena.Reset()
			if it, txErr = parseItem(buf.Parser, buf.Arena, doc, true); txErr != nil {
				return txErr
			}
			if _, txErr = c.insertItem(ctx, buf, it); txErr != nil {
				return txErr
			}
		}
		return
	})
	return replaceUniqErr(err, ErrDocExists)
}

func (c *collection) insertItem(ctx context.Context, buf *syncpool.DocBuffer, it item) (id []byte, err error) {
	buf.SmallBuf = it.appendId(buf.SmallBuf[:0])
	buf.DocBuf = it.Value().MarshalTo(buf.DocBuf[:0])
	id = buf.SmallBuf
	if err = c.stmts.insert.Exec(ctx, func(stmt *sqlite.Stmt) {
		stmt.BindBytes(1, buf.SmallBuf)
		stmt.BindBytes(2, buf.DocBuf)
	}, driver.StmtExecNoResults); err != nil {
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

	return c.db.doWriteTx(ctx, func(cn *driver.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		return c.update(ctx, it, item{})
	})
}

func (c *collection) UpdateId(ctx context.Context, id any, mod query.Modifier) (res ModifyResult, err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	buf2 := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf2)

	if err = c.db.doWriteTx(ctx, func(cn *driver.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		buf.SmallBuf = encoding.AppendAnyValue(buf.SmallBuf[:0], id)
		it, txErr := c.loadById(ctx, buf, buf.SmallBuf)
		if txErr != nil {
			return
		}

		buf2.Arena.Reset()
		newVal, modified, txErr := mod.Modify(buf2.Arena, copyItem(buf2, it).val)
		if txErr != nil {
			return
		}
		if !modified {
			return
		}
		res.Modified = 1
		res.Matched = 1
		return c.update(ctx, item{val: newVal}, it)
	}); err != nil {
		return ModifyResult{}, err
	}
	return
}

func (c *collection) UpsertId(ctx context.Context, id any, mod query.Modifier) (res ModifyResult, err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	buf2 := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf2)

	if err = c.db.doWriteTx(ctx, func(cn *driver.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		buf.SmallBuf = encoding.AppendAnyValue(buf.SmallBuf[:0], id)
		var (
			isInsert bool
			modValue *fastjson.Value
			prevItem item
		)
		it, loadErr := c.loadById(ctx, buf, buf.SmallBuf)
		if loadErr != nil {
			if errors.Is(loadErr, ErrDocNotFound) {
				// create an object with only id field
				var idVal *fastjson.Value
				buf.Arena.Reset()
				modValue = buf.Arena.NewObject()
				idVal, _, txErr = encoding.DecodeToJSON(buf.Parser, buf.Arena, buf.SmallBuf)
				if txErr != nil {
					return txErr
				}
				modValue.Set("id", idVal)
				isInsert = true
			} else {
				return loadErr
			}
		} else {
			prevItem = it
			modValue = copyItem(buf2, it).val
		}

		buf2.Arena.Reset()
		newVal, modified, txErr := mod.Modify(buf2.Arena, modValue)
		if txErr != nil {
			return
		}
		if !modified {
			return
		}
		res.Modified = 1
		if isInsert {
			_, txErr = c.insertItem(ctx, buf2, item{val: newVal})
			return txErr
		} else {
			res.Matched = 1
			return c.update(ctx, item{val: newVal}, prevItem)
		}
	}); err != nil {
		return ModifyResult{}, err
	}
	return
}

func (c *collection) update(ctx context.Context, it, prevIt item) (err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	buf.SmallBuf = it.appendId(buf.SmallBuf[:0])
	if prevIt.val == nil {
		prevIt, err = c.loadById(ctx, buf, buf.SmallBuf)
		if err != nil {
			return
		}
	}

	buf.DocBuf = it.Value().MarshalTo(buf.DocBuf[:0])
	if err = c.stmts.update.Exec(ctx, func(stmt *sqlite.Stmt) {
		stmt.BindBytes(1, buf.DocBuf)
		stmt.BindBytes(2, buf.SmallBuf)
	}, driver.StmtExecNoResults); err != nil {
		return
	}

	return c.indexesHandleUpdate(ctx, buf.SmallBuf, prevIt, it)
}

func (c *collection) loadById(ctx context.Context, buf *syncpool.DocBuffer, id key.Key) (it item, err error) {
	err = c.stmts.findId.Exec(ctx, func(stmt *sqlite.Stmt) {
		stmt.BindBytes(1, id)
	}, func(stmt *sqlite.Stmt) error {
		hasRow, stepErr := stmt.Step()
		if stepErr != nil {
			return stepErr
		}
		if !hasRow {
			return ErrDocNotFound
		}
		buf.DocBuf = readBytes(stmt, buf.DocBuf)
		return nil
	})
	if err != nil {
		return
	}

	return parseItem(buf.Parser, nil, buf.DocBuf, false)
}

func (c *collection) UpsertOne(ctx context.Context, doc any) (id any, err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	var it item
	buf.Arena.Reset()
	if it, err = parseItem(buf.Parser, buf.Arena, doc, true); err != nil {
		return
	}

	err = c.db.doWriteTx(ctx, func(cn *driver.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		_, insErr := c.insertItem(ctx, buf, it)
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

	return c.db.doWriteTx(ctx, func(cn *driver.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}
		buf.SmallBuf = encoding.AppendAnyValue(buf.SmallBuf[:0], id)
		it, txErr := c.loadById(ctx, buf, buf.SmallBuf)
		if txErr != nil {
			return
		}
		return c.deleteItem(ctx, buf.SmallBuf, it)
	})
}

func (c *collection) deleteItem(ctx context.Context, id []byte, prevItem item) (err error) {
	if err = c.stmts.delete.Exec(ctx, func(stmt *sqlite.Stmt) {
		stmt.BindBytes(1, id)
	}, driver.StmtExecNoResults); err != nil {
		return
	}
	return c.indexesHandleDelete(ctx, id, prevItem)
}

func (c *collection) Count(ctx context.Context) (count int, err error) {
	err = c.db.doReadTx(ctx, func(cn *driver.Conn) error {
		txErr := cn.Exec(ctx, c.queries.count, nil, func(stmt *sqlite.Stmt) error {
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
		if txErr != nil {
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
	return c.db.doWriteTx(ctx, func(cn *driver.Conn) (txErr error) {
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

		txErr = cn.Exec(ctx, c.queries.findAll, nil, func(stmt *sqlite.Stmt) error {
			for {
				hasRow, stepErr := stmt.Step()
				if stepErr != nil {
					return stepErr
				}
				if !hasRow {
					break
				}
				buf.DocBuf = readBytes(stmt, buf.DocBuf)
				var it item
				if it, txErr = parseItem(buf.Parser, nil, buf.DocBuf, false); txErr != nil {
					return txErr
				}
				buf.SmallBuf = it.appendId(buf.SmallBuf[:0])
				for _, idx = range newIndexes {
					if txErr = idx.Insert(ctx, buf.SmallBuf, it); txErr != nil {
						return txErr
					}
				}
			}
			return nil
		})
		if txErr != nil {
			return
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		c.indexes = append(c.indexes, newIndexes...)
		return nil
	})
}

func (c *collection) createIndex(ctx context.Context, cn *driver.Conn, info IndexInfo) (idx *index, err error) {
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
	if err = c.db.stmt.registerIndex.Exec(ctx, func(stmt *sqlite.Stmt) {
		stmt.SetText(":indexName", info.Name)
		stmt.SetText(":collName", c.name)
		stmt.SetText(":fields", stringArrayToJson(&fastjson.Arena{}, info.Fields))
		stmt.SetBool(":sparse", info.Sparse)
		stmt.SetBool(":unique", info.Unique)
	}, driver.StmtExecNoResults); err != nil {
		return nil, replaceUniqErr(err, ErrIndexExists)
	}

	if err = cn.ExecNoResult(ctx, c.sql.Index(info.Name).Create(info.Unique, fieldsIsDesc)); err != nil {
		return
	}
	return newIndex(ctx, c, info)
}

func (c *collection) DropIndex(ctx context.Context, indexName string) (err error) {
	return c.db.doWriteTx(ctx, func(cn *driver.Conn) (txErr error) {
		if txErr = c.checkStmts(ctx, cn); txErr != nil {
			return
		}

		txErr = c.db.stmt.removeIndex.Exec(ctx, func(stmt *sqlite.Stmt) {
			stmt.SetText(":indexName", indexName)
			stmt.SetText(":collName", c.name)
		}, func(stmt *sqlite.Stmt) error {
			hasRow, stepErr := stmt.Step()
			if stepErr != nil {
				return stepErr
			}
			if !hasRow {
				return ErrIndexNotFound
			}
			return nil
		})

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
	return c.db.doWriteTx(ctx, func(cn *driver.Conn) (err error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, stmt := range []driver.Stmt{c.db.stmt.renameCollection, c.db.stmt.renameCollectionIndex} {
			if err = stmt.Exec(ctx, func(sStmt *sqlite.Stmt) {
				sStmt.SetText(":oldName", c.name)
				sStmt.SetText(":newName", newName)
			}, driver.StmtExecNoResults); err != nil {
				return
			}
		}

		if err = cn.ExecNoResult(ctx, c.sql.Rename(newName)); err != nil {
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
	return c.db.doWriteTx(ctx, func(cn *driver.Conn) (err error) {
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
		if err = c.db.stmt.removeCollection.Exec(ctx, func(stmt *sqlite.Stmt) {
			stmt.SetText(":collName", c.name)
		}, driver.StmtExecNoResults); err != nil {
			return
		}
		if err = cn.ExecNoResult(ctx, c.sql.Drop()); err != nil {
			return
		}
		return nil
	})
}

func (c *collection) closeStmts() {
	if c.stmtsReady.CompareAndSwap(true, false) {
		for _, stmt := range []driver.Stmt{
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
