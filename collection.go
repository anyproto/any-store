package anystore

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/anyenc/anyencutil"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// Collection represents a collection of documents.
type Collection interface {
	// Name returns the name of the collection.
	Name() string

	// FindId finds a document by its ID.
	// Returns the document or an error if the document is not found.
	FindId(ctx context.Context, id any) (Doc, error)

	// FindIdWithParser finds a document by its ID. Uses provided anyenc parser.
	// Returns the document or an error if the document is not found.
	FindIdWithParser(ctx context.Context, p *anyenc.Parser, id any) (Doc, error)

	// Find returns a new Query object with given filter
	Find(filter any) Query

	// Insert inserts multiple documents into the collection.
	// Returns an error if the insertion fails.
	Insert(ctx context.Context, docs ...*anyenc.Value) (err error)

	// UpdateOne updates a single document in the collection.
	// Provided document must contain an id field
	// Returns an error if the update fails.
	UpdateOne(ctx context.Context, doc *anyenc.Value) (err error)

	// UpdateId updates a single document in the collection with provided modifier
	// Returns a modify result or error.
	UpdateId(ctx context.Context, id any, mod query.Modifier) (res ModifyResult, err error)

	// UpsertOne inserts a document if it does not exist, or updates it if it does.
	// Returns the ID of the upserted document or an error if the operation fails.
	UpsertOne(ctx context.Context, doc *anyenc.Value) (err error)

	// UpsertId updates a single document or creates new one
	// Returns a modify result or error.
	UpsertId(ctx context.Context, id any, mod query.Modifier) (res ModifyResult, err error)

	// DeleteId deletes a single document by its ID.
	// Returns an error if the deletion fails.
	DeleteId(ctx context.Context, id any) (err error)

	// Count returns the number of documents in the collection.
	// Returns the count of documents or an error if the operation fails.
	Count(ctx context.Context) (count int, err error)

	// CreateIndex creates a new index.
	// Returns an error if index exists or the operation fails.
	CreateIndex(ctx context.Context, info ...IndexInfo) (err error)

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
		db:   db,
	}
	if err := coll.init(ctx); err != nil {
		return nil, err
	}
	return coll, nil
}

type collection struct {
	name    string
	indexes []*index
	db      *db
	ns      *btree.Namespace

	closed atomic.Bool
	mu     sync.Mutex
}

func (c *collection) init(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get the namespace for this collection
	ns, err := c.db.btreeDB.GetNamespace(c.name)
	if err != nil {
		return err
	}
	c.ns = ns

	return c.db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		idxInfos, err := c.db.getIndexInfos(tx, c.name)
		if err != nil {
			return err
		}
		for _, info := range idxInfos {
			nsName := indexNsName(c.name, info.Name)
			ns, nsErr := c.db.btreeDB.GetNamespace(nsName)
			if nsErr != nil {
				return nsErr
			}
			idx, idxErr := newIndex(c, info, ns)
			if idxErr != nil {
				return idxErr
			}
			c.indexes = append(c.indexes, idx)
		}
		return nil
	})
}

func (c *collection) Name() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.name
}

func (c *collection) FindId(ctx context.Context, docId any) (doc Doc, err error) {
	return c.FindIdWithParser(ctx, &anyenc.Parser{}, docId)
}

func (c *collection) FindIdWithParser(ctx context.Context, p *anyenc.Parser, docId any) (doc Doc, err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	buf.SmallBuf = anyenc.AppendAnyValue(buf.SmallBuf[:0], docId)
	err = c.db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		val, err := tx.Get(c.ns, buf.SmallBuf)
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				return ErrDocNotFound
			}
			return err
		}
		buf.DocBuf = append(buf.DocBuf[:0], val...)
		data, err := p.Parse(buf.DocBuf)
		doc = item{val: data}
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

func (c *collection) Insert(ctx context.Context, docs ...*anyenc.Value) (err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) (txErr error) {
		var it item
		for _, doc := range docs {
			buf.Arena.Reset()
			if it, txErr = newItem(doc); txErr != nil {
				return txErr
			}
			if txErr = c.insertItem(tx, buf, it); txErr != nil {
				return txErr
			}
		}
		return
	})
	return
}

func (c *collection) insertItem(tx *btree.WriteTx, buf *syncpool.DocBuffer, it item) (err error) {
	buf.SmallBuf = it.appendId(buf.SmallBuf[:0])
	buf.DocBuf = it.Value().MarshalTo(buf.DocBuf[:0])

	// Check if key already exists
	if _, err := tx.Get(c.ns, buf.SmallBuf); err == nil {
		return ErrDocExists
	}

	if err = tx.Put(c.ns, buf.SmallBuf, buf.DocBuf); err != nil {
		return err
	}

	// Insert index entries
	for _, idx := range c.indexes {
		if err = idx.insertKeys(tx, it); err != nil {
			return err
		}
	}
	return nil
}

func (c *collection) UpdateOne(ctx context.Context, doc *anyenc.Value) (err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	var it item
	if it, err = newItem(doc); err != nil {
		return
	}

	return c.db.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		return c.update(tx, it, item{})
	})
}

func (c *collection) UpdateId(ctx context.Context, id any, mod query.Modifier) (res ModifyResult, err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	buf2 := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf2)

	if err = c.db.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		buf.SmallBuf = anyenc.AppendAnyValue(buf.SmallBuf[:0], id)
		it, txErr := c.loadById(tx, buf, buf.SmallBuf)
		if txErr != nil {
			return
		}

		buf2.Arena.Reset()
		newVal, modified, txErr := mod.Modify(buf2.Arena, copyItem(buf2, it).val)
		if txErr != nil {
			return
		}
		res.Matched = 1
		if !modified {
			return
		}
		res.Modified = 1
		return c.update(tx, item{val: newVal}, it)
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

	if err = c.db.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		buf.SmallBuf = anyenc.AppendAnyValue(buf.SmallBuf[:0], id)
		var (
			isInsert bool
			modValue *anyenc.Value
			prevItem item
		)
		it, loadErr := c.loadById(tx, buf, buf.SmallBuf)
		if loadErr != nil {
			if errors.Is(loadErr, ErrDocNotFound) {
				var idVal *anyenc.Value
				buf.Arena.Reset()
				modValue = buf.Arena.NewObject()
				idVal, txErr = buf.Parser.Parse(buf.SmallBuf)
				if txErr != nil {
					return false, txErr
				}
				modValue.Set("id", idVal)
				isInsert = true
			} else {
				return false, loadErr
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
			if !isInsert {
				res.Matched = 1
			}
			return
		}
		res.Modified = 1
		if isInsert {
			txErr = c.insertItem(tx, buf2, item{val: newVal})
			return true, txErr
		} else {
			res.Matched = 1
			return c.update(tx, item{val: newVal}, prevItem)
		}
	}); err != nil {
		return ModifyResult{}, err
	}
	return
}

func (c *collection) update(tx *btree.WriteTx, it, prevIt item) (modified bool, err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	buf.SmallBuf = it.appendId(buf.SmallBuf[:0])
	if prevIt.val == nil {
		prevIt, err = c.loadById(tx, buf, buf.SmallBuf)
		if err != nil {
			return
		}

		if anyencutil.Equal(prevIt.Value(), it.Value()) {
			return false, nil
		}
	}

	// Update index entries: delete old, insert new
	for _, idx := range c.indexes {
		if err = idx.deleteKeys(tx, prevIt); err != nil {
			return
		}
		if err = idx.insertKeys(tx, it); err != nil {
			return
		}
	}

	buf.DocBuf = it.Value().MarshalTo(buf.DocBuf[:0])
	if err = tx.Put(c.ns, buf.SmallBuf, buf.DocBuf); err != nil {
		return
	}

	return true, nil
}

func (c *collection) loadById(tx *btree.WriteTx, buf *syncpool.DocBuffer, id anyenc.Tuple) (it item, err error) {
	val, err := tx.Get(c.ns, id)
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return item{}, ErrDocNotFound
		}
		return
	}
	buf.DocBuf = append(buf.DocBuf[:0], val...)
	doc, err := buf.Parser.Parse(buf.DocBuf)
	if err != nil {
		return
	}
	return newItem(doc)
}

func (c *collection) UpsertOne(ctx context.Context, doc *anyenc.Value) (err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	var it item
	if it, err = newItem(doc); err != nil {
		return
	}

	err = c.db.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		insErr := c.insertItem(tx, buf, it)
		if errors.Is(insErr, ErrDocExists) {
			return c.update(tx, it, item{})
		}
		return true, insErr
	})
	return err
}

func (c *collection) DeleteId(ctx context.Context, id any) (err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	return c.db.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		buf.SmallBuf = anyenc.AppendAnyValue(buf.SmallBuf[:0], id)
		// Verify document exists
		_, txErr = c.loadById(tx, buf, buf.SmallBuf)
		if txErr != nil {
			return
		}
		return true, c.deleteItem(tx, buf, buf.SmallBuf)
	})
}

func (c *collection) deleteItem(tx *btree.WriteTx, buf *syncpool.DocBuffer, id []byte) (err error) {
	// Delete index entries
	if len(c.indexes) > 0 {
		it, loadErr := c.loadById(tx, buf, id)
		if loadErr != nil {
			return loadErr
		}
		for _, idx := range c.indexes {
			if err = idx.deleteKeys(tx, it); err != nil {
				return err
			}
		}
	}
	return tx.Delete(c.ns, id)
}

func (c *collection) Count(ctx context.Context) (count int, err error) {
	err = c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		var txErr error
		count, txErr = tx.Count(c.ns)
		return txErr
	})
	return
}

func (c *collection) CreateIndex(ctx context.Context, info ...IndexInfo) (err error) {
	return c.createIndexes(ctx, false, info...)
}

func (c *collection) EnsureIndex(ctx context.Context, info ...IndexInfo) (err error) {
	return c.createIndexes(ctx, true, info...)
}

func (c *collection) createIndexes(ctx context.Context, ensure bool, info ...IndexInfo) (err error) {
	if len(info) == 0 {
		return nil
	}
	return c.db.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		var newIndexes []*index
		for _, idxInfo := range info {
			idx, txErr := c.createIndex(ctx, tx, idxInfo)
			if txErr != nil {
				if ensure && errors.Is(txErr, ErrIndexExists) {
					continue
				}
				return false, txErr
			}
			newIndexes = append(newIndexes, idx)
		}

		if len(newIndexes) == 0 {
			return false, nil
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		c.indexes = append(c.indexes, newIndexes...)
		return true, nil
	})
}

func (c *collection) createIndex(ctx context.Context, tx *btree.WriteTx, info IndexInfo) (idx *index, err error) {
	if info.Name == "" {
		info.Name = info.createName()
	}
	for _, field := range info.Fields {
		if err = validateIndexField(field); err != nil {
			return nil, err
		}
	}

	// Register in system namespace
	if err = c.db.registerIndex(tx, c.name, info); err != nil {
		return nil, err
	}

	// Create index namespace
	nsName := indexNsName(c.name, info.Name)
	ns, err := tx.CreateNamespace(nsName)
	if err != nil {
		return nil, err
	}

	idx, err = newIndex(c, info, ns)
	if err != nil {
		return nil, err
	}

	// Build index from existing documents
	if err = c.buildIndex(tx, idx); err != nil {
		return nil, err
	}

	return idx, nil
}

func (c *collection) DropIndex(ctx context.Context, indexName string) (err error) {
	return c.db.doWriteTx(ctx, func(tx *btree.WriteTx) (txErr error) {
		// Check index exists
		found := false
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, idx := range c.indexes {
			if idx.Info().Name == indexName {
				found = true
				break
			}
		}
		if !found {
			return ErrIndexNotFound
		}

		if txErr = c.db.removeIndex(tx, c.name, indexName); txErr != nil {
			return
		}
		// Delete the index namespace
		nsName := indexNsName(c.name, indexName)
		if txErr = tx.DeleteNamespace(nsName); txErr != nil {
			if !errors.Is(txErr, btree.ErrNamespaceNotFound) {
				return
			}
		}
		c.indexes = slices.DeleteFunc(c.indexes, func(i *index) bool {
			return i.Info().Name == indexName
		})
		return nil
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

func (c *collection) Rename(ctx context.Context, newName string) error {
	return c.db.doWriteTx(ctx, func(tx *btree.WriteTx) (err error) {
		c.mu.Lock()
		defer c.mu.Unlock()

		if err = c.db.renameCollection(tx, c.name, newName); err != nil {
			return err
		}

		// Note: btree namespaces can't be renamed, so we keep the old namespace
		// but update the name in our metadata
		c.name = newName
		return nil
	})
}

func (c *collection) Drop(ctx context.Context) error {
	return c.db.doWriteTx(ctx, func(tx *btree.WriteTx) (err error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if err = c.close(); err != nil {
			return err
		}
		// Delete all index namespaces
		for _, idx := range c.indexes {
			nsName := indexNsName(c.name, idx.info.Name)
			if err = tx.DeleteNamespace(nsName); err != nil {
				if !errors.Is(err, btree.ErrNamespaceNotFound) {
					return
				}
			}
		}
		if err = c.db.removeCollection(tx, c.name); err != nil {
			return
		}
		// Delete the collection namespace
		if err = tx.DeleteNamespace(c.name); err != nil {
			if !errors.Is(err, btree.ErrNamespaceNotFound) {
				return
			}
		}
		return nil
	})
}

func (c *collection) WriteTx(ctx context.Context) (WriteTx, error) {
	return c.db.WriteTx(ctx)
}

func (c *collection) ReadTx(ctx context.Context) (ReadTx, error) {
	return c.db.ReadTx(ctx)
}

func (c *collection) Close() error {
	if err := c.close(); err != nil {
		return err
	}
	return nil
}

func (c *collection) close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.db.onCollectionClose(c.name)
	return nil
}

// buildIndex populates index entries from all existing documents in the collection.
func (c *collection) buildIndex(tx *btree.WriteTx, idx *index) error {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	cursor := tx.NewCursor(c.ns)
	if err := cursor.First(); err != nil {
		return err
	}
	for cursor.Valid() {
		val, err := cursor.Value()
		if err != nil {
			return err
		}
		buf.DocBuf = append(buf.DocBuf[:0], val...)
		doc, err := buf.Parser.Parse(buf.DocBuf)
		if err != nil {
			return err
		}
		it, err := newItem(doc)
		if err != nil {
			return err
		}
		if err = idx.insertKeys(tx, it); err != nil {
			return err
		}
		if err = cursor.Next(); err != nil {
			return err
		}
	}
	return nil
}

// loadByIdRead loads a document by ID using a read transaction
func (c *collection) loadByIdRead(tx *btree.ReadTx, buf *syncpool.DocBuffer, id anyenc.Tuple) (it item, err error) {
	val, err := tx.Get(c.ns, id)
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return item{}, ErrDocNotFound
		}
		return
	}
	buf.DocBuf = append(buf.DocBuf[:0], val...)
	doc, err := buf.Parser.Parse(buf.DocBuf)
	if err != nil {
		return
	}
	return newItem(doc)
}
