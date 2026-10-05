package anystore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// Collection represents a collection of documents.
type Collection interface {
	// Name returns the name of the collection.
	Name() string

	// PrimaryKey returns the document field used as this collection's primary key.
	PrimaryKey() string

	// FindId finds a document by its ID.
	// Returns the document or an error if the document is not found.
	FindId(ctx context.Context, id any) (Doc, error)

	// FindIdWithParser finds a document by its ID. Uses provided anyenc parser.
	// Returns the document or an error if the document is not found.
	FindIdWithParser(ctx context.Context, p *anyenc.Parser, id any) (Doc, error)

	// Find returns a new Query object with given filter
	Find(filter any) Query

	// Aggregate returns a new aggregation query for the given MongoDB-style
	// pipeline (an array of stages like $match, $group, $sort, $unwind, ...).
	Aggregate(pipeline any) AggQuery

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

	// GetIndexes returns a list of indexes on the collection: range indexes
	// followed by full-text indexes (a full-text Len is the number of indexed
	// documents). Vector indexes are not listed yet.
	GetIndexes() (indexes []Index)

	// CompactVectorIndex rebuilds the named vector index from its live vectors,
	// reclaiming nodes and storage left behind by deletes and replaces (which only
	// tombstone). It is synchronous and holds the write lock for the rebuild;
	// prefer a maintenance window for large indexes. No-op when nothing is
	// reclaimable or for a brute-force index. Returns ErrIndexNotFound if absent.
	CompactVectorIndex(ctx context.Context, indexName string) error

	// Stats returns the storage footprint of the collection: document count,
	// stored and uncompressed sizes, compression ratio and per-index sizes.
	// It scans the whole collection and is intended for diagnostics.
	Stats(ctx context.Context) (CollectionStats, error)

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

	// Close releases the handle: later operations through it fail with
	// ErrCollectionClosed. A handle that changed the schema in a write
	// transaction is released once that change is committed or rolled back,
	// and stays open if the collection is opened again before that.
	Close() error
}

// newCollection makes the handle of the collection name and loads its schema
// through tx, the view the caller opens it in: a read snapshot, or the open
// write transaction's own view (a collection created earlier in it). The
// caller registers the handle and installs the version, or keeps it for its
// transaction alone (see resolveSlow).
func newCollection(db *db, name string, tx *btree.ReadTx) (*collection, *collSchema, error) {
	c := &collection{db: db, openName: name}
	s, err := c.loadSchema(tx, name, nil)
	if err != nil {
		return nil, nil, err
	}
	return c, s, nil
}

type collection struct {
	// head is the current schema version, nil while none is installed: the
	// newest state this process knows, with the open write transaction's own
	// schema changes in it once they are made. Which transactions it serves
	// is resolve's business. Read lock-free; replaced under c.mu.
	head atomic.Pointer[collSchema]
	db   *db

	compression Compression // 0 = use db default

	// catalogID and dataRoot identify the collection this handle stands for:
	// the coll: record value and the data namespace's root page, fixed at the
	// first load (identify). A view that has another collection under the
	// name — dropped and recreated, or renamed away and created anew — does
	// not have this one (loadSchema). The token moves with a rename; the root
	// is the second check for the entries of old files that share one token.
	catalogID []byte
	dataRoot  uint32

	// openName is the name the handle was opened under: what the collection
	// is called while no version is installed.
	openName string

	// since is the epoch's known cookie at the moment the handle entered the
	// registry. A schema change of this process to the collection went
	// through an earlier handle if its cookie is at or below since, and
	// through this one otherwise — so a version loaded by a transaction older
	// than since may miss a change nothing here will ever tell it of, and is
	// never installed (resolveSlow). Set under db.mu before the handle is
	// reachable.
	since uint32

	// primaryKey is the document field whose value is the btree key. Resolved
	// once at the first load (default "id") and never mutated after, so the
	// data and query paths read it lock-free.
	primaryKey string

	// sketchReadBuf is reused scratch for decoding persisted sketch bytes during
	// the advisory reload (reloadSketch). Only touched under c.mu, so a single
	// per-collection buffer is safe and keeps the stale-reload path from
	// allocating a fresh read buffer per index.
	sketchReadBuf []byte

	// sketchDirty and ftsDirty record membership in db.sketchDirty and
	// db.ftsDirty; writer-owned like the lists.
	sketchDirty bool
	ftsDirty    bool

	// ddlTxs counts the uncommitted schema changes the open write tx made
	// through this handle: its creation, index DDL, a vector compaction, a
	// rename, a drop. While it is non-zero the handle stays the collection's
	// one registered handle. A second one — built from the writer's view or
	// from the committed one — would not follow the transaction's outcome:
	// the changes are published through this handle and undone on it, and a
	// commit of this process verifies no handle (see schemaEpoch).
	//
	// SQLite keeps one in-memory schema per connection, resets it when a
	// transaction that changed it rolls back (sqlite3RollbackAll,
	// OP_Savepoint), and under shared cache prepares no statement on
	// another connection while the changes are uncommitted (prepare.c,
	// sqlite3BtreeSchemaLocked). What goes stale all the same it detects:
	// every statement checks the schema cookie and the schema generation it
	// was prepared with (OP_Transaction). A handle carries no such check,
	// so a second one must not come to exist; other users get this handle
	// instead of an error, as they would without the Close().
	//
	// Guarded by db.mu.
	ddlTxs int
	// closePending records a Close() that waits for ddlTxs to reach zero
	// (db.ddlEnd). An open that hands this handle to a caller in between
	// clears it (db.handOut). Written under db.mu.
	closePending atomic.Bool

	closed atomic.Bool
	mu     sync.Mutex
}

// cur returns the head. For the write path and the schema changes, which
// resolved the handle in their transaction first; nil while no version is
// installed.
func (c *collection) cur() *collSchema {
	return c.head.Load()
}

// publish makes a copy of the head with edit applied the head: the schema
// change of the open write transaction tx, valid from the cookie tx commits
// with. Transactions at older snapshots — every reader until the commit —
// load their own version meanwhile (resolve). The caller holds c.mu and has
// registered the restore of the previous head for a rollback
// (registerHeadRestore).
func (c *collection) publish(tx *btree.WriteTx, edit func(s *collSchema)) {
	next := c.cur().clone()
	edit(next)
	next.validFrom = tx.DiskSchemaCookie() + 1
	next.gen.Store(c.db.epoch.Load().gen)
	c.head.Store(next)
}

// loadIndexes returns the head's range-index set. Lock-free; the slice is
// immutable.
func (c *collection) loadIndexes() []*index {
	if s := c.cur(); s != nil {
		return s.indexes
	}
	return nil
}

// loadFtsIndexes returns the head's full-text index set (lock-free).
func (c *collection) loadFtsIndexes() []*ftsIndex {
	if s := c.cur(); s != nil {
		return s.ftsIndexes
	}
	return nil
}

// markSketchDirty lists the collection in db.sketchDirty. Writer only.
func (c *collection) markSketchDirty() {
	if !c.sketchDirty {
		c.sketchDirty = true
		c.db.sketchDirty = append(c.db.sketchDirty, c)
	}
}

// markFtsDirty lists the collection in db.ftsDirty. Writer only.
func (c *collection) markFtsDirty() {
	if !c.ftsDirty {
		c.ftsDirty = true
		c.db.ftsDirty = append(c.db.ftsDirty, c)
	}
}

// loadVectorIndexes returns the head's vector-index set (lock-free).
func (c *collection) loadVectorIndexes() []*vectorIndex {
	if s := c.cur(); s != nil {
		return s.vindexes
	}
	return nil
}

func (c *collection) Name() string {
	if s := c.cur(); s != nil {
		return s.name
	}
	return c.openName
}

func (c *collection) PrimaryKey() string {
	return c.primaryKey
}

// validatePrimaryKey checks a primary-key field name: non-empty, no "$" prefix,
// and a single top-level field (no "." path separators).
func validatePrimaryKey(s string) error {
	if s == "" {
		return fmt.Errorf("any-store: primary key field is empty")
	}
	if strings.HasPrefix(s, "$") {
		return fmt.Errorf("any-store: invalid primary key field name: %s", s)
	}
	if strings.HasPrefix(s, "-") {
		// A leading '-' is parsed by Sort as a descending marker, so the
		// natural-order fast path could never recognize such a field.
		return fmt.Errorf("any-store: primary key field must not start with '-': %s", s)
	}
	if strings.Contains(s, ".") {
		return fmt.Errorf("any-store: primary key must be a single top-level field: %s", s)
	}
	return nil
}

// compressionDisabled returns whether compression is disabled for this collection.
// Per-collection setting takes priority; falls back to db-wide config.
func (c *collection) compressionDisabled() bool {
	switch c.compression {
	case NoCompression:
		return true
	case S2:
		return false
	default:
		return c.db.config.DisableCompression
	}
}

// newItem wraps a document value, validating that it carries this collection's
// primary-key field and that the pk value is not an array. Returns
// ErrDocWithoutId when the field is absent and ErrArrayPrimaryKey when it is an
// array. Every write path (insert, update, upsert, index backfill) constructs
// items here, so the array ban holds for all data written by this version;
// pure read paths do not re-check (see ErrArrayPrimaryKey).
func (c *collection) newItem(val *anyenc.Value) (item, error) {
	objVal, err := val.Object()
	if err != nil {
		return item{}, err
	}
	pkVal := objVal.Get(c.primaryKey)
	if pkVal == nil {
		return item{}, ErrDocWithoutId
	}
	if pkVal.Type() == anyenc.TypeArray {
		return item{}, ErrArrayPrimaryKey
	}
	return item{val: val}, nil
}

// appendId appends the marshaled primary-key value of val to dst. The field is
// always present here because items are validated on construction.
func (c *collection) appendId(dst []byte, val *anyenc.Value) []byte {
	idVal := val.Get(c.primaryKey)
	if idVal == nil {
		panic("document without primary key")
	}
	return idVal.MarshalTo(dst)
}

func (c *collection) FindId(ctx context.Context, docId any) (doc Doc, err error) {
	return c.FindIdWithParser(ctx, &anyenc.Parser{}, docId)
}

func (c *collection) FindIdWithParser(ctx context.Context, p *anyenc.Parser, docId any) (doc Doc, err error) {
	if err = c.alive(); err != nil {
		return
	}
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	buf.SmallBuf = anyenc.AppendAnyValue(buf.SmallBuf[:0], docId)
	if ctx.Value(ctxKeyTx) == nil {
		return c.findIdOwnTx(p, buf)
	}

	err = c.db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		s, err := c.resolve(tx)
		if err != nil {
			return err
		}
		buf.DocBuf, err = tx.AppendValue(s.ns, buf.SmallBuf, buf.DocBuf[:0])
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				return ErrDocNotFound
			}
			return err
		}
		data, err := p.Parse(buf.DocBuf)
		doc = item{val: data}
		return
	})
	return doc, err
}

// findIdOwnTx is the point lookup of a caller without a transaction: one
// fast read tx around the fetch of the key in buf.SmallBuf. A function of
// its own to keep its return count low: past returns x defers = 15 the
// compiler stops open-coding defers, which costs this path about 3%.
func (c *collection) findIdOwnTx(p *anyenc.Parser, buf *syncpool.DocBuffer) (doc Doc, err error) {
	tx, err := c.db.btreeDB.BeginReadFast()
	if err != nil {
		return nil, err
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && err == nil {
			err = rbErr
		}
	}()
	// The fast begin skips checkStale; the schema check it needs is this.
	c.db.observeCookie(tx)
	s, err := c.resolve(tx)
	if err != nil {
		return nil, err
	}
	if buf.DocBuf, err = tx.AppendValue(s.ns, buf.SmallBuf, buf.DocBuf[:0]); err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			err = ErrDocNotFound
		}
		return nil, err
	}
	data, err := p.Parse(buf.DocBuf)
	if err != nil {
		return nil, err
	}
	return item{val: data}, nil
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
	// Inserts drive IVF-PQ centroid drift (they create no HNSW tombstones, so this
	// is a no-op for the graph modes beyond a cheap enabled-check), so an insert can
	// cross a vector index's auto-maintenance threshold just like an update/delete.
	// returned is set once the write tx returns; err == nil alone would also hold
	// while a panic unwinds, and compaction opens its own write tx — it must never
	// run mid-panic. See doWriteTxW.
	returned := false
	defer func() {
		if returned && err == nil {
			c.maybeAutoCompactVectors(ctx)
		}
	}()
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) (txErr error) {
		s, txErr := c.resolve(&tx.ReadTx)
		if txErr != nil {
			return txErr
		}
		var it item
		for _, doc := range docs {
			buf.Arena.Reset()
			if it, txErr = c.newItem(doc); txErr != nil {
				return txErr
			}
			if txErr = c.insertItem(tx, s, buf, it); txErr != nil {
				return txErr
			}
		}
		return nil
	})
	returned = true
	return
}

func (c *collection) insertItem(tx *btree.WriteTx, s *collSchema, buf *syncpool.DocBuffer, it item) (err error) {
	buf.SmallBuf = c.appendId(buf.SmallBuf[:0], it.Value())
	if c.compressionDisabled() {
		buf.DocBuf = it.Value().MarshalTo(buf.DocBuf[:0])
	} else {
		buf.DocBuf, buf.ScratchBuf = it.Value().MarshalCompressed(buf.DocBuf[:0], buf.ScratchBuf)
	}

	// Check if key already exists
	if _, err := tx.Get(s.ns, buf.SmallBuf); err == nil {
		return ErrDocExists
	}

	if err = tx.Put(s.ns, buf.SmallBuf, buf.DocBuf); err != nil {
		return err
	}

	// Insert index entries
	for _, idx := range s.indexes {
		if err = idx.insertKeys(tx, it); err != nil {
			return err
		}
	}
	for _, fx := range s.ftsIndexes {
		if err = fx.insertDoc(tx, it); err != nil {
			return err
		}
	}
	for _, vi := range s.vindexes {
		if err = vi.insert(tx, it, nil); err != nil {
			return err
		}
	}
	return nil
}

func (c *collection) UpdateOne(ctx context.Context, doc *anyenc.Value) (err error) {
	// See Insert: gated on a normal return, not on err == nil alone.
	returned := false
	defer func() {
		if returned && err == nil {
			c.maybeAutoCompactVectors(ctx)
		}
	}()
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	var it item
	if it, err = c.newItem(doc); err != nil {
		return
	}

	err = c.db.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		s, txErr := c.resolve(&tx.ReadTx)
		if txErr != nil {
			return false, txErr
		}
		return c.update(tx, s, it, item{})
	})
	returned = true
	return
}

func (c *collection) UpdateId(ctx context.Context, id any, mod query.Modifier) (res ModifyResult, err error) {
	// mod.Modify (user code) runs inside the write tx below. A panic there unwinds
	// through here with err still nil, so gate on a normal return: compaction opens
	// its own write tx and must never run mid-panic. See Insert and doWriteTxW.
	returned := false
	defer func() {
		if returned && err == nil {
			c.maybeAutoCompactVectors(ctx)
		}
	}()
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	buf2 := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf2)

	err = c.db.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		s, txErr := c.resolve(&tx.ReadTx)
		if txErr != nil {
			return false, txErr
		}
		buf.SmallBuf = anyenc.AppendAnyValue(buf.SmallBuf[:0], id)
		it, txErr := c.loadById(tx, s, buf, buf.SmallBuf)
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
		newIt, vErr := c.newItem(newVal)
		if vErr != nil {
			return false, vErr
		}
		return c.update(tx, s, newIt, it)
	})
	returned = true
	if err != nil {
		return ModifyResult{}, err
	}
	return
}

func (c *collection) UpsertId(ctx context.Context, id any, mod query.Modifier) (res ModifyResult, err error) {
	// mod.Modify (user code) runs inside the write tx below; see UpdateId.
	returned := false
	defer func() {
		if returned && err == nil {
			c.maybeAutoCompactVectors(ctx)
		}
	}()
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	buf2 := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf2)

	err = c.db.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		s, txErr := c.resolve(&tx.ReadTx)
		if txErr != nil {
			return false, txErr
		}
		buf.SmallBuf = anyenc.AppendAnyValue(buf.SmallBuf[:0], id)
		var (
			isInsert bool
			modValue *anyenc.Value
			prevItem item
		)
		it, loadErr := c.loadById(tx, s, buf, buf.SmallBuf)
		if loadErr != nil {
			if errors.Is(loadErr, ErrDocNotFound) {
				var idVal *anyenc.Value
				buf.Arena.Reset()
				modValue = buf.Arena.NewObject()
				idVal, txErr = buf.Parser.Parse(buf.SmallBuf)
				if txErr != nil {
					return false, txErr
				}
				modValue.Set(c.primaryKey, idVal)
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
		newIt, vErr := c.newItem(newVal)
		if vErr != nil {
			return false, vErr
		}
		if isInsert {
			txErr = c.insertItem(tx, s, buf2, newIt)
			return true, txErr
		} else {
			res.Matched = 1
			return c.update(tx, s, newIt, prevItem)
		}
	})
	returned = true
	if err != nil {
		return ModifyResult{}, err
	}
	return
}

func (c *collection) update(tx *btree.WriteTx, s *collSchema, it, prevIt item) (modified bool, err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	buf.SmallBuf = c.appendId(buf.SmallBuf[:0], it.Value())
	if prevIt.val == nil {
		prevIt, err = c.loadById(tx, s, buf, buf.SmallBuf)
		if err != nil {
			return
		}

		if anyencutil.Equal(prevIt.Value(), it.Value()) {
			return false, nil
		}
	}

	// Primary key is immutable (Mongo _id semantics): reject a pk change
	// instead of leaving a ghost data record under the old key. buf.SmallBuf
	// already holds the new pk; compare it against the previous item's pk.
	if oldId := c.appendId(nil, prevIt.Value()); !bytes.Equal(oldId, buf.SmallBuf) {
		return false, ErrPrimaryKeyModification
	}

	// Update index entries: delete old, insert new
	for _, idx := range s.indexes {
		if err = idx.deleteKeys(tx, prevIt); err != nil {
			return
		}
		if err = idx.insertKeys(tx, it); err != nil {
			return
		}
	}
	for _, fx := range s.ftsIndexes {
		if err = fx.updateDoc(tx, prevIt, it); err != nil {
			return
		}
	}
	// Vector indexes: only touch the graph when the embedding actually changed.
	for _, vi := range s.vindexes {
		if err = vi.update(tx, prevIt, it); err != nil {
			return
		}
	}

	if c.compressionDisabled() {
		buf.DocBuf = it.Value().MarshalTo(buf.DocBuf[:0])
	} else {
		buf.DocBuf, buf.ScratchBuf = it.Value().MarshalCompressed(buf.DocBuf[:0], buf.ScratchBuf)
	}
	if err = tx.Put(s.ns, buf.SmallBuf, buf.DocBuf); err != nil {
		return
	}

	return true, nil
}

func (c *collection) loadById(tx *btree.WriteTx, s *collSchema, buf *syncpool.DocBuffer, id anyenc.Tuple) (it item, err error) {
	buf.DocBuf, err = tx.AppendValue(s.ns, id, buf.DocBuf[:0])
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return item{}, ErrDocNotFound
		}
		return
	}
	doc, err := buf.Parser.ParseOwned(buf.DocBuf)
	if err != nil {
		return
	}
	return c.newItem(doc)
}

func (c *collection) UpsertOne(ctx context.Context, doc *anyenc.Value) (err error) {
	// See Insert: gated on a normal return, not on err == nil alone.
	returned := false
	defer func() {
		if returned && err == nil {
			c.maybeAutoCompactVectors(ctx)
		}
	}()
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	var it item
	if it, err = c.newItem(doc); err != nil {
		return
	}

	err = c.db.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		s, txErr := c.resolve(&tx.ReadTx)
		if txErr != nil {
			return false, txErr
		}
		insErr := c.insertItem(tx, s, buf, it)
		if errors.Is(insErr, ErrDocExists) {
			return c.update(tx, s, it, item{})
		}
		if insErr != nil {
			return false, insErr
		}
		return true, nil
	})
	returned = true
	return err
}

func (c *collection) DeleteId(ctx context.Context, id any) (err error) {
	// See Insert: gated on a normal return, not on err == nil alone.
	returned := false
	defer func() {
		if returned && err == nil {
			c.maybeAutoCompactVectors(ctx)
		}
	}()
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	err = c.db.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		s, txErr := c.resolve(&tx.ReadTx)
		if txErr != nil {
			return false, txErr
		}
		buf.SmallBuf = anyenc.AppendAnyValue(buf.SmallBuf[:0], id)
		// Verify document exists
		_, txErr = c.loadById(tx, s, buf, buf.SmallBuf)
		if txErr != nil {
			return
		}
		return true, c.deleteItem(tx, s, buf, buf.SmallBuf)
	})
	returned = true
	return err
}

func (c *collection) deleteItem(tx *btree.WriteTx, s *collSchema, buf *syncpool.DocBuffer, id []byte) (err error) {
	// Delete index entries. The document is loaded once and reused for the range,
	// full-text and vector index removals (all need the field values).
	idxs, ftsIdxs, vidxs := s.indexes, s.ftsIndexes, s.vindexes
	if len(idxs) > 0 || len(ftsIdxs) > 0 || len(vidxs) > 0 {
		it, loadErr := c.loadById(tx, s, buf, id)
		if loadErr != nil {
			return loadErr
		}
		for _, idx := range idxs {
			if err = idx.deleteKeys(tx, it); err != nil {
				return err
			}
		}
		for _, fx := range ftsIdxs {
			if err = fx.deleteDoc(tx, it); err != nil {
				return err
			}
		}
		for _, vi := range vidxs {
			if err = vi.delete(tx, it); err != nil {
				return err
			}
		}
	}
	return tx.Delete(s.ns, id)
}

func (c *collection) Count(ctx context.Context) (count int, err error) {
	err = c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		s, txErr := c.resolve(tx)
		if txErr != nil {
			return txErr
		}
		count, txErr = tx.Count(s.ns)
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
	return c.db.doWriteTxModifiedW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (modified bool, txErr error) {
		if txErr = c.beginDDL(wtx); txErr != nil {
			return false, txErr
		}
		name := c.cur().name
		var newIndexes []*index
		var newFtsIndexes []*ftsIndex
		var newVIndexes []*vectorIndex
		for _, idxInfo := range info {
			if isFulltext(idxInfo) {
				if txErr = c.checkSingleFulltextIndex(idxInfo, newFtsIndexes); txErr != nil {
					return false, txErr
				}
				fx, fErr := c.createFtsIndex(ctx, tx, idxInfo)
				if fErr != nil {
					if ensure && errors.Is(fErr, ErrIndexExists) {
						continue
					}
					return false, fErr
				}
				newFtsIndexes = append(newFtsIndexes, fx)
				continue
			}
			if idxInfo.Kind == IndexKindVector {
				vi, viErr := c.createVectorIndex(tx, idxInfo)
				if viErr != nil {
					if ensure && errors.Is(viErr, ErrIndexExists) {
						continue
					}
					return false, viErr
				}
				newVIndexes = append(newVIndexes, vi)
				continue
			}
			idx, txErr := c.createIndex(ctx, tx, idxInfo)
			if txErr != nil {
				// ensure=true is idempotent only for an EXISTING index whose
				// definition matches. A same-name index with a DIFFERENT
				// definition is a genuine conflict (ErrIndexExists) that must be
				// surfaced even under ensure — never silently kept at the old
				// shape. createIndex returns ErrIndexMismatch for that case so it
				// is not swallowed here.
				if ensure && errors.Is(txErr, ErrIndexExists) {
					continue
				}
				return false, txErr
			}
			newIndexes = append(newIndexes, idx)
		}

		if len(newIndexes) == 0 && len(newFtsIndexes) == 0 && len(newVIndexes) == 0 {
			return false, nil
		}

		c.mu.Lock()
		defer c.mu.Unlock()
		c.registerHeadRestore(wtx)
		// The new handles' namespaces exist only in this tx's uncommitted
		// view: stamp them valid from the cookie this commit will publish —
		// every create path MarkSchemaChanged, and SchemaCookie bumps exactly
		// once per schema-changing commit, atomically with the DDL (the
		// OP_Transaction analog, vdbe.c:4091-4192 in SQLite), so there is no
		// commit→publication gap for readers to fall into. A rollback
		// discards the handles themselves with the version they are in.
		validFrom := tx.DiskSchemaCookie() + 1
		for _, idx := range newIndexes {
			idx.validFromCookie = validFrom
		}
		for _, fx := range newFtsIndexes {
			fx.validFromCookie = validFrom
		}
		for _, vi := range newVIndexes {
			vi.bindIdentity(name)
			vi.validFromCookie = validFrom
		}
		c.publish(tx, func(s *collSchema) {
			s.indexes = append(s.indexes[:len(s.indexes):len(s.indexes)], newIndexes...)
			s.ftsIndexes = append(s.ftsIndexes[:len(s.ftsIndexes):len(s.ftsIndexes)], newFtsIndexes...)
			s.vindexes = append(s.vindexes[:len(s.vindexes):len(s.vindexes)], newVIndexes...)
		})
		return true, nil
	})
}

// registerHeadRestore registers the return to the present head on rollback
// of the given tx scope (see commonTx.undo). A reverted schema change must
// not leave the head pointing at state the on-disk catalog no longer has (a
// created index over freed namespace pages) or missing state it still has (a
// rolled-back drop whose index would silently stop being maintained). Call
// with c.mu held, BEFORE publishing. Nothing else replaces a head published
// by the open write tx (resolveSlow), so the restore undoes that and only
// that.
func (c *collection) registerHeadRestore(wtx WriteTx) {
	prev := c.cur()
	wtx.onRollbackUndo(func() {
		c.mu.Lock()
		c.head.Store(prev)
		c.mu.Unlock()
	})
}

func (c *collection) createIndex(ctx context.Context, tx *btree.WriteTx, info IndexInfo) (idx *index, err error) {
	tx.MarkSchemaChanged()
	if info.Name == "" {
		info.Name = info.createName()
	}
	if err = validateIndexName(info.Name); err != nil {
		return nil, err
	}
	for _, field := range info.Fields {
		if err = validateIndexField(field); err != nil {
			return nil, err
		}
	}

	// Register in system namespace
	if err = c.db.registerIndex(tx, c.cur().name, info); err != nil {
		return nil, err
	}
	return c.buildRangeIndex(tx, info)
}

// buildRangeIndex creates the index namespace and fills it from the
// collection's documents; the catalog record is already registered and
// stamped with the current format. Shared by createIndex and rebuildIndex.
func (c *collection) buildRangeIndex(tx *btree.WriteTx, info IndexInfo) (idx *index, err error) {
	nsName := indexNsName(c.cur().name, info.Name)
	ns, err := tx.CreateNamespace(nsName)
	if err != nil {
		return nil, err
	}

	idx, err = newIndex(c, c.cur().name, info, ns)
	if err != nil {
		return nil, err
	}
	idx.format = indexFormatVersion

	// Write the scalar-so-far multikey marker BEFORE the backfill: buildIndex
	// runs through insertKeys, which flips it to multikey in this same tx if
	// the existing docs fan out. Writing the marker after the backfill would
	// clobber that flip and unsoundly enable tight seeks.
	if err = tx.Put(c.db.systemNS, multikeyKey(nsName), mkValScalar); err != nil {
		return nil, err
	}

	// Initialize sketch for the new index (one prefix level per index field)
	idx.sketch = qplanner.NewIndexSketch(qplanner.DefaultSketchSize, len(idx.fieldPaths))

	// Build index from existing documents (also populates sketch via insertKeys)
	if err = c.buildIndex(tx, idx); err != nil {
		return nil, err
	}

	// Persist the sketch
	skKey := sketchKey(c.cur().name, info.Name)
	if err = tx.Put(c.db.systemNS, skKey, idx.sketch.MarshalBinary(nil)); err != nil {
		return nil, err
	}
	// Publish the live sketch as the reader snapshot before the index becomes
	// reader-visible (added to the index set by the caller), so no reader ever
	// observes a nil sketchPub.
	idx.storePubSketch(idx.sketch)
	idx.sketchModified = false

	return idx, nil
}

// createFtsIndex creates the five namespaces of a full-text index, registers
// its metadata, and backfills existing documents. Mirrors createIndex.
// checkSingleFulltextIndex enforces at most one full-text index per collection.
// A redefinition under the SAME name is not a second index — createFtsIndex
// decides whether that is idempotent (ErrIndexExists, swallowed under ensure)
// or a genuine conflict (ErrIndexMismatch) — so only a DIFFERENT name is
// rejected here. pending covers indexes created earlier in this same batch,
// which are not published to c.loadFtsIndexes() until the tx commits.
func (c *collection) checkSingleFulltextIndex(info IndexInfo, pending []*ftsIndex) error {
	name := info.Name
	if name == "" {
		name = info.createName()
	}
	existing := ""
	for _, fx := range c.loadFtsIndexes() {
		if fx.info.Name != name {
			existing = fx.info.Name
			break
		}
	}
	if existing == "" {
		for _, fx := range pending {
			if fx.info.Name != name {
				existing = fx.info.Name
				break
			}
		}
	}
	if existing != "" {
		return fmt.Errorf("%w (have %q, requested %q)", ErrMultipleFulltextIndexes, existing, name)
	}
	return nil
}

func (c *collection) createFtsIndex(ctx context.Context, tx *btree.WriteTx, info IndexInfo) (*ftsIndex, error) {
	tx.MarkSchemaChanged()
	if info.Name == "" {
		info.Name = info.createName()
	}
	if err := validateIndexName(info.Name); err != nil {
		return nil, err
	}
	for _, field := range info.Fields {
		if err := validateIndexField(field); err != nil {
			return nil, err
		}
	}

	// Register in system namespace (carries Kind so reopen rebuilds an fts index).
	if err := c.db.registerIndex(tx, c.cur().name, info); err != nil {
		return nil, err
	}

	fx, err := newFtsIndex(c, c.cur().name, info)
	if err != nil {
		return nil, err
	}

	// Create the five namespaces.
	for _, nsName := range ftsIndexNames(c.cur().name, info.Name) {
		if _, err = tx.CreateNamespace(nsName); err != nil {
			return nil, err
		}
	}
	if err = fx.bindNamespaces(c.cur().name, tx.GetNamespace); err != nil {
		return nil, err
	}

	// Backfill existing documents.
	if err = c.buildFtsIndex(tx, fx); err != nil {
		return nil, err
	}
	return fx, nil
}

// buildFtsIndex indexes every existing document into a new full-text index.
func (c *collection) buildFtsIndex(tx *btree.WriteTx, fx *ftsIndex) error {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	cursor := tx.NewCursor(c.cur().ns)
	defer cursor.Close()
	if err := cursor.First(); err != nil {
		return err
	}
	for cursor.Valid() {
		val, err := cursor.Value()
		if err != nil {
			return err
		}
		buf.DocBuf = append(buf.DocBuf[:0], val...)
		doc, err := buf.Parser.ParseOwned(buf.DocBuf)
		if err != nil {
			return err
		}
		it, err := c.newItem(doc)
		if err != nil {
			return err
		}
		if err = fx.insertDoc(tx, it); err != nil {
			return err
		}
		if err = cursor.Next(); err != nil {
			return err
		}
	}
	// Stamp the postings format version so a future upgrade can detect old data.
	return fx.putMetaUint(tx, ftsMetaFormat, ftsFormatVersion)
}

func (c *collection) DropIndex(ctx context.Context, indexName string) (err error) {
	return c.db.doWriteTxW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (txErr error) {
		if txErr = c.beginDDL(wtx); txErr != nil {
			return txErr
		}
		tx.MarkSchemaChanged()
		// beginDDL resolved the handle for this write transaction, so the
		// head reflects on-disk truth: an index a peer created is present
		// (drop must succeed), and one a peer dropped is absent (return
		// ErrIndexNotFound, not a false success).
		c.mu.Lock()
		defer c.mu.Unlock()
		c.registerHeadRestore(wtx)

		// Vector index drop: unregister, delete its namespaces, republish.
		for _, vi := range c.loadVectorIndexes() {
			if vi.info.Name != indexName {
				continue
			}
			if txErr = c.db.removeIndex(tx, c.cur().name, indexName); txErr != nil {
				return
			}
			if txErr = dropVectorIndexNamespaces(tx, c.cur().name, indexName); txErr != nil {
				return
			}
			cur := c.loadVectorIndexes()
			next := make([]*vectorIndex, 0, len(cur))
			for _, v := range cur {
				if v.info.Name != indexName {
					next = append(next, v)
				}
			}
			c.publish(tx, func(s *collSchema) { s.vindexes = next })
			return nil
		}

		found := false
		isFts := false
		for _, idx := range c.loadIndexes() {
			if idx.info.Name == indexName {
				found = true
				break
			}
		}
		if !found {
			for _, fx := range c.loadFtsIndexes() {
				if fx.info.Name == indexName {
					found, isFts = true, true
					break
				}
			}
		}
		if !found {
			return ErrIndexNotFound
		}

		// removeIndex tolerates an already-absent metadata key: a concurrent peer
		// may have dropped the same index between our checkStale snapshot and
		// here, and a raw btree.ErrKeyNotFound must never escape DropIndex (whose
		// contract is nil or ErrIndexNotFound).
		if txErr = c.db.removeIndex(tx, c.cur().name, indexName); txErr != nil {
			return
		}

		if isFts {
			// Delete the five full-text namespaces.
			for _, nsName := range ftsIndexNames(c.cur().name, indexName) {
				if txErr = tx.DeleteNamespace(nsName); txErr != nil {
					if !errors.Is(txErr, btree.ErrNamespaceNotFound) {
						return
					}
					txErr = nil
				}
			}
			cur := c.loadFtsIndexes()
			next := make([]*ftsIndex, 0, len(cur))
			for _, fx := range cur {
				if fx.info.Name != indexName {
					next = append(next, fx)
				}
			}
			c.publish(tx, func(s *collSchema) { s.ftsIndexes = next })
			return nil
		}

		// Delete the index namespace
		nsName := indexNsName(c.cur().name, indexName)
		if txErr = tx.DeleteNamespace(nsName); txErr != nil {
			if !errors.Is(txErr, btree.ErrNamespaceNotFound) {
				return
			}
			txErr = nil
		}
		// Delete sketch data
		skKey := sketchKey(c.cur().name, indexName)
		_ = tx.Delete(c.db.systemNS, skKey) // ignore if not found
		// Delete the multikey flag (keyed by the immutable namespace name —
		// resolve it from the live index object, since nsName computed from
		// the CURRENT collection name is wrong after a rename): drop+recreate
		// is the flag's only reset path.
		for _, idx := range c.loadIndexes() {
			if idx.info.Name == indexName {
				_ = tx.Delete(c.db.systemNS, multikeyKey(idx.ns.Name()))
				break
			}
		}
		// Copy-on-write publish: build a fresh slice without the dropped index
		// and swap it in atomically for lock-free query readers.
		cur := c.loadIndexes()
		next := make([]*index, 0, len(cur))
		for _, idx := range cur {
			if idx.info.Name != indexName {
				next = append(next, idx)
			}
		}
		c.publish(tx, func(s *collSchema) { s.indexes = next })
		return nil
	})
}

// committed returns the head, loading one first if none is installed yet (a
// handle opened through a transaction too old to speak for it, see
// resolveSlow); nil if that fails. For the accessors that take no context.
func (c *collection) committed() *collSchema {
	if s := c.cur(); s != nil {
		return s
	}
	_ = c.db.doReadTx(context.Background(), func(tx *btree.ReadTx) error {
		_, err := c.resolve(tx)
		return err
	})
	return c.cur()
}

func (c *collection) GetIndexes() (indexes []Index) {
	s := c.committed()
	if s == nil {
		return nil
	}
	idxs := s.indexes
	ftsIdxs := s.ftsIndexes
	indexes = make([]Index, 0, len(idxs)+len(ftsIdxs))
	for _, idx := range idxs {
		indexes = append(indexes, idx)
	}
	for _, fx := range ftsIdxs {
		indexes = append(indexes, fx)
	}
	return
}

func (c *collection) Rename(ctx context.Context, newName string) error {
	return c.db.doWriteTxW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (err error) {
		if err = c.beginDDL(wtx); err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()

		prev := c.cur()
		oldName := prev.name
		if newName == oldName {
			return nil
		}
		// A foreign live handle under the new name implies the name is taken
		// even if some stale state made the catalog check below miss it.
		c.db.mu.Lock()
		if cur, ok := c.db.openedCollections[newName]; ok && cur != Collection(c) {
			c.db.mu.Unlock()
			return ErrCollectionExists
		}
		c.db.mu.Unlock()

		// Re-keys the catalog metadata AND every derived btree namespace
		// (data, per-index) in this tx.
		if err = c.db.renameCollection(tx, oldName, newName); err != nil {
			return err
		}

		// Re-bind range-index handles to their renamed namespaces. The old
		// handles keep working by root page, but their stale Name() would
		// feed multikeyKey: a post-rename fan-out write would then flag the
		// WRONG record, and a later rename back would clobber the real one —
		// re-enabling tight seeks over an index holding arrays (dropped
		// docs). Published copy-on-write: readers access idx.ns lock-free.
		prevIdxs := c.loadIndexes()
		next := make([]*index, len(prevIdxs))
		for i, idx := range prevIdxs {
			nsName := indexNsName(newName, idx.info.Name)
			ns, nsErr := tx.GetNamespace(nsName)
			if nsErr != nil {
				if errors.Is(nsErr, btree.ErrNamespaceNotFound) {
					// Legacy-broken index (pre-fix rename victim): the sweep
					// tolerated it; keep the old handle.
					next[i] = idx
					continue
				}
				return nsErr
			}
			next[i] = idx.cloneWithNs(ns, nsName, indexKey(newName, idx.info.Name))
		}
		// The data namespace handle is rebound with the rest: a version is
		// what one view has under one name.
		ns, err := tx.GetNamespace(newName)
		if err != nil {
			return err
		}
		renameCookie := tx.DiskSchemaCookie() + 1
		c.publish(tx, func(s *collSchema) {
			s.indexes, s.name, s.ns = next, newName, ns
			names := s.names
			if names == nil {
				names = []nameSince{{name: oldName}}
			}
			s.names = append(names[:len(names):len(names)], nameSince{name: newName, from: renameCookie})
		})
		c.db.renaming = append(c.db.renaming, c)

		// The handle registry is re-keyed only at COMMIT (see commonTx.pubs):
		// re-keying here would open a window where a concurrent
		// OpenCollection(oldName) misses the map, passes the catalog check on
		// the still-committed old snapshot, and registers a duplicate live
		// handle the rename published through this one never reaches. Until
		// commit, OpenCollection(oldName)
		// keeps returning this handle — correct for the committed state: a
		// transaction at a snapshot before the rename resolves it under the
		// name of that snapshot (collSchema.names).
		// Deliberately untouched: the fts/vector-internal namespace handles
		// — root pages are unchanged and nothing derives keys from their
		// stale internal names; fts pending buffers flush at commit through
		// those handles into the renamed namespaces.
		wtx.onCommitPublish(func() {
			c.db.renameResolved(c)
			// The key flips to the name the handle commits with, not to
			// newName: after a further rename in this tx newName is an
			// intermediate name, free for a collection created since, and
			// that collection's handle holds the entry.
			committed := c.cur().name
			c.db.mu.Lock()
			if cur, ok := c.db.openedCollections[oldName]; ok && cur == Collection(c) {
				delete(c.db.openedCollections, oldName)
				// A Rename→Drop in the same tx closed and evicted the handle
				// already; don't resurrect it under the new name.
				if !c.closed.Load() {
					// An open of the new name between the btree commit and
					// this publication missed the registry and registered a
					// handle of its own. Displaced it would stay live outside
					// the registry: retire it.
					if other, ok := c.db.openedCollections[committed]; ok && other != Collection(c) {
						other.(*collection).closed.Store(true)
					}
					c.db.openedCollections[committed] = c
				}
			}
			c.db.mu.Unlock()
		})

		// A rollback restores the old catalog and namespaces; the in-memory
		// name and index generation must follow (the map was never touched —
		// the commit publication above is dropped unrun). Post-rename writes
		// in this tx mutated the sketches SHARED with the restored snapshot
		// through the clones: propagate their modified flags so the next tx
		// begin rebases the restored sketches to committed bytes.
		wtx.onRollbackUndo(func() {
			c.db.renameResolved(c)
			c.mu.Lock()
			defer c.mu.Unlock()
			for i, idx := range prevIdxs {
				if next[i] != idx && next[i].sketchModified {
					idx.markSketchModified()
				}
			}
			c.head.Store(prev)
		})
		return nil
	})
}

// Drop closes the handle at execution time (same-tx semantics: later ops
// through any alias fail ErrCollectionClosed) but defers the eviction from
// db.openedCollections to COMMIT (see commonTx.pubs). Evicting at execution —
// what Close() does — opens a corruption window: a concurrent
// OpenCollection(name) misses the map, passes the catalog check on the
// still-committed snapshot, and registers a fresh live handle the drop never
// reaches; once the drop commits and a new collection reuses the freed root pages,
// writes through that handle land inside the wrong collection. Deferring the
// eviction keeps the closed handle registered through the window, so a
// concurrent open returns it and fails fail-safe instead. A rollback un-closes
// the handle: the btree restored the on-disk catalog, the map entry is the
// handle again (a same-tx recreate that replaced it put it back in its own
// undo), and Drop mutates no in-memory index set — so the handle is whole
// again.
func (c *collection) Drop(ctx context.Context) error {
	return c.db.doWriteTxW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (err error) {
		if err = c.beginDDL(wtx); err != nil {
			return err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		// Discard buffered fts writes: Drop deletes the fts namespaces in this
		// same tx — flushing a surviving buffer at commit would write into
		// dropped namespaces and fail the whole transaction.
		for _, fx := range c.loadFtsIndexes() {
			fx.pending.reset()
		}
		// CAS, not Store: the undo reverts only the flip this Drop made.
		if c.closed.CompareAndSwap(false, true) {
			wtx.onRollbackUndo(func() {
				// A Close() requested meanwhile stays pending: ddlEnd applies
				// it once the handle's last schema change resolves.
				// Un-close only while registered: a handle outside the
				// registry would dangle past future peer DDL (the staleness
				// pass only walks the registry).
				c.db.mu.Lock()
				for _, cur := range c.db.openedCollections {
					if cur == Collection(c) {
						c.closed.Store(false)
						break
					}
				}
				c.db.mu.Unlock()
			})
		}
		// The eviction is an identity scan: a Rename earlier in this tx
		// re-keys the entry in its own, earlier publication and declines to
		// resurrect a closed handle; a same-tx recreate replaced it, making
		// this a no-op.
		wtx.onCommitPublish(func() {
			c.db.mu.Lock()
			c.db.evictLocked(c)
			c.db.mu.Unlock()
		})
		// Delete all index namespaces. Enumerate indexes from the SAME on-disk
		// source (idx:<coll>: metadata keys) that removeCollection deletes,
		// rather than the in-memory index set which can lag the on-disk metadata
		// (a peer handle's create, or this handle's own create that committed but
		// has not yet been published to the in-memory snapshot). This keeps the
		// namespace-delete set and the metadata-delete set identical within the
		// single atomic Drop tx, so Drop can never leave an orphaned index
		// namespace. Enumerate BEFORE removeCollection deletes the idx: keys.
		idxInfos, err := c.db.getIndexInfos(&tx.ReadTx, c.cur().name)
		if err != nil {
			return err
		}
		for _, info := range idxInfos {
			if info.Kind == IndexKindVector {
				if err = dropVectorIndexNamespaces(tx, c.cur().name, info.Name); err != nil {
					return
				}
				continue
			}
			var nsNames []string
			if isFulltext(info) {
				nsNames = ftsIndexNames(c.cur().name, info.Name)
			} else {
				nsNames = []string{indexNsName(c.cur().name, info.Name)}
			}
			for _, nsName := range nsNames {
				if err = tx.DeleteNamespace(nsName); err != nil {
					if !errors.Is(err, btree.ErrNamespaceNotFound) {
						return
					}
				}
			}
		}
		if err = c.db.removeCollection(tx, c.cur().name); err != nil {
			return
		}
		// Delete the collection namespace
		if err = tx.DeleteNamespace(c.cur().name); err != nil {
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
	return c.close(false)
}

// close evicts the handle from the registry and fails its later operations.
// While the handle carries an uncommitted schema change (see ddlTxs) the
// close waits until the last of them is committed or rolled back, and the
// handle stays usable until then — unless force: the database is closing and
// no transaction will end.
func (c *collection) close(force bool) error {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	if c.ddlTxs > 0 && !force {
		c.closePending.Store(true)
		return nil
	}
	if c.closed.CompareAndSwap(false, true) {
		c.db.evictLocked(c)
	}
	return nil
}

// beginDDL rejects a handle that is no longer live and marks a live one as
// carrying an uncommitted schema change of wtx (see ddlTxs) until that scope
// commits or rolls back. Check and mark share db.mu with close, so a Close()
// cannot evict the handle between them. A verb that then changes nothing
// keeps the mark all the same; it only delays a Close().
func (c *collection) beginDDL(wtx WriteTx) error {
	// The verb works on the head: bring it to the write transaction's view
	// first (a handle another process's schema change left unverified, or
	// one with no version installed). Before c.mu, which the load takes.
	if _, err := c.resolve(wtx.btreeReadTx()); err != nil {
		return err
	}
	c.db.mu.Lock()
	if err := c.alive(); err != nil {
		c.db.mu.Unlock()
		return err
	}
	c.ddlTxs++
	c.db.mu.Unlock()
	c.registerDDLEnd(wtx)
	return nil
}

// registerDDLEnd releases one ddlTxs mark when the scope of wtx resolves:
// exactly one of the two callbacks runs per outcome. Registered before the
// verb's own callbacks, so on a rollback it runs after the verb's undo has
// restored the handle.
func (c *collection) registerDDLEnd(wtx WriteTx) {
	wtx.onRollbackUndo(func() { c.db.ddlEnd(c) })
	wtx.onCommitPublish(func() { c.db.ddlEnd(c) })
}

// alive rejects operations on a handle that is no longer live: explicitly
// closed, dropped, or retired because another process renamed or dropped the
// collection (see retire). Operations check it INSIDE their transaction
// scope, through resolve, which is also what retires a handle.
func (c *collection) alive() error {
	if c.closed.Load() {
		if c.db.closed.Load() {
			return ErrDBIsClosed
		}
		return ErrCollectionClosed
	}
	return nil
}

// buildIndex populates index entries from all existing documents in the collection.
func (c *collection) buildIndex(tx *btree.WriteTx, idx *index) error {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	cursor := tx.NewCursor(c.cur().ns)
	defer cursor.Close()
	if err := cursor.First(); err != nil {
		return err
	}
	for cursor.Valid() {
		val, err := cursor.Value()
		if err != nil {
			return err
		}
		buf.DocBuf = append(buf.DocBuf[:0], val...)
		doc, err := buf.Parser.ParseOwned(buf.DocBuf)
		if err != nil {
			return err
		}
		it, err := c.newItem(doc)
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

// loadSketchAtOpen populates a brand-new (not-yet-reader-visible) index's live
// sketch from the _system namespace and publishes it as the reader snapshot.
// Called from init, createIndex, and reconcile's rebuild arm — in every case the
// index is not yet in the index set, so filling live in place and publishing it is
// unobservable to readers (no copy-on-write ceremony needed).
func (c *collection) loadSketchAtOpen(tx *btree.ReadTx, collName string, idx *index) {
	if idx.sketch == nil {
		idx.sketch = qplanner.NewIndexSketch(qplanner.DefaultSketchSize, len(idx.fieldPaths))
	}
	key := sketchKey(collName, idx.info.Name)
	if data, err := tx.AppendValue(c.db.systemNS, key, nil); err == nil {
		idx.sketch.UnmarshalBinary(data)
	}
	// Publish the live object as the initial reader snapshot. The index is not
	// yet visible to readers, so no reader can be holding the previous value.
	idx.storePubSketch(idx.sketch)
}

// reloadSketch refreshes one already-published index's sketch from the _system
// namespace during the advisory staleness tier (checkStale -> reloadSketches).
// It is the sqlite_stat1 reload analog and is advisory/fail-soft: a missing key
// or decode error leaves current state intact and never aborts the transaction.
//
//	WRITE tx (writable): the calling goroutine holds the btree writeMu and is the
//	  SOLE mutator of idx.sketch, so it reloads IN PLACE into the live object (0
//	  alloc) — catching up to a peer's committed counts BEFORE applying its own
//	  deltas (a reader that consumed the peer's commit first leaves live
//	  behind until the next stale verdict: advisory, see IndexSketch) — then
//	  republishes live so readers see the peer's state immediately.
//
//	READ tx (!writable): a concurrent in-process writer may be incrementing
//	  idx.sketch right now, so we NEVER touch it. The disk bytes are decoded
//	  into the reader-owned published sketch — in place, as the writer path
//	  does with its own: every field is read and written atomically and a
//	  planner tolerates a mix of two reloads (plan-time only, see IndexSketch)
//	  — and a fresh one is allocated only while the published sketch IS the
//	  live object (initially, and again after each commit republishes it).
//	  The writer's live object is untouched; its in-flight increments can
//	  never be lost.
func (c *collection) reloadSketch(tx *btree.ReadTx, idx *index, writable bool) {
	key := sketchKey(c.cur().name, idx.info.Name)
	data, err := tx.AppendValue(c.db.systemNS, key, c.sketchReadBuf[:0])
	if err != nil {
		return // no persisted bytes: preserve current state (advisory)
	}
	c.sketchReadBuf = data
	if writable {
		if idx.sketch == nil {
			idx.sketch = qplanner.NewIndexSketch(qplanner.DefaultSketchSize, len(idx.fieldPaths))
		}
		idx.sketch.UnmarshalBinary(data) // in-place into live (sole mutator)
		idx.storePubSketch(idx.sketch)   // republish live for readers
		// Live now equals the committed on-disk state, so any prior
		// uncommitted (e.g. rolled-back) deltas are discarded — clear the dirty
		// flag so they are not re-persisted on the next commit.
		idx.sketchModified = false
		return
	}
	// idx.sketch is only replaced under c.mu (held here) or before the index
	// is published, so the identity check is race-free; a commit that
	// republishes live concurrently (persistSketches takes no c.mu) is
	// overtaken by this snapshot's bytes, as a fresh object would be. Bytes
	// of another shape (a snapshot older than this handle's definition) go
	// into a fresh object: a published one is never reshaped.
	pub := idx.loadPubSketch()
	if pub == nil || pub == idx.sketch || !pub.SameShape(data) {
		pub = qplanner.NewIndexSketch(qplanner.DefaultSketchSize, len(idx.fieldPaths))
	}
	pub.UnmarshalBinary(data)
	idx.storePubSketch(pub)
}

// indexInfoEqual reports whether two IndexInfo values describe the same index
// definition: same name, same fields (order significant), same unique and
// sparse flags.
func indexInfoEqual(a, b IndexInfo) bool {
	if a.Name != b.Name || a.Unique != b.Unique || a.Sparse != b.Sparse {
		return false
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i] != b.Fields[i] {
			return false
		}
	}
	return true
}

// persistSketches writes all modified live sketches to the _system namespace and
// republishes each as the reader snapshot. Last-writer-wins (no cross-process
// merge — the sketch is advisory, like sqlite_stat1). Republishing is a pointer
// Store of the writer's own live object — no clone: the writer will not mutate
// live again until its next write tx, and a reader that loads it sees an
// advisory, atomically-fielded snapshot. Runs inside Commit before pager.commit,
// so the bytes are atomic with the file-change counter / schema cookie.
func (c *collection) persistSketches(tx *btree.WriteTx) error {
	for _, idx := range c.loadIndexes() {
		if idx.sketchModified {
			key := sketchKey(c.cur().name, idx.info.Name)
			idx.sketchBuf = idx.sketch.MarshalBinary(idx.sketchBuf)
			if err := tx.Put(c.db.systemNS, key, idx.sketchBuf); err != nil {
				return err
			}
			idx.storePubSketch(idx.sketch) // republish live as the reader snapshot
			idx.sketchModified = false
		}
	}
	return nil
}
