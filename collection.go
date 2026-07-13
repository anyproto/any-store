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

	// Close closes the collection.
	// Returns an error if the operation fails.
	Close() error
}

// newCollection creates and initializes a collection. When called from within
// a write transaction (e.g. CreateCollection), pass the WriteTx so that
// namespace resolution can see uncommitted pages. Pass nil otherwise.
func newCollection(ctx context.Context, db *db, name string, wtx ...*btree.WriteTx) (Collection, error) {
	coll := &collection{
		name: name,
		db:   db,
	}
	var tx *btree.WriteTx
	if len(wtx) > 0 {
		tx = wtx[0]
	}
	if err := coll.init(ctx, tx); err != nil {
		return nil, err
	}
	return coll, nil
}

type collection struct {
	name string
	// indexes is an atomic copy-on-write snapshot of the collection's index
	// set. The query path (query.go) reads it lock-free via loadIndexes();
	// structural mutations (createIndex / DropIndex / reconcileIndexSet) build a
	// fresh slice under c.mu and publish it via storeIndexes(). Readers always
	// observe a complete generation, never a torn slice — mirroring SQLite's
	// whole-schema reload on a schema-cookie bump (a reader sees either the old
	// or the new schema, never half-applied DDL).
	indexes atomic.Pointer[[]*index]
	// ftsIndexes is the parallel copy-on-write snapshot of full-text indexes,
	// maintained alongside indexes (range) on the same hot paths.
	ftsIndexes atomic.Pointer[[]*ftsIndex]
	// vindexes is the parallel CoW snapshot of vector (HNSW) indexes. The write
	// hooks update them alongside the range indexes; the Find() pipeline reads
	// them when a query has a `{vectorField: [..]}` clause.
	vindexes atomic.Pointer[[]*vectorIndex]
	db       *db
	ns       *btree.Namespace

	compression Compression // 0 = use db default

	// catalogID is the coll: record value captured at init — the collection's
	// identity token, compared by the staleness pass to detect that a peer
	// dropped/renamed this collection and a different one now answers for the
	// name. Set once in init (under c.mu), never mutated after (a rename moves
	// the token with the collection).
	catalogID []byte

	// primaryKey is the document field whose value is the btree key. Resolved
	// once in init (default "id") and never mutated after, so the data and query
	// paths read it lock-free.
	primaryKey string

	// sketchReadBuf is reused scratch for decoding persisted sketch bytes during
	// the advisory reload (reloadSketch). Only touched under c.mu, so a single
	// per-collection buffer is safe and keeps the stale-reload path from
	// allocating a fresh read buffer per index.
	sketchReadBuf []byte

	closed atomic.Bool
	mu     sync.Mutex
}

// loadIndexes returns the current index-set snapshot. Safe to call without
// holding c.mu — the slice it returns is immutable (publishers always build a
// fresh slice rather than mutating in place), so callers may range over it
// freely while a concurrent reconcile swaps in a new generation.
func (c *collection) loadIndexes() []*index {
	if p := c.indexes.Load(); p != nil {
		return *p
	}
	return nil
}

// storeIndexes publishes a new index-set snapshot. Callers must hold c.mu (to
// serialise concurrent publishers); readers need no lock.
func (c *collection) storeIndexes(idxs []*index) {
	c.indexes.Store(&idxs)
}

// loadFtsIndexes returns the current full-text index snapshot (lock-free).
func (c *collection) loadFtsIndexes() []*ftsIndex {
	if p := c.ftsIndexes.Load(); p != nil {
		return *p
	}
	return nil
}

// storeFtsIndexes publishes a new full-text index snapshot. Callers hold c.mu.
func (c *collection) storeFtsIndexes(idxs []*ftsIndex) {
	c.ftsIndexes.Store(&idxs)
}

// loadVectorIndexes returns the current vector-index snapshot (lock-free).
func (c *collection) loadVectorIndexes() []*vectorIndex {
	if p := c.vindexes.Load(); p != nil {
		return *p
	}
	return nil
}

// storeVectorIndexes publishes a new vector-index snapshot. Callers hold c.mu.
func (c *collection) storeVectorIndexes(vidxs []*vectorIndex) {
	c.vindexes.Store(&vidxs)
}

// init initializes the collection, loading namespace handles and index metadata.
// When wtx is non-nil (called from within a write transaction), namespace resolution
// uses the writer path to see uncommitted pages.
func (c *collection) init(ctx context.Context, wtx *btree.WriteTx) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// getNamespace resolves a namespace, using the writer path when available.
	getNamespace := func(name string) (*btree.Namespace, error) {
		if wtx != nil {
			return wtx.GetNamespace(name)
		}
		return c.db.btreeDB.GetNamespace(name)
	}

	// Get the namespace for this collection
	ns, err := getNamespace(c.name)
	if err != nil {
		return err
	}
	c.ns = ns

	// load reads per-collection config + index metadata. When invoked within a
	// write tx (CreateCollection) it MUST read through the writer's own view so
	// it observes config written earlier in this same uncommitted tx — a fresh
	// read tx would see only committed state and miss it (read-your-writes,
	// matching SQLite where a write tx's reads go through the shared page cache).
	load := func(tx *btree.ReadTx) (err error) {
		// Cache the catalog identity token (the coll: record value) — the
		// staleness pass compares it to detect that a peer replaced this
		// collection with a same-named one (see collectionVanished). The key
		// can be legitimately absent here: openCollection's existence check
		// ran in an earlier snapshot, and a peer may have dropped/renamed the
		// collection in between — surface the public error, not the raw
		// btree one, so open-or-create callers take their create path.
		if c.catalogID, err = tx.AppendValue(c.db.systemNS, collKey(c.name), nil); err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				return ErrCollectionNotFound
			}
			return err
		}
		// Load per-collection config
		cfg, err := c.db.loadCollConfig(tx, c.name)
		if err != nil {
			return err
		}
		c.compression = cfg.Compression
		c.primaryKey = cfg.PrimaryKey
		if c.primaryKey == "" {
			c.primaryKey = "id"
		}

		idxInfos, err := c.db.getIndexInfos(tx, c.name)
		if err != nil {
			return err
		}
		var idxs []*index
		var ftsIdxs []*ftsIndex
		var vidxs []*vectorIndex
		for _, info := range idxInfos {
			if isFulltext(info) {
				fx, fErr := newFtsIndex(c, info)
				if fErr != nil {
					return fErr
				}
				if fErr = fx.bindNamespaces(getNamespace); fErr != nil {
					return fErr
				}
				ftsIdxs = append(ftsIdxs, fx)
				continue
			}
			if info.Kind == IndexKindVector {
				vi, viErr := c.loadVectorIndex(tx, info)
				if viErr != nil {
					return viErr
				}
				vidxs = append(vidxs, vi)
				continue
			}
			nsName := indexNsName(c.name, info.Name)
			ns, nsErr := getNamespace(nsName)
			if nsErr != nil {
				return nsErr
			}
			idx, idxErr := newIndex(c, info, ns)
			if idxErr != nil {
				return idxErr
			}
			c.loadSketchAtOpen(tx, idx)
			idxs = append(idxs, idx)
		}
		c.storeIndexes(idxs)
		c.storeFtsIndexes(ftsIdxs)
		c.storeVectorIndexes(vidxs)
		return nil
	}

	if wtx != nil {
		return load(&wtx.ReadTx)
	}
	return c.db.doReadTx(ctx, load)
}

func (c *collection) Name() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.name
}

func (c *collection) PrimaryKey() string {
	c.mu.Lock()
	defer c.mu.Unlock()
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
		tx, txErr := c.db.btreeDB.BeginReadFast()
		if txErr != nil {
			return nil, txErr
		}
		defer func() {
			if rbErr := tx.Rollback(); rbErr != nil && err == nil {
				err = rbErr
			}
		}()
		buf.DocBuf, err = tx.AppendValue(c.ns, buf.SmallBuf, buf.DocBuf[:0])
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				return nil, ErrDocNotFound
			}
			return nil, err
		}
		data, pErr := p.Parse(buf.DocBuf)
		if pErr != nil {
			return nil, pErr
		}
		return item{val: data}, nil
	}

	err = c.db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		if err = c.alive(); err != nil {
			return err
		}
		buf.DocBuf, err = tx.AppendValue(c.ns, buf.SmallBuf, buf.DocBuf[:0])
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
		if txErr = c.alive(); txErr != nil {
			return txErr
		}
		var it item
		for _, doc := range docs {
			buf.Arena.Reset()
			if it, txErr = c.newItem(doc); txErr != nil {
				return txErr
			}
			if txErr = c.insertItem(tx, buf, it); txErr != nil {
				return txErr
			}
		}
		return nil
	})
	returned = true
	return
}

func (c *collection) insertItem(tx *btree.WriteTx, buf *syncpool.DocBuffer, it item) (err error) {
	buf.SmallBuf = c.appendId(buf.SmallBuf[:0], it.Value())
	if c.compressionDisabled() {
		buf.DocBuf = it.Value().MarshalTo(buf.DocBuf[:0])
	} else {
		buf.DocBuf, buf.ScratchBuf = it.Value().MarshalCompressed(buf.DocBuf[:0], buf.ScratchBuf)
	}

	// Check if key already exists
	if _, err := tx.Get(c.ns, buf.SmallBuf); err == nil {
		return ErrDocExists
	}

	if err = tx.Put(c.ns, buf.SmallBuf, buf.DocBuf); err != nil {
		return err
	}

	// Insert index entries
	for _, idx := range c.loadIndexes() {
		if err = idx.insertKeys(tx, it); err != nil {
			return err
		}
	}
	for _, fx := range c.loadFtsIndexes() {
		if err = fx.insertDoc(tx, it); err != nil {
			return err
		}
	}
	for _, vi := range c.loadVectorIndexes() {
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
		if txErr = c.alive(); txErr != nil {
			return false, txErr
		}
		return c.update(tx, it, item{})
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
		if txErr = c.alive(); txErr != nil {
			return false, txErr
		}
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
		newIt, vErr := c.newItem(newVal)
		if vErr != nil {
			return false, vErr
		}
		return c.update(tx, newIt, it)
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
		if txErr = c.alive(); txErr != nil {
			return false, txErr
		}
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
			txErr = c.insertItem(tx, buf2, newIt)
			return true, txErr
		} else {
			res.Matched = 1
			return c.update(tx, newIt, prevItem)
		}
	})
	returned = true
	if err != nil {
		return ModifyResult{}, err
	}
	return
}

func (c *collection) update(tx *btree.WriteTx, it, prevIt item) (modified bool, err error) {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	buf.SmallBuf = c.appendId(buf.SmallBuf[:0], it.Value())
	if prevIt.val == nil {
		prevIt, err = c.loadById(tx, buf, buf.SmallBuf)
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
	for _, idx := range c.loadIndexes() {
		if err = idx.deleteKeys(tx, prevIt); err != nil {
			return
		}
		if err = idx.insertKeys(tx, it); err != nil {
			return
		}
	}
	for _, fx := range c.loadFtsIndexes() {
		if err = fx.updateDoc(tx, prevIt, it); err != nil {
			return
		}
	}
	// Vector indexes: only touch the graph when the embedding actually changed.
	for _, vi := range c.loadVectorIndexes() {
		if err = vi.update(tx, prevIt, it); err != nil {
			return
		}
	}

	if c.compressionDisabled() {
		buf.DocBuf = it.Value().MarshalTo(buf.DocBuf[:0])
	} else {
		buf.DocBuf, buf.ScratchBuf = it.Value().MarshalCompressed(buf.DocBuf[:0], buf.ScratchBuf)
	}
	if err = tx.Put(c.ns, buf.SmallBuf, buf.DocBuf); err != nil {
		return
	}

	return true, nil
}

func (c *collection) loadById(tx *btree.WriteTx, buf *syncpool.DocBuffer, id anyenc.Tuple) (it item, err error) {
	buf.DocBuf, err = tx.AppendValue(c.ns, id, buf.DocBuf[:0])
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
		if txErr = c.alive(); txErr != nil {
			return false, txErr
		}
		insErr := c.insertItem(tx, buf, it)
		if errors.Is(insErr, ErrDocExists) {
			return c.update(tx, it, item{})
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
		if txErr = c.alive(); txErr != nil {
			return false, txErr
		}
		buf.SmallBuf = anyenc.AppendAnyValue(buf.SmallBuf[:0], id)
		// Verify document exists
		_, txErr = c.loadById(tx, buf, buf.SmallBuf)
		if txErr != nil {
			return
		}
		return true, c.deleteItem(tx, buf, buf.SmallBuf)
	})
	returned = true
	return err
}

func (c *collection) deleteItem(tx *btree.WriteTx, buf *syncpool.DocBuffer, id []byte) (err error) {
	// Delete index entries. The document is loaded once and reused for the range,
	// full-text and vector index removals (all need the field values).
	idxs := c.loadIndexes()
	ftsIdxs := c.loadFtsIndexes()
	vidxs := c.loadVectorIndexes()
	if len(idxs) > 0 || len(ftsIdxs) > 0 || len(vidxs) > 0 {
		it, loadErr := c.loadById(tx, buf, id)
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
	return tx.Delete(c.ns, id)
}

func (c *collection) Count(ctx context.Context) (count int, err error) {
	err = c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		if aErr := c.alive(); aErr != nil {
			return aErr
		}
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
	return c.db.doWriteTxModifiedW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (modified bool, txErr error) {
		if txErr = c.alive(); txErr != nil {
			return false, txErr
		}
		var newIndexes []*index
		var newFtsIndexes []*ftsIndex
		var newVIndexes []*vectorIndex
		for _, idxInfo := range info {
			if isFulltext(idxInfo) {
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
		c.registerIndexSetRestore(wtx)
		// Copy-on-write publish: build fresh slices (current snapshot + new
		// indexes) and swap them in atomically so lock-free query readers always
		// see a complete generation.
		if len(newIndexes) > 0 {
			cur := c.loadIndexes()
			merged := make([]*index, 0, len(cur)+len(newIndexes))
			merged = append(merged, cur...)
			merged = append(merged, newIndexes...)
			c.storeIndexes(merged)
		}
		if len(newFtsIndexes) > 0 {
			cur := c.loadFtsIndexes()
			merged := make([]*ftsIndex, 0, len(cur)+len(newFtsIndexes))
			merged = append(merged, cur...)
			merged = append(merged, newFtsIndexes...)
			c.storeFtsIndexes(merged)
		}
		if len(newVIndexes) > 0 {
			cur := c.loadVectorIndexes()
			merged := make([]*vectorIndex, 0, len(cur)+len(newVIndexes))
			merged = append(merged, cur...)
			merged = append(merged, newVIndexes...)
			c.storeVectorIndexes(merged)
		}
		return true, nil
	})
}

// registerIndexSetRestore snapshots the collection's three index sets and
// registers their restoration on rollback of the given tx scope (see
// commonTx.undo). A reverted DDL must not leave the in-memory sets pointing at
// state the on-disk catalog no longer has (a created index over freed
// namespace pages) or missing state it still has (a rolled-back drop whose
// index would silently stop being maintained). Call with c.mu held, BEFORE
// swapping; restoring an unchanged set is a harmless pointer store.
func (c *collection) registerIndexSetRestore(wtx WriteTx) {
	prevIndexes, prevFts, prevVec := c.loadIndexes(), c.loadFtsIndexes(), c.loadVectorIndexes()
	wtx.onRollbackUndo(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.storeIndexes(prevIndexes)
		c.storeFtsIndexes(prevFts)
		c.storeVectorIndexes(prevVec)
	})
}

func (c *collection) createIndex(ctx context.Context, tx *btree.WriteTx, info IndexInfo) (idx *index, err error) {
	tx.MarkSchemaChanged()
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
	skKey := sketchKey(c.name, info.Name)
	if err = tx.Put(c.db.systemNS, skKey, idx.sketch.MarshalBinary(nil)); err != nil {
		return nil, err
	}
	// Publish the live sketch as the reader snapshot before the index becomes
	// reader-visible (appended to c.indexes by the caller), so no reader ever
	// observes a nil sketchPub.
	idx.storePubSketch(idx.sketch)
	idx.sketchModified = false

	return idx, nil
}

// createFtsIndex creates the five namespaces of a full-text index, registers
// its metadata, and backfills existing documents. Mirrors createIndex.
func (c *collection) createFtsIndex(ctx context.Context, tx *btree.WriteTx, info IndexInfo) (*ftsIndex, error) {
	tx.MarkSchemaChanged()
	if info.Name == "" {
		info.Name = info.createName()
	}
	for _, field := range info.Fields {
		if err := validateIndexField(field); err != nil {
			return nil, err
		}
	}

	// Register in system namespace (carries Kind so reopen rebuilds an fts index).
	if err := c.db.registerIndex(tx, c.name, info); err != nil {
		return nil, err
	}

	fx, err := newFtsIndex(c, info)
	if err != nil {
		return nil, err
	}

	// Create the five namespaces.
	for _, nsName := range ftsIndexNames(c.name, info.Name) {
		if _, err = tx.CreateNamespace(nsName); err != nil {
			return nil, err
		}
	}
	if err = fx.bindNamespaces(tx.GetNamespace); err != nil {
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

	cursor := tx.NewCursor(c.ns)
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
		if txErr = c.alive(); txErr != nil {
			return txErr
		}
		tx.MarkSchemaChanged()
		// The write transaction began via checkStale, which reconciled the index
		// set against on-disk metadata. So the snapshot here reflects on-disk
		// truth: an index a peer created is present (drop must succeed), and one
		// a peer dropped is absent (return ErrIndexNotFound, not a false success).
		c.mu.Lock()
		defer c.mu.Unlock()
		c.registerIndexSetRestore(wtx)

		// Vector index drop: unregister, delete its namespaces, republish.
		for _, vi := range c.loadVectorIndexes() {
			if vi.info.Name != indexName {
				continue
			}
			if txErr = c.db.removeIndex(tx, c.name, indexName); txErr != nil {
				return
			}
			if txErr = dropVectorIndexNamespaces(tx, c.name, indexName); txErr != nil {
				return
			}
			cur := c.loadVectorIndexes()
			next := make([]*vectorIndex, 0, len(cur))
			for _, v := range cur {
				if v.info.Name != indexName {
					next = append(next, v)
				}
			}
			c.storeVectorIndexes(next)
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
		if txErr = c.db.removeIndex(tx, c.name, indexName); txErr != nil {
			return
		}

		if isFts {
			// Delete the five full-text namespaces.
			for _, nsName := range ftsIndexNames(c.name, indexName) {
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
			c.storeFtsIndexes(next)
			return nil
		}

		// Delete the index namespace
		nsName := indexNsName(c.name, indexName)
		if txErr = tx.DeleteNamespace(nsName); txErr != nil {
			if !errors.Is(txErr, btree.ErrNamespaceNotFound) {
				return
			}
			txErr = nil
		}
		// Delete sketch data
		skKey := sketchKey(c.name, indexName)
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
		c.storeIndexes(next)
		return nil
	})
}

func (c *collection) GetIndexes() (indexes []Index) {
	idxs := c.loadIndexes()
	ftsIdxs := c.loadFtsIndexes()
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
		c.mu.Lock()
		defer c.mu.Unlock()
		if err = c.alive(); err != nil {
			return err
		}

		oldName := c.name
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
			ns, nsErr := tx.GetNamespace(indexNsName(newName, idx.info.Name))
			if nsErr != nil {
				if errors.Is(nsErr, btree.ErrNamespaceNotFound) {
					// Legacy-broken index (pre-fix rename victim): the sweep
					// tolerated it; keep the old handle.
					next[i] = idx
					continue
				}
				return nsErr
			}
			next[i] = idx.cloneWithNs(ns)
		}
		c.storeIndexes(next)

		c.name = newName

		// The handle registry is re-keyed only at COMMIT (see commonTx.pubs):
		// re-keying here would open a window where a concurrent
		// OpenCollection(oldName) misses the map, passes the catalog check on
		// the still-committed old snapshot, and registers a duplicate live
		// handle the in-process staleness pass never invalidates (own commits
		// update localSchemaCookie). Until commit, OpenCollection(oldName)
		// keeps returning this handle — correct for the committed state — and
		// the staleness pass skips this collection while its registered name
		// and c.name disagree (see reconcileIndexSet).
		// Deliberately untouched: c.ns and the fts/vector-internal namespace
		// handles — root pages are unchanged and nothing derives keys from
		// their stale internal names; fts pending buffers flush at commit
		// through those handles into the renamed namespaces.
		wtx.onCommitPublish(func() {
			c.db.mu.Lock()
			if cur, ok := c.db.openedCollections[oldName]; ok && cur == Collection(c) {
				delete(c.db.openedCollections, oldName)
				// A Rename→Drop in the same tx closed and evicted the handle
				// already; don't resurrect it under the new name.
				if !c.closed.Load() {
					c.db.openedCollections[newName] = c
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
			c.mu.Lock()
			defer c.mu.Unlock()
			c.name = oldName
			for i, idx := range prevIdxs {
				if next[i] != idx && next[i].sketchModified {
					idx.sketchModified = true
				}
			}
			c.storeIndexes(prevIdxs)
		})
		return nil
	})
}

// Drop runs through doWriteTx — the variant that registers no undo — yet its
// callback calls c.close(), evicting the handle from db.openedCollections. A
// rollback (an error, or now a panic: see doWriteTxW) therefore restores the
// on-disk collection while this handle stays closed; callers re-obtain it via
// db.Collection, which heals the divergence because the map entry is gone.
// Pre-existing on the error path; tracked separately.
func (c *collection) Drop(ctx context.Context) error {
	return c.db.doWriteTx(ctx, func(tx *btree.WriteTx) (err error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if err = c.alive(); err != nil {
			return err
		}
		// Discard buffered fts writes before close: close() preserves a
		// non-empty pending buffer for the commit-time flush (BUG-01 fix),
		// but Drop deletes the fts namespaces in this same tx — flushing the
		// orphaned buffer at commit would write into dropped namespaces and
		// fail the whole transaction.
		for _, fx := range c.loadFtsIndexes() {
			fx.pending.reset()
		}
		if err = c.close(); err != nil {
			return err
		}
		// Delete all index namespaces. Enumerate indexes from the SAME on-disk
		// source (idx:<coll>: metadata keys) that removeCollection deletes,
		// rather than the in-memory index set which can lag the on-disk metadata
		// (a peer handle's create, or this handle's own create that committed but
		// has not yet been published to the in-memory snapshot). This keeps the
		// namespace-delete set and the metadata-delete set identical within the
		// single atomic Drop tx, so Drop can never leave an orphaned index
		// namespace. Enumerate BEFORE removeCollection deletes the idx: keys.
		idxInfos, err := c.db.getIndexInfos(&tx.ReadTx, c.name)
		if err != nil {
			return err
		}
		for _, info := range idxInfos {
			if info.Kind == IndexKindVector {
				if err = dropVectorIndexNamespaces(tx, c.name, info.Name); err != nil {
					return
				}
				continue
			}
			var nsNames []string
			if isFulltext(info) {
				nsNames = ftsIndexNames(c.name, info.Name)
			} else {
				nsNames = []string{indexNsName(c.name, info.Name)}
			}
			for _, nsName := range nsNames {
				if err = tx.DeleteNamespace(nsName); err != nil {
					if !errors.Is(err, btree.ErrNamespaceNotFound) {
						return
					}
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
	c.db.onCollectionClose(c)
	return nil
}

// alive rejects operations on a handle that is no longer live: explicitly
// closed, dropped, or invalidated because another process renamed or dropped
// the collection. Checked INSIDE the operation's transaction scope (not at
// entry) so it runs after the tx-begin staleness pass that performs the
// invalidation — an op that raced the invalidation still fails before
// touching the tree.
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

	cursor := tx.NewCursor(c.ns)
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
// index is not yet in c.indexes, so filling live in place and publishing it is
// unobservable to readers (no copy-on-write ceremony needed).
func (c *collection) loadSketchAtOpen(tx *btree.ReadTx, idx *index) {
	if idx.sketch == nil {
		idx.sketch = qplanner.NewIndexSketch(qplanner.DefaultSketchSize, len(idx.fieldPaths))
	}
	key := sketchKey(c.name, idx.info.Name)
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
//	  deltas, which keeps sequential cross-process counts exact for free — then
//	  republishes live so readers see the peer's state immediately.
//
//	READ tx (!writable): a concurrent in-process writer may be incrementing
//	  idx.sketch right now, so we NEVER touch it. We decode the disk bytes into a
//	  FRESH sketch and swap it into sketchPub (copy-on-write). The writer's live
//	  object is untouched; its in-flight increments can never be lost. This is the
//	  fix for the reader-clobbers-writer count loss.
func (c *collection) reloadSketch(tx *btree.ReadTx, idx *index, writable bool) {
	key := sketchKey(c.name, idx.info.Name)
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
	fresh := qplanner.NewIndexSketch(qplanner.DefaultSketchSize, len(idx.fieldPaths))
	fresh.UnmarshalBinary(data)
	idx.storePubSketch(fresh) // copy-on-write swap; live object untouched
}

// reconcileIndexes rebuilds the collection's index set from on-disk metadata
// after a peer committed DDL (detected via the schema cookie in checkStale). It
// is the any-store analog of SQLite's sqlite3InitOne: the authoritative source
// is the persisted metadata, and the in-memory set is brought into agreement
// with it — never the other way round.
//
// An existing in-memory *index is REUSED (preserving its loaded sketch) only
// when its persisted definition is byte-for-byte unchanged AND its namespace
// root page still matches disk. The root-page check is essential: a peer
// drop+recreate of an index reuses the same name-keyed metadata slot but
// allocates a FRESH btree root, so the cached *index (whose idx.ns/cboInfo.Ns
// still address the freed old root) must be discarded and rebuilt — otherwise a
// long-lived handle would read or write the wrong/old namespace and return
// wrong results or corrupt the recreated index.
//
// Indexes present in memory but absent on disk are dropped. Indexes present on
// disk but not yet in memory are built fresh. The new set is published
// copy-on-write so lock-free query readers see a complete generation.
//
// Errors resolving an individual index's namespace (e.g. a transient view where
// the metadata is visible but the namespace page is not) cause that index to be
// skipped for this reconcile rather than aborting the transaction; the next
// schema-stale tx will retry. The whole operation is best-effort and never
// surfaces an error to the caller — a failed reconcile must not break an
// otherwise valid read/write transaction.
func (c *collection) reconcileIndexes(tx *btree.ReadTx) {
	infos, err := c.db.getIndexInfos(tx, c.name)
	if err != nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cur := c.loadIndexes()
	byName := make(map[string]*index, len(cur))
	for _, idx := range cur {
		byName[idx.info.Name] = idx
	}

	// Reconcile vector indexes separately (different namespaces / type).
	c.reconcileVectorIndexesLocked(tx, infos)

	rangeInfos := infos[:0:0]
	for _, info := range infos {
		if info.Kind != IndexKindVector {
			rangeInfos = append(rangeInfos, info)
		}
	}
	infos = rangeInfos

	rebuilt := make([]*index, 0, len(infos))
	changed := len(infos) != len(cur)
	for _, info := range infos {
		nsName := indexNsName(c.name, info.Name)
		ns, nsErr := tx.GetNamespace(nsName)
		if nsErr != nil {
			// Namespace not resolvable in this snapshot: keep the existing
			// in-memory index if we have one (so we don't lose a working index
			// over a transient view), otherwise skip it this round.
			if existing, ok := byName[info.Name]; ok {
				rebuilt = append(rebuilt, existing)
			} else {
				changed = true
			}
			continue
		}
		if existing, ok := byName[info.Name]; ok &&
			indexInfoEqual(existing.info, info) &&
			existing.ns != nil && existing.ns.RootPage() == ns.RootPage() {
			// Unchanged: reuse the live object (and its loaded sketch).
			rebuilt = append(rebuilt, existing)
			continue
		}
		// Added or changed (definition differs, or root moved via
		// drop+recreate): build a fresh index bound to the current namespace.
		idx, idxErr := newIndex(c, info, ns)
		if idxErr != nil {
			// Malformed definition on disk: keep any existing object rather than
			// dropping a working index over a parse error.
			if existing, ok := byName[info.Name]; ok {
				rebuilt = append(rebuilt, existing)
			}
			continue
		}
		c.loadSketchAtOpen(tx, idx)
		rebuilt = append(rebuilt, idx)
		changed = true
	}

	if !changed {
		// Fast path: same set, same definitions, same roots — nothing to publish.
		return
	}
	c.storeIndexes(rebuilt)
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
			key := sketchKey(c.name, idx.info.Name)
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

// loadByIdRead loads a document by ID using a read transaction
func (c *collection) loadByIdRead(tx *btree.ReadTx, buf *syncpool.DocBuffer, id anyenc.Tuple) (it item, err error) {
	buf.DocBuf, err = tx.AppendValue(c.ns, id, buf.DocBuf[:0])
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
