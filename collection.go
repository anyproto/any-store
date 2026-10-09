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
//
// A handle is shared by every caller that opens the collection and serves
// transactions at different snapshots. Each operation works with the schema
// of the transaction it runs in: the indexes that transaction has, under the
// name it has. A transaction that does not have the collection — it began
// before the collection was created, or the collection was dropped or
// replaced since — gets ErrCollectionNotFound. A schema change made in a
// write transaction — a collection or index created or dropped, a rename —
// is that transaction's own until it commits: every other caller, and the
// accessors that take no context, have the committed schema.
type Collection interface {
	// Name returns the committed name of the collection: a rename made in an
	// open write transaction shows once it commits.
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

	// DropIndex drops an index by its name: ErrIndexNotFound for none of
	// that name, ErrIterOpen while an iterator of the transaction is open
	// on the collection.
	DropIndex(ctx context.Context, indexName string) (err error)

	// GetIndexes returns a list of indexes on the collection: range indexes
	// followed by full-text indexes (a full-text Len is the number of indexed
	// documents). Vector indexes are not listed yet. The list is the
	// committed one: an index created or dropped in an open write
	// transaction shows once it commits.
	GetIndexes() (indexes []Index)

	// CompactVectorIndex rebuilds the named vector index from its live vectors,
	// reclaiming nodes and storage left behind by deletes and replaces (which only
	// tombstone). It is synchronous and holds the write lock for the rebuild;
	// prefer a maintenance window for large indexes. No-op when nothing is
	// reclaimable or for a brute-force index. Returns ErrIndexNotFound if absent,
	// ErrIterOpen while an iterator of the transaction is open on the collection.
	CompactVectorIndex(ctx context.Context, indexName string) error

	// Stats returns the storage footprint of the collection: document count,
	// stored and uncompressed sizes, compression ratio and per-index sizes.
	// It scans the whole collection and is intended for diagnostics.
	Stats(ctx context.Context) (CollectionStats, error)

	// Rename renames the collection. In the renaming transaction the old
	// name is free from here on; the handle keeps working under the new
	// name, for everyone once the rename commits.
	Rename(ctx context.Context, newName string) (err error)

	// Drop drops the collection. Later operations through the handle fail
	// with ErrCollectionClosed: in the dropping transaction at once, for
	// everyone once the drop commits. A rollback leaves the handle as it
	// was. Refused with ErrIterOpen while an iterator of the transaction
	// is open on the collection.
	Drop(ctx context.Context) (err error)

	// ReadTx starts a new read-only transaction. It's just a proxy to db object.
	// Returns a ReadTx or an error if there is an issue starting the transaction.
	ReadTx(ctx context.Context) (ReadTx, error)

	// WriteTx starts a new read-write transaction. It's just a proxy to db object.
	// Returns a WriteTx or an error if there is an issue starting the transaction.
	WriteTx(ctx context.Context) (WriteTx, error)

	// Close releases the handle: later operations through it fail with
	// ErrCollectionClosed. A handle a write transaction changed the schema
	// through, or ran a query.Modifier of, is released once that change or
	// operation commits or rolls back — with the transaction, or with a
	// savepoint that encloses it — and stays open if the collection is
	// opened again before that.
	Close() error
}

// newCollection makes the handle of the collection name and loads its schema
// through tx, the view the caller opens it in: a read snapshot, or the open
// write transaction's own view (a collection created earlier in it). The
// caller registers the handle and installs the version, or keeps it for its
// transaction alone (see resolveSlow).
func newCollection(db *db, name string, tx *btree.ReadTx) (*collection, *collSchema, error) {
	c := &collection{db: db, openName: name}
	// The sketches are about to be read: as of the epoch before that.
	c.sketchSeen.Store(db.sketchEpoch.Load())
	s, err := c.loadSchema(tx, name, nil)
	if err != nil {
		return nil, nil, err
	}
	return c, s, nil
}

type collection struct {
	// head is the current committed schema version, nil while none is
	// installed: the newest state this process knows. The open write
	// transaction's own changes are in its log until its commit installs
	// them (txSchema). Which transactions it serves is resolve's business.
	// Read lock-free; replaced by compare-and-swap under c.mu, and by the
	// commit (txSchema.install).
	head atomic.Pointer[collSchema]
	db   *db

	compression Compression // 0 = use db default

	// openName is the name the handle was opened under: what the collection
	// is called while no version is installed.
	openName string

	// identity identifies the collection this handle stands for: the coll:
	// record value (the catalog token) followed by the data namespace's root
	// page, fixed at the first load (identify); empty until then. A view
	// that has another collection under the name — dropped and recreated,
	// or renamed away and created anew — does not have this one
	// (loadSchema). The token moves with a rename; the root is the second
	// check for the entries of old files that share one token. The registry
	// knows a collection by it, besides its name (db.byIdentity).
	identity string
	dataRoot uint32

	// since is the cookie the handle is proven from: the epoch's known at
	// the moment it entered the registry, or the cookie of the commit this
	// process was publishing then (db.sinceNow), or the one a created
	// collection commits with. A schema change of this process to the
	// collection went through an earlier handle if its cookie is at or
	// below since, and through this one otherwise — so a version loaded by
	// a transaction older than since may miss a change nothing here will
	// ever tell it of, and is never installed (resolveSlow). Set under db.mu
	// before the handle is reachable.
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

	// sketchSeen is the db.sketchEpoch the sketches of this handle were last
	// brought up to (refreshSketches).
	sketchSeen atomic.Uint64

	// pinned counts the entries of the open write transaction's schema log
	// that reference this handle (txSchema.log): its creation, each schema
	// change made through it, and the mark a verb leaves when it changes
	// the schema or runs a modifier (logPin).
	// While it is non-zero the handle stays the collection's one registered
	// handle. A second one — built from the writer's view or from the
	// committed one — would not follow the transaction's outcome: its
	// changes are installed on this handle at the commit, and a commit of
	// this process verifies no handle (see schemaEpoch).
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
	pinned int
	// logLast is one past the index of this handle's last entry in the open
	// write transaction's schema log (txSchema.log), 0 while it has none:
	// how the writer finds its version of the handle without a scan.
	// Writer-owned, like the log.
	logLast int

	// sketchDirty and ftsDirty record membership in db.sketchDirty and
	// db.ftsDirty; writer-owned like the lists.
	sketchDirty bool
	ftsDirty    bool

	// modifiers is the modifiers of this collection running right now
	// (runModifier). Read inside a write scope (writable), where the one
	// live write transaction of the db is the caller's: a count above zero
	// there is the caller's own modifier.
	modifiers atomic.Int32

	// closePending records a Close() that waits for pinned to reach zero
	// (db.unpinLocked). An open that hands this handle to a caller in between
	// clears it (db.handOut). Written under db.mu.
	closePending atomic.Bool

	closed atomic.Bool
	mu     sync.Mutex
}

// cur returns the head: the committed schema as this process last saw it;
// nil while no version is installed. The write path and the schema changes
// resolve the handle in their transaction instead.
func (c *collection) cur() *collSchema {
	return c.head.Load()
}

// answersFor reports that the handle stands for a live collection called
// name as far as this process has verified: a head of the current
// generation under that name. For the callers that have no transaction to
// resolve in; anything else is resolved through a short read.
func (c *collection) answersFor(name string) bool {
	if c.closed.Load() {
		return false
	}
	s := c.head.Load()
	return s != nil && s.gen.Load() == c.db.epoch.Load().gen && s.name == name
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
	if s := c.committed(); s != nil {
		return s.name
	}
	// Gone: the name it had.
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
	// Inserts drive IVF centroid drift (they create no HNSW tombstones, so this
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

	err = c.doWriteTx(ctx, func(tx *btree.WriteTx) (txErr error) {
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

	err = c.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
		s, txErr := c.resolve(&tx.ReadTx)
		if txErr != nil {
			return false, txErr
		}
		return c.update(tx, s, it, item{})
	})
	returned = true
	return
}

// runModifier runs a user modifier inside the write scope wtx as a callback
// of the call (callbackBegin): the calls it makes on the transaction nest
// in this one, and a write to this collection is refused (writable). The
// handle is pinned in the transaction's log first (logPin), as a schema
// change pins it: a Close() while the modifier runs then waits for the
// operation's scope to end — the transaction's commit or rollback, or the
// rollback of a savepoint enclosing the operation. Evicted at once, the
// handle would be replaced by an
// open by name with a second one that counts no modifier, and a write
// through that one — a verb, a $merge/$out sink — would pass the gate.
func (c *collection) runModifier(wtx WriteTx, mod query.Modifier, a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
	if err := c.logPin(wtx); err != nil {
		return nil, false, err
	}
	wtx.callbackBegin()
	defer wtx.callbackEnd()
	c.modifiers.Add(1)
	defer c.modifiers.Add(-1)
	return mod.Modify(a, v)
}

// writable is the error that refuses a write to, or a schema change of,
// the collection from inside one of its modifiers (ErrWriteInModifier):
// the operation running the modifier loaded the document and resolved the
// schema it writes with. Checked inside the write scope: a modifier runs
// in a write transaction, and the btree admits one at a time
// (btree.DB.BeginWrite, writeMu), so a modifier seen from inside a scope
// runs in the scope's own transaction. A write from another transaction
// waits for the writer lock instead; one made from inside the modifier
// with a context that carries no transaction waits for the lock its own
// transaction holds until the database closes, as such a write to any
// other collection does.
func (c *collection) writable() error {
	if c.modifiers.Load() > 0 {
		return ErrWriteInModifier
	}
	return nil
}

// droppable is the error that refuses a schema change which frees the
// collection's trees — Drop, DropIndex, CompactVectorIndex — while an
// iterator of the transaction is open on the collection (ErrIterOpen):
// its cursors stand on those trees, or are created on them at its next
// move — the planner opens its cursors at the first Next, the Doc
// fallback at the first fallback — and would read the freed pages, or
// what another tree of the transaction put there. Checked inside the
// write scope, with the version the change starts from, once the change
// has something to free: an index that does not exist, or a compaction
// with nothing to rebuild, reports that first. SQLite refuses the drop
// while another statement of the connection runs (OP_Destroy,
// SQLITE_LOCKED); see commonTx.iterOpenOn.
func (c *collection) droppable(wtx WriteTx, s *collSchema) error {
	if wtx.iterOpenOn(s.ns.RootPage()) {
		return ErrIterOpen
	}
	return nil
}

// doWriteTx, doWriteTxW, doWriteTxModified and doWriteTxModifiedW are the
// db's, for a write to this collection: refused, inside the scope, while
// a modifier of the collection runs (writable).
func (c *collection) doWriteTx(ctx context.Context, do func(tx *btree.WriteTx) error) error {
	return c.doWriteTxW(ctx, func(_ WriteTx, tx *btree.WriteTx) error {
		return do(tx)
	})
}

func (c *collection) doWriteTxW(ctx context.Context, do func(wtx WriteTx, tx *btree.WriteTx) error) error {
	return c.doWriteTxModifiedW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (bool, error) {
		return true, do(wtx, tx)
	})
}

func (c *collection) doWriteTxModified(ctx context.Context, do func(tx *btree.WriteTx) (bool, error)) error {
	return c.doWriteTxModifiedW(ctx, func(_ WriteTx, tx *btree.WriteTx) (bool, error) {
		return do(tx)
	})
}

func (c *collection) doWriteTxModifiedW(ctx context.Context, do func(wtx WriteTx, tx *btree.WriteTx) (bool, error)) error {
	return c.db.doWriteTxModifiedW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (bool, error) {
		if err := c.writable(); err != nil {
			return false, err
		}
		return do(wtx, tx)
	})
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

	err = c.doWriteTxModifiedW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (modified bool, txErr error) {
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
		newVal, modified, txErr := c.runModifier(wtx, mod, buf2.Arena, copyItem(buf2, it).val)
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

	err = c.doWriteTxModifiedW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (modified bool, txErr error) {
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
		newVal, modified, txErr := c.runModifier(wtx, mod, buf2.Arena, modValue)
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

	// Range indexes: each moves this document's entries to its new keys; one
	// whose keys the update leaves as they were is not written (updateKeys).
	// buf.SmallBuf is the primary key both versions share (checked above).
	for _, idx := range s.indexes {
		if err = idx.updateKeys(tx, prevIt, it, buf.SmallBuf); err != nil {
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

	err = c.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
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

	err = c.doWriteTxModified(ctx, func(tx *btree.WriteTx) (modified bool, txErr error) {
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
	return c.doWriteTxModifiedW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (modified bool, txErr error) {
		s, txErr := c.beginDDL(wtx)
		if txErr != nil {
			return false, txErr
		}
		var newIndexes []*index
		var newFtsIndexes []*ftsIndex
		var newVIndexes []*vectorIndex
		for _, idxInfo := range info {
			if isFulltext(idxInfo) {
				if txErr = c.checkSingleFulltextIndex(s, idxInfo, newFtsIndexes); txErr != nil {
					return false, txErr
				}
				fx, fErr := c.createFtsIndex(tx, s, idxInfo)
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
				vi, viErr := c.createVectorIndex(tx, s, idxInfo)
				if viErr != nil {
					if ensure && errors.Is(viErr, ErrIndexExists) {
						continue
					}
					return false, viErr
				}
				newVIndexes = append(newVIndexes, vi)
				continue
			}
			idx, txErr := c.createIndex(tx, s, idxInfo)
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

		// The new index objects' namespaces exist only in this tx's
		// uncommitted view: the version carrying them is the transaction's
		// own until its commit installs it (txSchema.log); a rollback
		// discards the objects with it.
		for _, vi := range newVIndexes {
			vi.bindIdentity(s.name)
		}
		next := s.clone()
		next.indexes = append(s.indexes[:len(s.indexes):len(s.indexes)], newIndexes...)
		next.ftsIndexes = append(s.ftsIndexes[:len(s.ftsIndexes):len(s.ftsIndexes)], newFtsIndexes...)
		next.vindexes = append(s.vindexes[:len(s.vindexes):len(s.vindexes)], newVIndexes...)
		c.logVersion(wtx, s, next)
		return true, nil
	})
}

func (c *collection) createIndex(tx *btree.WriteTx, s *collSchema, info IndexInfo) (idx *index, err error) {
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
	if err = c.db.registerIndex(tx, s.name, info); err != nil {
		return nil, err
	}
	return c.buildRangeIndex(tx, s, info)
}

// buildRangeIndex creates the index namespace and fills it from the
// collection's documents (s: the writer's version); the catalog record is
// already registered and stamped with the current format. Shared by
// createIndex and rebuildIndex.
func (c *collection) buildRangeIndex(tx *btree.WriteTx, s *collSchema, info IndexInfo) (idx *index, err error) {
	nsName := indexNsName(s.name, info.Name)
	ns, err := tx.CreateNamespace(nsName)
	if err != nil {
		return nil, err
	}

	idx, err = newIndex(c, s.name, info, ns)
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
	if err = c.buildIndex(tx, s, idx); err != nil {
		return nil, err
	}

	// Persist the sketch
	skKey := sketchKey(s.name, info.Name)
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
// not in s yet.
func (c *collection) checkSingleFulltextIndex(s *collSchema, info IndexInfo, pending []*ftsIndex) error {
	name := info.Name
	if name == "" {
		name = info.createName()
	}
	existing := ""
	for _, fx := range s.ftsIndexes {
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

func (c *collection) createFtsIndex(tx *btree.WriteTx, s *collSchema, info IndexInfo) (*ftsIndex, error) {
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
	if err := c.db.registerIndex(tx, s.name, info); err != nil {
		return nil, err
	}

	fx, err := newFtsIndex(c, s.name, info)
	if err != nil {
		return nil, err
	}

	// Create the five namespaces.
	for _, nsName := range ftsIndexNames(s.name, info.Name) {
		if _, err = tx.CreateNamespace(nsName); err != nil {
			return nil, err
		}
	}
	if err = fx.bindNamespaces(s.name, tx.GetNamespace); err != nil {
		return nil, err
	}

	// Backfill existing documents.
	if err = c.buildFtsIndex(tx, s, fx); err != nil {
		return nil, err
	}
	return fx, nil
}

// buildFtsIndex indexes every existing document into a new full-text index.
func (c *collection) buildFtsIndex(tx *btree.WriteTx, s *collSchema, fx *ftsIndex) error {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	cursor := tx.NewCursor(s.ns)
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
	return c.doWriteTxW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (txErr error) {
		// The version resolved for this write transaction reflects on-disk
		// truth: an index a peer created is present (drop must succeed),
		// and one a peer dropped is absent (return ErrIndexNotFound, not a
		// false success).
		s, txErr := c.beginDDL(wtx)
		if txErr != nil {
			return txErr
		}
		tx.MarkSchemaChanged()
		next := s.clone()

		// Vector index drop: unregister, delete its namespaces.
		for _, vi := range s.vindexes {
			if vi.info.Name != indexName {
				continue
			}
			if txErr = c.droppable(wtx, s); txErr != nil {
				return txErr
			}
			if txErr = c.db.removeIndex(tx, s.name, indexName); txErr != nil {
				return
			}
			if txErr = dropVectorIndexNamespaces(tx, s.name, indexName); txErr != nil {
				return
			}
			next.vindexes = make([]*vectorIndex, 0, len(s.vindexes))
			for _, v := range s.vindexes {
				if v.info.Name != indexName {
					next.vindexes = append(next.vindexes, v)
				}
			}
			c.logVersion(wtx, s, next)
			return nil
		}

		found := false
		isFts := false
		for _, idx := range s.indexes {
			if idx.info.Name == indexName {
				found = true
				break
			}
		}
		if !found {
			for _, fx := range s.ftsIndexes {
				if fx.info.Name == indexName {
					found, isFts = true, true
					break
				}
			}
		}
		if !found {
			return ErrIndexNotFound
		}
		if txErr = c.droppable(wtx, s); txErr != nil {
			return txErr
		}

		// removeIndex tolerates an already-absent metadata key: a concurrent peer
		// may have dropped the same index between our checkStale snapshot and
		// here, and a raw btree.ErrKeyNotFound must never escape DropIndex (whose
		// contract is nil or ErrIndexNotFound).
		if txErr = c.db.removeIndex(tx, s.name, indexName); txErr != nil {
			return
		}

		if isFts {
			// Delete the five full-text namespaces.
			for _, nsName := range ftsIndexNames(s.name, indexName) {
				if txErr = tx.DeleteNamespace(nsName); txErr != nil {
					if !errors.Is(txErr, btree.ErrNamespaceNotFound) {
						return
					}
					txErr = nil
				}
			}
			next.ftsIndexes = make([]*ftsIndex, 0, len(s.ftsIndexes))
			for _, fx := range s.ftsIndexes {
				if fx.info.Name != indexName {
					next.ftsIndexes = append(next.ftsIndexes, fx)
				}
			}
			c.logVersion(wtx, s, next)
			return nil
		}

		// Delete the index namespace
		nsName := indexNsName(s.name, indexName)
		if txErr = tx.DeleteNamespace(nsName); txErr != nil {
			if !errors.Is(txErr, btree.ErrNamespaceNotFound) {
				return
			}
			txErr = nil
		}
		// Delete sketch data
		skKey := sketchKey(s.name, indexName)
		_ = tx.Delete(c.db.systemNS, skKey) // ignore if not found
		// Delete the multikey flag (keyed by the immutable namespace name —
		// resolve it from the live index object, since nsName computed from
		// the CURRENT collection name is wrong after a rename): drop+recreate
		// is the flag's only reset path.
		next.indexes = make([]*index, 0, len(s.indexes))
		for _, idx := range s.indexes {
			if idx.info.Name == indexName {
				_ = tx.Delete(c.db.systemNS, multikeyKey(idx.ns.Name()))
				continue
			}
			next.indexes = append(next.indexes, idx)
		}
		c.logVersion(wtx, s, next)
		return nil
	})
}

// committed returns the head, brought up to date first if it is not known to
// be: none installed yet (a handle opened through a transaction too old to
// speak for it, see resolveSlow), or not verified since another process
// changed the schema. nil if the collection is gone. For the accessors that
// take no context: the schema as committed, as far as this process has
// looked. The verification takes a read transaction; when none can begin at
// once — every reader slot is held, maybe by the caller itself — the head
// stands as it is.
func (c *collection) committed() *collSchema {
	s := c.cur()
	if s != nil && s.gen.Load() == c.db.epoch.Load().gen {
		return s
	}
	tx, err := c.db.btreeDB.TryBeginRead()
	if err != nil {
		return s
	}
	defer func() { _ = tx.Rollback() }()
	c.db.checkStale(tx)
	v, err := c.resolve(tx)
	if err != nil {
		if errors.Is(err, ErrCollectionNotFound) {
			return nil
		}
		// The verification failed for a reason other than the collection
		// being gone: as last seen.
		return s
	}
	return v
}

// schemaFor returns the schema a verb about to run in ctx works with: the
// version of the transaction ctx carries, else the committed one its own
// transaction is about to see — verified through a short read when it is
// not known to be, with the error that ends the verb if that fails. The
// second result reports that the schema is verified: resolved in a
// transaction, as opposed to the cached head, which a peer process's DDL may
// have left behind (its cookie is observed only through a transaction).
func (c *collection) schemaFor(ctx context.Context) (*collSchema, bool, error) {
	tx, entered, err := c.db.enterCtxTx(ctx)
	if err != nil {
		return nil, false, err
	}
	if tx != nil {
		if entered != nil {
			defer entered.exit()
		}
		s, err := c.resolve(tx.btreeReadTx())
		return s, true, err
	}
	if s := c.cur(); s != nil && s.gen.Load() == c.db.epoch.Load().gen {
		return s, false, nil
	}
	s, err := c.committedSchema(ctx)
	return s, true, err
}

// committedSchema resolves the committed schema through a short read.
func (c *collection) committedSchema(ctx context.Context) (*collSchema, error) {
	var s *collSchema
	err := c.db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		s, err = c.resolve(tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return s, nil
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
	return c.doWriteTxW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (err error) {
		s, err := c.beginDDL(wtx)
		if err != nil {
			return err
		}
		oldName := s.name
		if newName == oldName {
			return nil
		}

		// Re-keys the catalog metadata AND every derived btree namespace
		// (data, per-index) in this tx; a collection under the new name in
		// this transaction's view is ErrCollectionExists.
		if err = c.db.renameCollection(tx, oldName, newName); err != nil {
			return err
		}

		// Re-bind range-index handles to their renamed namespaces. The old
		// handles keep working by root page, but their stale Name() would
		// feed multikeyKey: a post-rename fan-out write would then flag the
		// WRONG record, and a later rename back would clobber the real one —
		// re-enabling tight seeks over an index holding arrays (dropped
		// docs). Clones: readers access idx.ns lock-free through the
		// committed version.
		next := s.clone()
		next.indexes = make([]*index, len(s.indexes))
		for i, idx := range s.indexes {
			nsName := indexNsName(newName, idx.info.Name)
			ns, nsErr := tx.GetNamespace(nsName)
			if nsErr != nil {
				if errors.Is(nsErr, btree.ErrNamespaceNotFound) {
					// Legacy-broken index (pre-fix rename victim): the sweep
					// tolerated it; keep the old handle.
					next.indexes[i] = idx
					continue
				}
				return nsErr
			}
			next.indexes[i] = idx.cloneWithNs(ns, nsName, indexKey(newName, idx.info.Name))
		}
		// The data namespace handle is rebound with the rest: a version is
		// what one view has under one name. Deliberately untouched: the
		// fts/vector-internal namespace handles — root pages are unchanged
		// and nothing derives keys from their stale internal names; fts
		// pending buffers flush at commit through those handles into the
		// renamed namespaces.
		if next.ns, err = tx.GetNamespace(newName); err != nil {
			return err
		}
		next.name = newName
		next.names = s.renamed(newName, tx.DiskSchemaCookie()+1)
		// The registry is re-keyed at the commit (settleLog). Until then
		// OpenCollection(oldName) keeps returning this handle, which is right
		// for the committed state; inside this transaction the old name is
		// gone and the new one finds the handle through the log.
		c.logVersion(wtx, s, next)
		return nil
	})
}

// Drop deletes the collection in the write transaction. The handle keeps
// serving the committed state to every other transaction until the commit
// closes it (txSchema.install) and takes it out of the registry
// (settleLog); the dropping transaction's own later operations through it
// fail with ErrCollectionClosed. A rollback leaves the handle as it was.
// Refused while an iterator of the transaction is open on the collection
// (droppable).
func (c *collection) Drop(ctx context.Context) error {
	return c.doWriteTxW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (err error) {
		s, err := c.beginDDL(wtx)
		if err != nil {
			return err
		}
		if err = c.droppable(wtx, s); err != nil {
			return err
		}
		// Discard buffered fts writes: Drop deletes the fts namespaces in this
		// same tx — flushing a surviving buffer at commit would write into
		// dropped namespaces and fail the whole transaction.
		for _, fx := range s.ftsIndexes {
			fx.pending.reset()
		}
		// Delete all index namespaces. Enumerate indexes from the SAME on-disk
		// source (idx:<coll>: metadata keys) that removeCollection deletes,
		// rather than the in-memory index set which can lag the on-disk metadata
		// (a peer handle's create, or this handle's own create that committed but
		// has not yet been published to the in-memory snapshot). This keeps the
		// namespace-delete set and the metadata-delete set identical within the
		// single atomic Drop tx, so Drop can never leave an orphaned index
		// namespace. Enumerate BEFORE removeCollection deletes the idx: keys.
		idxInfos, err := c.db.getIndexInfos(&tx.ReadTx, s.name)
		if err != nil {
			return err
		}
		for _, info := range idxInfos {
			if info.Kind == IndexKindVector {
				if err = dropVectorIndexNamespaces(tx, s.name, info.Name); err != nil {
					return
				}
				continue
			}
			var nsNames []string
			if isFulltext(info) {
				nsNames = ftsIndexNames(s.name, info.Name)
			} else {
				nsNames = []string{indexNsName(s.name, info.Name)}
			}
			for _, nsName := range nsNames {
				if err = tx.DeleteNamespace(nsName); err != nil {
					if !errors.Is(err, btree.ErrNamespaceNotFound) {
						return
					}
				}
			}
		}
		if err = c.db.removeCollection(tx, s.name); err != nil {
			return
		}
		// Delete the collection namespace
		if err = tx.DeleteNamespace(s.name); err != nil {
			if !errors.Is(err, btree.ErrNamespaceNotFound) {
				return
			}
		}
		c.logDrop(wtx, s)
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
// While the open write transaction's schema log references the handle (see
// pinned) the close waits until that transaction commits or rolls back, and
// the handle stays usable until then — unless force: the database is
// closing and no transaction will end.
func (c *collection) close(force bool) error {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	if c.pinned > 0 && !force {
		c.closePending.Store(true)
		return nil
	}
	if c.closed.CompareAndSwap(false, true) {
		c.db.evictLocked(c)
	}
	return nil
}

// beginDDL returns the version a schema change of wtx starts from — the
// write transaction's own (resolve: its earlier changes through the handle,
// else the head brought to its view) — and pins the handle in the log until
// the transaction ends (logPin), so a Close() cannot take it out of the
// registry under the change. A verb that then changes nothing leaves the
// pin all the same; it only delays a Close().
func (c *collection) beginDDL(wtx WriteTx) (*collSchema, error) {
	s, err := c.resolve(wtx.btreeReadTx())
	if err != nil {
		return nil, err
	}
	if err = c.logPin(wtx); err != nil {
		return nil, err
	}
	return s, nil
}

// alive rejects operations on a handle that is no longer live: explicitly
// closed, dropped, or retired because another process renamed or dropped the
// collection (see retire). Operations check it INSIDE their transaction
// scope, through resolve, which is also what retires a handle.
func (c *collection) alive() error {
	if c.db.closed.Load() {
		// Every handle, the ones a closing write transaction created and
		// never registered included.
		return ErrDBIsClosed
	}
	if c.closed.Load() {
		return ErrCollectionClosed
	}
	return nil
}

// buildIndex populates index entries from all existing documents in the collection.
func (c *collection) buildIndex(tx *btree.WriteTx, s *collSchema, idx *index) error {
	buf := c.db.syncPool.GetDocBuf()
	defer c.db.syncPool.ReleaseDocBuf(buf)

	cursor := tx.NewCursor(s.ns)
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
// Called from the loader and createIndex — in both the index is not yet in a
// published set, so filling live in place and publishing it is unobservable
// to readers (no copy-on-write ceremony needed).
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

// refreshSketches reloads the sketches of the version s, resolved for tx, if
// another process committed since they were last loaded (db.sketchEpoch).
// Called where sketches are used: a reader's plan, and a writer's first
// operation on the collection, before its own deltas go on top. A reader at
// a snapshot older than the newest this process has consumed loads nothing:
// what it would publish is older than what is published.
func (c *collection) refreshSketches(tx *btree.ReadTx, s *collSchema) {
	epoch := c.db.sketchEpoch.Load()
	if c.sketchSeen.Load() == epoch {
		return
	}
	writable := tx.IsWriteTx()
	if fcc, _ := c.db.btreeDB.LocalCounters(); !writable && !cookieLE(fcc, tx.SnapshotFileChangeCounter()) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sketchSeen.Load() == epoch {
		return
	}
	for _, idx := range s.indexes {
		c.reloadSketch(tx, s.name, idx, writable)
	}
	c.sketchSeen.Store(epoch)
}

// reloadSketch refreshes one already-published index's sketch from the _system
// namespace (refreshSketches, and the rebase of resetUncommittedSketches).
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
func (c *collection) reloadSketch(tx *btree.ReadTx, collName string, idx *index, writable bool) {
	key := sketchKey(collName, idx.info.Name)
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
	// republishes live concurrently (settleSketches takes no c.mu) is
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

// persistSketches writes all modified live sketches to the _system namespace
// and lists them on the transaction for the settle that follows the commit
// (commonTx.settleSketches: the republish as the reader snapshot and the
// flag clear, once the commit is visible). Last-writer-wins (no
// cross-process merge — the sketch is advisory, like sqlite_stat1). Runs
// inside Commit before pager.commit, so the bytes are atomic with the
// file-change counter / schema cookie. s is the writer's version
// (collection.inTx).
func (c *collection) persistSketches(tx *btree.WriteTx, s *collSchema, t *commonTx) error {
	for _, idx := range s.indexes {
		if idx.sketchModified {
			key := sketchKey(s.name, idx.info.Name)
			idx.sketchBuf = idx.sketch.MarshalBinary(idx.sketchBuf)
			if err := tx.Put(c.db.systemNS, key, idx.sketchBuf); err != nil {
				return err
			}
			t.sketches = append(t.sketches, idx)
		}
	}
	return nil
}
