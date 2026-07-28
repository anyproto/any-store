package anystore

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/durability"
	"github.com/anyproto/any-store/v2/internal/durability/sentinel"
	"github.com/anyproto/any-store/v2/syncpool"
)

const systemNamespace = "_system"

// DB represents a document-oriented database.
type DB interface {
	// CreateCollection creates a new collection with the specified name.
	// Returns the created Collection or an error if the collection already exists.
	// Possible errors:
	// - ErrCollectionExists: if the collection already exists.
	CreateCollection(ctx context.Context, collectionName string, opts ...CollectionOptions) (Collection, error)

	// OpenCollection opens an existing collection with the specified name.
	// Returns the opened Collection or an error if the collection does not exist.
	// Possible errors:
	// - ErrCollectionNotFound: if the collection does not exist.
	OpenCollection(ctx context.Context, collectionName string) (Collection, error)

	// Collection is a convenience method to get or create a collection.
	// It first attempts to open the collection, and if it does not exist, it creates the collection.
	// Returns the Collection or an error if there is an issue creating or opening the collection.
	Collection(ctx context.Context, collectionName string, opts ...CollectionOptions) (Collection, error)

	// GetCollectionNames returns a list of all collection names in the database.
	// Returns a slice of collection names or an error if there is an issue retrieving the names.
	GetCollectionNames(ctx context.Context) ([]string, error)

	// Stats returns the statistics of the database.
	// Returns a DBStats struct containing the database statistics or an error if there is an issue retrieving the stats.
	Stats(ctx context.Context) (DBStats, error)

	// QuickCheck performs a quick integrity check. If result not ok returns error.
	QuickCheck(ctx context.Context) (err error)

	// IntegrityCheck runs the full structural btree integrity check:
	// reachable-page coverage, orphan detection, freelist consistency,
	// overflow-chain validation, key ordering, and master-page consistency.
	// Returns nil if the database is structurally consistent, or an error
	// aggregating up to 100 issues found. More expensive than QuickCheck —
	// intended for stress tests and offline diagnostics, not normal opens.
	IntegrityCheck(ctx context.Context) (err error)

	// Flush perform checkpoint on the btree database
	// When waitIdleDuration > 0, wait for waitIdleTime since the last write tx got released
	Flush(ctx context.Context, waitIdleDuration time.Duration, mode FlushMode) error

	// Backup creates a backup of the database at the specified file path.
	// Returns an error if the operation fails.
	Backup(ctx context.Context, path string) (err error)

	// ReadTx starts a new read-only transaction.
	// Returns a ReadTx or an error if there is an issue starting the transaction.
	ReadTx(ctx context.Context) (ReadTx, error)

	// WriteTx starts a new read-write transaction.
	// Returns a WriteTx or an error if there is an issue starting the transaction.
	WriteTx(ctx context.Context) (WriteTx, error)

	// Close closes the database connection.
	// Returns an error if there is an issue closing the connection.
	Close() error

	// IntegrityMode reports the page-level integrity mode of this database
	// (none / checksum / AEAD).
	IntegrityMode() IntegrityMode

	// VerifyIntegrity walks every page and verifies its per-page integrity
	// tag (XXH3-128 trailer for cksum mode, AEAD auth tag for encrypted mode).
	// Plain DBs return IntegrityNone with zero pages scanned. Mismatches are
	// returned in IntegrityReport.Errors; the function only errors on I/O or
	// context cancellation. See IntegrityConfig.
	VerifyIntegrity(ctx context.Context) (IntegrityReport, error)
}

// DBStats represents the statistics of the database.
type DBStats struct {
	// CollectionsCount is the total number of collections in the database.
	CollectionsCount int

	// IndexesCount is the total number of indexes across all collections in the database.
	IndexesCount int

	// TotalSizeBytes is the total size of the database in bytes.
	TotalSizeBytes int

	// DataSizeBytes is the total size of the data stored in the database in bytes, excluding free space.
	DataSizeBytes int

	DirtyOnOpen             bool          // indicates we have sentinel file on open
	DirtyQuickCheckDuration time.Duration // time spent in quickcheck if dirty
}

// Open opens a database at the specified path with the given configuration.
// The config parameter can be nil for default settings.
// Returns a DB instance or an error.
func Open(ctx context.Context, path string, config *Config) (DB, error) {
	if config == nil {
		config = &Config{}
	}
	config.setDefaults()

	sPool := syncpool.NewSyncPool(config.SyncPoolElementMaxSize)

	ds := &db{
		instanceId:        anyenc.NewObjectID().Hex(),
		config:            config,
		syncPool:          sPool,
		openedCollections: make(map[string]Collection),
	}

	var quickCheckNeeded bool
	ds.recoveryController, quickCheckNeeded = ds.createRecoveryController(ctx, path)

	cacheSize := config.CacheSize
	if cacheSize <= 0 {
		cacheSize = 5000
	}
	// Concurrent read transactions are bounded by a semaphore (each live reader
	// holds its own page cache). The default scales with the host — max(NumCPU-1,
	// 4) — so concurrent $text/Find reads aren't artificially serialized on a
	// multi-core machine, while staying modest on small devices. Reader caches are
	// created lazily up to this cap, so RAM follows actual concurrency, not the
	// cap. Override via Config.ReadConcurrency.
	readConcurrency := config.ReadConcurrency
	if readConcurrency <= 0 {
		readConcurrency = max(runtime.NumCPU()-1, 4)
	}
	// Page-level integrity is on by default for non-encrypted databases.
	// XXH3-128 trailer per page costs <1% on writes and is invisible on
	// reads. Encrypted databases get stronger integrity from the
	// cipher's AEAD tag, so the cksum codec is skipped there. File-state
	// is authoritative on reopen — existing plain DBs stay plain
	// regardless of this default.
	opts := btree.Options{
		PageSize:              4096,
		CacheSize:             cacheSize,
		MaxReaders:            readConcurrency,
		InProcess:             false,
		NoCommitSync:          !config.CommitSync,
		InMemory:              config.InMemory,
		DisableAutoCheckpoint: config.DisableAutoCheckpoint,
		AutoCheckpointAfter:   config.AutoCheckpointAfter,
		UsePageSlab:           config.UseGlobalPageBuffer,
		Key:                   config.Encryption.Passphrase,
		KDFIterations:         config.Encryption.KDFIterations,
		CipherType:            config.Encryption.CipherType,
		Codec:                 config.Encryption.Codec,
		MmapSize:              config.MmapSize,
		Checksum:              !config.Encryption.Enabled() && !config.InMemory,
	}
	if cb := config.OnIntegrityError; cb != nil {
		// Determine the kind discriminator at Open time. The actual codec
		// won't have been installed yet, so we compute mode from config:
		// encrypted → AEAD, otherwise → checksum (the cksum codec is the
		// only non-AEAD codec we install). InMemory has no on-disk codec.
		kind := IntegrityChecksumMismatch
		if config.Encryption.Enabled() {
			kind = IntegrityAEADAuthFail
		}
		opts.OnIntegrityError = func(pgno uint32, inner error) {
			cb(IntegrityError{PageNo: pgno, Kind: kind, Inner: inner})
		}
	}

	var err error
	if ds.btreeDB, err = btree.Open(path, opts); err != nil {
		if errors.Is(err, btree.ErrPageSlabNotInitialized) {
			return nil, ErrPageBufferNotInitialized
		}
		return nil, err
	}
	// ContinueOnIntegrityError only applies to checksum mode. AEAD mode
	// ignores it by design (disabling AEAD verification would return
	// attacker-controlled plaintext); plain mode has nothing to verify.
	if config.ContinueOnIntegrityError {
		if c := ds.btreeDB.CksumCodec(); c != nil {
			c.SetVerify(false)
		}
	}

	if err = ds.init(ctx); err != nil {
		_ = ds.recoveryController.Stop()
		_ = ds.btreeDB.Close()
		return nil, err
	}

	// Run QuickCheck if database was dirty
	if quickCheckNeeded {
		ds.dirtyOnOpen = true
		start := time.Now()
		quickCheckCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		if err := ds.QuickCheck(quickCheckCtx); err != nil {
			if ds.recoveryController != nil {
				_ = ds.recoveryController.Stop()
			}
			_ = ds.btreeDB.Close()
			return nil, fmt.Errorf("%w: %w", ErrQuickCheckFailed, err)
		}
		ds.dirtyQuickCheckDuration = time.Since(start)
		if ds.recoveryController != nil {
			ds.recoveryController.MarkCleanAfterCheck()
		}
	}

	// Start recovery controller after initialization
	if ds.recoveryController != nil {
		if err = ds.recoveryController.Start(ctx); err != nil {
			_ = ds.btreeDB.Close()
			return nil, err
		}
	}

	return ds, nil
}

type db struct {
	instanceId string

	config *Config

	btreeDB            *btree.DB
	systemNS           *btree.Namespace
	recoveryController *durability.Controller

	syncPool *syncpool.SyncPool

	// ddlUnwindGate serializes the failed-COMMIT undo unwind against new write
	// txs. btree WriteTx.Commit releases the global write lock before
	// returning its error (and a failed pager commit self-recovers to
	// pagerOpen), so without this gate a writer beginning in that gap could
	// still observe the phantom schema publications the unwind is about to
	// revert — the I-11 corruption class. A tx that registered DDL undos holds
	// the gate across btree Commit + unwind; newWriteTx passes through it once
	// after acquiring the write lock. Plain data txs (no undos) skip it.
	// The rollback and savepoint paths need no gate: their unwind runs while
	// the btree write lock is still held.
	ddlUnwindGate sync.Mutex

	openedCollections map[string]Collection
	// orphanFtsPending holds fts indexes of collections closed mid-write-tx
	// with a non-empty pending buffer. flushAllFtsPending / resetAllFtsPending
	// enumerate openedCollections, so without this registry a Close() between
	// Insert and Commit would silently drop the buffered postings — the doc
	// commits but stays invisible to $text forever (guarded by
	// TestFtsPendingSurvivesCollectionCloseMidTx).
	// Guarded by db.mu; drained by the next flush or reset.
	orphanFtsPending []*ftsIndex
	closed           atomic.Bool

	dirtyOnOpen             bool
	dirtyQuickCheckDuration time.Duration
	mu                      sync.Mutex
}

func collKey(name string) []byte {
	return []byte("coll:" + name)
}

// newCatalogID returns a fresh identity token stored as the coll: record
// value and cached on the handle at init. It distinguishes a recreated
// same-named collection from the one a cached handle was opened against —
// the data-namespace root page alone is unreliable for that (an immediate
// drop+recreate typically gets the same root back from the freelist).
// renameCollection moves the value verbatim, so identity survives a rename.
// Legacy files store "1" for every collection; any post-fix recreate writes a
// fresh token, so the comparison still detects recreation of legacy entries.
func newCatalogID() []byte {
	id := make([]byte, 8)
	_, _ = rand.Read(id) // crypto/rand never fails on supported platforms
	return id
}

func collConfigKey(name string) []byte {
	return []byte("collcfg:" + name)
}

type collConfig struct {
	Compression Compression
	PrimaryKey  string
}

func indexKey(collName, indexName string) []byte {
	return []byte("idx:" + collName + ":" + indexName)
}

func sketchKey(collName, indexName string) []byte {
	return []byte("stat_data:" + collName + ":" + indexName)
}

// multikeyKey is the system-namespace key of an index's sticky multikey flag.
// Unlike the advisory sketch, this record is ANSWER-DETERMINING: it gates
// whether the planner may seek with tight (intersected) bounds, which silently
// drops docs if the index actually holds fan-out entries. It therefore lives
// in its own record (never inside the sketch blob), is written transactionally
// with the entries it describes, and defaults to "assume multikey" when
// absent (files created before the flag existed).
//
// Keyed by the index NAMESPACE name, unique among live namespaces.
// renameCollection re-keys this record in the SAME tx that renames the
// namespace itself, so key and namespace can never diverge — a rollback
// reverts both together, and because the re-key moves (delete+put) rather
// than copies, a rename cycle can never resurrect a stale record.
// createIndex overwrites the record for the namespace it just created, so
// even an orphan left by an incomplete cleanup can never be adopted by a new
// index.
func multikeyKey(nsName string) []byte {
	return []byte("idx_mk:" + nsName)
}

// multikeyKey record values. One byte: scalar-so-far (written at index
// creation, before backfill) or multikey (flipped by the first fan-out write,
// one-way — deletes never clear it because older snapshots may still hold the
// fanned-out entries; drop+recreate is the reset).
var (
	mkValScalar   = []byte{1}
	mkValMultiKey = []byte{2}
)

func indexKeyPrefix(collName string) string {
	return "idx:" + collName + ":"
}

// validateCollectionName rejects names that collide with the system namespace
// or a reserved namespace family — the collection's data namespace IS the raw
// name, so a name like "ix:x:y" could alias a derived index namespace.
//
// ":" inside a name is allowed: every catalog key family is prefixed
// ("coll:", "collcfg:", "idx:", "stat_data:", "idx_mk:") so families can't
// cross-collide, and the residual within-family ambiguity (collection "A:b"
// index "c" vs collection "A" index "b:c" both deriving ix:A:b:c) fails loudly
// with ErrNamespaceExists at index-create or rename time — it can never
// corrupt silently. Rejecting ":" would also strand pre-validation files whose
// collection names legally contain it.
// maxNameLen caps collection and index names. Derived namespace names embed
// both plus a family prefix/suffix (widest: fts "ftx:"+coll+":"+index+":vocab",
// +11 bytes), and a master-table cell must keep its 4-byte root value within
// maxLocalPayload(4096) = 1002, i.e. len(coll)+len(index) <= 987. 255 each
// leaves ample margin and matches common identifier limits.
const maxNameLen = 255

func validateCollectionName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: name must not be empty", ErrInvalidCollectionName)
	}
	if len(name) > maxNameLen {
		return fmt.Errorf("%w: name exceeds %d bytes", ErrInvalidCollectionName, maxNameLen)
	}
	if name == systemNamespace {
		return fmt.Errorf("%w: %q is reserved", ErrInvalidCollectionName, name)
	}
	for _, prefix := range []string{"ix:", "ftx:", "vix:"} {
		if strings.HasPrefix(name, prefix) {
			return fmt.Errorf("%w: prefix %q is reserved for index namespaces", ErrInvalidCollectionName, prefix)
		}
	}
	return nil
}

// validateIndexName caps the index name AFTER createName() defaulting — a long
// field list can synthesize a name past the cap just as easily as a caller can
// pass one.
func validateIndexName(name string) error {
	if len(name) > maxNameLen {
		return fmt.Errorf("%w: name exceeds %d bytes", ErrInvalidIndexName, maxNameLen)
	}
	return nil
}

func (db *db) init(ctx context.Context) error {
	return db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		// Ensure system namespace exists
		ns, err := tx.GetNamespace(systemNamespace)
		if err != nil {
			if errors.Is(err, btree.ErrNamespaceNotFound) {
				ns, err = tx.CreateNamespace(systemNamespace)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		db.systemNS = ns
		return nil
	})
}

func (db *db) newWriteTx(ctx context.Context) (WriteTx, error) {
	btWtx, err := db.btreeDB.BeginWrite()
	if err != nil {
		return nil, err
	}

	// Pass the DDL-unwind gate (see its field comment): a prior tx whose btree
	// commit failed may still be unwinding its schema publications after the
	// write lock was released; block until its in-memory state is consistent.
	db.ddlUnwindGate.Lock()
	db.ddlUnwindGate.Unlock() //lint:ignore SA2001 gate pass-through, not a critical section

	db.checkStale(&btWtx.ReadTx)
	db.resetUncommittedSketches(&btWtx.ReadTx)
	db.resetAllFtsPending()

	version := newTxVersion()
	tx := txPool.Get().(*commonTx)
	tx.db = db
	tx.readTx = &btWtx.ReadTx
	tx.writeTx = btWtx
	tx.modified = false
	tx.undo = tx.undo[:0]
	tx.pubs = tx.pubs[:0]
	tx.version.Store(version)
	wTx := writeTx{commonTx: tx, version: version}
	tx.ctx = context.WithValue(ctx, ctxKeyTx, wTx)
	return wTx, nil
}

func (db *db) ReadTx(ctx context.Context) (ReadTx, error) {
	btRtx, err := db.btreeDB.BeginRead()
	if err != nil {
		return nil, err
	}

	db.checkStale(btRtx)

	version := newTxVersion()
	tx := txPool.Get().(*commonTx)
	tx.db = db
	tx.readTx = btRtx
	tx.writeTx = nil
	tx.version.Store(version)
	rTx := readTx{commonTx: tx, version: version}
	tx.ctx = context.WithValue(ctx, ctxKeyTx, rTx)
	return rTx, nil
}

// checkStale checks if the on-disk data or schema has changed (by another
// process) and reloads in-memory caches if necessary. It runs at the start of
// every top-level read and write transaction (the analog of SQLite verifying
// the schema cookie in OP_Transaction before a statement runs).
//
// The reaction is two-tiered, mirroring SQLite's sqlite3InitOne (structure)
// running before sqlite3AnalysisLoad (statistics):
//
//	Tier 1 — STRUCTURAL (correctness): when the SCHEMA cookie advanced, a peer
//	  committed DDL (index create/drop/recreate). reconcileIndexSet rebuilds
//	  each open collection's index set from on-disk metadata: it adds
//	  peer-created indexes, drops peer-removed ones, and re-resolves the btree
//	  namespace handle (root) of any index whose definition or root changed —
//	  so a long-lived handle can never keep reading/writing a dropped or
//	  recreated index's stale namespace. A stale schema is never tolerated.
//
//	Tier 2 — STATISTICAL (advisory): reloadSketches refreshes the selectivity
//	  sketches over the (now reconciled) index set. A stale sketch only affects
//	  which index the planner CHOOSES, never query RESULTS (the any-store analog
//	  of sqlite_stat1), so it runs strictly after the structural reconcile.
// The begin-time disk counters driving the verdict are RAISED to the newest
// committed frame (see btree.ReadTx.SnapshotHeaderCounters), so a read tx can
// detect staleness its own snapshot does not yet contain — a begin racing a
// commit, or a reader slot pinned behind. The pass therefore reconciles and
// consumes only up to the SNAPSHOT counters: reconciling judges what this
// snapshot can see, and recording anything newer as consumed would mark DDL
// as reconciled that never was — later txs (including writers) would trust an
// index set missing a peer's index and stop maintaining it. Left short, the
// verdict stays stale and the next begin (with a newer snapshot) converges.
func (db *db) checkStale(tx *btree.ReadTx) {
	if !tx.IsSchemaStale() && !tx.IsDataStale() {
		return
	}
	snapFCC, snapSC := tx.DiskFileChangeCounter(), tx.DiskSchemaCookie()
	if !tx.IsWriteTx() {
		var err error
		if snapFCC, snapSC, err = tx.SnapshotHeaderCounters(); err != nil {
			// Cannot bound the snapshot: reconcile nothing, consume nothing —
			// the verdict stays stale and the next begin retries.
			return
		}
	}
	if tx.IsSchemaStale() {
		db.reconcileIndexSet(tx, snapSC)
	}
	db.reloadSketches(tx)
	db.btreeDB.UpdateLocalCounters(snapFCC, snapSC)
}

// reconcileIndexSet rebuilds the in-memory index set of every open collection
// from on-disk metadata, called from checkStale when the schema cookie advanced.
// See checkStale for the contract. A handle whose collection no longer exists
// in the snapshot (renamed away or dropped by another process) is invalidated
// instead of reconciled — reconciling it against an empty index set would
// publish exactly the unindexed-write corruption of I-16. Each surviving
// collection is reconciled under its own c.mu and the result published
// atomically (copy-on-write), so lock-free query readers always observe a
// complete index generation.
func (db *db) reconcileIndexSet(tx *btree.ReadTx, snapCookie uint32) {
	type namedColl struct {
		name string
		c    *collection
	}
	db.mu.Lock()
	colls := make([]namedColl, 0, len(db.openedCollections))
	for name, coll := range db.openedCollections {
		colls = append(colls, namedColl{name: name, c: coll.(*collection)})
	}
	db.mu.Unlock()

	for _, nc := range colls {
		nc.c.mu.Lock()
		renameInFlight := nc.c.name != nc.name
		nc.c.mu.Unlock()
		if renameInFlight {
			// A local Rename is between its name flip and its commit (the
			// registry re-keys only at commit). Skip: the renaming writer
			// holds the cross-process write lock, so the cookie bump this
			// pass consumes predates its begin and was reconciled there;
			// checking c.name against this tx's older snapshot would
			// spuriously invalidate the handle, and reconciling its index
			// set under the flipped name would publish an empty one.
			continue
		}
		if !tx.IsWriteTx() && snapCookie < nc.c.validFromCookie.Load() {
			// The snapshot predates the handle's visibility bound: a create
			// or rename committed after — or is still committing while —
			// this reader began, so its catalog key is legitimately absent
			// here. Judging existence needs a snapshot at or past the bound
			// (same tolerance rule as index.forTx); skip. snapCookie is the
			// SNAPSHOT cookie, not the raised begin-time one — the raised
			// value can exceed what this tx's reads see and would let the
			// vanished check judge a handle whose catalog key sits in frames
			// past the snapshot. A write tx always sees latest state and
			// needs no skip.
			continue
		}
		if db.collectionVanished(tx, nc.name, nc.c) {
			db.invalidateCollection(nc.c)
			continue
		}
		nc.c.reconcileIndexes(tx)
	}
}

// collectionVanished reports whether the collection this handle points at no
// longer exists in the tx snapshot: its catalog key is gone (renamed away or
// dropped by a peer process), or a DIFFERENT collection now answers for the
// name (drop+recreate, or rename-away followed by a fresh create). The name is
// the handle's REGISTERED key in openedCollections, which tracks committed
// state (a local rename re-keys it only at commit) and therefore matches the
// snapshot this tx reads — c.name may already be flipped by an in-flight
// rename. Identity is the catalog token (see newCatalogID) plus the
// data-namespace root page as a belt for legacy "1"-valued entries — the root
// alone is unreliable because an immediate drop+recreate usually gets the same
// root back from the freelist. Errors other than definite absence keep the
// handle: invalidation must never fire on a read hiccup.
func (db *db) collectionVanished(tx *btree.ReadTx, name string, c *collection) bool {
	val, err := tx.AppendValue(db.systemNS, collKey(name), nil)
	if err != nil {
		return errors.Is(err, btree.ErrKeyNotFound)
	}
	if !bytes.Equal(val, c.catalogID) {
		return true
	}
	ns, err := tx.GetNamespace(name)
	if err != nil {
		return errors.Is(err, btree.ErrNamespaceNotFound)
	}
	return ns.RootPage() != c.ns.RootPage()
}

// invalidateCollection retires a handle whose collection a peer process
// renamed or dropped (SQLite re-prepare style: subsequent operations fail with
// ErrCollectionClosed and the caller re-opens). Modeled on the CreateCollection
// rollback undo, NOT on close(): onCollectionClose would orphan non-empty fts
// pending buffers for the commit-time flush, and a dead handle's buffered
// writes must never flush into the renamed/dropped collection's namespaces —
// reset them instead. The CAS makes concurrent invalidations (multiple tx
// begins observing the same cookie bump) idempotent.
func (db *db) invalidateCollection(c *collection) {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	for _, fx := range c.loadFtsIndexes() {
		fx.pending.reset()
	}
	db.mu.Lock()
	for name, cur := range db.openedCollections {
		if cur == Collection(c) {
			delete(db.openedCollections, name)
			break
		}
	}
	db.mu.Unlock()
}

// reloadSketches reloads all sketch data from the _system namespace for opened
// collections (the advisory Tier-2 of checkStale). The per-index leaf branches
// on whether this tx is the writer: a write tx (sole mutator under writeMu)
// reloads in place into the live sketch; a read tx swaps a fresh copy-on-write
// snapshot so it can never clobber a concurrent writer's in-flight increments.
func (db *db) reloadSketches(tx *btree.ReadTx) {
	writable := tx.IsWriteTx()
	db.mu.Lock()
	colls := make([]*collection, 0, len(db.openedCollections))
	for _, coll := range db.openedCollections {
		colls = append(colls, coll.(*collection))
	}
	db.mu.Unlock()

	for _, c := range colls {
		c.mu.Lock()
		for _, idx := range c.loadIndexes() {
			c.reloadSketch(tx, idx, writable)
		}
		c.mu.Unlock()
	}
}

// resetUncommittedSketches discards leftover, never-committed sketch deltas at
// write-tx begin. insertKeys/deleteKeys mutate the live sketch in place and set
// sketchModified; a committed tx clears that flag via persistSketches, but a
// ROLLED-BACK tx does not — so a still-set sketchModified at the start of a new
// write tx means a prior tx incremented the sketch and then rolled back. Left
// alone, those phantom deltas would accumulate across rolled-back txs (and be
// persisted on the next commit), drifting the planner's cardinality estimate
// (advisory only — never query results). Here we rebase any such index's live
// sketch to the last committed on-disk state before the new tx applies its own
// deltas — the in-methodology analog of resetting to the committed snapshot at
// tx begin, with zero cost on the all-commit happy path (sketchModified is
// false there, so the reload is skipped). Write-tx only: the caller holds the
// btree write lock, so the live sketch has a single mutator.
func (db *db) resetUncommittedSketches(tx *btree.ReadTx) {
	db.mu.Lock()
	colls := make([]*collection, 0, len(db.openedCollections))
	for _, coll := range db.openedCollections {
		colls = append(colls, coll.(*collection))
	}
	db.mu.Unlock()

	for _, c := range colls {
		c.mu.Lock()
		for _, idx := range c.loadIndexes() {
			if idx.sketchModified {
				// reloadSketch (writable) rebases live to the committed bytes and
				// clears sketchModified; if there are no committed bytes yet
				// (brand-new pre-commit index) it preserves the built sketch.
				c.reloadSketch(tx, idx, true)
			}
		}
		c.mu.Unlock()
	}
}

func mergeCollOpts(opts []CollectionOptions) CollectionOptions {
	var merged CollectionOptions
	for _, o := range opts {
		if o.Compression != 0 {
			merged.Compression = o.Compression
		}
		if o.PrimaryKey != "" {
			merged.PrimaryKey = o.PrimaryKey
		}
	}
	return merged
}

func (db *db) CreateCollection(ctx context.Context, collectionName string, opts ...CollectionOptions) (Collection, error) {
	if err := validateCollectionName(collectionName); err != nil {
		return nil, err
	}
	db.mu.Lock()
	if existing, ok := db.openedCollections[collectionName]; ok && !existing.(*collection).closed.Load() {
		db.mu.Unlock()
		return nil, ErrCollectionExists
	}
	// A CLOSED registered handle is a Drop in an open tx (eviction is
	// deferred to its commit publication): whether the name is creatable is
	// decided by the catalog check inside the tx below — the dropping tx
	// sees its own delete and recreates; a concurrent creator blocks on the
	// write lock and then sees whatever committed.
	db.mu.Unlock()
	merged := mergeCollOpts(opts)
	pk := merged.PrimaryKey
	if pk == "" {
		pk = "id"
	}
	if err := validatePrimaryKey(pk); err != nil {
		return nil, err
	}
	var coll Collection
	err := db.doWriteTxW(ctx, func(wtx WriteTx, tx *btree.WriteTx) error {
		tx.MarkSchemaChanged()

		// Check if collection already exists in system namespace
		key := collKey(collectionName)
		_, err := tx.Get(db.systemNS, key)
		if err == nil {
			return ErrCollectionExists
		}
		if !errors.Is(err, btree.ErrKeyNotFound) {
			return err
		}

		// Create namespace for the collection
		_, err = tx.CreateNamespace(collectionName)
		if err != nil {
			if errors.Is(err, btree.ErrNamespaceExists) {
				return ErrCollectionExists
			}
			return err
		}

		// Register in system namespace under a fresh identity token.
		if err = tx.Put(db.systemNS, key, newCatalogID()); err != nil {
			return err
		}

		// Persist per-collection config when any setting is non-default.
		if merged.Compression != 0 || pk != "id" {
			var a anyenc.Arena
			obj := a.NewObject()
			if merged.Compression != 0 {
				obj.Set("compression", a.NewNumberInt(int(merged.Compression)))
			}
			if pk != "id" {
				obj.Set("primaryKey", a.NewString(pk))
			}
			if err = tx.Put(db.systemNS, collConfigKey(collectionName), obj.MarshalTo(nil)); err != nil {
				return err
			}
		}

		if coll, err = newCollection(ctx, db, collectionName, tx); err != nil {
			return err
		}

		db.mu.Lock()
		// Plain assignment: a same-tx Drop's closed handle may still occupy
		// the slot (its deferred eviction then finds nothing to evict).
		db.openedCollections[collectionName] = coll
		db.mu.Unlock()

		// A rollback of this scope reverts the namespace + catalog entries
		// created above, but not the registration — a later write through the
		// cached handle would land on the freed root page. Evict on rollback.
		// Deliberately NOT close(): onCollectionClose unconditionally deletes
		// by name (the guarded delete here spares a same-name handle
		// re-registered after the rollback) and orphans non-empty fts pending
		// buffers for the commit-time flush — but this collection's fts writes
		// belong to the rolled-back tx and must never flush. Reset them and
		// mark closed so the handle's own Close is a no-op.
		wtx.onRollbackUndo(func() {
			db.mu.Lock()
			if cur, ok := db.openedCollections[collectionName]; ok && cur == coll {
				delete(db.openedCollections, collectionName)
			}
			db.mu.Unlock()
			cc := coll.(*collection)
			for _, fx := range cc.loadFtsIndexes() {
				fx.pending.reset()
			}
			cc.closed.Store(true)
		})

		return nil
	})
	if err != nil {
		return nil, err
	}
	return coll, nil
}

func (db *db) OpenCollection(ctx context.Context, collectionName string) (Collection, error) {
	db.mu.Lock()
	if coll, ok := db.openedCollections[collectionName]; ok && !coll.(*collection).closed.Load() {
		db.mu.Unlock()
		return coll, nil
	}
	// A CLOSED registered handle is a Drop in an open tx: fall through to the
	// catalog check, which is ctx-aware — the dropping tx sees its own delete
	// (ErrCollectionNotFound, the correct same-tx answer), while a concurrent
	// caller sees the committed row and gets the closed handle back below
	// (fail-safe: ops error until the drop resolves).
	db.mu.Unlock()
	return db.openCollection(ctx, collectionName)
}

func (db *db) openCollection(ctx context.Context, collectionName string) (Collection, error) {
	db.mu.Lock()
	if coll, ok := db.openedCollections[collectionName]; ok && !coll.(*collection).closed.Load() {
		db.mu.Unlock()
		return coll, nil
	}
	db.mu.Unlock()

	err := db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		key := collKey(collectionName)
		_, err := tx.Get(db.systemNS, key)
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				return ErrCollectionNotFound
			}
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// A reopen through an ambient write tx must bind and stamp through the
	// WRITER'S view: the tx's own uncommitted DDL is visible only there (the
	// embedded read view resolves committed namespaces only, so a mid-tx
	// reopen after a same-tx CreateIndex would fail), and init stamps the
	// loaded handles begin+1 when the tx already changed schema — an
	// uncommitted index reloaded here must stay invisible to concurrent
	// readers at the begin cookie.
	var wtx *btree.WriteTx
	if ctxTx := ctx.Value(ctxKeyTx); ctxTx != nil {
		if w, ok := ctxTx.(WriteTx); ok && !w.Done() && w.instanceId() == db.instanceId {
			wtx = w.btreeWriteTx()
		}
	}
	coll, err := newCollection(ctx, db, collectionName, wtx)
	if err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if existing, ok := db.openedCollections[collectionName]; ok {
		// Includes a CLOSED drop-in-flight handle: registering the fresh one
		// over it would revive the dangling-handle corruption Drop's deferred
		// eviction closes — return the closed handle instead (fail-safe).
		return existing, nil
	}
	db.openedCollections[collectionName] = coll
	return coll, nil
}

func (db *db) Collection(ctx context.Context, collectionName string, opts ...CollectionOptions) (Collection, error) {
	coll, err := db.OpenCollection(ctx, collectionName)
	if err == nil {
		// Existing collection: a conflicting PrimaryKey option is a misuse, not a
		// silent no-op — the primary key is immutable after creation.
		if merged := mergeCollOpts(opts); merged.PrimaryKey != "" && merged.PrimaryKey != coll.PrimaryKey() {
			return nil, ErrPrimaryKeyMismatch
		}
		return coll, nil
	}
	if !errors.Is(err, ErrCollectionNotFound) {
		return nil, err
	}
	coll, err = db.CreateCollection(ctx, collectionName, opts...)
	if err == nil {
		return coll, nil
	}
	if !errors.Is(err, ErrCollectionExists) {
		return nil, err
	}
	return db.OpenCollection(ctx, collectionName)
}

func (db *db) GetCollectionNames(ctx context.Context) (collectionNames []string, err error) {
	err = db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		cursor := tx.NewCursor(db.systemNS)
		defer cursor.Close()
		prefix := []byte("coll:")
		if err := cursor.Seek(prefix); err != nil {
			return nil
		}
		for cursor.Valid() {
			key, err := cursor.Key()
			if err != nil {
				return err
			}
			if !strings.HasPrefix(string(key), "coll:") {
				break
			}
			collectionNames = append(collectionNames, string(key[5:]))
			if err := cursor.Next(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return
}

func (db *db) Stats(ctx context.Context) (stats DBStats, err error) {
	err = db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		cursor := tx.NewCursor(db.systemNS)
		defer cursor.Close()
		if err := cursor.Seek([]byte("coll:")); err == nil {
			for cursor.Valid() {
				key, err := cursor.Key()
				if err != nil {
					return err
				}
				if !strings.HasPrefix(string(key), "coll:") {
					break
				}
				stats.CollectionsCount++
				if err := cursor.Next(); err != nil {
					return err
				}
			}
		}
		if err := cursor.Seek([]byte("idx:")); err == nil {
			for cursor.Valid() {
				key, err := cursor.Key()
				if err != nil {
					return err
				}
				if !strings.HasPrefix(string(key), "idx:") {
					break
				}
				stats.IndexesCount++
				if err := cursor.Next(); err != nil {
					return err
				}
			}
		}
		return nil
	})

	// Get file size for TotalSizeBytes
	if fi, fErr := osStat(db.btreeDB.Path()); fErr == nil {
		stats.TotalSizeBytes = int(fi.Size())
		stats.DataSizeBytes = stats.TotalSizeBytes
	}

	stats.DirtyOnOpen = db.dirtyOnOpen
	stats.DirtyQuickCheckDuration = db.dirtyQuickCheckDuration
	return
}

func (db *db) QuickCheck(ctx context.Context) (err error) {
	// btree doesn't have a built-in quick check, just verify we can read
	return db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		cursor := tx.NewCursor(db.systemNS)
		defer cursor.Close()
		if err := cursor.First(); err != nil {
			return err
		}
		return nil
	})
}

func (db *db) IntegrityCheck(ctx context.Context) (err error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err = db.btreeDB.IntegrityCheck(); err != nil {
		return err
	}
	// Catalog consistency: every cataloged collection must have its data
	// namespace. A missing one is the signature of a rename performed by a
	// pre-fix version (catalog re-keyed to the new name, data left under the
	// old — see docs/known-issues.md I-16); its data survives as an orphan
	// namespace but the collection is unopenable until manually repaired.
	return db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		names, lErr := db.listCollectionNames(tx)
		if lErr != nil {
			return lErr
		}
		for _, name := range names {
			if _, nsErr := tx.GetNamespace(name); nsErr != nil {
				if errors.Is(nsErr, btree.ErrNamespaceNotFound) {
					return fmt.Errorf("collection %q: data namespace missing (renamed by a pre-fix version, see docs/known-issues.md I-16); its data survives under the pre-rename namespace name", name)
				}
				// Any other failure to resolve the namespace is itself an
				// integrity problem — never mask it as a clean result.
				return fmt.Errorf("collection %q: resolving data namespace: %w", name, nsErr)
			}
		}
		return nil
	})
}

func (db *db) Backup(ctx context.Context, path string) (err error) {
	// ~ SQLite's recommended online-backup pattern (see backup.c header
	// comment at lines 11-13 and the sqlite3_backup_init/step/finish
	// sequence). We open a fresh destination DB at `path` with
	// identical Options to the source so page sizes match.
	dstOpts := db.btreeDB.Options()
	dstOpts.InMemory = false // destination is always a file (user gave us a path)

	dstDB, err := btree.Open(path, dstOpts)
	if err != nil {
		return fmt.Errorf("open backup destination: %w", err)
	}
	defer func() {
		if cerr := dstDB.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			_ = osRemove(path)
		}
	}()

	b, err := dstDB.BackupInit(db.btreeDB)
	if err != nil {
		return err
	}
	defer func() {
		if ferr := b.Finish(); ferr != nil && err == nil && !errors.Is(ferr, btree.ErrBackupFinished) {
			err = ferr
		}
	}()

	// Copy in bounded batches so ctx cancellation is responsive.
	// SQLite's sqlite3BtreeCopyFile (backup.c:751) uses 0x7FFFFFFF in one
	// shot; we prefer yielding.
	const batch = 256
	for {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		serr := b.Step(batch)
		if errors.Is(serr, btree.ErrBackupDone) {
			return nil
		}
		if serr != nil {
			return serr
		}
	}
}

func (db *db) WriteTx(ctx context.Context) (tx WriteTx, err error) {
	ctxTx := ctx.Value(ctxKeyTx)
	if ctxTx == nil {
		return db.newWriteTx(ctx)
	}

	var ok bool
	if tx, ok = ctxTx.(WriteTx); ok {
		if tx.Done() {
			return nil, ErrTxIsUsed
		}
		if tx.instanceId() != db.instanceId {
			return nil, ErrTxOtherInstance
		}
		return newSavepointTx(ctx, tx)
	}
	return nil, ErrTxIsReadOnly
}

func (db *db) doWriteTx(ctx context.Context, do func(tx *btree.WriteTx) error) error {
	return db.doWriteTxW(ctx, func(_ WriteTx, tx *btree.WriteTx) error {
		return do(tx)
	})
}

// doWriteTxW is doWriteTx for DDL callbacks that publish in-memory schema
// state and must register its reversal on the wrapper tx (onRollbackUndo, see
// commonTx.undo) so a rollback of this scope — or of any enclosing tx — leaves
// the in-memory maps consistent with the reverted on-disk catalog.
func (db *db) doWriteTxW(ctx context.Context, do func(wtx WriteTx, tx *btree.WriteTx) error) error {
	return db.doWriteTxModifiedW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (bool, error) {
		return true, do(wtx, tx)
	})
}

// doWriteTxModifiedW is doWriteTxW whose callback also reports whether it
// modified data (SetModified is skipped otherwise).
func (db *db) doWriteTxModifiedW(ctx context.Context, do func(wtx WriteTx, tx *btree.WriteTx) (bool, error)) error {
	tx, err := db.WriteTx(ctx)
	if err != nil {
		return err
	}
	// User code runs inside this tx (a query.Modifier — UpdateId/UpsertId call
	// mod.Modify inside the callback — or a DDL callback). A panic must not
	// escape holding the btree write lock: BeginWrite has no ctx-cancel escape,
	// so every later write — and Close() — would block forever. Roll back to
	// release the lock, then re-panic so the caller still sees their bug.
	// Guarded by TestTxPanic_UpdateIdDoesNotWedgeDB and
	// TestTxPanic_UpsertIdDoesNotWedgeDB.
	//
	// Deliberately not armed across Commit: both commit layers mark themselves
	// done before doing any work (writeTx.Commit consumes the version CAS on
	// entry and pools the commonTx; btree.WriteTx.Commit sets closed before
	// pager.commit), so a rollback once commit has begun is a silent no-op on an
	// already-recycled tx. A commit-time panic still leaks the lock — that needs
	// the release moved into a defer inside btree.WriteTx.Commit (the abandoned-tx
	// case is guarded by btree's TestWriteTxAbandonedDoesNotDeadlockClose).
	//
	// The rollback runs the undo log, whose closures take c.mu / db.mu: a
	// callback must not panic while holding either with an explicit (non-defer)
	// unlock, or the unwind deadlocks here.
	committing := false
	defer func() {
		if r := recover(); r != nil {
			if !committing {
				_ = tx.Rollback()
			}
			panic(r)
		}
	}()
	var modified bool
	if modified, err = do(tx, tx.btreeWriteTx()); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	if modified {
		tx.SetModified()
	}
	committing = true
	return tx.Commit()
}

func (db *db) doWriteTxModified(ctx context.Context, do func(tx *btree.WriteTx) (bool, error)) error {
	return db.doWriteTxModifiedW(ctx, func(_ WriteTx, tx *btree.WriteTx) (bool, error) {
		return do(tx)
	})
}

func (db *db) getReadTx(ctx context.Context) (tx ReadTx, err error) {
	ctxTx := ctx.Value(ctxKeyTx)
	if ctxTx == nil {
		return db.ReadTx(ctx)
	}

	var ok bool
	if tx, ok = ctxTx.(ReadTx); ok {
		if tx.Done() {
			return nil, ErrTxIsUsed
		}
		if tx.instanceId() != db.instanceId {
			return nil, ErrTxOtherInstance
		}
		return noOpTx{ReadTx: tx}, nil
	}
	return nil, ErrTxIsReadOnly
}

func (db *db) doReadTx(ctx context.Context, do func(tx *btree.ReadTx) error) error {
	tx, err := db.getReadTx(ctx)
	if err != nil {
		return err
	}
	// Rollback-on-panic, read side: Commit is a read tx's release path
	// (readTx.Commit rolls back the underlying btree tx) — ReadTx has no
	// Rollback. Left unreleased, a panic strands the reader-sem slot and
	// db.mu.RLock; after MaxReaders of them every read blocks and Close() hangs.
	// See doWriteTxW.
	committing := false
	defer func() {
		if r := recover(); r != nil {
			if !committing {
				_ = tx.Commit()
			}
			panic(r)
		}
	}()
	if err = do(tx.btreeReadTx()); err != nil {
		_ = tx.Commit()
		return err
	}
	committing = true
	return tx.Commit()
}

func (db *db) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return ErrDBIsClosed
	}

	// Signal btree to reject new transactions early
	db.btreeDB.SetClosing()

	var collToClose []Collection
	db.mu.Lock()
	for _, c := range db.openedCollections {
		collToClose = append(collToClose, c)
	}
	db.mu.Unlock()
	for _, c := range collToClose {
		if cErr := c.(*collection).close(); cErr != nil {
			log.Printf("collection close error: %v", cErr)
		}
	}

	if err := db.recoveryController.Stop(); err != nil {
		log.Printf("recovery controller stop error: %v", err)
	}

	return db.btreeDB.Close()
}

func (db *db) createRecoveryController(ctx context.Context, path string) (*durability.Controller, bool) {
	flushFunc, err := durability.NewFlushFunc(db.config.Durability.FlushMode.toRecoveryFlushMode())
	if err != nil {
		return nil, false
	}

	opts := durability.Options{
		AutoFlushEnable:    db.config.Durability.AutoFlush,
		AutoFlushIdleAfter: db.config.Durability.IdleAfter,
		AcquireWrite: func(ctx context.Context, fn func(bdb *btree.DB) error) error {
			return fn(db.btreeDB)
		},
		AutoFlushFunc: flushFunc,
	}

	if db.config.Durability.Sentinel {
		opts.Sentinel = sentinel.New(path)
	}
	controller := durability.NewController(opts)

	ctx = context.WithValue(ctx, "dbPath", path)

	dirty, err := controller.OnOpen(ctx)
	if err != nil {
		return controller, false
	}

	return controller, dirty
}

func (db *db) Flush(ctx context.Context, waitIdleTime time.Duration, mode FlushMode) error {
	if db.recoveryController == nil {
		return fmt.Errorf("recovery is not enabled")
	}

	return db.recoveryController.Flush(ctx, waitIdleTime, mode.toRecoveryFlushMode())
}

func (db *db) onCollectionClose(c *collection) {
	db.mu.Lock()
	// Identity scan, not a delete by c.name: c.name is read here without c.mu
	// (taking it would deadlock — Drop holds it when calling close()), so a
	// name-keyed delete races with Rename's name flip and could evict a
	// same-named successor handle or miss the entry entirely.
	for name, cur := range db.openedCollections {
		if cur == Collection(c) {
			delete(db.openedCollections, name)
			break
		}
	}
	// Keep non-empty fts pending buffers reachable for the commit-time flush
	// (or the next tx-begin reset). The buffer is only ever non-empty inside
	// an open write tx, so outside a tx this appends nothing.
	for _, fx := range c.loadFtsIndexes() {
		if !fx.pending.empty() {
			db.orphanFtsPending = append(db.orphanFtsPending, fx)
		}
	}
	db.mu.Unlock()
}

// persistAllDirtySketches writes all modified sketches for all open collections.
// Called once per write transaction commit to batch sketch persistence.
func (db *db) persistAllDirtySketches(tx *btree.WriteTx) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, coll := range db.openedCollections {
		c := coll.(*collection)
		if c.closed.Load() {
			// A Drop in this tx (eviction deferred to its commit
			// publication): its stat_data rows were deleted with the
			// collection — persisting the still-dirty sketches would durably
			// resurrect orphaned rows a later same-named index would adopt.
			continue
		}
		if err := c.persistSketches(tx); err != nil {
			return err
		}
	}
	return nil
}

// flushAmbientFtsPending flushes buffered full-text postings into the write
// tx carried by ctx, if any — so a $text READ inside that tx sees the tx's
// own uncommitted writes, matching the flush newSavepointTx already performs
// for the write verbs. A no-op outside an ambient write tx: nothing is
// buffered at the start of a fresh write tx (resetAllFtsPending), and a
// read-only ambient tx cannot have buffered anything.
// Caveat (documented, same class as iterate-while-mutate being undefined):
// the flush WRITES into the ambient tx, so a $text read performed while an
// iterator on the same tx is mid-iteration restructures pages under that
// iterator's cursors — collect results before issuing in-tx $text reads.
func (db *db) flushAmbientFtsPending(ctx context.Context) error {
	wtx, ok := db.ambientWriteTx(ctx)
	if !ok {
		return nil
	}
	return db.flushAllFtsPending(wtx.btreeWriteTx())
}

// ambientWriteTx extracts a usable write tx carried by ctx: present, not
// done, and belonging to this db instance.
func (db *db) ambientWriteTx(ctx context.Context) (WriteTx, bool) {
	ctxTx := ctx.Value(ctxKeyTx)
	if ctxTx == nil {
		return nil, false
	}
	wtx, ok := ctxTx.(WriteTx)
	if !ok || wtx.Done() || wtx.instanceId() != db.instanceId {
		return nil, false
	}
	return wtx, true
}

// flushAllFtsPending flushes every open collection's full-text write-back
// buffer into the B-tree. Called once per write-tx commit, BEFORE the btree
// commit, so the buffered postings land in the SAME atomic transaction as the
// document writes — preserving strong cross-process consistency (another
// process opening a read tx after commit sees a complete, consistent index; a
// crash leaves no doc without its postings). The buffer never survives the
// commit boundary.
func (db *db) flushAllFtsPending(tx *btree.WriteTx) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	// Orphans first: their writes chronologically precede anything buffered
	// by a same-name collection reopened after the close, so for a document
	// touched on both sides of the close the later postings win.
	for _, fx := range db.orphanFtsPending {
		if err := fx.flushPending(tx); err != nil {
			return err
		}
	}
	db.orphanFtsPending = db.orphanFtsPending[:0]
	for _, coll := range db.openedCollections {
		c := coll.(*collection)
		for _, fx := range c.loadFtsIndexes() {
			if err := fx.flushPending(tx); err != nil {
				return err
			}
		}
	}
	return nil
}

// resetAllFtsPending discards any buffered full-text writes left over from a
// rolled-back transaction. Called at the start of every write tx so a new tx
// begins with an empty buffer.
func (db *db) resetAllFtsPending() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, fx := range db.orphanFtsPending {
		fx.pending.reset()
	}
	db.orphanFtsPending = db.orphanFtsPending[:0]
	for _, coll := range db.openedCollections {
		c := coll.(*collection)
		for _, fx := range c.loadFtsIndexes() {
			fx.pending.reset()
		}
	}
}

// getIndexInfos reads all index metadata for a collection from the system namespace
func (db *db) getIndexInfos(tx *btree.ReadTx, collName string) ([]IndexInfo, error) {
	prefix := indexKeyPrefix(collName)
	cursor := tx.NewCursor(db.systemNS)
	defer cursor.Close()
	// A Seek error is a real read failure, not "no indexes" — an empty or
	// non-matching prefix leaves the cursor invalid with a nil error.
	if err := cursor.Seek([]byte(prefix)); err != nil {
		return nil, err
	}
	var (
		result []IndexInfo
		p      anyenc.Parser
	)
	for cursor.Valid() {
		key, err := cursor.Key()
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(string(key), prefix) {
			break
		}
		val, err := cursor.Value()
		if err != nil {
			return nil, err
		}
		v, err := p.Parse(val)
		if err != nil {
			return nil, err
		}
		info := IndexInfo{
			Name:   v.GetString("name"),
			Sparse: v.GetBool("sparse"),
			Unique: v.GetBool("unique"),
			Kind:   IndexKind(v.GetInt("kind")),
		}
		if info.Kind == IndexKindFulltext {
			info.Fulltext = &FulltextParams{}
			if fv := v.Get("fulltext"); fv != nil {
				info.Fulltext.B = fv.GetFloat64("b")
				info.Fulltext.K1 = fv.GetFloat64("k1")
				if wv := fv.Get("weights"); wv != nil {
					if wobj, oerr := wv.Object(); oerr == nil {
						weights := map[string]float64{}
						wobj.Visit(func(key []byte, val *anyenc.Value) {
							weights[string(key)] = val.GetFloat64()
						})
						if len(weights) > 0 {
							info.Fulltext.Weights = weights
						}
					}
				}
			}
		}
		for _, fv := range v.GetArray("fields") {
			info.Fields = append(info.Fields, string(fv.GetStringBytes()))
		}
		if IndexKind(v.GetInt("kind")) == IndexKindVector {
			if vv := v.Get("vector"); vv != nil {
				info.Kind = IndexKindVector
				info.Vector = &VectorParams{
					Field:              vv.GetString("field"),
					Dim:                vv.GetInt("dim"),
					Metric:             VectorMetric(vv.GetInt("metric")),
					M:                  vv.GetInt("m"),
					EfConstruction:     vv.GetInt("efc"),
					EfSearch:           vv.GetInt("efs"),
					Quantization:       VectorQuantization(vv.GetInt("quant")),
					Mode:               VectorMode(vv.GetInt("mode")),
					HybridCacheVectors: vv.GetInt("hvc") != 0,
					CompactRatio:       vv.GetFloat64("cr"),
					NList:              vv.GetInt("nlist"),
					NProbe:             vv.GetInt("nprobe"),
					Closure:            vv.GetInt("closure"),
					PrecomputeTableMiB: vv.GetInt("ptmib"),
				}
			}
		}
		result = append(result, info)
		if err := cursor.Next(); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// indexDefMatches reports whether a persisted index record (the system-namespace
// value bytes) describes the same definition as info: same fields (order
// significant), same unique and sparse flags. The name is implicitly equal —
// the record was fetched by name. Used to distinguish an idempotent
// EnsureIndex (identical definition) from a conflicting redefinition.
func indexDefMatches(persisted []byte, info IndexInfo) bool {
	var p anyenc.Parser
	v, err := p.Parse(persisted)
	if err != nil {
		return false
	}
	if v.GetBool("unique") != info.Unique || v.GetBool("sparse") != info.Sparse {
		return false
	}
	if IndexKind(v.GetInt("kind")) != info.Kind {
		return false
	}
	fields := v.GetArray("fields")
	if len(fields) != len(info.Fields) {
		return false
	}
	for i, fv := range fields {
		if string(fv.GetStringBytes()) != info.Fields[i] {
			return false
		}
	}
	// Kind-specific params are part of the definition too. Vector indexes have
	// empty Fields and fulltext scoring params don't appear in Fields either, so
	// without these checks ANY same-name redefinition (dim/metric/mode change,
	// different weights) would read as identical and EnsureIndex would silently
	// keep the old index.
	if info.Kind == IndexKindVector && !vectorDefMatches(v.Get("vector"), info.Vector) {
		return false
	}
	if info.Kind == IndexKindFulltext && !fulltextDefMatches(v.Get("fulltext"), info.Fulltext) {
		return false
	}
	return true
}

// vectorDefMatches compares a persisted vector-param record against the given
// VectorParams (nil-safe on both sides; absent numeric fields read as zero,
// matching how zero-valued params are omitted at write time).
func vectorDefMatches(vv *anyenc.Value, p *VectorParams) bool {
	if p == nil {
		p = &VectorParams{}
	}
	var (
		field                                 string
		dim, metric, m, efc, efs, quant, mode int
		hvc                                   bool
		cr                                    float64
		nlist, nprobe, closure, ptmib         int
	)
	if vv != nil {
		field = vv.GetString("field")
		dim = vv.GetInt("dim")
		metric = vv.GetInt("metric")
		m = vv.GetInt("m")
		efc = vv.GetInt("efc")
		efs = vv.GetInt("efs")
		quant = vv.GetInt("quant")
		mode = vv.GetInt("mode")
		hvc = vv.GetInt("hvc") != 0
		cr = vv.GetFloat64("cr")
		nlist = vv.GetInt("nlist")
		nprobe = vv.GetInt("nprobe")
		closure = vv.GetInt("closure")
		ptmib = vv.GetInt("ptmib")
	}
	return field == p.Field &&
		dim == p.Dim &&
		metric == int(p.Metric) &&
		m == p.M &&
		efc == p.EfConstruction &&
		efs == p.EfSearch &&
		quant == int(p.Quantization) &&
		mode == int(p.Mode) &&
		hvc == p.HybridCacheVectors &&
		cr == p.CompactRatio &&
		nlist == p.NList &&
		nprobe == p.NProbe &&
		closure == p.Closure &&
		ptmib == p.PrecomputeTableMiB
}

// fulltextDefMatches compares a persisted fulltext-param record against the
// given FulltextParams (nil-safe; absent fields read as zero, matching the
// omit-zero write side).
func fulltextDefMatches(fv *anyenc.Value, p *FulltextParams) bool {
	if p == nil {
		p = &FulltextParams{}
	}
	var b, k1 float64
	weights := map[string]float64{}
	if fv != nil {
		b = fv.GetFloat64("b")
		k1 = fv.GetFloat64("k1")
		if wv := fv.Get("weights"); wv != nil {
			if wobj, oerr := wv.Object(); oerr == nil {
				wobj.Visit(func(key []byte, val *anyenc.Value) {
					weights[string(key)] = val.GetFloat64()
				})
			}
		}
	}
	if b != p.B || k1 != p.K1 || len(weights) != len(p.Weights) {
		return false
	}
	for f, w := range p.Weights {
		pw, ok := weights[f]
		if !ok || pw != w {
			return false
		}
	}
	return true
}

// registerIndex stores index metadata in the system namespace.
//
// If an index with the same name already exists, the persisted definition is
// compared against info: an IDENTICAL definition returns ErrIndexExists (which
// EnsureIndex treats as an idempotent no-op), while a DIFFERENT definition
// (fields / unique / sparse) returns ErrIndexMismatch — a redefinition is never
// applied silently. This mirrors SQLite, where CREATE INDEX never mutates an
// existing object's definition in place; the caller must DROP then CREATE.
func (db *db) registerIndex(tx *btree.WriteTx, collName string, info IndexInfo) error {
	key := indexKey(collName, info.Name)
	// Check if already exists
	if existing, err := tx.Get(db.systemNS, key); err == nil {
		if indexDefMatches(existing, info) {
			return ErrIndexExists
		}
		return ErrIndexMismatch
	}
	var a anyenc.Arena
	obj := a.NewObject()
	obj.Set("name", a.NewString(info.Name))
	fields := a.NewArray()
	for i, f := range info.Fields {
		fields.SetArrayItem(i, a.NewString(f))
	}
	obj.Set("fields", fields)
	if info.Sparse {
		obj.Set("sparse", a.NewTrue())
	}
	if info.Unique {
		obj.Set("unique", a.NewTrue())
	}
	if info.Kind != IndexKindRange {
		obj.Set("kind", a.NewNumberInt(int(info.Kind)))
	}
	if info.Kind == IndexKindVector && info.Vector != nil {
		vobj := a.NewObject()
		vobj.Set("field", a.NewString(info.Vector.Field))
		vobj.Set("dim", a.NewNumberInt(info.Vector.Dim))
		vobj.Set("metric", a.NewNumberInt(int(info.Vector.Metric)))
		vobj.Set("m", a.NewNumberInt(info.Vector.M))
		vobj.Set("efc", a.NewNumberInt(info.Vector.EfConstruction))
		vobj.Set("efs", a.NewNumberInt(info.Vector.EfSearch))
		vobj.Set("quant", a.NewNumberInt(int(info.Vector.Quantization)))
		vobj.Set("mode", a.NewNumberInt(int(info.Vector.Mode)))
		if info.Vector.HybridCacheVectors {
			vobj.Set("hvc", a.NewNumberInt(1))
		}
		if info.Vector.CompactRatio != 0 {
			vobj.Set("cr", a.NewNumberFloat64(info.Vector.CompactRatio))
		}
		if info.Vector.NList != 0 {
			vobj.Set("nlist", a.NewNumberInt(info.Vector.NList))
		}
		if info.Vector.NProbe != 0 {
			vobj.Set("nprobe", a.NewNumberInt(info.Vector.NProbe))
		}
		if info.Vector.Closure != 0 {
			vobj.Set("closure", a.NewNumberInt(info.Vector.Closure))
		}
		if info.Vector.PrecomputeTableMiB != 0 {
			vobj.Set("ptmib", a.NewNumberInt(info.Vector.PrecomputeTableMiB))
		}
		obj.Set("vector", vobj)
	}
	if info.Kind == IndexKindFulltext && info.Fulltext != nil {
		ft := info.Fulltext
		if ft.B != 0 || ft.K1 != 0 || len(ft.Weights) != 0 {
			fobj := a.NewObject()
			if ft.B != 0 {
				fobj.Set("b", a.NewNumberFloat64(ft.B))
			}
			if ft.K1 != 0 {
				fobj.Set("k1", a.NewNumberFloat64(ft.K1))
			}
			if len(ft.Weights) != 0 {
				wobj := a.NewObject()
				for field, w := range ft.Weights {
					wobj.Set(field, a.NewNumberFloat64(w))
				}
				fobj.Set("weights", wobj)
			}
			obj.Set("fulltext", fobj)
		}
	}
	return tx.Put(db.systemNS, key, obj.MarshalTo(nil))
}

// removeIndex removes index metadata from the system namespace. An already-absent
// key is not an error: a peer (or a racing same-process caller that committed
// between our staleness snapshot and now) may have removed it first. Callers
// rely on this so DropIndex never leaks a raw btree.ErrKeyNotFound.
func (db *db) removeIndex(tx *btree.WriteTx, collName, indexName string) error {
	key := indexKey(collName, indexName)
	if err := tx.Delete(db.systemNS, key); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
		return err
	}
	return nil
}

// removeCollection removes collection metadata from the system namespace
func (db *db) removeCollection(tx *btree.WriteTx, collName string) error {
	tx.MarkSchemaChanged()
	// Remove collection key
	if err := tx.Delete(db.systemNS, collKey(collName)); err != nil {
		return err
	}
	// Remove all index keys for this collection
	prefix := indexKeyPrefix(collName)
	cursor := tx.NewCursor(db.systemNS)
	defer cursor.Close()
	if err := cursor.Seek([]byte(prefix)); err != nil {
		return nil
	}
	var keysToDelete [][]byte
	var idxNames []string
	var p anyenc.Parser
	for cursor.Valid() {
		key, err := cursor.Key()
		if err != nil {
			return err
		}
		if !strings.HasPrefix(string(key), prefix) {
			break
		}
		keysToDelete = append(keysToDelete, append([]byte(nil), key...))
		val, err := cursor.Value()
		if err != nil {
			return err
		}
		if v, err := p.Parse(val); err == nil {
			idxNames = append(idxNames, v.GetString("name"))
		}
		if err := cursor.Next(); err != nil {
			return err
		}
	}
	for _, key := range keysToDelete {
		if err := tx.Delete(db.systemNS, key); err != nil {
			return err
		}
	}
	// Remove the per-index sketch and multikey records too. The sketch is
	// keyed by collection name; the multikey flag is keyed by the namespace
	// name.
	for _, name := range idxNames {
		_ = tx.Delete(db.systemNS, sketchKey(collName, name))
		_ = tx.Delete(db.systemNS, multikeyKey(indexNsName(collName, name)))
	}
	// Remove collection config
	_ = tx.Delete(db.systemNS, collConfigKey(collName))
	return nil
}

// renameCollection renames a collection: the catalog metadata in the system
// namespace AND every btree namespace derived from the collection name (data,
// per-index) are re-keyed in one transaction.
func (db *db) renameCollection(tx *btree.WriteTx, oldName, newName string) error {
	if oldName == newName {
		return nil
	}
	if err := validateCollectionName(newName); err != nil {
		return err
	}
	// Reject a rename onto an existing collection instead of silently
	// overwriting its catalog entry. Read-your-writes through the write tx
	// also covers a collection created earlier in the same tx.
	if _, err := tx.Get(db.systemNS, collKey(newName)); err == nil {
		return ErrCollectionExists
	} else if !errors.Is(err, btree.ErrKeyNotFound) {
		return err
	}
	tx.MarkSchemaChanged()

	// Enumerate the on-disk index metadata BEFORE the idx: keys move (the
	// same source Drop uses, for the same reason).
	infos, err := db.getIndexInfos(&tx.ReadTx, oldName)
	if err != nil {
		return err
	}

	// Data namespace: hard error if missing — "renaming" only the metadata of
	// a contents-less collection would recreate the pre-fix I-16 breakage.
	if err = tx.RenameNamespace(oldName, newName); err != nil {
		if errors.Is(err, btree.ErrNamespaceExists) {
			return fmt.Errorf("rename %q -> %q: target namespace already exists: %w", oldName, newName, ErrCollectionExists)
		}
		if errors.Is(err, btree.ErrNamespaceNotFound) {
			return fmt.Errorf("rename %q -> %q: data namespace missing (collection likely broken by a pre-fix rename, see docs/known-issues.md I-16): %w", oldName, newName, err)
		}
		return err
	}
	// Per-index namespaces. Absent ones are tolerated like Drop tolerates
	// them: a missing namespace means the index is already broken (legacy
	// pre-fix rename) or backend-dependent (HNSW has no :cb/:cell, IVF no
	// :adj); failing the whole rename wouldn't repair it.
	renameNs := func(from, to string) error {
		if nsErr := tx.RenameNamespace(from, to); nsErr != nil && !errors.Is(nsErr, btree.ErrNamespaceNotFound) {
			return nsErr
		}
		return nil
	}
	for _, info := range infos {
		switch {
		case isFulltext(info):
			fromNames := ftsIndexNames(oldName, info.Name)
			toNames := ftsIndexNames(newName, info.Name)
			for i := range fromNames {
				if err = renameNs(fromNames[i], toNames[i]); err != nil {
					return err
				}
			}
		case info.Kind == IndexKindVector:
			fromPrefix := vectorIndexNsPrefix(oldName, info.Name)
			toPrefix := vectorIndexNsPrefix(newName, info.Name)
			for _, suf := range vectorIndexNsSuffixes {
				if err = renameNs(fromPrefix+suf, toPrefix+suf); err != nil {
					return err
				}
			}
		default:
			if err = renameNs(indexNsName(oldName, info.Name), indexNsName(newName, info.Name)); err != nil {
				return err
			}
		}
	}

	// Move the collection key, preserving the identity token in its value —
	// cached handles (local and peer) recognise the renamed collection is a
	// different one than any later same-named creation.
	catalogID, err := tx.AppendValue(db.systemNS, collKey(oldName), nil)
	if err != nil {
		return err
	}
	if err = tx.Delete(db.systemNS, collKey(oldName)); err != nil {
		return err
	}
	if err = tx.Put(db.systemNS, collKey(newName), catalogID); err != nil {
		return err
	}

	// Rename collection config
	if cfgData, err := tx.AppendValue(db.systemNS, collConfigKey(oldName), nil); err == nil {
		_ = tx.Delete(db.systemNS, collConfigKey(oldName))
		_ = tx.Put(db.systemNS, collConfigKey(newName), cfgData)
	}

	// Rename index keys. A Seek error is a real read failure (an empty or
	// non-matching prefix leaves the cursor invalid with a nil error) — it
	// must fail the rename, or the tx would commit with the namespaces
	// renamed but the idx:/stat_data:/idx_mk: records left under the old name.
	oldPrefix := indexKeyPrefix(oldName)
	cursor := tx.NewCursor(db.systemNS)
	defer cursor.Close()
	if err := cursor.Seek([]byte(oldPrefix)); err != nil {
		return err
	}
	type kv struct {
		oldKey []byte
		name   string
		val    []byte
	}
	var entries []kv
	var p anyenc.Parser
	for cursor.Valid() {
		key, err := cursor.Key()
		if err != nil {
			return err
		}
		if !strings.HasPrefix(string(key), oldPrefix) {
			break
		}
		val, err := cursor.Value()
		if err != nil {
			return err
		}
		v, err := p.Parse(val)
		if err != nil {
			return err
		}
		entries = append(entries, kv{
			oldKey: append([]byte(nil), key...),
			name:   v.GetString("name"),
			val:    append([]byte(nil), val...),
		})
		if err := cursor.Next(); err != nil {
			return err
		}
	}
	for _, e := range entries {
		if err := tx.Delete(db.systemNS, e.oldKey); err != nil {
			return err
		}
		if err := tx.Put(db.systemNS, indexKey(newName, e.name), e.val); err != nil {
			return err
		}
		// The per-index sketch and multikey records derive from the collection
		// name too; leaving them behind would orphan them AND let a later
		// rename back resurrect a stale record — for the multikey flag that
		// means tight seeks against an index that has since seen arrays,
		// i.e. silently dropped docs. Moved (delete+put), never copied.
		if skData, err := tx.AppendValue(db.systemNS, sketchKey(oldName, e.name), nil); err == nil {
			_ = tx.Delete(db.systemNS, sketchKey(oldName, e.name))
			if err = tx.Put(db.systemNS, sketchKey(newName, e.name), skData); err != nil {
				return err
			}
		}
		oldMk := multikeyKey(indexNsName(oldName, e.name))
		if mkVal, err := tx.AppendValue(db.systemNS, oldMk, nil); err == nil {
			_ = tx.Delete(db.systemNS, oldMk)
			if err = tx.Put(db.systemNS, multikeyKey(indexNsName(newName, e.name)), mkVal); err != nil {
				return err
			}
		}
	}
	return nil
}

// loadCollConfig loads per-collection config from the system namespace.
func (db *db) loadCollConfig(tx *btree.ReadTx, collName string) (collConfig, error) {
	var cfg collConfig
	data, err := tx.AppendValue(db.systemNS, collConfigKey(collName), nil)
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return cfg, nil // no config stored — use defaults
		}
		return cfg, err
	}
	var p anyenc.Parser
	val, err := p.Parse(data)
	if err != nil {
		return cfg, err
	}
	cfg.Compression = Compression(val.GetInt("compression"))
	cfg.PrimaryKey = val.GetString("primaryKey")
	return cfg, nil
}

// listCollectionNames returns sorted collection names from system namespace
func (db *db) listCollectionNames(tx *btree.ReadTx) ([]string, error) {
	cursor := tx.NewCursor(db.systemNS)
	defer cursor.Close()
	prefix := []byte("coll:")
	if err := cursor.Seek(prefix); err != nil {
		return nil, nil
	}
	var names []string
	for cursor.Valid() {
		key, err := cursor.Key()
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(string(key), "coll:") {
			break
		}
		names = append(names, string(key[5:]))
		if err := cursor.Next(); err != nil {
			return nil, err
		}
	}
	sort.Strings(names)
	return names, nil
}
