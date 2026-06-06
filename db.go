package anystore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/durability"
	"github.com/anyproto/any-store/v2/internal/durability/sentinel"
	"github.com/anyproto/any-store/v2/internal/objectid"
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
		instanceId:        objectid.NewObjectID().Hex(),
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
	// Page-level integrity is on by default for non-encrypted databases.
	// XXH3-128 trailer per page costs <1% on writes and is invisible on
	// reads. Encrypted databases get stronger integrity from the
	// cipher's AEAD tag, so the cksum codec is skipped there. File-state
	// is authoritative on reopen — existing plain DBs stay plain
	// regardless of this default.
	opts := btree.Options{
		PageSize:              4096,
		CacheSize:             cacheSize,
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

	openedCollections map[string]Collection
	closed            atomic.Bool

	dirtyOnOpen             bool
	dirtyQuickCheckDuration time.Duration
	mu                      sync.Mutex
	writeMu                 sync.Mutex
}

func collKey(name string) []byte {
	return []byte("coll:" + name)
}

func collConfigKey(name string) []byte {
	return []byte("collcfg:" + name)
}

type collConfig struct {
	Compression Compression
}

func indexKey(collName, indexName string) []byte {
	return []byte("idx:" + collName + ":" + indexName)
}

func sketchKey(collName, indexName string) []byte {
	return []byte("stat_data:" + collName + ":" + indexName)
}

func indexKeyPrefix(collName string) string {
	return "idx:" + collName + ":"
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

	db.checkStale(&btWtx.ReadTx)
	db.resetUncommittedSketches(&btWtx.ReadTx)

	version := newTxVersion()
	tx := txPool.Get().(*commonTx)
	tx.db = db
	tx.readTx = &btWtx.ReadTx
	tx.writeTx = btWtx
	tx.modified = false
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
func (db *db) checkStale(tx *btree.ReadTx) {
	if tx.IsSchemaStale() {
		db.reconcileIndexSet(tx)
	}
	if tx.IsSchemaStale() || tx.IsDataStale() {
		db.reloadSketches(tx)
		db.btreeDB.UpdateLocalCounters(tx.DiskFileChangeCounter(), tx.DiskSchemaCookie())
	}
}

// reconcileIndexSet rebuilds the in-memory index set of every open collection
// from on-disk metadata, called from checkStale when the schema cookie advanced.
// See checkStale for the contract. Each collection is reconciled under its own
// c.mu and the result published atomically (copy-on-write), so lock-free query
// readers always observe a complete index generation.
func (db *db) reconcileIndexSet(tx *btree.ReadTx) {
	db.mu.Lock()
	colls := make([]*collection, 0, len(db.openedCollections))
	for _, coll := range db.openedCollections {
		colls = append(colls, coll.(*collection))
	}
	db.mu.Unlock()

	for _, c := range colls {
		c.reconcileIndexes(tx)
	}
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
	}
	return merged
}

func (db *db) CreateCollection(ctx context.Context, collectionName string, opts ...CollectionOptions) (Collection, error) {
	db.mu.Lock()
	if _, ok := db.openedCollections[collectionName]; ok {
		db.mu.Unlock()
		return nil, ErrCollectionExists
	}
	db.mu.Unlock()
	merged := mergeCollOpts(opts)
	var coll Collection
	err := db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
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

		// Register in system namespace
		if err = tx.Put(db.systemNS, key, []byte("1")); err != nil {
			return err
		}

		// Persist per-collection config if non-default
		if merged.Compression != 0 {
			var a anyenc.Arena
			obj := a.NewObject()
			obj.Set("compression", a.NewNumberInt(int(merged.Compression)))
			if err = tx.Put(db.systemNS, collConfigKey(collectionName), obj.MarshalTo(nil)); err != nil {
				return err
			}
		}

		if coll, err = newCollection(ctx, db, collectionName, tx); err != nil {
			return err
		}

		db.mu.Lock()
		defer db.mu.Unlock()
		db.openedCollections[collectionName] = coll

		return nil
	})
	if err != nil {
		return nil, err
	}
	return coll, nil
}

func (db *db) OpenCollection(ctx context.Context, collectionName string) (Collection, error) {
	db.mu.Lock()
	if coll, ok := db.openedCollections[collectionName]; ok {
		db.mu.Unlock()
		return coll, nil
	}
	db.mu.Unlock()
	return db.openCollection(ctx, collectionName)
}

func (db *db) openCollection(ctx context.Context, collectionName string) (Collection, error) {
	db.mu.Lock()
	if coll, ok := db.openedCollections[collectionName]; ok {
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

	coll, err := newCollection(ctx, db, collectionName)
	if err != nil {
		return nil, err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if existing, ok := db.openedCollections[collectionName]; ok {
		return existing, nil
	}
	db.openedCollections[collectionName] = coll
	return coll, nil
}

func (db *db) Collection(ctx context.Context, collectionName string, opts ...CollectionOptions) (Collection, error) {
	coll, err := db.OpenCollection(ctx, collectionName)
	if err == nil {
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
	return db.btreeDB.IntegrityCheck()
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
	tx, err := db.WriteTx(ctx)
	if err != nil {
		return err
	}
	if err = do(tx.btreeWriteTx()); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	tx.SetModified()
	return tx.Commit()
}

func (db *db) doWriteTxModified(ctx context.Context, do func(tx *btree.WriteTx) (bool, error)) error {
	tx, err := db.WriteTx(ctx)
	if err != nil {
		return err
	}
	var modified bool
	if modified, err = do(tx.btreeWriteTx()); err != nil {
		return errors.Join(err, tx.Rollback())
	}

	if modified {
		tx.SetModified()
	}
	return tx.Commit()
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
	if err = do(tx.btreeReadTx()); err != nil {
		_ = tx.Commit()
		return err
	}
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

func (db *db) onCollectionClose(name string) {
	db.mu.Lock()
	delete(db.openedCollections, name)
	db.mu.Unlock()
}

// persistAllDirtySketches writes all modified sketches for all open collections.
// Called once per write transaction commit to batch sketch persistence.
func (db *db) persistAllDirtySketches(tx *btree.WriteTx) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, coll := range db.openedCollections {
		c := coll.(*collection)
		if err := c.persistSketches(tx); err != nil {
			return err
		}
	}
	return nil
}

// getIndexInfos reads all index metadata for a collection from the system namespace
func (db *db) getIndexInfos(tx *btree.ReadTx, collName string) ([]IndexInfo, error) {
	prefix := indexKeyPrefix(collName)
	cursor := tx.NewCursor(db.systemNS)
	defer cursor.Close()
	if err := cursor.Seek([]byte(prefix)); err != nil {
		return nil, nil
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
		}
		for _, fv := range v.GetArray("fields") {
			info.Fields = append(info.Fields, string(fv.GetStringBytes()))
		}
		if IndexKind(v.GetInt("kind")) == IndexKindVector {
			if vv := v.Get("vector"); vv != nil {
				info.Kind = IndexKindVector
				info.Vector = &VectorParams{
					Field:          vv.GetString("field"),
					Dim:            vv.GetInt("dim"),
					Metric:         VectorMetric(vv.GetInt("metric")),
					M:              vv.GetInt("m"),
					EfConstruction: vv.GetInt("efc"),
					EfSearch:       vv.GetInt("efs"),
					Quantization:   VectorQuantization(vv.GetInt("quant")),
					Mode:           VectorMode(vv.GetInt("mode")),
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
	fields := v.GetArray("fields")
	if len(fields) != len(info.Fields) {
		return false
	}
	for i, fv := range fields {
		if string(fv.GetStringBytes()) != info.Fields[i] {
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
	if info.Kind == IndexKindVector && info.Vector != nil {
		obj.Set("kind", a.NewNumberInt(int(IndexKindVector)))
		vobj := a.NewObject()
		vobj.Set("field", a.NewString(info.Vector.Field))
		vobj.Set("dim", a.NewNumberInt(info.Vector.Dim))
		vobj.Set("metric", a.NewNumberInt(int(info.Vector.Metric)))
		vobj.Set("m", a.NewNumberInt(info.Vector.M))
		vobj.Set("efc", a.NewNumberInt(info.Vector.EfConstruction))
		vobj.Set("efs", a.NewNumberInt(info.Vector.EfSearch))
		vobj.Set("quant", a.NewNumberInt(int(info.Vector.Quantization)))
		vobj.Set("mode", a.NewNumberInt(int(info.Vector.Mode)))
		obj.Set("vector", vobj)
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
	for cursor.Valid() {
		key, err := cursor.Key()
		if err != nil {
			return err
		}
		if !strings.HasPrefix(string(key), prefix) {
			break
		}
		keysToDelete = append(keysToDelete, append([]byte(nil), key...))
		if err := cursor.Next(); err != nil {
			return err
		}
	}
	for _, key := range keysToDelete {
		if err := tx.Delete(db.systemNS, key); err != nil {
			return err
		}
	}
	// Remove collection config
	_ = tx.Delete(db.systemNS, collConfigKey(collName))
	return nil
}

// renameCollection renames collection metadata in the system namespace
func (db *db) renameCollection(tx *btree.WriteTx, oldName, newName string) error {
	tx.MarkSchemaChanged()
	// Remove old collection key, add new one
	if err := tx.Delete(db.systemNS, collKey(oldName)); err != nil {
		return err
	}
	if err := tx.Put(db.systemNS, collKey(newName), []byte("1")); err != nil {
		return err
	}

	// Rename collection config
	if cfgData, err := tx.AppendValue(db.systemNS, collConfigKey(oldName), nil); err == nil {
		_ = tx.Delete(db.systemNS, collConfigKey(oldName))
		_ = tx.Put(db.systemNS, collConfigKey(newName), cfgData)
	}

	// Rename index keys
	oldPrefix := indexKeyPrefix(oldName)
	cursor := tx.NewCursor(db.systemNS)
	defer cursor.Close()
	if err := cursor.Seek([]byte(oldPrefix)); err != nil {
		return nil
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
