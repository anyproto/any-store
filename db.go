package anystore

import (
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
	// With a transaction in ctx the collection is looked up in that
	// transaction's view.
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
		byIdentity:        make(map[string]*collection),
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
		if errors.Is(err, btree.ErrSQLiteFormat) {
			return nil, ErrV1Database
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
	_, cookie := ds.btreeDB.LocalCounters()
	ds.epoch.Store(&schemaEpoch{gen: 1, start: cookie, known: cookie})

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
		if err = ds.recoveryController.Start(); err != nil {
			_ = ds.btreeDB.Close()
			return nil, err
		}
	}

	// Bring range indexes older releases built up to the current format
	// before any handle exists (see upgradeIndexFormats); a settled file
	// skips the catalog walk. Runs with the recovery controller started: its
	// writes arm the dirty sentinel like any other. Only a cancelled ctx
	// fails here.
	if err = ds.upgradeIndexFormats(ctx); err != nil {
		if ds.recoveryController != nil {
			_ = ds.recoveryController.Stop()
		}
		_ = ds.btreeDB.Close()
		return nil, err
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

	// schemaGate serializes the end of a schema-changing commit against the
	// next write tx. btree WriteTx.Commit releases the global write lock
	// before it returns, and only then does the commit withdraw the cookie
	// it announced (observeCookie) when it published nothing, or discard
	// the schema log it did not publish: a writer beginning in that gap
	// would take another process's commit at the announced cookie for this
	// one's, or race the discard on the handles' marks. A tx that announced
	// or logged holds the gate across btree Commit + that; newWriteTx
	// passes through it once after acquiring the write lock. Plain data txs
	// skip it.
	// The rollback and savepoint paths need no gate: they discard the log
	// while the btree write lock is still held.
	schemaGate sync.Mutex

	openedCollections map[string]Collection
	// byIdentity holds the registered handles by the collection they stand
	// for (collection.identity): one collection has one handle, whatever
	// names transactions at different snapshots know it by — the one of an
	// older snapshot, or the one the open write tx just gave it. Under mu,
	// like openedCollections.
	byIdentity map[string]*collection

	// epoch is what this process knows of the schema cookie's history, and
	// ownCookie the cookie a commit of this process is producing right now
	// (flag in the high half); see schemaEpoch and observeCookie. settling
	// is that cookie (flag likewise) from the commit's first change to a
	// handle until the epoch is past it (commonTx.schemaCommitted); see
	// sinceNow.
	epoch     atomic.Pointer[schemaEpoch]
	ownCookie atomic.Uint64
	settling  atomic.Uint64
	// sketchEpoch counts the commits of other processes this one noticed:
	// each may have moved the sketches of any collection (see
	// collection.refreshSketches).
	sketchEpoch atomic.Uint64

	// sketchDirty and ftsDirty list the collections whose writer state the
	// end of a transaction has to visit: an index whose live sketch may hold
	// unpersisted deltas (index.sketchModified), and buffered full-text
	// postings (ftsIndex.pending). A collection joins when that state is
	// created, so begin, commit, rollback and savepoints visit only what was
	// written — the analog of SQLite's db->aVTrans, the virtual tables that
	// joined the transaction (vtab.c).
	//
	// ftsDirty is emptied whenever the whole list is flushed or discarded,
	// not at transaction end as aVTrans is: a verb in a long tx visits the
	// non-empty buffers only.
	//
	// sketchDirty is a superset: a commit persists the listed collections
	// and removes none; a collection leaves at the next write-tx begin,
	// which also rebases what a rollback left. SQLite drops such state at
	// rollback; here the committed sketch bytes are readable only once the
	// btree rollback has released the write lock.
	//
	// Writer-owned, no mutex: every access holds the btree write lock, or
	// schemaGate while a failed commit discards its log (newWriteTx passes
	// the gate before reading them). Nothing touches them after a btree
	// commit succeeded. A closed collection stays listed: postings buffered
	// before a Close() in mid-tx still flush at commit (guarded by
	// TestFtsPendingSurvivesCollectionCloseMidTx).
	sketchDirty []*collection
	ftsDirty    []*collection

	closed atomic.Bool

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
// ("coll:", "collcfg:", "idx:", "stat_data:", "idx_mk:"; "idx_format" is a
// single record) so families can't cross-collide, and the residual within-family ambiguity (collection "A:b"
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

	// Pass the schema gate (see its field comment): a prior schema-changing
	// tx may still be settling its log after the write lock was released;
	// block until the registry is consistent with the catalog.
	db.schemaGate.Lock()
	db.schemaGate.Unlock() //lint:ignore SA2001 gate pass-through, not a critical section

	version := newTxVersion()
	tx := txPool.Get().(*commonTx)
	tx.db = db
	tx.readTx = &btWtx.ReadTx
	tx.writeTx = btWtx
	tx.modified = false
	tx.savepoints = tx.savepoints[:0]
	btWtx.SetAux(&tx.schema)

	db.checkStale(&btWtx.ReadTx)
	db.resetUncommittedSketches(&btWtx.ReadTx)
	db.resetAllFtsPending(&btWtx.ReadTx)

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
	btRtx.SetAux(&tx.schema)
	rTx := readTx{commonTx: tx, version: version}
	tx.ctx = context.WithValue(ctx, ctxKeyTx, rTx)
	return rTx, nil
}

// checkStale brings what this process keeps in memory up to the snapshot a
// transaction begins at. It runs at the start of every top-level read and
// write transaction (the analog of SQLite verifying the schema cookie in
// OP_Transaction before a statement runs).
//
// Two tiers, mirroring SQLite's sqlite3InitOne (structure) running before
// sqlite3AnalysisLoad (statistics):
//
//	STRUCTURAL (correctness): a schema cookie this process did not produce is
//	  another process's schema change. observeCookie starts a new generation
//	  for it, and that is all: which collection changed the cookie does not
//	  say, so each handle is verified against the catalog by the first
//	  transaction of the generation that uses it (collection.resolve). A
//	  stale schema is never used; nothing is read for the handles nobody
//	  touches.
//
//	STATISTICAL (advisory): a commit of another process may have moved the
//	  selectivity sketches of any collection. sketchEpoch records that one
//	  was noticed, and each handle reloads its sketches when a transaction
//	  next plans with them or writes through it (collection.refreshSketches).
//	  A stale sketch only affects which index the planner CHOOSES, never
//	  query RESULTS (the any-store analog of sqlite_stat1).
//
// Neither tier reads anything here: what a begin costs does not depend on how
// many collections are open.
//
// The begin-time disk counters driving the sketch verdict are RAISED to the
// newest committed frame (see btree.ReadTx.SnapshotHeaderCounters), so a read
// tx can detect staleness its own snapshot does not yet contain. The pass
// consumes only up to the SNAPSHOT counters; left short, the verdict stays
// stale and the next begin converges.
//
// A verdict a commit of THIS process produced — it landed between the begin's
// read of the local counters and its read of the disk ones — is none: the
// writer published its sketches in-process and recorded the committed
// counters. The pass returns once the local counters have reached the
// counters this tx saw on disk (LocalCaughtUp), and the consumption below
// never moves them back (AdvanceLocalCounters).
func (db *db) checkStale(tx *btree.ReadTx) {
	db.observeCookie(tx)
	if !tx.IsSchemaStale() && !tx.IsDataStale() {
		return
	}
	if tx.LocalCaughtUp() {
		return
	}
	db.sketchEpoch.Add(1)
	db.btreeDB.AdvanceLocalCounters(tx.SnapshotFileChangeCounter(), tx.SnapshotSchemaCookie())
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
//
// Only db.sketchDirty is visited, and this is where a collection leaves it:
// once closed, or once none of its indexes is flagged. An index with no
// committed bytes keeps its flag through the rebase and stays listed for the
// next commit to persist.
func (db *db) resetUncommittedSketches(tx *btree.ReadTx) {
	kept := db.sketchDirty[:0]
	gen := db.epoch.Load().gen
	for _, c := range db.sketchDirty {
		flagged := false
		// Only a head verified for the present is rebased: another
		// process may have redefined the index whose bytes the stale name
		// finds, and the live object is shared with the readers' versions.
		if s := c.cur(); !c.closed.Load() && s != nil && s.gen.Load() == gen {
			c.mu.Lock()
			for _, idx := range s.indexes {
				if idx.sketchModified {
					// reloadSketch (writable) rebases live to the committed bytes and
					// clears sketchModified; if there are no committed bytes yet
					// (brand-new pre-commit index) it preserves the built sketch.
					c.reloadSketch(tx, s.name, idx, true)
					flagged = flagged || idx.sketchModified
				}
			}
			c.mu.Unlock()
		} else if s != nil && !c.closed.Load() {
			for _, idx := range s.indexes {
				flagged = flagged || idx.sketchModified
			}
		}
		if flagged {
			kept = append(kept, c)
		} else {
			c.sketchDirty = false
		}
	}
	clear(db.sketchDirty[len(kept):])
	db.sketchDirty = kept
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
	if _, ok := db.ambientWriteTx(ctx); !ok {
		// A live handle whose head is verified for the present — the
		// current generation, under this name — answers for the name at
		// once. One without a head (opened through an old snapshot, see
		// resolveSlow), one another process's change left unverified, or
		// one a commit renamed and not yet re-keyed (settleLog) proves
		// nothing: the catalog check inside the tx decides. Inside a write
		// tx the catalog alone decides: the registry holds the committed
		// names, and the tx may have dropped or renamed the collection
		// under this one.
		db.mu.Lock()
		if existing, ok := db.openedCollections[collectionName]; ok && existing.(*collection).answersFor(collectionName) {
			db.mu.Unlock()
			return nil, ErrCollectionExists
		}
		db.mu.Unlock()
	}
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

		c, schema, err := newCollection(db, collectionName, &tx.ReadTx)
		if err != nil {
			return err
		}
		// The collection exists from the cookie this tx commits with, and
		// so does its handle: in the transaction's log until then, where
		// the writer finds it by name, out of the registry (settleLog puts
		// it there; a rollback closes it).
		c.logCreate(wtx, schema)
		coll = c
		return nil
	})
	if err != nil {
		return nil, err
	}
	return coll, nil
}

func (db *db) OpenCollection(ctx context.Context, collectionName string) (Collection, error) {
	coll, err := db.openCollection(ctx, collectionName)
	if err != nil {
		return nil, err
	}
	return db.handOut(coll), nil
}

// handOut returns coll to a caller that keeps it. That cancels a Close() an
// uncommitted schema change deferred (see collection.closePending): the
// handle has a user again. An open the library makes for itself, or one that
// ends in an error, hands nothing out. The unlocked read may miss a Close()
// racing with the open; that close then stands, as if it came second.
func (db *db) handOut(coll Collection) Collection {
	if c := coll.(*collection); c.closePending.Load() {
		db.mu.Lock()
		c.closePending.Store(false)
		db.mu.Unlock()
	}
	return coll
}

// registered returns the live handle the registry holds under name. The
// registry is keyed by committed names: a collection renamed by the open
// write tx stays under its old name until the commit, which is right for
// every caller but that tx — for it the old name is gone, as a table's old
// name is after ALTER TABLE RENAME in the same SQLite transaction, which the
// name check of resolveIn answers (the version the writer resolves has the
// new name).
func (db *db) registered(name string) (Collection, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.liveLocked(name)
}

// openCollection returns the handle of the collection as the caller of ctx
// has it: with a transaction in ctx, the collection must be in that
// transaction's view under that name. The handle is the one the open write
// tx has under the name in its log, the registered one, or a new one loaded
// through the caller's view — its own transaction or a short read of the
// newest state.
func (db *db) openCollection(ctx context.Context, collectionName string) (Collection, error) {
	for attempt := 0; ; attempt++ {
		coll, err := db.openCollectionOnce(ctx, collectionName)
		if err != errHandleGone {
			return coll, err
		}
		// The registered handle was closed or retired while it was resolved
		// for this caller: the name is free for the collection that has it
		// now, if any.
		if attempt == 2 {
			return nil, ErrCollectionNotFound
		}
	}
}

// testHookBeforeRegister, when set, runs between the load of a new handle and
// its registration. Tests only.
var testHookBeforeRegister func(name string)

// errHandleGone tells openCollection that the handle it found registered is
// not live any more.
var errHandleGone = errors.New("any-store: registered handle is gone")

func (db *db) openCollectionOnce(ctx context.Context, collectionName string) (Collection, error) {
	if wtx, ok := db.ambientWriteTx(ctx); ok {
		// The write tx's own uncommitted DDL first: a collection it created
		// or renamed to the name has its handle in the log and nowhere
		// else; one it dropped under the name is gone for it.
		if e := wtx.schemaLog().byName(collectionName); e != nil {
			if e.kind == logDrop {
				return nil, ErrCollectionNotFound
			}
			if err := e.c.alive(); err != nil {
				return nil, err
			}
			return e.c, nil
		}
	}
	if coll, ok := db.registered(collectionName); ok {
		return coll, db.resolveOpened(ctx, coll.(*collection), collectionName)
	}

	var opened Collection
	err := db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		// Through an ambient write tx this is the WRITER'S view: the tx's own
		// uncommitted DDL is visible only there.
		c, schema, err := newCollection(db, collectionName, tx)
		if err != nil {
			return err
		}
		if testHookBeforeRegister != nil {
			testHookBeforeRegister(collectionName)
		}
		db.mu.Lock()
		if live, ok := db.liveLocked(collectionName); ok {
			db.mu.Unlock()
			opened = live
			return db.resolveIn(tx, live.(*collection), collectionName)
		}
		// The collection tx has under this name has a handle already, under
		// another name: the one the newest state gives it, or the open write
		// tx. That is its handle; a second one would not follow the schema
		// changes made through the first.
		if other := db.byIdentity[c.identity]; other != nil && !other.closed.Load() {
			db.mu.Unlock()
			opened = other
			return db.resolveIn(tx, other, collectionName)
		}
		if existing, ok := db.openedCollections[collectionName]; ok {
			// A closed handle still in the slot: a drop that is published
			// but not settled (settleLog). A caller whose snapshot still
			// has that collection gets the closed handle (fail-safe: its
			// operations fail), not a second one; a caller that has
			// another collection under the name takes the slot.
			if existing.(*collection).identity == c.identity {
				db.mu.Unlock()
				opened = existing
				return nil
			}
			db.forgetLocked(existing.(*collection))
		}
		c.since = db.sinceNow()
		db.openedCollections[collectionName] = c
		db.byIdentity[c.identity] = c
		db.mu.Unlock()
		opened = c
		c.adopt(tx, schema)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return opened, nil
}

// resolveOpened answers an open that found c registered. With a transaction
// in ctx the collection must be in its view under name. Without one a handle
// whose head is verified for the present under the name is the answer as it
// is; any other — none installed yet, unverified since another process's
// change, renamed by a commit not yet settled — is resolved through a short
// read first, so a handle left behind never answers for a collection that
// is gone or called otherwise.
func (db *db) resolveOpened(ctx context.Context, c *collection, name string) error {
	if ctx.Value(ctxKeyTx) == nil && c.answersFor(name) {
		return nil
	}
	return db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		return db.resolveIn(tx, c, name)
	})
}

// resolveIn reports whether tx has the collection of c under name. The name
// is also where a transaction older than the handle looks the collection up
// when the handle itself does not know what it was called then.
func (db *db) resolveIn(tx *btree.ReadTx, c *collection, name string) error {
	if tx.IsWriteTx() {
		// Dropped by the write tx itself: not under this name, or any.
		if e := c.logged(tx); e != nil && e.kind == logDrop {
			return ErrCollectionNotFound
		}
	}
	s, err := c.resolveAs(tx, name)
	if err != nil {
		if c.closed.Load() && !db.closed.Load() {
			return errHandleGone
		}
		return err
	}
	if s.name != name {
		// The collection had another name at this snapshot.
		return ErrCollectionNotFound
	}
	return nil
}

// liveLocked returns the live handle registered under name. The caller holds
// db.mu.
func (db *db) liveLocked(name string) (Collection, bool) {
	coll, ok := db.openedCollections[name]
	if !ok || coll.(*collection).closed.Load() {
		return nil, false
	}
	return coll, true
}

func (db *db) Collection(ctx context.Context, collectionName string, opts ...CollectionOptions) (Collection, error) {
	coll, err := db.collection(ctx, collectionName, opts...)
	if err != nil {
		return nil, err
	}
	return db.handOut(coll), nil
}

// collection opens or creates the collection without handing it out (see
// handOut): for a caller inside the library that keeps no handle.
func (db *db) collection(ctx context.Context, collectionName string, opts ...CollectionOptions) (Collection, error) {
	coll, err := db.openCollection(ctx, collectionName)
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
	return db.openCollection(ctx, collectionName)
}

func (db *db) GetCollectionNames(ctx context.Context) (collectionNames []string, err error) {
	err = db.doReadTx(ctx, func(tx *btree.ReadTx) (err error) {
		collectionNames, err = db.collectionNames(tx)
		return err
	})
	return
}

// collectionNames lists the collections the catalog holds in this view. A
// Seek error is a read failure, not an empty catalog (an empty or
// non-matching prefix leaves the cursor invalid with a nil error).
func (db *db) collectionNames(tx *btree.ReadTx) (collectionNames []string, err error) {
	cursor := tx.NewCursor(db.systemNS)
	defer cursor.Close()
	prefix := []byte("coll:")
	if err = cursor.Seek(prefix); err != nil {
		return nil, err
	}
	for cursor.Valid() {
		key, err := cursor.Key()
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(string(key), "coll:") {
			break
		}
		collectionNames = append(collectionNames, string(key[5:]))
		if err = cursor.Next(); err != nil {
			return nil, err
		}
	}
	return collectionNames, nil
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
	// old); its data survives as an orphan
	// namespace but the collection is unopenable until manually repaired.
	return db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		names, lErr := db.listCollectionNames(tx)
		if lErr != nil {
			return lErr
		}
		for _, name := range names {
			if _, nsErr := tx.GetNamespace(name); nsErr != nil {
				if errors.Is(nsErr, btree.ErrNamespaceNotFound) {
					return fmt.Errorf("collection %q: data namespace missing (renamed by a pre-fix version); its data survives under the pre-rename namespace name", name)
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
			break
		}
		if serr != nil {
			return serr
		}
	}
	if err = b.Finish(); err != nil {
		return err
	}
	// The copy is a database of its own for the format settled record (see
	// dropFormatSettled).
	return dropFormatSettled(dstDB)
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

// doWriteTxW is doWriteTx for DDL callbacks, which log their schema changes
// on the wrapper tx (txSchema.log) so a rollback of this scope — or of any
// enclosing tx — discards them with the reverted on-disk catalog.
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
	// TestTxPanic_UpsertIdDoesNotWedgeDB in any-store-tests:apitest.
	//
	// Deliberately not armed across Commit: both commit layers mark themselves
	// done before doing any work (writeTx.Commit consumes the version CAS on
	// entry and pools the commonTx; btree.WriteTx.Commit sets closed before
	// pager.commit), so a rollback once commit has begun is a silent no-op on an
	// already-recycled tx. A commit-time panic still leaks the lock — that needs
	// the release moved into a defer inside btree.WriteTx.Commit (the abandoned-tx
	// case is guarded by btree's TestWriteTxAbandonedDoesNotDeadlockClose).
	//
	// The rollback discards the schema log under db.mu: a callback must not
	// panic while holding it with an explicit (non-defer) unlock, or the
	// unwind deadlocks here.
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
		if cErr := c.(*collection).close(true); cErr != nil {
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

// evictLocked removes c from the registry. Identity scan, not a delete by
// name: the slot may be under an older name than the head's (a handle
// opened through an old snapshot, a rename being settled). The caller holds
// db.mu.
func (db *db) evictLocked(c *collection) {
	for name, cur := range db.openedCollections {
		if cur == Collection(c) {
			delete(db.openedCollections, name)
			break
		}
	}
	db.forgetLocked(c)
}

// forgetLocked takes c out of byIdentity. The caller holds db.mu.
func (db *db) forgetLocked(c *collection) {
	if db.byIdentity[c.identity] == c {
		delete(db.byIdentity, c.identity)
	}
}

// persistAllDirtySketches writes the modified sketches of the collections in
// db.sketchDirty, each under the version the transaction has for it. Called
// once per write transaction commit to batch sketch persistence. It removes
// nothing from the list: the next write-tx begin does
// (resetUncommittedSketches).
func (db *db) persistAllDirtySketches(tx *btree.WriteTx) error {
	gen := db.epoch.Load().gen
	for _, c := range db.sketchDirty {
		if c.closed.Load() {
			continue
		}
		var s *collSchema
		if e := c.logged(&tx.ReadTx); e != nil {
			// A version this transaction logged is its own, made from its
			// verified view; its generation is stamped at the install.
			// Dropped in this tx: the stat_data rows were deleted with the
			// collection — persisting the still-dirty sketches would
			// durably resurrect orphaned rows a later same-named index
			// would adopt.
			if e.kind == logDrop {
				continue
			}
			s = e.s
		} else if s = c.cur(); s == nil || s.gen.Load() != gen {
			// A head another process's change left unverified: its name
			// and indexes may be gone; the flags wait for the handle to be
			// verified.
			continue
		}
		if err := c.persistSketches(tx, s); err != nil {
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

// flushAllFtsPending flushes the full-text write-back buffers of the
// collections in db.ftsDirty into the B-tree. Called once per write-tx commit,
// BEFORE the btree commit, so the buffered postings land in the SAME atomic
// transaction as the document writes — preserving strong cross-process
// consistency (another process opening a read tx after commit sees a
// complete, consistent index; a crash leaves no doc without its postings).
// The buffer never survives the commit boundary. A failed flush leaves the
// list as it was, for the rollback to discard.
func (db *db) flushAllFtsPending(tx *btree.WriteTx) error {
	if len(db.ftsDirty) == 0 {
		return nil
	}
	for _, c := range db.ftsDirty {
		// The version the tx has for the collection: an index it created
		// holds postings too, one it dropped holds none (Drop resets them).
		s, dropped := c.inTx(&tx.ReadTx)
		if dropped || s == nil {
			continue
		}
		for _, fx := range s.ftsIndexes {
			if err := fx.flushPending(tx); err != nil {
				return err
			}
		}
	}
	db.clearFtsDirty()
	return nil
}

// resetAllFtsPending discards the buffered full-text writes of the
// collections in db.ftsDirty: when the tx or one of its savepoints rolls
// back, and at the start of every write tx, which finds the list empty unless
// a tx ended without either. tx is the write transaction, for the versions
// it has (flushAllFtsPending).
func (db *db) resetAllFtsPending(tx *btree.ReadTx) {
	if len(db.ftsDirty) == 0 {
		return
	}
	for _, c := range db.ftsDirty {
		s, _ := c.inTx(tx)
		if s == nil {
			continue
		}
		for _, fx := range s.ftsIndexes {
			fx.pending.reset()
		}
	}
	db.clearFtsDirty()
}

// clearFtsDirty empties db.ftsDirty once its buffers are flushed or reset.
func (db *db) clearFtsDirty() {
	for i, c := range db.ftsDirty {
		c.ftsDirty = false
		db.ftsDirty[i] = nil
	}
	db.ftsDirty = db.ftsDirty[:0]
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
	} else {
		// The format the entries are built under; see indexFormatVersion.
		obj.Set("v", a.NewNumberInt(indexFormatVersion))
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
	// a contents-less collection would recreate the pre-fix half-rename breakage.
	if err = tx.RenameNamespace(oldName, newName); err != nil {
		if errors.Is(err, btree.ErrNamespaceExists) {
			return fmt.Errorf("rename %q -> %q: target namespace already exists: %w", oldName, newName, ErrCollectionExists)
		}
		if errors.Is(err, btree.ErrNamespaceNotFound) {
			return fmt.Errorf("rename %q -> %q: data namespace missing (collection likely broken by a pre-fix rename): %w", oldName, newName, err)
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
