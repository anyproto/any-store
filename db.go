package anystore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/internal/durability"
	"github.com/anyproto/any-store/internal/durability/sentinel"
	"github.com/anyproto/any-store/internal/objectid"
	"github.com/anyproto/any-store/internal/registry"
	"github.com/anyproto/any-store/syncpool"
)

const systemNamespace = "_system"

// DB represents a document-oriented database.
type DB interface {
	// CreateCollection creates a new collection with the specified name.
	// Returns the created Collection or an error if the collection already exists.
	// Possible errors:
	// - ErrCollectionExists: if the collection already exists.
	CreateCollection(ctx context.Context, collectionName string) (Collection, error)

	// OpenCollection opens an existing collection with the specified name.
	// Returns the opened Collection or an error if the collection does not exist.
	// Possible errors:
	// - ErrCollectionNotFound: if the collection does not exist.
	OpenCollection(ctx context.Context, collectionName string) (Collection, error)

	// Collection is a convenience method to get or create a collection.
	// It first attempts to open the collection, and if it does not exist, it creates the collection.
	// Returns the Collection or an error if there is an issue creating or opening the collection.
	Collection(ctx context.Context, collectionName string) (Collection, error)

	// GetCollectionNames returns a list of all collection names in the database.
	// Returns a slice of collection names or an error if there is an issue retrieving the names.
	GetCollectionNames(ctx context.Context) ([]string, error)

	// Stats returns the statistics of the database.
	// Returns a DBStats struct containing the database statistics or an error if there is an issue retrieving the stats.
	Stats(ctx context.Context) (DBStats, error)

	// QuickCheck performs a quick integrity check. If result not ok returns error.
	QuickCheck(ctx context.Context) (err error)

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

	registryBufSize := 4
	ds := &db{
		instanceId:        objectid.NewObjectID().Hex(),
		config:            config,
		syncPool:          sPool,
		filterReg:         registry.NewFilterRegistry(sPool, registryBufSize),
		sortReg:           registry.NewSortRegistry(sPool, registryBufSize),
		openedCollections: make(map[string]Collection),
	}

	var quickCheckNeeded bool
	ds.recoveryController, quickCheckNeeded = ds.createRecoveryController(ctx, path)

	opts := btree.Options{
		PageSize:     4096,
		CacheSize:    5000,
		InProcess:    false,
		NoCommitSync: config.NoCommitSync,
		InMemory:     config.InMemory,
	}

	var err error
	if ds.btreeDB, err = btree.Open(path, opts); err != nil {
		return nil, err
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
	filterReg          *registry.FilterRegistry
	sortReg            *registry.SortRegistry

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

type indexMeta struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
	Sparse bool     `json:"sparse"`
	Unique bool     `json:"unique"`
}

func indexKey(collName, indexName string) []byte {
	return []byte("idx:" + collName + ":" + indexName)
}

func indexKeyPrefix(collName string) string {
	return "idx:" + collName + ":"
}

func (db *db) init(ctx context.Context) error {
	return db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		// Ensure system namespace exists
		ns, err := db.btreeDB.GetNamespace(systemNamespace)
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

func (db *db) CreateCollection(ctx context.Context, collectionName string) (Collection, error) {
	return db.createCollection(ctx, collectionName)
}

func (db *db) createCollection(ctx context.Context, collectionName string) (Collection, error) {
	db.mu.Lock()
	if _, ok := db.openedCollections[collectionName]; ok {
		db.mu.Unlock()
		return nil, ErrCollectionExists
	}
	db.mu.Unlock()
	var coll Collection
	err := db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		db.mu.Lock()
		defer db.mu.Unlock()

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

		if coll, err = newCollection(ctx, db, collectionName); err != nil {
			return err
		}
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
	defer db.mu.Unlock()
	return db.openCollection(ctx, collectionName)
}

func (db *db) openCollection(ctx context.Context, collectionName string) (Collection, error) {
	coll, ok := db.openedCollections[collectionName]
	if ok {
		return coll, nil
	}

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

	coll, err = newCollection(ctx, db, collectionName)
	if err != nil {
		return nil, err
	}
	db.openedCollections[collectionName] = coll
	return coll, nil
}

func (db *db) Collection(ctx context.Context, collectionName string) (Collection, error) {
	coll, err := db.OpenCollection(ctx, collectionName)
	if err == nil {
		return coll, nil
	}
	if !errors.Is(err, ErrCollectionNotFound) {
		return nil, err
	}
	coll, err = db.createCollection(ctx, collectionName)
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
	if fi, fErr := os.Stat(db.btreeDB.Path()); fErr == nil {
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

func (db *db) Backup(ctx context.Context, path string) (err error) {
	// Read the source file and copy it
	srcPath := db.btreeDB.Path()

	// Checkpoint first to ensure all WAL data is in the main file
	if err = db.btreeDB.Checkpoint(); err != nil {
		return err
	}

	srcData, err := os.ReadFile(srcPath)
	if err != nil {
		return err
	}
	return os.WriteFile(path, srcData, 0644)
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

// getIndexInfos reads all index metadata for a collection from the system namespace
func (db *db) getIndexInfos(tx *btree.ReadTx, collName string) ([]IndexInfo, error) {
	prefix := indexKeyPrefix(collName)
	cursor := tx.NewCursor(db.systemNS)
	defer cursor.Close()
	if err := cursor.Seek([]byte(prefix)); err != nil {
		return nil, nil
	}
	var result []IndexInfo
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
		var meta indexMeta
		if err := json.Unmarshal(val, &meta); err != nil {
			return nil, err
		}
		result = append(result, IndexInfo{
			Name:   meta.Name,
			Fields: meta.Fields,
			Sparse: meta.Sparse,
			Unique: meta.Unique,
		})
		if err := cursor.Next(); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// registerIndex stores index metadata in the system namespace
func (db *db) registerIndex(tx *btree.WriteTx, collName string, info IndexInfo) error {
	key := indexKey(collName, info.Name)
	// Check if already exists
	if _, err := tx.Get(db.systemNS, key); err == nil {
		return ErrIndexExists
	}
	meta := indexMeta{
		Name:   info.Name,
		Fields: info.Fields,
		Sparse: info.Sparse,
		Unique: info.Unique,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return tx.Put(db.systemNS, key, data)
}

// removeIndex removes index metadata from the system namespace
func (db *db) removeIndex(tx *btree.WriteTx, collName, indexName string) error {
	key := indexKey(collName, indexName)
	return tx.Delete(db.systemNS, key)
}

// removeCollection removes collection metadata from the system namespace
func (db *db) removeCollection(tx *btree.WriteTx, collName string) error {
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
	return nil
}

// renameCollection renames collection metadata in the system namespace
func (db *db) renameCollection(tx *btree.WriteTx, oldName, newName string) error {
	// Remove old collection key, add new one
	if err := tx.Delete(db.systemNS, collKey(oldName)); err != nil {
		return err
	}
	if err := tx.Put(db.systemNS, collKey(newName), []byte("1")); err != nil {
		return err
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
		meta   indexMeta
	}
	var entries []kv
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
		var meta indexMeta
		if err := json.Unmarshal(val, &meta); err != nil {
			return err
		}
		entries = append(entries, kv{oldKey: append([]byte(nil), key...), meta: meta})
		if err := cursor.Next(); err != nil {
			return err
		}
	}
	for _, e := range entries {
		if err := tx.Delete(db.systemNS, e.oldKey); err != nil {
			return err
		}
		newKey := indexKey(newName, e.meta.Name)
		data, err := json.Marshal(e.meta)
		if err != nil {
			return err
		}
		if err := tx.Put(db.systemNS, newKey, data); err != nil {
			return err
		}
	}
	return nil
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
