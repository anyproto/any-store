package anystore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"zombiezen.com/go/sqlite"

	"github.com/anyproto/any-store/internal/driver"
	"github.com/anyproto/any-store/internal/objectid"
	"github.com/anyproto/any-store/internal/registry"
	"github.com/anyproto/any-store/internal/sql"
	"github.com/anyproto/any-store/syncpool"
)

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

	// QuickCheck performs PRAGMA quick_check to sqlite. If result not ok returns error.
	QuickCheck(ctx context.Context) (err error)

	// Checkpoint performs PRAGMA wal_checkpoint to sqlite. isFull=true - wal_checkpoint(FULL), isFull=false - wal_checkpoint(PASSIVE);
	Checkpoint(ctx context.Context, isFull bool) (err error)

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

	registryBufSize := (config.ReadConnections + 1) * 4
	ds := &db{
		instanceId:        objectid.NewObjectID().Hex(),
		config:            config,
		syncPool:          sPool,
		filterReg:         registry.NewFilterRegistry(sPool, registryBufSize),
		sortReg:           registry.NewSortRegistry(sPool, registryBufSize),
		openedCollections: make(map[string]Collection),
	}

	var err error
	conf := driver.Config{
		Pragma:                    config.pragma(),
		ReadCount:                 config.ReadConnections,
		PreAllocatedPageCacheSize: config.SQLiteGlobalPageCachePreallocateSizeBytes,
		SortRegistry:              ds.sortReg,
		FilterRegistry:            ds.filterReg,
		Version:                   2,
		ReadConnTTL:               time.Minute,
	}
	if ds.cm, err = driver.NewConnManager(path, conf); err != nil {
		return nil, err
	}
	if err = ds.init(ctx); err != nil {
		_ = ds.cm.Close()
		return nil, err
	}
	return ds, nil
}

type db struct {
	instanceId string

	config *Config

	cm        *driver.ConnManager
	filterReg *registry.FilterRegistry
	sortReg   *registry.SortRegistry

	syncPool *syncpool.SyncPool

	sql  sql.DBSql
	stmt struct {
		registerCollection,
		removeCollection,
		renameCollection,
		renameCollectionIndex,
		registerIndex,
		removeIndex *driver.Stmt
	}

	openedCollections map[string]Collection
	closed            atomic.Bool
	mu                sync.Mutex
}

func (db *db) init(ctx context.Context) error {
	return db.doWriteTx(ctx, func(c *driver.Conn) (err error) {
		if err = c.ExecNoResult(ctx, db.sql.InitDB()); err != nil {
			return
		}
		if db.stmt.registerCollection, err = c.Prepare(db.sql.RegisterCollectionStmt()); err != nil {
			return
		}
		if db.stmt.removeCollection, err = c.Prepare(db.sql.RemoveCollectionStmt()); err != nil {
			return
		}
		if db.stmt.renameCollection, err = c.Prepare(db.sql.RenameCollectionStmt()); err != nil {
			return
		}
		if db.stmt.renameCollectionIndex, err = c.Prepare(db.sql.RenameCollectionIndexStmt()); err != nil {
			return
		}
		if db.stmt.registerIndex, err = c.Prepare(db.sql.RegisterIndexStmt()); err != nil {
			return
		}
		if db.stmt.removeIndex, err = c.Prepare(db.sql.RemoveIndexStmt()); err != nil {
			return
		}
		return
	})
}

func (db *db) newWriteTx(ctx context.Context) (WriteTx, error) {
	connWrite, err := db.cm.GetWrite(ctx)
	if err != nil {
		return nil, err
	}

	if err = connWrite.BeginImmediate(ctx); err != nil {
		db.cm.ReleaseWrite(connWrite)
		return nil, err
	}

	version := newTxVersion()
	tx := txPool.Get().(*commonTx)
	tx.db = db
	tx.con = connWrite
	tx.version.Store(version)
	wTx := writeTx{commonTx: tx, version: version}
	tx.ctx = context.WithValue(ctx, ctxKeyTx, wTx)
	return wTx, nil
}

func (db *db) ReadTx(ctx context.Context) (ReadTx, error) {
	connRead, err := db.cm.GetRead(ctx)
	if err != nil {
		return nil, err
	}

	if err = connRead.Begin(ctx); err != nil {
		db.cm.ReleaseRead(connRead)
		return nil, err
	}

	version := newTxVersion()
	tx := txPool.Get().(*commonTx)
	tx.db = db
	tx.con = connRead
	tx.version.Store(version)
	rTx := readTx{commonTx: tx, version: version}
	tx.ctx = context.WithValue(ctx, ctxKeyTx, rTx)
	return rTx, nil
}

func (db *db) CreateCollection(ctx context.Context, collectionName string) (Collection, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.createCollection(ctx, collectionName)
}

func (db *db) createCollection(ctx context.Context, collectionName string) (Collection, error) {
	if _, ok := db.openedCollections[collectionName]; ok {
		return nil, ErrCollectionExists
	}
	err := db.doWriteTx(ctx, func(c *driver.Conn) error {
		err := db.stmt.registerCollection.Exec(ctx, func(stmt *sqlite.Stmt) {
			stmt.BindText(1, collectionName)
		}, driver.StmtExecNoResults)
		if err != nil {
			return replaceUniqErr(err, ErrCollectionExists)
		}

		if err = c.ExecNoResult(ctx, db.sql.Collection(collectionName).Create()); err != nil {
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
	db.openedCollections[collectionName] = coll
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

	err := db.doReadTx(ctx, func(c *driver.Conn) error {
		return c.Exec(ctx, db.sql.FindCollection(), func(stmt *sqlite.Stmt) {
			stmt.BindText(1, collectionName)
		}, func(stmt *sqlite.Stmt) error {
			hasRow, stepErr := stmt.Step()
			if stepErr != nil {
				return nil
			}
			if !hasRow {
				return ErrCollectionNotFound
			}
			return nil
		})
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
	db.mu.Lock()
	defer db.mu.Unlock()
	coll, err = db.createCollection(ctx, collectionName)
	if err == nil {
		return coll, nil
	}
	if !errors.Is(err, ErrCollectionExists) {
		return nil, err
	}
	return db.openCollection(ctx, collectionName)
}

func (db *db) GetCollectionNames(ctx context.Context) (collectionNames []string, err error) {
	err = db.doReadTx(ctx, func(c *driver.Conn) error {
		return c.ExecCached(ctx, db.sql.FindCollections(), nil, func(stmt *sqlite.Stmt) error {
			for {
				hasRow, stepErr := stmt.Step()
				if stepErr != nil {
					return stepErr
				}
				if !hasRow {
					break
				}
				collectionNames = append(collectionNames, stmt.ColumnText(0))
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return
}

func (db *db) Stats(ctx context.Context) (stats DBStats, err error) {
	err = db.doReadTx(ctx, func(cn *driver.Conn) (txErr error) {
		var getIntByQuery = func(q string) (result int, err error) {
			err = cn.Exec(ctx, q, nil, func(stmt *sqlite.Stmt) error {
				hasRow, stepErr := stmt.Step()
				if !hasRow {
					return nil
				}
				if stepErr != nil {
					return stepErr
				}
				result = stmt.ColumnInt(0)
				return nil
			})
			return
		}
		if stats.CollectionsCount, txErr = getIntByQuery(db.sql.CountCollections()); txErr != nil {
			return
		}
		if stats.IndexesCount, txErr = getIntByQuery(db.sql.CountIndexes()); txErr != nil {
			return
		}
		if stats.TotalSizeBytes, txErr = getIntByQuery(db.sql.StatsTotalSize()); txErr != nil {
			return
		}
		if stats.DataSizeBytes, txErr = getIntByQuery(db.sql.StatsDataSize()); txErr != nil {
			return
		}
		return
	})
	return
}

func (db *db) QuickCheck(ctx context.Context) (err error) {
	return db.doWriteTx(ctx, func(c *driver.Conn) error {
		return c.Exec(ctx, "PRAGMA quick_check", nil, func(stmt *sqlite.Stmt) error {
			hasRow, stepErr := stmt.Step()
			if !hasRow {
				return nil
			}
			if stepErr != nil {
				return stepErr
			}
			result := stmt.ColumnText(0)
			if result != "ok" {
				return fmt.Errorf("quick_check not ok: %s", result)
			}
			return nil
		})
	})
}

func (db *db) Checkpoint(ctx context.Context, isFull bool) (err error) {
	var q = "PRAGMA wal_checkpoint(PASSIVE)"
	if isFull {
		q = "PRAGMA wal_checkpoint(FULL)"
	}
	conn, err := db.cm.GetWrite(ctx)
	if err != nil {
		return err
	}
	defer db.cm.ReleaseWrite(conn)
	return conn.ExecNoResult(ctx, q)
}

func (db *db) Backup(ctx context.Context, path string) (err error) {
	conn, err := db.cm.GetWrite(ctx)
	if err != nil {
		return err
	}
	defer db.cm.ReleaseWrite(conn)
	return conn.Backup(ctx, path)
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

func (db *db) doWriteTx(ctx context.Context, do func(c *driver.Conn) error) error {
	tx, err := db.WriteTx(ctx)
	if err != nil {
		return err
	}
	if err = do(tx.conn()); err != nil {
		err = replaceInterruptErr(err)
		return errors.Join(err, tx.Rollback())
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

func (db *db) doReadTx(ctx context.Context, do func(c *driver.Conn) error) error {
	tx, err := db.getReadTx(ctx)
	if err != nil {
		return err
	}
	if err = do(tx.conn()); err != nil {
		err = replaceInterruptErr(err)
		_ = tx.Commit()
		return err
	}
	return tx.Commit()
}

func (db *db) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return ErrDBIsClosed
	}
	if _, err := db.cm.GetWrite(context.Background()); err != nil {
		return err
	}
	for _, stmt := range []*driver.Stmt{
		db.stmt.registerCollection,
		db.stmt.removeCollection,
		db.stmt.renameCollection,
		db.stmt.renameCollectionIndex,
		db.stmt.registerIndex,
		db.stmt.removeIndex,
	} {
		_ = stmt.Close()
	}

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
	return db.cm.Close()
}

func (db *db) onCollectionClose(name string) {
	db.mu.Lock()
	delete(db.openedCollections, name)
	db.mu.Unlock()
}
