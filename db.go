package anystore

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"

	"github.com/anyproto/any-store/internal/conn"
	"github.com/anyproto/any-store/internal/objectid"
	"github.com/anyproto/any-store/internal/registry"
	"github.com/anyproto/any-store/internal/sql"
	"github.com/anyproto/any-store/internal/syncpool"
)

type DB interface {
	CreateCollection(ctx context.Context, collectionName string) (Collection, error)
	OpenCollection(ctx context.Context, collectionName string) (Collection, error)
	GetCollectionNames(ctx context.Context) (collectionNames []string, err error)

	ReadTx(ctx context.Context) (ReadTx, error)
	WriteTx(ctx context.Context) (WriteTx, error)

	Close() error
}

func Open(ctx context.Context, path string, config *Config) (DB, error) {
	if config == nil {
		config = &Config{}
	}
	config.setDefaults()

	sPool := syncpool.NewSyncPool()

	ds := &db{
		instanceId:        objectid.NewObjectID().Hex(),
		config:            config,
		syncPool:          sPool,
		filterReg:         registry.NewFilterRegistry(sPool),
		sortReg:           registry.NewSortRegistry(sPool),
		openedCollections: make(map[string]Collection),
	}

	var err error
	if ds.cm, err = conn.NewConnManager(
		conn.NewDriver(ds.filterReg, ds.sortReg),
		config.dsn(path),
		1,
		config.ReadConnections,
	); err != nil {
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

	cm        *conn.ConnManager
	filterReg *registry.FilterRegistry
	sortReg   *registry.SortRegistry

	syncPool *syncpool.SyncPool

	sql  sql.DBSql
	stmt struct {
		registerCollection,
		removeCollection,
		renameCollection,
		registerIndex,
		removeIndex conn.Stmt
	}

	openedCollections map[string]Collection

	mu sync.Mutex
}

func (db *db) init(ctx context.Context) error {
	return db.doWriteTx(ctx, func(c conn.Conn) (err error) {
		if _, err = c.ExecContext(ctx, db.sql.InitDB(), nil); err != nil {
			return
		}
		if db.stmt.registerCollection, err = db.sql.RegisterCollectionStmt(ctx, c); err != nil {
			return
		}
		if db.stmt.removeCollection, err = db.sql.RemoveCollectionStmt(ctx, c); err != nil {
			return
		}
		if db.stmt.renameCollection, err = db.sql.RenameCollectionStmt(ctx, c); err != nil {
			return
		}
		if db.stmt.registerIndex, err = db.sql.RegisterIndexStmt(ctx, c); err != nil {
			return
		}
		if db.stmt.removeIndex, err = db.sql.RemoveIndexStmt(ctx, c); err != nil {
			return
		}
		return
	})
}

func (db *db) WriteTx(ctx context.Context) (WriteTx, error) {
	connWrite, err := db.cm.GetWrite(ctx)
	if err != nil {
		return nil, err
	}

	dTx, err := connWrite.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		db.cm.ReleaseWrite(connWrite)
		return nil, err
	}
	return &writeTx{
		readTx{
			commonTx: commonTx{
				db:         db,
				initialCtx: ctx,
				con:        connWrite,
				tx:         dTx,
			},
		},
	}, nil
}

func (db *db) ReadTx(ctx context.Context) (ReadTx, error) {
	connRead, err := db.cm.GetRead(ctx)
	if err != nil {
		return nil, err
	}

	dTx, err := connRead.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		db.cm.ReleaseRead(connRead)
		return nil, err
	}
	return &readTx{
		commonTx: commonTx{
			db:         db,
			initialCtx: ctx,
			con:        connRead,
			tx:         dTx,
		},
	}, nil
}

func (db *db) CreateCollection(ctx context.Context, collectionName string) (Collection, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.openedCollections[collectionName]; ok {
		return nil, ErrCollectionExists
	}

	err := db.doWriteTx(ctx, func(c conn.Conn) error {
		_, err := db.stmt.registerCollection.ExecContext(ctx, []driver.NamedValue{
			{
				Name:    "collName",
				Ordinal: 1,
				Value:   collectionName,
			},
		})
		if err != nil {
			return replaceUniqErr(err, ErrCollectionExists)
		}

		if _, err = c.ExecContext(ctx, db.sql.Collection(collectionName).Create(), nil); err != nil {
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

	coll, ok := db.openedCollections[collectionName]
	if ok {
		return coll, nil
	}

	err := db.doReadTx(ctx, func(c conn.Conn) error {
		rows, err := c.QueryContext(ctx, db.sql.FindCollection(), []driver.NamedValue{{
			Name:    "collName",
			Ordinal: 1,
			Value:   collectionName,
		}})
		if err != nil {
			return replaceNoRowsErr(err, ErrCollectionNotFound)
		}
		return rows.Close()
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

func (db *db) GetCollectionNames(ctx context.Context) (collectionNames []string, err error) {
	err = db.doReadTx(ctx, func(c conn.Conn) error {
		rows, err := c.QueryContext(ctx, db.sql.FindCollections(), nil)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()
		collectionNames, err = readRowsString(rows)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return
}

func (db *db) getWriteTx(ctx context.Context) (tx WriteTx, err error) {
	ctxTx := ctx.Value(ctxKeyTx)
	if ctxTx == nil {
		return db.WriteTx(ctx)
	}

	var ok bool
	if tx, ok = ctxTx.(WriteTx); ok {
		if tx.instanceId() != db.instanceId {
			return nil, ErrTxOtherInstance
		}
		return noOpTx{ReadTx: tx}, nil
	}
	return nil, ErrTxIsReadOnly
}

func (db *db) doWriteTx(ctx context.Context, do func(c conn.Conn) error) error {
	tx, err := db.getWriteTx(ctx)
	if err != nil {
		return err
	}
	if err = do(tx.conn()); err != nil {
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
		if tx.instanceId() != db.instanceId {
			return nil, ErrTxOtherInstance
		}
		return noOpTx{ReadTx: tx}, nil
	}
	return nil, ErrTxIsReadOnly
}

func (db *db) doReadTx(ctx context.Context, do func(c conn.Conn) error) error {
	tx, err := db.getReadTx(ctx)
	if err != nil {
		return err
	}
	if err = do(tx.conn()); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *db) Close() error {
	return db.cm.Close()
}

func (db *db) onCollectionClose(name string) {
	db.mu.Lock()
	delete(db.openedCollections, name)
	db.mu.Unlock()
}
