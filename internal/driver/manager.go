package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"modernc.org/libc"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/anyproto/any-store/internal/registry"
)

var (
	ErrDBIsClosed          = errors.New("any-store: db is closed")
	ErrDBIsNotOpened       = errors.New("any-store: db is not opened")
	ErrIncompatibleVersion = errors.New("any-store: incompatible version")
	ErrStmtIsClosed        = errors.New("any-store: stmt is closed")
	initSqliteOnce         sync.Once
)

func NewConnManager(
	path string,
	pragma map[string]string,
	writeCount, readCount int,
	preAllocatedPageCacheSize int,
	fr *registry.FilterRegistry, sr *registry.SortRegistry,
	version int,
) (*ConnManager, error) {
	_, statErr := os.Stat(path)
	var newDb bool
	if os.IsNotExist(statErr) {
		newDb = true
	}

	initSqliteOnce.Do(func() {
		if preAllocatedPageCacheSize <= 0 {
			return
		}
		tls := libc.NewTLS()
		err := sqlitePreallocatePageCache(tls, preAllocatedPageCacheSize)
		if err != nil {
			// ignore this error because it's not critical, we can continue without preallocated cache
			_, _ = fmt.Fprintf(os.Stderr, "sqlite: failed to preallocate pagecache: %v\n", err)
		}
	})

	var (
		writeConn = make([]*Conn, 0, writeCount)
		readConn  = make([]*Conn, 0, readCount)
	)
	closeAll := func() {
		for _, conn := range writeConn {
			_ = conn.Close()
		}
	}

	cm := &ConnManager{
		readCh:         make(chan *Conn),
		readConnLimit:  readCount,
		readConn:       readConn,
		readConnTTL:    time.Minute,
		writeCh:        make(chan *Conn, writeCount),
		closed:         make(chan struct{}),
		sortRegistry:   sr,
		filterRegistry: fr,
		path:           path,
		pragma:         pragma,
	}

	for i := 0; i < writeCount; i++ {
		conn, err := sqlite.OpenConn(path, sqlite.OpenCreate|sqlite.OpenWAL|sqlite.OpenURI|sqlite.OpenReadWrite)
		if err != nil {
			closeAll()
			return nil, err
		}
		if err = cm.setupConn(conn); err != nil {
			closeAll()
			return nil, err
		}
		wConn := &Conn{conn: conn}
		writeConn = append(writeConn, wConn)
		if i == 0 {
			if err = checkVersion(conn, version, newDb); err != nil {
				closeAll()
				return nil, err
			}
		}
		cm.writeCh <- wConn
	}
	cm.writeConn = writeConn
	return cm, nil
}

type ConnManager struct {
	readCh         chan *Conn
	writeCh        chan *Conn
	readConn       []*Conn
	writeConn      []*Conn
	closed         chan struct{}
	path           string
	sortRegistry   *registry.SortRegistry
	filterRegistry *registry.FilterRegistry
	pragma         map[string]string
	readConnLimit  int
	mu             sync.Mutex
	readConnTTL    time.Duration
}

func (c *ConnManager) GetWrite(ctx context.Context) (conn *Conn, err error) {
	if c == nil {
		return nil, ErrDBIsNotOpened
	}

	select {
	case <-c.closed:
		return nil, ErrDBIsClosed
	case conn = <-c.writeCh:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *ConnManager) ReleaseWrite(conn *Conn) {
	c.writeCh <- conn
}

func (c *ConnManager) GetRead(ctx context.Context) (conn *Conn, err error) {
	if c == nil {
		return nil, ErrDBIsNotOpened
	}

	c.mu.Lock()

	select {
	case <-c.closed:
		c.mu.Unlock()
		return nil, ErrDBIsClosed
	case <-ctx.Done():
		c.mu.Unlock()
		return nil, ctx.Err()
	default:
	}

	// find inactive conn
	for _, conn = range c.readConn {
		if conn.isActive.CompareAndSwap(false, true) {
			c.mu.Unlock()
			return conn, nil
		}
	}

	// open new conn if limit is not reached
	if len(c.readConn) < c.readConnLimit {
		if conn, err = c.openReadConn(); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		c.readConn = append(c.readConn, conn)
		conn.isActive.Store(true)
		c.mu.Unlock()
		return conn, nil
	}

	c.mu.Unlock()
	// wait released conn
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, ErrDBIsClosed
	case conn = <-c.readCh:
		c.mu.Lock()
		conn.isActive.Store(true)
		c.mu.Unlock()
		return conn, nil
	}
}

func (c *ConnManager) ReleaseRead(conn *Conn) {
	now := time.Now()
	conn.isActive.Store(false)
	conn.lastUsage.Store(now.Unix())
	select {
	case c.readCh <- conn:
		return
	case <-c.closed:
		c.readCh <- conn
		return
	default:
	}

	var filteredConn = c.readConn[:0]
	for _, conn = range c.readConn {
		if !conn.isActive.Load() && now.Sub(time.Unix(conn.lastUsage.Load(), 0)) > c.readConnTTL {
			_ = conn.Close()
		} else {
			filteredConn = append(filteredConn, conn)
		}
	}
}

func (c *ConnManager) openReadConn() (*Conn, error) {
	conn, err := sqlite.OpenConn(c.path, sqlite.OpenWAL|sqlite.OpenURI|sqlite.OpenReadOnly)
	if err != nil {
		return nil, err
	}
	if err = c.setupConn(conn); err != nil {
		return nil, err
	}
	return &Conn{conn: conn}, nil
}

func (c *ConnManager) setupConn(conn *sqlite.Conn) (err error) {
	err = conn.CreateFunction("any_filter", &sqlite.FunctionImpl{
		NArgs: 2,
		Scalar: func(ctx sqlite.Context, args []sqlite.Value) (sqlite.Value, error) {
			if c.filterRegistry.Filter(args[0].Int(), args[1].Blob()) {
				return sqlite.IntegerValue(1), nil
			} else {
				return sqlite.IntegerValue(0), nil
			}
		},
		Deterministic: true,
	})
	if err != nil {
		return
	}
	err = conn.CreateFunction("any_sort", &sqlite.FunctionImpl{
		NArgs: 2,
		Scalar: func(ctx sqlite.Context, args []sqlite.Value) (sqlite.Value, error) {
			return sqlite.BlobValue(c.sortRegistry.Sort(args[0].Int(), args[1].Blob())), nil
		},
		Deterministic: true,
	})

	if c.pragma != nil {
		for k, v := range c.pragma {
			if err = sqlitex.ExecuteTransient(conn, fmt.Sprintf("PRAGMA %s = %s", k, v), nil); err != nil {
				return
			}
		}
	}
	return
}

func (c *ConnManager) Close() (err error) {
	close(c.closed)
	var activeCount int
	c.mu.Lock()
	for _, conn := range c.readConn {
		if !conn.isActive.Load() {
			if cErr := conn.Close(); cErr != nil {
				err = errors.Join(err, cErr)
			}
		} else {
			activeCount++
		}
	}
	c.mu.Unlock()
	for range activeCount {
		conn := <-c.readCh
		if cErr := conn.Close(); cErr != nil {
			err = errors.Join(err, cErr)
		}
	}

	if err = c.writeConn[0].Close(); err != nil {
		err = errors.Join(err, err)
	}
	return err
}

func checkVersion(conn *sqlite.Conn, version int, isNewDb bool) (err error) {
	var currVersion int
	if !isNewDb {
		err = sqlitex.ExecuteTransient(conn, "PRAGMA user_version", &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				currVersion = stmt.ColumnInt(0)
				return nil
			},
		})
		if err != nil {
			return err
		}
		if version != currVersion {
			return errors.Join(ErrIncompatibleVersion, fmt.Errorf("want version: %d; got: %d", version, currVersion))
		}
	}
	return sqlitex.ExecuteTransient(conn, fmt.Sprintf("PRAGMA user_version = %d", version), nil)
}
