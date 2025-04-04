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
	enableStalledConnDetector bool,
	stalledConnDetectorCloseTimeout time.Duration,
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
		for _, conn := range readConn {
			_ = conn.Close()
		}
	}
	for i := 0; i < writeCount; i++ {
		conn, err := sqlite.OpenConn(path, sqlite.OpenCreate|sqlite.OpenWAL|sqlite.OpenURI|sqlite.OpenReadWrite)
		if err != nil {
			closeAll()
			return nil, err
		}
		if err = setupConn(fr, sr, conn, pragma); err != nil {
			closeAll()
			return nil, err
		}
		writeConn = append(writeConn, &Conn{conn: conn})
		if i == 0 {
			if err = checkVersion(conn, version, newDb); err != nil {
				closeAll()
				return nil, err
			}
		}
	}

	for i := 0; i < readCount; i++ {
		conn, err := sqlite.OpenConn(path, sqlite.OpenWAL|sqlite.OpenURI|sqlite.OpenReadOnly)
		if err != nil {
			closeAll()
			return nil, err
		}
		if err = setupConn(fr, sr, conn, pragma); err != nil {
			closeAll()
			return nil, err
		}
		readConn = append(readConn, &Conn{conn: conn})
	}

	cm := &ConnManager{
		readCh:                          make(chan *Conn, len(readConn)),
		writeCh:                         make(chan *Conn, len(writeConn)),
		closed:                          make(chan struct{}),
		readConn:                        readConn,
		writeConn:                       writeConn,
		stalledConnStackTraces:          make(map[uintptr][]uintptr),
		stalledConnDetectorEnabled:      enableStalledConnDetector,
		stalledConnDetectorCloseTimeout: stalledConnDetectorCloseTimeout,
	}
	for _, conn := range writeConn {
		cm.writeCh <- conn
	}
	for _, conn := range readConn {
		cm.readCh <- conn
	}
	return cm, nil
}

type ConnManager struct {
	readCh    chan *Conn
	writeCh   chan *Conn
	readConn  []*Conn
	writeConn []*Conn
	closed    chan struct{}

	// when enabled, collects stack traces and duration of taken connections
	stalledConnDetectorEnabled bool
	// when stalledConnDetectorCloseTimeout > 0 and stalledConnDetectorEnabled is true,
	// ConnManager panics on Close in case of any connection is not released after this timeout
	stalledConnDetectorCloseTimeout time.Duration
	stalledConnStackMutex           sync.Mutex
	stalledConnStackTraces          map[uintptr][]uintptr
}

func (c *ConnManager) GetWrite(ctx context.Context) (conn *Conn, err error) {
	if c == nil {
		return nil, ErrDBIsNotOpened
	}

	select {
	case <-c.closed:
		return nil, ErrDBIsClosed
	case conn = <-c.writeCh:
		c.stalledAcquireConn(conn)
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *ConnManager) ReleaseWrite(conn *Conn) {
	c.writeCh <- conn
	c.stalledReleaseConn(conn)
}

func (c *ConnManager) GetRead(ctx context.Context) (conn *Conn, err error) {
	if c == nil {
		return nil, ErrDBIsNotOpened
	}

	select {
	case <-c.closed:
		return nil, ErrDBIsClosed
	case conn = <-c.readCh:
		c.stalledAcquireConn(conn)
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *ConnManager) ReleaseRead(conn *Conn) {
	c.readCh <- conn
	c.stalledReleaseConn(conn)
}

func setupConn(fr *registry.FilterRegistry, sr *registry.SortRegistry, conn *sqlite.Conn, pragma map[string]string) (err error) {
	err = conn.CreateFunction("any_filter", &sqlite.FunctionImpl{
		NArgs: 2,
		Scalar: func(ctx sqlite.Context, args []sqlite.Value) (sqlite.Value, error) {
			if fr.Filter(args[0].Int(), args[1].Blob()) {
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
			return sqlite.BlobValue(sr.Sort(args[0].Int(), args[1].Blob())), nil
		},
		Deterministic: true,
	})

	if pragma != nil {
		for k, v := range pragma {
			if err = sqlitex.ExecuteTransient(conn, fmt.Sprintf("PRAGMA %s = %s", k, v), nil); err != nil {
				return
			}
		}
	}
	return
}

func (c *ConnManager) Close() (err error) {
	/*

		Can't interrupt connections yet because there is a race in sqlite driver
		Also trying to close active connections causes some panics in the driver

		var closedChan = make(chan struct{})
		close(closedChan)

		for _, conn := range c.readConn {
			conn.conn.SetInterrupt(closedChan)
		}
		for _, conn := range c.writeConn {
			conn.conn.SetInterrupt(closedChan)
		}

	*/
	close(c.closed)

	allClosedChan := make(chan struct{})
	if c.stalledConnDetectorEnabled && c.stalledConnDetectorCloseTimeout > 0 {
		go c.stalledCloseWatcher(allClosedChan)
	}

	var conn *Conn
	for range c.readConn {
		conn = <-c.readCh
		err = errors.Join(err, conn.Close())
	}

	if err = c.writeConn[0].Close(); err != nil {
		err = errors.Join(err, err)
	}

	close(allClosedChan)
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
