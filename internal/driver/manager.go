package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anyproto/go-sqlite"
	"github.com/anyproto/go-sqlite/sqlitex"
	"modernc.org/libc"

	"github.com/anyproto/any-store/internal/registry"
)

var (
	ErrDBIsClosed          = errors.New("any-store: db is closed")
	ErrDBIsNotOpened       = errors.New("any-store: db is not opened")
	ErrIncompatibleVersion = errors.New("any-store: incompatible version")
	ErrStmtIsClosed        = errors.New("any-store: stmt is closed")
	initSqliteOnce         sync.Once
)

type Config struct {
	Pragma                    map[string]string
	ReadCount                 int
	PreAllocatedPageCacheSize int
	SortRegistry              *registry.SortRegistry
	FilterRegistry            *registry.FilterRegistry
	Version                   int
	ReadConnTTL               time.Duration
}

type EventType int

const (
	EventAcquireWrite EventType = iota
	EventReleaseWrite
)

type Event struct {
	Type EventType
	When time.Time
}

type WriteObserver func(Event)

func NewConnManager(path string, conf Config) (*ConnManager, error) {
	_, statErr := os.Stat(path)
	var newDb bool
	if os.IsNotExist(statErr) {
		newDb = true
	}

	initSqliteOnce.Do(func() {
		if conf.PreAllocatedPageCacheSize <= 0 {
			return
		}
		tls := libc.NewTLS()
		err := sqlitePreallocatePageCache(tls, conf.PreAllocatedPageCacheSize)
		if err != nil {
			// ignore this error because it's not critical, we can continue without preallocated cache
			_, _ = fmt.Fprintf(os.Stderr, "sqlite: failed to preallocate pagecache: %v\n", err)
		}
	})

	var readConn = make([]*Conn, 0, conf.ReadCount)

	cm := &ConnManager{
		readCh:         make(chan *Conn),
		readConnLimit:  conf.ReadCount,
		readConn:       readConn,
		readConnTTL:    conf.ReadConnTTL,
		writeCh:        make(chan *Conn, 1),
		closed:         make(chan struct{}),
		sortRegistry:   conf.SortRegistry,
		filterRegistry: conf.FilterRegistry,
		path:           path,
		pragma:         conf.Pragma,
	}

	// open write connection
	conn, err := sqlite.OpenConn(path, sqlite.OpenCreate|sqlite.OpenWAL|sqlite.OpenURI|sqlite.OpenReadWrite)
	if err != nil {
		return nil, err
	}
	if err = cm.setupConn(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	wConn := &Conn{conn: conn, activeStmts: map[*sqlite.Stmt]*Stmt{}}

	if err = checkVersion(conn, conf.Version, newDb); err != nil {
		_ = wConn.Close()
		return nil, err
	}
	cm.writeCh <- wConn
	cm.writeConn = wConn
	return cm, nil
}

type ConnManager struct {
	readCh         chan *Conn
	writeCh        chan *Conn
	readConn       []*Conn
	writeConn      *Conn
	closed         chan struct{}
	path           string
	sortRegistry   *registry.SortRegistry
	filterRegistry *registry.FilterRegistry
	pragma         map[string]string
	readConnLimit  int
	mu             sync.Mutex
	readConnTTL    time.Duration

	stalledConnStackMutex      sync.Mutex
	stalledConnStackTraces     map[uintptr][]uintptr
	stalledConnDetectorEnabled bool

	lastWriteRelease atomic.Value
	observersMu      sync.RWMutex
	observers        []WriteObserver
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
		c.notifyObservers(Event{Type: EventAcquireWrite, When: time.Now()})
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *ConnManager) ReleaseWrite(conn *Conn) {
	now := time.Now()
	c.lastWriteRelease.Store(now)
	c.writeCh <- conn
	c.stalledReleaseConn(conn)
	c.notifyObservers(Event{Type: EventReleaseWrite, When: now})
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
			c.stalledAcquireConn(conn)
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
		c.stalledAcquireConn(conn)
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
		return c.GetRead(ctx)
	case <-time.After(time.Second):
		return c.GetRead(ctx)
	}
}

func (c *ConnManager) ReleaseRead(conn *Conn) {
	now := time.Now()
	conn.isActive.Store(false)
	conn.lastUsage.Store(now.Unix())
	c.stalledReleaseConn(conn)
	select {
	case c.readCh <- conn:
		return
	case <-c.closed:
		return
	default:
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var filteredConn = c.readConn[:0]
	for _, conn = range c.readConn {
		if !conn.isActive.Load() && now.Sub(time.Unix(conn.lastUsage.Load(), 0)) > c.readConnTTL {
			_ = conn.Close()
		} else {
			filteredConn = append(filteredConn, conn)
		}
	}
	c.readConn = filteredConn
}

func (c *ConnManager) openReadConn() (*Conn, error) {
	conn, err := sqlite.OpenConn(c.path, sqlite.OpenWAL|sqlite.OpenURI|sqlite.OpenReadOnly)
	if err != nil {
		return nil, err
	}
	if err = c.setupConn(conn); err != nil {
		return nil, err
	}
	return &Conn{conn: conn, activeStmts: map[*sqlite.Stmt]*Stmt{}}, nil
}

func (c *ConnManager) setupConn(conn *sqlite.Conn) (err error) {
	err = conn.CreateFunction("any_filter", &sqlite.FunctionImpl{
		NArgs: 2,
		AnyStore: func(ctx sqlite.Context, index int, value []byte) (sqlite.Value, error) {
			if c.filterRegistry.Filter(index, value) {
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
		AnyStore: func(ctx sqlite.Context, index int, value []byte) (sqlite.Value, error) {
			return sqlite.BlobValue(c.sortRegistry.Sort(index, value)), nil
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
	c.mu.Lock()
	for _, conn := range c.readConn {
		err = errors.Join(err, conn.Close())
	}
	err = errors.Join(err, c.writeConn.Close())
	c.mu.Unlock()
	return err
}

func (c *ConnManager) RegisterWriteObserver(observer WriteObserver) {
	c.observersMu.Lock()
	defer c.observersMu.Unlock()
	c.observers = append(c.observers, observer)
}

func (c *ConnManager) notifyObservers(event Event) {
	c.observersMu.RLock()
	observers := c.observers
	c.observersMu.RUnlock()

	for _, observer := range observers {
		go observer(event)
	}
}

func (c *ConnManager) LastWriteRelease() time.Time {
	if v := c.lastWriteRelease.Load(); v != nil {
		return v.(time.Time)
	}
	return time.Time{}
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
