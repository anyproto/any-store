package conn

import (
	"context"
	"errors"

	"github.com/mattn/go-sqlite3"

	"github.com/anyproto/any-store/internal/registry"
)

var (
	ErrDBIsClosed    = errors.New("db is closed")
	ErrDBIsNotOpened = errors.New("db is not opened")
)

func NewDriver(fr *registry.FilterRegistry, sr *registry.SortRegistry) *sqlite3.SQLiteDriver {
	return &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if err := conn.RegisterFunc("any_filter", fr.Filter, true); err != nil {
				return err
			}
			if err := conn.RegisterFunc("any_sort", sr.Sort, true); err != nil {
				return err
			}
			return nil
		},
	}
}

func NewConnManager(driver *sqlite3.SQLiteDriver, dsn string, writeCount, readCount int) (*ConnManager, error) {
	var (
		writeConn = make([]Conn, 0, writeCount)
		readConn  = make([]Conn, 0, readCount)
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
		conn, err := driver.Open(dsn)
		if err != nil {
			closeAll()
			return nil, err
		}
		writeConn = append(writeConn, conn.(Conn))
	}

	readOnlyDsn := dsn + "&mode=ro"
	for i := 0; i < readCount; i++ {
		conn, err := driver.Open(readOnlyDsn)
		if err != nil {
			closeAll()
			return nil, err
		}
		readConn = append(readConn, conn.(*sqlite3.SQLiteConn))
	}

	cm := &ConnManager{
		readCh:    make(chan Conn, len(readConn)),
		writeCh:   make(chan Conn, len(writeConn)),
		readConn:  readConn,
		writeConn: writeConn,
		driver:    driver,
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
	readCh    chan Conn
	writeCh   chan Conn
	readConn  []Conn
	writeConn []Conn
	driver    *sqlite3.SQLiteDriver
	closed    chan struct{}
}

func (c *ConnManager) GetWrite(ctx context.Context) (conn Conn, err error) {
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

func (c *ConnManager) ReleaseWrite(conn Conn) {
	c.writeCh <- conn
}

func (c *ConnManager) GetRead(ctx context.Context) (conn Conn, err error) {
	if c == nil {
		return nil, ErrDBIsNotOpened
	}

	select {
	case <-c.closed:
		return nil, ErrDBIsClosed
	case conn = <-c.readCh:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *ConnManager) ReleaseRead(conn Conn) {
	c.readCh <- conn
}

func (c *ConnManager) Close() (err error) {
	for _, conn := range c.readConn {
		if cErr := conn.Close(); cErr != nil {
			err = errors.Join(err, cErr)
		}
	}
	for _, conn := range c.writeConn {
		if cErr := conn.Close(); cErr != nil {
			err = errors.Join(err, cErr)
		}
	}
	return err
}
