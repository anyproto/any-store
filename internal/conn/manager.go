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
		writeConn = make([]*sqlite3.SQLiteConn, 0, writeCount)
		readConn  = make([]*sqlite3.SQLiteConn, 0, readCount)
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
		writeConn = append(writeConn, conn.(*sqlite3.SQLiteConn))
	}

	for i := 0; i < readCount; i++ {
		conn, err := driver.Open(dsn)
		if err != nil {
			closeAll()
			return nil, err
		}
		readConn = append(readConn, conn.(*sqlite3.SQLiteConn))
	}

	cm := &ConnManager{
		readCh:    make(chan *sqlite3.SQLiteConn, len(readConn)),
		writeCh:   make(chan *sqlite3.SQLiteConn, len(writeConn)),
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
	readCh    chan *sqlite3.SQLiteConn
	writeCh   chan *sqlite3.SQLiteConn
	readConn  []*sqlite3.SQLiteConn
	writeConn []*sqlite3.SQLiteConn
	driver    *sqlite3.SQLiteDriver
	closed    chan struct{}
}

func (c *ConnManager) GetWrite(ctx context.Context) (conn *sqlite3.SQLiteConn, err error) {
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

func (c *ConnManager) ReleaseWrite(conn *sqlite3.SQLiteConn) {
	c.writeCh <- conn
}

func (c *ConnManager) GetRead(ctx context.Context) (conn *sqlite3.SQLiteConn, err error) {
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

func (c *ConnManager) ReleaseRead(conn *sqlite3.SQLiteConn) {
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
