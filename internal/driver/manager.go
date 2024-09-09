package driver

import (
	"context"
	"errors"
	"fmt"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/anyproto/any-store/internal/registry"
)

var (
	ErrDBIsClosed    = errors.New("any-store: db is closed")
	ErrDBIsNotOpened = errors.New("any-store: db is not opened")
)

func NewConnManager(path string, pragma map[string]string, writeCount, readCount int, fr *registry.FilterRegistry, sr *registry.SortRegistry) (*ConnManager, error) {
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
		readCh:    make(chan *Conn, len(readConn)),
		writeCh:   make(chan *Conn, len(writeConn)),
		closed:    make(chan struct{}),
		readConn:  readConn,
		writeConn: writeConn,
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

	select {
	case <-c.closed:
		return nil, ErrDBIsClosed
	case conn = <-c.readCh:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *ConnManager) ReleaseRead(conn *Conn) {
	c.readCh <- conn
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
	var conn *Conn
	if err = c.writeConn[0].Close(); err != nil {
		err = errors.Join(err, err)
	}

	for range c.readConn {
		conn = <-c.readCh
		if err != nil {
			err = errors.Join(err, err)
		} else {
			err = errors.Join(err, conn.Close())
		}
	}

	return err
}
