package driver

import (
	"context"
	"errors"

	"zombiezen.com/go/sqlite"

	"github.com/anyproto/any-store/internal/registry"
)

var (
	ErrDBIsClosed    = errors.New("db is closed")
	ErrDBIsNotOpened = errors.New("db is not opened")
)

func NewConnManager(dsn string, writeCount, readCount int, fr *registry.FilterRegistry, sr *registry.SortRegistry) (*ConnManager, error) {
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
		conn, err := sqlite.OpenConn(dsn)
		if err != nil {
			closeAll()
			return nil, err
		}
		if err = setupConn(fr, sr, conn); err != nil {
			closeAll()
			return nil, err
		}
		writeConn = append(writeConn, &Conn{conn: conn})
	}

	for i := 0; i < readCount; i++ {
		conn, err := sqlite.OpenConn(dsn, sqlite.OpenWAL|sqlite.OpenURI|sqlite.OpenReadOnly)
		if err != nil {
			closeAll()
			return nil, err
		}
		if err = setupConn(fr, sr, conn); err != nil {
			closeAll()
			return nil, err
		}
		readConn = append(readConn, &Conn{conn: conn})
	}

	cm := &ConnManager{
		readCh:    make(chan *Conn, len(readConn)),
		writeCh:   make(chan *Conn, len(writeConn)),
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

func setupConn(fr *registry.FilterRegistry, sr *registry.SortRegistry, conn *sqlite.Conn) (err error) {
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
	return
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
