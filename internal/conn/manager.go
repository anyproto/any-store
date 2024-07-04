package conn

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

func NewConnManager(sr *registry.SortRegistry, fr *registry.FilterRegistry, path string, writeCount, readCount int) (*ConnManager, error) {
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
		conn, err := sqlite.OpenConn(path)

		if err != nil {
			closeAll()
			return nil, err
		}
		if err = setupConn(conn, sr, fr); err != nil {
			closeAll()
			return nil, err
		}
		writeConn = append(writeConn, conn.(Conn))
	}

	for i := 0; i < readCount; i++ {
		conn, err := sqlite.OpenConn(path, sqlite.OpenWAL, sqlite.OpenURI)
		if err != nil {
			closeAll()
			return nil, err
		}
		if err = setupConn(conn, sr, fr); err != nil {
			closeAll()
			return nil, err
		}
		readConn = append(readConn, conn.(Conn))
	}

	cm := &ConnManager{
		readCh:    make(chan Conn, len(readConn)),
		writeCh:   make(chan Conn, len(writeConn)),
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
	readCh    chan Conn
	writeCh   chan Conn
	readConn  []Conn
	writeConn []Conn

	closed chan struct{}
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

func setupConn(conn *sqlite.Conn, sr *registry.SortRegistry, fr *registry.FilterRegistry) (err error) {
	if err = conn.CreateFunction("any_filter", &sqlite.FunctionImpl{
		NArgs:         2,
		Deterministic: true,
		Scalar: func(ctx sqlite.Context, args []sqlite.Value) (sqlite.Value, error) {
			return sqlite.IntegerValue(fr.Filter(args[0].Int(), args[2].Blob())), nil
		},
	}); err != nil {
		return
	}
	if err = conn.CreateFunction("any_sort", &sqlite.FunctionImpl{
		NArgs:         2,
		Deterministic: true,
		Scalar: func(ctx sqlite.Context, args []sqlite.Value) (sqlite.Value, error) {
			return sqlite.BlobValue(sr.Sort(args[0].Int(), args[2].Blob())), nil
		},
	}); err != nil {
		return
	}
	return
}
