package syncpool

import (
	"database/sql/driver"
	"sync"

	"github.com/valyala/fastjson"
)

func NewSyncPool() *SyncPool {
	return &SyncPool{
		pool: &sync.Pool{
			New: func() any {
				return &DocBuffer{
					Arena:  &fastjson.Arena{},
					Parser: &fastjson.Parser{},
					IdDataValues: []driver.NamedValue{
						{Name: "id"},
						{Name: "data"},
					},
				}
			},
		},
	}
}

type SyncPool struct {
	pool *sync.Pool
}

type DocBuffer struct {
	SmallBuf     []byte
	DocBuf       []byte
	Arena        *fastjson.Arena
	Parser       *fastjson.Parser
	IdDataValues []driver.NamedValue
}

func (d *DocBuffer) DriverValues(id, value []byte) []driver.NamedValue {
	d.IdDataValues[0].Value = id
	d.IdDataValues[1].Value = value
	return d.IdDataValues
}

func (d *DocBuffer) DriverValuesId(id []byte) []driver.NamedValue {
	d.IdDataValues[0].Value = id
	return d.IdDataValues[:1]
}

func (sp *SyncPool) GetDocBuf() *DocBuffer {
	buf := sp.pool.Get().(*DocBuffer)
	if buf == nil {
		buf = &DocBuffer{
			Arena:  &fastjson.Arena{},
			Parser: &fastjson.Parser{},
			IdDataValues: []driver.NamedValue{
				{Name: "id"},
				{Name: "data"},
			},
		}
	}
	return buf
}

func (sp *SyncPool) ReleaseDocBuf(b *DocBuffer) {
	sp.pool.Put(b)
}
