package syncpool

import (
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
				}
			},
		},
	}
}

type SyncPool struct {
	pool *sync.Pool
}

type DocBuffer struct {
	SmallBuf []byte
	DocBuf   []byte
	Arena    *fastjson.Arena
	Parser   *fastjson.Parser
}

func (sp *SyncPool) GetDocBuf() *DocBuffer {
	buf := sp.pool.Get().(*DocBuffer)
	if buf == nil {
		buf = &DocBuffer{
			Arena:  &fastjson.Arena{},
			Parser: &fastjson.Parser{},
		}
	}
	return buf
}

func (sp *SyncPool) ReleaseDocBuf(b *DocBuffer) {
	sp.pool.Put(b)
}
