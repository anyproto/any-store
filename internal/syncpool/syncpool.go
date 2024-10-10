package syncpool

import (
	"sync"

	"github.com/anyproto/any-store/anyenc"
)

func NewSyncPool() *SyncPool {
	return &SyncPool{
		pool: &sync.Pool{
			New: func() any {
				return &DocBuffer{
					Arena:  &anyenc.Arena{},
					Parser: &anyenc.Parser{},
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
	Arena    *anyenc.Arena
	Parser   *anyenc.Parser
}

func (sp *SyncPool) GetDocBuf() *DocBuffer {
	buf := sp.pool.Get().(*DocBuffer)
	if buf == nil {
		buf = &DocBuffer{
			Arena:  &anyenc.Arena{},
			Parser: &anyenc.Parser{},
		}
	}
	return buf
}

func (sp *SyncPool) ReleaseDocBuf(b *DocBuffer) {
	sp.pool.Put(b)
}
