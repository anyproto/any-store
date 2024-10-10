package syncpool

import (
	"sync"

	"github.com/anyproto/any-store/anyenc"
)

func NewSyncPool(sizeLimit int) *SyncPool {
	return &SyncPool{
		sizeLimit: sizeLimit,
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
	pool      *sync.Pool
	sizeLimit int
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
	if sp.sizeLimit > 0 && cap(b.DocBuf)+cap(b.SmallBuf)+b.Arena.ApproxSize()+b.Parser.ApproxSize() > sp.sizeLimit {
		return
	}
	sp.pool.Put(b)
}
