package syncpool

import (
	"sync"
	"unsafe"

	"github.com/anyproto/any-store/v2/anyenc"
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
	SmallBuf   []byte
	DocBuf     []byte
	ScratchBuf []byte
	// Leaves is the scratch for path resolution through arrays (query.Key).
	Leaves []anyenc.Leaf
	Arena  *anyenc.Arena
	Parser *anyenc.Parser
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

// leafSize is the pooled footprint of one Leaves entry.
const leafSize = int(unsafe.Sizeof(anyenc.Leaf{}))

func (sp *SyncPool) ReleaseDocBuf(b *DocBuffer) {
	// Size check excludes reusable scratch buffers inside Parser (decompBuf,
	// input copy buffer) — those are kept deliberately to avoid re-allocation
	// on subsequent large-document operations. They are bounded separately:
	// TrimScratch frees them when their combined capacity exceeds the element
	// size limit, so one huge document can't pin its high-water mark in the
	// pool while the buffer keeps passing the size check below.
	b.Parser.TrimScratch(sp.sizeLimit)
	if sp.sizeLimit > 0 && cap(b.DocBuf)+cap(b.SmallBuf)+cap(b.ScratchBuf)+cap(b.Leaves)*leafSize+b.Arena.ApproxSize()+b.Parser.ApproxSize() > sp.sizeLimit {
		return
	}
	sp.pool.Put(b)
}
