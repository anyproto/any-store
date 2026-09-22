package anystore

import (
	"github.com/anyproto/any-store/v2/syncpool"
)

func copyItem(buf *syncpool.DocBuffer, it item) item {
	buf.DocBuf = it.val.MarshalTo(buf.DocBuf[:0])
	res, _ := buf.Parser.ParseOwned(buf.DocBuf)
	return item{val: res}
}
