//go:build !(linux || darwin) || nopwritev

package btree

import (
	"os"
	"syscall"
)

// walWriteFrameData writes WAL frames by assembling a contiguous buffer.
// Used on platforms without pwritev, or when pwritev is disabled via
// the "nopwritev" build tag (go build -tags nopwritev).
func walWriteFrameData(fd uintptr, hdrBuf []byte, pages []*page, offset int64, pageSize uint32) error {
	frameSize := walFrameSize + int(pageSize)
	buf := make([]byte, len(pages)*frameSize)
	for i, p := range pages {
		pos := i * frameSize
		copy(buf[pos:pos+walFrameSize], hdrBuf[i*walFrameSize:(i+1)*walFrameSize])
		copy(buf[pos+walFrameSize:pos+frameSize], p.data[:pageSize])
	}
	n, err := syscall.Pwrite(int(fd), buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return os.ErrClosed
	}
	return nil
}
