//go:build !(linux || darwin)

package btree

import (
	"os"
	"syscall"
)

// walWriteFrameData writes WAL frames by assembling a contiguous buffer.
// On non-Linux platforms, pwritev is not available, so page data is copied
// into a single buffer for a single pwrite call.
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
