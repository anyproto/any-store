//go:build linux

package btree

import (
	"syscall"
	"unsafe"
)

// walWriteFrameData writes WAL frames using pwritev (scatter-gather I/O).
// Frame headers come from hdrBuf, page data comes directly from page buffers —
// no intermediate copy of page data is needed. This matches SQLite's
// walWriteOneFrame approach of writing page data from the original buffer.
func walWriteFrameData(fd uintptr, hdrBuf []byte, pages []*page, offset int64, pageSize uint32) error {
	// Build iovec list: [header0, pagedata0, header1, pagedata1, ...]
	n := len(pages)
	iovecs := make([]syscall.Iovec, 2*n)
	for i, p := range pages {
		hdrStart := i * walFrameSize
		iovecs[2*i] = syscall.Iovec{
			Base: &hdrBuf[hdrStart],
			Len:  walFrameSize,
		}
		iovecs[2*i+1] = syscall.Iovec{
			Base: &p.data[0],
			Len:  uint64(pageSize),
		}
	}

	// Write all iovecs, handling partial writes and IOV_MAX (1024 on Linux).
	for len(iovecs) > 0 {
		batch := iovecs
		if len(batch) > 1024 {
			batch = batch[:1024]
		}
		written, _, errno := syscall.Syscall6(
			syscall.SYS_PWRITEV,
			fd,
			uintptr(unsafe.Pointer(&batch[0])),
			uintptr(len(batch)),
			uintptr(offset),
			0, 0,
		)
		if errno != 0 {
			return errno
		}
		if written == 0 {
			return syscall.EIO
		}
		offset += int64(written)
		// Advance past fully written iovecs.
		rem := uint64(written)
		for len(iovecs) > 0 && rem > 0 {
			if rem >= iovecs[0].Len {
				rem -= iovecs[0].Len
				iovecs = iovecs[1:]
			} else {
				iovecs[0].Base = (*byte)(unsafe.Add(unsafe.Pointer(iovecs[0].Base), rem))
				iovecs[0].Len -= rem
				rem = 0
			}
		}
	}
	return nil
}
