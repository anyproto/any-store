//go:build vfs

package btree

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// readReply is what a shortReadFile answers a read with instead of the
// file: n bytes of what the file holds there, and err. ok false passes the
// read to the file.
type readReply func(b []byte, off int64) (n int, err error, ok bool)

// shortReadFile answers the reads its reply picks as the reply says, as a
// file system whose reads can come up short does; every other read goes to
// the file.
type shortReadFile struct {
	File
	reply *atomic.Pointer[readReply]
}

func (f *shortReadFile) ReadAt(b []byte, off int64) (int, error) {
	if r := f.reply.Load(); r != nil {
		if n, err, ok := (*r)(b, off); ok {
			if n > 0 {
				if _, rerr := f.File.ReadAt(b[:n], off); rerr != nil {
					return 0, rerr
				}
			}
			return n, err
		}
	}
	return f.File.ReadAt(b, off)
}

// shortReadVFS wraps the file opened by the name name with a shortReadFile
// answering by reply, until the test ends.
func shortReadVFS(t *testing.T, name string, reply *atomic.Pointer[readReply]) {
	t.Helper()
	SetVFS(VFS{OpenFile: func(n string, flag int, perm os.FileMode) (File, error) {
		f, err := defaultOpenFile(n, flag, perm)
		if err != nil || n != name {
			return f, err
		}
		return &shortReadFile{File: f, reply: reply}, nil
	}})
	t.Cleanup(ResetVFS)
}

// replyAt is a reply for the reads at off alone: n bytes and err.
func replyAt(at int64, n int, err error) *readReply {
	r := readReply(func(b []byte, off int64) (int, error, bool) {
		if off != at {
			return 0, nil, false
		}
		return min(n, len(b)), err, true
	})
	return &r
}

// A read of a page of the database file that comes up short of bytes the
// file holds is that read's error, as from a file system that can return a
// short read: not damage, and not zeros. The same read tried again reads
// the page. A read that returns the whole page with io.EOF, which ReadAt
// may do at the end of the file, read the page.
func TestReadDBPage_ShortReadOfBytesTheFileHolds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	opts := DefaultOptions()
	opts.Checksum = true
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	commitRows(t, db, "t", 64, 200)
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	require.NoError(t, db.Close())
	info, err := os.Stat(path)
	require.NoError(t, err)
	pageSize := int64(opts.PageSize)
	last := uint32(info.Size() / pageSize)
	at := int64(last-1) * pageSize
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	want = want[at:]

	var reply atomic.Pointer[readReply]
	shortReadVFS(t, path, &reply)
	db, err = testOpen(t, path, opts)
	require.NoError(t, err)
	defer db.Close()
	buf := make([]byte, pageSize)

	for _, tc := range []struct {
		name string
		n    int
		err  error
		want error
	}{
		{"none, io.EOF", 0, io.EOF, io.EOF},
		{"half, io.EOF", int(pageSize / 2), io.EOF, io.EOF},
		{"half, no error", int(pageSize / 2), nil, io.ErrUnexpectedEOF},
		{"none, io.ErrUnexpectedEOF", 0, io.ErrUnexpectedEOF, io.ErrUnexpectedEOF},
	} {
		reply.Store(replyAt(at, tc.n, tc.err))
		err := db.pager.readDBPage(last, buf)
		reply.Store(nil)
		require.ErrorIs(t, err, tc.want, tc.name)
		require.NotErrorIs(t, err, ErrCorrupt, tc.name)
		require.NoError(t, db.pager.readDBPage(last, buf), tc.name)
	}

	reply.Store(replyAt(at, int(pageSize), io.EOF))
	clear(buf)
	err = db.pager.readDBPage(last, buf)
	reply.Store(nil)
	require.NoError(t, err)
	require.Equal(t, want, buf)
}

// A read of a WAL frame is judged by its count as well as its error: fewer
// bytes than the frame with no error, which some file systems return, is
// a short read (ErrWALShortRead with io.ErrUnexpectedEOF) through every
// reader, and not a partial page handed to the codec, which would fail its
// checksum as damage. All the bytes with io.EOF read the frame, for the
// readers and for a checkpoint.
func TestWALFrameRead_CountsItsBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	opts := DefaultOptions()
	opts.Checksum = true
	opts.DisableAutoCheckpoint = true
	var reply atomic.Pointer[readReply]
	shortReadVFS(t, path+"-wal", &reply)
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer db.Close()
	commitRows(t, db, "t", 64, 200)

	w := db.pager.wal
	nf := w.nFrame.Load()
	require.Greater(t, nf, uint32(1))
	frameSize := int64(walFrameSize) + int64(opts.PageSize)
	at := int64(walHeaderSize) + int64(nf-1)*frameSize + walFrameSize
	buf := make([]byte, opts.PageSize)
	readers := []struct {
		name string
		size int // the bytes of buf the reader fills
		read func() error
	}{
		{"decoded", len(buf), func() error { return w.readFrame(nf, buf, nil, nil) }},
		{"raw", len(buf), func() error { return w.readFrameRaw(nf, buf) }},
		{"page-1 header", dbHeaderSize, func() error { return db.pager.readWalFrameData(nf, buf[:dbHeaderSize]) }},
	}
	for _, r := range readers {
		reply.Store(replyAt(at, 10, nil))
		err := r.read()
		reply.Store(nil)
		require.ErrorIs(t, err, ErrWALShortRead, r.name)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF, r.name)
		require.NotErrorIs(t, err, ErrCorrupt, r.name)
		require.NotErrorIs(t, err, ErrPageIntegrity, r.name)

		require.NoError(t, r.read(), r.name)
		want := bytes.Clone(buf[:r.size])
		clear(buf)
		reply.Store(replyAt(at, r.size, io.EOF))
		err = r.read()
		reply.Store(nil)
		require.NoError(t, err, r.name)
		require.Equal(t, want, buf[:r.size], r.name)
	}

	// Every read of the log returns its bytes with io.EOF.
	all := readReply(func(b []byte, _ int64) (int, error, bool) { return len(b), io.EOF, true })
	reply.Store(&all)
	err = db.Checkpoint(CheckpointFull)
	reply.Store(nil)
	require.NoError(t, err)
	require.NoError(t, db.IntegrityCheck())
}

// The key proof at open reads the pages it tries as every read of a page
// is judged: a read of the database file that comes up short of bytes the
// file holds could not read the page, which might have proved the key. It
// is not a page that fails its authentication: the open returns the read's
// error, not ErrWrongKey and not damage, and tells the integrity hook
// nothing. The same open without the short reads proves the key.
func TestOpen_ShortReadInTheKeyProofIsAReadError(t *testing.T) {
	path, opts := keyProofDB(t, true, nil)
	var fired atomic.Int32
	opts.OnIntegrityError = func(uint32, error) { fired.Add(1) }
	damageFile(t, path, 1000)

	var reply atomic.Pointer[readReply]
	shortReadVFS(t, path, &reply)
	pageSize := int64(opts.PageSize)
	pastPage1 := readReply(func(b []byte, off int64) (int, error, bool) {
		return 0, io.EOF, off >= pageSize
	})
	// Armed after the WAL attach: the reads that follow are page 1's and
	// the pages the open tries.
	testOpenAfterWALOpenHook = func(p *pager) {
		if p.path == path {
			reply.Store(&pastPage1)
		}
	}
	t.Cleanup(func() { testOpenAfterWALOpenHook = nil })

	_, err := Open(path, opts)
	reply.Store(nil)
	require.ErrorIs(t, err, io.EOF)
	require.NotErrorIs(t, err, ErrWrongKey)
	require.NotErrorIs(t, err, ErrCorrupt)
	require.Zero(t, fired.Load())

	testOpenAfterWALOpenHook = nil
	_, err = Open(path, opts)
	require.ErrorIs(t, err, ErrPageIntegrity)
	require.EqualValues(t, 1, fired.Load())
}

// Page 1 read at open, from a file that holds it whole, by a read that
// comes up short past the header: the read's error, under a key too. A
// page 1 filled with zeros past the read would fail its authentication, and
// another page would then prove the key: damage reported for a page that
// reads whole a moment later.
func TestOpen_ShortReadOfPage1IsAReadError(t *testing.T) {
	path, opts := keyProofDB(t, true, nil)
	var fired atomic.Int32
	opts.OnIntegrityError = func(uint32, error) { fired.Add(1) }

	var reply atomic.Pointer[readReply]
	shortReadVFS(t, path, &reply)
	// The read of the whole of page 1, past its header.
	page1 := readReply(func(b []byte, off int64) (int, error, bool) {
		return 2 * dbHeaderSize, io.EOF, off == 0 && len(b) == int(opts.PageSize)
	})
	testOpenAfterWALOpenHook = func(p *pager) {
		if p.path == path {
			reply.Store(&page1)
		}
	}
	t.Cleanup(func() { testOpenAfterWALOpenHook = nil })

	_, err := Open(path, opts)
	reply.Store(nil)
	require.ErrorIs(t, err, io.EOF)
	require.NotErrorIs(t, err, ErrCorrupt)
	require.NotErrorIs(t, err, ErrWrongKey)
	require.Zero(t, fired.Load())

	testOpenAfterWALOpenHook = nil
	db, err := Open(path, opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}
