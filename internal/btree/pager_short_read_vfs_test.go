//go:build vfs

package btree

import (
	"bytes"
	"context"
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

// onceReply answers the first read pick chooses with n bytes and err, and
// passes every other read to the file.
func onceReply(pick func(b []byte, off int64) bool, n int, err error) *readReply {
	var done atomic.Bool
	r := readReply(func(b []byte, off int64) (int, error, bool) {
		if !pick(b, off) || !done.CompareAndSwap(false, true) {
			return 0, nil, false
		}
		return min(n, len(b)), err, true
	})
	return &r
}

// The open's first read of the header, under the DB-file lock, that comes
// up short is judged by the file's size: a file that holds the header had
// the read come up short on its own, and the open returns the read's error
// (io.ErrUnexpectedEOF when it gave none), not ErrHeaderCorrupt, which a
// caller may discard a healthy database on. The same open without the
// short read succeeds. A file shorter than the header is ErrHeaderCorrupt.
func TestOpen_ShortReadOfTheHeaderIsAReadError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	opts := DefaultOptions()
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	commitRows(t, db, "t", 8, 100)
	require.NoError(t, db.Close())

	var reply atomic.Pointer[readReply]
	shortReadVFS(t, path, &reply)
	pageSize := int(opts.PageSize)
	for _, tc := range []struct {
		name string
		n    int
		err  error
		want error
	}{
		{"none, io.EOF", 0, io.EOF, io.EOF},
		{"64 bytes, io.EOF", 64, io.EOF, io.EOF},
		{"8 bytes, no error", 8, nil, io.ErrUnexpectedEOF},
	} {
		reply.Store(onceReply(func(b []byte, off int64) bool { return off == 0 && len(b) == pageSize }, tc.n, tc.err))
		_, err := Open(path, opts)
		reply.Store(nil)
		require.ErrorIs(t, err, tc.want, tc.name)
		require.ErrorContains(t, err, "failed to read database header", tc.name)
		require.NotErrorIs(t, err, ErrHeaderCorrupt, tc.name)
		require.NotErrorIs(t, err, ErrCorrupt, tc.name)

		db, err := Open(path, opts)
		require.NoError(t, err, tc.name)
		require.NoError(t, db.Close())
	}

	require.NoError(t, os.Truncate(path, dbHeaderSize-1))
	_, err = Open(path, opts)
	require.ErrorIs(t, err, ErrHeaderCorrupt)
}

// A file that does not open for writing has its header judged on a
// read-only descriptor (headerErrorOfUnwritableFile), only from a read that
// returned all of it: one that comes up short, with no error too, judges
// nothing, and the open returns the error of its open for writing, not
// ErrHeaderCorrupt from the zeros the read did not fill.
func TestOpen_ShortReadOfAnUnwritableFilesHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	opts := DefaultOptions()
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	var reply atomic.Pointer[readReply]
	SetVFS(VFS{OpenFile: func(n string, flag int, perm os.FileMode) (File, error) {
		if n == path && flag&(os.O_WRONLY|os.O_RDWR) != 0 {
			return nil, &os.PathError{Op: "open", Path: n, Err: os.ErrPermission}
		}
		f, err := defaultOpenFile(n, flag, perm)
		if err != nil || n != path {
			return f, err
		}
		return &shortReadFile{File: f, reply: &reply}, nil
	}})
	t.Cleanup(ResetVFS)

	for _, tc := range []struct {
		name string
		n    int
		err  error
	}{
		{"8 bytes, no error", 8, nil},
		{"none, io.EOF", 0, io.EOF},
	} {
		var read atomic.Bool
		r := readReply(func(b []byte, off int64) (int, error, bool) {
			if off != 0 || len(b) != dbHeaderSize {
				return 0, nil, false
			}
			read.Store(true)
			return min(tc.n, len(b)), tc.err, true
		})
		reply.Store(&r)
		_, err := Open(path, opts)
		reply.Store(nil)
		require.True(t, read.Load(), tc.name)
		require.ErrorIs(t, err, os.ErrPermission, tc.name)
		require.NotErrorIs(t, err, ErrCorrupt, tc.name)
	}
}

// VerifyIntegrity reads page 1's header on its own when page 1 cannot be
// read whole, and judges that read as every read of the database file: one
// that comes up short of a header the file holds, with no error too, is
// the read's error, and not a header of zeros, ErrHeaderCorrupt, which is
// the sweep's error where no WAL commit records the database's size.
func TestVerifyIntegrity_ShortReadOfPage1sHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	opts := DefaultOptions()
	opts.Checksum = true
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	commitRows(t, db, "t", 64, 200)
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	require.NoError(t, db.Close())

	var reply atomic.Pointer[readReply]
	shortReadVFS(t, path, &reply)
	db, err = testOpen(t, path, opts)
	require.NoError(t, err)
	defer db.Close()
	require.False(t, db.pager.dbMmap.enabled(), "the reads go to the file")

	// The read of the whole of page 1 fails, and the read of its header
	// that follows returns 8 bytes and no error.
	var page1Failed, headerRead atomic.Bool
	r := readReply(func(b []byte, off int64) (int, error, bool) {
		switch {
		case off != 0:
		case len(b) == int(opts.PageSize):
			page1Failed.Store(true)
			return 0, io.EOF, true
		case len(b) == dbHeaderSize && page1Failed.Load():
			headerRead.Store(true)
			return 8, nil, true
		}
		return 0, nil, false
	})
	reply.Store(&r)
	_, err = db.VerifyIntegrity(context.Background())
	reply.Store(nil)
	require.True(t, headerRead.Load())
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.NotErrorIs(t, err, ErrHeaderCorrupt)
	require.NotErrorIs(t, err, ErrCorrupt)

	res, err := db.VerifyIntegrity(context.Background())
	require.NoError(t, err)
	require.Empty(t, res.Errors)
}

// WAL recovery reads the log's header, which its caller found the file to
// hold, and a read of it that comes up short is the read's error
// (io.ErrUnexpectedEOF when it gave none): not ErrWALShortRead, since no
// wal-index has pointed at a frame, and not a header of zeros, which would
// start the log over and drop its commits. The open fails, the log is as
// it was, and the next open recovers its commits.
func TestWALRecovery_ShortReadOfTheHeader(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	opts := DefaultOptions()
	opts.InProcess = true
	opts.DisableAutoCheckpoint = true
	db, err := testOpen(t, src, opts)
	require.NoError(t, err)
	commitRows(t, db, "t", 16, 100)
	path := filepath.Join(dir, "test.db")
	for _, suffix := range []string{"", "-wal"} {
		data, err := os.ReadFile(src + suffix)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path+suffix, data, 0o600))
	}
	require.NoError(t, db.Close())
	wal, err := os.ReadFile(path + "-wal")
	require.NoError(t, err)
	require.Greater(t, len(wal), walHeaderSize)

	var reply atomic.Pointer[readReply]
	shortReadVFS(t, path+"-wal", &reply)
	for _, tc := range []struct {
		name string
		n    int
		err  error
		want error
	}{
		{"none, io.EOF", 0, io.EOF, io.EOF},
		{"8 bytes, no error", 8, nil, io.ErrUnexpectedEOF},
	} {
		reply.Store(onceReply(func(b []byte, off int64) bool { return off == 0 && len(b) == walHeaderSize }, tc.n, tc.err))
		_, err := Open(path, opts)
		reply.Store(nil)
		require.ErrorIs(t, err, tc.want, tc.name)
		require.ErrorContains(t, err, "read the WAL header", tc.name)
		require.NotErrorIs(t, err, ErrWALShortRead, tc.name)
		require.NotErrorIs(t, err, ErrCorrupt, tc.name)
		after, err := os.ReadFile(path + "-wal")
		require.NoError(t, err)
		require.Equal(t, wal, after, tc.name)
	}

	db, err = Open(path, opts)
	require.NoError(t, err)
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns, err := rtx.GetNamespace("t")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	defer cur.Close()
	rows := 0
	for err = cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		rows++
	}
	require.NoError(t, err)
	require.Equal(t, 16, rows)
}

// WAL recovery reads the log's frames within the file's size it took, and a
// read of one that comes up short is the read's error (io.ErrUnexpectedEOF
// when it gave none), as the header's is: not ErrWALShortRead, whose remedy,
// an open after every handle has closed, runs the recovery that failed, and
// not the end of the log. The open fails, the log is as it was, and the
// next open recovers its commits.
func TestWALRecovery_ShortReadOfAFrame(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.db")
	opts := DefaultOptions()
	opts.InProcess = true
	opts.DisableAutoCheckpoint = true
	db, err := testOpen(t, src, opts)
	require.NoError(t, err)
	commitRows(t, db, "t", 16, 100)
	path := filepath.Join(dir, "test.db")
	for _, suffix := range []string{"", "-wal"} {
		data, err := os.ReadFile(src + suffix)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path+suffix, data, 0o600))
	}
	require.NoError(t, db.Close())
	wal, err := os.ReadFile(path + "-wal")
	require.NoError(t, err)
	frameSize := walFrameSize + int(opts.PageSize)
	nFrames := (len(wal) - walHeaderSize) / frameSize
	require.Greater(t, nFrames, 1)
	last := int64(walHeaderSize + (nFrames-1)*frameSize)

	var reply atomic.Pointer[readReply]
	shortReadVFS(t, path+"-wal", &reply)
	for _, tc := range []struct {
		name string
		n    int
		err  error
		want error
	}{
		{"none, io.EOF", 0, io.EOF, io.EOF},
		{"half the frame, no error", frameSize / 2, nil, io.ErrUnexpectedEOF},
	} {
		reply.Store(onceReply(func(b []byte, off int64) bool { return off == last && len(b) == frameSize }, tc.n, tc.err))
		_, err := Open(path, opts)
		reply.Store(nil)
		require.ErrorIs(t, err, tc.want, tc.name)
		require.ErrorContains(t, err, "WAL recovery: read frame", tc.name)
		require.NotErrorIs(t, err, ErrWALShortRead, tc.name)
		require.NotErrorIs(t, err, ErrCorrupt, tc.name)
		after, err := os.ReadFile(path + "-wal")
		require.NoError(t, err)
		require.Equal(t, wal, after, tc.name)
	}

	db, err = Open(path, opts)
	require.NoError(t, err)
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns, err := rtx.GetNamespace("t")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	defer cur.Close()
	rows := 0
	for err = cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		rows++
	}
	require.NoError(t, err)
	require.Equal(t, 16, rows)
}

// A creation journal that holds its whole header and reads short is not
// taken for one that is not hot: the open fails with the read's error
// (io.ErrUnexpectedEOF when it gave none) and leaves the database file and
// the journal as they were. Taken for one that is not hot, it would stay
// beside a database the handle commits to, and the next open would play it
// back and truncate the database to nothing. The next open plays it back,
// as it plays back every dead creator's hot journal.
func TestCreationJournal_ShortReadOfAHotJournalFailsTheOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	opts := DefaultOptions()
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	journal := creationJournalHdr(opts.PageSize)
	require.NoError(t, os.WriteFile(path+creationJournalSuffix, journal, 0o644))

	var reply atomic.Pointer[readReply]
	shortReadVFS(t, path+creationJournalSuffix, &reply)
	for _, tc := range []struct {
		name string
		n    int
		err  error
		want error
	}{
		{"none, io.EOF", 0, io.EOF, io.EOF},
		{"10 bytes, io.EOF", 10, io.EOF, io.EOF},
		{"10 bytes, no error", 10, nil, io.ErrUnexpectedEOF},
	} {
		reply.Store(replyAt(0, tc.n, tc.err))
		_, err := Open(path, opts)
		reply.Store(nil)
		require.ErrorIs(t, err, tc.want, tc.name)
		require.NotErrorIs(t, err, ErrCorrupt, tc.name)
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, content, after, tc.name)
		j, err := os.ReadFile(path + creationJournalSuffix)
		require.NoError(t, err)
		require.Equal(t, journal, j, tc.name)
	}

	db, err = Open(path, opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	_, err = os.Stat(path + creationJournalSuffix)
	require.ErrorIs(t, err, os.ErrNotExist, "the journal was played back")
}
