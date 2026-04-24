//go:build js && wasm

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall/js"
	"time"

	anystore "github.com/anyproto/any-store"
)

// OPFSFS implements a filesystem using the Origin Private File System (OPFS) browser API.
// It must be used in a Web Worker context where synchronous access handles are
// available.
type OPFSFS struct {
	root js.Value // FileSystemDirectoryHandle for the OPFS root
}

// InitOPFS creates an OPFSFS by obtaining the OPFS root directory via
// navigator.storage.getDirectory().
func InitOPFS() (*OPFSFS, error) {
	storage := js.Global().Get("navigator").Get("storage")
	promise := storage.Call("getDirectory")
	root, err := awaitPromise(promise)
	if err != nil {
		return nil, fmt.Errorf("opfs: get root directory: %w", err)
	}
	return &OPFSFS{root: root}, nil
}

// getDirectoryHandle traverses directory segments starting from root,
// optionally creating them. Returns the final directory handle.
func (o *OPFSFS) getDirectoryHandle(path string, create bool) (js.Value, error) {
	segments := splitPathSegments(path)
	dir := o.root
	for _, seg := range segments {
		opts := js.Global().Get("Object").New()
		opts.Set("create", create)
		promise := dir.Call("getDirectoryHandle", seg, opts)
		next, err := awaitPromise(promise)
		if err != nil {
			if !create && isJSNotFound(err) {
				return js.Value{}, os.ErrNotExist
			}
			return js.Value{}, fmt.Errorf("opfs: getDirectoryHandle(%q): %w", seg, err)
		}
		dir = next
	}
	return dir, nil
}

// getParentAndBase splits a path into the parent directory handle and the
// base filename. The parent directories are optionally created.
func (o *OPFSFS) getParentAndBase(name string, createParent bool) (js.Value, string, error) {
	name = filepath.Clean(name)
	dir := filepath.Dir(name)
	base := filepath.Base(name)
	parent, err := o.getDirectoryHandle(dir, createParent)
	if err != nil {
		return js.Value{}, "", err
	}
	return parent, base, nil
}

// OpenFile opens (or creates) a file in OPFS and returns a sync access handle
// wrapped as an anystore.File.
func (o *OPFSFS) OpenFile(name string, flag int, perm fs.FileMode) (anystore.File, error) {
	name = filepath.Clean(name)
	createFile := flag&os.O_CREATE != 0

	parent, base, err := o.getParentAndBase(name, createFile)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}

	if flag&os.O_EXCL != 0 && flag&os.O_CREATE != 0 {
		// Check if file already exists -- getFileHandle without create throws
		// if not found, so we try without create first.
		checkOpts := js.Global().Get("Object").New()
		checkOpts.Set("create", false)
		checkPromise := parent.Call("getFileHandle", base, checkOpts)
		_, checkErr := awaitPromise(checkPromise)
		if checkErr == nil {
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
		}
	}

	opts := js.Global().Get("Object").New()
	opts.Set("create", createFile)
	fhPromise := parent.Call("getFileHandle", base, opts)
	fileHandle, err := awaitPromise(fhPromise)
	if err != nil {
		if !createFile && isJSNotFound(err) {
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
		}
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}

	sahPromise := fileHandle.Call("createSyncAccessHandle")
	syncHandle, err := awaitPromise(sahPromise)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: fmt.Errorf("createSyncAccessHandle: %w", err)}
	}

	if flag&os.O_TRUNC != 0 {
		syncHandle.Call("truncate", 0)
	}

	return &OPFSFile{
		handle: syncHandle,
		name:   base,
	}, nil
}

// MkdirAll creates a directory path and all parents that do not yet exist.
func (o *OPFSFS) MkdirAll(path string, perm fs.FileMode) error {
	_, err := o.getDirectoryHandle(path, true)
	return err
}

// Remove deletes a file or empty directory by name.
func (o *OPFSFS) Remove(name string) error {
	name = filepath.Clean(name)
	parent, base, err := o.getParentAndBase(name, false)
	if err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: err}
	}
	promise := parent.Call("removeEntry", base)
	if _, err := awaitPromise(promise); err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: err}
	}
	return nil
}

// Stat returns file info for the named file. It opens a temporary sync access
// handle to obtain the file size.
func (o *OPFSFS) Stat(name string) (fs.FileInfo, error) {
	name = filepath.Clean(name)
	parent, base, err := o.getParentAndBase(name, false)
	if err != nil {
		return nil, &os.PathError{Op: "stat", Path: name, Err: err}
	}

	// Try as file first.
	opts := js.Global().Get("Object").New()
	opts.Set("create", false)
	fhPromise := parent.Call("getFileHandle", base, opts)
	fileHandle, fhErr := awaitPromise(fhPromise)
	if fhErr == nil {
		sahPromise := fileHandle.Call("createSyncAccessHandle")
		syncHandle, sahErr := awaitPromise(sahPromise)
		if sahErr != nil {
			return nil, &os.PathError{Op: "stat", Path: name, Err: fmt.Errorf("createSyncAccessHandle: %w", sahErr)}
		}
		size := syncHandle.Call("getSize").Int()
		syncHandle.Call("close")
		return &opfsFileInfo{
			name:  base,
			size:  int64(size),
			mode:  0644,
			isDir: false,
		}, nil
	}

	// Try as directory.
	dirOpts := js.Global().Get("Object").New()
	dirOpts.Set("create", false)
	dhPromise := parent.Call("getDirectoryHandle", base, dirOpts)
	_, dhErr := awaitPromise(dhPromise)
	if dhErr == nil {
		return &opfsFileInfo{
			name:  base,
			size:  0,
			mode:  fs.ModeDir | 0755,
			isDir: true,
		}, nil
	}

	return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
}

// ReadFile reads the entire contents of the named file.
func (o *OPFSFS) ReadFile(name string) ([]byte, error) {
	f, err := o.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	n, err := f.ReadAt(buf, 0)
	if err != nil && n == 0 {
		return nil, err
	}
	return buf[:n], nil
}

// WriteFile writes data to the named file, creating it if necessary and
// truncating it if it already exists.
func (o *OPFSFS) WriteFile(name string, data []byte, perm fs.FileMode) error {
	f, err := o.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = f.WriteAt(data, 0)
	if err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// isJSNotFound returns true when the error originates from a JavaScript
// DOMException with name "NotFoundError" — the standard OPFS error when
// getFileHandle or getDirectoryHandle is called with {create: false} on a
// non-existent entry.
func isJSNotFound(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "NotFoundError") || strings.Contains(s, "not found")
}

// splitPathSegments splits a cleaned path into directory segments, stripping
// leading slashes and dots.
func splitPathSegments(path string) []string {
	path = filepath.Clean(path)
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimPrefix(path, ".")
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

// =========================================================================
// OPFSFile implements anystore.File using a FileSystemSyncAccessHandle.
// =========================================================================

// OPFSFile wraps a OPFS FileSystemSyncAccessHandle. The read/write/truncate/
// flush/close/getSize methods on the sync access handle are synchronous (they
// return values directly, not Promises) when used in a Worker context.
type OPFSFile struct {
	handle js.Value // FileSystemSyncAccessHandle
	name   string
}

// ReadAt reads len(p) bytes from the file starting at byte offset off.
func (f *OPFSFile) ReadAt(p []byte, off int64) (int, error) {
	buf := js.Global().Get("Uint8Array").New(len(p))
	opts := js.Global().Get("Object").New()
	opts.Set("at", off)
	n := f.handle.Call("read", buf, opts).Int()
	js.CopyBytesToGo(p, buf)
	return n, nil
}

// WriteAt writes len(p) bytes to the file starting at byte offset off.
func (f *OPFSFile) WriteAt(p []byte, off int64) (int, error) {
	buf := goSliceToJS(p)
	opts := js.Global().Get("Object").New()
	opts.Set("at", off)
	n := f.handle.Call("write", buf, opts).Int()
	return n, nil
}

// Stat returns file information including the current size via getSize().
func (f *OPFSFile) Stat() (fs.FileInfo, error) {
	size := f.handle.Call("getSize").Int()
	return &opfsFileInfo{
		name: f.name,
		size: int64(size),
		mode: 0644,
	}, nil
}

// Truncate changes the size of the file.
func (f *OPFSFile) Truncate(size int64) error {
	f.handle.Call("truncate", size)
	return nil
}

// Sync flushes pending writes to stable storage.
func (f *OPFSFile) Sync() error {
	f.handle.Call("flush")
	return nil
}

// Fd returns 0 since OPFS has no real file descriptor.
func (f *OPFSFile) Fd() uintptr { return 0 }

// Close releases the sync access handle.
func (f *OPFSFile) Close() error {
	f.handle.Call("close")
	return nil
}

// =========================================================================
// opfsFileInfo implements fs.FileInfo.
// =========================================================================

type opfsFileInfo struct {
	name  string
	size  int64
	mode  fs.FileMode
	isDir bool
}

func (fi *opfsFileInfo) Name() string       { return fi.name }
func (fi *opfsFileInfo) Size() int64        { return fi.size }
func (fi *opfsFileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi *opfsFileInfo) ModTime() time.Time { return time.Time{} }
func (fi *opfsFileInfo) IsDir() bool        { return fi.isDir }
func (fi *opfsFileInfo) Sys() interface{}   { return nil }
