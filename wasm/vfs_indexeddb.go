//go:build js && wasm

package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall/js"
	"time"

	anystore "github.com/anyproto/any-store/v2"
)

// ---------------------------------------------------------------------------
// IDBFS – filesystem backed by IndexedDB
// ---------------------------------------------------------------------------

// IDBFS implements a filesystem using the browser IndexedDB API. Files are loaded
// into memory on OpenFile; reads and writes operate on the in-memory buffer.
// Sync and Close flush dirty data back to IndexedDB.
type IDBFS struct {
	mu sync.RWMutex
	db js.Value // IDBDatabase reference
}

// InitIDBFS opens (or creates) the "anystore-vfs" IndexedDB database and
// returns a ready-to-use *IDBFS.
func InitIDBFS() (*IDBFS, error) {
	indexedDB := js.Global().Get("indexedDB")
	if indexedDB.IsUndefined() || indexedDB.IsNull() {
		self := js.Global().Get("self")
		if !self.IsUndefined() && !self.IsNull() {
			indexedDB = self.Get("indexedDB")
		}
	}
	if indexedDB.IsUndefined() || indexedDB.IsNull() {
		return nil, fmt.Errorf("indexeddb: API not available")
	}

	request := indexedDB.Call("open", "anystore-vfs", 2)

	ch := make(chan struct{})
	var database js.Value
	var openErr error

	onUpgrade := js.FuncOf(func(_ js.Value, args []js.Value) any {
		db := args[0].Get("target").Get("result")
		names := db.Get("objectStoreNames")
		if names.IsUndefined() || names.IsNull() || !names.Call("contains", "files").Bool() {
			opts := js.Global().Get("Object").New()
			opts.Set("keyPath", "path")
			db.Call("createObjectStore", "files", opts)
		}
		return nil
	})
	onSuccess := js.FuncOf(func(_ js.Value, args []js.Value) any {
		database = args[0].Get("target").Get("result")
		close(ch)
		return nil
	})
	onError := js.FuncOf(func(_ js.Value, _ []js.Value) any {
		openErr = fmt.Errorf("indexeddb: open failed: %s", request.Get("error").Call("toString").String())
		close(ch)
		return nil
	})

	request.Set("onupgradeneeded", onUpgrade)
	request.Set("onsuccess", onSuccess)
	request.Set("onerror", onError)

	<-ch
	onUpgrade.Release()
	onSuccess.Release()
	onError.Release()

	if openErr != nil {
		return nil, openErr
	}
	return &IDBFS{db: database}, nil
}

// ---------------------------------------------------------------------------
// IDB transaction helpers
// ---------------------------------------------------------------------------

func (f *IDBFS) idbGet(path string) (js.Value, error) {
	tx := f.db.Call("transaction", "files", "readonly")
	store := tx.Call("objectStore", "files")
	req := store.Call("get", path)
	return awaitIDBRequest(req)
}

func (f *IDBFS) idbPut(record js.Value) error {
	tx := f.db.Call("transaction", "files", "readwrite")
	store := tx.Call("objectStore", "files")
	req := store.Call("put", record)
	_, err := awaitIDBRequest(req)
	return err
}

func (f *IDBFS) idbDelete(path string) error {
	tx := f.db.Call("transaction", "files", "readwrite")
	store := tx.Call("objectStore", "files")
	req := store.Call("delete", path)
	_, err := awaitIDBRequest(req)
	return err
}

// makeRecord builds a JS object suitable for storing in the "files" object store.
func makeRecord(path string, data []byte, isDir bool, mode fs.FileMode, modTime time.Time) js.Value {
	rec := js.Global().Get("Object").New()
	rec.Set("path", path)
	if data != nil {
		rec.Set("data", goSliceToJS(data).Get("buffer"))
	} else {
		rec.Set("data", js.Null())
	}
	rec.Set("isDir", isDir)
	rec.Set("mode", int(mode))
	rec.Set("modTime", float64(modTime.UnixMilli()))
	return rec
}

// ---------------------------------------------------------------------------
// FS implementation
// ---------------------------------------------------------------------------

func (f *IDBFS) OpenFile(name string, flag int, perm fs.FileMode) (anystore.File, error) {
	name = filepath.Clean(name)

	f.mu.Lock()
	defer f.mu.Unlock()

	result, err := f.idbGet(name)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}

	exists := !result.IsUndefined() && !result.IsNull()

	if !exists {
		if flag&os.O_CREATE == 0 {
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
		}
		// Auto-create parent directories.
		dir := filepath.Dir(name)
		if dir != "." && dir != "/" {
			for _, seg := range mkdirSegments(dir) {
				segResult, segErr := f.idbGet(seg)
				if segErr != nil {
					return nil, &os.PathError{Op: "open", Path: name, Err: segErr}
				}
				if segResult.IsUndefined() || segResult.IsNull() {
					rec := makeRecord(seg, nil, true, 0755, time.Now())
					if putErr := f.idbPut(rec); putErr != nil {
						return nil, &os.PathError{Op: "open", Path: name, Err: putErr}
					}
				}
			}
		}
		// Create new empty file in IDB.
		now := time.Now()
		rec := makeRecord(name, []byte{}, false, perm, now)
		if putErr := f.idbPut(rec); putErr != nil {
			return nil, &os.PathError{Op: "open", Path: name, Err: putErr}
		}
		return &IDBFile{
			fs:      f,
			name:    name,
			data:    []byte{},
			mode:    perm,
			modTime: now,
		}, nil
	}

	// Entry exists.
	if flag&os.O_EXCL != 0 && flag&os.O_CREATE != 0 {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
	}

	// Copy data from IDB into memory.
	var fileData []byte
	dataVal := result.Get("data")
	if !dataVal.IsNull() && !dataVal.IsUndefined() {
		length := dataVal.Get("byteLength").Int()
		fileData = jsToGoSlice(dataVal, length)
	}

	mode := fs.FileMode(result.Get("mode").Int())
	modTime := time.UnixMilli(int64(result.Get("modTime").Float()))

	idbFile := &IDBFile{
		fs:      f,
		name:    name,
		data:    fileData,
		mode:    mode,
		modTime: modTime,
	}

	if flag&os.O_TRUNC != 0 {
		idbFile.data = idbFile.data[:0]
		idbFile.dirty = true
	}

	return idbFile, nil
}

func (f *IDBFS) MkdirAll(path string, perm fs.FileMode) error {
	path = filepath.Clean(path)

	f.mu.Lock()
	defer f.mu.Unlock()

	for _, seg := range mkdirSegments(path) {
		result, err := f.idbGet(seg)
		if err != nil {
			return &os.PathError{Op: "mkdir", Path: seg, Err: err}
		}
		if !result.IsUndefined() && !result.IsNull() {
			if !result.Get("isDir").Bool() {
				return &os.PathError{Op: "mkdir", Path: seg, Err: os.ErrExist}
			}
			continue
		}
		rec := makeRecord(seg, nil, true, perm, time.Now())
		if putErr := f.idbPut(rec); putErr != nil {
			return &os.PathError{Op: "mkdir", Path: seg, Err: putErr}
		}
	}
	return nil
}

func (f *IDBFS) Remove(name string) error {
	name = filepath.Clean(name)

	f.mu.Lock()
	defer f.mu.Unlock()

	result, err := f.idbGet(name)
	if err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: err}
	}
	if result.IsUndefined() || result.IsNull() {
		return &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
	}
	if delErr := f.idbDelete(name); delErr != nil {
		return &os.PathError{Op: "remove", Path: name, Err: delErr}
	}
	return nil
}

func (f *IDBFS) Stat(name string) (fs.FileInfo, error) {
	name = filepath.Clean(name)

	f.mu.RLock()
	defer f.mu.RUnlock()

	result, err := f.idbGet(name)
	if err != nil {
		return nil, &os.PathError{Op: "stat", Path: name, Err: err}
	}
	if result.IsUndefined() || result.IsNull() {
		return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
	}

	var size int64
	dataVal := result.Get("data")
	if !dataVal.IsNull() && !dataVal.IsUndefined() {
		size = int64(dataVal.Get("byteLength").Int())
	}

	return &memFileInfo{
		name:    filepath.Base(name),
		size:    size,
		mode:    fs.FileMode(result.Get("mode").Int()),
		modTime: time.UnixMilli(int64(result.Get("modTime").Float())),
		isDir:   result.Get("isDir").Bool(),
	}, nil
}

func (f *IDBFS) ReadFile(name string) ([]byte, error) {
	name = filepath.Clean(name)

	f.mu.RLock()
	defer f.mu.RUnlock()

	result, err := f.idbGet(name)
	if err != nil {
		return nil, &os.PathError{Op: "read", Path: name, Err: err}
	}
	if result.IsUndefined() || result.IsNull() {
		return nil, &os.PathError{Op: "read", Path: name, Err: os.ErrNotExist}
	}

	dataVal := result.Get("data")
	if dataVal.IsNull() || dataVal.IsUndefined() {
		return []byte{}, nil
	}
	length := dataVal.Get("byteLength").Int()
	return jsToGoSlice(dataVal, length), nil
}

func (f *IDBFS) WriteFile(name string, data []byte, perm fs.FileMode) error {
	name = filepath.Clean(name)

	f.mu.Lock()
	defer f.mu.Unlock()

	rec := makeRecord(name, data, false, perm, time.Now())
	if putErr := f.idbPut(rec); putErr != nil {
		return &os.PathError{Op: "write", Path: name, Err: putErr}
	}
	return nil
}

// ---------------------------------------------------------------------------
// IDBFile – anystore.File backed by an in-memory buffer + IndexedDB persistence
// ---------------------------------------------------------------------------

// IDBFile holds a file's contents in memory. Reads and writes operate on the
// in-memory buffer. Sync and Close flush dirty data back to IndexedDB.
type IDBFile struct {
	fs      *IDBFS
	name    string
	mu      sync.RWMutex
	data    []byte
	mode    fs.FileMode
	modTime time.Time
	dirty   bool
}

func (f *IDBFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if int(off) >= len(f.data) {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: os.ErrClosed}
	}
	n := copy(p, f.data[off:])
	return n, nil
}

func (f *IDBFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	end := int(off) + len(p)
	if end > len(f.data) {
		grown := make([]byte, end)
		copy(grown, f.data)
		f.data = grown
	}
	copy(f.data[off:], p)
	f.modTime = time.Now()
	f.dirty = true
	return len(p), nil
}

func (f *IDBFile) Stat() (fs.FileInfo, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &memFileInfo{
		name:    filepath.Base(f.name),
		size:    int64(len(f.data)),
		mode:    f.mode,
		modTime: f.modTime,
		isDir:   false,
	}, nil
}

func (f *IDBFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if int(size) < len(f.data) {
		f.data = f.data[:size]
	} else if int(size) > len(f.data) {
		grown := make([]byte, size)
		copy(grown, f.data)
		f.data = grown
	}
	f.modTime = time.Now()
	f.dirty = true
	return nil
}

func (f *IDBFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dirty {
		return nil
	}
	rec := makeRecord(f.name, f.data, false, f.mode, f.modTime)
	f.fs.mu.Lock()
	err := f.fs.idbPut(rec)
	f.fs.mu.Unlock()
	if err != nil {
		return &os.PathError{Op: "sync", Path: f.name, Err: err}
	}
	f.dirty = false
	return nil
}

// Fd returns 0 since IndexedDB has no real file descriptor.
func (f *IDBFile) Fd() uintptr { return 0 }

func (f *IDBFile) Close() error {
	return f.Sync()
}
