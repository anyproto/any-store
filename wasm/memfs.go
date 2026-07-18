//go:build js && wasm

package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	anystore "github.com/anyproto/any-store/v2"
)

// MemFS implements a purely in-memory filesystem.
// Safe for concurrent use.
type MemFS struct {
	mu    sync.RWMutex
	files map[string]*fileData
}

type fileData struct {
	mu      sync.RWMutex
	data    []byte
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
}

func (m *MemFS) ensureInit() {
	if m.files == nil {
		m.files = make(map[string]*fileData)
	}
}

func (m *MemFS) OpenFile(name string, flag int, perm fs.FileMode) (anystore.File, error) {
	name = filepath.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureInit()

	fd, exists := m.files[name]
	if !exists {
		if flag&os.O_CREATE == 0 {
			return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
		}
		// Auto-create parent directories for O_CREATE.
		dir := filepath.Dir(name)
		if dir != "." && dir != "/" {
			for _, seg := range mkdirSegments(dir) {
				if _, ok := m.files[seg]; !ok {
					m.files[seg] = &fileData{isDir: true, mode: 0755, modTime: time.Now()}
				}
			}
		}
		fd = &fileData{mode: perm, modTime: time.Now()}
		m.files[name] = fd
	} else if flag&os.O_EXCL != 0 && flag&os.O_CREATE != 0 {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrExist}
	}

	if flag&os.O_TRUNC != 0 {
		fd.mu.Lock()
		fd.data = fd.data[:0]
		fd.mu.Unlock()
	}

	return &memFile{name: name, fd: fd}, nil
}

func (m *MemFS) MkdirAll(path string, perm fs.FileMode) error {
	path = filepath.Clean(path)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureInit()

	for _, seg := range mkdirSegments(path) {
		if fd, ok := m.files[seg]; ok {
			if !fd.isDir {
				return &os.PathError{Op: "mkdir", Path: seg, Err: os.ErrExist}
			}
			continue
		}
		m.files[seg] = &fileData{isDir: true, mode: perm, modTime: time.Now()}
	}
	return nil
}

func (m *MemFS) Remove(name string) error {
	name = filepath.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureInit()

	if _, ok := m.files[name]; !ok {
		return &os.PathError{Op: "remove", Path: name, Err: os.ErrNotExist}
	}
	delete(m.files, name)
	return nil
}

func (m *MemFS) Stat(name string) (fs.FileInfo, error) {
	name = filepath.Clean(name)
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.ensureInit()

	fd, ok := m.files[name]
	if !ok {
		return nil, &os.PathError{Op: "stat", Path: name, Err: os.ErrNotExist}
	}
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	return &memFileInfo{
		name:    filepath.Base(name),
		size:    int64(len(fd.data)),
		mode:    fd.mode,
		modTime: fd.modTime,
		isDir:   fd.isDir,
	}, nil
}

func (m *MemFS) ReadFile(name string) ([]byte, error) {
	name = filepath.Clean(name)
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.ensureInit()

	fd, ok := m.files[name]
	if !ok {
		return nil, &os.PathError{Op: "read", Path: name, Err: os.ErrNotExist}
	}
	fd.mu.RLock()
	defer fd.mu.RUnlock()
	cp := make([]byte, len(fd.data))
	copy(cp, fd.data)
	return cp, nil
}

func (m *MemFS) WriteFile(name string, data []byte, perm fs.FileMode) error {
	name = filepath.Clean(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureInit()

	fd, ok := m.files[name]
	if !ok {
		fd = &fileData{mode: perm}
		m.files[name] = fd
	}
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.data = make([]byte, len(data))
	copy(fd.data, data)
	fd.modTime = time.Now()
	return nil
}

// --- memFile implements anystore.File ---

type memFile struct {
	name string
	fd   *fileData
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.fd.mu.RLock()
	defer f.fd.mu.RUnlock()
	if int(off) >= len(f.fd.data) {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: os.ErrClosed}
	}
	n := copy(p, f.fd.data[off:])
	return n, nil
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.fd.mu.Lock()
	defer f.fd.mu.Unlock()
	end := int(off) + len(p)
	if end > len(f.fd.data) {
		grown := make([]byte, end)
		copy(grown, f.fd.data)
		f.fd.data = grown
	}
	copy(f.fd.data[off:], p)
	f.fd.modTime = time.Now()
	return len(p), nil
}

func (f *memFile) Stat() (fs.FileInfo, error) {
	f.fd.mu.RLock()
	defer f.fd.mu.RUnlock()
	return &memFileInfo{
		name:    filepath.Base(f.name),
		size:    int64(len(f.fd.data)),
		mode:    f.fd.mode,
		modTime: f.fd.modTime,
		isDir:   f.fd.isDir,
	}, nil
}

func (f *memFile) Truncate(size int64) error {
	f.fd.mu.Lock()
	defer f.fd.mu.Unlock()
	if int(size) < len(f.fd.data) {
		f.fd.data = f.fd.data[:size]
	} else if int(size) > len(f.fd.data) {
		grown := make([]byte, size)
		copy(grown, f.fd.data)
		f.fd.data = grown
	}
	f.fd.modTime = time.Now()
	return nil
}

func (f *memFile) Sync() error  { return nil }
func (f *memFile) Fd() uintptr  { return 0 }
func (f *memFile) Close() error { return nil }

// --- memFileInfo implements fs.FileInfo ---

type memFileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *memFileInfo) Name() string       { return fi.name }
func (fi *memFileInfo) Size() int64        { return fi.size }
func (fi *memFileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi *memFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *memFileInfo) IsDir() bool        { return fi.isDir }
func (fi *memFileInfo) Sys() any           { return nil }

// mkdirSegments returns cumulative path segments for MkdirAll-style creation.
// e.g. "/tmp/any-store-wasm" -> ["/tmp", "/tmp/any-store-wasm"]
func mkdirSegments(path string) []string {
	parts := splitPath(filepath.Clean(path))
	segs := make([]string, len(parts))
	for i, p := range parts {
		if i == 0 {
			segs[i] = p
		} else {
			segs[i] = segs[i-1] + "/" + p
		}
	}
	return segs
}

// splitPath breaks a cleaned path into its components.
func splitPath(path string) []string {
	if path == "/" || path == "." {
		return []string{path}
	}
	var parts []string
	for {
		dir, base := filepath.Split(path)
		if base != "" {
			parts = append([]string{base}, parts...)
		}
		path = filepath.Clean(dir)
		if path == "." || path == "/" {
			if path == "/" {
				parts = append([]string{"/"}, parts...)
			}
			break
		}
	}
	return parts
}
