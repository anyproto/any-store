//go:build js && wasm

package main

import (
	"io/fs"
	"syscall/js"

	anystore "github.com/anyproto/any-store"
)

// VFS backend name constants.
const (
	VFSBackendOPFS      = "opfs"
	VFSBackendIndexedDB = "indexeddb"
	VFSBackendMemory    = "memory"
)

// DetectVFS detects the best available storage backend for the current
// browser environment, configures anystore's VFS hook, and returns the
// backend name. Detection priority:
//
//  1. OPFS (Origin Private File System) — requires Web Worker context
//  2. IndexedDB — universal persistent fallback
//  3. MemFS — in-memory, no persistence (always available)
//
// Must be called before anystore.Open. Subsequent OS operations panic
// under wasm if this is not called first.
func DetectVFS() string {
	console := js.Global().Get("console")
	log := func(msg string) {
		if !console.IsUndefined() && !console.IsNull() {
			console.Call("log", "[anystore-vfs] "+msg)
		}
	}

	if hasOPFS() {
		opfs, err := InitOPFS()
		if err == nil {
			log("using OPFS backend (persistent, high-performance)")
			setBackendFuncs(opfs)
			return VFSBackendOPFS
		}
		log("OPFS detected but init failed: " + err.Error())
	}

	if hasIndexedDB() {
		idb, err := InitIDBFS()
		if err == nil {
			log("using IndexedDB backend (persistent, universal)")
			setBackendFuncs(idb)
			return VFSBackendIndexedDB
		}
		log("IndexedDB detected but init failed: " + err.Error())
	}

	log("using in-memory backend (no persistence)")
	mem := &MemFS{}
	setBackendFuncs(mem)
	return VFSBackendMemory
}

// backend is the interface all three VFS implementations satisfy.
// anystore only consumes OpenFile + Remove; the other methods live on
// the concrete types for downstream app-level use and are not wired here.
type backend interface {
	OpenFile(name string, flag int, perm fs.FileMode) (anystore.File, error)
	Remove(name string) error
}

func setBackendFuncs(b backend) {
	anystore.SetVFS(anystore.VFS{
		OpenFile: b.OpenFile,
		Remove:   b.Remove,
		Fdatasync: func(f anystore.File) error {
			return f.Sync()
		},
	})
}

// hasOPFS checks whether the Origin Private File System API is available.
// OPFS sync access handles require a dedicated Worker context.
func hasOPFS() bool {
	navigator := js.Global().Get("navigator")
	if navigator.IsUndefined() || navigator.IsNull() {
		return false
	}
	storage := navigator.Get("storage")
	if storage.IsUndefined() || storage.IsNull() {
		return false
	}
	getDir := storage.Get("getDirectory")
	if getDir.IsUndefined() || getDir.IsNull() {
		return false
	}
	// createSyncAccessHandle is only available in dedicated Workers.
	wgs := js.Global().Get("WorkerGlobalScope")
	return !wgs.IsUndefined()
}

// hasIndexedDB checks whether the IndexedDB API is available.
func hasIndexedDB() bool {
	idb := js.Global().Get("indexedDB")
	if !idb.IsUndefined() && !idb.IsNull() {
		return true
	}
	self := js.Global().Get("self")
	if !self.IsUndefined() && !self.IsNull() {
		idb = self.Get("indexedDB")
		return !idb.IsUndefined() && !idb.IsNull()
	}
	return false
}
