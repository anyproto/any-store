//go:build !unix && !windows

package btree

// Single-process stubs for targets without a usable cross-process file lock
// (wasm/js). These platforms run with InProcess=true forced (db.go via
// hasMmapShm=false) and have no second-process notion, so all DB-file lock
// primitives — including acquireExclusiveDBLock — are no-ops. They must still
// compile because the pager open/close paths reference them unconditionally.
// (Windows has its own real exclusive lock in dbfile_lock_windows.go.)

func acquireSharedDBLock(_ fileHandle) error               { return nil }
func tryUpgradeDBLockExclusive(_ fileHandle) (bool, error) { return true, nil }
func downgradeDBLockToShared(_ fileHandle) error           { return nil }
func acquireExclusiveDBLock(_ fileHandle) error            { return nil }
