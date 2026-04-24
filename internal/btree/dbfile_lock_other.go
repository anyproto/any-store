//go:build !unix

package btree

// Single-process stubs. On non-unix targets (wasm, Windows) any-store runs
// with InProcess=true forced (db.go:201-204 via hasMmapShm=false), so these
// lock primitives are dead code at runtime — but the pager open/close paths
// reference them unconditionally, so they must still compile.

func acquireSharedDBLock(_ fileHandle) error               { return nil }
func tryUpgradeDBLockExclusive(_ fileHandle) (bool, error) { return true, nil }
func downgradeDBLockToShared(_ fileHandle) error           { return nil }
