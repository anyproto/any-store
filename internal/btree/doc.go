// Package btree implements an embedded, crash-safe, ordered key-value database
// engine in pure Go, closely following SQLite's design for the B-tree, pager,
// WAL, and transaction subsystems.
//
// # Architecture
//
// The engine is structured around four core components, mirroring SQLite internals:
//
//   - Page format: Fixed-size pages with SQLite-compatible cell layout
//   - Pager: Manages page cache, reads/writes pages, coordinates with WAL
//   - WAL (Write-Ahead Log): Provides crash-safe durability with WAL mode
//   - B-tree: Ordered key-value storage with variable-length keys and values
//
// # File Layout
//
//   - Database file (.db): Pages stored sequentially, page 1 contains the DB header
//   - WAL file (.db-wal): Append-only log of committed page changes
//   - Shared memory file (.db-shm): WAL index for coordinating readers/writers
//
// # Transaction Model
//
//   - Single-writer, multiple-reader (SQLite-style)
//   - Read transactions see a consistent snapshot via WAL
//   - Write transactions support nested savepoints
//
// # Cache Spill
//
// During large write transactions, the page cache may fill up. When this
// happens, the pagerStress callback spills dirty pages to the WAL
// mid-transaction (without a commit marker), marks them clean, and makes
// them evictable. This bounds memory usage to approximately CacheSize pages
// regardless of transaction size. Spilled frames are invisible to readers
// until commit, matching SQLite's pagerStress design (pager.c:4609-4681).
//
// # VFS Build Tag
//
// By default the engine calls *os.File methods directly with zero interface
// overhead. Building with -tags vfs switches to an interface-based I/O path
// that allows replacing OS operations at runtime via SetVFS/ResetVFS for
// fault injection in tests. Without the tag, SetVFS panics.
//
//	go test -tags vfs ./internal/btree/ -run TestCheckpoint
//
// # Namespaces
//
// Data is organized into namespaces, each backed by a separate B-tree root page.
// A master namespace table (on page 1) tracks all namespace root pages.
package btree
