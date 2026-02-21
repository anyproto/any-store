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
// # Namespaces
//
// Data is organized into namespaces, each backed by a separate B-tree root page.
// A master namespace table (on page 1) tracks all namespace root pages.
//
// # Build Tags
//
//   - nopwritev: Disable pwritev scatter-gather I/O for WAL writes.
//     Forces the copy-based fallback (single pwrite per batch) on all platforms.
//     Useful for debugging or working around kernel bugs.
//
//     go build  -tags nopwritev ./...
//     go test   -tags nopwritev ./...
package btree
