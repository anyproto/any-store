//go:build !btreetesthooks

package btree

// walTestHooks gates test-only fault-injection hooks — the Go analog of
// SQLite's SQLITE_TEST-gated helpers. False in default builds so every
// hook check compiles away from production hot paths.
// DRIFT: build-tag-gated test-hook toggle See docs/btree/NOTES.md#drift-131-build-tag-gated-test-fault-hooks
const walTestHooks = false
