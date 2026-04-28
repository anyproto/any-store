# SQLite Online Backup Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port SQLite's `sqlite3_backup_*` online-backup API (from `/home/dev/work/sqlitec/src/backup.c`) into any-store's btree layer, then replace the current naive file-copy `DB.Backup()` with a call into the new engine — producing a drift-free port that supports concurrent-write online backup.

**Architecture:** A new `internal/btree/Backup` type mirrors the C `struct sqlite3_backup` (backup.c:21–41). It copies the source DB page-by-page into a destination `*btree.DB` under a destination write-transaction. The source pager gains a registered-backups list and an update/restart hook pair (C `sqlite3BackupUpdate` / `sqlite3BackupRestart` from backup.c:661–707), invoked from the end of `pager.commit()` so that pages already copied are re-synced from the source's post-commit data. The any-store-level `db.Backup(ctx, path)` is rewritten to open a fresh destination DB at `path` and drive `Backup.Step → Finish` to completion.

**Tech Stack:** Go 1.24+, any-store internal btree/pager/WAL, stdlib only. No new deps.

**Intentional DRIFT from SQLite** — every divergence below is either (a) required by an any-store architectural constraint, or (b) a C code path that is unreachable given our constraints. No behavioral feature of backup.c is silently dropped; every omission is either justified here or turned into a task below.

1. **No `PENDING_BYTE_PAGE`.** SQLite reserves a lock byte at the ~1GB mark (C macro `PENDING_BYTE_PAGE(pBt)` used at backup.c:243, 254, 392, 452, 480, 492). any-store has no such concept — pages are copied contiguously. **Skipped:** all `PENDING_BYTE_PAGE` guards.
2. **Single b-tree per handle.** SQLite has attached databases — `zSrcDb`/`zDestDb` name strings and a `findBtree` resolver (backup.c:82–106). any-store `*btree.DB` is one b-tree container. The Go `BackupInit` takes `(dst, src *btree.DB)` directly. **Skipped:** `findBtree`, the `zSrcDb`/`zDestDb` parameters, the `pDestDb`/`pSrcDb` sqlite3* vs Btree* distinction.
3. **No `SQLITE_OMIT_VACUUM` wrapper.** `sqlite3BtreeCopyFile` (backup.c:718–767) is a thin wrapper used by VACUUM. any-store has no VACUUM. **Skipped:** that whole function.
4. **Mutex strategy.** SQLite enters three mutexes (source db, dest db, btree-shared) per call (backup.c:163–164, 323–327, 579–583). any-store uses `DB.writeMu` + `DB.mu` + the existing `writerOpMu`. We rely on those; no new mutexes.
5. **Cross-page-size backup code path is unreachable, not skipped.** backup.c:378–383 rejects WAL-mode destinations with `SQLITE_READONLY` when page sizes differ, AND rejects memdb destinations the same way. any-store is *always* WAL-mode (there is no rollback-journal backend). Therefore the cross-page-size packing path at backup.c:449–528 is never entered in SQLite's own code when dst is WAL. We match SQLite's WAL behavior by returning `ErrBackupPageSizeMismatch` at `BackupInit`, which is the faithful port of backup.c:378–383 given our WAL-only constraint. **Not drift — faithful by construction.**
6. **`nBackup` counter on source (backup.c:204, 587).** Increments/decrements a counter on the source Btree to block certain operations during backup (page-size changes, VACUUM). any-store has immutable page size and no VACUUM, so there is nothing for this counter to block. **Faithful omission; no equivalent needed.**

All **kept** C behaviors retain their original identifier comments (e.g. `// ~ sqlite3_backup_step (backup.c:314)`) so future readers can diff against upstream.

**Drifts that were caught during self-review and turned into tasks** (not in the list above because they are ported, not skipped):

- `checkReadTransaction` at init (backup.c:124–130) → **Task 8**
- Schema-cookie bump on completion (backup.c:423) → **Task 9**
- Destination truncation on completion (backup.c:530) → **Task 9**
- External-process write detection → restart (backup.c:701–707, driven from `sqlite3BackupRestart` callers in pager internals) → **Task 10**

---

## File Structure

**New files:**

- `internal/btree/backup.go` — `Backup` struct, all `Backup.*` methods (`Step`, `Finish`, `Remaining`, `PageCount`, `update`, `restart`, `onePage`, `finalize`). One focused file, mirrors `backup.c`'s single-file layout.
- `internal/btree/backup_test.go` — low-level btree-layer tests: offline copy, `Step`/`Remaining`/`PageCount` semantics, online-backup-across-commits, source-dest-equal rejection, page-size-mismatch rejection, dst-has-open-tx rejection, larger-dst truncation, schema-cookie bump, external-process-write restart, Finish-without-Step.

**Modified files:**

- `internal/btree/db.go` — add public accessors `PageSize`, `DatabaseSize`, `HasOpenTransaction`, `Options`, and constructor `BackupInit(src *DB)`.
- `internal/btree/pager.go` — add `backups []*Backup` + `backupsMu sync.Mutex` fields, `attachBackup`/`detachBackup`/`dispatchBackupUpdate`/`dispatchBackupRestart` methods, and `truncateTo(newDbSize uint32) error`. Wire dispatchBackupUpdate into `commit` post-`writeFrames`; wire dispatchBackupRestart into `checkpointWithMode` and `tryCheckpoint` on Restart/Truncate modes.
- `db.go` (top-level) — rewrite `(*db).Backup(ctx, path)` at db.go:506–520 to open a fresh destination btree.DB at `path` with the source's Options, drive `BackupInit → Step(batch) → Finish`, close the destination.
- `db_test.go` — drop the `ANYSTORE_TEST_INMEMORY` skip and add `TestDb_Backup_OnlineDuringWrites`.

**Untouched:** `wal.go`, `pcache.go`, `page.go` (header layout already matches SQLite's offset 28–31 for `DatabaseSize`). No changes to the cache layer.

---

## Task 1: Expose `PageSize()` and `DatabaseSize()` on `*btree.DB`

**Why:** Backup needs both. C analogues: `sqlite3BtreeGetPageSize` (btree.c) and `sqlite3BtreeLastPage` (btree.c). Currently any-store only exposes `Path()`; page size is buried in `pager.pageSize` and `pager.dbSize`.

**Files:**
- Modify: `internal/btree/db.go` (add two methods right after `Path()` at line 311)
- Test: `internal/btree/db_test.go` (create if absent) — there's no existing `db_test.go` at the btree layer; tests live in topic-named files. Put these in `internal/btree/backup_test.go` since that's where they'll be used — but the getters deserve their own quick test. Put them in `internal/btree/btree_test.go` alongside the existing `Path()` test at line 71.

- [ ] **Step 1: Write failing test for `PageSize`/`DatabaseSize` in btree_test.go**

Append at the end of `/home/dev/work/any-store/internal/btree/btree_test.go`:

```go
func TestDB_PageSizeAndDatabaseSize(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Fresh DB: 1 header page + 1 namespace root = 2 pages minimum.
	// DatabaseSize is read from the in-memory header via Load().
	if ps := db.PageSize(); ps != DefaultPageSize {
		t.Fatalf("PageSize = %d, want %d", ps, DefaultPageSize)
	}
	if sz := db.DatabaseSize(); sz < 2 {
		t.Fatalf("DatabaseSize = %d, want >= 2", sz)
	}

	// Insert enough to force growth, then re-check.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())
	if sz := db.DatabaseSize(); sz < 3 {
		t.Fatalf("DatabaseSize after inserts = %d, want >= 3", sz)
	}
}
```

- [ ] **Step 2: Run test — expect compile failure**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestDB_PageSizeAndDatabaseSize -count=1`
Expected: `db.PageSize undefined` / `db.DatabaseSize undefined`.

- [ ] **Step 3: Add the two methods to `internal/btree/db.go`**

Insert immediately after `Path()` at line 313 (after the closing `}` of `Path`):

```go
// PageSize returns the fixed page size in bytes for this database.
// ~ sqlite3BtreeGetPageSize (btree.c). Unlike SQLite, our page size is
// immutable after Open — set from Options.PageSize or read from the header.
func (db *DB) PageSize() uint32 {
	return db.pager.pageSize
}

// DatabaseSize returns the current number of pages in the database,
// including page 1 (the header page). ~ sqlite3BtreeLastPage (btree.c).
// Reads the atomic pager.dbSize directly; safe under any concurrent
// transaction state because dbSize is monotonic within the current
// WAL snapshot visible to the caller.
func (db *DB) DatabaseSize() uint32 {
	return db.pager.dbSize.Load()
}
```

- [ ] **Step 4: Run test — expect PASS**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestDB_PageSizeAndDatabaseSize -count=1 -race`
Expected: PASS.

- [ ] **Step 5: Run full btree test suite to confirm no regression**

Run: `cd /home/dev/work/any-store/internal/btree && go test -short -count=1 -timeout=120s ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /home/dev/work/any-store
git add internal/btree/db.go internal/btree/btree_test.go
git -c commit.gpgsign=false commit -m "btree: expose DB.PageSize and DB.DatabaseSize

Public accessors needed by the upcoming Backup port. Mirrors
sqlite3BtreeGetPageSize / sqlite3BtreeLastPage from SQLite btree.c."
```

---

## Task 2: Introduce the `Backup` struct and `BackupInit` factory

**Why:** Mirror `struct sqlite3_backup` (backup.c:21–41) and `sqlite3_backup_init` (backup.c:140–210), excluding the DB-attachment-name plumbing (see DRIFT #3). This task creates the struct and factory only; `Step`/`Finish` come next.

**Files:**
- Create: `internal/btree/backup.go`
- Modify: `internal/btree/db.go` — expose `DB.BackupInit`
- Test: `internal/btree/backup_test.go`

- [ ] **Step 1: Write the failing test**

Create `/home/dev/work/any-store/internal/btree/backup_test.go`:

```go
package btree

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// backupPair returns two independent on-disk DBs: src (with a namespace
// "data") and an empty dst. Both use identical Options so page sizes match.
func backupPair(t *testing.T) (src, dst *DB) {
	t.Helper()
	dir := t.TempDir()
	opts := DefaultOptions()

	s, err := Open(filepath.Join(dir, "src.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	stx, err := s.BeginWrite()
	require.NoError(t, err)
	_, err = stx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, stx.Commit())

	d, err := Open(filepath.Join(dir, "dst.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	return s, d
}

func TestBackupInit_BasicFields(t *testing.T) {
	src, dst := backupPair(t)

	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.NotNil(t, b)
	require.Equal(t, src, b.src)
	require.Equal(t, dst, b.dst)
	require.Equal(t, uint32(1), b.iNext, "iNext should start at 1 per sqlite3_backup_init (backup.c:188)")
	require.False(t, b.dstLocked, "dstLocked starts false per backup.c:25")
}

func TestBackupInit_RejectsSameDB(t *testing.T) {
	src, _ := backupPair(t)
	// ~ backup.c:166–170: "source and destination must be distinct".
	_, err := src.BackupInit(src)
	require.ErrorIs(t, err, ErrBackupSameDB)
}

func TestBackupInit_RejectsPageSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	srcOpts := DefaultOptions()
	srcOpts.PageSize = 4096
	dstOpts := DefaultOptions()
	dstOpts.PageSize = 8192

	s, err := Open(filepath.Join(dir, "s.db"), srcOpts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	d, err := Open(filepath.Join(dir, "d.db"), dstOpts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	_, err = d.BackupInit(s)
	require.ErrorIs(t, err, ErrBackupPageSizeMismatch, "DRIFT #1: same page size required")
}
```

- [ ] **Step 2: Run test — expect compile failure**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackupInit -count=1`
Expected: compile errors for `BackupInit`, `ErrBackupSameDB`, `ErrBackupPageSizeMismatch`, and field accesses.

- [ ] **Step 3: Create `internal/btree/backup.go`**

Write `/home/dev/work/any-store/internal/btree/backup.go`:

```go
package btree

// This file is a port of SQLite's online-backup API from
// /home/dev/work/sqlitec/src/backup.c. Each function is annotated with
// its C counterpart. Intentional drift from SQLite is documented in
// docs/plans/2026-04-22-sqlite-backup-port.md under "Intentional DRIFT".

import (
	"errors"
	"sync"
)

var (
	// ErrBackupSameDB — ~ backup.c:166–170
	// ("source and destination must be distinct").
	ErrBackupSameDB = errors.New("btree: backup source and destination must be distinct")

	// ErrBackupPageSizeMismatch — DRIFT #1. SQLite instead rewrites
	// pages across differing sizes (backup.c:449–528); we require equal.
	ErrBackupPageSizeMismatch = errors.New("btree: backup source and destination must have equal page size")

	// ErrBackupDstBusy — ~ checkReadTransaction (backup.c:124–130)
	// ("destination database is in use"). Raised when dst already has an
	// open read/write transaction at BackupInit time.
	ErrBackupDstBusy = errors.New("btree: backup destination has an open transaction")

	// ErrBackupFinished — returned by Step after Finish or after a previous
	// Step returned io.EOF-equivalent (see sentinel ErrBackupDone).
	ErrBackupFinished = errors.New("btree: backup already finished")

	// ErrBackupDone — non-error sentinel returned by Step once every
	// source page has been copied. ~ SQLITE_DONE (sqliteInt.h, used
	// throughout backup.c starting at backup.c:406).
	ErrBackupDone = errors.New("btree: backup done")
)

// Backup tracks the state of an online-copy operation from src to dst.
// ~ struct sqlite3_backup (backup.c:21–41). Fields renamed to Go idioms;
// C names kept in comments.
type Backup struct {
	dst        *DB  // ~ pDest            (backup.c:23)
	iDstSchema uint32 // ~ iDestSchema    (backup.c:24) — captured on first lock
	dstLocked  bool   // ~ bDestLocked    (backup.c:25)

	iNext uint32 // ~ iNext (backup.c:27) — next source page to copy (1-based)

	src *DB // ~ pSrc (backup.c:29)

	// rc is sticky: once a fatal error occurs, every subsequent call
	// short-circuits with it. ~ backup.c:31 + isFatalError (backup.c:217–219).
	rc error

	// Statistics updated at every Step. ~ backup.c:36–37.
	nRemaining uint32 // pages left to copy
	nPagecount uint32 // total pages in source (as of last Step)

	// isAttached mirrors backup.c:39 — once true, this Backup is in the
	// src pager's backups list and receives update/restart callbacks.
	isAttached bool

	// mu protects nRemaining/nPagecount/rc against concurrent reads from
	// Remaining()/PageCount(). ~ "thread safety notes" (backup.c:43–71).
	// We don't need the full SQLite mutex hierarchy; see DRIFT #5.
	mu sync.Mutex
}

// BackupInit starts a new backup operation. The caller ("dst") is the
// database to write into; "src" is the database to read from.
// ~ sqlite3_backup_init (backup.c:140–210), minus the handle/name
// resolution described in DRIFT #3.
func (dst *DB) BackupInit(src *DB) (*Backup, error) {
	// ~ backup.c:166–170.
	if dst == src {
		return nil, ErrBackupSameDB
	}

	// DRIFT #1 + #6: collapse SQLite's page-size adapter path into a
	// hard error at init time. setDestPgsz (backup.c:112–116) and
	// backup.c:378–383 are both subsumed.
	if dst.PageSize() != src.PageSize() {
		return nil, ErrBackupPageSizeMismatch
	}

	return &Backup{
		dst:   dst,
		src:   src,
		iNext: 1, // ~ backup.c:188
	}, nil
}
```

- [ ] **Step 4: Run test — expect PASS for Init/SameDB/PageSizeMismatch**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackupInit -count=1 -race`
Expected: PASS for all three subtests.

- [ ] **Step 5: Commit**

```bash
cd /home/dev/work/any-store
git add internal/btree/backup.go internal/btree/backup_test.go
git -c commit.gpgsign=false commit -m "btree: scaffold Backup struct and BackupInit

Port sqlite3_backup_init (backup.c:140-210) with DRIFT #1 (same
page size required) and DRIFT #3 (no attached-db name plumbing)
applied. Step/Finish follow in subsequent commits."
```

---

## Task 3: Port `backupOnePage` — the per-page copy primitive

**Why:** This is the innermost work unit. C: `backupOnePage` (backup.c:226–279). Our port is ~20 lines because DRIFT #1 removes the size-adapter loop at backup.c:251–276.

**Files:**
- Modify: `internal/btree/backup.go` — add `onePage` method
- Test: `internal/btree/backup_test.go`

- [ ] **Step 1: Write failing test**

Append to `/home/dev/work/any-store/internal/btree/backup_test.go`:

```go
func TestBackup_OnePage_CopiesPageDataAndClearsMemPageFlag(t *testing.T) {
	src, dst := backupPair(t)

	// Insert data into src so page 1 is non-trivial and we have a page 2.
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	ns, _ := src.GetNamespace("data")
	require.NoError(t, stx.Put(ns, []byte("k1"), []byte("v1")))
	require.NoError(t, stx.Commit())

	b, err := dst.BackupInit(src)
	require.NoError(t, err)

	// Manually open a dst write tx and a src read tx to mimic what Step does.
	dtx, err := dst.BeginWrite()
	require.NoError(t, err)
	defer dtx.Rollback()
	b.dstLocked = true // normally set by Step after BeginWrite

	rtx, err := src.BeginRead()
	require.NoError(t, err)
	defer rtx.Close()

	srcPg1, err := src.pager.getPageReader(1, rtx.walMaxFrame, rtx.cache)
	require.NoError(t, err)

	// ~ backup.c:226–279 — copy one page.
	require.NoError(t, b.onePage(1, srcPg1.data, false))

	// Verify dst page 1 equals src page 1 byte-for-byte.
	dstPg1, err := dst.pager.getPageWriter(1, dst.pager.wal.nFrame.Load())
	require.NoError(t, err)
	require.Equal(t, srcPg1.data, dstPg1.data, "dst page 1 must equal src page 1 after onePage")
}
```

- [ ] **Step 2: Run — expect compile failure (missing `onePage`)**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackup_OnePage -count=1`
Expected: `b.onePage undefined`.

- [ ] **Step 3: Add `onePage` to `backup.go`**

Append to `/home/dev/work/any-store/internal/btree/backup.go`:

```go
// onePage copies page iSrcPg from the source into the destination.
// ~ backupOnePage (backup.c:226–279). The bUpdate parameter matches
// SQLite: false for normal Step copies, true for update-callback copies.
//
// DRIFT #1 simplifies this dramatically: because src and dst share a
// page size, the for-loop at backup.c:251–276 collapses to one
// iteration and no offset arithmetic is needed. DRIFT #2 removes the
// PENDING_BYTE_PAGE guard at backup.c:243/254.
func (b *Backup) onePage(iSrcPg uint32, srcData []byte, bUpdate bool) error {
	if b.dst.pager.pageSize != b.src.pager.pageSize {
		// Defensive: should be caught at Init, but a post-init reopen
		// could in theory change sizes. Return the same error.
		return ErrBackupPageSizeMismatch
	}
	if !b.dstLocked {
		return errors.New("btree: Backup.onePage called before dst locked (bug)")
	}

	// ~ backup.c:255–256: sqlite3PagerGet + sqlite3PagerWrite in one call.
	// getWritablePage (pager.go:698) combines both: it admits the page
	// into writerCache, handles savepoint copy-on-write, clears
	// dontWrite, and marks the page dirty via makeDirty (pcache.go:335).
	// This is the exact SQLite pattern: get-then-write.
	dstPg, err := b.dst.pager.getWritablePage(iSrcPg)
	if err != nil {
		return err
	}
	defer b.dst.pager.releasePage(dstPg)

	// ~ backup.c:269: memcpy(zOut, zIn, nCopy). DRIFT #1 means nCopy ==
	// pageSize; SQLite's MIN(nSrcPgsz, nDestPgsz) reduces to pageSize.
	copy(dstPg.data, srcData[:b.dst.pager.pageSize])

	// ~ backup.c:271–273: "sqlite3Put4byte(&zOut[28], sqlite3BtreeLastPage(p->pSrc))".
	// For page 1 on the initial (non-update) copy, patch the database-size
	// field so dst's header reflects the source's page count. This matters
	// because Step's main loop copies raw page bytes; page 1's header is
	// *the* authoritative DatabaseSize for readers after Finish.
	if iSrcPg == 1 && !bUpdate {
		putUint32BE(dstPg.data[28:32], b.src.DatabaseSize())
	}

	// DRIFT from backup.c:270: SQLite zeros the first byte of the page's
	// "extra space" to invalidate the MemPage.isInit parse cache. any-store
	// has no MemPage.isInit — dstPg.header is re-derived on next access by
	// deserialize(). The copy above clobbers any cached header bytes;
	// getPageWriter on a subsequent access re-parses via deserialize()
	// (see pager.go:668–670 for the isInit-style pattern on readers).
	// However we DO need to invalidate dstPg.header for this specific page
	// object, since it's cached in writerCache:
	off := 0
	if iSrcPg == 1 {
		off = dbHeaderSize
	}
	if dstPg.data[off] != 0 {
		_ = dstPg.header.deserialize(dstPg.data[off:])
	}

	return nil
}

// putUint32BE writes a big-endian u32. Avoids importing encoding/binary
// in a hot path; matches SQLite's sqlite3Put4byte (util.c).
func putUint32BE(buf []byte, v uint32) {
	_ = buf[3]
	buf[0] = byte(v >> 24)
	buf[1] = byte(v >> 16)
	buf[2] = byte(v >> 8)
	buf[3] = byte(v)
}
```

- [ ] **Step 4: Run test — expect PASS**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackup_OnePage -count=1 -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /home/dev/work/any-store
git add internal/btree/backup.go internal/btree/backup_test.go
git -c commit.gpgsign=false commit -m "btree: port backupOnePage (backup.c:226-279)

Core per-page copy primitive. DRIFT #1 collapses the size-adapter
loop (backup.c:251-276) to a single copy. Page 1 gets its
DatabaseSize field patched to src's page count per backup.c:271-273."
```

---

## Task 4: Port `Step` for the offline case (no concurrent writers)

**Why:** `sqlite3_backup_step` (backup.c:314–566). This task ports the *offline* path: Step walks source pages, copies them via `onePage`, and on final iteration updates schema/commits. We defer the update/restart hook (Task 8) — so this task only handles the "no source writes during backup" scenario.

**Files:**
- Modify: `internal/btree/backup.go` — add `Step`, `Remaining`, `PageCount`
- Modify: `internal/btree/db.go` — no changes
- Test: `internal/btree/backup_test.go`

- [ ] **Step 1: Write failing test for offline copy**

Append to `backup_test.go`:

```go
func TestBackup_Step_OfflineCopy(t *testing.T) {
	src, dst := backupPair(t)

	// Populate src with 500 records to exercise multi-page copies.
	ns, _ := src.GetNamespace("data")
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 500; i++ {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d-%s", i, "padding-padding-padding")
		require.NoError(t, stx.Put(ns, k, v))
	}
	require.NoError(t, stx.Commit())
	require.NoError(t, src.Checkpoint(CheckpointFull))

	srcPageCount := src.DatabaseSize()
	require.Greater(t, srcPageCount, uint32(2), "need multi-page src for realistic test")

	b, err := dst.BackupInit(src)
	require.NoError(t, err)

	// Copy everything in one step — nPage < 0 means "all remaining".
	// ~ sqlite3_backup_step signature at backup.c:314; nPage=-1 means unlimited.
	err = b.Step(-1)
	require.ErrorIs(t, err, ErrBackupDone, "Step must signal completion with ErrBackupDone ~ SQLITE_DONE (backup.c:406)")

	require.Equal(t, srcPageCount, b.PageCount())
	require.Equal(t, uint32(0), b.Remaining())

	require.NoError(t, b.Finish())

	// Reopen dst and verify data.
	_ = dst.Close()
	d2, err := Open(dst.Path(), DefaultOptions())
	require.NoError(t, err)
	defer d2.Close()
	rtx, err := d2.BeginRead()
	require.NoError(t, err)
	defer rtx.Close()
	ns2, err := d2.GetNamespace("data")
	require.NoError(t, err, "namespace 'data' must exist in backup")
	got, err := rtx.Get(ns2, []byte("key-0042"))
	require.NoError(t, err)
	require.Contains(t, string(got), "val-0042")
}

func TestBackup_Step_BatchedCopy(t *testing.T) {
	src, dst := backupPair(t)

	ns, _ := src.GetNamespace("data")
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 300; i++ {
		require.NoError(t, stx.Put(ns, fmt.Appendf(nil, "k-%04d", i), []byte("v")))
	}
	require.NoError(t, stx.Commit())
	require.NoError(t, src.Checkpoint(CheckpointFull))

	b, err := dst.BackupInit(src)
	require.NoError(t, err)

	// Batched: each Step copies exactly 2 pages. Keep looping until done.
	for {
		err := b.Step(2)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err, "Step should return nil while pages remain")
		require.Greater(t, b.PageCount(), uint32(0))
	}
	require.NoError(t, b.Finish())
}
```

- [ ] **Step 2: Run — expect compile failure**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackup_Step -count=1`
Expected: `b.Step undefined`, `b.Remaining undefined`, `b.PageCount undefined`, `b.Finish undefined`.

- [ ] **Step 3: Implement `Step` (offline path only)**

Append to `backup.go`:

```go
// Step copies up to nPage pages from the source to the destination.
// nPage < 0 means "copy all remaining pages in one call". Returns
// ErrBackupDone when every source page has been copied; returns nil
// when more work remains. Any other error is sticky — recorded in b.rc
// and re-returned by future calls (~ backup.c:329 + backup.c:558).
//
// ~ sqlite3_backup_step (backup.c:314–566).
func (b *Backup) Step(nPage int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.rc != nil && b.rc != ErrBackupDone {
		return b.rc // ~ isFatalError check at backup.c:329
	}

	// Open a read transaction on source for this Step. ~ backup.c:346–353.
	// DRIFT: SQLite conditionally opens a tx only if none exists, to
	// support explicit sqlite3_backup_step-inside-a-user-tx patterns.
	// any-store doesn't expose that; we always open+close our own.
	rtx, err := b.src.BeginRead()
	if err != nil {
		b.rc = err
		return err
	}
	defer rtx.Close()

	// Lock destination (exclusive/write tx) on first Step.
	// ~ backup.c:366–371: sqlite3BtreeBeginTrans(p->pDest, 2, &iDestSchema).
	if !b.dstLocked {
		wtx, err := b.dst.BeginWrite()
		if err != nil {
			b.rc = err
			return err
		}
		b.dstWriteTx = wtx
		b.iDstSchema = b.dst.localSchemaCookie.Load() // ~ iDestSchema capture
		b.dstLocked = true
	}

	nSrcPage := b.src.DatabaseSize() // ~ backup.c:388
	srcPgsz := b.src.PageSize()

	// Main copy loop. ~ backup.c:390–401.
	for ii := 0; (nPage < 0 || ii < nPage) && b.iNext <= nSrcPage; ii++ {
		iSrcPg := b.iNext
		// DRIFT #2 skips PENDING_BYTE_PAGE check at backup.c:392.

		srcPg, err := b.src.pager.getPageReader(iSrcPg, rtx.walMaxFrame, rtx.cache)
		if err != nil {
			b.rc = err
			return err
		}
		// No explicit releasePage on reader cache — tx.Close handles it.

		if err := b.onePage(iSrcPg, srcPg.data[:srcPgsz], false); err != nil {
			b.rc = err
			return err
		}
		b.iNext++
	}

	// ~ backup.c:402–410.
	b.nPagecount = nSrcPage
	if b.iNext > nSrcPage {
		b.nRemaining = 0
	} else {
		b.nRemaining = nSrcPage + 1 - b.iNext
	}

	if b.iNext > nSrcPage {
		// All pages copied. Finalize on dst. ~ backup.c:417–541 simplified:
		// we skip the schema-cookie increment path for the MVP (the dst
		// DB is brand new; its local schema cookie starts at 0). Task 9
		// adds the UpdateMeta-equivalent for the online-restart case.
		b.rc = ErrBackupDone
		return ErrBackupDone
	}

	// Still pages to go — attach for update hooks (Task 8). No-op here.
	if !b.isAttached {
		b.src.pager.attachBackup(b)
		b.isAttached = true
	}
	return nil
}

// Remaining returns the number of pages still to be backed up as of
// the most recent Step call. ~ sqlite3_backup_remaining (backup.c:625–633).
func (b *Backup) Remaining() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.nRemaining
}

// PageCount returns the total number of pages in the source as of the
// most recent Step call. ~ sqlite3_backup_pagecount (backup.c:639–647).
func (b *Backup) PageCount() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.nPagecount
}
```

**Also:** add `dstWriteTx *WriteTx` to the `Backup` struct definition (next to `dstLocked`). Document it with `// held from first Step until Finish — ~ bDestLocked's companion`.

- [ ] **Step 4: Stub `pager.attachBackup` so compile succeeds**

Add to `/home/dev/work/any-store/internal/btree/pager.go`, near the bottom of the file (just before `close()`):

```go
// attachBackup registers a Backup object to receive update/restart
// callbacks. ~ attachBackupObject (backup.c:302–309). Task 8 wires the
// actual callbacks; this stub satisfies Step's compile dependency.
func (p *pager) attachBackup(b *Backup) {
	p.backupsMu.Lock()
	p.backups = append(p.backups, b)
	p.backupsMu.Unlock()
}

// detachBackup removes a Backup from the list. ~ inverse of
// attachBackupObject; called by Backup.Finish (~ backup.c:589–597).
func (p *pager) detachBackup(b *Backup) {
	p.backupsMu.Lock()
	defer p.backupsMu.Unlock()
	for i, x := range p.backups {
		if x == b {
			p.backups = append(p.backups[:i], p.backups[i+1:]...)
			return
		}
	}
}
```

Then add two fields to the `pager` struct (around pager.go:140, after `dontWritePages`):

```go
// backups is the list of in-flight online backups reading from this
// pager. ~ Pager.pBackup (linked-list head in sqlite3). Go slice is
// simpler than the C singly-linked list since we don't need in-place
// insertion from callback context.
backups   []*Backup
backupsMu sync.Mutex
```

- [ ] **Step 5: Stub `Backup.Finish` so the test compiles**

Append to `backup.go`:

```go
// Finish releases all resources associated with a Backup and commits
// or rolls back the destination write transaction based on b.rc.
// ~ sqlite3_backup_finish (backup.c:571–619).
func (b *Backup) Finish() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.isAttached {
		b.src.pager.detachBackup(b)
		b.isAttached = false
	}

	// ~ backup.c:600: if a dst tx is still open, decide commit vs rollback.
	// rc == ErrBackupDone → success → commit; anything else → rollback.
	var finishErr error
	if b.dstWriteTx != nil {
		if b.rc == ErrBackupDone {
			finishErr = b.dstWriteTx.Commit()
		} else {
			finishErr = b.dstWriteTx.Rollback()
		}
		b.dstWriteTx = nil
	}

	// ~ backup.c:603: return rc translated to OK on DONE.
	if b.rc == ErrBackupDone {
		if finishErr != nil {
			return finishErr
		}
		return nil
	}
	return b.rc
}
```

- [ ] **Step 6: Run test — expect PASS for offline copy and batched copy**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackup_Step -count=1 -race`
Expected: PASS for both `TestBackup_Step_OfflineCopy` and `TestBackup_Step_BatchedCopy`.

If `TestBackup_Step_OfflineCopy` fails on the reopen/Get step with "namespace not found", the dbHeader patch at Task 3 Step 3 is likely off. Verify via:
```bash
go test -run TestBackup_Step_OfflineCopy -count=1 -v
```
and inspect the error. Header offset 28 is correct (see page.go:194).

- [ ] **Step 7: Run full btree test suite to confirm no regression**

Run: `cd /home/dev/work/any-store/internal/btree && go test -short -count=1 -timeout=180s ./...`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cd /home/dev/work/any-store
git add internal/btree/backup.go internal/btree/backup_test.go internal/btree/pager.go
git -c commit.gpgsign=false commit -m "btree: port sqlite3_backup_step offline path

Ports sqlite3_backup_step (backup.c:314-566) for the no-concurrent-
writers case. Adds Backup.Step/Remaining/PageCount/Finish and the
pager.backups list with attach/detach stubs (Task 8 fills in the
update/restart callbacks)."
```

---

## Task 5: Port `backupUpdate` and `backupRestart` hooks

**Why:** This is what makes backup *online*. Without it, a source commit during a partial backup silently corrupts the destination (already-copied pages lose their new content). C: `backupUpdate` / `sqlite3BackupUpdate` (backup.c:661–688) and `sqlite3BackupRestart` (backup.c:701–707).

**Files:**
- Modify: `internal/btree/backup.go` — add `update(iPage, data)` and `restart()` methods
- Modify: `internal/btree/pager.go` — call `pager.dispatchBackupUpdate(dirtyBuf)` inside `commit()` right after `wal.writeFrames` succeeds (line 1382)
- Test: `internal/btree/backup_test.go` — online-backup-across-commit test

- [ ] **Step 1: Write failing test — online backup with concurrent write**

Append to `backup_test.go`:

```go
func TestBackup_OnlineWriteBetweenSteps(t *testing.T) {
	src, dst := backupPair(t)
	ns, _ := src.GetNamespace("data")

	// Seed: insert 100 records.
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.NoError(t, stx.Put(ns, fmt.Appendf(nil, "k-%04d", i), []byte("initial")))
	}
	require.NoError(t, stx.Commit())
	require.NoError(t, src.Checkpoint(CheckpointFull))

	b, err := dst.BackupInit(src)
	require.NoError(t, err)

	// Copy only 2 pages (partial).
	err = b.Step(2)
	require.NoError(t, err, "Step(2) should leave more pages to copy")
	require.Greater(t, b.Remaining(), uint32(0))

	// Concurrent write to src updates a key that was likely in one of the
	// first 2 pages. ~ backup.c:669 ("iPage<p->iNext" triggers the update).
	stx2, err := src.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, stx2.Put(ns, []byte("k-0000"), []byte("updated-online")))
	require.NoError(t, stx2.Commit())

	// Finish copy.
	for {
		err := b.Step(2)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, b.Finish())

	// Reopen dst and verify k-0000 is the updated value (the update
	// callback must have re-copied the affected page after the commit).
	_ = dst.Close()
	d2, err := Open(dst.Path(), DefaultOptions())
	require.NoError(t, err)
	defer d2.Close()
	rtx, err := d2.BeginRead()
	require.NoError(t, err)
	defer rtx.Close()
	ns2, _ := d2.GetNamespace("data")
	got, err := rtx.Get(ns2, []byte("k-0000"))
	require.NoError(t, err)
	require.Equal(t, "updated-online", string(got),
		"update hook (backup.c:661-688) must re-copy pages modified after they were copied")
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackup_OnlineWriteBetweenSteps -count=1 -race`
Expected: FAIL — got `"initial"`, want `"updated-online"`. Explanation: no update hook yet.

- [ ] **Step 3: Implement `Backup.update` and `Backup.restart`**

Append to `backup.go`:

```go
// update is called by the source pager right after a committed page is
// written to the WAL. If the page has already been copied into the
// destination (iPage < b.iNext), re-copy it so the destination stays
// current. ~ backupUpdate (backup.c:661–685).
//
// Caller must hold the source pager's backups lock — see pager.go
// dispatchBackupUpdate. b.mu is NOT held by caller.
func (b *Backup) update(iPage uint32, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// ~ backup.c:669 — skip if fatal error or page not yet copied.
	if b.rc != nil && b.rc != ErrBackupDone {
		return
	}
	if iPage >= b.iNext {
		return
	}

	// bUpdate=true suppresses the page-1 DatabaseSize patch, which would
	// be wrong to redo mid-backup (the page-count value baked in during
	// the first copy was correct as of then; subsequent commits may grow
	// the DB, and Step will patch the final count on the last iteration).
	if err := b.onePage(iPage, data, true); err != nil {
		b.rc = err
	}
}

// restart resets the backup to start copying from page 1 again. Called
// by the source pager when the page cache is invalidated in a way that
// makes "already copied" tracking meaningless (e.g. WAL reset, external
// overwrite). ~ sqlite3BackupRestart (backup.c:701–707).
func (b *Backup) restart() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.iNext = 1
}
```

- [ ] **Step 4: Implement dispatcher on pager and wire into commit**

Add to `/home/dev/work/any-store/internal/btree/pager.go`, next to `attachBackup`:

```go
// dispatchBackupUpdate notifies every registered Backup that the given
// dirty pages have been committed to this pager's WAL. Called from
// pager.commit just after wal.writeFrames succeeds, so the page data
// we hand to each backup is the authoritative post-commit version.
// ~ sqlite3BackupUpdate dispatch (backup.c:687–688).
func (p *pager) dispatchBackupUpdate(dirty []*page) {
	p.backupsMu.Lock()
	// Copy the slice so callbacks can run outside the lock without
	// racing attach/detach (SQLite holds BtShared.mutex; we split).
	bs := make([]*Backup, len(p.backups))
	copy(bs, p.backups)
	p.backupsMu.Unlock()

	if len(bs) == 0 {
		return
	}
	for _, pg := range dirty {
		for _, b := range bs {
			b.update(pg.pgno, pg.data)
		}
	}
}

// dispatchBackupRestart notifies every registered Backup to restart
// from page 1. ~ sqlite3BackupRestart (backup.c:701–707). Currently
// called only from the checkpoint path when the WAL has been reset
// (Task 6 adds the call site). Stub keeps the symmetry.
func (p *pager) dispatchBackupRestart() {
	p.backupsMu.Lock()
	bs := make([]*Backup, len(p.backups))
	copy(bs, p.backups)
	p.backupsMu.Unlock()
	for _, b := range bs {
		b.restart()
	}
}
```

Then edit `pager.commit()` at `/home/dev/work/any-store/internal/btree/pager.go`. Locate the block:

```go
// Write all dirty pages to WAL
if err := p.wal.writeFrames(p.dirtyBuf, true, p.dbSize.Load()); err != nil {
    p.pagerError()
    return 0, 0, 0, err
}

// Capture nFrame atomically for checkpoint threshold decision.
nFrame = p.wal.nFrame.Load()
```

Insert immediately after `nFrame = p.wal.nFrame.Load()` (should be pager.go:1388 currently):

```go
// Notify online backups that these pages have new committed content.
// ~ sqlite3BackupUpdate dispatch — see backup.c:687. Must happen here,
// AFTER the WAL frames are durable, so dst sees committed data only.
p.dispatchBackupUpdate(p.dirtyBuf)
```

- [ ] **Step 5: Run the online test — expect PASS**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackup_OnlineWriteBetweenSteps -count=1 -race`
Expected: PASS.

- [ ] **Step 6: Run full btree test suite — no regression**

Run: `cd /home/dev/work/any-store/internal/btree && go test -short -count=1 -race -timeout=180s ./...`
Expected: PASS. The `dispatchBackupUpdate` call is a no-op when `len(backups)==0`, so existing tests are unaffected.

- [ ] **Step 7: Commit**

```bash
cd /home/dev/work/any-store
git add internal/btree/backup.go internal/btree/pager.go
git -c commit.gpgsign=false commit -m "btree: port backupUpdate/backupRestart hooks (backup.c:661-707)

Adds Backup.update/restart and pager.dispatchBackupUpdate/Restart.
Wires dispatch into pager.commit() right after wal.writeFrames so
online backups see post-commit page content."
```

---

## Task 6: Call `dispatchBackupRestart` on checkpoint-restart

**Why:** When a checkpoint resets WAL (`CheckpointRestart`/`CheckpointTruncate`), in-flight reader snapshots can see a page numbering discontinuity. SQLite's `sqlite3BackupRestart` (backup.c:701–707) exists exactly for this. We must call `dispatchBackupRestart` from the checkpoint path at the same logical point.

**Files:**
- Modify: `internal/btree/pager.go` — call `dispatchBackupRestart` inside `checkpointWithMode` after WAL reset
- Test: `internal/btree/backup_test.go`

- [ ] **Step 1: Write failing test — backup across WAL restart**

Append to `backup_test.go`:

```go
func TestBackup_RestartOnCheckpointRestart(t *testing.T) {
	src, dst := backupPair(t)
	ns, _ := src.GetNamespace("data")

	// Seed.
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.NoError(t, stx.Put(ns, fmt.Appendf(nil, "k-%04d", i), []byte("v")))
	}
	require.NoError(t, stx.Commit())

	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.NoError(t, b.Step(2)) // partial: iNext > 1 now
	require.Greater(t, b.src.DatabaseSize(), uint32(2))
	require.NotEqual(t, uint32(1), b.iNext, "Step(2) should have advanced iNext past 1")

	// CheckpointRestart resets WAL. ~ backup.c:701 trigger.
	require.NoError(t, src.Checkpoint(CheckpointRestart))

	// iNext should be back to 1 now.
	require.Equal(t, uint32(1), b.iNext, "CheckpointRestart must trigger Backup.restart (backup.c:701-707)")
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackup_RestartOnCheckpointRestart -count=1`
Expected: FAIL — iNext unchanged.

- [ ] **Step 3: Wire dispatchBackupRestart into checkpointWithMode**

`pager.checkpointWithMode` at pager.go:1749–1751 is a thin wrapper around `wal.checkpointWithMode`. Replace its body with:

```go
func (p *pager) checkpointWithMode(mode CheckpointMode) error {
	err := p.wal.checkpointWithMode(p.file, p.master, mode, p.wal.busyHandler)
	if err == nil && (mode == CheckpointRestart || mode == CheckpointTruncate) {
		// ~ sqlite3BackupRestart (backup.c:701-707). WAL frame numbering
		// has reset, so any in-flight backup must start over.
		p.dispatchBackupRestart()
	}
	return err
}
```

Also patch `tryCheckpoint` at pager.go:1755–1769 so the best-effort RESTART inside auto-checkpoint also notifies:

```go
// In tryCheckpoint, change line 1766 from:
//     _ = p.wal.checkpointWithMode(p.file, p.master, CheckpointRestart, nil)
// to:
if err := p.wal.checkpointWithMode(p.file, p.master, CheckpointRestart, nil); err == nil {
	p.dispatchBackupRestart()
}
```

- [ ] **Step 4: Run test — expect PASS**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackup_RestartOnCheckpointRestart -count=1 -race`
Expected: PASS.

- [ ] **Step 5: Run full btree suite**

Run: `cd /home/dev/work/any-store/internal/btree && go test -short -count=1 -race -timeout=180s ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /home/dev/work/any-store
git add internal/btree/pager.go internal/btree/backup_test.go
git -c commit.gpgsign=false commit -m "btree: trigger backup restart on checkpoint-restart/truncate

~ sqlite3BackupRestart (backup.c:701-707). Only modes that reset
WAL frame numbering trigger the restart; passive/full checkpoints
do not (backup cache stays valid)."
```

---

## Task 7: Reject Step/Finish called in wrong order + sticky error propagation

**Why:** Harden the state machine so misuse is obvious. C: sticky `p->rc` set at backup.c:558 and translation at backup.c:603. Also the `p==0` guards at backup.c:321/577/627/641.

**Files:**
- Modify: `internal/btree/backup.go`
- Test: `internal/btree/backup_test.go`

- [ ] **Step 1: Write failing test**

Append to `backup_test.go`:

```go
func TestBackup_FinishTwiceIsError(t *testing.T) {
	src, dst := backupPair(t)
	b, err := dst.BackupInit(src)
	require.NoError(t, err)

	// Drain Step until done.
	for {
		err := b.Step(-1)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}

	require.NoError(t, b.Finish())
	// Second Finish is a no-op per SQLite "p==0 return OK" (backup.c:577).
	// We choose explicit error to catch double-close bugs (DRIFT from C).
	require.ErrorIs(t, b.Finish(), ErrBackupFinished)
}

func TestBackup_StepAfterFinishIsError(t *testing.T) {
	src, dst := backupPair(t)
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.NoError(t, b.Finish())
	require.ErrorIs(t, b.Step(10), ErrBackupFinished)
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run "TestBackup_FinishTwice|TestBackup_StepAfterFinish" -count=1`
Expected: FAIL.

- [ ] **Step 3: Add a `finished` flag and guard Step/Finish**

In `backup.go`, add to the `Backup` struct:

```go
// finished is set by Finish to reject subsequent Step/Finish calls.
// DRIFT from C: SQLite's backup.c:577 treats Finish(nil) as a no-op,
// but we don't allow reusing the pointer — Go has explicit lifecycle
// management via defer, so catching misuse is cheap.
finished bool
```

Modify `Finish` (first line inside the function, after `b.mu.Lock()`):

```go
if b.finished {
    return ErrBackupFinished
}
b.finished = true
```

Modify `Step` (first line after `b.mu.Lock()`, before the `b.rc` check):

```go
if b.finished {
    return ErrBackupFinished
}
```

- [ ] **Step 4: Run test — expect PASS**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run "TestBackup_FinishTwice|TestBackup_StepAfterFinish" -count=1 -race`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /home/dev/work/any-store
git add internal/btree/backup.go internal/btree/backup_test.go
git -c commit.gpgsign=false commit -m "btree: Backup state machine hardening

Reject Step after Finish and double-Finish. DRIFT from backup.c:577
(which tolerates NULL pointer) — Go gets an explicit error instead."
```

---

## Task 8: Enforce `checkReadTransaction` at `BackupInit`

**Why:** C `checkReadTransaction` (backup.c:124–130) rejects Init when the destination already has an open read or write transaction with error message "destination database is in use". My original plan omitted this because `BeginWrite` would block on `writeMu` — but blocking vs. fast-failing is *different observable behavior*, which is drift.

**Files:**
- Modify: `internal/btree/db.go` — add `(*DB).HasOpenTransaction() bool`
- Modify: `internal/btree/backup.go` — call it from `BackupInit`
- Test: `internal/btree/backup_test.go`

- [ ] **Step 1: Write failing test**

Append to `backup_test.go`:

```go
func TestBackupInit_RejectsDstWithOpenTx(t *testing.T) {
	src, dst := backupPair(t)

	// Open a read tx on dst and keep it alive.
	rtx, err := dst.BeginRead()
	require.NoError(t, err)
	defer rtx.Close()

	// ~ backup.c:124-130 ("destination database is in use").
	_, err = dst.BackupInit(src)
	require.ErrorIs(t, err, ErrBackupDstBusy)
}

func TestBackupInit_RejectsDstWithOpenWriteTx(t *testing.T) {
	src, dst := backupPair(t)

	wtx, err := dst.BeginWrite()
	require.NoError(t, err)
	defer wtx.Rollback()

	_, err = dst.BackupInit(src)
	require.ErrorIs(t, err, ErrBackupDstBusy)
}
```

- [ ] **Step 2: Run — expect FAIL**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackupInit_RejectsDst -count=1`
Expected: FAIL — `BackupInit` currently succeeds (or blocks, in the write-tx case, which is itself a test timeout).

Note: the write-tx test will time out if `BackupInit` blocks rather than errors — confirming we have the drift. Use `-timeout=10s` to bound the test.

- [ ] **Step 3: Add `HasOpenTransaction()` accessor**

In `/home/dev/work/any-store/internal/btree/db.go`, after `DatabaseSize()` added in Task 1:

```go
// HasOpenTransaction returns true if the DB has an active read or write
// transaction. Inspects the pager state directly — atomic, no lock.
// ~ sqlite3BtreeTxnState != SQLITE_TXN_NONE (backup.c:125).
func (db *DB) HasOpenTransaction() bool {
	return pagerState(db.pager.state.Load()) != pagerOpen
}
```

- [ ] **Step 4: Gate `BackupInit` on the check**

In `/home/dev/work/any-store/internal/btree/backup.go`, modify `BackupInit` — insert between the same-DB check and the page-size check:

```go
// ~ checkReadTransaction (backup.c:124-130).
if dst.HasOpenTransaction() {
	return nil, ErrBackupDstBusy
}
```

(The `ErrBackupDstBusy` sentinel was already declared in Task 2's initial errors block.)

- [ ] **Step 5: Run tests — expect PASS**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackupInit -count=1 -race -timeout=30s`
Expected: PASS — all four `TestBackupInit_*` variants (basic, same-DB, page-size, dst-busy read, dst-busy write).

- [ ] **Step 6: Commit**

```bash
cd /home/dev/work/any-store
git add internal/btree/backup.go internal/btree/db.go internal/btree/backup_test.go
git -c commit.gpgsign=false commit -m "btree: reject BackupInit when dst has an open transaction

~ checkReadTransaction (backup.c:124-130). Adds DB.HasOpenTransaction
accessor and gates BackupInit on it. Fast-fail instead of blocking
on writeMu, matching SQLite's observable behavior."
```

---

## Task 9: Finalization — schema-cookie bump + destination truncation

**Why:** At the end of the last `Step`, SQLite does two things that I originally glossed over:

1. **Schema-cookie bump** (backup.c:423): `sqlite3BtreeUpdateMeta(p->pDest, 1, p->iDestSchema+1)`. Forces any cached schema parses on dst to invalidate.
2. **Destination truncation** (backup.c:530): `sqlite3PagerTruncateImage(pDestPager, nDestTruncate)`. If dst was larger than src (e.g., reused existing dst), shrink it.

Both matter when dst is *not* a freshly-opened empty DB. The anystore-level `(*db).Backup` always opens fresh, but `BackupInit` is also a public btree-layer API; users may reuse dst.

**Files:**
- Modify: `internal/btree/backup.go` — add a finalize step before commit
- Modify: `internal/btree/pager.go` — expose a `(*pager).truncateTo(newDbSize uint32) error` method (or reuse an existing equivalent — check first)
- Test: `internal/btree/backup_test.go`

- [ ] **Step 1: Check if pager already has a truncate primitive**

Run:
```bash
grep -n "Truncate\|truncate" /home/dev/work/any-store/internal/btree/pager.go | grep -i "func\|dbSize" | head
```

If `(*pager).truncate`, `truncateImage`, or similar exists, use it. If not, proceed to Step 4 to add one. The `pcache.truncate` at pcache.go:524 is cache-only; we need the pager-level equivalent that also updates `dbSize` and arranges the file to shrink at the next checkpoint.

- [ ] **Step 2: Write failing test — reused dst must be truncated**

Append to `backup_test.go`:

```go
func TestBackup_TruncatesLargerDst(t *testing.T) {
	src, dst := backupPair(t)

	// Populate dst with MORE data than src will have.
	dns, _ := dst.GetNamespace("data") // backupPair already created "data" on dst? Check backupPair def — if not, create here.
	if dns == nil {
		dtx, err := dst.BeginWrite()
		require.NoError(t, err)
		_, err = dtx.CreateNamespace("data")
		require.NoError(t, err)
		require.NoError(t, dtx.Commit())
		dns, _ = dst.GetNamespace("data")
	}
	dtx, err := dst.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 2000; i++ { // lots of pages
		require.NoError(t, dtx.Put(dns, fmt.Appendf(nil, "dst-%04d", i), []byte("xxxxxxxxxxxxxxxx")))
	}
	require.NoError(t, dtx.Commit())
	dstOrigSize := dst.DatabaseSize()

	// src has much less data.
	sns, _ := src.GetNamespace("data")
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, stx.Put(sns, []byte("small"), []byte("v")))
	require.NoError(t, stx.Commit())
	srcSize := src.DatabaseSize()
	require.Less(t, srcSize, dstOrigSize, "test setup: dst must be larger than src")

	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	for {
		err := b.Step(-1)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, b.Finish())

	// ~ backup.c:530 — dst must be truncated to srcSize.
	require.Equal(t, srcSize, dst.DatabaseSize(),
		"post-backup dst size should equal src size (backup.c:530 truncate)")
}

func TestBackup_BumpsDstSchemaCookie(t *testing.T) {
	src, dst := backupPair(t)

	// Capture dst's starting schema cookie.
	rtx0, err := dst.BeginRead()
	require.NoError(t, err)
	dstCookieBefore := rtx0.DiskSchemaCookie()
	require.NoError(t, rtx0.Close())

	// Make src match (empty except for namespace).
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	for {
		err := b.Step(-1)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, b.Finish())

	// ~ backup.c:423 — schema cookie must have been bumped.
	rtx1, err := dst.BeginRead()
	require.NoError(t, err)
	defer rtx1.Close()
	require.NotEqual(t, dstCookieBefore, rtx1.DiskSchemaCookie(),
		"post-backup dst schema cookie must differ from pre-backup (backup.c:423 bump)")
}
```

**Caveat:** `backupPair` as defined in Task 2 creates `"data"` only on src, not dst. Either modify `backupPair` to also create it on dst, or the test handles that itself as shown above. Prefer the test-level handling (less cross-test coupling).

- [ ] **Step 3: Run — expect FAIL**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run "TestBackup_TruncatesLargerDst|TestBackup_BumpsDstSchemaCookie" -count=1`
Expected: FAIL on both.

- [ ] **Step 4: Add finalize block in `Backup.Step` when transitioning to ErrBackupDone**

In `internal/btree/backup.go`, locate the block in `Step` where `iNext > nSrcPage` triggers `ErrBackupDone` (see Task 4 Step 3). Insert a finalize call BEFORE setting `b.rc = ErrBackupDone`:

```go
if b.iNext > nSrcPage {
	// Finalize: shrink dst and bump schema cookie before Finish commits.
	if err := b.finalize(nSrcPage); err != nil {
		b.rc = err
		return err
	}
	b.rc = ErrBackupDone
	return ErrBackupDone
}
```

Then add the `finalize` method:

```go
// finalize performs the pre-commit housekeeping that backup.c does at
// the end of its final Step iteration (backup.c:417–541):
//   - ~ backup.c:423: sqlite3BtreeUpdateMeta(p->pDest, 1, p->iDestSchema+1)
//   - ~ backup.c:530: sqlite3PagerTruncateImage(pDestPager, nDestTruncate)
//
// Both mutate the destination; Finish then commits the result.
func (b *Backup) finalize(nSrcPage uint32) error {
	// 1. Bump dst schema cookie. We do this by modifying the in-memory
	// header directly and letting commit serialize it to page 1. The
	// commit path at pager.go:1311 copies p.header.DatabaseSize from
	// atomic dbSize — we do the same dance for SchemaCookie by mutating
	// the writable page 1 post-copy.
	//
	// Simpler: ~ sqlite3BtreeUpdateMeta writes meta value #1 (the
	// schema cookie) to the header at offset 40. Our page 1 already
	// got its header copied from src during the normal Step loop; we
	// overwrite offset 40 with iDstSchema+1.
	dstPg1, err := b.dst.pager.getWritablePage(1)
	if err != nil {
		return err
	}
	putUint32BE(dstPg1.data[40:44], b.iDstSchema+1)
	// Keep the in-memory header in sync so commit's page-1 serialize
	// (pager.go:1371) doesn't clobber us.
	_ = b.dst.pager.header.deserialize(dstPg1.data[dbHeaderSize:])
	b.dst.pager.header.SchemaCookie = b.iDstSchema + 1
	b.dst.pager.releasePage(dstPg1)

	// 2. Truncate dst to nSrcPage. ~ sqlite3PagerTruncateImage
	// (backup.c:530). Shrinks dst.dbSize; the file on disk shrinks at
	// the next checkpoint (pager.go:2035–2042 handles the physical
	// truncation in checkpoint).
	return b.dst.pager.truncateTo(nSrcPage)
}
```

- [ ] **Step 5: Add `(*pager).truncateTo` if not already present**

Search for an existing equivalent:
```bash
grep -n "truncateTo\|truncateImage\|dbSize.Store" /home/dev/work/any-store/internal/btree/pager.go | head
```

If absent, add to `pager.go` near `allocatePage`:

```go
// truncateTo shrinks the database to the given page count. Matches
// SQLite's sqlite3PagerTruncateImage (pager.c). Discards writerCache
// entries above the new size (via pcache.truncate) and updates the
// atomic dbSize so subsequent writes see the new bound. Physical file
// truncation happens at the next checkpoint, per pager.go:2035-2042.
func (p *pager) truncateTo(newDbSize uint32) error {
	if pagerState(p.state.Load()) != pagerWriter {
		return ErrReadOnly
	}
	if newDbSize == 0 {
		return errors.New("btree: cannot truncate to zero pages")
	}
	cur := p.dbSize.Load()
	if newDbSize >= cur {
		return nil // no-op — already at or below requested size
	}
	p.writerCache.truncate(newDbSize)
	p.dbSize.Store(newDbSize)
	p.header.DatabaseSize = newDbSize
	return nil
}
```

Verify `errors` import is already present in `pager.go`; if not, add it (check with `head -20 /home/dev/work/any-store/internal/btree/pager.go`).

- [ ] **Step 6: Run tests — expect PASS**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run "TestBackup_TruncatesLargerDst|TestBackup_BumpsDstSchemaCookie" -count=1 -race`
Expected: PASS.

- [ ] **Step 7: Run full backup suite to confirm no regression**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackup -count=1 -race -timeout=60s`
Expected: PASS. In particular the `TestBackup_Step_OfflineCopy` and online tests from earlier tasks must still pass — the new finalize code runs on their completion path too.

- [ ] **Step 8: Commit**

```bash
cd /home/dev/work/any-store
git add internal/btree/backup.go internal/btree/pager.go internal/btree/backup_test.go
git -c commit.gpgsign=false commit -m "btree: finalize dst on last Step — schema bump + truncate

~ backup.c:423 (sqlite3BtreeUpdateMeta for schema cookie+1)
~ backup.c:530 (sqlite3PagerTruncateImage)

Without these, reusing an already-populated dst would leave
orphan pages past the new dbSize and stale cached schemas on dst
readers. Adds pager.truncateTo helper."
```

---

## Task 10: Detect external-process writes — restart via `fileChangeCounter`

**Why:** `sqlite3BackupRestart` (backup.c:701–707) resets a backup to page 1 whenever the page cache becomes invalidated in ways the in-process update hook cannot observe — including external-process writes. SQLite calls this from pager internals under `BtShared.mutex`. In any-store, cross-process writes are detected via the page-1 `FileChangeCounter` (already tracked at `tx.diskFileChangeCounter`, db.go:390 / 486). Step can compare the counter across calls and trigger `restart` when it jumps.

**Files:**
- Modify: `internal/btree/backup.go` — add `lastFileChangeCounter` to `Backup`; check at Step entry; also check on each Step's fresh read tx
- Test: `internal/btree/backup_test.go` — requires two processes or two DB handles to same file

- [ ] **Step 1: Check how multi-handle tests work today**

Run:
```bash
grep -n "openDBs\|multiprocess\|two DB handles\|multi-handle" /home/dev/work/any-store/internal/btree/*_test.go /home/dev/work/any-store/internal/btree/db.go | head -15
```

Look at `multiprocess_test.go` (it exists per the ls at plan-start). Its test pattern — whether it forks or uses exec — determines how we structure our test. If it uses `exec.Command`, follow that pattern. If it uses `openDBs` bypass (second-handle-in-process), see if that's exposed for tests.

Check also:
```bash
grep -n "InProcess\|openDBs.Delete\|testOpenSameFile" /home/dev/work/any-store/internal/btree/db.go | head
```

`openDBs` at db.go:17–18 blocks double-open by path. Multi-process tests in SQLite's manner require forking. If any-store's existing multi-handle tests go through `exec`, follow suit.

- [ ] **Step 2: Write failing test**

Append to `backup_test.go`. If `multiprocess_test.go` uses `exec`, mirror that here; if it uses some same-process trick, use that. For clarity I give the exec-based shape; adapt to the repo's convention if different:

```go
func TestBackup_RestartsOnExternalProcessWrite(t *testing.T) {
	// If any-store's existing multi-process tests use a helper like
	// runInSubprocess(t, func(...)), use that. Otherwise:
	if testing.Short() {
		t.Skip("spawns subprocess; excluded in -short")
	}

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")

	// Seed src in this process.
	{
		s, err := Open(srcPath, DefaultOptions())
		require.NoError(t, err)
		stx, err := s.BeginWrite()
		require.NoError(t, err)
		ns, err := stx.CreateNamespace("data")
		require.NoError(t, err)
		for i := 0; i < 50; i++ {
			require.NoError(t, stx.Put(ns, fmt.Appendf(nil, "k-%04d", i), []byte("initial")))
		}
		require.NoError(t, stx.Commit())
		require.NoError(t, s.Close())
	}

	// Open src in this process for backup.
	src, err := Open(srcPath, DefaultOptions())
	require.NoError(t, err)
	defer src.Close()
	dst, err := Open(dstPath, DefaultOptions())
	require.NoError(t, err)
	defer dst.Close()

	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.NoError(t, b.Step(2)) // partial
	require.Greater(t, b.iNext, uint32(1))

	// External process writes to src. Use go run with a short program,
	// or better, a subprocess helper. For this plan, pseudocode:
	runExternalWriter(t, srcPath, "write k-0000 updated-externally")

	// Next Step should observe the changed file-change-counter and restart.
	err = b.Step(-1)
	if err != ErrBackupDone {
		require.NoError(t, err)
	}
	// After restart, iNext must have been reset to 1 at some point during
	// the copy. We can't observe that directly, but we CAN observe that
	// the final dst has the updated value.
	require.NoError(t, b.Finish())

	// Reopen dst and verify.
	_ = dst.Close()
	d2, err := Open(dstPath, DefaultOptions())
	require.NoError(t, err)
	defer d2.Close()
	rtx, err := d2.BeginRead()
	require.NoError(t, err)
	defer rtx.Close()
	ns, _ := d2.GetNamespace("data")
	got, err := rtx.Get(ns, []byte("k-0000"))
	require.NoError(t, err)
	require.Equal(t, "updated-externally", string(got),
		"external-process write mid-backup must be reflected in dst")
}
```

If this repo lacks a `runExternalWriter` helper, this task produces one in `helpers_test.go`. Consult `multiprocess_test.go` before inventing — use whatever is already there. If the helper doesn't exist, the implementation is roughly:

```go
// helpers_test.go or shared test file
func runExternalWriter(t *testing.T, dbPath, op string) {
	t.Helper()
	// Use go run with a small tool program, OR invoke the test binary
	// itself with a sentinel env var. Pattern used in any-store
	// multi-process tests — mirror exactly.
}
```

- [ ] **Step 3: Run — expect FAIL**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackup_RestartsOnExternalProcessWrite -count=1`
Expected: FAIL — dst has `"initial"`, want `"updated-externally"`.

- [ ] **Step 4: Track last-seen `FileChangeCounter` on the Backup**

In `internal/btree/backup.go`:

1. Add field to `Backup` struct:

```go
// lastFCC is the FileChangeCounter observed at the previous Step's
// read-tx open. A jump between Steps signals an external-process
// commit: the on-disk page cache semantics are invalidated, so we
// must restart per backup.c:701-707.
// 0 = "no prior Step" (init state).
lastFCC uint32
```

2. Inside `Step`, immediately after `rtx, err := b.src.BeginRead()` (and before the dst-lock block), inspect the counter:

```go
// ~ sqlite3BackupRestart trigger for external writes.
// rtx.diskFileChangeCounter is the page-1 FCC read at BeginRead; a
// change since our last Step means another writer (in this process
// or any other) committed. Our in-process dispatchBackupUpdate hook
// handles same-process same-DB-handle case; this catches the rest.
if b.lastFCC != 0 && rtx.diskFileChangeCounter != b.lastFCC {
	// Don't call b.restart() directly — it re-acquires b.mu, which
	// we already hold. Inline the reset:
	b.iNext = 1
}
b.lastFCC = rtx.diskFileChangeCounter
```

Note: `ReadTx.diskFileChangeCounter` is lowercase/unexported (db.go:390, 486). Accessing from within package `btree` is fine; if the field is private-to-struct within its own file, verify via:
```bash
grep -n "diskFileChangeCounter" /home/dev/work/any-store/internal/btree/db.go | head
```

If access is somehow blocked, add an accessor `(tx *ReadTx) DiskFileChangeCounter() uint32` alongside the existing `DiskSchemaCookie()` at db.go:1181.

- [ ] **Step 5: Run tests — expect PASS**

Run: `cd /home/dev/work/any-store/internal/btree && go test -run TestBackup_RestartsOnExternalProcessWrite -count=1 -race -timeout=60s`
Expected: PASS.

Also re-run the same-process online test to ensure no regression:
```bash
cd /home/dev/work/any-store/internal/btree && go test -run TestBackup_OnlineWriteBetweenSteps -count=1 -race
```
Expected: PASS.

- [ ] **Step 6: Run full btree suite**

Run: `cd /home/dev/work/any-store/internal/btree && go test -short -count=1 -race -timeout=180s ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cd /home/dev/work/any-store
git add internal/btree/backup.go internal/btree/backup_test.go
git -c commit.gpgsign=false commit -m "btree: restart backup on external-process write

~ sqlite3BackupRestart external trigger (backup.c:701-707, called
from pager internals under BtShared.mutex in C). any-store lacks
a cross-process pager-level hook, so we inspect the page-1
FileChangeCounter at each Step's read tx and reset iNext when it
jumps. Complements the same-process dispatchBackupUpdate hook."
```

---

## Task 11: Rewrite top-level `(*db).Backup(ctx, path)` to use the new engine

**Why:** The current implementation at db.go:506–520 is a naive checkpoint+file-copy. It can't run during writes and doesn't work for InMemory sources. Replace it with a driver that opens a fresh destination DB at `path` and runs `BackupInit → Step(MaxInt) → Finish`.

**Files:**
- Modify: `/home/dev/work/any-store/db.go` (replace body of `Backup`)
- Modify: `/home/dev/work/any-store/db_test.go` (remove InMemory skip; add online test)

- [ ] **Step 1: Write failing tests at the anystore level**

Modify `/home/dev/work/any-store/db_test.go`. Locate `TestDb_Backup` at lines 116–136 and replace the InMemory skip (lines 117–119) so the test runs for InMemory too. Append a new online test below it.

Read current state first:
```bash
sed -n '116,140p' /home/dev/work/any-store/db_test.go
```

Replace `TestDb_Backup` with:

```go
func TestDb_Backup(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.Collection(ctx, "coll")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"doc"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "doc":"a"}`), anyenc.MustParseJson(`{"id":2, "doc":"b"}`)))

	tmpDir, err := os.MkdirTemp("", "any-store-backup-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	backupPath := filepath.Join(tmpDir, "any-store-test.db")
	require.NoError(t, fx.Backup(ctx, backupPath))

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.Collection(ctx, "coll")
	require.NoError(t, err)
	assert.Len(t, coll2.GetIndexes(), 1)
	assertCollCount(t, coll2, 2)
}

func TestDb_Backup_OnlineDuringWrites(t *testing.T) {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("concurrent-writer test; file backend only")
	}
	fx := newFixture(t)
	coll, err := fx.Collection(ctx, "coll")
	require.NoError(t, err)

	// Seed 500 docs.
	for i := 0; i < 500; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "val":"seed"}`, i))))
	}

	tmpDir, err := os.MkdirTemp("", "any-store-backup-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	backupPath := filepath.Join(tmpDir, "online.db")

	// Start backup and a concurrent writer.
	done := make(chan error, 1)
	go func() { done <- fx.Backup(ctx, backupPath) }()

	// Overlap some writes with the backup.
	for i := 0; i < 50; i++ {
		_ = coll.UpsertOne(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "val":"updated"}`, i)))
	}

	require.NoError(t, <-done)

	// Verify backup opens and has expected doc count.
	fx2 := newFixturePath(t, tmpDir)
	require.NoError(t, copyFile(backupPath, filepath.Join(fx2.dir, "any-store-test.db")))
	// (If newFixturePath opens a specific filename, adjust: we may need
	// to rename backupPath or pass its dir directly. Check newFixturePath
	// impl in db_test.go lines ~191-228 and adapt.)

	coll2, err := fx2.Collection(ctx, "coll")
	require.NoError(t, err)
	assertCollCount(t, coll2, 500)
}
```

Before running, check `newFixturePath` — what filename does it expect? The path passed to `fx.Backup` at Step 1 must match. Run:
```bash
grep -n "newFixturePath" /home/dev/work/any-store/db_test.go | head
```
and read ~5 lines around it. Adjust `backupPath` filename accordingly. If `newFixturePath` expects a directory containing `any-store-test.db`, use that filename (as `TestDb_Backup` already does).

- [ ] **Step 2: Run — expect original test passing (old impl), online test failing**

Run: `cd /home/dev/work/any-store && go test -short -run "TestDb_Backup" -count=1 -race -timeout=60s`
Expected: `TestDb_Backup` still passes; `TestDb_Backup_OnlineDuringWrites` may pass or fail depending on timing (old impl may happen to complete before concurrent writes). This is OK — we're replacing the impl anyway.

- [ ] **Step 3: Rewrite `(*db).Backup`**

Replace `/home/dev/work/any-store/db.go:506–520` with:

```go
func (db *db) Backup(ctx context.Context, path string) (err error) {
	// ~ SQLite's recommended online-backup pattern (see backup.c header
	// comment at lines 11-13 and the sqlite3_backup_init/step/finish
	// sequence). We open a fresh destination DB at `path` with
	// identical Options to the source so page sizes match
	// (btree.ErrBackupPageSizeMismatch otherwise).
	dstOpts := db.btreeDB.Options() // see Step 4 below — may need to add this accessor
	dstOpts.InMemory = false         // destination is always a file (user gave us a path)

	dstDB, err := btree.Open(path, dstOpts)
	if err != nil {
		return fmt.Errorf("open backup destination: %w", err)
	}
	defer func() {
		// Close on both success and failure. On failure we also unlink
		// the partial file so the caller can retry cleanly.
		if cerr := dstDB.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			_ = osRemove(path)
		}
	}()

	b, err := dstDB.BackupInit(db.btreeDB)
	if err != nil {
		return err
	}
	defer func() {
		// Finish is idempotent-ish (returns ErrBackupFinished on 2nd call);
		// we want to ensure the dst write tx is resolved.
		if ferr := b.Finish(); ferr != nil && err == nil && !errors.Is(ferr, btree.ErrBackupFinished) {
			err = ferr
		}
	}()

	// Copy in bounded batches so ctx cancellation is responsive.
	// SQLite's sqlite3BtreeCopyFile (backup.c:751) uses 0x7FFFFFFF in one
	// shot; we prefer yielding.
	const batch = 256
	for {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		err := b.Step(batch)
		if errors.Is(err, btree.ErrBackupDone) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
```

- [ ] **Step 4: Add `btree.DB.Options()` accessor and `osRemove` helper**

`DB.opts Options` exists on the struct at db.go:91 (verified). Add to `/home/dev/work/any-store/internal/btree/db.go` right after `Path()` at line 313:

```go
// Options returns a copy of the options this DB was opened with. Used
// by high-level callers (e.g. anystore.Backup) to open a destination
// DB with matching page size. No direct SQLite counterpart — SQLite
// uses attached-db paths to imply options.
func (db *DB) Options() Options {
	return db.opts
}
```

For `osRemove`: check how `osReadFile`/`osWriteFile` are declared:
```bash
grep -n "osReadFile\s*=\|osWriteFile\s*=\|osRemove" /home/dev/work/any-store/osfuncs*.go /home/dev/work/any-store/*.go 2>/dev/null | head
```
If they're declared as `var osReadFile = os.ReadFile` in `osfuncs.go`, add `var osRemove = os.Remove` in the same file next to them. If they are thin wrapper funcs, follow that style instead. Match what's there verbatim — do not introduce a new style.

- [ ] **Step 5: Run tests — expect PASS**

Run: `cd /home/dev/work/any-store && go test -short -run TestDb_Backup -count=1 -race -timeout=60s`
Expected: both tests PASS.

- [ ] **Step 6: Run short suite at both levels**

Run: `cd /home/dev/work/any-store && go test -short -count=1 -race -timeout=180s ./...`
Expected: PASS.

- [ ] **Step 7: Run InMemory variant to confirm backup now works in memory mode**

Run: `cd /home/dev/work/any-store && ANYSTORE_TEST_INMEMORY=1 go test -short -run TestDb_Backup$ -count=1 -race -timeout=60s`
Expected: PASS (the skip that was at db_test.go:117–119 is gone).

- [ ] **Step 8: Commit**

```bash
cd /home/dev/work/any-store
git add db.go db_test.go internal/btree/db.go
git -c commit.gpgsign=false commit -m "anystore: rewrite DB.Backup on top of new online-backup engine

Replaces the naive Checkpoint+osReadFile/osWriteFile copy with an
sqlite3_backup-style driver that opens a fresh destination DB and
drives Backup.Step/Finish. Now supports InMemory sources (skip
removed) and concurrent writers during backup."
```

---

## Task 12: Post-port validation — run stress-adjacent tests

**Why:** The backup hook fires from `pager.commit()`, which is on the critical path for every write. Stress tests exercise that path under concurrent load and will flush out races we didn't see in the targeted tests.

**Files:** none (test runner only).

- [ ] **Step 1: Run the usual stress battery**

Run:
```bash
cd /home/dev/work/any-store/internal/btree && go test -race -run 'TestCacheStress|TestCheckpoint|TestConcurrent|TestSavepoint|TestOverflow' -count=3 -timeout=300s
```
Expected: PASS. Known flaky: `TestGetPageReader_CacheHit` (EOF from FD exhaustion) and `TestMasterStore_InMemoryCheckpointBackfill` (temp dir cleanup) per memory `MEMORY.md`.

- [ ] **Step 2: Run targeted backup tests 10× under race**

Run:
```bash
cd /home/dev/work/any-store/internal/btree && go test -race -run TestBackup -count=10 -timeout=180s
```
Expected: PASS all 10 iterations.

- [ ] **Step 3: Run top-level tests 5× under race**

Run:
```bash
cd /home/dev/work/any-store && go test -race -run TestDb_Backup -count=5 -timeout=180s
```
Expected: PASS.

- [ ] **Step 4: If anything above fails, stop and investigate before the next step**

Do not mask flakes by rerunning; they indicate either a real race in the dispatcher or a test-setup issue. Use `go test -v -run <Name>` to get full output.

- [ ] **Step 5: Commit test-only changes if any were needed**

```bash
cd /home/dev/work/any-store
git status
# If any test files were tweaked for flake-resistance:
git add <files>
git -c commit.gpgsign=false commit -m "btree: stabilize backup tests under stress"
```

Otherwise skip.

---

## Task 13: Documentation sweep

**Why:** Keep the drift register in `internal/btree/NOTES.md` current (see MEMORY.md). Future maintainers following SQLite upstream need to know where we diverged.

**Files:**
- Modify: `/home/dev/work/any-store/internal/btree/NOTES.md`

- [ ] **Step 1: Add a backup section to NOTES.md**

Read the existing NOTES.md first to match style:
```bash
cat /home/dev/work/any-store/internal/btree/NOTES.md | head -50
```

Append a new section at the end (do not duplicate the DRIFT list verbatim — reference this plan):

```markdown
## Online Backup (backup.go)

Port of SQLite's `sqlite3_backup_*` API from `sqlite/src/backup.c`.
See `docs/plans/2026-04-22-sqlite-backup-port.md` for the full drift
register. Key entry points:

- `(*DB).BackupInit(src *DB) (*Backup, error)` — ~ `sqlite3_backup_init`
- `(*Backup).Step(n int) error` — ~ `sqlite3_backup_step`
- `(*Backup).Finish() error` — ~ `sqlite3_backup_finish`
- `(*Backup).Remaining() uint32` — ~ `sqlite3_backup_remaining`
- `(*Backup).PageCount() uint32` — ~ `sqlite3_backup_pagecount`

Hooks in `pager.commit()` (post-`wal.writeFrames`) and
`pager.checkpointWithMode()` (Restart/Truncate modes) dispatch the
C equivalents of `sqlite3BackupUpdate` and `sqlite3BackupRestart`
respectively. See `pager.dispatchBackupUpdate`/`dispatchBackupRestart`.

Anystore-level `(*db).Backup(ctx, path)` drives the engine by opening
a fresh destination DB at `path` with matching options.
```

- [ ] **Step 2: Commit**

```bash
cd /home/dev/work/any-store
git add internal/btree/NOTES.md
git -c commit.gpgsign=false commit -m "docs: add backup section to btree NOTES.md"
```

---

## Task 14: Final review — self-check

**Why:** One last pass before declaring the port complete.

- [ ] **Step 1: Re-read backup.c against backup.go side-by-side**

Mentally or on screen, walk through `/home/dev/work/sqlitec/src/backup.c` top-to-bottom and verify every public function has a Go counterpart (or an explicit DRIFT note in the plan):

| C function (backup.c line) | Go equivalent | Status |
|-----|-----|-----|
| `findBtree` (82) | n/a — DRIFT #2 (no attached-db name resolution) | ✅ skipped |
| `setDestPgsz` (112) | subsumed into `BackupInit` page-size check | ✅ Task 2 |
| `checkReadTransaction` (124) | `DB.HasOpenTransaction` + `ErrBackupDstBusy` | ✅ Task 8 |
| `sqlite3_backup_init` (140) | `(*DB).BackupInit` | ✅ Task 2 |
| `isFatalError` (217) | inline check `b.rc != ErrBackupDone` | ✅ Task 4 |
| `backupOnePage` (226) | `(*Backup).onePage` | ✅ Task 3 |
| `backupTruncateFile` (289) | n/a — DRIFT #5 (cross-size path unreachable; WAL-only) | ✅ skipped |
| `attachBackupObject` (302) | `(*pager).attachBackup` | ✅ Task 4 |
| `sqlite3_backup_step` (314) | `(*Backup).Step` | ✅ Task 4 |
| — WAL+size-mismatch readonly (378) | `ErrBackupPageSizeMismatch` at init | ✅ Task 2 |
| — cross-size packing (449–528) | unreachable given WAL-only (DRIFT #5) | ✅ n/a |
| — schema-cookie bump (423) | `(*Backup).finalize` writes offset 40 | ✅ Task 9 |
| — dst truncate (530) | `(*pager).truncateTo` via `finalize` | ✅ Task 9 |
| `sqlite3_backup_finish` (571) | `(*Backup).Finish` | ✅ Task 4 |
| `sqlite3_backup_remaining` (625) | `(*Backup).Remaining` | ✅ Task 4 |
| `sqlite3_backup_pagecount` (639) | `(*Backup).PageCount` | ✅ Task 4 |
| `backupUpdate` (661) | `(*Backup).update` | ✅ Task 5 |
| `sqlite3BackupUpdate` (686) | `(*pager).dispatchBackupUpdate` (commit hook) | ✅ Task 5 |
| `sqlite3BackupRestart` (701) — checkpoint-restart trigger | `(*pager).dispatchBackupRestart` | ✅ Task 6 |
| `sqlite3BackupRestart` (701) — external-process-write trigger | per-Step `FileChangeCounter` check | ✅ Task 10 |
| `nBackup` counter (204, 587) | n/a — DRIFT #6 (no ops to block) | ✅ skipped |
| `sqlite3BtreeCopyFile` (718) | n/a — DRIFT #3 (no VACUUM) | ✅ skipped |

If any row says "⚠️ verify" in actual results (not the plan as written), open the file and confirm the behavior. If any row is unexpectedly blank, file a follow-up — do not paper over gaps.

- [ ] **Step 2: Run the whole suite one last time**

Run:
```bash
cd /home/dev/work/any-store && go test -short -race -count=1 -timeout=300s ./...
```
Expected: PASS.

- [ ] **Step 3: Final commit (if any cleanup landed)**

If any additional fix commits landed during review, group them under one clean commit message. Otherwise proceed to the handoff.
