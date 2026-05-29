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

	// ErrBackupPageSizeMismatch — faithful match of backup.c:378-383's
	// SQLITE_READONLY for WAL destinations with mismatched page size
	// (any-store is always WAL, so this always applies).
	ErrBackupPageSizeMismatch = errors.New("btree: backup source and destination must have equal page size")

	// ErrBackupDstBusy — ~ checkReadTransaction (backup.c:124–130)
	// ("destination database is in use"). Raised when dst already has an
	// open read/write transaction at BackupInit time.
	ErrBackupDstBusy = errors.New("btree: backup destination has an open transaction")

	// ErrBackupFinished — returned by Step after Finish or by Finish
	// called twice. DRIFT from C: backup.c:577 tolerates NULL; Go
	// prefers explicit misuse errors.
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
	dst        *DB    // ~ pDest            (backup.c:23)
	iDstSchema uint32 // ~ iDestSchema      (backup.c:24) — captured on first lock
	dstLocked  bool   // ~ bDestLocked      (backup.c:25)

	// dstWriteTx is held from first Step until Finish. SQLite tracks the
	// equivalent via Btree.inTrans; we use the explicit Go tx handle.
	dstWriteTx *WriteTx

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

	// lastFCC is the FileChangeCounter observed at the previous Step's
	// read-tx open. A jump between Steps signals an external-process
	// commit: the in-process dispatchBackupUpdate hook can't observe
	// cross-process writes, so we restart per backup.c:701-707.
	// 0 = "no prior Step" (init state).
	lastFCC uint32

	// finished is set by Finish. DRIFT from backup.c:577 which tolerates
	// NULL; we want explicit double-close detection.
	finished bool

	// mu protects Backup state against concurrent Step/Finish/update/restart
	// and the external Remaining/PageCount accessors. SQLite splits this
	// across two mutexes (src db + BtShared); our single-mutex model is
	// DRIFT #4.
	mu sync.Mutex
}

// BackupInit starts a new backup operation. The caller ("dst") is the
// database to write into; "src" is the database to read from.
// ~ sqlite3_backup_init (backup.c:140–210), minus the handle/name
// resolution described in DRIFT #2.
func (dst *DB) BackupInit(src *DB) (*Backup, error) {
	// ~ backup.c:166–170.
	if dst == src {
		return nil, ErrBackupSameDB
	}

	// ~ checkReadTransaction (backup.c:124-130): reject if dst already
	// has an open tx. We fast-fail here instead of letting BeginWrite
	// block in the first Step — matches SQLite's observable behavior.
	if dst.HasOpenTransaction() {
		return nil, ErrBackupDstBusy
	}

	// Faithful port of backup.c:378-383 for WAL destinations: we are
	// always WAL, so a size mismatch is always SQLITE_READONLY-equivalent.
	if dst.PageSize() != src.PageSize() {
		return nil, ErrBackupPageSizeMismatch
	}

	return &Backup{
		dst:   dst,
		src:   src,
		iNext: 1, // ~ backup.c:188
	}, nil
}

// onePage copies page iSrcPg from the source into the destination.
// ~ backupOnePage (backup.c:226–279). The bUpdate parameter matches
// SQLite: false for normal Step copies, true for update-callback copies.
//
// Our page sizes are always equal (enforced at BackupInit), so the
// for-loop at backup.c:251–276 collapses to one iteration with no
// offset arithmetic. DRIFT #1 removes the PENDING_BYTE_PAGE guard at
// backup.c:243/254.
// DRIFT: backup uses global pager dbSize for page count, not the read-tx snapshot See docs/btree/NOTES.md#drift-38-backup-step-page-count-from-global-dbsize-not-read-snapshot
func (b *Backup) onePage(iSrcPg uint32, srcData []byte, bUpdate bool) error {
	if b.dst.pager.pageSize != b.src.pager.pageSize {
		// Defensive: caught at Init, but reopen-race could in theory
		// change sizes. Return the same error.
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

	// ~ backup.c:269: memcpy(zOut, zIn, nCopy). nCopy == pageSize given
	// our same-size invariant; SQLite's MIN(nSrcPgsz, nDestPgsz) reduces
	// to pageSize.
	copy(dstPg.data, srcData[:b.dst.pager.pageSize])

	// ~ backup.c:271–273: "sqlite3Put4byte(&zOut[28], sqlite3BtreeLastPage(p->pSrc))".
	// For page 1 on the initial (non-update) copy, patch the database-size
	// field so dst's header reflects the source's page count.
	if iSrcPg == 1 && !bUpdate {
		putUint32BE(dstPg.data[28:32], b.src.DatabaseSize())
	}

	// ~ backup.c:270: invalidate the btree parse-cache on the dst page.
	// SQLite zeros the first byte of the "extra" space. any-store has no
	// MemPage.isInit equivalent; the in-cache dstPg.header is stale now
	// because we rewrote dstPg.data. Re-parse so the cached struct
	// matches the new bytes.
	off := 0
	if iSrcPg == 1 {
		off = dbHeaderSize
	}
	if dstPg.data[off] != 0 {
		dstPg.header.deserialize(dstPg.data[off:])
	}

	return nil
}

// putUint32BE writes a big-endian u32. Matches SQLite's sqlite3Put4byte
// (util.c).
func putUint32BE(buf []byte, v uint32) {
	_ = buf[3]
	buf[0] = byte(v >> 24)
	buf[1] = byte(v >> 16)
	buf[2] = byte(v >> 8)
	buf[3] = byte(v)
}

// Step copies up to nPage pages from the source to the destination.
// nPage < 0 means "copy all remaining pages in one call". Returns
// ErrBackupDone when every source page has been copied; returns nil
// when more work remains. Any other error is sticky — recorded in b.rc
// and re-returned by future calls (~ backup.c:329 + backup.c:558).
//
// ~ sqlite3_backup_step (backup.c:314–566).
// DRIFT: backup uses global pager dbSize for page count, not the read-tx snapshot See docs/btree/NOTES.md#drift-38-backup-step-page-count-from-global-dbsize-not-read-snapshot
// DRIFT: backup empty-source (nSrcPage==0 -> NewDb) path missing; truncateTo(0) errors See docs/btree/NOTES.md#drift-41-backup-empty-source-finalization-path-missing
func (b *Backup) Step(nPage int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.finished {
		return ErrBackupFinished
	}
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
	defer rtx.Rollback()

	// ~ sqlite3BackupRestart (backup.c:701-707) external-trigger path.
	// Page-1 FileChangeCounter changes on every write commit, including
	// from other processes. The in-process dispatchBackupUpdate hook
	// only fires for same-process commits (same DB handle), so this
	// detects the rest. Inline the iNext reset to avoid re-acquiring b.mu.
	if b.lastFCC != 0 && rtx.diskFileChangeCounter != b.lastFCC {
		b.iNext = 1
	}
	b.lastFCC = rtx.diskFileChangeCounter

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
		// DRIFT #1 skips PENDING_BYTE_PAGE check at backup.c:392.

		srcPg, err := b.src.pager.getPageReader(iSrcPg, rtx.walMaxFrame, rtx.cache)
		if err != nil {
			b.rc = err
			return err
		}
		// No explicit releasePage on reader cache — tx.Rollback handles it.

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
		// All pages copied. ~ backup.c:417-541 finalization: schema bump
		// and truncate dst to nSrcPage. Finish commits the write tx.
		if err := b.finalize(nSrcPage); err != nil {
			b.rc = err
			return err
		}
		b.rc = ErrBackupDone
		return ErrBackupDone
	}

	// Still pages to go — attach for update hooks (Task 5). No-op here.
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

// finalize performs the pre-commit housekeeping that backup.c does at
// the end of its final Step iteration (backup.c:417–541):
//   - ~ backup.c:423: sqlite3BtreeUpdateMeta(p->pDest, 1, p->iDestSchema+1)
//   - ~ backup.c:530: sqlite3PagerTruncateImage(pDestPager, nDestTruncate)
//
// Both mutate the destination; Finish then commits the result.
// Caller holds b.mu.
// DRIFT: backup dst page-1 header fields reverted at commit (only cookie+dbsize survive) See docs/btree/NOTES.md#drift-40-backup-page-1-header-fields-reverted-at-commit
// DRIFT: backup empty-source (nSrcPage==0 -> NewDb) path missing; truncateTo(0) errors See docs/btree/NOTES.md#drift-41-backup-empty-source-finalization-path-missing
// DRIFT: backup finalize omits SetVersion(2) for WAL dest (page-1 bytes 18/19) See docs/btree/NOTES.md#drift-42-backup-finalize-omits-setversion-for-wal-destination
func (b *Backup) finalize(nSrcPage uint32) error {
	// 1. Bump dst schema cookie. ~ sqlite3BtreeUpdateMeta writes meta
	// value #1 (the schema cookie) to the header at offset 40. Our page 1
	// has the source's header bytes copied in; we overwrite offset 40
	// with iDstSchema+1 so readers on dst observe a different cookie
	// than they did pre-backup, invalidating any cached schema parses.
	dstPg1, err := b.dst.pager.getWritablePage(1)
	if err != nil {
		return err
	}
	newCookie := b.iDstSchema + 1
	putUint32BE(dstPg1.data[40:44], newCookie)
	// Keep the in-memory header in sync so commit's page-1 serialize
	// (pager.commit:1371) doesn't clobber our write.
	b.dst.pager.header.SchemaCookie = newCookie
	b.dst.pager.releasePage(dstPg1)

	// 2. Truncate dst to nSrcPage. ~ sqlite3PagerTruncateImage
	// (backup.c:530). Shrinks dst.dbSize; file shrinks at next checkpoint.
	return b.dst.pager.truncateTo(nSrcPage)
}

// update is called by the source pager right after a committed page is
// written to the WAL. If the page has already been copied into the
// destination (iPage < b.iNext), re-copy it so the destination stays
// current. ~ backupUpdate (backup.c:661–685).
//
// Caller holds the source pager's backups list lock momentarily — see
// pager.dispatchBackupUpdate — but the callback runs outside that lock
// to avoid nesting b.mu under backupsMu.
// DRIFT: backup re-copies pages after DONE (DONE not treated fatal); corrupts finalized dst See docs/btree/NOTES.md#drift-39-backup-treats-done-as-non-fatal-allowing-post-completion-re-
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
	// be wrong to redo mid-backup.
	if err := b.onePage(iPage, data, true); err != nil {
		b.rc = err
	}
}

// restart resets the backup to start copying from page 1 again. Called
// by the source pager when the page cache is invalidated in a way that
// makes "already copied" tracking meaningless (e.g. WAL reset).
// ~ sqlite3BackupRestart (backup.c:701–707).
func (b *Backup) restart() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.iNext = 1
}

// Finish releases all resources associated with a Backup and commits
// or rolls back the destination write transaction based on b.rc.
// ~ sqlite3_backup_finish (backup.c:571–619).
// DRIFT: backup dst commit deferred Step->Finish; durability/error point moved See docs/btree/NOTES.md#drift-43-backup-commit-point-moved-from-step-to-finish
// DRIFT: double/late backup Finish returns ErrBackupFinished vs C no-op OK See docs/btree/NOTES.md#drift-44-backup-finish-double-call-returns-error-not-no-op
func (b *Backup) Finish() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.finished {
		return ErrBackupFinished
	}
	b.finished = true

	if b.isAttached {
		b.src.pager.detachBackup(b)
		b.isAttached = false
	}

	// ~ backup.c:600: if a dst tx is still open, commit on success, else rollback.
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
