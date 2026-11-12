package btree

// This file is a port of SQLite's online-backup API from src/backup.c. Each
// function is annotated with its C counterpart. Intentional drift from SQLite
// is documented under "Intentional DRIFT" in
// any-store-tests:docs/any-store/btree/plans/2026-04-22-sqlite-backup-port.md.

import (
	"context"
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

	// ErrBackupFormatMismatch — the destination's on-disk format differs from
	// the source's. A backup copies the source's pages, page 1 included, so
	// it would turn the destination into a file of the source's format under
	// handles and snapshots that read it by the old one. Create the
	// destination with Options.SchemaFormat set to the source's SchemaFormat.
	ErrBackupFormatMismatch = errors.New("btree: backup source and destination must have the same schema format")

	// ErrBackupDstBusy — ~ checkReadTransaction (backup.c:124–130)
	// ("destination database is in use"). Raised when dst already has an
	// open read/write transaction at BackupInit time.
	ErrBackupDstBusy = errors.New("btree: backup destination has an open transaction")

	// ErrBackupFinished — returned by Step after Finish or by Finish called twice.
	// DRIFT: SQLite's finish is NULL-tolerant/idempotent; Go surfaces double-finish. See docs/btree/NOTES.md#drift-44-backup-finish-double-call-returns-error-not-no-op
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
	iNext      uint32 // ~ iNext            (backup.c:27) — next source page to copy (1-based)

	// dstWriteTx is held from first Step until Finish. SQLite tracks the
	// equivalent via Btree.inTrans; we use the explicit Go tx handle.
	dstWriteTx *WriteTx

	src *DB // ~ pSrc (backup.c:29)

	// rc is sticky: once a fatal error occurs, every subsequent call
	// short-circuits with it. ~ backup.c:31 + isFatalError (backup.c:217–219).
	rc error

	// Statistics updated at every Step. ~ backup.c:36–37.
	nRemaining uint32 // pages left to copy
	nPagecount uint32 // total pages in source (as of last Step)

	// epoch and exact are what the previous Step's snapshot saw of the
	// source's change state (ReadTx.ChangeEpoch); stepped is whether there
	// was one. The pages copied so far are of one committed state of the
	// source as long as every Step's snapshot is exact for the same epoch:
	// this process's own commits reach the copy through update, and anything
	// else moves the epoch. Otherwise the copy starts over, as SQLite's does
	// when the Step's own read transaction resets the pager
	// (sqlite3BackupRestart from pager_reset, pager.c:1774; backup.c:351).
	epoch   uint64
	exact   bool
	stepped bool

	// busy is the busy error of the last Step, nil if that Step was not
	// busy: see stepBusy.
	busy error

	dstLocked bool // ~ bDestLocked (backup.c:25)

	// isAttached mirrors backup.c:39 — once true, this Backup is in the
	// src pager's backups list and receives update/restart callbacks.
	isAttached bool

	// finished is set by Finish; enables explicit double-close detection.
	// DRIFT: SQLite's finish is NULL-tolerant/idempotent; Go surfaces double-finish. See docs/btree/NOTES.md#drift-44-backup-finish-double-call-returns-error-not-no-op
	finished bool

	// mu protects Backup state against concurrent Step/Finish/update/restart
	// and the external Remaining/PageCount accessors. SQLite splits this
	// across two mutexes (src db + BtShared); we use a single mutex.
	mu sync.Mutex
}

// BackupInit starts a new backup operation. The caller ("dst") is the
// database to write into; "src" is the database to read from.
// ~ sqlite3_backup_init (backup.c:140–210).
// DRIFT: WAL-only: same-page-size enforced at init; no PENDING_BYTE/findDatabase/nBackup See docs/btree/NOTES.md#old-drift-backup-intentional-simplifications
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

	// DRIFT: SQLite's backup turns the destination into the source's file format; here the two must already agree, because the leaf payload rule is held per open file, not per page. See docs/btree/NOTES.md#drift-2026-11-11-2-backup-refuses-a-destination-of-another-schema-format
	if dst.pager.tableLeaf.Load() != src.pager.tableLeaf.Load() {
		return nil, ErrBackupFormatMismatch
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
// DRIFT: WAL-only: same-page-size enforced at init; no PENDING_BYTE/findDatabase/nBackup See docs/btree/NOTES.md#old-drift-backup-intentional-simplifications
func (b *Backup) onePage(iSrcPg uint32, srcData []byte, bUpdate bool, snapPageCount uint32) error {
	if b.dst.pager.pageSize != b.src.pager.pageSize {
		// Defensive: caught at Init, but reopen-race could in theory
		// change sizes. Return the same error.
		return ErrBackupPageSizeMismatch
	}
	if !b.dstLocked {
		return errors.New("btree: Backup.onePage called before dst locked (bug)")
	}

	// ~ backup.c:255–256: sqlite3PagerGet + sqlite3PagerWrite in one call.
	// getWritablePage combines both: it admits the page
	// into writerCache, handles savepoint copy-on-write, and marks the
	// page dirty via makeDirty.
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
		putUint32BE(dstPg.data[28:32], snapPageCount)
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

// backupBusy reports an error that says "not now" and nothing about the
// backup: a busy lock, a recovery in progress, no free reader slot, or the
// caller's ctx ending. Such an error is returned and not kept: a later Step
// on the same Backup goes on. ~ isFatalError (backup.c:217-219), which keeps
// SQLITE_BUSY and SQLITE_LOCKED out of p->rc.
// DRIFT: ErrBusyRecovery is not sticky for a backup; C's isFatalError compares exact codes, so SQLITE_BUSY_RECOVERY is fatal there See docs/btree/NOTES.md#drift-2026-11-13-2-a-backup-step-that-met-a-recovery-can-be-retried
func backupBusy(ctx context.Context, err error) bool {
	if cerr := ctx.Err(); cerr != nil && errors.Is(err, cerr) {
		return true // the ctx's own error; any other error keeps its meaning
	}
	return errors.Is(err, ErrBusy) || errors.Is(err, ErrBusySnapshot) ||
		errors.Is(err, ErrBusyRecovery) || errors.Is(err, ErrReadersBusy)
}

// stepBusy records a Step's busy result for Finish and returns it.
// sqlite3_backup_step stores SQLITE_BUSY in p->rc (backup.c:558), where it
// stops no later step (isFatalError) and is what sqlite3_backup_finish
// returns if no later step replaced it (backup.c:603).
func (b *Backup) stepBusy(err error) error {
	if backupBusy(context.Background(), err) { // a busy error, not the ctx's
		b.busy = err
	}
	return err
}

// testBackupSampledHook, when non-nil, runs in Step right after it read the
// change epoch for its snapshot. Test-only.
var testBackupSampledHook func()

// Step copies up to nPage pages from the source to the destination.
// nPage < 0 means "copy all remaining pages in one call". Returns
// ErrBackupDone when every source page has been copied; returns nil
// when more work remains.
//
// ErrBusy means the Step did nothing and can be tried again: the source has
// a write transaction open in this process (as sqlite3_backup_step returns
// SQLITE_BUSY for one, backup.c:340-341), or no reader slot is free. The
// other busy errors of a transaction begin, and the error of a ctx that
// ended, are returned the same way (backupBusy). Any other error is sticky,
// and so is ErrBackupDone: recorded in b.rc and re-returned by future calls
// (~ backup.c:329 + backup.c:558).
//
// A Step holds the source's write lock of this process from its start to its
// end, so no write transaction of this process overlaps it: a commit and the
// updates it sends this backup are either wholly before the Step's snapshot
// or wholly after the Step (backup.c:323-324, 668). It takes that lock, and
// its reader slot, without waiting: the caller may be the one holding them.
//
// ~ sqlite3_backup_step (backup.c:314–566).
// DRIFT: backup empty-source (nSrcPage==0 -> NewDb) path missing; truncateTo(0) errors See docs/btree/NOTES.md#drift-41-backup-empty-source-finalization-path-missing
func (b *Backup) Step(ctx context.Context, nPage int) error {
	// Before anything is taken: the non-waiting begin below does not look
	// at ctx.
	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.finished {
		return ErrBackupFinished
	}
	// ~ isFatalError check at backup.c:329. A backup that is done is not
	// stepped again either: SQLITE_DONE is fatal in that sense
	// (backup.c:217-219), and the source's commits since are not sent to it
	// (update), so the pages a further Step copied would be of a later
	// state than the rest.
	if b.rc != nil {
		return b.rc
	}
	b.busy = nil

	// Lock destination (exclusive/write tx) on first Step.
	// ~ backup.c:366–371: sqlite3BtreeBeginTrans(p->pDest, 2, &iDestSchema).
	// Before the source's lock is taken: this begin can wait.
	if !b.dstLocked {
		wtx, err := b.dst.BeginWriteContext(ctx)
		if err != nil {
			if !backupBusy(ctx, err) {
				b.rc = err
				return err
			}
			return b.stepBusy(err)
		}
		b.dstWriteTx = wtx
		b.iDstSchema = b.dst.localSchemaCookie.Load() // ~ iDestSchema capture
		b.dstLocked = true
	}

	// ~ backup.c:337-344: "If the source pager is currently in a
	// write-transaction, return SQLITE_BUSY immediately."
	if !b.src.tryLockWriter() {
		return b.stepBusy(ErrBusy)
	}
	defer b.src.unlockWriter()

	// Open a read transaction on source for this Step. ~ backup.c:346–353.
	// DRIFT: SQLite conditionally opens a tx only if none exists, to
	// support explicit sqlite3_backup_step-inside-a-user-tx patterns.
	// any-store doesn't expose that; we always open+close our own.
	rtx, err := b.src.beginRead(ctx, false, false)
	if err != nil {
		if errors.Is(err, ErrReadersBusy) {
			return b.stepBusy(ErrBusy)
		}
		if !backupBusy(ctx, err) {
			b.rc = err
			return err
		}
		return b.stepBusy(err)
	}
	defer rtx.Rollback()

	// ~ sqlite3BackupRestart (backup.c:701-707), as pager_reset calls it
	// when the Step's read transaction finds the source changed
	// (pager.c:1774). See the epoch field.
	epoch, exact := rtx.ChangeEpoch()
	if testBackupSampledHook != nil {
		testBackupSampledHook()
	}
	if b.stepped && (!exact || !b.exact || epoch != b.epoch) {
		b.iNext = 1
	}
	b.epoch, b.exact, b.stepped = epoch, exact, true

	// ~ backup.c:394: nSrcPage = sqlite3BtreeLastPage(pSrc), read once under
	// the source read-lock. Derive from the read-tx snapshot opened above so
	// the loop bound, nPagecount/nRemaining, finalize, and the page-1 size
	// patch are all consistent with the same snapshot. It is the logical
	// size lockBtree derives from page 1, not the pager's count, which can
	// include pages past it (ReadTx.DatabaseSize).
	nSrcPage, err := rtx.DatabaseSize()
	if err != nil {
		b.rc = err
		return err
	}
	srcPgsz := b.src.PageSize()

	// Main copy loop. ~ backup.c:390–401.
	for ii := 0; (nPage < 0 || ii < nPage) && b.iNext <= nSrcPage; ii++ {
		iSrcPg := b.iNext

		srcPg, err := b.src.pager.getPageReader(iSrcPg, rtx.walMaxFrame, rtx.cache)
		if err != nil {
			b.rc = err
			return err
		}
		err = b.onePage(iSrcPg, srcPg.data[:srcPgsz], false, nSrcPage)
		// ~ backup.c:397 sqlite3PagerUnref(pSrcPg). A cached page is unpinned
		// so the reader cache can evict it instead of filling up with pinned
		// pages; an uncached fallback page (soft create refused) returns its
		// buffer to the slab or pool; the slab has no other way to get it back.
		b.src.pager.releasePage(srcPg)
		if err != nil {
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
// DRIFT: backup empty-source (nSrcPage==0 -> NewDb) path missing; truncateTo(0) errors See docs/btree/NOTES.md#drift-41-backup-empty-source-finalization-path-missing
// DRIFT: backup finalize omits SetVersion(2) for WAL dest (page-1 bytes 18/19) See docs/btree/NOTES.md#drift-42-backup-finalize-omits-setversion-for-wal-destination
// DRIFT: the in-memory header is reloaded from the copied page 1 and written back at commit; SQLite keeps the copied page image and patches fields in it. See docs/btree/NOTES.md#drift-2026-11-11-6-backup-reloads-the-header-from-the-copied-page-1
func (b *Backup) finalize(nSrcPage uint32) error {
	// 1. Re-sync the in-memory header from the source page-1 bytes, then
	// bump the dst schema cookie. ~ sqlite3BtreeUpdateMeta writes meta
	// value #1 (the schema cookie) to the header at offset 40. Our page 1
	// has the source's header bytes copied in; we overwrite offset 40
	// with iDstSchema+1 so readers on dst observe a different cookie
	// than they did pre-backup, invalidating any cached schema parses.
	dstPg1, err := b.dst.pager.getWritablePage(1)
	if err != nil {
		return err
	}
	// Re-sync the in-memory db-header from the source page-1 bytes that
	// onePage copied verbatim into dstPg1 (~ backup.c:269 memcpy). The pager
	// unconditionally re-serializes p.header over page 1 at commit
	// (pager.go:1946); without this reload that serialize would revert every
	// page-1 header field (freelist trunk pointers, page size, text encoding,
	// user/application metadata, salt, file-change/version counters, ...) to
	// the destination's own stale values. SQLite has no such re-serialize, so
	// the verbatim-copied source fields survive there (only offset 28/40 and
	// the WAL version bytes are patched). This mirrors the established
	// savepoint page-1 restore at pager.go:2274.
	b.dst.pager.header.deserialize(dstPg1.data[:dbHeaderSize])
	// Bump the schema cookie AFTER the deserialize so it is not overwritten.
	// ~ sqlite3BtreeUpdateMeta writes meta value #1 (the cookie) to offset 40.
	newCookie := b.iDstSchema + 1
	putUint32BE(dstPg1.data[40:44], newCookie)
	// Keep the in-memory header in sync so commit's page-1 serialize
	// (pager.go:1946) doesn't clobber our write.
	b.dst.pager.header.SchemaCookie = newCookie
	b.dst.pager.releasePage(dstPg1)

	// 2. Set dst's logical page count to nSrcPage. ~ sqlite3PagerTruncateImage
	// (backup.c:530). In SQLite the destination pager's dbSize is grown as each
	// page is written through sqlite3PagerWrite; here onePage copies pages via
	// getWritablePage, which does NOT grow p.dbSize (only allocatePage does).
	// So after copying a source larger than the dst's prior size, p.dbSize is
	// still stale (e.g. 1 for a fresh dst) even though nSrcPage physical pages
	// were written. Commit serializes p.header.DatabaseSize = p.dbSize.Load()
	// (pager.go:1937), so a stale p.dbSize would write a wrong page-1
	// DatabaseSize — a reopened dst would then report DatabaseSize=1 while
	// holding nSrcPage pages, and the descent bound check would flag a valid
	// interior child (pgno>1) as corrupt. Grow p.dbSize to nSrcPage here; the
	// shrink case (dst was larger) is handled by truncateTo, which also evicts
	// writerCache pages beyond nSrcPage.
	if cur := b.dst.pager.dbSize.Load(); nSrcPage > cur {
		b.dst.pager.dbSize.Store(nSrcPage)
		b.dst.pager.header.DatabaseSize = nSrcPage
	}
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
func (b *Backup) update(iPage uint32, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// ~ backup.c:675 — skip if fatal error or page not yet copied.
	// Any non-nil b.rc is fatal-for-update here, including ErrBackupDone:
	// isFatalError(SQLITE_DONE)==TRUE (backup.c:217-219, DONE is neither
	// SQLITE_OK/BUSY/LOCKED), so backupUpdate never re-touches a DONE backup's
	// finalized destination.
	//
	// A finished backup is not touched either. The source copies its list of
	// backups and calls them outside the list's lock, so Finish can run
	// between the copy and this call, and the destination may be in a
	// transaction of its owner's by now. SQLite's finish holds the source
	// mutex that backupUpdate asserts (backup.c:579-580, 668).
	if b.finished || b.rc != nil {
		return
	}
	if iPage >= b.iNext {
		return
	}

	// bUpdate=true suppresses the page-1 DatabaseSize patch, which would
	// be wrong to redo mid-backup.
	if err := b.onePage(iPage, data, true, 0); err != nil {
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
	if b.finished {
		return // see update
	}
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
	if b.rc == nil && finishErr == nil {
		// Not done and not failed: the last Step's busy result, if it was
		// busy (backup.c:603 returns p->rc, where the step left it).
		return b.busy
	}
	if b.rc == nil {
		return finishErr
	}
	return b.rc
}
