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
