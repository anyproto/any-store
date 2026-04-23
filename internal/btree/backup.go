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
