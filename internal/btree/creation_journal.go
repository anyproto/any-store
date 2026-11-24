package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// The creation journal is the rollback journal of the one transaction that
// does not go through the WAL: the write of a new database's page 1. SQLite
// creates page 1 inside a rollback-journal transaction whatever the journal
// mode (OP_JournalMode, vdbe.c:8061-8066; newDatabase, btree.c:3715). Its
// journal header records the size the file had, zero pages (writeJournalHdr,
// pager.c:1520). Page 1 is beyond that size, so it is not journaled, but it
// is marked PGHDR_NEED_SYNC (pager_write, pager.c:6093-6100) and reaches the
// file only after the journal is synced (syncJournal at pager.c:6612, before
// pager_write_pagelist at pager.c:6650). A creator that dies before its
// commit point leaves a hot journal (hasHotJournal, pager.c:5134-5199), and
// the next opener truncates the file back to zero pages (pager_playback,
// pager.c:2895-2897) before it trusts anything in it.
//
// Here the journal is <path>-journal, holds its header and nothing else, and
// lives from createPage1's first step to its last. The steps, all under the
// exclusive DB-file lock, after createIfEmpty has deleted a journal and a WAL
// it found beside the empty file:
//
//  1. open the journal, with the create flag, and write its header;
//  2. sync the journal; this first sync of its descriptor also syncs the
//     directory (dirSync), which makes the journal's name, the database
//     file's name and the stale WAL's removal durable;
//  3. write page 1;
//  4. sync the database file;
//  5. delete the journal: the commit point, as in journal_mode=DELETE.

// creationJournalSuffix names the journal after the database, as SQLite does
// (pager.c:4924-4927 `memcpy(pPtr, "-journal", 8)`).
const creationJournalSuffix = "-journal"

// creationJournalHdrSize is the size of the journal header: SQLite's 28
// bytes, in SQLite's order (writeJournalHdr, pager.c:1438-1530): 8 of magic,
// then five big-endian 32-bit fields, the record count, the checksum seed,
// the original page count, the sector size and the page size. There are no
// page records, so the record count, the checksum seed and the sector size
// are zero, and the header is not padded to a sector.
const creationJournalHdrSize = 28

// creationJournalMagic opens the header. It is not SQLite's aJournalMagic
// (pager.c:757): the file is not a journal SQLite can play back, and a
// journal SQLite wrote is never taken for one of these.
// DRIFT: the journal has its own magic; beside a database with content, a journal that does not carry it is left alone, where pager_playback ends at a bad magic and deletes the file. See docs/btree/NOTES.md#drift-2026-11-21-7-creation-journal
var creationJournalMagic = [8]byte{'a', 'n', 'y', 'j', 'r', 'n', 'l', 0x01}

// creationJournalHdr returns the header of a creation journal: original size
// zero pages, and the page size of the database being created.
func creationJournalHdr(pageSize uint32) []byte {
	hdr := make([]byte, creationJournalHdrSize)
	copy(hdr, creationJournalMagic[:])
	// hdr[8:12] record count, hdr[12:16] checksum seed, hdr[16:20] original
	// page count (pPager->dbOrigSize), hdr[20:24] sector size: all zero.
	binary.BigEndian.PutUint32(hdr[24:], pageSize)
	return hdr
}

// hasHotCreationJournal reports whether a creation journal that needs
// playing back lies beside the database file. The caller holds the DB-file
// lock, shared or exclusive, or accepts an answer that may be out of date.
//
// hasHotJournal's four conditions (pager.c:5108-5114) map as follows.
//   - "The journal file exists": the open below.
//   - "No process holds a RESERVED or greater lock": a creator holds the
//     DB-file lock exclusively for as long as its journal is live, so a
//     caller that holds the lock at all is looking at a dead creator's.
//   - "The database file itself is greater than 0 bytes": the caller's test.
//     An empty file is created again, and a journal beside it is deleted
//     first (deleteJournalOfEmptyDatabase), as SQLite deletes it
//     (pager.c:5178-5181).
//   - "The first byte of the journal file exists and is not 0x00".
//
// Beyond those, the header must be a whole creation header that records an
// original size of zero pages: the only rollback this journal describes is
// the truncation to nothing, and nothing else is ever rolled back from it.
func (p *pager) hasHotCreationJournal() (bool, error) {
	jf, err := osOpenFile(p.path+creationJournalSuffix, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("btree: open the creation journal: %w", err)
	}
	defer jf.Close()
	hdr := make([]byte, creationJournalHdrSize)
	n, err := jf.ReadAt(hdr, 0)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, fmt.Errorf("btree: read the creation journal: %w", err)
	}
	if n < creationJournalHdrSize || !bytes.Equal(hdr[:8], creationJournalMagic[:]) {
		return false, nil
	}
	return binary.BigEndian.Uint32(hdr[16:]) == 0, nil
}

// creationPending reports, with the file's FileInfo, whether the database
// file has no database a process may use: it is empty, or a creator died
// inside its creation and left its journal hot. Either way the next step is
// createIfEmpty.
func (p *pager) creationPending(f fileHandle) (os.FileInfo, bool, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	if info.Size() == 0 {
		return info, true, nil
	}
	hot, err := p.hasHotCreationJournal()
	return info, hot, err
}

// playbackCreationJournal rolls a dead creator's creation back, in
// pager_playback's order: the file is truncated to the original size its
// journal records, zero pages (pager_truncate, called for the first journal
// header, pager.c:2895-2897), and synced (sqlite3PagerSync, pager.c:2978-2981),
// and only then is the journal finalized, by its delete
// (pager_end_transaction, pager.c:2982-2983, 2109). Nothing is written to the
// journal: it lies as the dead creator synced it until the truncation is
// durable. The caller holds the DB-file lock exclusively and has seen the
// journal hot under it.
//
// A process killed between the truncation and its sync leaves an empty file
// beside the hot journal; deleteJournalOfEmptyDatabase finishes that
// playback.
//
// pagerSyncHotJournal (pager.c:4048) has no counterpart: the journal is hot
// only beside a file with content, and page 1 is written only after the
// journal and its directory entry were synced.
func (p *pager) playbackCreationJournal(f fileHandle) error {
	if h := testOpenStageHook; h != nil {
		h(p, "journal-playback")
	}
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("btree: roll back an interrupted database creation: %w", err)
	}
	return p.finalizeJournalOfEmptyDatabase(f)
}

// deleteJournalOfEmptyDatabase deletes whatever lies under the journal's
// name beside a database file of zero bytes, as hasHotJournal does: "If the
// database is zero pages in size, that means that either (1) the journal is a
// remnant from a prior database with the same name where the database file
// but not the journal was deleted, or (2) the initial transaction that
// populates a new database is being rolled back. In either case, the journal
// file can be deleted" (pager.c:5170-5181). What the file holds decides
// nothing: a dead creator's journal, SQLite's and any other file go the same
// way. With native file access it is found by a stat, as SQLite's
// xAccess(SQLITE_ACCESS_EXISTS) is one (unixAccess, os_unix.c:6895-6898), so
// a file this process may not read is deleted too. The caller holds the
// DB-file lock exclusively.
func (p *pager) deleteJournalOfEmptyDatabase(f fileHandle) error {
	if _, err := osStat(p.path + creationJournalSuffix); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("btree: look for a journal beside the empty database file: %w", err)
	}
	return p.finalizeJournalOfEmptyDatabase(f)
}

// finalizeJournalOfEmptyDatabase syncs the database file, which is empty,
// and then deletes the journal beside it. The sync comes first because the
// journal may be the hot journal of a playback that was killed after its
// truncation: until a sync covers the truncation, the disk may still hold the
// dead creator's page 1, and the journal is what marks it.
// DRIFT: an empty database file is synced before a journal found beside it is deleted, where hasHotJournal deletes the journal at once. See docs/btree/NOTES.md#drift-2026-11-21-7-creation-journal
func (p *pager) finalizeJournalOfEmptyDatabase(f fileHandle) error {
	if err := fdatasync(f); err != nil {
		return fmt.Errorf("btree: roll back an interrupted database creation: %w", err)
	}
	if err := osRemove(p.path + creationJournalSuffix); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("btree: delete the journal beside an empty database file: %w", err)
	}
	return nil
}

// creationJournal is the open journal of a creation in progress.
type creationJournal struct {
	f    fileHandle
	name string
	// dirSync is set by the open, which has the create flag, so the
	// journal's first sync also syncs its directory.
	dirSync dirSync
}

// openCreationJournal is step 1 of the creation: the journal is open and
// holds its header. pager_open_journal opens the journal with the create flag
// (SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_MAIN_JOURNAL,
// pager.c:5855-5863), which makes unixOpen set UNIXFILE_DIRSYNC on the
// descriptor (os_unix.c:6569-6573, 6777), and writes the header
// (writeJournalHdr, at pager.c:5910). createIfEmpty has deleted any file that
// lay under the name, so the open creates the journal.
func (p *pager) openCreationJournal() (*creationJournal, error) {
	name := p.path + creationJournalSuffix
	f, err := osOpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("btree: open the creation journal: %w", err)
	}
	j := &creationJournal{f: f, name: name}
	j.dirSync.set(name)
	if h := testOpenStageHook; h != nil {
		h(p, "journal-opened")
	}
	if _, err = f.WriteAt(creationJournalHdr(p.pageSize), 0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("btree: write the creation journal: %w", err)
	}
	return j, nil
}

// sync is step 2: the journal is synced before page 1 is written to the
// database file, as syncJournal syncs it before any page of the transaction
// is (pager.c:4286-4392, called at pager.c:6612). It is the descriptor's first
// sync, so the directory is synced after it (dirSync).
func (j *creationJournal) sync() error {
	if err := j.dirSync.sync(j.f); err != nil {
		return fmt.Errorf("btree: sync the creation journal: %w", err)
	}
	return nil
}

// abandon closes the journal of a creation that failed before its commit
// point. The file stays: hot, if its header reached it, and then the next
// open rolls back whatever the creation put in the database file.
func (j *creationJournal) abandon() {
	_ = j.f.Close()
}

// commit is step 5, the creation's commit point: the journal is closed and
// deleted, as pager_end_transaction finalizes a journal in
// journal_mode=DELETE (pager.c:2100-2110). A delete that fails fails the
// creation, which then is not committed, as the delete's error is the
// commit's there (pager.c:2109).
//
// The delete syncs no directory. SQLite passes pPager->extraSync to its
// delete (pager.c:2109). Until the directory is synced again the name can
// come back after a power loss, hot, beside the committed page 1; the open
// after the power loss then rolls the creation back and creates the database
// again, which loses nothing, as long as nothing was committed to the
// database. What is committed to it goes to the WAL, and a commit is durable
// only after a sync of the WAL, which is the first sync of some WAL
// descriptor or comes after one. Every WAL descriptor is opened after this
// delete, since a creation runs only while no process is attached to the
// database, and with the create flag (wal.open, as sqlite3WalOpen at
// wal.c:1719-1720), so its first sync also syncs this directory (dirSync,
// os_unix.c:6777, 3936-3954) and makes the delete durable first.
func (j *creationJournal) commit(p *pager) error {
	_ = j.f.Close()
	if err := osRemove(j.name); err != nil {
		return fmt.Errorf("btree: commit the creation of the database: delete its journal: %w", err)
	}
	if h := testOpenStageHook; h != nil {
		h(p, "journal-deleted")
	}
	return nil
}

// testSyncDirHook runs after every directory fsync that succeeded
// (syncdir_unix.go), with the directory's open descriptor, the one that was
// synced. Nil outside tests.
var testSyncDirHook func(dir *os.File)
