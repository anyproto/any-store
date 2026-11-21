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
// exclusive DB-file lock:
//
//  1. create the journal and write its header;
//  2. sync the journal, then the directory (the journal's name is new);
//  3. write page 1;
//  4. sync the database file;
//  5. zero the journal's header and sync the journal: the commit point;
//  6. delete the journal.
//
// DRIFT: the commit point is the synced zeroing of the journal header, journal_mode=PERSIST's finalization, followed by the delete; SQLite's default finalization is the delete alone. See docs/btree/NOTES.md#drift-2026-11-21-7-creation-journal

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
// DRIFT: the journal has its own magic; a journal that does not carry it is left alone, where pager_playback ends at a bad magic and deletes the file. See docs/btree/NOTES.md#drift-2026-11-21-7-creation-journal
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
//     An empty file is created again, and the creation writes the journal
//     over whatever is there, where SQLite deletes it (pager.c:5170-5181).
//   - "The first byte of the journal file exists and is not 0x00": a
//     finalized header is zeros.
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

// playbackCreationJournal rolls a dead creator's creation back: the file is
// truncated to the original size its journal records, zero pages
// (pager_truncate, called for the first journal header, pager.c:2895-2897),
// and synced (pager_playback's sqlite3PagerSync, pager.c:2978-2982). The
// caller holds the DB-file lock exclusively and has seen the journal hot
// under it. The journal stays: the caller creates the database next, and
// that creation starts by writing the journal anew. A process that dies in
// between leaves an empty file, which is created again.
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
	if err := fdatasync(f); err != nil {
		return fmt.Errorf("btree: roll back an interrupted database creation: %w", err)
	}
	return nil
}

// openCreationJournal is steps 1 and 2 of the creation: the journal exists,
// holds its header, and both it and its name are durable when it returns.
// pager_open_journal creates the file and writes the header
// (pager.c:5831-5923, writeJournalHdr at pager.c:5910); syncJournal syncs it
// before any page of the transaction is written to the database file
// (pager.c:4286-4392). The journal is a new file, so that sync is followed
// by one of its directory: unixOpen marks a journal it may create
// (os_unix.c:6569-6573, UNIXFILE_DIRSYNC at os_unix.c:6777) and unixSync
// then fsyncs the directory (os_unix.c:3940-3952). The same directory sync
// covers the removal of a stale WAL just before (createIfEmpty), so that
// name is gone for good before page 1 exists.
func (p *pager) openCreationJournal() (fileHandle, error) {
	name := p.path + creationJournalSuffix
	jf, err := osOpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, fmt.Errorf("btree: create the creation journal: %w", err)
	}
	if h := testOpenStageHook; h != nil {
		h(p, "journal-created")
	}
	if _, err = jf.WriteAt(creationJournalHdr(p.pageSize), 0); err == nil {
		if h := testOpenStageHook; h != nil {
			h(p, "before-journal-sync")
		}
		if err = fdatasync(jf); err == nil {
			err = syncDir(name)
		}
	}
	if err != nil {
		_ = jf.Close()
		return nil, fmt.Errorf("btree: write the creation journal: %w", err)
	}
	return jf, nil
}

// finalizeCreationJournal is steps 5 and 6: the creation's commit point and
// the journal's removal. The header is zeroed and the journal synced, as
// zeroJournalHdr does for a persistent journal (pager.c:1399-1404, reached
// from pager_end_transaction, pager.c:2090-2094); from then on the journal
// is not hot whatever becomes of its name. Then it is deleted
// (pager.c:2107-2110), without a directory sync, as SQLite deletes with
// dirSync 0 unless synchronous=EXTRA: a name that comes back after a power
// loss names a journal of zeros. A failed delete is not an error for the
// same reason.
func (p *pager) finalizeCreationJournal(jf fileHandle) error {
	_, err := jf.WriteAt(make([]byte, creationJournalHdrSize), 0)
	if err == nil {
		err = fdatasync(jf)
	}
	if cerr := jf.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("btree: finalize the creation journal: %w", err)
	}
	if h := testOpenStageHook; h != nil {
		h(p, "journal-finalized")
	}
	_ = osRemove(p.path + creationJournalSuffix)
	return nil
}

// testSyncDirHook runs at the start of every syncDir with its argument. Nil
// outside tests.
var testSyncDirHook func(path string)
