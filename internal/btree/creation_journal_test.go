package btree

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// sqliteJournalHdr is a rollback-journal header as SQLite writes it
// (aJournalMagic, pager.c:757), recording an original size of zero pages.
func sqliteJournalHdr() []byte {
	hdr := make([]byte, 512)
	copy(hdr, []byte{0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7})
	hdr[26] = 0x10 // page size 4096 in bytes 24..27
	return hdr
}

func createPutRow(t *testing.T, db *DB, nsName string) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace(nsName)
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("row-of-"+nsName), []byte("written-by-"+nsName)))
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())
}

func createRequireRow(t *testing.T, db *DB, nsName string) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	ns, err := rtx.GetNamespace(nsName)
	require.NoError(t, err, "namespace %q written by the other process must be there", nsName)
	val, err := rtx.Get(ns, []byte("row-of-"+nsName))
	require.NoError(t, err)
	require.Equal(t, []byte("written-by-"+nsName), val)
}

func TestCreationJournal_WhichHeaderIsHot(t *testing.T) {
	valid := creationJournalHdr(4096)
	origSizeOne := bytes.Clone(valid)
	origSizeOne[19] = 1
	for _, tc := range []struct {
		name    string
		content []byte // nil: no file
		hot     bool
	}{
		{"no journal", nil, false},
		{"empty file", []byte{}, false},
		{"a header of zeros", make([]byte, creationJournalHdrSize), false},
		{"a creation header", valid, true},
		{"a creation header with bytes after it", append(bytes.Clone(valid), 1, 2, 3), true},
		{"a header cut short", valid[:creationJournalHdrSize-1], false},
		{"SQLite's journal", sqliteJournalHdr(), false},
		{"an original size that is not zero", origSizeOne, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &pager{path: filepath.Join(t.TempDir(), "test.db")}
			if tc.content != nil {
				require.NoError(t, os.WriteFile(p.path+creationJournalSuffix, tc.content, 0o644))
			}
			hot, err := p.hasHotCreationJournal()
			require.NoError(t, err)
			require.Equal(t, tc.hot, hot)
		})
	}
}

// A hot journal beside a file with content: the content is a dead creator's
// page 1, whatever state the crash left it in, and the open rolls it back
// and creates the database. Without the journal the same bytes are a
// database file to be judged by its header.
func TestCreationJournal_HotJournalRollsBackWhateverIsInTheFile(t *testing.T) {
	whole := referenceEmptyPage1(t, DefaultOptions())
	torn := bytes.Clone(whole)
	clear(torn[dbHeaderSize:]) // the header reached the file, the page's body did not
	for _, tc := range []struct {
		name    string
		content []byte
		bare    error // the open's error for the same file with no journal; nil: it opens
	}{
		{"whole page 1", whole, nil},
		{"page 1 of zeros", make([]byte, DefaultPageSize), ErrHeaderCorrupt},
		{"half a header", whole[:dbHeaderSize/2], ErrHeaderCorrupt},
		{"header without its page", torn[:dbHeaderSize], nil},
	} {
		for _, inProcess := range []bool{false, true} {
			name := tc.name + "/multiprocess"
			if inProcess {
				name = tc.name + "/inprocess"
			}
			t.Run(name, func(t *testing.T) {
				opts := DefaultOptions()
				opts.InProcess = inProcess

				if tc.bare != nil {
					bare := filepath.Join(t.TempDir(), "bare.db")
					require.NoError(t, os.WriteFile(bare, tc.content, 0o644))
					_, err := testOpen(t, bare, opts)
					require.ErrorIs(t, err, tc.bare, "setup: without a journal the file is refused")
				}

				path := filepath.Join(t.TempDir(), "test.db")
				require.NoError(t, os.WriteFile(path, tc.content, 0o644))
				require.NoError(t, os.WriteFile(path+creationJournalSuffix, creationJournalHdr(DefaultPageSize), 0o644))

				var seen []string
				testOpenStageHook = func(_ *pager, stage string) { seen = append(seen, stage) }
				t.Cleanup(func() { testOpenStageHook = nil })
				db, err := testOpen(t, path, opts)
				testOpenStageHook = nil
				require.NoError(t, err)
				require.Contains(t, seen, "journal-playback")
				require.Contains(t, seen, "before-page1-write")
				require.NoError(t, db.Close())
				require.NoFileExists(t, path+creationJournalSuffix)
				img, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, whole, img, "the file is a new database's page 1")
				assertValidEmptyDB(t, path, opts)
			})
		}
	}
}

// A journal that is not hot is not played back: a database with rows in it
// opens with its rows beside a header of zeros, a journal SQLite wrote, and a
// file of that name that is no journal. The file is left as it is.
func TestCreationJournal_JournalThatIsNotHotIsLeftAlone(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content []byte
	}{
		{"zeros", make([]byte, creationJournalHdrSize)},
		{"sqlite", sqliteJournalHdr()},
		{"empty", []byte{}},
		{"text", []byte("not a journal")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "test.db")
			opts := DefaultOptions()
			db, err := testOpen(t, path, opts)
			require.NoError(t, err)
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.CreateNamespace("ns")
			require.NoError(t, err)
			require.NoError(t, tx.Put(ns, []byte("k"), []byte("v")))
			require.NoError(t, tx.Commit())
			require.NoError(t, db.Close())

			journal := path + creationJournalSuffix
			require.NoError(t, os.WriteFile(journal, tc.content, 0o644))
			var seen []string
			testOpenStageHook = func(_ *pager, stage string) { seen = append(seen, stage) }
			t.Cleanup(func() { testOpenStageHook = nil })
			db, err = testOpen(t, path, opts)
			testOpenStageHook = nil
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			require.Equal(t, []string{"before-lock"}, seen, "the open rolled back or created")
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			ns, err = rtx.GetNamespace("ns")
			require.NoError(t, err)
			val, err := rtx.Get(ns, []byte("k"))
			require.NoError(t, err)
			require.Equal(t, []byte("v"), val)
			require.NoError(t, rtx.Rollback())
			left, err := os.ReadFile(journal)
			require.NoError(t, err)
			require.Equal(t, tc.content, left)
		})
	}
}

// Whatever lies under the journal's name beside an empty or missing database
// file is deleted, and the database is created: a dead creator's journal,
// whole or in part, SQLite's journal of a database once at that path, any
// other file.
func TestCreationJournal_JournalBesideEmptyFileIsDeleted(t *testing.T) {
	hdr := creationJournalHdr(DefaultPageSize)
	origSizeOne := bytes.Clone(hdr)
	origSizeOne[19] = 1
	tornMagic := make([]byte, creationJournalHdrSize)
	copy(tornMagic, hdr[:3])
	persisted := sqliteJournalHdr()
	clear(persisted[:creationJournalHdrSize]) // journal_mode=PERSIST's finalized journal
	for _, tc := range []struct {
		name    string
		content []byte
	}{
		{"empty", []byte{}},
		{"a creation header", hdr},
		{"a creation header with bytes after it", append(bytes.Clone(hdr), 1, 2, 3)},
		{"the start of the magic", hdr[:3]},
		{"the start of the magic, then zeros", tornMagic},
		{"a header cut short", hdr[:creationJournalHdrSize-1]},
		{"a header of zeros", make([]byte, creationJournalHdrSize)},
		{"the magic with an original size of one page", origSizeOne},
		{"sqlite", sqliteJournalHdr()},
		{"sqlite, finalized by zeroing", persisted},
		{"zeros longer than a header", make([]byte, creationJournalHdrSize+1)},
		{"text", []byte("x")},
	} {
		for _, emptyFile := range []bool{false, true} {
			for _, inProcess := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/emptyFile=%v/inProcess=%v", tc.name, emptyFile, inProcess), func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "test.db")
					journal := path + creationJournalSuffix
					require.NoError(t, os.WriteFile(journal, tc.content, 0o644))
					if emptyFile {
						require.NoError(t, os.WriteFile(path, nil, 0o644))
					}
					opts := DefaultOptions()
					opts.InProcess = inProcess
					db, err := testOpen(t, path, opts)
					require.NoError(t, err)
					require.NoFileExists(t, journal)
					require.NoError(t, db.Close())
					assertValidEmptyDB(t, path, opts)
				})
			}
		}
	}
}
