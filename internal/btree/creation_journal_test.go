package btree

import (
	"bytes"
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
		{"finalized: a header of zeros", make([]byte, creationJournalHdrSize), false},
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

// The order of a creation's steps, seen from inside it: the journal and its
// directory entry are synced before page 1 is written, the removal of a
// stale WAL comes before that directory sync, and the journal stays hot
// until page 1 is synced.
func TestCreationJournal_OrderOfTheCreation(t *testing.T) {
	for _, inProcess := range []bool{false, true} {
		t.Run(map[bool]string{false: "multiprocess", true: "inprocess"}[inProcess], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "test.db")
			journal := path + creationJournalSuffix
			require.NoError(t, os.WriteFile(path+"-wal", []byte("the log of a database that is gone"), 0o644))

			var events []string
			size := func(name string) int64 {
				info, err := os.Stat(name)
				if err != nil {
					return -1
				}
				return info.Size()
			}
			testSyncDirHook = func(name string) {
				require.Equal(t, journal, name)
				require.NoFileExists(t, path+"-wal", "the stale WAL is removed before the directory is synced")
				events = append(events, "sync-dir")
			}
			testOpenStageHook = func(p *pager, stage string) {
				events = append(events, stage)
				hot, err := p.hasHotCreationJournal()
				require.NoError(t, err)
				switch stage {
				case "journal-created":
					require.Zero(t, size(journal))
					require.Zero(t, size(path))
				case "before-journal-sync":
					require.True(t, hot)
					require.Zero(t, size(path), "page 1 is written before the journal is synced")
				case "before-page1-write":
					require.True(t, hot)
					require.Zero(t, size(path))
				case "before-page1-sync", "before-journal-finalize":
					require.True(t, hot, "the journal is finalized before page 1 is synced")
					require.Equal(t, int64(DefaultPageSize), size(path))
				case "journal-finalized":
					require.False(t, hot)
					require.FileExists(t, journal)
				}
			}
			t.Cleanup(func() { testOpenStageHook, testSyncDirHook = nil, nil })

			opts := DefaultOptions()
			opts.InProcess = inProcess
			db, err := testOpen(t, path, opts)
			testOpenStageHook, testSyncDirHook = nil, nil
			require.NoError(t, err)
			require.NoError(t, db.Close())
			require.Equal(t, []string{
				"before-lock",
				"journal-created",
				"before-journal-sync",
				"sync-dir",
				"before-page1-write",
				"before-page1-sync",
				"before-journal-finalize",
				"journal-finalized",
			}, events)
			require.NoFileExists(t, journal)
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
// opens with its rows beside a finalized journal that was never deleted, a
// journal SQLite wrote, and a file of that name that is no journal. The file
// is left as it is.
func TestCreationJournal_JournalThatIsNotHotIsLeftAlone(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content []byte
	}{
		{"finalized", make([]byte, creationJournalHdrSize)},
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

// A journal beside an empty or missing database file is a leftover (of a
// database that was removed, or of a creator that died before its page 1):
// the creation writes its own journal over it.
func TestCreationJournal_StaleJournalBesideEmptyFile(t *testing.T) {
	for _, content := range [][]byte{creationJournalHdr(DefaultPageSize), sqliteJournalHdr(), []byte("x")} {
		for _, emptyFile := range []bool{false, true} {
			path := filepath.Join(t.TempDir(), "test.db")
			require.NoError(t, os.WriteFile(path+creationJournalSuffix, content, 0o644))
			if emptyFile {
				require.NoError(t, os.WriteFile(path, nil, 0o644))
			}
			assertValidEmptyDB(t, path, DefaultOptions())
			require.NoFileExists(t, path+creationJournalSuffix)
		}
	}
}
