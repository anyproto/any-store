//go:build (linux || darwin) && (amd64 || arm64)

package btree

// Two-process tests of database creation (re-exec pattern, like
// multiprocess_test.go). A child is held at a named point of its open by
// testOpenStageHook and released over its stdin, so each interleaving is
// pinned rather than raced.

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	createChildRoleEnv = "TEST_CREATE_CHILD_ROLE"
	createChildPathEnv = "TEST_CREATE_CHILD_PATH"
	createChildPassEnv = "TEST_CREATE_CHILD_PASS"
	createChildModeEnv = "TEST_CREATE_CHILD_MODE"
)

// createOpts are the options of both processes: multi-process mode, and a
// passphrase (not a raw 32-byte key, so the key comes from the file's salt)
// when pass is set.
func createOpts(pass string) Options {
	opts := Options{
		PageSize:              4096,
		CacheSize:             100,
		InProcess:             false,
		DisableAutoCheckpoint: true,
	}
	if pass != "" {
		opts.Key = []byte(pass)
		opts.KDFIterations = 1000
	}
	return opts
}

// createChild is a re-exec'ed child the parent talks to in lines.
type createChild struct {
	t     *testing.T
	cmd   *exec.Cmd
	stdin io.WriteCloser
	lines chan string
}

func startCreateChild(t *testing.T, role, path, pass, mode string) *createChild {
	t.Helper()
	name := t.Name()
	if i := strings.IndexByte(name, '/'); i >= 0 {
		name = name[:i]
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.timeout=120s")
	cmd.Env = append(os.Environ(),
		createChildRoleEnv+"="+role,
		createChildPathEnv+"="+path,
		createChildPassEnv+"="+pass,
		createChildModeEnv+"="+mode,
	)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	c := &createChild{t: t, cmd: cmd, stdin: stdin, lines: make(chan string, 64)}
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			c.lines <- sc.Text()
		}
		close(c.lines)
	}()
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return c
}

// expect waits for the child to say token. Anything else it says is logged;
// its exit or a 60 s silence fails the test.
func (c *createChild) expect(token string) {
	c.t.Helper()
	timeout := time.After(60 * time.Second)
	for {
		select {
		case line, ok := <-c.lines:
			if !ok {
				c.t.Fatalf("child exited before saying %q", token)
			}
			if line == token {
				return
			}
			c.t.Logf("child: %s", line)
		case <-timeout:
			c.t.Fatalf("child did not say %q", token)
		}
	}
}

func (c *createChild) send(line string) {
	c.t.Helper()
	_, err := io.WriteString(c.stdin, line+"\n")
	require.NoError(c.t, err)
}

// childSay and childWait are the child's half of the conversation.
func childSay(token string) {
	fmt.Println(token)
	_ = os.Stdout.Sync()
}

func childWait(t *testing.T, in *bufio.Reader, token string) {
	t.Helper()
	line, err := in.ReadString('\n')
	require.NoError(t, err, "the parent went away")
	require.Equal(t, token, strings.TrimSpace(line))
}

// TestMultiProcessCreateRaceOneDatabase: two processes open the same new path
// at once. The child is held just before its write of page 1 — after it has
// decided the file is a new database — while the parent opens the same path.
// Both must end up on one database: each sees the other's commit, and the
// file checks clean, live and after a reopen.
//
// Before creation ran under an exclusive DB-file lock, the parent's open went
// through while the child was held (both had seen an empty file), and the
// child's page 1 then replaced the parent's, checkpointed, database.
func TestMultiProcessCreateRaceOneDatabase(t *testing.T) {
	if os.Getenv(createChildRoleEnv) == "creator" {
		path, pass := os.Getenv(createChildPathEnv), os.Getenv(createChildPassEnv)
		in := bufio.NewReader(os.Stdin)
		testOpenStageHook = func(_ *pager, stage string) {
			if stage == "before-page1-write" {
				testOpenStageHook = nil
				childSay("STALLED")
				childWait(t, in, "GO")
			}
		}
		db, err := testOpen(t, path, createOpts(pass))
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		createPutRow(t, db, "child")
		childSay("WROTE")
		childWait(t, in, "CHECK")
		createRequireRow(t, db, "parent")
		createRequireRow(t, db, "child")
		require.NoError(t, db.IntegrityCheck())
		childSay("OK")
		childWait(t, in, "EXIT")
		require.NoError(t, db.Close())
		childSay("CLOSED")
		return
	}
	if os.Getenv(createChildRoleEnv) != "" {
		return
	}

	for _, pass := range []string{"", "correct horse battery staple"} {
		name := "plain"
		if pass != "" {
			name = "passphrase"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "race.db")
			opts := createOpts(pass)

			c := startCreateChild(t, "creator", path, pass, "")
			c.expect("STALLED")

			type opened struct {
				db  *DB
				err error
			}
			openedCh := make(chan opened, 1)
			go func() {
				db, err := testOpen(t, path, opts)
				openedCh <- opened{db, err}
			}()

			// With creation under an exclusive lock the parent's open waits
			// for the child, so the child is released after a moment. An
			// open that does not wait gets its commit and its checkpoint in
			// first: the interleaving that lost a database.
			var db *DB
			select {
			case o := <-openedCh:
				require.NoError(t, o.err)
				db = o.db
				createPutRow(t, db, "parent")
				require.NoError(t, db.Checkpoint(CheckpointTruncate))
			case <-time.After(250 * time.Millisecond):
			}
			c.send("GO")
			c.expect("WROTE")
			if db == nil {
				o := <-openedCh
				require.NoError(t, o.err, "the open that lost the race must open the winner's database")
				db = o.db
				createPutRow(t, db, "parent")
			}
			defer func() { _ = db.Close() }()

			c.send("CHECK")
			c.expect("OK")
			createRequireRow(t, db, "child")
			createRequireRow(t, db, "parent")
			require.NoError(t, db.IntegrityCheck())

			c.send("EXIT")
			c.expect("CLOSED")
			require.NoError(t, db.Close())

			db, err := testOpen(t, path, opts)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			createRequireRow(t, db, "child")
			createRequireRow(t, db, "parent")
			names, err := db.ListNamespaces()
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"child", "parent"}, names)
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// TestMultiProcessCreateManyAtOnce: several processes, released together,
// open the same new path with nothing holding any of them back, write a
// namespace each and close. Every open succeeds (none of them is told the
// file is busy or corrupt), and the database holds every process's commit.
// With a passphrase each process draws its own salt before it holds a lock,
// and all but one must take the creator's.
func TestMultiProcessCreateManyAtOnce(t *testing.T) {
	if os.Getenv(createChildRoleEnv) == "racer" {
		path, pass := os.Getenv(createChildPathEnv), os.Getenv(createChildPassEnv)
		in := bufio.NewReader(os.Stdin)
		childSay("READY")
		childWait(t, in, "GO")
		db, err := testOpen(t, path, createOpts(pass))
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		createPutRow(t, db, os.Getenv(createChildModeEnv))
		childSay("WROTE")
		childWait(t, in, "EXIT")
		require.NoError(t, db.Close())
		childSay("CLOSED")
		return
	}
	if os.Getenv(createChildRoleEnv) != "" {
		return
	}

	const nChildren = 6
	for _, pass := range []string{"", "correct horse battery staple"} {
		name := "plain"
		if pass != "" {
			name = "passphrase"
		}
		t.Run(name, func(t *testing.T) {
			for round := 0; round < 3; round++ {
				path := filepath.Join(t.TempDir(), "race.db")
				children := make([]*createChild, nChildren)
				var want []string
				for i := range children {
					ns := fmt.Sprintf("racer-%d", i)
					want = append(want, ns)
					children[i] = startCreateChild(t, "racer", path, pass, ns)
				}
				for _, c := range children {
					c.expect("READY")
				}
				for _, c := range children {
					c.send("GO")
				}
				for _, c := range children {
					c.expect("WROTE")
				}
				for _, c := range children {
					c.send("EXIT")
				}
				for _, c := range children {
					c.expect("CLOSED")
				}

				db, err := testOpen(t, path, createOpts(pass))
				require.NoError(t, err)
				names, err := db.ListNamespaces()
				require.NoError(t, err)
				require.ElementsMatch(t, want, names)
				for _, ns := range want {
					createRequireRow(t, db, ns)
				}
				require.NoError(t, db.IntegrityCheck())
				require.NoError(t, db.Close())
			}
		})
	}
}

// TestMultiProcessCreateLoserKeyFromWinnersSalt: an open with a passphrase
// finds no file on its first, unlocked look, and by the time it holds the
// DB-file lock another process has created the database. Its key must come
// from the salt in the file it opened, not from the salt it drew for a file
// it did not create.
//
// Before the codec was settled under the lock, this open failed with an AEAD
// error that matches ErrPageIntegrity, on a healthy database.
func TestMultiProcessCreateLoserKeyFromWinnersSalt(t *testing.T) {
	const pass = "correct horse battery staple"
	if os.Getenv(createChildRoleEnv) == "late-opener" {
		path := os.Getenv(createChildPathEnv)
		in := bufio.NewReader(os.Stdin)
		testOpenStageHook = func(_ *pager, stage string) {
			if stage == "before-lock" {
				testOpenStageHook = nil
				childSay("STALLED")
				childWait(t, in, "GO")
			}
		}
		db, err := testOpen(t, path, createOpts(pass))
		require.NoError(t, err, "the database is healthy: its creator's key opens it")
		defer func() { _ = db.Close() }()
		createRequireRow(t, db, "parent")
		createPutRow(t, db, "child")
		require.NoError(t, db.IntegrityCheck())
		childSay("OK")
		childWait(t, in, "EXIT")
		require.NoError(t, db.Close())
		childSay("CLOSED")
		return
	}
	if os.Getenv(createChildRoleEnv) != "" {
		return
	}

	for _, peer := range []string{"attached", "closed"} {
		t.Run("creator-"+peer, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "enc.db")
			opts := createOpts(pass)

			c := startCreateChild(t, "late-opener", path, pass, "")
			c.expect("STALLED")
			require.NoFileExists(t, path, "the child has looked and found no file")

			db, err := testOpen(t, path, opts)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			createPutRow(t, db, "parent")
			if peer == "closed" {
				require.NoError(t, db.Close())
			}

			c.send("GO")
			c.expect("OK")
			c.send("EXIT")
			c.expect("CLOSED")

			if peer == "closed" {
				db, err = testOpen(t, path, opts)
				require.NoError(t, err)
				defer func() { _ = db.Close() }()
			}
			createRequireRow(t, db, "child")
			createRequireRow(t, db, "parent")
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// TestStaleWALBesideEmptyDatabaseNotReplayed: a process dies leaving
// committed frames in the -wal, and the main file is then removed or cut to
// zero bytes. The next open creates a new database, and a new database is
// empty: none of the old frames are in it, in either mode of the wal-index.
// SQLite deletes a WAL it finds beside a database of zero pages
// (pagerOpenWalIfPresent).
//
// The old frames were recovered and applied before: a WAL holding the whole
// old database brought it back, and a WAL holding its tail gave the new file
// the old page count or pages of a tree whose other pages were gone.
func TestStaleWALBesideEmptyDatabaseNotReplayed(t *testing.T) {
	if os.Getenv(createChildRoleEnv) == "wal-writer" {
		path, pass := os.Getenv(createChildPathEnv), os.Getenv(createChildPassEnv)
		db, err := testOpen(t, path, createOpts(pass))
		require.NoError(t, err)
		fill := func(nsName string, n int) {
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.CreateNamespace(nsName)
			require.NoError(t, err)
			for i := 0; i < n; i++ {
				require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "key-%06d", i), make([]byte, 300)))
			}
			tx.MarkDataChanged()
			require.NoError(t, tx.Commit())
		}
		switch os.Getenv(createChildModeEnv) {
		case "whole":
			// The whole database is in the WAL.
			fill("old", 200)
		case "tail":
			// Most of it is in the file; the WAL holds what came after the
			// checkpoint, a new page 1 among it.
			fill("old", 3000)
			require.NoError(t, db.Checkpoint(CheckpointTruncate))
			fill("old2", 50)
		}
		childSay("READY")
		// Killed here by the parent: no Close, no checkpoint.
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
		return
	}
	if os.Getenv(createChildRoleEnv) != "" {
		return
	}

	copyFile := func(t *testing.T, from, to string) {
		data, err := os.ReadFile(from)
		if os.IsNotExist(err) {
			return
		}
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(to, data, 0o644))
	}

	for _, pass := range []string{"", "correct horse battery staple"} {
		for _, mode := range []string{"whole", "tail"} {
			name := mode + "-plain"
			if pass != "" {
				name = mode + "-passphrase"
			}
			t.Run(name, func(t *testing.T) {
				src := filepath.Join(t.TempDir(), "old.db")
				c := startCreateChild(t, "wal-writer", src, pass, mode)
				c.expect("READY")
				require.NoError(t, c.cmd.Process.Kill())
				_ = c.cmd.Wait()
				info, err := os.Stat(src + "-wal")
				require.NoError(t, err)
				require.Greater(t, info.Size(), int64(walHeaderSize), "the dead process left frames in its WAL")

				for _, variant := range []string{"main-removed", "main-truncated"} {
					for _, inProcess := range []bool{false, true} {
						sub := variant + "-multiprocess"
						if inProcess {
							sub = variant + "-inprocess"
						}
						t.Run(sub, func(t *testing.T) {
							path := filepath.Join(t.TempDir(), "new.db")
							copyFile(t, src+"-wal", path+"-wal")
							copyFile(t, src+"-wal-shm", path+"-wal-shm")
							if variant == "main-truncated" {
								require.NoError(t, os.WriteFile(path, nil, 0o644))
							}
							opts := createOpts(pass)
							opts.InProcess = inProcess

							db, err := testOpen(t, path, opts)
							require.NoError(t, err)
							defer func() { _ = db.Close() }()
							names, err := db.ListNamespaces()
							require.NoError(t, err)
							require.Empty(t, names, "a new database has no namespaces")
							require.NoError(t, db.IntegrityCheck())
							require.Equal(t, uint32(1), db.pager.dbSize.Load(), "a new database is one page")

							createPutRow(t, db, "new")
							require.NoError(t, db.IntegrityCheck())
							require.NoError(t, db.Close())

							db, err = testOpen(t, path, opts)
							require.NoError(t, err)
							defer func() { _ = db.Close() }()
							names, err = db.ListNamespaces()
							require.NoError(t, err)
							require.Equal(t, []string{"new"}, names)
							createRequireRow(t, db, "new")
							require.NoError(t, db.IntegrityCheck())
						})
					}
				}
			})
		}
	}
}

// creationStagesSeen records the creation stages the opens of this process
// pass through until the returned function is called.
func creationStagesSeen(t *testing.T) (seen *[]string, stop func()) {
	t.Helper()
	seen = new([]string)
	testOpenStageHook = func(_ *pager, stage string) {
		if stage != "before-lock" && stage != "db-lock-busy" {
			*seen = append(*seen, stage)
		}
	}
	stop = func() { testOpenStageHook = nil }
	t.Cleanup(stop)
	return seen, stop
}

// killedCreatorChild is a creator that stops at one point of its creation,
// named by the mode, and is killed there by the parent.
func killedCreatorChild(t *testing.T) bool {
	if os.Getenv(createChildRoleEnv) != "killed-creator" {
		return os.Getenv(createChildRoleEnv) != ""
	}
	path, pass, at := os.Getenv(createChildPathEnv), os.Getenv(createChildPassEnv), os.Getenv(createChildModeEnv)
	testOpenStageHook = func(_ *pager, stage string) {
		if stage == at {
			childSay("AT")
			time.Sleep(time.Hour) // killed by the parent
		}
	}
	_, err := testOpen(t, path, createOpts(pass))
	t.Errorf("the open went past %q: %v", at, err)
	return true
}

// TestCreationJournal_CreatorKilled: a creator is killed (SIGKILL) at each
// point of its creation, and another process opens the path. The opener must
// end with a database whose page 1 it knows to be synced:
//
//   - killed before page 1 is written, the creator leaves an empty file, and
//     the opener creates the database;
//   - killed with page 1 in the file and the creation not committed (before
//     the page's sync, or after it and before the journal is deleted), it
//     leaves a hot journal: the opener rolls the file back to nothing and
//     creates the database itself;
//   - killed after the commit point, the journal's delete, it leaves a
//     database, and the opener opens it.
//
// Without the journal an opener took the dead creator's page 1 as it found
// it, synced or not, and could acknowledge a synced commit to the WAL on top
// of a page 1 the next power loss takes away; the open after that found an
// empty file and deleted the WAL.
func TestCreationJournal_CreatorKilled(t *testing.T) {
	if killedCreatorChild(t) {
		return
	}
	for _, tc := range []struct {
		at         string
		page1      bool // the dead creator's page 1 is in the file
		journal    bool // the dead creator's journal is beside it
		rolledBack bool // the opener rolls it back
		creates    bool // the opener creates the database
	}{
		{at: "journal-opened", journal: true, creates: true},
		{at: "before-journal-sync", journal: true, creates: true},
		{at: "before-page1-write", journal: true, creates: true},
		{at: "before-page1-sync", page1: true, journal: true, rolledBack: true, creates: true},
		{at: "before-journal-delete", page1: true, journal: true, rolledBack: true, creates: true},
		{at: "journal-deleted", page1: true},
	} {
		for _, pass := range []string{"", "correct horse battery staple"} {
			name := tc.at + "-plain"
			if pass != "" {
				name = tc.at + "-passphrase"
			}
			t.Run(name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "new.db")
				opts := createOpts(pass)
				c := startCreateChild(t, "killed-creator", path, pass, tc.at)
				c.expect("AT")
				require.NoError(t, c.cmd.Process.Kill())
				_ = c.cmd.Wait()

				info, err := os.Stat(path)
				require.NoError(t, err)
				if tc.page1 {
					require.Equal(t, int64(opts.PageSize), info.Size(), "setup: the dead creator's page 1 is in the file")
				} else {
					require.Zero(t, info.Size(), "setup: the dead creator wrote no page 1")
				}
				if tc.journal {
					require.FileExists(t, path+creationJournalSuffix, "setup: the dead creator's journal")
				} else {
					require.NoFileExists(t, path+creationJournalSuffix, "setup: the dead creator committed")
				}

				seen, stop := creationStagesSeen(t)
				db, err := testOpen(t, path, opts)
				stop()
				require.NoError(t, err)
				defer func() { _ = db.Close() }()
				if tc.rolledBack {
					require.Contains(t, *seen, "journal-playback", "the opener used a page 1 whose creation was not committed")
				} else {
					require.NotContains(t, *seen, "journal-playback")
				}
				if tc.creates {
					require.Contains(t, *seen, "before-page1-write", "the opener did not create the database")
					require.Contains(t, *seen, "journal-deleted")
					require.NoFileExists(t, path+creationJournalSuffix)
				} else {
					require.Empty(t, *seen, "the opener created a database over a committed one")
				}

				names, err := db.ListNamespaces()
				require.NoError(t, err)
				require.Empty(t, names)
				require.Equal(t, uint32(1), db.pager.dbSize.Load())
				createPutRow(t, db, "opener")
				require.NoError(t, db.IntegrityCheck())
				require.NoError(t, db.Close())

				db, err = testOpen(t, path, opts)
				require.NoError(t, err)
				defer func() { _ = db.Close() }()
				createRequireRow(t, db, "opener")
				require.NoError(t, db.IntegrityCheck())
			})
		}
	}
}

// TestCreationJournal_WaitingOpenerRollsBack: an open is already waiting for
// the creator's lock when the creator is killed between its write of page 1
// and the sync. The lock comes free with the page in the file; the waiting
// open must not take it for a database. The creator is killed only once the
// open has been refused the lock.
func TestCreationJournal_WaitingOpenerRollsBack(t *testing.T) {
	if killedCreatorChild(t) {
		return
	}
	path := filepath.Join(t.TempDir(), "new.db")
	opts := createOpts("")
	c := startCreateChild(t, "killed-creator", path, "", "before-page1-sync")
	c.expect("AT")

	var (
		seen      []string
		refused   = make(chan struct{})
		refusedMu sync.Once
	)
	testOpenStageHook = func(_ *pager, stage string) {
		switch stage {
		case "before-lock":
		case "db-lock-busy":
			refusedMu.Do(func() { close(refused) })
		default:
			seen = append(seen, stage)
		}
	}
	t.Cleanup(func() { testOpenStageHook = nil })
	type opened struct {
		db  *DB
		err error
	}
	openedCh := make(chan opened, 1)
	go func() {
		db, err := testOpen(t, path, opts)
		openedCh <- opened{db, err}
	}()
	select {
	case o := <-openedCh:
		if o.db != nil {
			_ = o.db.Close()
		}
		t.Fatalf("the open did not wait for the creator: %v", o.err)
	case <-refused:
	}
	require.NoError(t, c.cmd.Process.Kill())
	_ = c.cmd.Wait()

	o := <-openedCh
	testOpenStageHook = nil
	require.NoError(t, o.err)
	defer func() { _ = o.db.Close() }()
	require.Contains(t, seen, "journal-playback", "the waiting open used the dead creator's page 1")
	require.Contains(t, seen, "before-page1-write")
	require.NoFileExists(t, path+creationJournalSuffix)
	createPutRow(t, o.db, "opener")
	require.NoError(t, o.db.IntegrityCheck())
}
