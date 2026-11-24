# Migrating from format 5 to format 6

A database file keeps the on-disk format it was created with. Releases from
v2.0.0 to v2.4.x create format 5 files; v2.5.0 and later create format 6
files, and keep reading and writing format 5 files as they are. Moving an
existing file onto format 6 means copying it into a new file.

## When to migrate

Migrating is optional. A format 5 file works on v2.5.0 as it did before.

Format 6 changes two things, both only for a file created in it:

- A document or index entry of up to about one page (4,045 bytes on the
  default page size) stays on its b-tree leaf. Format 5 moves anything over
  997 bytes to an overflow page of its own: one more page to read, and to
  store, for every such document.
- The planner's statistics are stored compressed: about 100 bytes per index
  for a small collection, where format 5 keeps 8 KB per indexed field.

A database whose stored documents (after compression) are often between
about 1 KB and 4 KB gains the most; one with many indexes saves the space
of their statistics. Format 6 is one way: see
[Going back](#going-back) before migrating.

`SchemaFormat` reports the format of an open database. `LatestSchemaFormat`
is the format this release creates:

```go
format, err := anystore.SchemaFormat(db)
if err != nil {
	return err
}
if format < anystore.LatestSchemaFormat {
	// a candidate for CopyToLatestFormat
}
```

## Copying

```go
report, err := anystore.CopyToLatestFormat(ctx, db, "/data/app.db.format6", config)
```

`CopyToLatestFormat` writes a new database in the latest format holding
every collection, document and index of `db`:

- **One snapshot.** The source is read in one read transaction, from the
  catalog to the last document. Commits made while the copy runs, by this
  process or another, are not in the copy; the source stays open and
  writable meanwhile.
- **A logical copy.** Each collection is created with its primary key and
  its compression setting. Its documents are inserted again, 1,000 to a
  write transaction, and its indexes are built again afterwards from their
  complete definitions, vector and full-text ones included. What can come
  out different is listed under [What can differ](#what-can-differ).
- **`config` is the destination's.** It sets encryption, durability and the
  other options of the new file. `nil` means the defaults, so the copy of an
  encrypted database is not encrypted unless `config` says so. Pass the
  `Config` you will open the new file with. `InMemory` is refused. A
  collection that inherits the database's compression setting keeps
  inheriting, from `config.DisableCompression`.
- **Nothing is replaced.** If the destination path, or its `-wal`,
  `-wal-shm`, `.lock` or `-journal` file, exists, or the destination is one of those
  names of an existing file (`app.db-wal` beside `app.db`, in any case:
  `app.db-WAL` is `app.db`'s log on a case-insensitive filesystem, and is
  refused on every filesystem), the call returns
  `ErrDestinationExists` and leaves that file alone. Found before the copy
  takes its locks, it touches nothing. The check is made again under the
  locks and right before the rename (below), and a file that appeared
  meanwhile is found then, after the copy created its lock files, or built
  the whole copy under its staging name, and it removes them again. Apart
  from the read bookkeeping in the source's `-wal-shm`, which the copy's
  read of the source changes as any reader's does, no file but the copy's
  own lock and staging files is created, written or removed; the source's
  files are never renamed, removed or otherwise written.
- **One copy at a time to a destination.** For the whole call the copy holds
  an exclusive lock of the operating system on the file
  `<destination>.copy-lock` (`flock` on Unix, `LockFileEx` on Windows). The
  system drops the lock if the process dies, so a killed copy never blocks
  the next one. A second call for the same destination, from this process or
  another, does not wait: it returns `ErrBusy` at once, and touches no file
  but the copy's lock files. A destination that names a file a database
  keeps beside it (`app.db-wal`, `app.db-wal-shm`, `app.db.lock` or
  `app.db-journal`, in any case) is read as `app.db`'s file once `app.db` exists, so a copy to it
  holds `app.db.copy-lock` too: a copy to `app.db` and a copy to one of
  those names do not run at once either, and the one that comes second
  returns `ErrBusy`. The lock files are removed when the call returns,
  except after a failure to take a lock: then an unlocked lock file can
  stay, and the next call reuses it. On js/wasm and wasip1, which have no
  such lock, the lock is held in the process and covers every destination:
  one copy at a time runs in the process, and no lock file is made.
- **Published by a rename.** The copy is built in the staging file
  `<destination>.copy-staging`, in the destination's own directory,
  checkpointed, verified and closed, and only then renamed to the
  destination. The rename stays within one directory, so it works wherever
  the destination can be written, including filesystems and platforms
  without hard links. Right before the rename the destination and its
  `-wal`, `-wal-shm`, `.lock` and `-journal` are checked again, so a file that appeared
  there while the copy ran also gives `ErrDestinationExists`. The check and
  the rename are not one step, and the locks exclude only other copies: a
  file that something else creates at the destination between them is
  replaced, and a `-wal`, `-wal-shm`, `.lock` or `-journal` that it creates beside the
  destination then is not seen, and is read as the copy's own once the copy
  is opened. So do not let anything else create files under the
  destination's name while the copy runs.
- **Reserved names.** `<destination>.copy-lock` and
  `<destination>.copy-staging`, with the `-wal`, `-wal-shm`, `.lock` and
  `-journal` of the latter, belong to the copy, and so, for a destination
  `app.db-wal`, `app.db-wal-shm`, `app.db.lock` or `app.db-journal`, does
  `app.db.copy-lock`: it creates
  them, and removes whatever it finds there. Staging files a killed copy
  left are removed by the next copy to that destination, under its lock.
  Keep nothing of your own under these names; a destination that ends in one
  of them is refused. Two things are not removed. One is an open database at
  `<destination>.copy-staging`: an open database holds a lock of the
  operating system on its file (`flock` on Unix, `LockFileEx` on Windows),
  which the copy tries before it removes anything; if it is held, the call
  returns `ErrBusy` and removes nothing under the staging name. The other is
  anything a copy does not leave at a lock file's name or at the staging
  name. The copy opens a lock file, and `<destination>.copy-staging` to try
  its lock, only if it is a regular file with one link, which is what a copy
  leaves there. A symbolic link, a file with other hard links (on Unix;
  Windows does not report the count) or anything but a regular file at one
  of those names fails the call with an error, and the copy neither opens,
  writes nor removes it: it can be another name of a file that a database of
  this process has open, such as its `-wal-shm`, and closing a descriptor of
  a file drops every POSIX record lock the process holds on it. The copy
  assumes that nothing else links files to its reserved names: a reserved
  name that is itself a symbolic link, a non-regular file or a hard link is
  refused, but a regular file there that something else reaches through a
  link of its own, such as another database's `-wal-shm` made a symbolic
  link to it, is not detected. js/wasm and wasip1 have no such lock and make
  no lock file; there only a source under these names is refused, as
  described below.
- **Verified.** Before it is published, each collection of the copy is
  checked against the source's snapshot: its options, its document count
  and its index definitions. Then `IntegrityCheck` runs on the copy. A
  mismatch fails the call.
- **Returned closed.** The copy has no `-wal`, `-wal-shm` or `-journal`
  file beside it.
  `report` gives the number of collections, documents and indexes copied,
  and how long the call took.
- **The source's files are never renamed or removed.** A source whose
  database file, or its `-wal`, `-wal-shm`, `.lock` or `-journal`, is under one of the
  destination's reserved names (`app.db.copy-staging` copied to `app.db`)
  is refused before anything is created or removed, on every platform: the
  copy would take it for a killed copy's staging file. The files are
  compared by identity: what each reserved name names, with the files the
  source has open (its database file, its log, and its `-wal-shm` where
  that is a file) and with its `.lock` and `-journal`. So every spelling of the
  destination is refused: a relative path, one through a symbolic link,
  one in another case on a case-insensitive filesystem. So is a source
  opened under a path that names another file than the one it opened: a
  `..` after a symbolic link to a directory goes up from the link's
  target, as the system resolves it, not from the link.

The call fails with:

| Error | When |
| ----- | ---- |
| `ErrUnsupportedDB` | `db` was not opened by this package (a type of your own that implements `DB`). Nothing is created. |
| `ErrTxInContext` | `ctx` carries a read or write transaction. The copy reads in a transaction of its own, as SQLite refuses `VACUUM` inside one. |
| `ErrDestinationExists` | The destination or one of its sidecars exists, or appeared while the copy ran. No file was created, written or removed but the copy's own lock and staging files, apart from the read bookkeeping in the source's `-wal-shm`, which the copy's read of the source changes as any reader's does: a file that was there before the call refuses it before it creates anything, and one that appeared while it ran refuses it after it made its lock files or its copy, which it removed again, with any a killed copy had left under those names. The source's files are never renamed, removed or otherwise written. |
| `ErrBusy` | Another call is copying to the same destination right now, or to a name one of the two would keep beside the other (`app.db` and `app.db-wal`, either way round; on js/wasm and wasip1, to any destination in the process), or a database is open at the destination's staging name, `<destination>.copy-staging`. No file was created, written or removed but the copy's lock files (`<destination>.copy-lock`, and `app.db.copy-lock` for a destination `app.db-wal`): a lock the call took before it failed was released and its file removed, which may be one a killed copy left there. Try again once that call has returned, or that database is closed. |
| `ErrCopyPublished` | The copy is complete, verified and published at the destination, and a step after that failed: the sync of the destination's directory, or the removal of a file left under the staging name. It wraps that failure, and the report is filled. Use the copy; to make its name durable, sync its directory. |
| any other | The copy failed: a collection, index or document could not be copied (the error names it, see [What can differ](#what-can-differ)), the copy did not match the source, or an I/O operation failed. Cancelling `ctx` stops the copy with the context's error. |

The call creates the destination only by publishing a complete, verified,
closed copy. On a failure before that it closes the copy and removes its
staging files. A removal that fails is joined to the returned error, naming
the file left. The staging files are the only files a failed call owns, and
the only ones a crash before the publication leaves, beside an unlocked
lock file of one byte. There is nothing to clean up by hand: the next copy to
that destination removes them. If no copy will follow, remove the reserved
names once no copy is running.

After `ErrDestinationExists` the files at and beside the destination are not
the call's: they were there before it, or someone else put them there.
The error is no reason to remove them. A failure after the rename, of the
directory sync or of the removal of a staging leftover, comes back as
`ErrCopyPublished` wrapping what failed, with the filled report: the copy
is published at the destination, and running the copy again would only meet
`ErrDestinationExists`. Every other error comes with a zero report and
nothing at the destination that the call put there. A destination
the call published that survives a crash is always the complete copy.
Its name is durable only once the directory sync after the rename has
succeeded (on Unix; on Windows it relies on NTFS's metadata journal, as
below): power lost before that can leave no destination at all, with the
copy still under its staging name, which the next copy removes.

The CLI has the same copy: `db.migrate("new.db")` in `any-store-cli2` writes
the copy and prints the report. It never replaces the open file.

## Replacing the database with the copy

`CopyToLatestFormat` does not put the copy in the source's place: the
library cannot know whether another process has the source open, or will
open it. Replacing the source is safe only under your own exclusion, because
a commit made after the copy's snapshot is not in the copy and would be lost
with the old file. Do it in this order:

1. **Stop writes and keep them stopped**, in every process, before the copy
   starts and until the new file is open. Open no new handle on the database
   in the meantime.
2. **Copy** with `CopyToLatestFormat`, into the same directory, which must
   succeed. It returns with the copy closed. `ErrCopyPublished` counts as
   success here: the copy is complete, and step 5 syncs the directory again,
   checked, before anything relies on the copy's name. Before copying,
   remove a copy that an earlier run of this protocol left at the same
   name: it may be older than the database.
3. **Checkpoint the source** with `FlushModeCheckpointTruncate` and check the
   error. `Close` swallows a checkpoint error, so it cannot be relied on.
4. **Close every handle on the source, in every process**, idle and
   read-only ones included, and make sure they are closed before anything is
   removed. A handle that stays open keeps the old file and its removed
   sidecars: its reads go stale, and its writes never reach the new file.
5. **Remove the source's `-wal` and `-wal-shm`**, checking the errors (a file
   already absent is fine), then sync the directory. If any of this fails,
   stop: the source is still the database.
6. **Rename the copy over the source**, on the same filesystem, then sync the
   directory.
7. **Open the new file in every process** before any of them resumes
   writing.

A crash at any step leaves either the old file, complete and with the log it
had, or the copy, never the copy beside the old file's log.

Syncing the directory follows SQLite's file-system layers: on Unix (Linux,
macOS, Android, iOS) the directory is fsynced, as SQLite's Unix layer does
after it deletes a file; on Windows there is no directory sync, as in
SQLite's Windows layer, and the name changes rely on NTFS's metadata
journal. `CopyToLatestFormat` syncs the destination's directory by the same
rule when it publishes.

The example implements the protocol for one process. It assumes `db` is the
only handle on `path` and that nothing writes through it until it returns
(step 1 is the caller's). On an error, the handle it returns tells the
caller where things stand: until the source is closed it is `db`, still
open, and the database goes on as before; once the source is closed it is
nil, and the caller opens `path` again, which holds the old database before
the rename and the copy after it.

```go
import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"

	anystore "github.com/anyproto/any-store/v2"
)

// replaceWithCopy puts a copy of the database at path, in the latest
// format, in the file's place, and returns the database opened from it.
// config is the Config the database is opened with. On an error before the
// source is closed it returns db, still open; after that it returns nil,
// and path is to be opened again.
func replaceWithCopy(ctx context.Context, db anystore.DB, path string, config *anystore.Config) (anystore.DB, error) {
	tmp := path + ".format6"
	// 1. The caller has stopped every writer, and this is the only run.
	// So a file at tmp is a copy an earlier run left when it failed or was
	// killed. It can be older than the database: never put it in place.
	if err := os.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return db, err
	}
	// 2. Copy. With ErrCopyPublished the copy is complete at tmp and only a
	// step after it failed, the directory sync at worst: go on, step 5 syncs
	// the same directory before anything relies on it, and checks the error.
	if _, err := anystore.CopyToLatestFormat(ctx, db, tmp, config); err != nil && !errors.Is(err, anystore.ErrCopyPublished) {
		return db, err
	}
	// 3. Checkpoint the source and check the error: Close drops it.
	if err := db.Flush(ctx, 0, anystore.FlushModeCheckpointTruncate); err != nil {
		_ = os.Remove(tmp)
		return db, err
	}
	// 4. Close the source. From here on, path has to be opened again.
	if err := db.Close(); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	// 5. Remove the source's log and its index, then sync the directory.
	for _, sidecar := range []string{path + "-wal", path + "-wal-shm"} {
		if err := os.Remove(sidecar); err != nil && !errors.Is(err, fs.ErrNotExist) {
			_ = os.Remove(tmp)
			return nil, err
		}
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	// 6. Put the copy in place, then sync the directory.
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	// 7. Open the new file before any writer resumes.
	return anystore.Open(ctx, path, config)
}

// syncDir makes the names in dir durable: the directory is fsynced on Unix,
// and not on Windows, where NTFS journals the names.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
```

A run of the example that fails or is killed can leave its copy at
`path + ".format6"`. That copy is stale once writes have resumed, and is
never to be put in place, so the example removes a file at that name before
it copies. This is safe only under the protocol: writes are stopped and one
run is in progress, so the name is the example's own. If something else of
yours may use the name, pick another. The staging files of a copy that was
killed need no such step: `CopyToLatestFormat` removes them itself, under
its lock, the next time it copies to that name. A file beside the name that
is not a copy's (`path + ".format6-wal"`, say) still refuses the copy with
`ErrDestinationExists`, and the example returns `db`, open.

With several processes, steps 1, 4 and 7 have to be coordinated across them
by the application.

Copying a live database without replacing it needs none of this: the copy
is a consistent snapshot, as a backup is.

## What can differ

The copy is made through the same paths as new data: documents are encoded
by today's encoder and indexes are built under today's rules. What older
releases stored can therefore come out different. Each case below either
fails the copy with an error that names it, or is described here; none is
silent.

| Case | What happens |
| ---- | ------------ |
| A stored `-0.0` | Copied as `+0.0`. The current encoder normalizes negative zero, and the two compare equal. |
| A document whose primary key is an array, written by an older release (`FindId` still reads it) | The copy fails: `Insert` rejects array keys (`ErrArrayPrimaryKey`). The error names the collection and the document. Remove or re-key such documents first. |
| A trained IVF index whose collection holds no vectors | The copy fails: an IVF index cannot be built without vectors to train on. The error names the index. Drop the index first, and create it again once there is data. |
| A trained IVF index whose collection now holds fewer vectors than its `NList` (some were deleted since it was built) | Copied. The copy keeps the source's definition, `NList` included, and trains at most one cell per vector there is, as a rebuild of the source would. Creating that definition anew is refused with `ErrInvalidConfig`. |
| A vector index definition outside the bounds a new index is held to, which an earlier release created (an `EfSearch` above 65,536, a NaN `CompactRatio`) | Copied with the source's definition: the copy is not held to those bounds, as `EnsureIndex` of the same definition is not. |
| A unique index that an older release built and today's rules reject, which `Open` keeps as outdated (see the README on index formats) | The copy fails with `ErrUniqueConstraint`, naming the index. Remove the duplicates or drop the index first. |
| An index in a vector mode this release removed (IVF-PQ), which can only be dropped | The copy fails with `ErrVectorIndexUnsupported`, naming the index. Drop it first. |
| Vector indexes | Trained again from the copied vectors. HNSW graphs and IVF centroids differ from the source's, so approximate results can differ, while recall stays in its usual range. |
| Range indexes | Built again. Query results are the same. |
| Full-text indexes | Built again under today's tokenization and position rules. For an index built under an older layout, which documents match can change: a `names` array whose first element is empty gave the next element's first token position 100, so `^mario` did not match it; in the copy it does. For an index already on today's layout a document's match and its score are the same. Ties between equal scores are broken by an internal insertion id, and the copy assigns those in primary-key order, so the order of tied results, paging through them, and which document an update or delete bounded by `Limit` takes can change. |
| Statistics | Built again from the copied documents, in the compressed encoding. |
| Page size, free space, header counters | New. The copy uses the defaults; `Config` has no page-size field. |

## Cost

- **Time.** Measured on one production file: about 35 s for a 3.7 GB file
  holding 2.13 million documents, on a Ryzen 9950X. The time grows with the
  documents to insert and the indexes to build; vector indexes take the
  longest to build.
- **The long read transaction.** The source stays in one read transaction for
  the whole copy:
  - ordinary writers of the source keep working;
  - a PASSIVE checkpoint makes progress only up to the copy's snapshot;
  - a FULL, RESTART or TRUNCATE checkpoint waits for the copy, and can hold
    other writers up while it waits; when its busy timeout runs out it
    returns `ErrBusy` while the copy goes on;
  - the copy holds one of the source process's reader slots: with
    `ReadConcurrency: 1`, other reads in that process wait for the copy;
  - the source's log (`-wal`) and its index (`-wal-shm`) can grow for the
    whole copy.
- **Memory.** The read transaction uses one reusable page cache, not a copy
  of the database. Building a vector index holds that index's vectors in
  memory, as `CreateIndex` does.
- **Disk.** The copy is a second database beside the first until the
  source is removed.

## Going back

Format 6 is one way. No release before v2.5.0 reads a format 6 file: v2.4.4
refuses it with `ErrIncompatibleVersion` and leaves it unchanged, and older
releases refuse it as a corrupt file (`ErrCorrupt`). Once a database is
replaced by its format 6 copy, going back past v2.5.0 needs the format 5 file
kept from before, with whatever was written since lost. `Backup` writes a
copy in the source's format, so a backup of the format 5 database taken
before the copy stays readable by older releases.
