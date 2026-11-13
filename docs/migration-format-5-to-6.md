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
  `-wal-shm` or `.lock` file, exists, or the destination is one of those
  names of an existing file (`app.db-wal` beside `app.db`, in any case:
  `app.db-WAL` is `app.db`'s log on a case-insensitive filesystem, and is
  refused on every filesystem), the call returns
  `ErrDestinationExists` and leaves it alone. The copy is built under a
  private name beside the destination (the destination's name, `.tmp-` and
  16 hex digits), checkpointed, verified and closed, and only then given the
  destination's name by a hard link. Right before the link the destination's
  `-wal`, `-wal-shm` and `.lock` are checked again, and the link itself fails
  if the destination exists, so a file that appeared there while the copy
  ran also gives `ErrDestinationExists`. Of two calls copying to one path,
  one publishes and the other gets `ErrDestinationExists`. Only for the
  destination itself is this atomic: a `-wal`, `-wal-shm` or `.lock` created
  between the last check and the link is not seen, so do not let anything
  else create files under the destination's name while the copy runs.
- **Verified.** Before it is published, each collection of the copy is
  checked against the source's snapshot: its options, its document count
  and its index definitions. Then `IntegrityCheck` runs on the copy. A
  mismatch fails the call.
- **Returned closed.** The copy has no `-wal` or `-wal-shm` file beside it.
  `report` gives the number of collections, documents and indexes copied,
  and how long the call took.
- **The source's files are never renamed or removed.**

The call fails with:

| Error | When |
| ----- | ---- |
| `ErrUnsupportedDB` | `db` was not opened by this package (a type of your own that implements `DB`). Nothing is created. |
| `ErrTxInContext` | `ctx` carries a read or write transaction. The copy reads in a transaction of its own, as SQLite refuses `VACUUM` inside one. |
| `ErrDestinationExists` | The destination or one of its sidecars exists, or appeared while the copy ran. |
| `ErrPublishUnsupported` | The finished copy could not be linked to the destination path, as on a filesystem without hard links (FAT, exFAT). It wraps the link error. |
| any other | The copy failed: a collection, index or document could not be copied (the error names it, see [What can differ](#what-can-differ)), the copy did not match the source, or an I/O operation failed. Cancelling `ctx` stops the copy with the context's error. |

The call creates the destination only by publishing a complete, verified,
closed copy. On a failure before that it closes the copy and removes its
files, which all carry the staging name: `<destination>.tmp-<16 hex digits>`
and its `-wal`, `-wal-shm` and `.lock`. A removal that fails is joined to
the returned error, naming the file left. Those staging files are the only
files a failed call owns, and the only ones a crash before the publication
leaves: remove them once no copy is running.

After `ErrDestinationExists` the files at and beside the destination are not
the call's: they were there before it, or another caller published them.
The error is no reason to remove them. A failure after the link, of the
directory sync or of the removal of the staging name, comes back with the
copy published at the destination, and the error says so. A crash at that
point leaves the complete copy at the destination, maybe with its staging
name beside it as a second name of the same file.

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
   succeed. It returns with the copy closed.
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

On a filesystem without hard links the copy cannot be published
(`ErrPublishUnsupported`), so a database on such a filesystem cannot be
replaced this way.

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
	// 2. Copy. A file already at tmp is refused with ErrDestinationExists:
	// it is not this run's, and a copy from an earlier run is stale.
	if _, err := anystore.CopyToLatestFormat(ctx, db, tmp, config); err != nil {
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

If a file is at `path + ".format6"` when the example starts, the copy is
refused with `ErrDestinationExists` and nothing changes. The file may be
another run's, or a copy an earlier run left; such a copy is stale once
writes have resumed, and is never to be put in place. Remove it only when
you know it is yours and no other run is in progress.

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
