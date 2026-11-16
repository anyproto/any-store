package anystore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// LatestSchemaFormat is the on-disk format a database created by this
// release gets. It says what this build creates, not that an older file must
// change: a file keeps the format it was created with, and this release
// reads and writes every format it supports. CopyToLatestFormat copies a
// database into this format.
const LatestSchemaFormat = 6

// SchemaFormat returns the on-disk format of d's file: 5 for a file created
// before v2.5.0, 6 for one created since. A file a release before v2.5.0
// created whose header records no format reads as 5. The format of a file
// does not change while it is open or across reopens; a result below
// LatestSchemaFormat is a file CopyToLatestFormat can bring to it.
//
// A DB this package did not open returns ErrUnsupportedDB.
func SchemaFormat(d DB) (int, error) {
	dbi, ok := d.(*db)
	if !ok || dbi == nil {
		return 0, ErrUnsupportedDB
	}
	format := dbi.btreeDB.SchemaFormat()
	if format == 0 {
		// A header that never recorded a format: the btree reads it as 5.
		format = 5
	}
	return int(format), nil
}

// CopyReport is what CopyToLatestFormat copied.
type CopyReport struct {
	// Collections is the number of collections in the copy.
	Collections int
	// Documents is the number of documents in the copy, over all collections.
	Documents int
	// Indexes is the number of indexes the copy built, of every kind.
	Indexes int
	// Duration is how long the call took.
	Duration time.Duration
}

// CopyToLatestFormat writes a new database at dstPath in LatestSchemaFormat,
// holding every collection, document and index of src as of one snapshot.
// It is how a database created before v2.5.0 moves onto the current format;
// docs/migration-format-5-to-6.md describes when to do it and how to put the
// copy in the source's place.
//
// The copy is logical: each collection is created with its stored primary
// key and compression setting, its documents are inserted again, and its
// indexes, vector and full-text ones included, are built again from their
// complete definitions. A document is stored as today's encoder writes it,
// and every index is built under today's rules, so what an older release
// stored can come out different, as the guide lists. Anything that cannot be
// copied fails the call with an error naming the collection and the index or
// document: an index that does not build again (a unique index the current
// rules find duplicate, an IVF index whose collection holds no vectors to
// train on), a document Insert refuses (one whose primary key is an array).
//
// The source is read in one read transaction, from the catalog to the last
// document, as VACUUM INTO reads its source: commits made to src while the
// copy runs, by this process or another, are not in the copy, and src stays
// open and writable meanwhile. The call holds one of src's reader slots
// until it returns, and no checkpoint of src passes its snapshot: a FULL,
// RESTART or TRUNCATE checkpoint waits for the copy, and returns ErrBusy
// when its busy timeout runs out, while src's log can grow. A ctx that
// carries a transaction is refused with ErrTxInContext; cancelling ctx
// fails the copy. A src this package did not open returns ErrUnsupportedDB
// before anything is created.
//
// config configures the destination: encryption, durability and the rest.
// nil is the defaults, so the copy is not encrypted unless config says so,
// whatever src is. config.InMemory is refused. A collection that inherits
// the database's compression setting inherits config's in the copy.
//
// Nothing is replaced. If dstPath, dstPath+"-wal", dstPath+"-wal-shm" or
// dstPath+".lock" exists, or dstPath is one of those names of an existing
// file in any case (p+"-WAL" is p's log on a case-insensitive filesystem,
// and is refused on every one), the call returns ErrDestinationExists and
// leaves that file as it is. Found by the first check, before the call
// takes its locks, it returns having touched nothing. The check is made
// again under the locks and right before the publication (below), and a
// file found then, one that appeared meanwhile, returns
// ErrDestinationExists after the call created its lock files, or built the
// whole copy under its staging name, and removed them again. Apart from the
// read bookkeeping in src's wal-index (its -wal-shm), which the call's read
// of src changes as any reader of src does, no file but the copy's own lock
// and staging files is created, written or removed, and those removed can
// include ones a killed call left there; src's files are never renamed,
// removed or otherwise written. A dstPath that ends in one of the names
// this call keeps for itself, listed below, is refused too.
//
// One call at a time copies to a dstPath. For the whole call it holds an
// exclusive lock of the operating system on the file dstPath+".copy-lock"
// (flock on Unix, LockFileEx on Windows), which the system drops if the
// process dies. A second call for the same dstPath, from this process or
// another, does not wait: it returns ErrBusy at once, and touches no file
// but the copy's lock files. A dstPath that names a file a database at p
// keeps beside it (p+"-wal", p+"-wal-shm" or p+".lock", in any case) is
// read as p's file once p exists, so a call for it holds p+".copy-lock"
// too, taken first: a copy to p and a copy to one of those names do not
// run at once either, and the one that comes second returns ErrBusy. The
// lock files are removed when the call returns, except after a failure to
// take a lock, which can leave an unlocked lock file for the next call to
// reuse. So a call that returns ErrBusy has created, written or removed no
// file but lock files: a lock it took before it failed on the next one, or
// before it found a database open at the staging name (below), it released,
// removing its file, which may be one that a killed call left there
// unlocked. The other platforms (js/wasm, wasip1) have no such lock: there
// the lock is the process's, one for every dstPath, so one call at a time
// copies in the process, and no lock file is made.
//
// Under the lock the copy is built in the staging file
// dstPath+".copy-staging", in dstPath's own directory, so that publishing
// it never crosses filesystems. Staging files a killed call left there
// (that name, and its -wal, -wal-shm and .lock) are removed first: no other
// copy can be using them while the lock is held. A database open at the
// staging name is no killed call's: an open database holds a lock of the
// operating system on its file (a flock on Unix, LockFileEx on Windows),
// and the call tries that lock, exclusively and without waiting, before it
// removes anything; if it is held the call returns ErrBusy and removes
// nothing under the staging name. To try it the call opens the staging
// file, only if it is what a copy leaves there, as it opens a lock file
// (below). js/wasm and wasip1 have no such lock to try, and open nothing.
// The copy is checkpointed, verified (each collection's options, document
// count and index definitions against the source's snapshot, then
// IntegrityCheck) and closed, and then published by renaming the staging
// file to dstPath.
// The files at and beside dstPath are checked again right before the
// rename, so a file that appeared there while the copy ran also gives
// ErrDestinationExists. The check and the rename are not one step, and the
// locks exclude only other copies: a file that something else creates at
// dstPath between them is replaced, and a -wal, -wal-shm or .lock that it
// creates beside dstPath then is not seen, and is read as the copy's own
// once the copy is opened. So let nothing else create files under dstPath's
// name while the copy runs. The copy is never returned open; it has no
// -wal or -wal-shm beside it.
//
// The names dstPath+".copy-lock" and dstPath+".copy-staging", with the
// -wal, -wal-shm and .lock of the latter, are reserved for this call, and
// so, for a dstPath that is p's -wal, -wal-shm or .lock, is p+".copy-lock":
// it creates them and removes them, whatever a file found there holds, but
// a database open at the staging name and, at the staging name and at a
// lock file's name, anything a copy does not leave there. The call opens a
// lock file, and the staging file to try its lock, only if it is a regular
// file with one link, which is what a copy leaves there. A symbolic link, a
// file with other links (on Unix; Windows does not report the count) or
// anything but a regular file at one of those names can be another name of
// a file a database of this process has open, and closing a descriptor of
// that file would drop every record lock the process holds on it: such a
// file fails the call with an error, and is neither opened, written nor
// removed. Keep nothing of your own under them.
// A src whose database file, or a file it keeps beside it, is under one of
// them is refused, on every platform, before anything is created or
// removed. The files are compared by identity: each reserved name's file
// with the files src has open, as their descriptors identify them, and
// with its .lock as the spelling src was opened by names it. So every
// spelling of dstPath (relative, through a symbolic link, in another case
// on a case-insensitive filesystem) is refused, and so is a src whose path
// names another file than the one it has open (a ".." after a symbolic
// link, which the system resolves before going up). A closed src returns
// ErrDBIsClosed.
//
// dstPath is created only by the publication, as a complete, verified,
// closed copy. On a failure before it the call closes the copy and removes
// its staging files. A removal that fails is joined to the error, with the
// file's name. The staging files are the only ones a failed call owns, and
// the only ones a crash before the publication leaves, beside an unlocked
// lock file; the next call for dstPath removes them. After
// ErrDestinationExists the files at and beside dstPath are not this call's:
// they were there before, or someone else put them there, and the error
// is no reason to remove them. A failure after the rename (the directory
// sync, the removal of a staging leftover) leaves the copy published at
// dstPath: the call returns the filled report and an error that wraps
// ErrCopyPublished with the failure. Any file at dstPath that
// the call published, and that survives a crash, is the complete copy; but
// the name is durable only once the directory sync after the rename has
// succeeded (on Unix; on Windows NTFS's metadata journal keeps it), and
// power lost before that can leave the copy under its staging name, which
// the next call removes. On every error but ErrCopyPublished the report is
// zero.
//
// The call never renames or removes a file of src.
func CopyToLatestFormat(ctx context.Context, src DB, dstPath string, config *Config) (report CopyReport, err error) {
	start := time.Now()
	s, ok := src.(*db)
	if !ok || s == nil {
		return CopyReport{}, ErrUnsupportedDB
	}
	// ~ vacuum.c:169-172 (sqlite3RunVacuum): "cannot VACUUM from within a
	// transaction".
	if ctx.Value(ctxKeyTx) != nil {
		return CopyReport{}, ErrTxInContext
	}
	var cfg Config
	if config != nil {
		cfg = *config // Open fills in the defaults of the Config it gets
	}
	if cfg.InMemory {
		return CopyReport{}, errors.New("any-store: copy to latest format: Config.InMemory is not supported, the copy is a file")
	}
	if err = ctx.Err(); err != nil {
		return CopyReport{}, err
	}
	if err = notReservedName(dstPath); err != nil {
		return CopyReport{}, err
	}
	if err = sourceOutsideReservedNames(s.btreeDB, dstPath); err != nil {
		return CopyReport{}, err
	}
	// ~ vacuum.c:236-242: VACUUM INTO refuses an output file that has content.
	// Checked before the lock too, so that a destination that exists gets no
	// lock file put beside it.
	if err = destinationFree(dstPath); err != nil {
		return CopyReport{}, err
	}
	unlock, err := lockDestination(dstPath)
	if err != nil {
		return CopyReport{}, err
	}
	// Runs last, after the staging files are gone.
	defer unlock()
	// From here to the return no other call copies to dstPath, to a name a
	// database at dstPath keeps beside it, or to the name whose database
	// keeps dstPath beside it.
	if err = destinationFree(dstPath); err != nil {
		return CopyReport{}, err
	}
	stage, err := reserveStaging(dstPath)
	if err != nil {
		return CopyReport{}, err
	}
	var (
		dst       *db
		published bool
	)
	defer func() {
		if err != nil && !published {
			report = CopyReport{}
			if stage != "" {
				err = discardStaging(err, stage, dst)
			}
		}
	}()

	if dst, err = open(ctx, stage, &cfg, LatestSchemaFormat); err != nil {
		return
	}
	colls, err := copyCollections(ctx, s, dst, &report)
	if err != nil {
		return
	}
	if err = verifyCopy(ctx, dst, colls); err != nil {
		return
	}
	// The copy is complete in the database file: Close finds nothing to
	// checkpoint and leaves an empty log, which is checked before it goes.
	closeErr := dst.Close()
	dst = nil
	if closeErr != nil {
		return CopyReport{}, fmt.Errorf("any-store: copy to latest format: closing the copy: %w", closeErr)
	}
	if err = removeStagingSidecars(stage); err != nil {
		return
	}
	if testHookBeforePublish != nil {
		testHookBeforePublish(stage)
	}
	// Nothing after this point consults ctx: a cancellation that came while
	// the copy was verified or closed publishes nothing.
	if err = ctx.Err(); err != nil {
		return
	}
	published, err = publishStaging(stage, dstPath)
	// publishStaging removed what it could; whatever is left it named.
	stage = ""
	if !published {
		return
	}
	// The copy is at dstPath: the report is the caller's whether or not a
	// step after the rename failed (ErrCopyPublished).
	report.Duration = time.Since(start)
	return report, err
}

// copyBatch is how many documents one write transaction of a copy inserts.
const copyBatch = 1000

// Test hooks of CopyToLatestFormat. testHookCopiedBatch runs each time a
// full batch of a collection's documents has committed, with the number of
// its documents copied so far; testHookCopiedCollection runs once a
// collection's documents are in the copy, with the source's read
// transaction still open; testHookBeforePublish runs with the copy closed
// under its staging name, before the rename.
var (
	testHookCopiedBatch      func(docs int)
	testHookCopiedCollection func(name string)
	testHookBeforePublish    func(stage string)
)

// copiedCollection is what the source's snapshot holds of a collection,
// which the verification holds the copy to.
type copiedCollection struct {
	name    string
	cfg     collConfig
	indexes []IndexInfo
	docs    int
}

// copyCollections copies every collection of s, as of one read transaction
// of s, into dst, and returns what that snapshot holds of each. Everything
// of the source is read through that transaction: the catalog (getIndexInfos,
// not GetIndexes, which gives a handle's committed indexes, not the
// snapshot's, and leaves out vector ones) and the documents, which a cursor
// reads from each collection's data tree in primary-key order, as a scan of
// the collection does. No handle of s is opened: the handles are shared with
// s's other callers, and a handle the copy closed again could be one another
// caller had just been given. The documents go into dst through the
// collection API (Insert), and the indexes are built there (CreateIndex).
func copyCollections(ctx context.Context, s, dst *db, report *CopyReport) ([]copiedCollection, error) {
	rtx, err := s.ReadTx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rtx.Commit() }()
	rctx := rtx.Context()

	var colls []copiedCollection
	err = s.doReadTx(rctx, func(tx *btree.ReadTx) error {
		names, err := s.collectionNames(tx)
		if err != nil {
			return err
		}
		colls = make([]copiedCollection, len(names))
		for i, name := range names {
			c := &colls[i]
			c.name = name
			if c.cfg, err = s.loadCollConfig(tx, name); err != nil {
				return fmt.Errorf("any-store: copy to latest format: collection %q: %w", name, err)
			}
			if c.indexes, err = s.getIndexInfos(tx, name); err != nil {
				return fmt.Errorf("any-store: copy to latest format: collection %q: %w", name, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for i := range colls {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if err = copyCollection(ctx, rctx, s, dst, &colls[i], report); err != nil {
			return nil, err
		}
	}
	report.Collections = len(colls)
	return colls, nil
}

// copyCollection creates the collection c in dst, inserts its documents as
// rctx's snapshot of s holds them, counts them in that snapshot, and builds
// its indexes after the data.
func copyCollection(ctx, rctx context.Context, s, dst *db, c *copiedCollection, report *CopyReport) error {
	fail := func(err error) error {
		return fmt.Errorf("any-store: copy to latest format: collection %q: %w", c.name, err)
	}
	dc, err := dst.CreateCollection(ctx, c.name, CollectionOptions{Compression: c.cfg.Compression, PrimaryKey: c.cfg.PrimaryKey})
	if err != nil {
		return fail(err)
	}
	// The copy's own handle: nobody else has the copy open.
	defer func() { _ = dc.Close() }()
	err = s.doReadTx(rctx, func(tx *btree.ReadTx) error {
		ns, err := tx.GetNamespace(c.name)
		if err != nil {
			return fail(err)
		}
		if err = copyDocuments(ctx, tx, ns, dst, dc, c); err != nil {
			return err
		}
		if c.docs, err = tx.CountContext(ctx, ns); err != nil {
			return fail(err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if testHookCopiedCollection != nil {
		testHookCopiedCollection(c.name)
	}
	for _, info := range c.indexes {
		if err = dc.CreateIndex(ctx, info); err != nil {
			return fmt.Errorf("any-store: copy to latest format: collection %q: index %q: %w", c.name, info.Name, err)
		}
		report.Indexes++
	}
	report.Documents += c.docs
	return nil
}

// copyDocuments inserts the documents of the data tree ns, as tx holds them,
// into dc, copyBatch to a write transaction of dst.
func copyDocuments(ctx context.Context, tx *btree.ReadTx, ns *btree.Namespace, dst *db, dc Collection, c *copiedCollection) (err error) {
	cursor := tx.NewCursor(ns)
	defer cursor.Close()
	var (
		p        anyenc.Parser
		key, val []byte
		doc      *anyenc.Value
		wtx      WriteTx
		n        int
	)
	defer func() {
		if wtx != nil {
			_ = wtx.Rollback()
		}
	}()
	fail := func(key []byte, doc *anyenc.Value, err error) error {
		return fmt.Errorf("any-store: copy to latest format: collection %q: document %s: %w", c.name, documentName(key, doc, primaryKeyOf(c.cfg)), err)
	}
	for err = cursor.First(); err == nil && cursor.Valid(); err = cursor.Next() {
		if key, err = cursor.Key(); err != nil {
			return fmt.Errorf("any-store: copy to latest format: collection %q: %w", c.name, err)
		}
		if val, err = cursor.Value(); err != nil {
			return fail(key, nil, err)
		}
		if doc, err = p.Parse(val); err != nil {
			return fail(key, nil, err)
		}
		if wtx == nil {
			if wtx, err = dst.WriteTx(ctx); err != nil {
				return fmt.Errorf("any-store: copy to latest format: %w", err)
			}
		}
		if err = dc.Insert(wtx.Context(), doc); err != nil {
			return fail(key, doc, err)
		}
		if n++; n%copyBatch == 0 {
			err, wtx = wtx.Commit(), nil
			if err != nil {
				return fmt.Errorf("any-store: copy to latest format: %w", err)
			}
			if testHookCopiedBatch != nil {
				testHookCopiedBatch(n)
			}
		}
	}
	if err != nil {
		return fmt.Errorf("any-store: copy to latest format: collection %q: %w", c.name, err)
	}
	if wtx != nil {
		err, wtx = wtx.Commit(), nil
		if err != nil {
			return fmt.Errorf("any-store: copy to latest format: %w", err)
		}
	}
	return nil
}

// documentName names a document in an error: its primary-key value, or,
// for one that has none or does not parse, the key it is stored under.
func documentName(key []byte, doc *anyenc.Value, pk string) string {
	if doc != nil {
		if id := doc.Get(pk); id != nil {
			return id.String()
		}
	}
	var p anyenc.Parser
	if id, err := p.Parse(key); err == nil {
		return "stored under " + id.String()
	}
	return "stored under key " + hex.EncodeToString(key)
}

// verifyCopy holds the copy to the source's snapshot, before the copy is
// closed: its log checkpointed into the database file and truncated, then
// the collections, each with the source's options, document count and index
// definitions, then the btree's IntegrityCheck.
func verifyCopy(ctx context.Context, dst *db, want []copiedCollection) error {
	if err := dst.Flush(ctx, 0, FlushModeCheckpointTruncate); err != nil {
		return fmt.Errorf("any-store: copy to latest format: checkpoint of the copy: %w", err)
	}
	mismatch := func(format string, args ...any) error {
		return fmt.Errorf("the copy differs from the source: "+format, args...)
	}
	err := dst.doReadTx(ctx, func(tx *btree.ReadTx) error {
		names, err := dst.collectionNames(tx)
		if err != nil {
			return err
		}
		if len(names) != len(want) {
			return mismatch("%d collections, the source has %d", len(names), len(want))
		}
		for i, w := range want {
			if names[i] != w.name {
				return mismatch("collection %q where the source has %q", names[i], w.name)
			}
			cfg, err := dst.loadCollConfig(tx, w.name)
			if err != nil {
				return err
			}
			if cfg.Compression != w.cfg.Compression || primaryKeyOf(cfg) != primaryKeyOf(w.cfg) {
				return mismatch("collection %q: options %+v, the source has %+v", w.name, cfg, w.cfg)
			}
			ns, err := tx.GetNamespace(w.name)
			if err != nil {
				return fmt.Errorf("collection %q: %w", w.name, err)
			}
			n, err := tx.CountContext(ctx, ns)
			if err != nil {
				return fmt.Errorf("collection %q: %w", w.name, err)
			}
			if n != w.docs {
				return mismatch("collection %q: %d documents, the source has %d", w.name, n, w.docs)
			}
			indexes, err := dst.getIndexInfos(tx, w.name)
			if err != nil {
				return fmt.Errorf("collection %q: %w", w.name, err)
			}
			if len(indexes) != len(w.indexes) {
				return mismatch("collection %q: %d indexes, the source has %d", w.name, len(indexes), len(w.indexes))
			}
			for j := range indexes {
				if !sameIndexDefinition(indexes[j], w.indexes[j]) {
					return mismatch("collection %q: index %q is not defined as the source's %q", w.name, indexes[j].Name, w.indexes[j].Name)
				}
			}
		}
		return nil
	})
	if err == nil {
		err = dst.IntegrityCheck(ctx)
	}
	if err != nil {
		return fmt.Errorf("any-store: copy to latest format: verifying the copy: %w", err)
	}
	return nil
}

// primaryKeyOf is the primary key a stored collection config gives: "id"
// when it names none.
func primaryKeyOf(cfg collConfig) string {
	if cfg.PrimaryKey == "" {
		return "id"
	}
	return cfg.PrimaryKey
}

// sameIndexDefinition reports whether two index definitions are the same in
// full: name, kind, fields, flags and the full-text or vector parameters.
func sameIndexDefinition(a, b IndexInfo) bool {
	if !indexInfoEqual(a, b) || a.Kind != b.Kind {
		return false
	}
	if (a.Vector == nil) != (b.Vector == nil) || a.Vector != nil && *a.Vector != *b.Vector {
		return false
	}
	if (a.Fulltext == nil) != (b.Fulltext == nil) {
		return false
	}
	return a.Fulltext == nil || a.Fulltext.B == b.Fulltext.B && a.Fulltext.K1 == b.Fulltext.K1 &&
		maps.Equal(a.Fulltext.Weights, b.Fulltext.Weights)
}

// destinationFree returns ErrDestinationExists if path or a file a database
// keeps beside it exists, or if path is the name of such a file of an
// existing one, in any case: a copy published as p+"-wal", or p+"-WAL" on a
// case-insensitive filesystem, would be read as p's log.
func destinationFree(path string) error {
	for _, p := range databaseFiles(path) {
		if _, err := os.Lstat(p); err == nil {
			return fmt.Errorf("%w: %s", ErrDestinationExists, p)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("any-store: copy to latest format: %w", err)
		}
	}
	for _, base := range sidecarBases(path) {
		if _, err := os.Lstat(base); err == nil {
			return fmt.Errorf("%w: %s is a name the database %s keeps beside it", ErrDestinationExists, path, base)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("any-store: copy to latest format: %w", err)
		}
	}
	return nil
}

// cutSuffixFold is strings.CutSuffix with the suffix matched under Unicode
// case folding (strings.EqualFold). The end of s is taken by runes, as many
// as suffix has, not by bytes: a case equivalent may be longer than the
// letter it folds to, as U+017F LATIN SMALL LETTER LONG S is to s and
// U+212A KELVIN SIGN to k, and a case-insensitive filesystem takes it for
// that letter.
func cutSuffixFold(s, suffix string) (before string, found bool) {
	end := len(s)
	for range utf8.RuneCountInString(suffix) {
		if end == 0 {
			return s, false
		}
		_, size := utf8.DecodeLastRuneInString(s[:end])
		end -= size
	}
	if !strings.EqualFold(s[end:], suffix) {
		return s, false
	}
	return s[:end], true
}

// sidecarSuffixes are the suffixes of the files a database at path keeps
// beside it: its log, the log's index, and the durability sentinel.
var sidecarSuffixes = [...]string{"-wal", "-wal-shm", ".lock"}

// sidecarBases lists the names whose database would keep a file at path:
// path without a sidecar suffix it ends in, matched without regard to case,
// as on a case-insensitive filesystem p-WAL is p's log (the match holds on
// every filesystem; the base keeps path's own spelling). A path is the
// sidecar of at most one name, as no suffix ends another, and of that name
// only: a database reads the files beside its own name and no others, so
// p+"-wal-wal" is p+"-wal"'s log and none of p's. A path that is a suffix
// alone has no base.
func sidecarBases(path string) []string {
	var bases []string
	for _, suffix := range sidecarSuffixes {
		if base, ok := cutSuffixFold(path, suffix); ok && base != "" {
			bases = append(bases, base)
		}
	}
	return bases
}

// databaseFiles lists the files of the database at path: the file and the
// ones it keeps beside it.
func databaseFiles(path string) []string {
	files := []string{path}
	for _, suffix := range sidecarSuffixes {
		files = append(files, path+suffix)
	}
	return files
}

// The names CopyToLatestFormat keeps for itself beside dstPath. Neither is
// a name a database keeps beside dstPath or beside the staging file
// (sidecarSuffixes): the lock file in particular is not a ".lock", which is
// a database's durability sentinel. A destination that ends in one of them,
// or in a sidecar of the staging name, is refused (notReservedName), so the
// call never publishes a database under a name that another call would
// lock, or would remove as a killed call's staging file.
const (
	// copyLockSuffix names the file whose lock a copy holds for the whole
	// call (acquireCopyLock).
	copyLockSuffix = ".copy-lock"
	// copyStagingSuffix names the file the copy is built in. One fixed name
	// rather than a random one: under the lock one call at a time uses it,
	// and what a killed call left is found by its name alone, with no
	// search of the directory for names that look like this call's.
	copyStagingSuffix = ".copy-staging"
)

// notReservedName refuses a destination that ends, in any case, in one of
// the names a copy keeps for itself beside its destination.
func notReservedName(dstPath string) error {
	reserved := append([]string{copyLockSuffix}, databaseFiles(copyStagingSuffix)...)
	for _, suffix := range reserved {
		if _, ok := cutSuffixFold(dstPath, suffix); ok {
			return fmt.Errorf("any-store: copy to latest format: the destination %s ends in %q, a name the copy keeps for its own files", dstPath, suffix)
		}
	}
	return nil
}

// reservedNames lists the files a copy to dstPath keeps for itself beside
// it: its lock files (copyLockNames, here on every platform), and its
// staging file with the files a database keeps beside that.
func reservedNames(dstPath string) []string {
	var names []string
	for _, base := range sidecarBases(dstPath) {
		names = append(names, base+copyLockSuffix)
	}
	names = append(names, dstPath+copyLockSuffix)
	return append(names, databaseFiles(dstPath+copyStagingSuffix)...)
}

// copyLockNames lists the names whose copy locks a copy to dstPath holds,
// in the order it takes them: the database dstPath would be a file of
// (sidecarBases), then dstPath. A copy to p and a copy to p+"-wal" have
// locks of their own in different files, p+".copy-lock" and
// p+"-wal.copy-lock": with those alone both could pass the check before
// the rename (destinationFree) before either renamed, and both publish. So
// the copy to p+"-wal" holds p's lock too, and the two run one at a time.
// One level is enough: two destinations meet only when one is the other's
// sidecar (sidecarBases), and both then hold the lock of the shorter; p
// and p+"-wal-wal" do not meet, and need no common lock. Where one lock
// serves the process (copyLockPerName false) it is taken once, and covers
// every destination.
func copyLockNames(dstPath string) []string {
	var names []string
	if copyLockPerName {
		names = sidecarBases(dstPath)
	}
	return append(names, dstPath)
}

// lockDestination takes the copy locks of a copy to dstPath
// (copyLockNames), each without waiting, and returns the function that
// releases them, in the reverse order. A lock another holder has releases
// the ones taken and returns ErrBusy at once, naming the lock; as nobody
// waits, the order of the locks cannot deadlock. A lock file's name that
// holds something a copy does not leave there (errReservedNameAlias) fails
// the call the same way, with that error.
func lockDestination(dstPath string) (unlock func(), err error) {
	var releases []func()
	unlock = func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
	for _, name := range copyLockNames(dstPath) {
		release, err := lockCopy(name + copyLockSuffix)
		if err != nil {
			unlock()
			switch {
			case errors.Is(err, errReservedNameAlias):
				return nil, fmt.Errorf("any-store: copy to latest format: %w", err)
			case !errors.Is(err, ErrBusy):
				return nil, fmt.Errorf("any-store: copy to latest format: locking the destination: %w", err)
			case name == dstPath:
				return nil, fmt.Errorf("any-store: copy to latest format: another copy holds the copy lock of %s: %w", dstPath, err)
			default:
				return nil, fmt.Errorf("any-store: copy to latest format: another copy holds the copy lock of %s, and %s is a name the database there keeps beside it: %w", name, dstPath, err)
			}
		}
		releases = append(releases, release)
	}
	return unlock, nil
}

// sourceOutsideReservedNames refuses a copy to dstPath from the source src
// if a file of the source is under one of the names the copy keeps for
// itself (reservedNames): the copy would remove it as a killed call's
// staging file, or lock it as its lock file. The files of the source are
// its database file and the ones it keeps beside it (databaseFiles), and
// they are compared by identity (os.SameFile) with what each reserved name
// names (os.Stat, which follows symbolic links), so that every spelling of
// the destination is refused: a relative one, one through a symbolic link
// or a hard link, one in another case on a case-insensitive filesystem.
//
// The source's files are the ones it reads and writes, not the ones its
// path names: the identities of the files it has open, from fstat of their
// descriptors (btree.DB.OpenFiles), its database file and log always, and
// the log's index where that is a file. A path can name another file:
// btree.DB.Path is cleaned lexically, and a ".." after a symbolic link to a
// directory takes it to the link's parent, where the system resolves the
// link first and goes up from its target. The durability sentinel (.lock),
// which no process holds open, and the log's index where it is in memory
// are stat'ed under the spelling the source was opened by, which the system
// resolves as it did at the open.
//
// A file of the source that does not exist yet has no identity, and is not
// compared by name either: a name a database keeps beside its file is
// under a reserved name only if the database file is under the staging
// name itself (the reserved names that end in a sidecar suffix are the
// staging name's sidecars, and the .copy-lock ones end in none), and the
// database file always exists, open, so its identity refuses the copy.
//
// A name that exists but cannot be stat'ed fails the call: the copy cannot
// tell whether it is the source's. An in-memory source has no files. A
// closed source returns ErrDBIsClosed.
func sourceOutsideReservedNames(src *btree.DB, dstPath string) error {
	open, err := src.OpenFiles()
	if err != nil {
		return fmt.Errorf("any-store: copy to latest format: the source: %w", err)
	}
	if open.Path == "" {
		return nil
	}
	stat := func(p string) (os.FileInfo, error) {
		fi, err := os.Stat(p)
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("any-store: copy to latest format: %w", err)
		}
		return fi, nil
	}
	// In the order of databaseFiles: the file, -wal, -wal-shm, .lock.
	srcFiles := databaseFiles(open.Path)
	srcInfos := []os.FileInfo{open.DB, open.WAL, open.SHM, nil}
	for i, p := range srcFiles {
		if srcInfos[i] == nil {
			if srcInfos[i], err = stat(p); err != nil {
				return err
			}
		}
	}
	for _, name := range reservedNames(dstPath) {
		fi, err := stat(name)
		if err != nil {
			return err
		}
		if fi == nil {
			continue
		}
		for i, p := range srcFiles {
			if srcInfos[i] != nil && os.SameFile(fi, srcInfos[i]) {
				return fmt.Errorf("any-store: copy to latest format: %s, a name the copy keeps for its own files, is the source's file %s", name, p)
			}
		}
	}
	return nil
}

// errReservedNameAlias is what a copy refuses to open under the name of a
// lock file or of the staging file (lstatReserved, openReserved): something
// a copy does not make there.
var errReservedNameAlias = errors.New("a copy makes only a regular file with one link under that name, and opens or removes nothing else there")

// reserveStaging creates the empty file the copy for dstPath is built in,
// dstPath+copyStagingSuffix, and returns its name. The caller holds
// dstPath's copy lock, so no other copy uses the staging name, and what is
// under it or beside it was left by a call that died, and is removed first;
// these are names only this API creates. The file is then created
// exclusively: everything the copy creates under that name is this call's.
//
// The lock excludes copies, not a database someone opened under the
// staging name, and that one is no killed call's. An open database holds a
// lock of the operating system on its file for as long as it is open
// (stagingOpenAsDatabase), so the staging file is tried for that lock
// first: one that is held fails the call with ErrBusy, and nothing under
// the staging name is removed. A staging file that a copy does not leave
// (a symbolic link, a file with other links, anything but a regular file)
// is neither tried nor removed: it fails the call with errReservedNameAlias.
// The source is refused earlier, on every platform
// (sourceOutsideReservedNames); this covers another database, where the
// platform has the lock.
func reserveStaging(dstPath string) (string, error) {
	stage := dstPath + copyStagingSuffix
	switch open, err := stagingOpenAsDatabase(stage); {
	case errors.Is(err, errReservedNameAlias):
		return "", fmt.Errorf("any-store: copy to latest format: %w", err)
	case err != nil:
		return "", fmt.Errorf("any-store: copy to latest format: trying the lock of the staging file %s: %w", stage, err)
	case open:
		return "", fmt.Errorf("any-store: copy to latest format: the staging file %s is open as a database: %w", stage, ErrBusy)
	}
	for _, p := range databaseFiles(stage) {
		if err := osRemove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("any-store: copy to latest format: removing a staging file left by an earlier copy: %w", err)
		}
	}
	f, err := osOpenFile(stage, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return "", fmt.Errorf("any-store: copy to latest format: %w", err)
	}
	if err = f.Close(); err != nil {
		err = fmt.Errorf("any-store: copy to latest format: %w", err)
		if rerr := osRemove(stage); rerr != nil && !errors.Is(rerr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("any-store: left on disk: %s: %w", stage, rerr))
		}
		return "", err
	}
	return stage, nil
}

// discardStaging cleans up after a copy that failed before publication: it
// closes the copy, if open, and removes every file the copy has under its
// staging name. A file already gone counts as removed. What it cannot
// remove it names, joined to cause.
func discardStaging(cause error, stage string, dst *db) error {
	var errs []error
	if dst != nil {
		if err := dst.Close(); err != nil && !errors.Is(err, ErrDBIsClosed) {
			errs = append(errs, fmt.Errorf("any-store: closing the unfinished copy: %w", err))
		}
	}
	for _, p := range databaseFiles(stage) {
		if err := osRemove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("any-store: left on disk: %s: %w", p, err))
		}
	}
	if len(errs) == 0 {
		return cause
	}
	return errors.Join(append([]error{cause}, errs...)...)
}

// removeStagingSidecars removes the files the closed copy keeps beside its
// staging name, so that only the database file is published. The log must
// be empty: the copy is checkpointed into its database file before it is
// closed, and a log with frames would hold part of it.
func removeStagingSidecars(stage string) error {
	if fi, err := os.Lstat(stage + "-wal"); err == nil && fi.Size() != 0 {
		return fmt.Errorf("any-store: copy to latest format: the closed copy's log holds %d bytes", fi.Size())
	}
	for _, p := range databaseFiles(stage)[1:] {
		if err := osRemove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("any-store: copy to latest format: %w", err)
		}
	}
	return nil
}

// publishStaging gives the closed copy the name dstPath: a rename of the
// staging file, which is in dstPath's directory, so the rename never
// crosses filesystems and needs nothing a filesystem or a platform may
// lack (hard links are refused to applications on Android, and FAT has
// none). The directory is synced after it (syncDir).
//
// A rename replaces what is at dstPath, so the files at and beside dstPath
// are checked again right before it: one that appeared while the copy ran
// fails the call, and one beside dstPath would be read as the copy's log.
// The caller holds the copy locks of dstPath and of the database dstPath
// would be a file of (lockDestination), so meanwhile no other copy
// publishes at dstPath, beside it, or at the name it is beside. A file
// anything else creates at dstPath between the check and the rename is
// replaced, and one it creates beside dstPath then is not seen.
//
// Before the rename succeeds a failure removes the staging file, and
// published is false. Once it has, published is true and dstPath is never
// removed; a failure from there on still fails the call, as
// ErrCopyPublished wrapped with what failed.
func publishStaging(stage, dstPath string) (published bool, err error) {
	if err := destinationFree(dstPath); err != nil {
		return false, discardStaging(err, stage, nil)
	}
	if err := osRename(stage, dstPath); err != nil {
		return false, discardStaging(fmt.Errorf("any-store: copy to latest format: publishing the copy: %w", err), stage, nil)
	}
	// The copy is published: what fails from here on says so.
	at := fmt.Sprintf("any-store: copy to latest format: the copy is published at %s, but", dstPath)
	var errs []error
	if err := dirSync(dstPath); err != nil {
		errs = append(errs, fmt.Errorf("%s syncing its directory failed: %w", at, err))
	}
	// The closed copy's sidecars went before the rename; nothing should be
	// left under the staging name.
	for _, p := range databaseFiles(stage)[1:] {
		if err := osRemove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("%s a staging file is left on disk: %s: %w", at, p, err))
		}
	}
	if len(errs) != 0 {
		return true, fmt.Errorf("%w: %w", ErrCopyPublished, errors.Join(errs...))
	}
	return true, nil
}
