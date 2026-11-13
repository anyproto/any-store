package anystore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

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
// touches nothing. The copy
// is built under a private name beside dstPath, checkpointed, verified (each
// collection's options, document count and index definitions against the
// source's snapshot, then IntegrityCheck) and closed, and then published at
// dstPath by a hard link. The files beside dstPath are checked again right
// before the link, and the link fails if dstPath exists, so a file that
// appeared there while the copy ran also gives ErrDestinationExists: of two
// calls copying to one dstPath, one publishes. Only for dstPath itself is
// that atomic; a sidecar created between the last check and the link is not
// seen. A link that fails otherwise, as on
// a filesystem without hard links (FAT, exFAT), fails the call with
// ErrPublishUnsupported; there is no fallback to a rename, which would
// replace a file published meanwhile. The copy is never returned open; it
// has no -wal or -wal-shm beside it.
//
// dstPath is created only by the publication, as a complete, verified,
// closed copy. On a failure before it the call closes the copy and removes
// its files, all under the staging name: dstPath+".tmp-" and 16 hex digits,
// with the -wal, -wal-shm and .lock of that name. A removal that fails is
// joined to the error, with the file's name. Those staging files are the
// only ones a failed call owns, and the only ones a crash before the
// publication leaves; remove them once no copy runs. After
// ErrDestinationExists the files at and beside dstPath are not this call's:
// they were there before, or another caller put them there, and the error
// is no reason to remove them. A failure after the link (the directory
// sync, the removal of the staging name) is returned with the copy
// published at dstPath, and the error says so. Any file at dstPath that
// the call published, and that survives a crash, is the complete copy,
// maybe with its staging name beside it as a second name of the same file;
// but the name is durable only once the directory sync after the link has
// succeeded (on Unix; on Windows NTFS's metadata journal keeps it), and
// power lost before that can leave no file at dstPath. On an error the
// report is zero.
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
	// ~ vacuum.c:236-242: VACUUM INTO refuses an output file that has content.
	if err = destinationFree(dstPath); err != nil {
		return CopyReport{}, err
	}
	stage, err := reserveStaging(dstPath)
	if err != nil {
		return CopyReport{}, err
	}
	var dst *db
	defer func() {
		if err != nil {
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
	if err = publishStaging(stage, dstPath); err != nil {
		// publishStaging removed what it could; whatever is left it named.
		stage = ""
		return
	}
	report.Duration = time.Since(start)
	return report, nil
}

// copyBatch is how many documents one write transaction of a copy inserts.
const copyBatch = 1000

// Test hooks of CopyToLatestFormat. testHookCopiedBatch runs each time a
// full batch of a collection's documents has committed, with the number of
// its documents copied so far; testHookCopiedCollection runs once a
// collection's documents are in the copy, with the source's read
// transaction still open; testHookBeforePublish runs with the copy closed
// under its staging name, before the link.
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
	for _, suffix := range sidecarSuffixes {
		// Without regard to case: on a case-insensitive filesystem p-WAL is
		// p's log. The refusal holds on every filesystem; the base keeps its
		// own spelling.
		if len(path) <= len(suffix) || !strings.EqualFold(path[len(path)-len(suffix):], suffix) {
			continue
		}
		base := path[:len(path)-len(suffix)]
		if _, err := os.Lstat(base); err == nil {
			return fmt.Errorf("%w: %s is a name the database %s keeps beside it", ErrDestinationExists, path, base)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("any-store: copy to latest format: %w", err)
		}
	}
	return nil
}

// sidecarSuffixes are the suffixes of the files a database at path keeps
// beside it: its log, the log's index, and the durability sentinel.
var sidecarSuffixes = [...]string{"-wal", "-wal-shm", ".lock"}

// databaseFiles lists the files of the database at path: the file and the
// ones it keeps beside it.
func databaseFiles(path string) []string {
	files := []string{path}
	for _, suffix := range sidecarSuffixes {
		files = append(files, path+suffix)
	}
	return files
}

// reserveStaging creates the empty file the copy for dstPath is built in,
// under a name no other caller knows: dstPath, ".tmp-" and a random suffix,
// created exclusively, with none of the files a database keeps beside it
// existing. Everything the copy creates under that name is this call's.
func reserveStaging(dstPath string) (string, error) {
	var taken error
	for range 4 {
		var suffix [8]byte
		_, _ = rand.Read(suffix[:]) // crypto/rand never fails on supported platforms
		stage := dstPath + ".tmp-" + hex.EncodeToString(suffix[:])
		// Files left under this name are not this call's to remove.
		if taken = destinationFree(stage); taken != nil {
			continue
		}
		f, err := osOpenFile(stage, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
		if errors.Is(err, fs.ErrExist) {
			taken = err
			continue
		}
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
	return "", fmt.Errorf("any-store: copy to latest format: no free staging name beside %s: %w", dstPath, taken)
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

// publishStaging gives the closed copy the name dstPath without replacing
// anything: a hard link, which fails if dstPath exists, then the removal of
// the staging name. The directory is synced after each (syncDir). A rename
// would replace a file published at dstPath meanwhile, by another call or
// anyone else; a check before it would not close that window, so there is
// no fallback where links fail. Before the link succeeds a failure removes
// the staging file; once it has, dstPath is never removed, and a failure
// still fails the call.
//
// The files a database keeps beside dstPath are checked again right before
// the link: one that appeared while the copy ran, put there by anyone or
// published by another copy, would be read as the copy's log. The link
// decides atomically only for dstPath itself; a sidecar created between
// that check and the link is not seen.
func publishStaging(stage, dstPath string) error {
	if err := destinationFree(dstPath); err != nil {
		return discardStaging(err, stage, nil)
	}
	if err := osLink(stage, dstPath); err != nil {
		if errors.Is(err, fs.ErrExist) {
			err = fmt.Errorf("%w: %w", ErrDestinationExists, err)
		} else {
			err = fmt.Errorf("%w: %w", ErrPublishUnsupported, err)
		}
		return discardStaging(err, stage, nil)
	}
	// The copy is published: what fails from here on says so.
	published := fmt.Sprintf("any-store: copy to latest format: the copy is published at %s, but", dstPath)
	var errs []error
	if err := dirSync(dstPath); err != nil {
		errs = append(errs, fmt.Errorf("%s syncing its directory failed: %w", published, err))
	}
	if err := osRemove(stage); err != nil && !errors.Is(err, fs.ErrNotExist) {
		errs = append(errs, fmt.Errorf("%s its staging name is left on disk: %s: %w", published, stage, err))
	} else if err := dirSync(stage); err != nil {
		errs = append(errs, fmt.Errorf("%s syncing the directory of %s failed: %w", published, filepath.Dir(stage), err))
	}
	return errors.Join(errs...)
}
