package anystore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// collSchema is one version of a collection's schema: its name, its data
// namespace and its index sets, all read from one catalog view. A published
// version is immutable but for gen; a change publishes another version, so an
// operation that resolved one works with a complete generation — mirroring
// SQLite's whole-schema reload on a schema-cookie bump (a reader sees either
// the old or the new schema, never half-applied DDL).
//
// A version says for which transactions it holds (see collection.resolve):
// SQLite checks the schema of a connection against the cookie of the
// statement's own transaction and reloads it through that transaction on a
// mismatch (OP_Transaction, vdbe.c:4128-4190; sqlite3InitOne,
// prepare.c:257-293). A connection runs one transaction at a time, so one
// schema is enough there. Here one handle serves many transactions at
// different snapshots: the current version is shared by those it is proven
// for, and a transaction outside that range loads its own.
type collSchema struct {
	name string
	ns   *btree.Namespace
	// indexes, ftsIndexes and vindexes are the range, full-text and vector
	// index sets. The slices are never mutated in place. Index objects are
	// shared between versions: one whose definition, format and root pages a
	// view still has is carried over, with its sketch, buffers and caches.
	indexes    []*index
	ftsIndexes []*ftsIndex
	vindexes   []*vectorIndex

	// validFrom is the schema cookie of the view the version was read from,
	// or the cookie its schema change commits with. A transaction at an
	// older snapshot does not use it.
	validFrom uint32
	// gen is the peer generation (schemaEpoch.gen) the version is proven
	// for: installed or verified by a transaction of that generation.
	gen atomic.Uint64
	// base is the committed version under this one, while the write
	// transaction that published this one is open: what every reader of
	// that time works with. Set by publish, dropped at the commit.
	base atomic.Pointer[collSchema]
	// names lists the names the collection went by, oldest first, each with
	// the cookie it took effect at; nil for a collection never renamed
	// through this handle. A transaction older than a rename finds the
	// collection in its snapshot under the name of that time.
	names []nameSince
}

type nameSince struct {
	name string
	from uint32
}

// nameAt returns the name the collection has in a snapshot at cookie.
func (s *collSchema) nameAt(cookie uint32) string {
	for i := len(s.names) - 1; i >= 0; i-- {
		if cookieLE(s.names[i].from, cookie) {
			return s.names[i].name
		}
	}
	if len(s.names) > 0 {
		return s.names[0].name
	}
	return s.name
}

// clone returns a copy to edit and publish; gen and base start unset.
func (s *collSchema) clone() *collSchema {
	return &collSchema{
		name:       s.name,
		ns:         s.ns,
		indexes:    s.indexes,
		ftsIndexes: s.ftsIndexes,
		vindexes:   s.vindexes,
		validFrom:  s.validFrom,
		names:      s.names,
	}
}

// sameAs reports that two versions bind the same name, data tree and index
// objects.
func (s *collSchema) sameAs(o *collSchema) bool {
	if s.name != o.name || s.ns.RootPage() != o.ns.RootPage() ||
		len(s.indexes) != len(o.indexes) || len(s.ftsIndexes) != len(o.ftsIndexes) || len(s.vindexes) != len(o.vindexes) {
		return false
	}
	for i := range s.indexes {
		if s.indexes[i] != o.indexes[i] {
			return false
		}
	}
	for i := range s.ftsIndexes {
		if s.ftsIndexes[i] != o.ftsIndexes[i] {
			return false
		}
	}
	for i := range s.vindexes {
		if s.vindexes[i] != o.vindexes[i] {
			return false
		}
	}
	return true
}

// cookieLE reports a <= b for schema cookies: they increment per commit and
// are compared modulo 2^32, like the counters in btree.
func cookieLE(a, b uint32) bool {
	return int32(a-b) <= 0
}

// schemaEpoch is what this process knows of the schema cookie's history. It
// is replaced as a whole, so a reader sees the three values of one moment.
//
// known is the newest cookie through which every schema change is reflected
// in the handles: a change of this process replaces the head of the handle it
// went through before known passes its cookie. Another process's change says
// nothing of which collection it touched, so it starts a new generation at
// its cookie, and each handle is verified against the catalog when a
// transaction of that generation first uses it. Within a generation every
// change past start is one of this process, which makes a head stamped with
// the generation valid from max(validFrom, start) through known.
type schemaEpoch struct {
	gen   uint64
	start uint32
	known uint32
}

// observeCookie brings the epoch up to the snapshot a transaction begins at:
// a cookie past known that this process is not committing right now is
// another process's schema change.
func (db *db) observeCookie(tx *btree.ReadTx) {
	cookie := tx.SnapshotSchemaCookie()
	for {
		e := db.epoch.Load()
		if cookieLE(cookie, e.known) {
			return
		}
		if own := db.ownCookie.Load(); own>>32 != 0 && uint32(own) == cookie {
			// The commit of this process becoming visible: known follows
			// within instructions (schemaCommitted), and until then this
			// transaction loads what it uses through its own view.
			return
		}
		if db.epoch.CompareAndSwap(e, &schemaEpoch{gen: e.gen + 1, start: cookie, known: cookie}) {
			return
		}
	}
}

// announceCookie tells observeCookie which cookie the commit about to run
// produces, so a reader that begins once it is visible does not take it for
// another process's. The write lock is held: no other commit can produce it.
func (db *db) announceCookie(cookie uint32) {
	db.ownCookie.Store(1<<32 | uint64(cookie))
}

// schemaCommitted moves known to the cookie of the commit that is becoming
// visible and ends its announcement. It runs inside the btree commit
// (btree.WriteTx.OnCommitted), with the heads the commit changed in place.
func (db *db) schemaCommitted(_, cookie uint32) {
	for {
		e := db.epoch.Load()
		if cookieLE(cookie, e.known) ||
			db.epoch.CompareAndSwap(e, &schemaEpoch{gen: e.gen, start: e.start, known: cookie}) {
			break
		}
	}
	db.endAnnouncement(cookie)
}

// endAnnouncement withdraws the announcement of cookie, unless the next
// commit has announced its own by now.
func (db *db) endAnnouncement(cookie uint32) {
	db.ownCookie.CompareAndSwap(1<<32|uint64(cookie), 0)
}

// schemaMemo holds the versions a transaction loaded for itself; it lives in
// the transaction's Aux slot.
type schemaMemo struct {
	entries []schemaMemoEntry
}

type schemaMemoEntry struct {
	c *collection
	s *collSchema
}

func memoGet(tx *btree.ReadTx, c *collection) *collSchema {
	m, _ := tx.Aux().(*schemaMemo)
	if m == nil {
		return nil
	}
	for i := range m.entries {
		if m.entries[i].c == c {
			return m.entries[i].s
		}
	}
	return nil
}

func memoPut(tx *btree.ReadTx, c *collection, s *collSchema) {
	m, _ := tx.Aux().(*schemaMemo)
	if m == nil {
		m = &schemaMemo{}
		tx.SetAux(m)
	}
	m.entries = append(m.entries, schemaMemoEntry{c: c, s: s})
}

// current reports that the version holds for a transaction at cookie in the
// epoch e; a write transaction is always at the newest state of e.
func (s *collSchema) current(e *schemaEpoch, cookie uint32, writer bool) bool {
	if s.gen.Load() != e.gen {
		return false
	}
	return writer || (cookieLE(s.validFrom, cookie) && cookieLE(e.start, cookie) && cookieLE(cookie, e.known))
}

// resolve returns the schema version an operation in tx works with, or the
// error that ends it: the handle is closed (alive), or the collection is not
// in the transaction's snapshot. Everything the operation reads of the schema
// comes from the one version returned.
//
// The head serves the transactions it is proven for: those of its generation
// at a cookie from validFrom through the epoch's known. A head published by
// the open write tx carries the committed version for everyone else (base).
// Any other transaction — older than the head, in the gap between a commit
// and known, or the first of a generation — goes through resolveSlow.
func (c *collection) resolve(tx *btree.ReadTx) (*collSchema, error) {
	return c.resolveAs(tx, "")
}

// resolveAs is resolve by a caller that knows the name the collection has in
// tx's view (it just read the catalog there): a transaction older than the
// handle looks the collection up under it, where the handle may only know
// later names. A transaction that speaks for the handle (resolveSlow) looks
// under the handle's own name all the same: a collection another process
// renamed has a handle to retire, not one to carry over to the new name.
func (c *collection) resolveAs(tx *btree.ReadTx, name string) (*collSchema, error) {
	if err := c.alive(); err != nil {
		return nil, err
	}
	writer := tx.IsWriteTx()
	if s := c.head.Load(); s != nil {
		e, cookie := c.db.epoch.Load(), tx.SnapshotSchemaCookie()
		if s.current(e, cookie, writer) {
			if writer {
				c.refreshSketches(tx, s)
			}
			return s, nil
		}
		// A head the open write tx published is that tx's until it commits:
		// everyone else is served the committed version it stands on.
		if b := s.base.Load(); b != nil && !writer && b.current(e, cookie, false) {
			return b, nil
		}
	}
	s, err := c.resolveSlow(tx, name)
	if err == nil && writer {
		c.refreshSketches(tx, s)
	}
	return s, err
}

// resolveSlow loads the collection's schema through tx's own view. A
// transaction that is no older than anything the handle went through —
// current generation, at or past the cookie the handle was registered at, not
// older than the head — speaks for the handle: what it loads becomes the
// head, or confirms it for the generation, and a collection it does not find
// is gone, so the handle retires. Any other transaction keeps its version to
// itself for its lifetime.
//
// Loads of one handle are serialized under c.mu, with the schema changes
// published through it.
func (c *collection) resolveSlow(tx *btree.ReadTx, asName string) (*collSchema, error) {
	if s := memoGet(tx, c); s != nil {
		return s, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.alive(); err != nil {
		return nil, err
	}
	head := c.head.Load()
	e := c.db.epoch.Load()
	cookie, writer := tx.SnapshotSchemaCookie(), tx.IsWriteTx()
	if head != nil && head.current(e, cookie, writer) {
		return head, nil
	}
	speaks := cookieLE(e.start, cookie) && cookieLE(cookie, e.known) && cookieLE(c.since, cookie) &&
		(head == nil || cookieLE(head.validFrom, cookie))
	name := c.openName
	if head != nil {
		name = head.nameAt(cookie)
	}
	if asName != "" && !speaks {
		name = asName
	}
	s, err := c.loadSchema(tx, name, head)
	if err != nil {
		if speaks && errors.Is(err, ErrCollectionNotFound) {
			// Gone for everyone from here on, this operation included: the
			// error of a handle that is no longer live.
			c.retire()
			return nil, c.alive()
		}
		return nil, err
	}
	if !speaks {
		memoPut(tx, c, s)
		return s, nil
	}
	s.gen.Store(e.gen)
	if s != head {
		c.head.Store(s)
	}
	return s, nil
}

// adopt settles the version s that tx loaded when it opened the handle: the
// head, if tx speaks for the handle (resolveSlow), else tx's own.
func (c *collection) adopt(tx *btree.ReadTx, s *collSchema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.db.epoch.Load()
	cookie := tx.SnapshotSchemaCookie()
	if c.head.Load() == nil && cookieLE(e.start, cookie) && cookieLE(cookie, e.known) && cookieLE(c.since, cookie) {
		s.gen.Store(e.gen)
		c.head.Store(s)
		return
	}
	memoPut(tx, c, s)
}

// retire ends a handle whose collection the newest schema no longer has
// under its name: dropped, renamed or replaced by another process. Later
// operations fail with ErrCollectionClosed and the caller re-opens (SQLite
// re-prepare style). The caller holds c.mu.
func (c *collection) retire() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.db.mu.Lock()
	c.db.evictLocked(c)
	c.db.mu.Unlock()
}

// loadSchema reads the collection's schema under name through tx: catalog
// record, data namespace, index definitions and their root pages, all from
// that one view — as sqlite3InitOne reads the cookie and the schema rows in
// one transaction. An index object of prev is carried over when the view has
// the same definition, format and roots for it; any other is opened from the
// view. The result is prev itself when nothing differs.
//
// ErrCollectionNotFound: the view has no collection of that name, or another
// one than this handle's (the catalog token, and the data root for the
// entries of old files that share one token).
func (c *collection) loadSchema(tx *btree.ReadTx, name string, prev *collSchema) (*collSchema, error) {
	if testHookLoadSchema != nil {
		testHookLoadSchema()
	}
	db := c.db
	token, err := tx.AppendValue(db.systemNS, collKey(name), nil)
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return nil, ErrCollectionNotFound
		}
		return nil, err
	}
	ns, err := tx.GetNamespace(name)
	if err != nil {
		if errors.Is(err, btree.ErrNamespaceNotFound) {
			return nil, ErrCollectionNotFound
		}
		return nil, err
	}
	if c.catalogID == nil {
		if err = c.identify(tx, name, token, ns); err != nil {
			return nil, err
		}
	} else if !bytes.Equal(token, c.catalogID) || ns.RootPage() != c.dataRoot {
		return nil, ErrCollectionNotFound
	}
	infos, err := db.getIndexInfos(tx, name)
	if err != nil {
		return nil, err
	}

	s := &collSchema{name: name, ns: ns, validFrom: tx.SnapshotSchemaCookie()}
	if prev != nil {
		if prev.name == name {
			s.ns, s.names = prev.ns, prev.names
		}
	}
	for _, info := range infos {
		switch {
		case isFulltext(info):
			fx, fErr := c.loadFtsIndex(tx, name, info, prev)
			if fErr != nil {
				return nil, fErr
			}
			s.ftsIndexes = append(s.ftsIndexes, fx)
		case info.Kind == IndexKindVector:
			vi, vErr := c.loadVectorIndexFor(tx, name, info, prev)
			if vErr != nil {
				return nil, vErr
			}
			s.vindexes = append(s.vindexes, vi)
		default:
			idx, iErr := c.loadRangeIndex(tx, name, info, prev)
			if iErr != nil {
				return nil, iErr
			}
			s.indexes = append(s.indexes, idx)
		}
	}
	if prev != nil && s.sameAs(prev) {
		return prev, nil
	}
	return s, nil
}

// testHookLoadSchema, when set, runs at every load of a schema from the
// catalog. Tests only.
var testHookLoadSchema func()

// identify fixes the identity of the collection this handle stands for, at
// its first load: the catalog token, the data root and the settings that
// never change for a collection.
func (c *collection) identify(tx *btree.ReadTx, name string, token []byte, ns *btree.Namespace) error {
	cfg, err := c.db.loadCollConfig(tx, name)
	if err != nil {
		return err
	}
	c.compression = cfg.Compression
	c.primaryKey = cfg.PrimaryKey
	if c.primaryKey == "" {
		c.primaryKey = "id"
	}
	c.dataRoot = ns.RootPage()
	c.catalogID = token
	// The root tells apart the collections of old files that share one
	// token; no two live collections share a root.
	c.identity = string(binary.BigEndian.AppendUint32(append([]byte(nil), token...), c.dataRoot))
	return nil
}

func (c *collection) loadRangeIndex(tx *btree.ReadTx, name string, info IndexInfo, prev *collSchema) (*index, error) {
	nsName := indexNsName(name, info.Name)
	ns, err := tx.GetNamespace(nsName)
	if err != nil {
		return nil, err
	}
	format, err := c.db.readIndexFormat(tx, name, info.Name)
	if err != nil {
		return nil, err
	}
	if prev != nil {
		for _, old := range prev.indexes {
			if old.nsName == nsName && indexInfoEqual(old.info, info) && old.format == format &&
				old.ns != nil && old.ns.RootPage() == ns.RootPage() {
				return old, nil
			}
		}
	}
	idx, err := newIndex(c, name, info, ns)
	if err != nil {
		return nil, err
	}
	idx.format = format
	idx.outdated = indexFormatOutdated(format, info)
	c.loadSketchAtOpen(tx, name, idx)
	return idx, nil
}

func (c *collection) loadFtsIndex(tx *btree.ReadTx, name string, info IndexInfo, prev *collSchema) (*ftsIndex, error) {
	fx, err := newFtsIndex(c, name, info)
	if err != nil {
		return nil, err
	}
	if err = fx.bindNamespaces(name, tx.GetNamespace); err != nil {
		return nil, err
	}
	if prev != nil {
		for _, old := range prev.ftsIndexes {
			if indexInfoEqual(old.info, info) && old.sameRoots(fx) {
				return old, nil
			}
		}
	}
	return fx, nil
}

func (c *collection) loadVectorIndexFor(tx *btree.ReadTx, name string, info IndexInfo, prev *collSchema) (*vectorIndex, error) {
	if prev != nil {
		for _, old := range prev.vindexes {
			if old.info.Name != info.Name || old.collName != name || !old.boundIn(tx, name) {
				continue
			}
			// The definition as the view's catalog has it, mode and
			// parameters included: the roots alone do not tell a redefined
			// index from the one this object was opened for.
			raw, err := tx.AppendValue(c.db.systemNS, old.catalogKey, nil)
			if err != nil {
				return nil, err
			}
			if indexDefMatches(raw, old.info) {
				return old, nil
			}
		}
	}
	return c.loadVectorIndexAs(tx, name, info)
}
