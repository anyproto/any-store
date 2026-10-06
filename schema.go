package anystore

import (
	"encoding/binary"
	"errors"
	"runtime"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// collSchema is one version of a collection's schema: its name, its data
// namespace and its index sets, all read from one catalog view. A version is
// immutable once resolved by anyone, but for gen; a change makes another
// version, so an operation that resolved one works with a complete
// generation — mirroring SQLite's whole-schema reload on a schema-cookie
// bump (a reader sees either the old or the new schema, never half-applied
// DDL).
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
	// names lists the names the collection went by, oldest first, each with
	// the cookie it took effect at; nil for a collection never renamed
	// through this handle. A transaction older than a rename finds the
	// collection in its snapshot under the name of that time.
	names *[]nameSince
}

type nameSince struct {
	name string
	from uint32
}

// nameAt returns the name the collection has in a snapshot at cookie.
func (s *collSchema) nameAt(cookie uint32) string {
	if s.names == nil {
		return s.name
	}
	names := *s.names
	for i := len(names) - 1; i >= 0; i-- {
		if cookieLE(names[i].from, cookie) {
			return names[i].name
		}
	}
	return names[0].name
}

// renamed returns the names history with the name the collection takes at
// cookie appended.
func (s *collSchema) renamed(name string, cookie uint32) *[]nameSince {
	var names []nameSince
	if s.names != nil {
		names = *s.names
	} else {
		names = []nameSince{{name: s.name}}
	}
	names = append(names[:len(names):len(names)], nameSince{name: name, from: cookie})
	return &names
}

// clone returns a copy to edit and log; gen starts unset.
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
//
// The cookie this process announced is its commit becoming visible: the
// heads it changed are installed and known follows within instructions of
// the publication (commonTx.schemaCommitted), and the transaction waits for
// that rather than work beside it — the heads of that moment are the
// pre-commit ones, and so are the names the handles answer to. A commit
// that publishes nothing withdraws its announcement once it returns
// (writeTx.Commit); should the wait outlast that, the cookie is another
// process's after all. Bounded in case the announcement is never withdrawn
// (a commit that panicked): a generation taken for nothing costs one
// verification per handle, never correctness.
func (db *db) observeCookie(tx *btree.ReadTx) {
	cookie := tx.SnapshotSchemaCookie()
	for spins := 0; ; {
		e := db.epoch.Load()
		if cookieLE(cookie, e.known) {
			return
		}
		if own := db.ownCookie.Load(); own>>32 != 0 && uint32(own) == cookie && spins < announcedCookieSpins {
			spins++
			runtime.Gosched()
			continue
		}
		if db.epoch.CompareAndSwap(e, &schemaEpoch{gen: e.gen + 1, start: cookie, known: cookie}) {
			return
		}
	}
}

// announcedCookieSpins bounds the wait for an announced cookie's epoch.
// A variable for the tests that hold a commit in its publication.
var announcedCookieSpins = 1 << 16

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

// txSchema is a transaction's own schema state, in the btree transaction's
// Aux slot: the versions it loaded for itself (memo), and for the write
// transaction the schema changes it made and has not committed (log).
//
// The log is where uncommitted DDL lives. A verb appends the version its
// handle has from the change on — or the handle's end — and the writer's
// resolve answers from the log before the head; everyone else keeps the
// committed head, so an uncommitted create, drop, rename or index change is
// the writer's alone, as a DDL statement's effect is its connection's in
// SQLite until the commit. The commit installs the logged versions as the
// heads (install); a rollback, or one to a savepoint, discards the entries
// of its scope (discardLog) and has nothing to revert.
type txSchema struct {
	memo []memoEntry
	log  []logEntry
	// installed reports that the commit published the log (install ran):
	// the registry has to follow it (settleLog), whatever Commit returned.
	installed bool
}

type memoEntry struct {
	c *collection
	s *collSchema
}

// logEntry is one schema change of the open write transaction, or the mark
// a verb leaves on the handle it works on (logPin). s is the version the
// handle has from the entry on — for a drop, the version dropped, for its
// name — and prev the version the change was made to, nil for a collection
// the transaction created.
type logEntry struct {
	c    *collection
	s    *collSchema
	prev *collSchema
	kind logKind
}

type logKind uint8

const (
	// logPin references the handle without changing it: a verb began on it.
	logPin logKind = iota
	// logVersion replaces prev with s at the commit.
	logVersion
	// logCreate is the first version of a collection the transaction
	// created; its handle enters the registry at the commit.
	logCreate
	// logDrop ends the handle at the commit.
	logDrop
)

func txSchemaOf(tx *btree.ReadTx) *txSchema {
	t, _ := tx.Aux().(*txSchema)
	return t
}

func memoGet(tx *btree.ReadTx, c *collection) *collSchema {
	t := txSchemaOf(tx)
	if t == nil {
		return nil
	}
	for i := range t.memo {
		if t.memo[i].c == c {
			return t.memo[i].s
		}
	}
	return nil
}

func memoPut(tx *btree.ReadTx, c *collection, s *collSchema) {
	t := txSchemaOf(tx)
	if t == nil {
		t = &txSchema{}
		tx.SetAux(t)
	}
	t.memo = append(t.memo, memoEntry{c: c, s: s})
}

// release empties the state for the next transaction that reuses it.
func (t *txSchema) release() {
	clear(t.memo)
	t.memo = t.memo[:0]
	clear(t.log)
	t.log = t.log[:0]
	t.installed = false
}

// logged returns the last schema change the open write transaction tx made
// through c, nil when it made none: what the writer has in place of the
// head.
func (c *collection) logged(tx *btree.ReadTx) *logEntry {
	t := txSchemaOf(tx)
	if t == nil {
		return nil
	}
	for i := len(t.log) - 1; i >= 0; i-- {
		if e := &t.log[i]; e.c == c && e.kind != logPin {
			return e
		}
	}
	return nil
}

// inTx returns the version of c the open write transaction tx works with:
// the last it logged for the handle, else the head. dropped reports that
// the transaction dropped the collection. For the commit-time passes over
// the handles written through, closed or not.
func (c *collection) inTx(tx *btree.ReadTx) (s *collSchema, dropped bool) {
	if e := c.logged(tx); e != nil {
		return e.s, e.kind == logDrop
	}
	return c.head.Load(), false
}

// byName returns the entry under which the open write transaction has a
// collection by name: the one it created or renamed to it, or the drop that
// took the name. Only a handle's last change counts — the earlier ones are
// superseded.
func (t *txSchema) byName(name string) *logEntry {
	for i := len(t.log) - 1; i >= 0; i-- {
		e := &t.log[i]
		if e.kind == logPin || e.s.name != name {
			continue
		}
		if t.superseded(i) {
			continue
		}
		return e
	}
	return nil
}

// superseded reports that a later entry changes the handle of entry i.
func (t *txSchema) superseded(i int) bool {
	for j := i + 1; j < len(t.log); j++ {
		if t.log[j].c == t.log[i].c && t.log[j].kind != logPin {
			return true
		}
	}
	return false
}

// add appends an entry and pins its handle (collection.pinned).
func (t *txSchema) add(db *db, e logEntry) {
	db.mu.Lock()
	e.c.pinned++
	db.mu.Unlock()
	t.log = append(t.log, e)
}

// logVersion records next as the version c has from the commit of wtx on,
// made from prev. The version is valid from the cookie the commit produces:
// every schema-changing verb MarkSchemaChanged, and SchemaCookie moves
// exactly once per such commit, atomically with the DDL (the OP_Transaction
// analog, vdbe.c:4091-4192 in SQLite).
func (c *collection) logVersion(wtx WriteTx, prev, next *collSchema) {
	next.validFrom = wtx.btreeWriteTx().DiskSchemaCookie() + 1
	wtx.schemaLog().add(c.db, logEntry{c: c, s: next, prev: prev, kind: logVersion})
}

// logCreate records s as the first version of the collection c that wtx
// created. The handle stays out of the registry until the commit: the
// writer finds it by name through the log (db.openCollectionOnce).
func (c *collection) logCreate(wtx WriteTx, s *collSchema) {
	s.validFrom = wtx.btreeWriteTx().DiskSchemaCookie() + 1
	// No transaction older than the creation speaks for the handle
	// (resolveSlow): their snapshots do not have the collection, and that
	// must not retire it.
	c.since = s.validFrom
	wtx.schemaLog().add(c.db, logEntry{c: c, s: s, kind: logCreate})
}

// logDrop records the end of c, at version s, at the commit of wtx.
func (c *collection) logDrop(wtx WriteTx, s *collSchema) {
	wtx.schemaLog().add(c.db, logEntry{c: c, s: s, prev: s, kind: logDrop})
}

// logPin marks c as worked on by a verb of wtx, so that a Close() waits for
// the transaction to end (collection.pinned). Under db.mu with the check
// that the handle is live: a Close() cannot evict it in between. A handle
// the log references already is pinned already; the log does not grow with
// the verbs called on it.
func (c *collection) logPin(wtx WriteTx) error {
	t := wtx.schemaLog()
	for i := range t.log {
		if t.log[i].c == c {
			return c.alive()
		}
	}
	c.db.mu.Lock()
	if err := c.alive(); err != nil {
		c.db.mu.Unlock()
		return err
	}
	c.pinned++
	c.db.mu.Unlock()
	t.log = append(t.log, logEntry{c: c, kind: logPin})
	return nil
}

// install publishes the log as the commit becomes visible, before the epoch
// passes its cookie (schemaCommitted): the last version logged for a handle
// is its head from here on, a dropped handle is closed. Runs inside the
// btree commit (btree.WriteTx.OnCommitted) — atomic stores. A reader that
// loaded a version for the handle meanwhile installs nothing over these
// (the compare-and-swap in resolveSlow and adopt).
func (t *txSchema) install(gen uint64) {
	for i := range t.log {
		e := &t.log[i]
		switch e.kind {
		case logVersion, logCreate:
			e.s.gen.Store(gen)
			e.c.head.Store(e.s)
		case logDrop:
			e.c.closed.Store(true)
		}
	}
	t.installed = true
}

// settleLog brings the registry up to the committed log, inside the btree
// commit after install and before the epoch passes the cookie
// (commonTx.schemaCommitted): a created collection's handle enters, a
// renamed one is re-keyed, a dropped one leaves. In log order, so a name a
// handle went through on the way is free for the entry that takes it next.
// A handle another caller registered meanwhile for one of these
// collections or names — an older snapshot's, left in the slot — is
// displaced (registerLocked). The entries then release their handles.
//
// db.mu inside the commit: no holder of db.mu waits on the database (see
// btree.WriteTx.OnCommitted).
func (db *db) settleLog(t *txSchema) {
	db.mu.Lock()
	for i := range t.log {
		e := &t.log[i]
		if e.kind == logPin || e.c.closed.Load() && e.kind != logDrop {
			continue
		}
		switch e.kind {
		case logCreate:
			db.registerLocked(e.c, e.s.name)
		case logVersion:
			if e.s.name != e.prev.name {
				if cur, ok := db.openedCollections[e.prev.name]; ok && cur == Collection(e.c) {
					delete(db.openedCollections, e.prev.name)
				}
				db.registerLocked(e.c, e.s.name)
			}
		case logDrop:
			if cur, ok := db.openedCollections[e.s.name]; ok && cur == Collection(e.c) {
				delete(db.openedCollections, e.s.name)
				db.forgetLocked(e.c)
			} else {
				db.evictLocked(e.c)
			}
		}
	}
	db.unpinLocked(t.log)
	db.mu.Unlock()
	clear(t.log)
	t.log = t.log[:0]
}

// discardLog drops the entries [from:] of the log: their scope rolled back.
// Nothing of theirs was shared, so there is nothing to revert, except what
// the transaction's writes did to the sketches a discarded version shares
// with the head (index.cloneWithNs): those are flagged on the head's index
// objects, so the next write transaction rebases them
// (resetUncommittedSketches). The handle of a collection created in the
// scope ends here.
func (db *db) discardLog(t *txSchema, from int) {
	for i := len(t.log) - 1; i >= from; i-- {
		e := &t.log[i]
		switch e.kind {
		case logCreate:
			e.c.closed.Store(true)
		case logVersion:
			head := e.c.head.Load()
			if head == nil {
				continue
			}
			for _, idx := range e.s.indexes {
				if !idx.sketchModified {
					continue
				}
				for _, h := range head.indexes {
					if h.sketch == idx.sketch {
						h.markSketchModified()
					}
				}
			}
		}
	}
	db.mu.Lock()
	db.unpinLocked(t.log[from:])
	db.mu.Unlock()
	clear(t.log[from:])
	t.log = t.log[:from]
}

// registerLocked puts c in the registry under name. A handle that holds the
// name or stands for the same collection is displaced: left out of the
// registry it would stay live and miss the schema changes made through c.
// The caller holds db.mu.
func (db *db) registerLocked(c *collection, name string) {
	if other, ok := db.openedCollections[name]; ok && other != Collection(c) {
		o := other.(*collection)
		o.closed.Store(true)
		db.forgetLocked(o)
	}
	if other := db.byIdentity[c.identity]; other != nil && other != c {
		other.closed.Store(true)
		db.evictLocked(other)
	}
	db.openedCollections[name] = c
	db.byIdentity[c.identity] = c
}

// unpinLocked releases the handles of the entries (collection.pinned); a
// Close() deferred meanwhile takes effect with the last reference. The
// caller holds db.mu.
func (db *db) unpinLocked(entries []logEntry) {
	for i := range entries {
		c := entries[i].c
		c.pinned--
		if c.pinned > 0 || !c.closePending.Load() {
			continue
		}
		c.closePending.Store(false)
		if c.closed.CompareAndSwap(false, true) {
			db.evictLocked(c)
		}
	}
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
// The write transaction has its own uncommitted changes first (txSchema.log).
// The head serves the transactions it is proven for: those of its generation
// at a cookie from validFrom through the epoch's known. Any other transaction
// — older than the head, in the gap between a commit and known, or the first
// of a generation — goes through resolveSlow.
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
	if writer {
		if e := c.logged(tx); e != nil {
			if e.kind == logDrop {
				// Dropped by this transaction: gone for it, as a table is
				// after DROP TABLE in the same SQLite transaction.
				return nil, ErrCollectionClosed
			}
			c.refreshSketches(tx, e.s)
			return e.s, nil
		}
	}
	// The epoch before the head: a commit of this process installs the
	// heads it changed before known passes its cookie (schemaCommitted), so
	// an epoch that admits the transaction's cookie was read after the
	// installs, and the head read after it is the committed one. The other
	// order could pair the pre-commit head with the epoch of after.
	e := c.db.epoch.Load()
	if s := c.head.Load(); s != nil {
		if s.current(e, tx.SnapshotSchemaCookie(), writer) {
			if writer {
				c.refreshSketches(tx, s)
			}
			return s, nil
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
// Loads of one handle are serialized under c.mu. The commit of this process
// installs the heads it changed without it (txSchema.install), so the head
// is replaced by compare-and-swap: a load that began before such a commit
// does not overwrite what it installed.
func (c *collection) resolveSlow(tx *btree.ReadTx, asName string) (*collSchema, error) {
	if s := memoGet(tx, c); s != nil {
		return s, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.alive(); err != nil {
		return nil, err
	}
	e := c.db.epoch.Load() // before the head, as in resolveAs
	head := c.head.Load()
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
	if s != head && !c.head.CompareAndSwap(head, s) {
		// A commit of this process installed a newer version meanwhile:
		// this transaction's own stays its own.
		memoPut(tx, c, s)
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
	if cookieLE(e.start, cookie) && cookieLE(cookie, e.known) && cookieLE(c.since, cookie) {
		s.gen.Store(e.gen)
		if c.head.CompareAndSwap(nil, s) {
			return
		}
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
	if c.identity == "" {
		if err = c.identify(tx, name, token, ns); err != nil {
			return nil, err
		}
	} else if ns.RootPage() != c.dataRoot || !c.sameToken(token) {
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
	// The root tells apart the collections of old files that share one
	// token; no two live collections share a root.
	c.identity = string(binary.BigEndian.AppendUint32(append([]byte(nil), token...), c.dataRoot))
	return nil
}

// sameToken reports that token is the catalog token of the handle's
// identity.
func (c *collection) sameToken(token []byte) bool {
	return len(c.identity) == len(token)+4 && c.identity[:len(token)] == string(token)
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
