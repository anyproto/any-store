package anystore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// indexFormatVersion is the current format of a range index: how a document's
// values map to the entries the index holds. Every range index's catalog
// record carries the version it was built under ("v"); a record without it
// predates versioning and reads as 0.
//
// Bump it, and append an entry to indexFormatChanges, whenever a change alters
// the entries an existing index would hold for the same documents. A
// collection open compares each range index's stamp against the table and
// rebuilds the indexes an intervening change affects (rebuildOutdatedIndexes).
const indexFormatVersion = 1

// indexFormatChanges describes every key-derivation change, oldest first: the
// entry at i introduced format version i+1 and reports whether an index
// definition is affected by it. An index stamped below the current version is
// rebuilt when at least one later entry reports it affected; otherwise its
// entries already match what the current code writes and it is left alone,
// stamp included.
var indexFormatChanges = [...]func(info IndexInfo) bool{
	// v1: a sparse index holds explicit nulls; a dotted path fans out through
	// arrays of objects, and fields sharing an array key each element, nested
	// arrays one level deeper included. A non-sparse index over top-level
	// fields only derives the same entries as before.
	func(info IndexInfo) bool {
		if info.Sparse {
			return true
		}
		for _, f := range info.Fields {
			if strings.Contains(f, ".") {
				return true
			}
		}
		return false
	},
}

// indexFormatOutdated reports whether a range index stamped at stored must be
// rebuilt for the current format. A stamp at or above the current version
// (a newer build wrote it) is left alone.
func indexFormatOutdated(stored int, info IndexInfo) bool {
	for v := max(stored, 0); v < len(indexFormatChanges); v++ {
		if indexFormatChanges[v](info) {
			return true
		}
	}
	return false
}

// readIndexFormat returns the format stamp of an index's catalog record: 0
// when the record predates versioning, ErrIndexNotFound when there is none.
func (db *db) readIndexFormat(tx *btree.ReadTx, collName, indexName string) (int, error) {
	raw, err := tx.AppendValue(db.systemNS, indexKey(collName, indexName), nil)
	if err != nil {
		if errors.Is(err, btree.ErrKeyNotFound) {
			return 0, ErrIndexNotFound
		}
		return 0, err
	}
	return indexFormatOf(raw), nil
}

// indexFormatOf returns the format stamp of a catalog record: 0 when the
// record predates versioning or does not parse.
func indexFormatOf(raw []byte) int {
	var p anyenc.Parser
	v, err := p.Parse(raw)
	if err != nil {
		return 0
	}
	return v.GetInt("v")
}

// stampIndexFormat rewrites an index's catalog record with the current format
// version, keeping every other field as stored.
func (db *db) stampIndexFormat(tx *btree.WriteTx, collName, indexName string) error {
	key := indexKey(collName, indexName)
	raw, err := tx.AppendValue(db.systemNS, key, nil)
	if err != nil {
		return err
	}
	var p anyenc.Parser
	v, err := p.Parse(raw)
	if err != nil {
		return err
	}
	var a anyenc.Arena
	v.Set("v", a.NewNumberInt(indexFormatVersion))
	return tx.Put(db.systemNS, key, v.MarshalTo(nil))
}

// rebuildOutdatedIndexes rebuilds the range indexes init found stamped below
// the current format (c.outdatedIndexes) in one write transaction: a
// savepoint of an ambient write tx, or a fresh tx otherwise. An ambient read
// tx is detached first: its snapshot keeps resolving the old namespaces, and
// its planner skips the rebuilt handles the way it skips any index a peer
// recreated after the snapshot (visibleTo).
//
// It runs from newCollection before the handle is published, so no reader
// holds it yet. Under an ambient write tx the handle is published while the
// rebuild is uncommitted; a rollback restores the old handles and re-arms
// the list, and the next open of the cached handle runs the rebuild again
// (rebuildPending, checked by openCollection). rebuildMu serialises those
// later runs so a concurrent open waits for the rebuild instead of proceeding
// on the outdated set.
//
// Each index is re-checked through the write tx, since a peer may have
// dropped or rebuilt it after init's snapshot. A rebuild is drop+recreate of
// the index namespace under the same name — fresh root, scalar multikey
// marker, fresh sketch, full backfill — so peers adopt it like any recreate
// (the root moved). A failure fails the open: the collection must not answer
// from an index whose entries the current code would not have written.
func (c *collection) rebuildOutdatedIndexes(ctx context.Context) error {
	c.rebuildMu.Lock()
	defer c.rebuildMu.Unlock()
	c.mu.Lock()
	names := c.outdatedIndexes
	c.outdatedIndexes = nil
	c.mu.Unlock()
	c.rebuildPending.Store(false)
	if len(names) == 0 {
		return nil
	}
	if ctxTx := ctx.Value(ctxKeyTx); ctxTx != nil {
		if _, isWrite := ctxTx.(WriteTx); !isWrite {
			ctx = context.WithValue(ctx, ctxKeyTx, nil)
		}
	}
	return c.db.doWriteTxModifiedW(ctx, func(wtx WriteTx, tx *btree.WriteTx) (bool, error) {
		cur := c.loadIndexes()
		next := make([]*index, len(cur))
		copy(next, cur)
		var rebuilt []*index
		for _, name := range names {
			pos := -1
			for i, idx := range cur {
				if idx.info.Name == name {
					pos = i
					break
				}
			}
			if pos < 0 {
				continue
			}
			format, err := c.db.readIndexFormat(&tx.ReadTx, c.name, name)
			if errors.Is(err, ErrIndexNotFound) {
				continue
			}
			if err != nil {
				return false, err
			}
			if !indexFormatOutdated(format, cur[pos].info) {
				continue
			}
			idx, err := c.rebuildIndex(tx, cur[pos])
			if err != nil {
				return false, fmt.Errorf("%w: %s.%s: %w", ErrIndexRebuild, c.name, name, err)
			}
			next[pos] = idx
			rebuilt = append(rebuilt, idx)
		}
		if len(rebuilt) == 0 {
			return false, nil
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.registerIndexSetRestore(wtx)
		wtx.onRollbackUndo(func() {
			c.mu.Lock()
			c.outdatedIndexes = names
			c.mu.Unlock()
			c.rebuildPending.Store(true)
		})
		// Same visibility bound as createIndexes: the new roots exist only
		// in this tx's view until its commit publishes the next cookie.
		validFrom := tx.DiskSchemaCookie() + 1
		for _, idx := range rebuilt {
			idx.validFromCookie = validFrom
		}
		c.storeIndexes(next)
		return true, nil
	})
}

// rebuildIndex drops the index namespace and builds it again from the
// collection's documents under the current format, restamping the catalog
// record. The old handle is discarded; the caller publishes the new one.
func (c *collection) rebuildIndex(tx *btree.WriteTx, old *index) (*index, error) {
	tx.MarkSchemaChanged()
	if err := tx.DeleteNamespace(old.ns.Name()); err != nil && !errors.Is(err, btree.ErrNamespaceNotFound) {
		return nil, err
	}
	if err := c.db.stampIndexFormat(tx, c.name, old.info.Name); err != nil {
		return nil, err
	}
	return c.buildRangeIndex(tx, old.info)
}
