package anystore

import (
	"context"
	"errors"
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
// the entries an existing index would hold for the same documents. Open
// compares each range index's stamp against the table and rebuilds the
// indexes an intervening change affects (upgradeIndexFormats).
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
// rebuilt for the current format. A stamp above the current version was
// written by a newer build: this build cannot know which changes lie between,
// so the index is rebuilt into this build's format and restamped, and the
// newer build rebuilds it back when it runs again.
func indexFormatOutdated(stored int, info IndexInfo) bool {
	if stored > indexFormatVersion {
		return true
	}
	for v := max(stored, 0); v < len(indexFormatChanges); v++ {
		if indexFormatChanges[v](info) {
			return true
		}
	}
	return false
}

// indexFormatRecord returns the stamps of a catalog record: the format its
// entries were built under ("v", 0 when the record predates versioning) and
// the format a rebuild last failed at ("vq", 0 when none; see
// upgradeIndexFormats). A record that does not parse reads as 0, 0.
func indexFormatRecord(raw []byte) (format, quarantined int) {
	var p anyenc.Parser
	v, err := p.Parse(raw)
	if err != nil {
		return 0, 0
	}
	return v.GetInt("v"), v.GetInt("vq")
}

// indexFormatOf returns the format stamp of a catalog record.
func indexFormatOf(raw []byte) int {
	format, _ := indexFormatRecord(raw)
	return format
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

// setIndexRecordInt rewrites one integer field of an index's catalog record,
// keeping every other field as stored.
func (db *db) setIndexRecordInt(tx *btree.WriteTx, collName, indexName, field string, n int) error {
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
	v.Set(field, a.NewNumberInt(n))
	return tx.Put(db.systemNS, key, v.MarshalTo(nil))
}

// outdatedIndexes lists the range indexes of a collection whose stamp an
// intervening format change leaves outdated, as the catalog describes them
// in this view, leaving out those a rebuild already failed at this format.
func (db *db) outdatedIndexes(tx *btree.ReadTx, collName string) ([]IndexInfo, error) {
	infos, err := db.getIndexInfos(tx, collName)
	if err != nil {
		return nil, err
	}
	var outdated []IndexInfo
	for _, info := range infos {
		if info.Kind != IndexKindRange {
			continue
		}
		raw, err := tx.AppendValue(db.systemNS, indexKey(collName, info.Name), nil)
		if err != nil {
			return nil, err
		}
		format, quarantined := indexFormatRecord(raw)
		if indexFormatOutdated(format, info) && quarantined != indexFormatVersion {
			outdated = append(outdated, info)
		}
	}
	return outdated, nil
}

// upgradeIndexFormats runs from Open, before any collection handle or user
// transaction exists, and rebuilds every range index whose stamp an
// intervening format change leaves outdated. A read pass finds the work, so
// a settled file takes no write lock.
//
// Each index rebuilds in its own write transaction, re-reading its record
// through the writer's view since a peer may have rebuilt, redefined or
// dropped it in between: progress survives a crash or a cancelled ctx, and
// peer writers interleave. A rebuild the data defeats — a unique index whose
// documents the current format finds duplicate (explicit nulls under a
// sparse index, elements fanned out under a dotted path), a document this
// release rejects — rolls back and marks the record with the format it
// failed at ("vq"): the index stays as it was, the planner never plans an
// outdated index (plannableIndexes), writes keep maintaining it, Stats and
// Explain report it, and no Open retries until the index is dropped and
// recreated or the format changes again.
//
// Nothing here fails the Open, which must succeed on a file whose damage
// only a later operation reports (a corrupt page under a collection): a
// catalog the read pass cannot read is left alone, and a rebuild whose
// transaction fails to commit is left for the next Open. Both are sound
// because an outdated index is never planned. Only a cancelled ctx stops
// the upgrade, and then the Open.
func (db *db) upgradeIndexFormats(ctx context.Context) error {
	type work struct{ coll, index string }
	var todo []work
	err := db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		names, err := db.collectionNames(tx)
		if err != nil {
			return err
		}
		for _, name := range names {
			outdated, err := db.outdatedIndexes(tx, name)
			if err != nil {
				continue
			}
			for _, info := range outdated {
				todo = append(todo, work{name, info.Name})
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	for _, w := range todo {
		if err = ctx.Err(); err != nil {
			return err
		}
		var rebuildErr error
		_ = db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			rebuildErr = db.rebuildOutdatedIndex(tx, w.coll, w.index)
			return rebuildErr
		})
		if rebuildErr != nil {
			_ = db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
				return db.setIndexRecordInt(tx, w.coll, w.index, "vq", indexFormatVersion)
			})
		}
	}
	return nil
}

// rebuildOutdatedIndex rebuilds one range index through the write tx if the
// catalog, read through the writer's view, still holds it outdated (see
// upgradeIndexFormats). The collection handle it builds is private to the
// rebuild: nothing is published.
func (db *db) rebuildOutdatedIndex(tx *btree.WriteTx, collName, indexName string) error {
	outdated, err := db.outdatedIndexes(&tx.ReadTx, collName)
	if err != nil {
		return err
	}
	var info *IndexInfo
	for i := range outdated {
		if outdated[i].Name == indexName {
			info = &outdated[i]
		}
	}
	if info == nil {
		return nil
	}
	ns, err := tx.GetNamespace(collName)
	if err != nil {
		if errors.Is(err, btree.ErrNamespaceNotFound) {
			return nil
		}
		return err
	}
	cfg, err := db.loadCollConfig(&tx.ReadTx, collName)
	if err != nil {
		return err
	}
	c := &collection{name: collName, db: db, ns: ns, primaryKey: cfg.PrimaryKey}
	if c.primaryKey == "" {
		c.primaryKey = "id"
	}
	return c.rebuildIndex(tx, *info)
}

// rebuildIndex drops the index namespace and builds it again from the
// collection's documents under the current format, restamping the catalog
// record.
func (c *collection) rebuildIndex(tx *btree.WriteTx, info IndexInfo) (err error) {
	tx.MarkSchemaChanged()
	if err = tx.DeleteNamespace(indexNsName(c.name, info.Name)); err != nil && !errors.Is(err, btree.ErrNamespaceNotFound) {
		return err
	}
	if err = c.db.setIndexRecordInt(tx, c.name, info.Name, "v", indexFormatVersion); err != nil {
		return err
	}
	_, err = c.buildRangeIndex(tx, info)
	return err
}
