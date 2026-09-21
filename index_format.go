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

// outdatedIndexes lists the range indexes of a collection whose stamp an
// intervening format change leaves outdated, as the catalog describes them
// in this view.
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
		format, err := db.readIndexFormat(tx, collName, info.Name)
		if err != nil {
			return nil, err
		}
		if indexFormatOutdated(format, info) {
			outdated = append(outdated, info)
		}
	}
	return outdated, nil
}

// upgradeIndexFormats runs from Open, before any collection handle or user
// transaction exists, and rebuilds every range index whose stamp an
// intervening format change leaves outdated. A read pass finds the work, so
// a settled file costs no write lock; only then does one write transaction
// begin, re-reading each collection's catalog through the writer's view,
// since a peer may have rebuilt, redefined or dropped an index in between.
//
// Each index rebuilds under its own savepoint. A rebuild the data defeats (a
// unique index whose documents the current format finds duplicate: explicit
// nulls under a sparse index, elements fanned out under a dotted path) rolls
// back to the savepoint and leaves the index as it was, stamp included: the
// planner never plans an outdated index (visibleIndexes), writes keep
// maintaining it, DropIndex and EnsureIndex report the duplicate, and the
// next Open tries again. Any other failure fails the Open with ErrIndexRebuild.
func (db *db) upgradeIndexFormats(ctx context.Context) error {
	var collNames []string
	err := db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		names, err := db.collectionNames(tx)
		if err != nil {
			return err
		}
		for _, name := range names {
			outdated, err := db.outdatedIndexes(tx, name)
			if err != nil {
				return err
			}
			if len(outdated) > 0 {
				collNames = append(collNames, name)
			}
		}
		return nil
	})
	if err != nil || len(collNames) == 0 {
		return err
	}
	return db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for _, name := range collNames {
			if err := db.rebuildOutdatedIndexes(tx, name); err != nil {
				return err
			}
		}
		return nil
	})
}

// rebuildOutdatedIndexes rebuilds one collection's outdated range indexes
// through the write tx (see upgradeIndexFormats). The collection handle it
// builds is private to the rebuild: nothing is published.
func (db *db) rebuildOutdatedIndexes(tx *btree.WriteTx, collName string) error {
	outdated, err := db.outdatedIndexes(&tx.ReadTx, collName)
	if err != nil || len(outdated) == 0 {
		return err
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
	for _, info := range outdated {
		sp, err := tx.Savepoint()
		if err != nil {
			return err
		}
		if err = c.rebuildIndex(tx, info); err != nil {
			if !errors.Is(err, ErrUniqueConstraint) {
				return fmt.Errorf("%w: %s.%s: %w", ErrIndexRebuild, collName, info.Name, err)
			}
			if err = tx.RollbackToSavepoint(sp); err != nil {
				return err
			}
			continue
		}
		if err = tx.ReleaseSavepoint(sp); err != nil {
			return err
		}
	}
	return nil
}

// rebuildIndex drops the index namespace and builds it again from the
// collection's documents under the current format, restamping the catalog
// record.
func (c *collection) rebuildIndex(tx *btree.WriteTx, info IndexInfo) (err error) {
	tx.MarkSchemaChanged()
	if err = tx.DeleteNamespace(indexNsName(c.name, info.Name)); err != nil && !errors.Is(err, btree.ErrNamespaceNotFound) {
		return err
	}
	if err = c.db.stampIndexFormat(tx, c.name, info.Name); err != nil {
		return err
	}
	_, err = c.buildRangeIndex(tx, info)
	return err
}
