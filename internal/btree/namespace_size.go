package btree

import "encoding/binary"

// NamespaceSize is a physical-storage summary of a single namespace's B-tree.
// It is produced by walking every page of the tree, so it is O(pages) and
// reflects the namespace's snapshot as seen by the originating ReadTx.
type NamespaceSize struct {
	Pages         int // interior + leaf pages in the B-tree
	OverflowPages int // overflow-chain pages referenced by cells
	Entries       int // leaf cells (key/value pairs)
	PayloadBytes  int // sum of full key+value byte lengths across all entries
}

// TotalPages returns the total page count, including overflow pages. Multiply
// by DB.PageSize to get the namespace's on-disk footprint in bytes.
func (ns NamespaceSize) TotalPages() int {
	return ns.Pages + ns.OverflowPages
}

// NamespaceSize walks the namespace's B-tree and returns its physical size.
// Read-only; safe to call concurrently with the writer goroutine.
func (tx *ReadTx) NamespaceSize(ns *Namespace) (NamespaceSize, error) {
	if tx.closed {
		return NamespaceSize{}, ErrTxClosed
	}
	bt := &btree{
		pager:       tx.pager,
		cache:       tx.cache,
		rootPage:    ns.rootPage,
		walMaxFrame: tx.walHdr.mxFrame,
		writable:    tx.writable,
	}
	var res NamespaceSize
	if err := bt.walkSize(ns.rootPage, 0, &res); err != nil {
		return NamespaceSize{}, err
	}
	return res, nil
}

// walkSize recursively accumulates page and payload counts for the subtree
// rooted at pgno. depth guards against circular page references.
func (bt *btree) walkSize(pgno uint32, depth int, res *NamespaceSize) error {
	if depth > btCursorMaxDepth {
		return ErrCorrupt
	}
	pg, err := bt.getPage(pgno)
	if err != nil {
		return err
	}
	res.Pages++
	n := int(pg.header.cellCount)
	usable := bt.usablePageSize()

	if pg.header.isLeaf() {
		for i := range n {
			off, oErr := pg.getCellOffsetSafe(i)
			if oErr != nil {
				bt.pager.releasePage(pg)
				return oErr
			}
			c, _, pErr := parseLeafCellWithSize(pg.data, int(off), usable)
			if pErr != nil {
				bt.pager.releasePage(pg)
				return pErr
			}
			keyLen, valLen, dErr := leafCellPayloadLen(pg.data, int(off))
			if dErr != nil {
				bt.pager.releasePage(pg)
				return dErr
			}
			res.Entries++
			res.PayloadBytes += keyLen + valLen
			if c.overflowPg != 0 {
				ovf, ovErr := bt.countOverflowChain(c.overflowPg)
				if ovErr != nil {
					bt.pager.releasePage(pg)
					return ovErr
				}
				res.OverflowPages += ovf
			}
		}
		bt.pager.releasePage(pg)
		return nil
	}

	// Interior page: recurse into each child, then the right-most child.
	for i := range n {
		off, oErr := pg.getCellOffsetSafe(i)
		if oErr != nil {
			bt.pager.releasePage(pg)
			return oErr
		}
		c, _, pErr := parseInteriorCell(pg.data, int(off), usable)
		if pErr != nil {
			bt.pager.releasePage(pg)
			return pErr
		}
		if c.overflowPg != 0 {
			ovf, ovErr := bt.countOverflowChain(c.overflowPg)
			if ovErr != nil {
				bt.pager.releasePage(pg)
				return ovErr
			}
			res.OverflowPages += ovf
		}
		if cErr := bt.walkSize(c.leftChild, depth+1, res); cErr != nil {
			bt.pager.releasePage(pg)
			return cErr
		}
	}
	rightChild := pg.header.rightChild
	bt.pager.releasePage(pg)
	return bt.walkSize(rightChild, depth+1, res)
}

// leafCellPayloadLen decodes the full key and value lengths from a leaf cell.
// For overflow cells these are the full logical lengths, not the on-page
// portions.
func leafCellPayloadLen(data []byte, offset int) (keyLen, valLen int, err error) {
	if offset >= len(data) {
		return 0, 0, ErrCorrupt
	}
	kl, n, e := getVarintSafe(data[offset:])
	if e != nil {
		return 0, 0, ErrCorrupt
	}
	if offset+n >= len(data) {
		return 0, 0, ErrCorrupt
	}
	vl, _, e := getVarintSafe(data[offset+n:])
	if e != nil {
		return 0, 0, ErrCorrupt
	}
	if int64(kl) < 0 || int64(vl) < 0 || kl > maxPayloadAlloc || vl > maxPayloadAlloc {
		return 0, 0, ErrCorrupt
	}
	return int(kl), int(vl), nil
}

// countOverflowChain counts the pages in an overflow chain starting at
// firstPgno by following each page's 4-byte next pointer.
func (bt *btree) countOverflowChain(firstPgno uint32) (int, error) {
	count := 0
	pgno := firstPgno
	// Snapshot bound, not pager.dbSize: a read-only process never refreshes
	// pager.dbSize, so chains through pages a peer process allocated after we
	// opened would be falsely rejected as corrupt. See readerDbSizeBound.
	dbSize := bt.pager.readerDbSizeBound(bt.cache)
	for pgno != 0 {
		if pgno < 2 || pgno > dbSize {
			return 0, ErrCorrupt
		}
		count++
		// A chain can never be longer than the database; anything longer
		// indicates a circular reference.
		if uint32(count) > dbSize {
			return 0, ErrCorrupt
		}
		pg, err := bt.getPage(pgno)
		if err != nil {
			return 0, err
		}
		pgno = binary.BigEndian.Uint32(pg.data[0:4])
		bt.pager.releasePage(pg)
	}
	return count, nil
}
