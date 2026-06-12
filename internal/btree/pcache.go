package btree

// pcache implements a page cache with LRU eviction, modeled after SQLite's
// pcache1.c. It maps page numbers to in-memory page objects and manages
// dirty page tracking, admission control, and buffer recycling through the
// global slab allocator (page_slab.go).
//
// Each pcache instance is owned by a single goroutine (writer or reader),
// so no mutex is needed. This matches SQLite's per-connection page cache model.
//
// Drifts from SQLite (see docs/btree/NOTES.md section 9 for full table):
//   - No PGroup: no cross-cache page stealing; each cache isolated (drift #1)
//   - No circular LRU: doubly-linked list with head/tail pointers (drift #3)
//   - dirtyTail pointer for O(1) spill victim (replaces pSynced, drift #19)
//   - No PgHdr/PgHdr1 split: single page struct (drift #5)
//   - No pcache2 plugin interface: direct implementation (drift #6)
//   - createFlag=0 dropped: fetch() handles lookup-only (drift #13)
//   - Max page check per-cache + global slab pressure (drift #14)

const defaultCacheSize = 5000
const defaultMaxReaders = 4

// pcache is the page cache.
type pcache struct {
	// apHash is a chained hash table mapping pgno -> *page, ported from SQLite's
	// PCache1.apHash (pcache1.c:200). A page is "in this cache" iff it is reachable
	// from apHash[pgno & (len(apHash)-1)] via page.hashNext. len(apHash) is always a
	// power of two so the bucket index is a mask, not a modulo. This replaces the
	// former Go map[uint32]*page and the two hot-path Go-map lookups it cost per
	// page touched (fetch + the release ghost-page re-probe). Closes NOTES.md §9
	// drift #2.
	apHash   []*page // hash buckets; head of each chain linked via page.hashNext
	nPage    int     // number of pages currently in apHash (was len(pc.pages))
	maxPages int     // maximum number of cached pages
	pageSize int     // size of each page in bytes
	useSlab  bool    // resolved once at creation; true when slab allocator is active

	// purgeable controls whether the cache can evict pages.
	// When false (InMemory databases), pages are never evicted and the
	// cache can grow beyond maxPages. Matches SQLite's pcache1.bPurgeable.
	purgeable bool

	// LRU list for clean pages (dirty pages are not evicted)
	lruHead     *page
	lruTail     *page
	nRecyclable int // unpinned clean pages in LRU; matches SQLite pcache1.c:197 nRecyclable

	// Dirty page list (doubly-linked, head = MRU, tail = LRU for spill)
	dirtyHead *page
	dirtyTail *page // oldest dirty page; spill victim search starts here
	nDirty    int

	// dataVersion and walHdr together identify the DB snapshot for which
	// this cache's pages are valid. On reuse from the pool, if EITHER value
	// differs from the current transaction, the cache is cleared.
	//
	// walHdr is the full WAL-index header snapshot, not just mxFrame:
	// mxFrame alone wraps after a checkpoint restart (ABA problem), and a
	// PEER PROCESS's restart never bumps this process's dataVersion, so the
	// salts (re-randomized on every restart) are the only signal that frame
	// numbers were recycled cross-process. dataVersion alone is insufficient
	// because of a TOCTOU race: the WAL mxFrame is updated inside
	// pager.commit() but dataVersion is incremented afterward. A reader
	// starting between these two points would see the new header but old
	// dataVersion, matching a stale cache. Checking both eliminates both
	// failure modes. (In-process mode synthesizes a salt-less header, where
	// dataVersion covers every local commit by itself.)
	//
	// Matches SQLite pager.c:3246-3267 (pagerBeginReadTransaction — pager_reset
	// only if change-counter changed) and pPager->iDataVersion (pager.c:1776).
	dataVersion uint64
	walHdr      WalIndexHdr
	walMaxFrame uint32

	// minFrame is the snapshot's WAL lookup floor (nBackfill+1 for slot 1-4
	// readers, walMaxFrame+1 for slot-0 readers), captured at BeginRead like
	// SQLite's pWal->minFrame (wal.c:3239) and refreshed on EVERY BeginRead
	// (not just on clear): a reader re-entering on slot 0 must raise the
	// floor so a peer's WAL restart cannot serve new-generation frames into
	// this snapshot. 0 means "fall back to the live frontier" (writer /
	// uncached paths via pager.readerMinFrame).
	minFrame uint32

	// dbSize is the database size in pages for the snapshot this cache serves
	// (WalIndexHdr.nPage, captured cross-process at BeginRead). Reader page/
	// overflow bound checks use this instead of the process-global
	// pager.dbSize — which only the writer refreshes — so a live reader
	// accepts pages a peer allocated after this process opened. Set in
	// lockstep with walMaxFrame at every snapshot-establish site.
	//
	// Mirrors SQLite's per-connection pPager->dbSize, recomputed each read
	// transaction from sqlite3WalDbsize() == pWal->hdr.nPage (wal.c:3672,
	// pager.c:5448 pagerPagecount). 0 means "fall back to pager.dbSize"
	// (in-process mode / empty WAL — matching pagerPagecount's file-size
	// fallback at pager.c:3300).
	dbSize uint32

	// pFree is a per-cache list of reusable page structs with data buffers.
	// In non-slab mode, initBulk() pre-allocates up to 20 pages from the heap
	// (marked isBulkLocal=true). In slab mode, initBulk is a no-op (SQLite
	// disables bulk init when SQLITE_CONFIG_PAGECACHE is set). Pages also
	// accumulate in pFree during clear() for reuse across transactions.
	// Matches SQLite pcache1.c:201 (pFree), pcache1.c:297-330 (pcache1InitBulk),
	// pcache1.c:434-438 (tries pFree first), pcache1.c:470-475 (isBulkLocal→pFree).
	pFree    []*page
	bulkInit bool // true after initBulk() has been called

	// xStress is invoked when the cache is full and all clean pages are
	// exhausted. It should write the dirty page to WAL and call makeClean.
	// Modeled after SQLite's xStress in pcache.c.
	xStress func(p *page) error

	// szSpill is the spill threshold: xStress fires when page count exceeds
	// szSpill. 0 means use maxPages as the threshold.
	szSpill int

	// BEGIN ENCRYPTION
	// codecScratch is a page-sized scratch buffer used by the reader path
	// (pager.getPageReader + wal.readFrame via getPageReader) to hold
	// plaintext while the encrypted page is decrypted. Allocated lazily on
	// first use when a codec is installed. Safe as a single shared buffer
	// per cache because read transactions are single-goroutine: one tx
	// owns one pcache and issues reads sequentially.
	codecScratch []byte
	// codecAEAD holds the nonce/AAD scratch for reader-side codec calls.
	// Same threading rules as codecScratch (per-tx pcache is
	// single-goroutine). Embedded by value so the slice headers returned
	// by aad[:]/nonce[:] point at heap memory owned by the pcache,
	// avoiding per-call escape into the cipher.AEAD interface.
	codecAEAD aeadScratch
	// END ENCRYPTION
}

// DRIFT: newPcache pre-sizes hash to capacity; SQLite always seeds exactly 256 buckets See docs/btree/NOTES.md#drift-130-newpcache-hash-table-pre-sized-to-capacity
func newPcache(pageSize, maxPages int, purgeable bool) *pcache {
	if maxPages <= 0 {
		maxPages = defaultCacheSize
	}
	return &pcache{
		// Seed apHash from maxPages rounded up to a power of two (floor 256, the
		// same floor pcache1ResizeHash uses, pcache1.c:543). Sizing the table to
		// the configured cache up front avoids the early doublings a cache would
		// otherwise pay while filling. The hash still grows via resizeHash if a
		// hard-create burst pushes nPage past nHash (load factor 1.0).
		apHash:    make([]*page, hashSizeFor(maxPages)),
		maxPages:  maxPages,
		pageSize:  pageSize,
		purgeable: purgeable,
		// useSlab defaults to false (sync.Pool mode). Callers that want slab
		// mode (btree.Open with SlabPages > 0) override this after creation.
	}
}

// minHashSize is the smallest apHash table size, matching SQLite's
// pcache1ResizeHash floor (pcache1.c:543: "if( nNew<256 ) nNew = 256").
const minHashSize = 256

// hashSizeFor returns the smallest power of two >= n that is also >= minHashSize.
// Used to seed apHash so pgno & (nHash-1) is a valid bucket mask.
func hashSizeFor(n int) int {
	sz := minHashSize
	for sz < n {
		sz <<= 1
	}
	return sz
}

// hashFind returns the cached page with the given pgno, or nil if not cached.
// It is the read-only half of the bucket probe shared by fetch/create — a single
// chained-bucket walk, with no LRU or pin side effects.
// Matches SQLite pcache1FetchNoMutex step 1 (pcache1.c:1009-1010).
func (pc *pcache) hashFind(pgno uint32) *page {
	for p := pc.apHash[pgno&uint32(len(pc.apHash)-1)]; p != nil; p = p.hashNext {
		if p.pgno == pgno {
			return p
		}
	}
	return nil
}

// hashInsert links p at the head of its bucket chain, marks it in-cache, and
// bumps nPage. The caller must have established (via hashFind) that pgno is not
// already present. Resizes the table first when the load factor would reach 1.0,
// matching SQLite's pre-insert check (pcache1.c:894: "if( nPage>=nHash )
// pcache1ResizeHash").
func (pc *pcache) hashInsert(p *page) {
	if pc.nPage >= len(pc.apHash) {
		pc.resizeHash()
	}
	h := p.pgno & uint32(len(pc.apHash)-1)
	p.hashNext = pc.apHash[h]
	pc.apHash[h] = p
	p.inCache = true
	pc.nPage++
}

// hashRemove unlinks p from its bucket chain, clears its in-cache state, and
// decrements nPage. After this returns p.inCache is false and p.hashNext is nil,
// so a later release() will not re-add it to the LRU (the ghost-page guard).
// Matches SQLite pcache1RemoveFromHash (pcache1.c:601-613).
func (pc *pcache) hashRemove(p *page) {
	h := p.pgno & uint32(len(pc.apHash)-1)
	pp := &pc.apHash[h]
	for *pp != p {
		pp = &(*pp).hashNext
	}
	*pp = p.hashNext
	p.hashNext = nil
	p.inCache = false
	pc.nPage--
}

// resizeHash doubles the bucket count and rehashes every page into the new
// table. Triggered when nPage would reach nHash (load factor 1.0). Power-of-two
// sizing keeps pgno & (nHash-1) a valid mask. Matches SQLite pcache1ResizeHash
// (pcache1.c:535-567).
func (pc *pcache) resizeHash() {
	nNew := len(pc.apHash) * 2
	if nNew < minHashSize {
		nNew = minHashSize
	}
	apNew := make([]*page, nNew)
	mask := uint32(nNew - 1)
	for _, head := range pc.apHash {
		for p := head; p != nil; {
			next := p.hashNext
			h := p.pgno & mask
			p.hashNext = apNew[h]
			apNew[h] = p
			p = next
		}
	}
	pc.apHash = apNew
}

// fetch retrieves a page from the cache, or returns nil if not cached.
// The lookup is a single chained-bucket walk (no Go-map hash), matching
// SQLite pcache1FetchNoMutex (pcache1.c:1009-1018): on a hit, pin the page
// and, if it is clean, splice it out of the LRU (pcache1PinPage).
func (pc *pcache) fetch(pgno uint32) *page {
	p := pc.hashFind(pgno)
	if p != nil {
		p.pinCount++
		if !p.dirty {
			pc.lruRemove(p)
		}
	}
	return p
}

// initBulk pre-allocates page structs with data buffers from the heap.
// Called once on first create(). Matches SQLite pcache1.c:297-330 (pcache1InitBulk).
//
// SQLite disables bulk init when a slab (SQLITE_CONFIG_PAGECACHE) is configured:
// pcache1Init sets nInitPage=0 when pPage!=0 (pcache1.c:730-737). In slab mode,
// all allocations go through pcache1Alloc (slab→heap fallback) instead.
//
// In non-slab mode, bulk allocation uses the heap (sync.Pool), matching SQLite's
// sqlite3Malloc call. Pages are marked isBulkLocal=true so that pcache1FreePage
// routes them to pCache->pFree (local reuse) rather than pcache1Free (slab/heap).
// The default nInitPage is SQLITE_DEFAULT_PCACHE_INITSZ=20, capped at nMax
// (pcache1.c:304-310).
func (pc *pcache) initBulk() {
	pc.bulkInit = true
	// SQLite disables bulk init when a slab is configured (pcache1.c:730-737:
	// nInitPage=0 when pPage!=0). All page buffers come from the slab instead.
	if pc.useSlab {
		return
	}
	nBulk := pc.maxPages
	// Match SQLITE_DEFAULT_PCACHE_INITSZ (20) and nMax cap (pcache1.c:309-310).
	if nBulk > 20 {
		nBulk = 20
	}
	// SQLite: "Do not bother with a bulk allocation if the cache size very small"
	// (pcache1.c:302). nMax < 3 → skip.
	if nBulk < 3 {
		return
	}
	pc.pFree = make([]*page, nBulk)
	for i := range nBulk {
		pc.pFree[i] = &page{
			// Heap allocation (useSlab=false), matching SQLite's sqlite3Malloc
			// in pcache1InitBulk. The slab is reserved for individual allocations
			// in create() step 5 (pcache1AllocPage → pcache1Alloc).
			data:        allocPageBuffer(pc.pageSize, false),
			cache:       pc,
			isBulkLocal: true, // matches SQLite pX->isBulkLocal = 1 (pcache1.c:321)
		}
	}
}

// create allocates a new page in the cache and returns it pinned.
// createFlag controls admission: 1=soft (may return nil under pressure),
// 2=hard (always allocates). Matches SQLite pcache1.c:881-892 step 3.
// If the cache is full, it evicts clean pages first. If no clean pages
// are available and xStress is set, invokes the stress callback to spill
// a dirty page, making it clean and evictable.
// DRIFT: pcache recycle/spill thresholds off-by-one ('>=' vs C 'nPage+1>=nMax' / strict '>') See docs/btree/NOTES.md#drift-127-pcache-recycle-and-spill-thresholds-off-by-one
// DRIFT: create() merges SQLite's two-phase Fetch + FetchStress (soft create then spill+hard retry) See docs/btree/NOTES.md#old-drift-merged-fetch-fetchstress
// DRIFT: pcache.create ignores xStress error (no error return); C FetchStress propagates non-BUSY r See docs/btree/NOTES.md#old-drift-pcache-create-drops-xstress-error
// DRIFT: create() keeps evicted victim as recycled & reuses its buffer for both reader/writer cache See docs/btree/NOTES.md#old-drift-pcache-buffer-reuse-on-eviction
// DRIFT: create() takes createFlag directly; no eCreate auto-select (readers=1 soft, writers=2 hard See docs/btree/NOTES.md#old-drift-no-ecreate-state-machine
func (pc *pcache) create(pgno uint32, createFlag int) *page {
	if p := pc.hashFind(pgno); p != nil {
		p.pinCount++
		if !p.dirty {
			pc.lruRemove(p)
		}
		return p
	}

	// Step 3: Admission control — refuse soft creates when thrashing.
	// Matches SQLite pcache1.c:886-891 (Mode 1 / separateCache):
	//   - nPinned >= n90pct: per-cache thrashing guard
	//   - underMemoryPressure && nRecyclable < nPinned: slab exhausted and
	//     most pages are actively pinned — adding more would overflow the slab
	//     with little chance of recycling. In Mode 1, both checks are per-cache
	//     (private PGroup), which is exactly our model.
	//
	// createFlag==1 (soft, readers): may return nil.
	// createFlag==2 (hard, writers/stress): always proceeds.
	if createFlag == 1 && pc.purgeable {
		nPinned := pc.nPage - pc.nRecyclable
		if nPinned >= pc.maxPages*9/10 {
			return nil
		}
		if pc.useSlab && globalPageSlab.UnderPressure() && pc.nRecyclable < nPinned {
			return nil
		}
	}

	// Step 4: Recycle LRU pages if cache is full OR slab is under pressure.
	// Matches SQLite pcache1.c:898-900 (Mode 1):
	//   (pCache->nPage+1>=pCache->nMax) || pcache1UnderMemoryPressure(pCache)
	// Under slab pressure, recycle even when the cache isn't full — this is
	// how SQLite bounds total memory across many caches sharing a single slab.
	// The evicted page's buffer is reused directly (net-zero slab allocation).
	var recycled *page
	if pc.purgeable {
		// First, if cache is over maxPages, evict down to maxPages.
		for pc.nPage >= pc.maxPages && pc.nRecyclable > 0 {
			evicted := pc.evictOne()
			if evicted != nil {
				if recycled != nil {
					pc.returnPageBuffer(recycled)
				}
				recycled = evicted
			}
		}
		// Under slab pressure, recycle one more LRU page for buffer reuse
		// even if cache is below maxPages. Matches SQLite step 4 (pcache1.c:900):
		//   (nPage+1>=nMax) || pcache1UnderMemoryPressure
		// This prevents overflow allocations when many caches share a slab.
		if recycled == nil && pc.useSlab && globalPageSlab.UnderPressure() && pc.nRecyclable > 0 {
			recycled = pc.evictOne()
		}

		// If still full and stress callback available, try to spill a dirty page.
		spill := pc.szSpill
		if spill == 0 {
			spill = pc.maxPages
		}
		if pc.nPage >= spill && pc.xStress != nil {
			victim := pc.findSpillVictim()
			if victim != nil {
				pc.xStress(victim)
				// After stress callback, victim should be clean. Retry eviction.
				for pc.nPage >= pc.maxPages && pc.nRecyclable > 0 {
					evicted := pc.evictOne()
					if evicted != nil && recycled == nil {
						recycled = evicted
					} else if evicted != nil {
						pc.returnPageBuffer(evicted)
					}
				}
			}
		}
	}

	// Allocate a page struct: try recycled (direct reuse from step 4), then
	// pFree, then bulk init, then allocPageBuffer (slab or sync.Pool).
	// Matches SQLite pcache1.c:434-438 (pcache1AllocPage tries pFree first)
	// and pcache1.c:900-914 (step 4 reuses evicted page directly).
	var p *page
	if recycled != nil {
		p = recycled
	} else if n := len(pc.pFree); n > 0 {
		p = pc.pFree[n-1]
		pc.pFree = pc.pFree[:n-1]
	} else {
		if !pc.bulkInit {
			pc.initBulk()
			if n := len(pc.pFree); n > 0 {
				p = pc.pFree[n-1]
				pc.pFree = pc.pFree[:n-1]
			}
		}
		if p == nil {
			p = &page{
				data:  allocPageBuffer(pc.pageSize, pc.useSlab),
				cache: pc,
			}
		}
	}
	pc.resetPage(p, pgno)
	// Link into the hash (sets p.inCache, bumps nPage, may resize). Replaces the
	// former pc.pages[pgno] = p. Matches SQLite where a freshly allocated page is
	// linked into apHash by pcache1FetchStage2 (pcache1.c).
	pc.hashInsert(p)
	return p
}

// resetPage initializes a page for use with the given pgno.
// Clears data buffer and resets all fields. Called from create() after
// obtaining a page struct from pFree, initBulk, or heap allocation.
// Consolidates the page initialization that was previously duplicated
// in three code paths within create().
// DRIFT: resetPage zeroes page buffer on every creation; SQLite never zeroes at fetch/recycle See docs/btree/NOTES.md#drift-129-resetpage-zeroes-buffer-on-every-page-creation
func (pc *pcache) resetPage(p *page, pgno uint32) {
	clear(p.data)
	p.pgno = pgno
	p.pinCount = 1
	p.dirty = false
	p.uncached = false
	p.next = nil
	p.prev = nil
	// Reset hash-chain membership; hashInsert (called right after) re-establishes
	// it. A page reused from pFree/evictOne already had these cleared by
	// hashRemove, but reset defensively so a struct from any source is clean.
	p.hashNext = nil
	p.inCache = false
	p.header = pageHeader{}
}

// release unpins a page. If the page is clean, it goes to the LRU list.
// If the page is dirty, it moves to the front of the dirty list (MRU position).
// Matches SQLite pcache.c:558 (pcacheManageDirtyList PCACHE_DIRTYLIST_FRONT on unpin).
//
// When the cache is overfull (len(pages) > maxPages), clean pages are
// immediately discarded instead of going to the LRU. Matches SQLite
// pcache1Unpin (pcache1.c:1094-1095, Mode 1 separateCache):
//
//	if( reuseUnlikely || pGroup->nPurgeable>pGroup->nMaxPage ){
//	    pcache1RemoveFromHash(pPage, 1);
//	}
//
// In Mode 1, pGroup is per-cache, so nPurgeable>nMaxPage is equivalent
// to nPage>nMax. This fires when createFlag=2 (hard create) lets the
// cache grow beyond nMax during a transaction — the excess pages are
// shed on release instead of accumulating in the LRU.
// DRIFT: release() lacks SQLite's reuseUnlikely flag; only matches the overfull nPage>maxPages disc See docs/btree/NOTES.md#old-drift-release-no-reuse-unlikely-hint
// DRIFT: release()/makeClean skip LRU for non-purgeable (InMemory) caches, matching SQLite pcacheUn See docs/btree/NOTES.md#old-drift-non-purgeable-skip-lru
func (pc *pcache) release(p *page) {
	p.pinCount--
	if p.pinCount <= 0 {
		p.pinCount = 0
		// Only manage the LRU/dirty list if the page is still in the cache.
		// After a stress spill + eviction — or a discard/truncate that removed
		// the page while a caller still held it pinned — the page is no longer
		// in apHash and p.inCache is false. Adding such a "ghost" page to the
		// LRU would make evictOne loop without reducing nPage (and leak the
		// page's already-returned buffer). p.inCache replaces the former
		// pc.pages[p.pgno]==p re-probe with a field read, so release does no
		// hash work on the hot path. Matches SQLite pcache1Unpin (pcache1.c:1076),
		// which adds to the LRU unconditionally because its invariants forbid
		// removing a pinned page from the hash; v2 allows that, so we gate on
		// inCache.
		if p.dirty {
			if p.inCache {
				pc.dirtyMoveToFront(p)
			}
		} else if pc.purgeable && p.inCache {
			// Non-purgeable caches (InMemory) skip LRU entirely — pages are
			// never evicted. Matches SQLite pcache.c:265-271 (pcacheUnpin is
			// a no-op for non-purgeable caches).
			//
			// Overfull check: if cache grew beyond maxPages (e.g. from hard
			// creates during a transaction), discard the page immediately
			// rather than adding to LRU. Matches SQLite pcache1Unpin
			// (pcache1.c:1094): pGroup->nPurgeable > pGroup->nMaxPage.
			if pc.nPage > pc.maxPages {
				pc.hashRemove(p)
				pc.returnPageBuffer(p)
			} else {
				pc.lruPrepend(p)
			}
		}
	}
}

// makeDirty marks a page as dirty.
func (pc *pcache) makeDirty(p *page) {
	if !p.dirty {
		p.dirty = true
		pc.lruRemove(p)
		p.next = pc.dirtyHead
		p.prev = nil
		if pc.dirtyHead != nil {
			pc.dirtyHead.prev = p
		} else {
			pc.dirtyTail = p
		}
		pc.dirtyHead = p
		pc.nDirty++
	}
}

// makeClean marks a page as clean (after writing to disk).
func (pc *pcache) makeClean(p *page) {
	if p.dirty {
		p.dirty = false
		// Remove from dirty list
		if p.prev != nil {
			p.prev.next = p.next
		} else {
			pc.dirtyHead = p.next
		}
		if p.next != nil {
			p.next.prev = p.prev
		} else {
			pc.dirtyTail = p.prev
		}
		p.next = nil
		p.prev = nil
		pc.nDirty--
		// Non-purgeable caches (InMemory) skip the LRU entirely — pages are
		// never evicted, so a cleaned unpinned page is left off the LRU.
		// Mirrors release()'s guard above and matches SQLite
		// sqlite3PcacheMakeClean (pcache.c:622-624), whose trailing
		// pcacheUnpin (pcache.c:265-271) is a no-op for non-purgeable caches.
		if p.pinCount == 0 && pc.purgeable {
			pc.lruPrepend(p)
		}
	}
}

// dirtyMoveToFront moves a dirty page to the front of the dirty list.
// Called when an unpinned dirty page is released, so recently-released dirty
// pages are at the front and oldest dirty pages are at the back (spill victims).
// Matches SQLite pcache.c:558 (PCACHE_DIRTYLIST_FRONT).
func (pc *pcache) dirtyMoveToFront(p *page) {
	if !p.dirty || pc.dirtyHead == p {
		return
	}
	// Unlink from current position
	if p.prev != nil {
		p.prev.next = p.next
	}
	if p.next != nil {
		p.next.prev = p.prev
	} else {
		pc.dirtyTail = p.prev
	}
	// Insert at head
	p.prev = nil
	p.next = pc.dirtyHead
	if pc.dirtyHead != nil {
		pc.dirtyHead.prev = p
	}
	pc.dirtyHead = p
}

// dirtyPages returns all dirty pages in a new slice.
func (pc *pcache) dirtyPages() []*page {
	return pc.appendDirtyPages(make([]*page, 0, pc.nDirty))
}

// appendDirtyPages appends all dirty pages to the provided slice and returns it.
// DRIFT: dirty list written to WAL unsorted (MRU->LRU); SQLite pgno-sorts. Harmless: WAL frames are See docs/btree/NOTES.md#old-drift-pcache-dirty-list-unsorted-wal-write
func (pc *pcache) appendDirtyPages(buf []*page) []*page {
	for p := pc.dirtyHead; p != nil; p = p.next {
		buf = append(buf, p)
	}
	return buf
}

// clear invalidates all cached pages. Clean pages that fit in the local
// pFree list are kept for buffer reuse by future create() calls. Excess
// buffers are returned via freePageBuffer so the GC can reclaim them.
//
// When the global slab is under pressure, ALL buffers are returned to
// the slab/pool instead of being hoarded locally. This prevents N caches
// from each holding maxPages buffers in pFree (N * maxPages * pageSize
// total), which would far exceed the slab budget. SQLite avoids this
// via PGroup-level recycling (Mode 2) or per-cache nMax gating (Mode 1);
// we use the slab pressure flag as the equivalent cross-cache signal.
//
// Used when the cache snapshot is invalidated but the cache object will be
// reused (e.g. persistent reader cache with changed dataVersion, or writer
// cache stale detection). For final disposal use destroy().
// DRIFT: pcache clear/truncate omit C's pgno==0 page-1 zero-and-retain (nRefSum>0) special case See docs/btree/NOTES.md#drift-126-pcache-truncate-and-clear-omit-page-1-zero-and-preserve-spec
func (pc *pcache) clear() {
	// Under slab pressure, return ALL existing pFree buffers to the pool
	// before adding new ones. This releases hoarded buffers from prior
	// clear() calls that accumulated when pressure wasn't active yet.
	if pc.useSlab && globalPageSlab.UnderPressure() {
		for _, p := range pc.pFree {
			freePageBuffer(p.data, pc.useSlab)
			p.data = nil
		}
		pc.pFree = pc.pFree[:0]
	}

	// Walk every bucket chain. hashRemove is not used here (we are tearing the
	// whole table down); instead we clear membership on each page as we route it,
	// then zero all buckets at the end. Save hashNext before clearing it because
	// pages routed to pFree are reused.
	for bi, head := range pc.apHash {
		for p := head; p != nil; {
			next := p.hashNext
			p.hashNext = nil
			p.inCache = false
			if pc.useSlab && globalPageSlab.UnderPressure() {
				// Slab mode under pressure: return all buffers to slab.
				// No isBulkLocal pages exist in slab mode (initBulk is skipped).
				freePageBuffer(p.data, pc.useSlab)
				p.data = nil
			} else if p.isBulkLocal {
				// Bulk-local pages always go to pFree (matches SQLite pcache1FreePage).
				// They were heap-allocated in initBulk and are never returned to slab/pool.
				pc.pFree = append(pc.pFree, p)
			} else if len(pc.pFree) < pc.maxPages {
				pc.pFree = append(pc.pFree, p)
			} else {
				freePageBuffer(p.data, pc.useSlab)
				p.data = nil
			}
			p = next
		}
		pc.apHash[bi] = nil
	}
	pc.nPage = 0
	pc.lruHead = nil
	pc.lruTail = nil
	pc.dirtyHead = nil
	pc.dirtyTail = nil
	pc.nRecyclable = 0
	pc.nDirty = 0
}

// destroy removes all pages and returns all data buffers to the slab/pool.
// Matches SQLite pcache1Destroy (pcache1.c:1169-1185): truncates all pages
// via pcache1FreePage, then frees the pBulk blocks via sqlite3_free.
// In Go, returning buffers to sync.Pool (non-slab) or slab achieves the same.
// Used when the cache will not be reused: temporary caches (getNamespace,
// listNamespaces, integrity check) and pager close/error paths.
func (pc *pcache) destroy() {
	for bi, head := range pc.apHash {
		for p := head; p != nil; {
			next := p.hashNext
			p.hashNext = nil
			p.inCache = false
			freePageBuffer(p.data, pc.useSlab)
			p.data = nil
			p = next
		}
		pc.apHash[bi] = nil
	}
	for _, p := range pc.pFree {
		freePageBuffer(p.data, pc.useSlab)
		p.data = nil
	}
	pc.pFree = nil
	pc.bulkInit = false
	pc.nPage = 0
	pc.lruHead = nil
	pc.lruTail = nil
	pc.dirtyHead = nil
	pc.dirtyTail = nil
	pc.nRecyclable = 0
	pc.nDirty = 0
}

// discard removes a specific page from the cache. Bulk-local pages go to
// pFree for local reuse; non-bulk pages return to the slab/pool.
// Matches SQLite pcache1FreePage (pcache1.c:470-482).
func (pc *pcache) discard(pgno uint32) {
	p := pc.hashFind(pgno)
	if p == nil {
		return
	}
	if p.dirty {
		if p.prev != nil {
			p.prev.next = p.next
		} else {
			pc.dirtyHead = p.next
		}
		if p.next != nil {
			p.next.prev = p.prev
		} else {
			pc.dirtyTail = p.prev
		}
		p.next = nil
		p.prev = nil
		pc.nDirty--
	} else {
		pc.lruRemove(p)
	}
	// hashRemove clears p.inCache, so if a caller still holds this page pinned
	// (discard can run on a pinned page), the eventual release() sees inCache
	// false and does not re-add the now-freed page to the LRU (ghost-page guard).
	pc.hashRemove(p)
	pc.returnPageBuffer(p)
}

// truncate removes all pages with pgno > maxPage. Bulk-local pages go to
// pFree; non-bulk pages return to the slab/pool. Pinned pages that match are
// removed too (hashRemove clears inCache so a later release won't re-LRU them).
// Walks each bucket chain in place via a pointer-to-link, mirroring SQLite
// pcache1TruncateUnsafe (pcache1.c:644-687).
// DRIFT: pcache clear/truncate omit C's pgno==0 page-1 zero-and-retain (nRefSum>0) special case See docs/btree/NOTES.md#drift-126-pcache-truncate-and-clear-omit-page-1-zero-and-preserve-spec
func (pc *pcache) truncate(maxPage uint32) {
	for bi := range pc.apHash {
		pp := &pc.apHash[bi]
		for *pp != nil {
			p := *pp
			if p.pgno > maxPage {
				// Unlink from the bucket chain in place (we already hold the
				// link pointer, so don't call hashRemove which would re-walk).
				*pp = p.hashNext
				p.hashNext = nil
				p.inCache = false
				pc.nPage--
				if p.dirty {
					if p.prev != nil {
						p.prev.next = p.next
					} else {
						pc.dirtyHead = p.next
					}
					if p.next != nil {
						p.next.prev = p.prev
					} else {
						pc.dirtyTail = p.prev
					}
					p.next = nil
					p.prev = nil
					pc.nDirty--
				} else {
					pc.lruRemove(p)
				}
				pc.returnPageBuffer(p)
			} else {
				pp = &p.hashNext
			}
		}
	}
}

// lruPrepend inserts a page at the HEAD of the LRU list (MRU position).
// Eviction happens from the TAIL (LRU position).
// Matches SQLite pcache1.c:1098-1101 (unpin inserts at pGroup->lru.pLruNext — HEAD).
func (pc *pcache) lruPrepend(p *page) {
	p.prev = nil
	p.next = pc.lruHead
	if pc.lruHead != nil {
		pc.lruHead.prev = p
	} else {
		pc.lruTail = p
	}
	pc.lruHead = p
	pc.nRecyclable++
}

func (pc *pcache) lruRemove(p *page) {
	// Not in LRU list — nothing to do.
	if p.prev == nil && p.next == nil && pc.lruHead != p {
		return
	}
	if p.prev != nil {
		p.prev.next = p.next
	} else {
		pc.lruHead = p.next
	}
	if p.next != nil {
		p.next.prev = p.prev
	} else {
		pc.lruTail = p.prev
	}
	p.next = nil
	p.prev = nil
	pc.nRecyclable--
}

// evictOne removes the page at the TAIL of the LRU list (least recently used)
// and returns it. Returns nil if the LRU list is empty.
// Matches SQLite pcache1.c:623-624 (evicts from pGroup->lru.pLruPrev — TAIL).
func (pc *pcache) evictOne() *page {
	if pc.lruTail == nil {
		return nil
	}
	p := pc.lruTail
	pc.lruTail = p.prev
	if pc.lruTail != nil {
		pc.lruTail.next = nil
	} else {
		pc.lruHead = nil
	}
	p.next = nil
	p.prev = nil
	pc.nRecyclable--
	pc.hashRemove(p)
	return p
}

// returnPageBuffer routes a freed page based on its allocation origin.
// Matches SQLite pcache1FreePage (pcache1.c:470-482):
//   - isBulkLocal pages go to pCache->pFree (local reuse, never to slab/pool)
//   - non-bulk pages go through pcache1Free (slab or sqlite3_free)
func (pc *pcache) returnPageBuffer(p *page) {
	if p.isBulkLocal {
		pc.pFree = append(pc.pFree, p)
	} else {
		freePageBuffer(p.data, pc.useSlab)
		p.data = nil
	}
}

// findSpillVictim returns the oldest unpinned dirty page, or nil if all dirty
// pages are pinned. Walks backward from dirtyTail for O(1) typical case
// (the oldest dirty page is usually unpinned).
// Matches SQLite pcache.c:463-469 (pDirtyTail search direction).
func (pc *pcache) findSpillVictim() *page {
	for p := pc.dirtyTail; p != nil; p = p.prev {
		if p.pinCount == 0 {
			return p
		}
	}
	return nil
}
