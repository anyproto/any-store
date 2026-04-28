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
//   - No hash table: Go map[uint32]*page instead of apHash[] (drift #2)
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
	pages    map[uint32]*page // pgno -> page
	maxPages int              // maximum number of cached pages
	pageSize int              // size of each page in bytes
	useSlab  bool             // resolved once at creation; true when slab allocator is active

	// purgeable controls whether the cache can evict pages.
	// When false (InMemory databases), pages are never evicted and the
	// cache can grow beyond maxPages. Matches SQLite's pcache1.bPurgeable.
	purgeable bool

	// LRU list for clean pages (dirty pages are not evicted)
	lruHead      *page
	lruTail      *page
	nRecyclable  int // unpinned clean pages in LRU; matches SQLite pcache1.c:197 nRecyclable

	// Dirty page list (doubly-linked, head = MRU, tail = LRU for spill)
	dirtyHead *page
	dirtyTail *page // oldest dirty page; spill victim search starts here
	nDirty    int

	// dataVersion and walMaxFrame together identify the DB snapshot for which
	// this cache's pages are valid. On reuse from the pool, if EITHER value
	// differs from the current transaction, the cache is cleared.
	//
	// walMaxFrame alone is insufficient because it can wrap after checkpoint
	// restart (ABA problem). dataVersion alone is insufficient because of a
	// TOCTOU race: the WAL mxFrame is updated inside pager.commit() but
	// dataVersion is incremented afterward. A reader starting between these
	// two points would see the new walMaxFrame but old dataVersion, matching
	// a stale cache. Checking both eliminates both failure modes.
	//
	// Matches SQLite pager.c:3246-3267 (pagerBeginReadTransaction — pager_reset
	// only if change-counter changed) and pPager->iDataVersion (pager.c:1776).
	dataVersion  uint64
	walMaxFrame  uint32

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

func newPcache(pageSize, maxPages int, purgeable bool) *pcache {
	if maxPages <= 0 {
		maxPages = defaultCacheSize
	}
	return &pcache{
		pages:     make(map[uint32]*page),
		maxPages:  maxPages,
		pageSize:  pageSize,
		purgeable: purgeable,
		// useSlab defaults to false (sync.Pool mode). Callers that want slab
		// mode (btree.Open with SlabPages > 0) override this after creation.
	}
}

// fetch retrieves a page from the cache, or returns nil if not cached.
func (pc *pcache) fetch(pgno uint32) *page {
	p := pc.pages[pgno]
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
func (pc *pcache) create(pgno uint32, createFlag int) *page {
	if p := pc.pages[pgno]; p != nil {
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
		nPinned := len(pc.pages) - pc.nRecyclable
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
		for len(pc.pages) >= pc.maxPages && pc.nRecyclable > 0 {
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
		// Merges SQLite's two-phase sqlite3PcacheFetch + sqlite3PcacheFetchStress
		// (pcache.c:403-490) into a single call. Admission control (step 3),
		// eviction (step 4), stress callback, and allocation (step 5) are all
		// handled inline. See "Known Drifts in Page Cache" in docs/btree/NOTES.md.
		spill := pc.szSpill
		if spill == 0 {
			spill = pc.maxPages
		}
		if len(pc.pages) >= spill && pc.xStress != nil {
			victim := pc.findSpillVictim()
			if victim != nil {
				// DRIFT from SQLite: we ignore the xStress error here because
				// create() has no error return. SQLite's FetchStress returns
				// the error but only for OOM/non-BUSY cases. The pagerStress
				// callback calls pagerError() on failure to transition the
				// pager to error state, so the error is not silently lost.
				pc.xStress(victim)
				// After stress callback, victim should be clean. Retry eviction.
				for len(pc.pages) >= pc.maxPages && pc.nRecyclable > 0 {
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
	pc.pages[pgno] = p
	return p
}

// resetPage initializes a page for use with the given pgno.
// Clears data buffer and resets all fields. Called from create() after
// obtaining a page struct from pFree, initBulk, or heap allocation.
// Consolidates the page initialization that was previously duplicated
// in three code paths within create().
func (pc *pcache) resetPage(p *page, pgno uint32) {
	clear(p.data)
	p.pgno = pgno
	p.pinCount = 1
	p.dirty = false
	p.uncached = false
	p.next = nil
	p.prev = nil
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
func (pc *pcache) release(p *page) {
	p.pinCount--
	if p.pinCount <= 0 {
		p.pinCount = 0
		// Only add to LRU if the page is still tracked in pcache.pages.
		// After a stress spill + eviction, the page may no longer be in
		// pcache.pages. Adding such "ghost" pages to the LRU would cause
		// evictOne to loop without reducing len(pages).
		if p.dirty {
			if pc.pages[p.pgno] == p {
				pc.dirtyMoveToFront(p)
			}
		} else if pc.purgeable && pc.pages[p.pgno] == p {
			// Non-purgeable caches (InMemory) skip LRU entirely — pages are
			// never evicted. Matches SQLite pcache.c:265-271 (pcacheUnpin is
			// a no-op for non-purgeable caches).
			//
			// Overfull check: if cache grew beyond maxPages (e.g. from hard
			// creates during a transaction), discard the page immediately
			// rather than adding to LRU. Matches SQLite pcache1Unpin
			// (pcache1.c:1094): pGroup->nPurgeable > pGroup->nMaxPage.
			if len(pc.pages) > pc.maxPages {
				delete(pc.pages, p.pgno)
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
		if p.pinCount == 0 {
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

	for _, p := range pc.pages {
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
	}
	clear(pc.pages)
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
	for _, p := range pc.pages {
		freePageBuffer(p.data, pc.useSlab)
		p.data = nil
	}
	for _, p := range pc.pFree {
		freePageBuffer(p.data, pc.useSlab)
		p.data = nil
	}
	pc.pFree = nil
	pc.bulkInit = false
	clear(pc.pages)
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
	p := pc.pages[pgno]
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
	delete(pc.pages, pgno)
	pc.returnPageBuffer(p)
}

// truncate removes all pages with pgno > maxPage. Bulk-local pages go to
// pFree; non-bulk pages return to the slab/pool.
func (pc *pcache) truncate(maxPage uint32) {
	for pgno, p := range pc.pages {
		if pgno > maxPage {
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
			delete(pc.pages, pgno)
			pc.returnPageBuffer(p)
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
	delete(pc.pages, p.pgno)
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
