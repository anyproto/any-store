package btree

// pcache implements a page cache with LRU eviction, modeled after SQLite's
// pcache1.c. It maps page numbers to in-memory page objects and manages
// dirty page tracking, admission control, and buffer recycling through the
// global slab allocator (page_slab.go).
//
// Each pcache instance is owned by a single goroutine (writer or reader),
// so no mutex is needed. This matches SQLite's per-connection page cache model.
//
// Drifts from SQLite (see NOTES.md section 9 for full table):
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

	// pFree is a per-cache bulk pre-allocated list of page structs.
	// On first use, initBulk() allocates up to 100 page structs with data
	// buffers drawn from the global slab. Matches SQLite pcache1.c:201 (pFree),
	// pcache1.c:297-330 (pcache1InitBulk), pcache1.c:434-438 (tries pFree first).
	pFree    []*page
	bulkInit bool // true after initBulk() has been called

	// xStress is invoked when the cache is full and all clean pages are
	// exhausted. It should write the dirty page to WAL and call makeClean.
	// Modeled after SQLite's xStress in pcache.c.
	xStress func(p *page) error

	// szSpill is the spill threshold: xStress fires when page count exceeds
	// szSpill. 0 means use maxPages as the threshold.
	szSpill int
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

// initBulk pre-allocates page structs with data buffers from the global slab.
// Called once on first create(). Matches SQLite pcache1.c:297-330 (pcache1InitBulk).
// If the global slab is not initialized (e.g. unit tests using newPcache directly),
// this is a no-op and create() falls back to make([]byte, pageSize).
func (pc *pcache) initBulk() {
	pc.bulkInit = true
	if !globalPageSlab.Initialized(pc.pageSize) {
		return
	}
	nBulk := pc.maxPages
	if nBulk > 100 {
		nBulk = 100
	}
	if nBulk <= 0 {
		return
	}
	pc.pFree = make([]*page, nBulk)
	for i := range nBulk {
		pc.pFree[i] = &page{
			data:  globalPageSlab.Get(),
			cache: pc,
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
	// Implements one of SQLite's three step-3 guards (pcache1.c:886-891):
	// nPinned >= n90pct (per-cache thrashing).
	//
	// DRIFT from SQLite: the global slab-pressure guard
	// (pcache1UnderMemoryPressure + nRecyclable < nPinned, pcache1.c:889-891)
	// is intentionally omitted. SQLite's check uses PGroup-level nPurgeable
	// (pages across ALL caches), so a single cache with low recyclable doesn't
	// trigger it. In our isolated model (no PGroup, drift #1), each cache's
	// nRecyclable is checked independently — causing the condition to fire when
	// even 1 page is pinned. This prevents the reader cache from filling at
	// all, pushing every page through readTempPage → acquireTempPage →
	// globalPageSlab.Get() → overflow allocation. The reader cache is already
	// bounded by maxPages; once full, step 4 evicts before creating (net zero
	// memory), so per-cache admission control via the 90% guard is sufficient.
	//
	// createFlag==1 (soft, readers): may return nil.
	// createFlag==2 (hard, writers/stress): always proceeds.
	if createFlag == 1 && pc.purgeable {
		nPinned := len(pc.pages) - pc.nRecyclable
		if nPinned >= pc.maxPages*9/10 {
			return nil
		}
	}

	// Step 4: Evict clean pages if cache is full (skip for non-purgeable / InMemory caches).
	// Adapted from SQLite pcache1.c:897-914 (step 4 — SQLite reuses victim's buffer).
	//
	// Reader caches (xStress == nil): the last evicted page is kept for direct
	// buffer reuse by the allocation step below, eliminating the Put→Get slab
	// round-trip. This matches SQLite's step 4 (pcache1.c:900) which reuses the
	// victim's PgHdr1 + buffer directly for the new page. Intermediate evictions
	// (rare: only when multiple pages must be evicted) return buffers to slab.
	//
	// Writer caches (xStress != nil): evicted buffers are NOT touched because
	// pager.writePages may still reference the evicted page struct after spill;
	// writer buffers are returned in clear()/discard()/truncate() instead.
	slabOk := globalPageSlab.Initialized(pc.pageSize)
	var recycled *page
	if pc.purgeable {
		for len(pc.pages) >= pc.maxPages && pc.nRecyclable > 0 {
			evicted := pc.evictOne()
			if evicted != nil && pc.xStress == nil {
				// Keep last evicted page for direct reuse (SQLite step 4:
				// pcache1.c:900 reuses victim's PgHdr1 + buffer directly).
				// Return intermediate evictions to slab.
				if recycled != nil && slabOk {
					globalPageSlab.Put(recycled.data)
					recycled.data = nil
				}
				recycled = evicted
			}
		}

		// If still full and stress callback available, try to spill a dirty page.
		// Merges SQLite's two-phase sqlite3PcacheFetch + sqlite3PcacheFetchStress
		// (pcache.c:403-490) into a single call. Admission control (step 3),
		// eviction (step 4), stress callback, and allocation (step 5) are all
		// handled inline. See "Known Drifts in Page Cache" in NOTES.md.
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
				// Writer cache: evicted buffers are NOT returned to slab here
				// (writePages aliasing concern — see comment above).
				for len(pc.pages) >= pc.maxPages && pc.nRecyclable > 0 {
					pc.evictOne()
				}
			}
		}
	}

	// Allocate a page struct: try recycled (direct reuse from step 4), then
	// pFree, then bulk init, then slab directly.
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
			var data []byte
			if globalPageSlab.Initialized(pc.pageSize) {
				data = globalPageSlab.Get()
			} else {
				data = make([]byte, pc.pageSize)
			}
			p = &page{
				data:  data,
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
// When the cache is overfull (len(pages) > maxPages) and the global slab is under
// memory pressure, clean pages are immediately evicted instead of being added to
// the LRU. Inspired by SQLite pcache1.c:1094 (pcache1Unpin). Our condition is
// stricter: requires both global slab pressure AND per-cache overfull, whereas
// SQLite uses an OR with reuseUnlikely || nPurgeable > nMaxPage at PGroup level.
func (pc *pcache) release(p *page) {
	p.pinCount--
	if p.pinCount <= 0 {
		p.pinCount = 0
		// Only add to LRU if the page is still tracked in pcache.pages.
		// After a stress spill + eviction, the page may still be referenced
		// by pager.writePages but is no longer in pcache.pages. Adding such
		// "ghost" pages to the LRU would cause evictOne to loop without
		// reducing len(pages).
		if p.dirty {
			if pc.pages[p.pgno] == p {
				pc.dirtyMoveToFront(p)
			}
		} else if pc.purgeable && pc.pages[p.pgno] == p {
			// Non-purgeable caches (InMemory) skip LRU entirely — pages are
			// never evicted. Matches SQLite pcache.c:265-271 (pcacheUnpin is
			// a no-op for non-purgeable caches).
			//
			// If cache is overfull and slab is under pressure, immediately
			// evict instead of adding to LRU (matches pcache1Unpin).
			// Skip for writer caches (xStress != nil) because pager.writePages
			// may still reference the evicted page struct after spill. Returning
			// the buffer to slab while writePages holds a reference would allow
			// another cache to reuse the buffer, causing data corruption when
			// the writer re-accesses the page via getWritablePage.
			// Writer pages are evicted through create() step 4 or stress callback;
			// their buffers are returned to slab at commit/rollback time.
			if pc.xStress == nil && globalPageSlab.UnderPressure() && len(pc.pages) > pc.maxPages {
				delete(pc.pages, p.pgno)
				// Add to pFree for direct reuse by create() instead of returning
				// buffer to slab (which would drop it — slab is full under
				// pressure). Matches SQLite pcache1FreePage: bulk-local pages
				// go to pCache->pFree (pcache1.c:475-477).
				pc.pFree = append(pc.pFree, p)
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

// clear removes all pages from the cache, returning all data buffers to the
// global slab. Also returns pFree buffers to the slab to prevent memory leaks
// when a cache is cleared and reused.
// Matches SQLite pcache.c release path — buffers go back to pcache1Free.
func (pc *pcache) clear() {
	slabOk := globalPageSlab.Initialized(pc.pageSize)
	for _, p := range pc.pages {
		if slabOk {
			globalPageSlab.Put(p.data)
			p.data = nil
		}
	}
	// Return pFree buffers to slab as well
	if slabOk {
		for _, p := range pc.pFree {
			globalPageSlab.Put(p.data)
			p.data = nil
		}
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

// discard removes a specific page from the cache and returns its data buffer
// to the global slab.
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
	if globalPageSlab.Initialized(pc.pageSize) {
		globalPageSlab.Put(p.data)
		p.data = nil
	}
	delete(pc.pages, pgno)
}

// truncate removes all pages with pgno > maxPage, returning their data
// buffers to the global slab.
func (pc *pcache) truncate(maxPage uint32) {
	slabOk := globalPageSlab.Initialized(pc.pageSize)
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
			if slabOk {
				globalPageSlab.Put(p.data)
				p.data = nil
			}
			delete(pc.pages, pgno)
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
