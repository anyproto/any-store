package btree

// pcache implements a simple page cache with LRU eviction, modeled after
// SQLite's pcache1.c. It maps page numbers to in-memory page objects and
// manages dirty page tracking.
//
// Each pcache instance is owned by a single goroutine (writer or reader),
// so no mutex is needed. This matches SQLite's per-connection page cache model.

const defaultCacheSize = 5000

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
	lruHead *page
	lruTail *page
	nClean  int

	// Dirty page list
	dirtyHead *page
	nDirty    int

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
// If the cache is full, it evicts clean pages first. If no clean pages
// are available and xStress is set, invokes the stress callback to spill
// a dirty page, making it clean and evictable.
func (pc *pcache) create(pgno uint32) *page {
	if p := pc.pages[pgno]; p != nil {
		p.pinCount++
		if !p.dirty {
			pc.lruRemove(p)
		}
		return p
	}

	// Evict clean pages if cache is full (skip for non-purgeable / InMemory caches).
	// Capture the last evicted page so we can reuse its buffer.
	// Matches SQLite pcache1.c:897-914 (step 4 — reuses LRU victim's buffer).
	// Evict clean pages if cache is full (skip for non-purgeable / InMemory caches).
	// NOTE: evicted page buffers are NOT returned to the slab here because the
	// pager's writePages map may still reference evicted page structs (after spill).
	// Buffers are returned to the slab in clear() and discard() instead, where the
	// pager has already cleaned up writePages references.
	if pc.purgeable {
		for len(pc.pages) >= pc.maxPages && pc.nClean > 0 {
			pc.evictOne()
		}

		// If still full and stress callback available, try to spill a dirty page.
		// Modeled after sqlite3PcacheFetchStress() in pcache.c.
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
				for len(pc.pages) >= pc.maxPages && pc.nClean > 0 {
					pc.evictOne()
				}
			}
		}
	}

	// Allocate a page struct: try pFree first, then bulk init, then slab directly.
	// Matches SQLite pcache1.c:434-438 (pcache1AllocPage tries pFree first).
	var p *page
	if n := len(pc.pFree); n > 0 {
		p = pc.pFree[n-1]
		pc.pFree = pc.pFree[:n-1]
		// Reset fields for the new page
		clear(p.data)
		p.pgno = pgno
		p.pinCount = 1
		p.dirty = false
		p.uncached = false
		p.next = nil
		p.prev = nil
		p.header = pageHeader{}
	} else {
		if !pc.bulkInit {
			pc.initBulk()
			// Retry from pFree after bulk init
			if n := len(pc.pFree); n > 0 {
				p = pc.pFree[n-1]
				pc.pFree = pc.pFree[:n-1]
				clear(p.data)
				p.pgno = pgno
				p.pinCount = 1
				p.dirty = false
				p.uncached = false
				p.next = nil
				p.prev = nil
				p.header = pageHeader{}
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
				pgno:     pgno,
				data:     data,
				cache:    pc,
				pinCount: 1,
			}
		}
	}
	pc.pages[pgno] = p
	return p
}

// release unpins a page. If the page is clean, it goes to the LRU list.
// If the page is dirty, it moves to the front of the dirty list (MRU position).
// Matches SQLite pcache.c:558 (pcacheManageDirtyList PCACHE_DIRTYLIST_FRONT on unpin).
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
			pc.dirtyMoveToFront(p)
		} else if pc.pages[p.pgno] == p {
			pc.lruPrepend(p)
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
	}
	// Insert at head
	p.prev = nil
	p.next = pc.dirtyHead
	if pc.dirtyHead != nil {
		pc.dirtyHead.prev = p
	}
	pc.dirtyHead = p
}

// dirtyPages returns all dirty pages using the provided slice to avoid allocations.
// The returned slice may be a sub-slice of buf or a new allocation if buf is too small.
func (pc *pcache) dirtyPages() []*page {
	result := make([]*page, 0, pc.nDirty)
	for p := pc.dirtyHead; p != nil; p = p.next {
		result = append(result, p)
	}
	return result
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
		}
	}
	// Return pFree buffers to slab as well
	if slabOk {
		for _, p := range pc.pFree {
			globalPageSlab.Put(p.data)
		}
	}
	pc.pFree = pc.pFree[:0]
	pc.bulkInit = false
	clear(pc.pages)
	pc.lruHead = nil
	pc.lruTail = nil
	pc.dirtyHead = nil
	pc.nClean = 0
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
		}
		pc.nDirty--
	} else {
		pc.lruRemove(p)
	}
	if globalPageSlab.Initialized(pc.pageSize) {
		globalPageSlab.Put(p.data)
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
				}
				pc.nDirty--
			} else {
				pc.lruRemove(p)
			}
			if slabOk {
				globalPageSlab.Put(p.data)
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
	pc.nClean++
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
	pc.nClean--
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
	pc.nClean--
	delete(pc.pages, p.pgno)
	return p
}

// findSpillVictim walks the dirty list and returns the oldest unpinned page
// (last match walking from head = back of the list), or nil if all dirty
// pages are pinned. Combined with dirtyMoveToFront on release, this ensures
// recently-released dirty pages are preserved while stale ones are spilled.
func (pc *pcache) findSpillVictim() *page {
	var victim *page
	for p := pc.dirtyHead; p != nil; p = p.next {
		if p.pinCount == 0 {
			victim = p
		}
	}
	return victim
}
