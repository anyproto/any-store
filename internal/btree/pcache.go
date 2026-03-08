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

	// Evict clean pages if cache is full (skip for non-purgeable / InMemory caches)
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
				// the error but only for OOM/non-BUSY cases.
				pc.xStress(victim)
				// After stress callback, victim should be clean. Retry eviction.
				for len(pc.pages) >= pc.maxPages && pc.nClean > 0 {
					pc.evictOne()
				}
			}
		}
	}

	p := &page{
		pgno:     pgno,
		data:     make([]byte, pc.pageSize),
		cache:    pc,
		pinCount: 1,
	}
	pc.pages[pgno] = p
	return p
}

// release unpins a page. If the page is clean, it goes to the LRU list.
func (pc *pcache) release(p *page) {
	p.pinCount--
	if p.pinCount <= 0 {
		p.pinCount = 0
		// Only add to LRU if the page is still tracked in pcache.pages.
		// After a stress spill + eviction, the page may still be referenced
		// by pager.writePages but is no longer in pcache.pages. Adding such
		// "ghost" pages to the LRU would cause evictOne to loop without
		// reducing len(pages).
		if !p.dirty && pc.pages[p.pgno] == p {
			pc.lruAppend(p)
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
			pc.lruAppend(p)
		}
	}
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

// clear removes all pages from the cache.
func (pc *pcache) clear() {
	clear(pc.pages)
	pc.lruHead = nil
	pc.lruTail = nil
	pc.dirtyHead = nil
	pc.nClean = 0
	pc.nDirty = 0
}

// discard removes a specific page from the cache.
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
	delete(pc.pages, pgno)
}

// truncate removes all pages with pgno > maxPage.
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
				}
				pc.nDirty--
			} else {
				pc.lruRemove(p)
			}
			delete(pc.pages, pgno)
		}
	}
}

func (pc *pcache) lruAppend(p *page) {
	p.next = nil
	p.prev = pc.lruTail
	if pc.lruTail != nil {
		pc.lruTail.next = p
	} else {
		pc.lruHead = p
	}
	pc.lruTail = p
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

func (pc *pcache) evictOne() {
	if pc.lruHead == nil {
		return
	}
	p := pc.lruHead
	pc.lruHead = p.next
	if pc.lruHead != nil {
		pc.lruHead.prev = nil
	} else {
		pc.lruTail = nil
	}
	p.next = nil
	p.prev = nil
	pc.nClean--
	delete(pc.pages, p.pgno)
}

// findSpillVictim walks the dirty list and returns the first page with
// pinCount == 0, or nil if all dirty pages are pinned.
func (pc *pcache) findSpillVictim() *page {
	for p := pc.dirtyHead; p != nil; p = p.next {
		if p.pinCount == 0 {
			return p
		}
	}
	return nil
}
