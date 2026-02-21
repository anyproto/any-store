package btree

// pcache implements a simple page cache with LRU eviction, modeled after
// SQLite's pcache1.c. It maps page numbers to in-memory page objects and
// manages dirty page tracking.

import "sync"

const defaultCacheSize = 5000

// pcache is the page cache.
type pcache struct {
	mu       sync.Mutex
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
	pc.mu.Lock()
	p := pc.pages[pgno]
	if p != nil {
		p.pinCount++
		if !p.dirty {
			pc.lruRemove(p)
		}
	}
	pc.mu.Unlock()
	return p
}

// create allocates a new page in the cache and returns it pinned.
// If the cache is full, it evicts clean pages first.
func (pc *pcache) create(pgno uint32) *page {
	pc.mu.Lock()
	defer pc.mu.Unlock()

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
	}

	p := &page{
		pgno:     pgno,
		data:     make([]byte, pc.pageSize),
		pinCount: 1,
	}
	pc.pages[pgno] = p
	return p
}

// release unpins a page. If the page is clean and still current in the cache,
// it goes to the LRU list. Orphaned pages (replaced by commit's insertClean)
// are silently dropped — they'll be GC'd when all references are released.
func (pc *pcache) release(p *page) {
	pc.mu.Lock()
	p.pinCount--
	if p.pinCount <= 0 {
		p.pinCount = 0
		if !p.dirty && pc.pages[p.pgno] == p {
			pc.lruAppend(p)
		}
	}
	pc.mu.Unlock()
}

// makeDirty marks a page as dirty.
func (pc *pcache) makeDirty(p *page) {
	pc.mu.Lock()
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
	pc.mu.Unlock()
}

// fetchAndMakeDirty retrieves a page and marks it dirty in one lock acquisition.
// Returns nil if page is not cached.
func (pc *pcache) fetchAndMakeDirty(pgno uint32) *page {
	pc.mu.Lock()
	p := pc.pages[pgno]
	if p != nil {
		p.pinCount++
		if !p.dirty {
			pc.lruRemove(p)
			p.dirty = true
			p.next = pc.dirtyHead
			p.prev = nil
			if pc.dirtyHead != nil {
				pc.dirtyHead.prev = p
			}
			pc.dirtyHead = p
			pc.nDirty++
		}
	}
	pc.mu.Unlock()
	return p
}

// makeClean marks a page as clean (after writing to disk).
func (pc *pcache) makeClean(p *page) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
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

// makeCleanBatch marks multiple pages as clean in a single lock acquisition.
func (pc *pcache) makeCleanBatch(pages []*page) {
	pc.mu.Lock()
	for _, p := range pages {
		if p.dirty {
			p.dirty = false
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
	pc.mu.Unlock()
}

// dirtyPages returns all dirty pages using the provided slice to avoid allocations.
// The returned slice may be a sub-slice of buf or a new allocation if buf is too small.
func (pc *pcache) dirtyPages() []*page {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	result := make([]*page, 0, pc.nDirty)
	for p := pc.dirtyHead; p != nil; p = p.next {
		result = append(result, p)
	}
	return result
}

// appendDirtyPages appends all dirty pages to the provided slice and returns it.
func (pc *pcache) appendDirtyPages(buf []*page) []*page {
	pc.mu.Lock()
	for p := pc.dirtyHead; p != nil; p = p.next {
		buf = append(buf, p)
	}
	pc.mu.Unlock()
	return buf
}

// clear removes all pages from the cache.
func (pc *pcache) clear() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	clear(pc.pages)
	pc.lruHead = nil
	pc.lruTail = nil
	pc.dirtyHead = nil
	pc.nClean = 0
	pc.nDirty = 0
}

// discard removes a specific page from the cache.
func (pc *pcache) discard(pgno uint32) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
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
	pc.mu.Lock()
	defer pc.mu.Unlock()
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
	if p.prev != nil {
		p.prev.next = p.next
	} else if pc.lruHead == p {
		pc.lruHead = p.next
	}
	if p.next != nil {
		p.next.prev = p.prev
	} else if pc.lruTail == p {
		pc.lruTail = p.prev
	}
	if pc.lruHead == p || pc.lruTail == p {
		// p was not in LRU list
		return
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

// insertClean inserts a page into the cache as clean and unpinned, replacing
// any existing entry for the same page number. The old entry (if any) is
// orphaned — readers that still hold it will use stale-but-valid data and
// the page will be GC'd when all references are released.
// Used by the commit path to update the cache with newly-committed COW pages.
func (pc *pcache) insertClean(pg *page) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if old := pc.pages[pg.pgno]; old != nil {
		// Remove old entry from LRU (it should be clean with COW).
		if !old.dirty {
			pc.lruRemove(old)
		} else {
			// Shouldn't happen with COW, but handle gracefully.
			if old.prev != nil {
				old.prev.next = old.next
			} else {
				pc.dirtyHead = old.next
			}
			if old.next != nil {
				old.next.prev = old.prev
			}
			old.next = nil
			old.prev = nil
			pc.nDirty--
		}
		// Old page is now orphaned. Readers holding it see valid old data.
	}

	pg.dirty = false
	pg.uncached = false
	pg.pinCount = 0
	pg.next = nil
	pg.prev = nil
	pc.pages[pg.pgno] = pg
	pc.lruAppend(pg)

	// Evict if over limit.
	if pc.purgeable {
		for len(pc.pages) > pc.maxPages && pc.nClean > 0 {
			pc.evictOne()
		}
	}
}
