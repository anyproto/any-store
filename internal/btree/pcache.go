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

	// LRU list for clean pages (dirty pages are not evicted)
	lruHead *page
	lruTail *page
	nClean  int

	// Dirty page list
	dirtyHead *page
	nDirty    int
}

func newPcache(pageSize, maxPages int) *pcache {
	if maxPages <= 0 {
		maxPages = defaultCacheSize
	}
	return &pcache{
		pages:    make(map[uint32]*page),
		maxPages: maxPages,
		pageSize: pageSize,
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

// fetchPinned retrieves a page from the cache and returns both the page and
// whether it was dirty at fetch time. The dirty flag is captured under the
// pcache lock, avoiding a data race with concurrent makeDirty calls.
// Returns (nil, false) if the page is not cached.
func (pc *pcache) fetchPinned(pgno uint32) (*page, bool) {
	pc.mu.Lock()
	p := pc.pages[pgno]
	if p == nil {
		pc.mu.Unlock()
		return nil, false
	}
	p.pinCount++
	wasDirty := p.dirty
	if !wasDirty {
		pc.lruRemove(p)
	}
	pc.mu.Unlock()
	return p, wasDirty
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

	// Evict clean pages if cache is full
	for len(pc.pages) >= pc.maxPages && pc.nClean > 0 {
		pc.evictOne()
	}

	p := &page{
		pgno:     pgno,
		data:     make([]byte, pc.pageSize),
		pinCount: 1,
	}
	pc.pages[pgno] = p
	return p
}

// release unpins a page. If the page is clean, it goes to the LRU list.
func (pc *pcache) release(p *page) {
	pc.mu.Lock()
	p.pinCount--
	if p.pinCount <= 0 {
		p.pinCount = 0
		if !p.dirty {
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
