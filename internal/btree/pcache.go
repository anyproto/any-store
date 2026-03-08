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
// If the cache is full, it evicts clean pages first. If no clean pages
// are available and xStress is set, invokes the stress callback to spill
// a dirty page, making it clean and evictable.
func (pc *pcache) create(pgno uint32) *page {
	return pc.createInternal(pgno, false)
}

// createNoStress is like create but never invokes the xStress callback.
// Used by getPageAt which is called from both writer and reader goroutines.
// Reader goroutines must not trigger xStress (pagerStress) because it
// accesses writer-only unsynchronized fields (doNotSpill, dontWritePages,
// savepoints). Normal clean-page eviction still happens.
func (pc *pcache) createNoStress(pgno uint32) *page {
	return pc.createInternal(pgno, true)
}

func (pc *pcache) createInternal(pgno uint32, noStress bool) *page {
	pc.mu.Lock()

	if p := pc.pages[pgno]; p != nil {
		p.pinCount++
		if !p.dirty {
			pc.lruRemove(p)
		}
		pc.mu.Unlock()
		return p
	}

	// Evict clean pages if cache is full (skip for non-purgeable / InMemory caches)
	if pc.purgeable {
		for len(pc.pages) >= pc.maxPages && pc.nClean > 0 {
			pc.evictOne()
		}

		// If still full and stress callback available, try to spill a dirty page.
		// Modeled after sqlite3PcacheFetchStress() in pcache.c.
		// noStress skips this: reader goroutines must not invoke pagerStress
		// which accesses writer-only fields without synchronization.
		if !noStress {
			spill := pc.szSpill
			if spill == 0 {
				spill = pc.maxPages
			}
			if len(pc.pages) >= spill && pc.xStress != nil {
				victim := pc.findSpillVictim()
				if victim != nil {
					pc.mu.Unlock()
					// DRIFT from SQLite: we ignore the xStress error here because
					// create() has no error return. SQLite's FetchStress returns
					// the error but only for OOM/non-BUSY cases.
					pc.xStress(victim)
					pc.mu.Lock()
					// DRIFT from SQLite: SQLite does not re-check after stress
					// because pcache operations are single-threaded per connection.
					// We must re-check because concurrent reader goroutines can
					// create cache entries while the pcache lock was dropped.
					if p := pc.pages[pgno]; p != nil {
						p.pinCount++
						if !p.dirty {
							pc.lruRemove(p)
						}
						pc.mu.Unlock()
						return p
					}
					// After stress callback, victim should be clean. Retry eviction.
					for len(pc.pages) >= pc.maxPages && pc.nClean > 0 {
						pc.evictOne()
					}
				}
			}
		}
	}

	p := &page{
		pgno:     pgno,
		data:     make([]byte, pc.pageSize),
		pinCount: 1,
	}
	pc.pages[pgno] = p
	pc.mu.Unlock()
	return p
}

// release unpins a page. If the page is clean, it goes to the LRU list.
func (pc *pcache) release(p *page) {
	pc.mu.Lock()
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

// reinsertDirty re-registers a page in the cache (if evicted) and marks it
// dirty. Used when a spilled page (made clean by pagerStress, possibly
// evicted from pcache) is re-acquired for writing via pager.writePages.
// Without this, post-spill modifications would be lost at commit time
// because appendDirtyPages only collects dirty pages.
func (pc *pcache) reinsertDirty(p *page) {
	pc.mu.Lock()
	if existing := pc.pages[p.pgno]; existing != p {
		// A concurrent reader may have created a new cache entry for this
		// pgno while the spilled page was evicted. Remove it before
		// overwriting to prevent evictOne from later deleting our entry
		// or orphaning a dirty page in the dirty linked list.
		if existing != nil {
			if existing.dirty {
				// Remove from dirty linked list to prevent orphaned entries
				// that would cause stale data in appendDirtyPages/dirtyPages.
				if existing.prev != nil {
					existing.prev.next = existing.next
				} else {
					pc.dirtyHead = existing.next
				}
				if existing.next != nil {
					existing.next.prev = existing.prev
				}
				existing.next = nil
				existing.prev = nil
				existing.dirty = false
				pc.nDirty--
			} else {
				pc.lruRemove(existing)
			}
		}
		pc.pages[p.pgno] = p
	}
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
// Must be called with pc.mu held.
func (pc *pcache) findSpillVictim() *page {
	for p := pc.dirtyHead; p != nil; p = p.next {
		if p.pinCount == 0 {
			return p
		}
	}
	return nil
}
