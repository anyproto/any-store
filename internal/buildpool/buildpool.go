// Package buildpool holds the process-wide budget that vector index builds
// and compactions — HNSW (internal/vindex) and IVF (internal/vivf) alike —
// run their build tasks under (anystore.SetVectorBuildConcurrency): however
// many indexes build at once, no more build tasks execute concurrently than
// the budget has tokens. Whatever executes a build task holds a token while
// it does, whether a worker goroutine or a phase's own goroutine running its
// tasks in turn. The serial work a build does between its phases — btree
// writes, sorts, centroid updates, sampling — holds none and is outside the
// cap.
//
// A phase takes the current budget when it starts and keeps it to the end, so
// a Set while it runs changes only the phases that start after. Every holder
// returns its token when its phase's tasks run out, so phases back to back
// each get the budget afresh. A goroutine must not wait on the budget while
// it holds one of its tokens: with every token so held, nothing completes.
package buildpool

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// Budget is a number of tokens; whatever executes a build task holds one.
type Budget struct{ tokens chan struct{} }

var current atomic.Pointer[Budget]

func init() { Set(runtime.GOMAXPROCS(0)) }

// Set replaces the process-wide budget with one of n tokens; n < 1 is treated
// as 1. Phases already running keep the budget they took.
func Set(n int) {
	current.Store(&Budget{tokens: make(chan struct{}, max(n, 1))})
}

// Current returns the budget a phase about to start draws its tokens from.
func Current() *Budget { return current.Load() }

// Size returns how many build tasks the budget lets execute at once.
func (b *Budget) Size() int { return cap(b.tokens) }

// Acquire takes a token, waiting until one is free. Release returns it.
func (b *Budget) Acquire() { b.tokens <- struct{}{} }

// Release returns a token taken by Acquire.
func (b *Budget) Release() { <-b.tokens }

// Run calls fn(task) once for each task in [0, tasks) and returns when all
// have returned. With room for more than one worker it runs them on up to
// Size() goroutines, each holding a token while it takes tasks in turn;
// otherwise the calling goroutine takes a token and calls them in order. The
// caller must not hold a token of b, and fn must not wait on the budget.
func (b *Budget) Run(tasks int, fn func(task int)) {
	if tasks <= 0 {
		return
	}
	workers := min(b.Size(), tasks)
	if workers == 1 {
		b.Acquire()
		defer b.Release()
		for t := 0; t < tasks; t++ {
			fn(t)
		}
		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			b.Acquire()
			defer b.Release()
			for {
				t := int(next.Add(1) - 1)
				if t >= tasks {
					return
				}
				fn(t)
			}
		})
	}
	wg.Wait()
}
